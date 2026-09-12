package service

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	_ "time/tzdata"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/bark"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/serverchan"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/webhook"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
)

const (
	defaultWebhookBodyBytes = 64 * 1024
	maxWebhookBodyBytes     = 1024 * 1024
	maxCustomHeaders        = 64
	maxCustomHeaderValue    = 8192
)

// WebhookService manages user-owned webhook subscriptions.
type WebhookService interface {
	List(ctx context.Context, uid int64) ([]*dto.WebhookSubscriptionDTO, error)
	Save(ctx context.Context, uid int64, request *dto.WebhookSubscriptionRequest) (*dto.WebhookSubscriptionDTO, error)
	Delete(ctx context.Context, uid, id int64) error
	DeliverEvent(ctx context.Context, uid, id int64, event *domain.ContentChangeEvent) error
	DeliverReminder(ctx context.Context, uid, id int64, task, due, timezone, vault, path, content, obsidianURI string) error
	Test(ctx context.Context, uid, id int64) error
	TestRequest(ctx context.Context, uid int64, request *dto.WebhookSubscriptionRequest) error
}

type webhookService struct {
	repo    domain.WebhookRepository
	senders map[string]notification.Sender
}

// NewWebhookService creates a webhook subscription service.
func NewWebhookService(repo domain.WebhookRepository) WebhookService {
	client := notification.HTTPClient(nil)
	return &webhookService{repo: repo, senders: map[string]notification.Sender{
		domain.WebhookProviderServerChan: serverchan.NewClient(client),
		domain.WebhookProviderBark:       bark.NewClient(client),
		domain.WebhookProviderCustom:     webhook.NewClient(client),
	}}
}

func (s *webhookService) List(ctx context.Context, uid int64) ([]*dto.WebhookSubscriptionDTO, error) {
	items, err := s.repo.List(ctx, uid)
	if err != nil {
		return nil, err
	}
	result := make([]*dto.WebhookSubscriptionDTO, 0, len(items))
	for _, item := range items {
		result = append(result, webhookToDTO(item))
	}
	return result, nil
}

func (s *webhookService) Save(ctx context.Context, uid int64, request *dto.WebhookSubscriptionRequest) (*dto.WebhookSubscriptionDTO, error) {
	request, err := s.prepareRequest(ctx, uid, request)
	if err != nil {
		return nil, err
	}
	item, err := s.repo.Save(ctx, &domain.WebhookSubscription{
		ID: request.ID, UID: uid, Provider: request.Provider, URL: strings.TrimSpace(request.URL), Method: request.Method, Headers: request.Headers, Secret: request.Secret,
		TitleTemplate: request.TitleTemplate, BodyTemplate: request.BodyTemplate,
	}, uid)
	if err != nil {
		return nil, err
	}
	return webhookToDTO(item), nil
}

func (s *webhookService) Delete(ctx context.Context, uid, id int64) error {
	if id <= 0 {
		return errors.New("webhook subscription id is required")
	}
	return s.repo.Delete(ctx, id, uid)
}

// DeliverEvent sends an automation-selected event through one saved channel.
// The channel remains the owner of provider credentials and transport details;
// an automation trigger only stores its ID.
func (s *webhookService) DeliverEvent(ctx context.Context, uid, id int64, event *domain.ContentChangeEvent) error {
	if id <= 0 {
		return errors.New("webhook subscription id is required")
	}
	if event == nil {
		return errors.New("webhook event is required")
	}
	subscription, err := s.repo.GetByID(ctx, id, uid)
	if err != nil {
		return err
	}
	if subscription == nil {
		return errors.New("webhook subscription not found")
	}
	sender := s.senders[normalizeWebhookProvider(subscription.Provider)]
	if sender == nil {
		return fmt.Errorf("unsupported webhook provider: %s", subscription.Provider)
	}
	sendCtx, cancel := context.WithTimeout(ctx, notification.Timeout)
	defer cancel()
	return sendNotification(sendCtx, sender, subscription, messageForNoteEvent(event, subscription))
}

// DeliverReminder sends a task reminder through one configured notification
// target. The reminder trigger owns scheduling and timezone; the channel only
// owns credentials and transport.
func (s *webhookService) DeliverReminder(ctx context.Context, uid, id int64, task, due, timezone, vault, path, content, obsidianURI string) error {
	if id <= 0 {
		return errors.New("webhook subscription id is required")
	}
	subscription, err := s.repo.GetByID(ctx, id, uid)
	if err != nil {
		return err
	}
	if subscription == nil {
		return errors.New("webhook subscription not found")
	}
	sender := s.senders[normalizeWebhookProvider(subscription.Provider)]
	if sender == nil {
		return fmt.Errorf("unsupported webhook provider: %s", subscription.Provider)
	}
	sendCtx, cancel := context.WithTimeout(ctx, notification.Timeout)
	defer cancel()
	return sendNotification(sendCtx, sender, subscription, messageForReminder(subscription, task, due, timezone, vault, path, content, obsidianURI))
}

// Test sends a synthetic message through a saved subscription.
func (s *webhookService) Test(ctx context.Context, uid, id int64) error {
	if id <= 0 {
		return errors.New("webhook subscription id is required")
	}
	subscription, err := s.repo.GetByID(ctx, id, uid)
	if err != nil {
		return err
	}
	if subscription == nil {
		return errors.New("webhook subscription not found")
	}
	sender := s.senders[normalizeWebhookProvider(subscription.Provider)]
	if sender == nil {
		return fmt.Errorf("unsupported webhook provider: %s", subscription.Provider)
	}
	return s.sendTest(ctx, subscription, sender)
}

// TestRequest sends a synthetic message using the current form values without
// persisting them. Existing credentials are reused when editing a saved
// subscription and the credential field is left blank.
func (s *webhookService) TestRequest(ctx context.Context, uid int64, request *dto.WebhookSubscriptionRequest) error {
	prepared, err := s.prepareRequest(ctx, uid, request)
	if err != nil {
		return err
	}
	value := *prepared
	subscription := &domain.WebhookSubscription{
		ID: value.ID, UID: uid, Provider: value.Provider, URL: value.URL, Method: value.Method, Headers: value.Headers, Secret: value.Secret,
		TitleTemplate: value.TitleTemplate, BodyTemplate: value.BodyTemplate,
	}
	sender := s.senders[normalizeWebhookProvider(subscription.Provider)]
	if sender == nil {
		return fmt.Errorf("unsupported webhook provider: %s", subscription.Provider)
	}
	return s.sendTest(ctx, subscription, sender)
}

// prepareRequest keeps save and preview credential handling identical.
func (s *webhookService) prepareRequest(ctx context.Context, uid int64, request *dto.WebhookSubscriptionRequest) (*dto.WebhookSubscriptionRequest, error) {
	if request == nil {
		return nil, errors.New("webhook request is required")
	}
	value := *request
	value.Provider = normalizeWebhookProvider(value.Provider)
	value.URL = strings.TrimSpace(value.URL)
	var old *domain.WebhookSubscription
	if value.ID > 0 {
		var err error
		old, err = s.repo.GetByID(ctx, value.ID, uid)
		if err != nil {
			return nil, err
		}
		if old == nil {
			return nil, errors.New("webhook subscription not found")
		}
	}
	if value.Provider == domain.WebhookProviderBark && value.URL == "" {
		value.URL = bark.DefaultEndpoint
	}
	if value.Provider == domain.WebhookProviderServerChan {
		value.URL = ""
	}
	if value.Provider == domain.WebhookProviderCustom {
		value.Secret = ""
		value.Method = normalizeWebhookMethod(value.Method)
		if err := validateCustomHeaders(value.Headers); err != nil {
			return nil, err
		}
		value.Headers = normalizeWebhookHeaders(value.Headers)
		if old != nil && normalizeWebhookProvider(old.Provider) == domain.WebhookProviderCustom {
			currentURL, currentErr := url.Parse(value.URL)
			oldURL, oldErr := url.Parse(old.URL)
			if currentErr != nil || oldErr != nil || !sameWebhookEndpointBase(currentURL, oldURL) {
				for name, headerValue := range value.Headers {
					if util.IsSensitiveHeaderName(name) && strings.TrimSpace(headerValue) == "" {
						return nil, errors.New("re-enter sensitive headers when changing the notification endpoint")
					}
				}
			} else {
				value.Headers = mergePreservedWebhookHeaders(value.Headers, old.Headers)
			}
			value.URL = mergePreservedWebhookURL(value.URL, old.URL)
		}
		value.TitleTemplate = ""
	} else {
		value.Method = ""
		value.Headers = nil
	}
	if value.Provider != domain.WebhookProviderCustom && old != nil && strings.TrimSpace(value.Secret) == "" {
		if normalizeWebhookProvider(old.Provider) != value.Provider {
			return nil, errors.New("enter a new credential when changing notification provider")
		}
		value.Secret = old.Secret
	}
	value.Secret = strings.TrimSpace(value.Secret)
	if err := validateWebhookRequest(&value); err != nil {
		return nil, err
	}
	return &value, nil
}

func (s *webhookService) sendTest(ctx context.Context, subscription *domain.WebhookSubscription, sender notification.Sender) error {
	sendCtx, cancel := context.WithTimeout(ctx, notification.Timeout)
	defer cancel()
	return sendNotification(sendCtx, sender, subscription, messageForTest(subscription))
}

func validateWebhookRequest(request *dto.WebhookSubscriptionRequest) error {
	if request == nil {
		return errors.New("webhook request is required")
	}
	provider := normalizeWebhookProvider(request.Provider)
	switch provider {
	case domain.WebhookProviderServerChan, domain.WebhookProviderBark, domain.WebhookProviderCustom:
	default:
		return fmt.Errorf("unsupported webhook provider: %s", provider)
	}
	if provider == domain.WebhookProviderServerChan && strings.TrimSpace(request.Secret) == "" {
		return errors.New("serverchan sendkey is required")
	}
	if provider == domain.WebhookProviderBark && strings.TrimSpace(request.Secret) == "" {
		return errors.New("bark device key is required")
	}
	if provider == domain.WebhookProviderCustom {
		if strings.TrimSpace(request.URL) == "" {
			return errors.New("custom webhook URL is required")
		}
		method := normalizeWebhookMethod(request.Method)
		if method != http.MethodGet && method != http.MethodPost {
			return errors.New("custom webhook method must be GET or POST")
		}
		if err := validateCustomHeaders(request.Headers); err != nil {
			return err
		}
	}
	if request.ID < 0 {
		return errors.New("invalid subscription ID")
	}
	if provider == domain.WebhookProviderServerChan {
		if _, err := serverchan.Endpoint(strings.TrimSpace(request.Secret)); err != nil {
			return err
		}
	}
	if provider == domain.WebhookProviderBark {
		if _, err := bark.Endpoint(request.URL); err != nil {
			return err
		}
	}
	if strings.TrimSpace(request.URL) != "" {
		parsed, err := url.Parse(strings.TrimSpace(request.URL))
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return errors.New("notification endpoint must use http or https")
		}
		if parsed.User != nil || parsed.Fragment != "" {
			return errors.New("notification endpoint must not contain user information")
		}
		if host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), "."); host == "localhost" || host == "localhost.localdomain" {
			return errors.New("notification endpoint must not target localhost")
		}
		if ip := net.ParseIP(hostnameWithoutBrackets(parsed.Hostname())); ip != nil && isPrivateWebhookIP(ip) {
			return errors.New("notification endpoint must not target a private address")
		}
	}
	if len(request.TitleTemplate) > 4096 {
		return errors.New("webhook title template is too long")
	}
	if len(request.BodyTemplate) > maxWebhookBodyBytes {
		return errors.New("webhook body template is too long")
	}
	return nil
}

func normalizeWebhookProvider(provider string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		return domain.WebhookProviderServerChan
	}
	return provider
}

func normalizeWebhookMethod(method string) string {
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		return http.MethodPost
	}
	return method
}

func normalizeWebhookHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return map[string]string{}
	}
	result := make(map[string]string, len(headers))
	for key, value := range headers {
		result[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return result
}

func mergePreservedWebhookHeaders(headers, old map[string]string) map[string]string {
	if len(headers) == 0 || len(old) == 0 {
		return headers
	}
	oldByCanonical := make(map[string]string, len(old))
	for key, value := range old {
		oldByCanonical[strings.ToLower(strings.TrimSpace(key))] = value
	}
	for key, value := range headers {
		canonical := strings.ToLower(strings.TrimSpace(key))
		if util.IsSensitiveHeaderName(canonical) && strings.TrimSpace(value) == "" {
			if previous, ok := oldByCanonical[canonical]; ok {
				headers[key] = previous
			}
		}
	}
	return headers
}

func validateCustomHeaders(headers map[string]string) error {
	if len(headers) > maxCustomHeaders {
		return fmt.Errorf("custom webhook supports at most %d headers", maxCustomHeaders)
	}
	seen := make(map[string]struct{}, len(headers))
	for rawName, value := range headers {
		name := strings.TrimSpace(rawName)
		if name == "" || rawName != name || len(name) > 256 || !validHTTPHeaderName(name) {
			return fmt.Errorf("invalid custom webhook header name %q", rawName)
		}
		if len(value) > maxCustomHeaderValue || !validHTTPHeaderValue(value) {
			return fmt.Errorf("invalid custom webhook header value for %q", name)
		}
		canonical := strings.ToLower(name)
		if _, exists := seen[canonical]; exists {
			return fmt.Errorf("duplicate custom webhook header %q", name)
		}
		seen[canonical] = struct{}{}
		switch canonical {
		case "host", "content-length", "transfer-encoding", "connection", "upgrade", "proxy-authorization", "proxy-authenticate":
			return fmt.Errorf("custom webhook header %q is not allowed", name)
		}
	}
	return nil
}

func validHTTPHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if (value[i] < 32 && value[i] != '\t') || value[i] == 127 {
			return false
		}
	}
	return true
}

func validHTTPHeaderName(name string) bool {
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		switch c {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		default:
			return false
		}
	}
	return true
}

func hostnameWithoutBrackets(host string) string { return strings.Trim(host, "[]") }

func isPrivateWebhookIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()
}

// redactWebhookURL removes values from credential-like query parameters before
// an endpoint is returned to the browser. The server keeps the original value
// and mergePreservedWebhookURL restores it when an unchanged endpoint is saved
// with the redacted value left blank.
func redactWebhookURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.RawQuery == "" {
		return rawURL
	}
	parsed.RawQuery = redactWebhookRawQuery(parsed.RawQuery)
	return parsed.String()
}

func redactWebhookRawQuery(rawQuery string) string {
	parts := strings.Split(rawQuery, "&")
	for index, part := range parts {
		separator := strings.IndexByte(part, '=')
		name := part
		if separator >= 0 {
			name = part[:separator]
		}
		decodedName, err := url.QueryUnescape(name)
		if err == nil && util.IsSensitiveURLQueryName(decodedName) {
			parts[index] = name + "="
		}
	}
	return strings.Join(parts, "&")
}

func mergePreservedWebhookURL(current, old string) string {
	currentURL, currentErr := url.Parse(current)
	oldURL, oldErr := url.Parse(old)
	if currentErr != nil || oldErr != nil || !sameWebhookEndpointBase(currentURL, oldURL) {
		return current
	}
	oldValues := rawWebhookQueryValues(oldURL.RawQuery)
	if len(oldValues) == 0 || currentURL.RawQuery == "" {
		return current
	}

	parts := strings.Split(currentURL.RawQuery, "&")
	for index, part := range parts {
		separator := strings.IndexByte(part, '=')
		if separator < 0 {
			continue
		}
		name := part[:separator]
		decodedName, err := url.QueryUnescape(name)
		if err != nil || !util.IsSensitiveURLQueryName(decodedName) {
			continue
		}
		if part[separator+1:] != "" {
			continue
		}
		if previous, ok := oldValues[strings.ToLower(decodedName)]; ok && previous != "" {
			parts[index] = name + "=" + previous
		}
	}
	currentURL.RawQuery = strings.Join(parts, "&")
	return currentURL.String()
}

func sameWebhookEndpointBase(current, old *url.URL) bool {
	if current == nil || old == nil {
		return false
	}
	return strings.EqualFold(current.Scheme, old.Scheme) &&
		strings.EqualFold(current.Host, old.Host) &&
		current.EscapedPath() == old.EscapedPath()
}

func rawWebhookQueryValues(rawQuery string) map[string]string {
	values := make(map[string]string)
	for _, part := range strings.Split(rawQuery, "&") {
		separator := strings.IndexByte(part, '=')
		if separator < 0 {
			continue
		}
		name, err := url.QueryUnescape(part[:separator])
		if err != nil {
			continue
		}
		values[strings.ToLower(name)] = part[separator+1:]
	}
	return values
}

func webhookToDTO(item *domain.WebhookSubscription) *dto.WebhookSubscriptionDTO {
	if item == nil {
		return nil
	}
	titleTemplate, bodyTemplate := notificationTemplates(item, false)
	headers := make(map[string]string, len(item.Headers))
	protectedHeaders := make([]string, 0)
	for key, value := range item.Headers {
		if util.IsSensitiveHeaderName(key) {
			// Keep the JSON object shape while never returning a credential to the
			// browser. Save/Test merge an empty sensitive value with the stored one.
			headers[key] = ""
			if value != "" {
				protectedHeaders = append(protectedHeaders, key)
			}
			continue
		}
		headers[key] = value
	}
	sort.Strings(protectedHeaders)
	return &dto.WebhookSubscriptionDTO{ID: item.ID, UID: item.UID, Provider: normalizeWebhookProvider(item.Provider), URL: redactWebhookURL(item.URL), Method: normalizeWebhookMethod(item.Method), Headers: headers, ProtectedHeaders: protectedHeaders, HasSecret: item.Secret != "", TitleTemplate: titleTemplate, BodyTemplate: bodyTemplate, CreatedAt: timex.Time(item.CreatedAt).String(), UpdatedAt: timex.Time(item.UpdatedAt).String()}
}
