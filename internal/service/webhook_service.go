package service

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	_ "time/tzdata"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/bark"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/serverchan"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/webhook"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
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
	DeliverReminder(ctx context.Context, uid, id int64, title, due, timezone, vault, path, content, link string) error
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
	if request == nil {
		return nil, errors.New("webhook request is required")
	}
	value := *request
	value.Provider = normalizeWebhookProvider(value.Provider)
	value.URL = strings.TrimSpace(value.URL)
	if value.Provider == domain.WebhookProviderBark && value.URL == "" {
		value.URL = bark.DefaultEndpoint
	}
	if value.Provider == domain.WebhookProviderServerChan {
		value.URL = ""
	}
	if value.Provider == domain.WebhookProviderCustom {
		value.Method = normalizeWebhookMethod(value.Method)
		value.Headers = normalizeWebhookHeaders(value.Headers)
	} else {
		value.Method = ""
		value.Headers = nil
	}
	secret := value.Secret
	if value.Provider == domain.WebhookProviderCustom {
		secret = ""
	}
	if value.ID > 0 {
		old, err := s.repo.GetByID(ctx, value.ID, uid)
		if err != nil {
			return nil, err
		}
		if old == nil {
			return nil, errors.New("webhook subscription not found")
		}
		if secret == "" && value.Provider != domain.WebhookProviderCustom {
			if normalizeWebhookProvider(old.Provider) != value.Provider {
				return nil, errors.New("enter a new credential when changing notification provider")
			}
			secret = old.Secret
		}
	}
	secret = strings.TrimSpace(secret)
	value.Secret = secret
	if err := validateWebhookRequest(&value); err != nil {
		return nil, err
	}
	request = &value
	item, err := s.repo.Save(ctx, &domain.WebhookSubscription{
		ID: request.ID, UID: uid, Enabled: request.Enabled, Provider: request.Provider, URL: strings.TrimSpace(request.URL), Method: request.Method, Headers: request.Headers, Secret: secret,
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
	if !subscription.Enabled {
		return errors.New("webhook subscription is disabled")
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
func (s *webhookService) DeliverReminder(ctx context.Context, uid, id int64, title, due, timezone, vault, path, content, link string) error {
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
	if !subscription.Enabled {
		return errors.New("webhook subscription is disabled")
	}
	sender := s.senders[normalizeWebhookProvider(subscription.Provider)]
	if sender == nil {
		return fmt.Errorf("unsupported webhook provider: %s", subscription.Provider)
	}
	sendCtx, cancel := context.WithTimeout(ctx, notification.Timeout)
	defer cancel()
	return sendNotification(sendCtx, sender, subscription, messageForReminder(subscription, title, due, timezone, vault, path, content, link))
}

// Test sends a synthetic message through a saved subscription. It intentionally
// does not require the channel to be enabled: this lets a user validate a new
// credential before turning the channel on.
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
	if request == nil {
		return errors.New("webhook request is required")
	}
	value := *request
	value.Provider = normalizeWebhookProvider(value.Provider)
	value.URL = strings.TrimSpace(value.URL)
	if value.Provider == domain.WebhookProviderBark && value.URL == "" {
		value.URL = bark.DefaultEndpoint
	}
	if value.Provider == domain.WebhookProviderServerChan {
		value.URL = ""
	}
	if value.Provider == domain.WebhookProviderCustom {
		value.Secret = ""
		value.Method = normalizeWebhookMethod(value.Method)
		value.Headers = normalizeWebhookHeaders(value.Headers)
	} else {
		value.Method = ""
		value.Headers = nil
	}
	if value.Provider != domain.WebhookProviderCustom && value.ID > 0 && strings.TrimSpace(value.Secret) == "" {
		old, err := s.repo.GetByID(ctx, value.ID, uid)
		if err != nil {
			return err
		}
		if old == nil {
			return errors.New("webhook subscription not found")
		}
		if normalizeWebhookProvider(old.Provider) != value.Provider {
			return errors.New("enter a new credential when changing notification provider")
		}
		value.Secret = old.Secret
	}
	value.Secret = strings.TrimSpace(value.Secret)
	if err := validateWebhookRequest(&value); err != nil {
		return err
	}
	subscription := &domain.WebhookSubscription{
		ID: value.ID, UID: uid, Enabled: value.Enabled, Provider: value.Provider, URL: value.URL, Method: value.Method, Headers: value.Headers, Secret: value.Secret,
		TitleTemplate: value.TitleTemplate, BodyTemplate: value.BodyTemplate,
	}
	sender := s.senders[normalizeWebhookProvider(subscription.Provider)]
	if sender == nil {
		return fmt.Errorf("unsupported webhook provider: %s", subscription.Provider)
	}
	return s.sendTest(ctx, subscription, sender)
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

func validateCustomHeaders(headers map[string]string) error {
	if len(headers) > maxCustomHeaders {
		return fmt.Errorf("custom webhook supports at most %d headers", maxCustomHeaders)
	}
	seen := make(map[string]struct{}, len(headers))
	for rawName, value := range headers {
		name := strings.TrimSpace(rawName)
		if name == "" || len(name) > 256 || !validHTTPHeaderName(name) {
			return fmt.Errorf("invalid custom webhook header name %q", rawName)
		}
		if len(value) > maxCustomHeaderValue || strings.ContainsAny(value, "\r\n") {
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

func webhookToDTO(item *domain.WebhookSubscription) *dto.WebhookSubscriptionDTO {
	if item == nil {
		return nil
	}
	titleTemplate, bodyTemplate := notificationTemplates(item, false)
	headers := normalizeWebhookHeaders(item.Headers)
	return &dto.WebhookSubscriptionDTO{ID: item.ID, UID: item.UID, Enabled: item.Enabled, Provider: normalizeWebhookProvider(item.Provider), URL: item.URL, Method: normalizeWebhookMethod(item.Method), Headers: headers, HasSecret: item.Secret != "", TitleTemplate: titleTemplate, BodyTemplate: bodyTemplate, CreatedAt: timex.Time(item.CreatedAt).String(), UpdatedAt: timex.Time(item.UpdatedAt).String()}
}
