package service

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
)

const (
	defaultNoteTitleTemplate     = "Fast Note Sync: {{action}} {{path}}"
	defaultNoteBodyTemplate      = "- Vault: {{vault}}\n- Action: {{action}}\n- Path: {{path}}\n\n{{content}}"
	defaultReminderTitleTemplate = "{{task}}"
	defaultReminderBodyTemplate  = "{{task}}\n到期：{{due}} ({{timezone}})\n笔记：{{vault}} / {{path}}"
	maxNotificationBodyBytes     = 8192
)

var notificationTemplatePattern = regexp.MustCompile(`\{\{\s*([A-Za-z0-9_]+)\s*\}\}`)

// defaultWebhookTemplates returns the templates used when the user leaves a
// template blank.
func defaultWebhookTemplates(reminder bool) (string, string) {
	if reminder {
		return defaultReminderTitleTemplate, defaultReminderBodyTemplate
	}
	return defaultNoteTitleTemplate, defaultNoteBodyTemplate
}

// renderNotificationTemplate replaces known placeholders and leaves unknown
// placeholders untouched. Leaving an unknown token visible is safer than
// silently dropping user-authored content and makes template typos apparent.
func renderNotificationTemplate(template string, values map[string]string) string {
	return notificationTemplatePattern.ReplaceAllStringFunc(template, func(match string) string {
		parts := notificationTemplatePattern.FindStringSubmatch(match)
		if len(parts) == 2 {
			if value, ok := values[parts[1]]; ok {
				return value
			}
		}
		return match
	})
}

func renderNotificationEndpoint(endpoint string, values map[string]string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return renderNotificationTemplate(endpoint, values)
	}
	if rendered := renderNotificationTemplate(parsed.Path, values); rendered != parsed.Path {
		parsed.Path, parsed.RawPath = rendered, ""
	}
	// Preserve literal query bytes and ordering (signed URLs may depend on
	// them). Only encode values that actually contain a replaced placeholder.
	parts := strings.Split(parsed.RawQuery, "&")
	for index, part := range parts {
		name, rawValue, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		value, err := url.QueryUnescape(rawValue)
		if err != nil {
			continue
		}
		if rendered := renderNotificationTemplate(value, values); rendered != value {
			parts[index] = name + "=" + url.QueryEscape(rendered)
		}
	}
	parsed.RawQuery = strings.Join(parts, "&")
	return parsed.String()
}

func notificationTemplates(subscription *domain.WebhookSubscription, reminder bool) (string, string) {
	title, body := defaultWebhookTemplates(reminder)
	if subscription != nil {
		isCustom := strings.EqualFold(strings.TrimSpace(subscription.Provider), domain.WebhookProviderCustom)
		if isCustom {
			return "", subscription.BodyTemplate
		} else if subscription.TitleTemplate != "" {
			title = subscription.TitleTemplate
		}
		if subscription.BodyTemplate != "" {
			body = subscription.BodyTemplate
		}
	}
	return title, body
}

func notificationBody(subscription *domain.WebhookSubscription, body string) string {
	if subscription != nil && normalizeWebhookProvider(subscription.Provider) == domain.WebhookProviderCustom {
		// A raw request may be JSON, XML or signed content. Never silently cut it.
		return body
	}
	return limitNotificationBody(body)
}

func limitNotificationBody(body string) string {
	if len(body) <= maxNotificationBodyBytes {
		return body
	}
	const suffix = "\n\n[content truncated]"
	end := maxNotificationBodyBytes - len(suffix)
	for end > 0 && !utf8.RuneStart(body[end]) {
		end--
	}
	return body[:end] + suffix
}

func messageForNoteEvent(event *domain.ContentChangeEvent, subscriptions ...*domain.WebhookSubscription) notification.Message {
	if event == nil {
		return notification.Message{}
	}
	var subscription *domain.WebhookSubscription
	if len(subscriptions) > 0 {
		subscription = subscriptions[0]
	}
	path := event.Path
	if path == "" {
		path = event.OldPath
	}
	titleTemplate, bodyTemplate := notificationTemplates(subscription, false)
	values := map[string]string{
		"content":        event.Content,
		"vault":          event.VaultName,
		"path":           path,
		"old_path":       event.OldPath,
		"action":         string(event.Action),
		"task":           "",
		"due":            "",
		"timezone":       "",
		"changed_fields": strings.Join(event.ChangedFields, ", "),
		"size":           strconv.FormatInt(event.Size, 10),
		"client":         event.ClientType,
		"client_name":    event.ClientName,
		"client_version": event.ClientVersion,
		"ob_uri":         "",
	}
	return notification.Message{
		Title:    renderNotificationTemplate(titleTemplate, values),
		Body:     notificationBody(subscription, renderNotificationTemplate(bodyTemplate, values)),
		Endpoint: renderNotificationEndpoint(subscriptionURL(subscription), values),
	}
}

// messageForReminder renders a task reminder. task is the todo item text
// (`xxx` in `- [ ] xxx @(...)`), not the message title; content is the whole
// note body and due/timezone describe the scheduled occurrence.
func messageForReminder(subscription *domain.WebhookSubscription, task, due, timezone, vault, path, content, obsidianURI string) notification.Message {
	titleTemplate, bodyTemplate := notificationTemplates(subscription, true)
	values := map[string]string{
		"task":           task,
		"due":            due,
		"timezone":       timezone,
		"vault":          vault,
		"path":           path,
		"old_path":       "",
		"content":        content,
		"action":         "reminder",
		"changed_fields": "",
		"size":           "",
		"client":         "",
		"client_name":    "",
		"client_version": "",
		"ob_uri":         obsidianURI,
	}
	return notification.Message{
		Title:       renderNotificationTemplate(titleTemplate, values),
		Body:        notificationBody(subscription, renderNotificationTemplate(bodyTemplate, values)),
		Short:       "到期：" + due,
		Tags:        "待办",
		Group:       vault,
		ObsidianURI: obsidianURI,
		Endpoint:    renderNotificationEndpoint(subscriptionURL(subscription), values),
	}
}

func subscriptionURL(subscription *domain.WebhookSubscription) string {
	if subscription == nil {
		return ""
	}
	return subscription.URL
}

func messageForTest(subscription *domain.WebhookSubscription) notification.Message {
	if subscription == nil {
		subscription = &domain.WebhookSubscription{}
	}
	return messageForNoteEvent(&domain.ContentChangeEvent{
		VaultName: "测试笔记库",
		Action:    domain.WebhookActionModify,
		Path:      "test-note.md",
		Content:   "这是一条测试通知。",
	}, subscription)
}

func sendNotification(ctx context.Context, sender notification.Sender, subscription *domain.WebhookSubscription, message notification.Message) error {
	endpoint := subscriptionURL(subscription)
	if message.Endpoint != "" {
		endpoint = message.Endpoint
	}
	if configured, ok := sender.(notification.ConfiguredSender); ok {
		if normalizeWebhookMethod(subscription.Method) == "POST" && len(message.Body) > maxNotificationBodyBytes {
			return errors.New("rendered webhook request body exceeds 8192 bytes")
		}
		return configured.SendWithOptions(ctx, endpoint, subscription.Method, subscription.Headers, message)
	}
	return sender.Send(ctx, endpoint, subscription.Secret, message)
}
