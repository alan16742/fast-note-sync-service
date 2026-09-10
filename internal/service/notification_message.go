package service

import (
	"context"
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
	defaultReminderTitleTemplate = "{{title}}"
	defaultReminderBodyTemplate  = "{{title}}\n到期：{{due}} ({{timezone}})\n笔记：{{vault}} / {{path}}"
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
	parsed.Path = renderNotificationTemplate(parsed.Path, values)
	parsed.Fragment = renderNotificationTemplate(parsed.Fragment, values)
	query := parsed.Query()
	for key, entries := range query {
		for index, entry := range entries {
			entries[index] = renderNotificationTemplate(entry, values)
		}
		query[key] = entries
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func notificationTemplates(subscription *domain.WebhookSubscription, reminder bool) (string, string) {
	title, body := defaultWebhookTemplates(reminder)
	if subscription != nil {
		isCustom := strings.EqualFold(strings.TrimSpace(subscription.Provider), domain.WebhookProviderCustom)
		if isCustom {
			title = ""
		} else if subscription.TitleTemplate != "" {
			title = subscription.TitleTemplate
		}
		if subscription.BodyTemplate != "" {
			body = subscription.BodyTemplate
		}
	}
	return title, body
}

func limitNotificationBody(body string) string {
	if len(body) <= maxNotificationBodyBytes {
		return body
	}
	end := maxNotificationBodyBytes
	for end > 0 && !utf8.RuneStart(body[end]) {
		end--
	}
	return body[:end] + "\n\n[content truncated]"
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
		"title":          "",
		"due":            "",
		"timezone":       "",
		"changed_fields": strings.Join(event.ChangedFields, ", "),
		"size":           strconv.FormatInt(event.Size, 10),
		"client":         event.ClientType,
		"client_name":    event.ClientName,
		"client_version": event.ClientVersion,
		"url":            "",
	}
	return notification.Message{
		Title:    renderNotificationTemplate(titleTemplate, values),
		Body:     limitNotificationBody(renderNotificationTemplate(bodyTemplate, values)),
		Endpoint: renderNotificationEndpoint(subscriptionURL(subscription), values),
	}
}

func messageForReminder(subscription *domain.WebhookSubscription, title, due, timezone, vault, path, content, link string) notification.Message {
	titleTemplate, bodyTemplate := notificationTemplates(subscription, true)
	values := map[string]string{
		"title":          title,
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
		"url":            link,
	}
	return notification.Message{
		Title:    renderNotificationTemplate(titleTemplate, values),
		Body:     limitNotificationBody(renderNotificationTemplate(bodyTemplate, values)),
		Short:    "到期：" + due,
		Tags:     "待办",
		Group:    vault,
		URL:      link,
		Endpoint: renderNotificationEndpoint(subscriptionURL(subscription), values),
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
		return configured.SendWithOptions(ctx, endpoint, subscription.Method, subscription.Headers, message)
	}
	return sender.Send(ctx, endpoint, subscription.Secret, message)
}
