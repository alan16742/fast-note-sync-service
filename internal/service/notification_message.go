package service

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"time"
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
func defaultWebhookTemplates(mode string) (string, string) {
	if normalizeNotificationMode(mode) == domain.NotificationModeReminder {
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

func notificationTemplates(subscription *domain.WebhookSubscription) (string, string) {
	mode := domain.NotificationModeNoteChange
	if subscription != nil {
		mode = subscription.Mode
	}
	title, body := defaultWebhookTemplates(mode)
	if subscription != nil {
		if subscription.TitleTemplate != "" {
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
	titleTemplate, bodyTemplate := notificationTemplates(subscription)
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
		Title: renderNotificationTemplate(titleTemplate, values),
		Body:  limitNotificationBody(renderNotificationTemplate(bodyTemplate, values)),
	}
}

func messageForReminder(subscription *domain.WebhookSubscription, title, due, timezone, vault, path, content, link string) notification.Message {
	titleTemplate, bodyTemplate := notificationTemplates(subscription)
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
		Title: renderNotificationTemplate(titleTemplate, values),
		Body:  limitNotificationBody(renderNotificationTemplate(bodyTemplate, values)),
		Short: "到期：" + due,
		Tags:  "待办",
		Group: vault,
		URL:   link,
	}
}

func messageForTest(subscription *domain.WebhookSubscription) notification.Message {
	if subscription == nil {
		subscription = &domain.WebhookSubscription{}
	}
	if normalizeNotificationMode(subscription.Mode) == domain.NotificationModeReminder {
		timezone := subscription.Timezone
		if timezone == "" {
			timezone = "Asia/Shanghai"
		}
		location, err := time.LoadLocation(timezone)
		if err != nil {
			location = time.FixedZone("test", 0)
		}
		due := time.Date(2026, 1, 1, 9, 0, 0, 0, location).Format("2006-01-02 15:04")
		return messageForReminder(subscription, "测试待办", due, timezone, "测试笔记库", "test-note.md", "这是一条测试提醒。", "")
	}
	return messageForNoteEvent(&domain.ContentChangeEvent{
		VaultName: "测试笔记库",
		Action:    domain.WebhookActionModify,
		Path:      "test-note.md",
		Content:   "这是一条测试通知。",
	}, subscription)
}

func sendNotification(ctx context.Context, sender notification.Sender, subscription *domain.WebhookSubscription, message notification.Message) error {
	if configured, ok := sender.(notification.ConfiguredSender); ok {
		return configured.SendWithOptions(ctx, subscription.URL, subscription.Method, subscription.Headers, message)
	}
	return sender.Send(ctx, subscription.URL, subscription.Secret, message)
}
