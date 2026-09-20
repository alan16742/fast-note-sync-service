package service

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/stretchr/testify/assert"
)

type configuredNotificationRecorder struct {
	notification.Sender
	called bool
}

func (s *configuredNotificationRecorder) SendWithOptions(context.Context, string, string, map[string]string, notification.Message) error {
	s.called = true
	return nil
}

func TestOversizedCustomWebhookIsRejectedBeforeSending(t *testing.T) {
	sender := &configuredNotificationRecorder{}
	err := sendNotification(context.Background(), sender, &domain.WebhookSubscription{Provider: "custom", Method: "POST"}, notification.Message{Body: strings.Repeat("x", maxNotificationBodyBytes+1)})
	assert.ErrorContains(t, err, "exceeds 8192")
	assert.False(t, sender.called)
}

func TestMessageForNoteEvent(t *testing.T) {
	message := messageForNoteEvent(&domain.ContentChangeEvent{VaultName: "ceshi", Action: domain.WebhookActionModify, Path: "test-note.md", Content: "- 1"})
	assert.Equal(t, "Fast Note Sync: modify test-note.md", message.Title)
	assert.Contains(t, message.Body, "- Vault: ceshi")
	assert.Contains(t, message.Body, "- 1")
}

func TestCustomWebhookPreservesLiteralPayloadAndSignedQuery(t *testing.T) {
	endpoint := "https://example.com/hook?z=a%20b&flag&x=%2f&x=second&task={{content}}"
	message := messageForNoteEvent(&domain.ContentChangeEvent{Content: "a & b"}, &domain.WebhookSubscription{
		Provider: domain.WebhookProviderCustom, URL: endpoint,
	})
	assert.Empty(t, message.Body)
	assert.Equal(t, "https://example.com/hook?z=a%20b&flag&x=%2f&x=second&task=a+%26+b", message.Endpoint)
	large := strings.Repeat("中", 4000)
	message = messageForNoteEvent(&domain.ContentChangeEvent{Content: large}, &domain.WebhookSubscription{
		Provider: domain.WebhookProviderCustom, BodyTemplate: `{"text":"{{content}}"}`,
	})
	assert.Equal(t, `{"text":"`+large+`"}`, message.Body, "raw requests must never be silently truncated")
	body := limitNotificationBody(large)
	assert.LessOrEqual(t, len(body), maxNotificationBodyBytes)
	assert.True(t, utf8.ValidString(body))
}

func TestMessageForNoteEventRendersCustomTemplates(t *testing.T) {
	message := messageForNoteEvent(&domain.ContentChangeEvent{
		VaultName: "vault",
		Action:    domain.WebhookActionRename,
		Path:      "new.md",
		OldPath:   "old.md",
		Content:   "content",
	}, &domain.WebhookSubscription{
		TitleTemplate: "{{action}}: {{path}}",
		BodyTemplate:  "{{vault}}|{{old_path}}|{{content}}|{{task}}",
	})
	assert.Equal(t, "rename: new.md", message.Title)
	assert.Equal(t, "vault|old.md|content|", message.Body)
}

func TestMessageForCustomWebhookDoesNotUseTitleTemplate(t *testing.T) {
	message := messageForNoteEvent(&domain.ContentChangeEvent{Action: domain.WebhookActionModify, Path: "note.md", Content: "content"}, &domain.WebhookSubscription{
		Provider:      domain.WebhookProviderCustom,
		TitleTemplate: "should be ignored",
		BodyTemplate:  "{{content}}",
	})
	assert.Empty(t, message.Title)
	assert.Equal(t, "content", message.Body)
}

func TestMessageForNoteEventRendersEndpointTemplate(t *testing.T) {
	message := messageForNoteEvent(&domain.ContentChangeEvent{Content: "a & b", Path: "note.md"}, &domain.WebhookSubscription{
		URL: "https://example.com/hook?title={{content}}&path={{path}}",
	})
	parsed, err := url.Parse(message.Endpoint)
	if err != nil {
		t.Fatalf("invalid endpoint: %v", err)
	}
	if parsed.Query().Get("title") != "a & b" || parsed.Query().Get("path") != "note.md" {
		t.Fatalf("unexpected endpoint query: %s", parsed.RawQuery)
	}
}

func TestMessageForReminderRendersCustomTemplates(t *testing.T) {
	message := messageForReminder(&domain.WebhookSubscription{
		TitleTemplate: "{{task}} @ {{due}}",
		BodyTemplate:  "{{vault}}/{{path}} {{ob_uri}} {{content}}",
	}, "todo", "2026-01-01 09:00", "Asia/Shanghai", "vault", "todo.md", "note", "obsidian://open")
	assert.Equal(t, "todo @ 2026-01-01 09:00", message.Title)
	assert.Equal(t, "vault/todo.md obsidian://open note", message.Body)
}
