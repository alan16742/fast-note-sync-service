package service

import (
	"net/url"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestMessageForNoteEvent(t *testing.T) {
	message := messageForNoteEvent(&domain.ContentChangeEvent{VaultName: "ceshi", Action: domain.WebhookActionModify, Path: "test-note.md", Content: "- 1"})
	assert.Equal(t, "Fast Note Sync: modify test-note.md", message.Title)
	assert.Contains(t, message.Body, "- Vault: ceshi")
	assert.Contains(t, message.Body, "- 1")
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
