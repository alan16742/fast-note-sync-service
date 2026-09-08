package service

import (
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
		BodyTemplate:  "{{vault}}|{{old_path}}|{{content}}|{{title}}",
	})
	assert.Equal(t, "rename: new.md", message.Title)
	assert.Equal(t, "vault|old.md|content|", message.Body)
}

func TestMessageForReminderRendersCustomTemplates(t *testing.T) {
	message := messageForReminder(&domain.WebhookSubscription{
		Mode:          domain.NotificationModeReminder,
		TitleTemplate: "{{title}} @ {{due}}",
		BodyTemplate:  "{{vault}}/{{path}} {{url}} {{content}}",
	}, "todo", "2026-01-01 09:00", "Asia/Shanghai", "vault", "todo.md", "note", "obsidian://open")
	assert.Equal(t, "todo @ 2026-01-01 09:00", message.Title)
	assert.Equal(t, "vault/todo.md obsidian://open note", message.Body)
}
