package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
)

// NoteEventPublisher accepts note changes after they have been persisted.
// The central implementation fans the event out to notification channels and
// automation triggers. Implementations must not make the original note
// operation fail.
type NoteEventPublisher interface {
	PublishNoteChange(ctx context.Context, event *domain.ContentChangeEvent)
}

func (s *noteService) publishNoteChange(ctx context.Context, uid, vaultID int64, action domain.WebhookAction, note *domain.Note, oldPath string, changedFields ...string) {
	s.publishNoteChangeWithVault(ctx, uid, vaultID, "", action, note, oldPath, changedFields...)
}

func (s *noteService) publishNoteChangeWithVault(ctx context.Context, uid, vaultID int64, vaultName string, action domain.WebhookAction, note *domain.Note, oldPath string, changedFields ...string) {
	if s.eventPublisher == nil || note == nil {
		return
	}

	s.eventPublisher.PublishNoteChange(ctx, &domain.ContentChangeEvent{
		ID:            uuid.NewString(),
		OccurredAt:    time.Now().UTC(),
		UID:           uid,
		VaultID:       vaultID,
		VaultName:     vaultName,
		Resource:      domain.WebhookResourceNote,
		Action:        action,
		Path:          note.Path,
		OldPath:       oldPath,
		PathHash:      note.PathHash,
		ChangedFields: append([]string(nil), changedFields...),
		Content:       note.Content,
		ContentHash:   note.ContentHash,
		Size:          note.Size,
		ClientType:    note.ClientType,
		ClientName:    note.ClientName,
		ClientVersion: note.ClientVersion,
		Source:        "note_service",
	})
}
