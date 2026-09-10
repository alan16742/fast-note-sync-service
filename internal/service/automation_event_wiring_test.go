package service

import (
	"context"
	"errors"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	domainmocks "github.com/haierkeys/fast-note-sync-service/internal/domain/mocks"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type capturedAutomationEvents struct {
	notes []*domain.ContentChangeEvent
	files []*domain.AutomationEvent
}

func (c *capturedAutomationEvents) PublishNoteChange(_ context.Context, event *domain.ContentChangeEvent) {
	c.notes = append(c.notes, event)
}

func (c *capturedAutomationEvents) Publish(_ context.Context, event *domain.AutomationEvent) {
	c.files = append(c.files, event)
}

func TestConflictCopyPublishesOnlyAfterPersistence(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "write failure"}[fail], func(t *testing.T) {
			capture := &capturedAutomationEvents{}
			logs := &syncLogService{logger: zap.NewNop(), ch: make(chan syncLogQueueItem, 1), automationPublisher: capture}
			repo := new(domainmocks.MockNoteRepository)
			stored := &domain.Note{}
			var writeErr error
			if fail {
				writeErr = errors.New("write failed")
			}
			repo.On("Create", mock.Anything, mock.Anything, int64(42)).Run(func(args mock.Arguments) {
				require.Empty(t, capture.notes)
				require.Empty(t, logs.ch)
				*stored = *args.Get(1).(*domain.Note)
				stored.ID = 99
			}).Return(stored, writeErr).Once()
			svc := NewConflictService(repo, &fakeVaultServiceForConflictTest{vaultID: 7}, logs, capture, zap.NewNop())
			response, err := svc.CreateConflictFile(context.Background(), 42, &dto.ConflictFileRequest{
				Vault: "MyVault", OriginalPath: "Notes/a.md", ClientContent: "conflicted content", ClientContentHash: "hash",
			})
			repo.AssertExpectations(t)
			if fail {
				require.Error(t, err)
				require.Empty(t, capture.notes)
				require.Empty(t, logs.ch)
				return
			}
			require.NoError(t, err)
			require.Len(t, capture.notes, 1)
			require.Empty(t, capture.files, "the note audit log must not publish a duplicate file event")
			event := capture.notes[0]
			require.Equal(t, response.ConflictPath, event.Path)
			require.Equal(t, "conflicted content", event.Content)
			require.Equal(t, "MyVault", event.VaultName)
			require.Equal(t, "server", event.ClientType)
			require.Equal(t, domain.WebhookActionCreate, event.Action)
			require.EqualValues(t, 42, event.UID)
			require.EqualValues(t, 7, event.VaultID)
			require.Len(t, logs.ch, 1)
			entry := (<-logs.ch).entry
			require.Equal(t, response.ConflictPath, entry.Path)
			require.Equal(t, domain.SyncLogTypeNote, entry.Type)
			require.Equal(t, domain.SyncLogActionCreate, entry.Action)
		})
	}
}

func TestSyncLogRenamePublishesAndQueuesOldPath(t *testing.T) {
	capture := &capturedAutomationEvents{}
	svc := &syncLogService{logger: zap.NewNop(), ch: make(chan syncLogQueueItem, 1), automationPublisher: capture}
	svc.Log(42, 7, domain.SyncLogTypeFile, domain.SyncLogActionRename, "path",
		"Archive/a.png", "new-hash", "web", "browser", "1", 10, WithOldPath("Projects/a.png"))
	require.Len(t, capture.files, 1)
	trigger := &domain.AutomationTrigger{Enabled: true, VaultID: 7, Events: []domain.AutomationEventRule{
		{Type: domain.AutomationEventFileBehavior, PathPrefix: "Projects/", EventActions: []string{"rename"}},
	}}
	require.True(t, automationTriggerMatches(trigger, capture.files[0]))
	require.Len(t, svc.ch, 1)
	require.Equal(t, "Projects/a.png", (<-svc.ch).entry.OldPath)
}
