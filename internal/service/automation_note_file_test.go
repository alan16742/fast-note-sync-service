package service

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type noteFileExecutor struct {
	events chan domain.AutomationEvent
	err    error
}

func (e *noteFileExecutor) Execute(_ context.Context, _ domain.AutomationAction, _ *domain.AutomationExecutionContext, event *domain.AutomationEvent) error {
	e.events <- *event
	return e.err
}

func newNoteFileAutomation(t *testing.T, events []dto.AutomationEventRuleDTO, withHistory bool) (*automationService, *noteFileExecutor) {
	t.Helper()
	ctx := context.Background()
	queue := false
	cfg := config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(t.TempDir(), "db.sqlite3"), EnableWriteQueue: &queue}
	db, err := dao.NewEngine(cfg, zap.NewNop())
	require.NoError(t, err)
	d := dao.New(db, ctx, dao.WithConfig(&cfg), dao.WithUserDatabaseConfig(&cfg), dao.WithLogger(zap.NewNop()))
	repo := dao.NewAutomationRepository(d)
	rule, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "Markdown files", Enabled: true, VaultID: 7, MatchMode: "any", Events: events,
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetWebhook, ConfigID: 1}},
	}, 42)
	require.NoError(t, err)
	_, err = repo.Save(ctx, rule, 42)
	require.NoError(t, err)
	executor := &noteFileExecutor{events: make(chan domain.AutomationEvent, 10)}
	registry := NewAutomationActionExecutorRegistry()
	require.NoError(t, registry.Register(domain.AutomationTargetWebhook, executor))
	var history domain.AutomationExecutionRepository
	if withHistory {
		history = dao.NewAutomationExecutionRepository(d)
	}
	svc := NewAutomationServiceWithExecutorRegistry(repo, nil, nil, nil, nil, nil, zap.NewNop(), registry, history).(*automationService)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, svc.Shutdown(ctx))
		d.CleanupConnections(-time.Second)
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})
	return svc, executor
}

func TestNoteChangesReachFileBehaviorRules(t *testing.T) {
	for _, action := range []domain.WebhookAction{
		domain.WebhookActionCreate, domain.WebhookActionModify, domain.WebhookActionRename,
		domain.WebhookActionDelete, domain.WebhookActionRestore, domain.WebhookActionPermanentDelete,
	} {
		t.Run(string(action), func(t *testing.T) {
			svc, executor := newNoteFileAutomation(t, []dto.AutomationEventRuleDTO{{
				Type: domain.AutomationEventFileBehavior, PathGlob: "*.md",
				EventActions: []string{"create", "modify", "rename", "delete", "restore", "permanent_delete"},
			}}, true)
			publishNoteChangeEvent(context.Background(), svc, 42, 7, "vault", action,
				&domain.Note{Path: "a.md", Content: "note content", ClientName: "Obsidian"}, "", "content")
			svc.eventWg.Wait()
			require.Len(t, executor.events, 1, "a persisted note change must reach a file-only rule")
			event := <-executor.events
			require.Equal(t, domain.AutomationEventFileBehavior, event.Type)
			require.Equal(t, string(action), event.Action)
			require.Equal(t, "a.md", event.Path)
			require.Equal(t, "note content", event.Content)
			require.Equal(t, "Obsidian", event.ClientName)
			rows, total, err := svc.ListExecutions(context.Background(), 42, 0, 1, 20)
			require.NoError(t, err)
			require.EqualValues(t, 1, total)
			require.Equal(t, domain.AutomationEventFileBehavior, rows[0].EventType)
			require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Status)
		})
	}
}

func TestNoteFileBehaviorKeepsPathAndActionFilters(t *testing.T) {
	for _, tc := range []struct {
		name, path, oldPath, prefix, glob string
		action                            domain.WebhookAction
		vaultID                           int64
		want                              int
	}{
		{name: "root markdown", path: "a.md", glob: "*.md", action: domain.WebhookActionModify, vaultID: 7, want: 1},
		{name: "nested markdown", path: "notes/a.md", glob: "*.md", action: domain.WebhookActionModify, vaultID: 7},
		{name: "direct child", path: "notes/a.md", glob: "notes/*.md", action: domain.WebhookActionModify, vaultID: 7, want: 1},
		{name: "prefix must also match", path: "a.md", prefix: "notes/", glob: "*.md", action: domain.WebhookActionModify, vaultID: 7},
		{name: "unselected action", path: "a.md", glob: "*.md", action: domain.WebhookActionDelete, vaultID: 7},
		{name: "another vault", path: "a.md", glob: "*.md", action: domain.WebhookActionModify, vaultID: 8},
		{name: "move out of root", path: "notes/a.md", oldPath: "a.md", glob: "*.md", action: domain.WebhookActionRename, vaultID: 7, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, executor := newNoteFileAutomation(t, []dto.AutomationEventRuleDTO{{
				Type: domain.AutomationEventFileBehavior, PathPrefix: tc.prefix, PathGlob: tc.glob, EventActions: []string{"modify", "rename"},
			}}, false)
			publishNoteChangeEvent(context.Background(), svc, 42, tc.vaultID, "vault", tc.action,
				&domain.Note{Path: tc.path, Content: "body"}, tc.oldPath, "mtime")
			svc.eventWg.Wait()
			require.Len(t, executor.events, tc.want)
		})
	}
}

func TestNoteContentAndFileORBranchesExecuteOnce(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		fail, history bool
	}{
		{name: "both branches", content: "release"},
		{name: "both branches with history", content: "release", history: true},
		{name: "failed action is not an implicit retry", content: "release", fail: true},
		{name: "file branch when content does not match", content: "draft"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, executor := newNoteFileAutomation(t, []dto.AutomationEventRuleDTO{
				{Type: domain.AutomationEventNoteContent, ContentContains: "release"},
				{Type: domain.AutomationEventFileBehavior, PathGlob: "*.md", EventActions: []string{"modify"}},
			}, tc.history)
			if tc.fail {
				executor.err = errors.New("delivery failed")
			}
			publishNoteChangeEvent(context.Background(), svc, 42, 7, "vault", domain.WebhookActionModify,
				&domain.Note{Path: "a.md", Content: tc.content}, "", "content")
			svc.eventWg.Wait()
			require.Len(t, executor.events, 1, "one mutation must execute an OR rule at most once")
		})
	}
}

func TestNoteChangeCanExecuteSeparateContentAndFileRules(t *testing.T) {
	svc, executor := newNoteFileAutomation(t, []dto.AutomationEventRuleDTO{{
		Type: domain.AutomationEventFileBehavior, PathGlob: "*.md", EventActions: []string{"modify"},
	}}, true)
	_, err := svc.repo.Save(context.Background(), &domain.AutomationTrigger{
		UID: 42, Name: "Content rule", Enabled: true, VaultID: 7,
		Events:  []domain.AutomationEventRule{{Type: domain.AutomationEventNoteContent, ContentContains: "release"}},
		Actions: []domain.AutomationAction{{Type: domain.AutomationTargetWebhook, ConfigID: 1}},
	}, 42)
	require.NoError(t, err)
	event := &domain.ContentChangeEvent{
		ID: "same-note-change", UID: 42, VaultID: 7, VaultName: "vault", Resource: domain.WebhookResourceNote,
		Path: "a.md", Action: domain.WebhookActionModify, Content: "release", ChangedFields: []string{"content"},
	}
	svc.PublishNoteChange(context.Background(), event)
	svc.eventWg.Wait()
	require.Len(t, executor.events, 2, "distinct rules should each execute")
	svc.PublishNoteChange(context.Background(), event)
	svc.eventWg.Wait()
	require.Len(t, executor.events, 2, "redelivery must preserve the original idempotency key")
	first, second := <-executor.events, <-executor.events
	require.Equal(t, first.ID, second.ID)
	require.NotEqual(t, first.Type, second.Type)
	_, total, err := svc.ListExecutions(context.Background(), 42, 0, 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
}

func TestAttachmentEventsDoNotTriggerNoteContentRules(t *testing.T) {
	svc, executor := newNoteFileAutomation(t, []dto.AutomationEventRuleDTO{{Type: domain.AutomationEventNoteContent}}, false)
	svc.Publish(context.Background(), &domain.AutomationEvent{ID: "attachment", UID: 42, VaultID: 7, Type: domain.AutomationEventFileBehavior, Action: "modify", Path: "a.png"})
	svc.eventWg.Wait()
	require.Empty(t, executor.events)
}
