package service

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/stretchr/testify/require"
)

type automationExecutorTestStub struct {
	calls atomic.Int32
}

func (s *automationExecutorTestStub) Execute(context.Context, domain.AutomationAction, *domain.AutomationExecutionContext, *domain.AutomationEvent) error {
	s.calls.Add(1)
	return nil
}

func TestAutomationActionExecutorRegistryRegisterAndGet(t *testing.T) {
	registry := NewAutomationActionExecutorRegistry()
	first := &automationExecutorTestStub{}
	second := &automationExecutorTestStub{}

	require.NoError(t, registry.Register(" Git ", first))
	executor, ok := registry.Get("git")
	require.True(t, ok)
	require.Same(t, first, executor)

	// Registration is replaceable so applications can override a default
	// executor during initialization.
	require.NoError(t, registry.Register("git", second))
	executor, ok = registry.Get(" GIT ")
	require.True(t, ok)
	require.Same(t, second, executor)

	_, ok = registry.Get("unknown")
	require.False(t, ok)
}

func TestAutomationServiceReturnsErrorForUnregisteredActionType(t *testing.T) {
	svc := &automationService{executors: NewAutomationActionExecutorRegistry()}

	err := svc.executeAction(context.Background(), 7, &domain.AutomationEvent{
		ID: "event-unknown", UID: 42, VaultID: 9,
	}, domain.AutomationAction{Type: "queue", ConfigID: 1})

	require.ErrorIs(t, err, ErrAutomationActionExecutorNotRegistered)
	require.ErrorContains(t, err, `"queue"`)
}

type automationGitExecutorTestStub struct {
	GitSyncService
	calls     atomic.Int32
	uid       int64
	configID  int64
	execution *domain.AutomationExecutionContext
}

func (s *automationGitExecutorTestStub) ExecuteSync(_ context.Context, uid, configID int64, execution *domain.AutomationExecutionContext) error {
	s.calls.Add(1)
	s.uid = uid
	s.configID = configID
	s.execution = execution
	return nil
}

type automationBackupExecutorTestStub struct {
	BackupService
	calls     atomic.Int32
	uid       int64
	configID  int64
	execution *domain.AutomationExecutionContext
}

func (s *automationBackupExecutorTestStub) ExecuteUserBackup(_ context.Context, uid, configID int64, execution *domain.AutomationExecutionContext) error {
	s.calls.Add(1)
	s.uid = uid
	s.configID = configID
	s.execution = execution
	return nil
}

type automationWebhookExecutorTestStub struct {
	WebhookService
	calls    atomic.Int32
	uid      int64
	configID int64
	event    *domain.ContentChangeEvent
}

func (s *automationWebhookExecutorTestStub) DeliverEvent(_ context.Context, uid, configID int64, event *domain.ContentChangeEvent) error {
	s.calls.Add(1)
	s.uid = uid
	s.configID = configID
	s.event = event
	return nil
}

func TestAutomationServiceDispatchesActionsThroughRegisteredExecutors(t *testing.T) {
	git := &automationGitExecutorTestStub{}
	backup := &automationBackupExecutorTestStub{}
	webhook := &automationWebhookExecutorTestStub{}
	registry := NewAutomationActionExecutorRegistry()
	require.NoError(t, registry.Register(domain.AutomationTargetGit, NewGitExecutor(git)))
	require.NoError(t, registry.Register(domain.AutomationTargetBackup, NewBackupExecutor(backup)))
	require.NoError(t, registry.Register(domain.AutomationTargetWebhook, NewWebhookExecutor(webhook)))

	svc := &automationService{executors: registry}
	event := &domain.AutomationEvent{
		ID: "event-1", UID: 42, VaultID: 9, VaultName: "vault", Action: "modify", Path: "note.md",
	}
	for _, action := range []domain.AutomationAction{
		{Type: domain.AutomationTargetGit, ConfigID: 11},
		{Type: domain.AutomationTargetBackup, ConfigID: 12},
		{Type: domain.AutomationTargetWebhook, ConfigID: 13},
	} {
		require.NoError(t, svc.executeAction(context.Background(), 7, event, action))
	}

	require.EqualValues(t, 1, git.calls.Load())
	require.Equal(t, int64(42), git.uid)
	require.Equal(t, int64(11), git.configID)
	require.Equal(t, int64(7), git.execution.TriggerID)
	require.Equal(t, "event-1", git.execution.EventID)

	require.EqualValues(t, 1, backup.calls.Load())
	require.Equal(t, int64(42), backup.uid)
	require.Equal(t, int64(12), backup.configID)
	require.Equal(t, int64(9), backup.execution.VaultID)

	require.EqualValues(t, 1, webhook.calls.Load())
	require.Equal(t, int64(42), webhook.uid)
	require.Equal(t, int64(13), webhook.configID)
	require.Equal(t, "event-1", webhook.event.ID)
	require.Equal(t, "note.md", webhook.event.Path)
}
