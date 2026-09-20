package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	domainmocks "github.com/haierkeys/fast-note-sync-service/internal/domain/mocks"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAutomationCronANDMatchesOneOccurrence(t *testing.T) {
	start := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		schedules []string
		want      time.Time
	}{
		{"disjoint past occurrences", []string{"1 * * * *", "2 * * * *"}, time.Time{}},
		{"intersecting schedules", []string{"*/2 * * * *", "*/3 * * * *"}, start.Add(6 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			trigger := &domain.AutomationTrigger{MatchMode: domain.AutomationMatchAll}
			for _, schedule := range tc.schedules {
				trigger.Events = append(trigger.Events, domain.AutomationEventRule{Type: domain.AutomationEventCron, Schedule: schedule})
			}
			next, due := nextAutomationCronOccurrence(trigger, start, start.Add(10*time.Minute))
			require.Equal(t, !tc.want.IsZero(), due)
			require.Equal(t, tc.want, next)
		})
	}
}

func TestAutomationCancellationWaitsForStartedAction(t *testing.T) {
	svc := &automationService{pool: newAutomationExecutionPool(t)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, stopped := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- svc.runAutomationAction(ctx, func(ctx context.Context) error {
			close(started)
			<-stopped
			return ctx.Err()
		})
	}()
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("execution became retryable while its target was still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(stopped)
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestAutomationExecutorPanicReturnsAnError(t *testing.T) {
	svc := &automationService{pool: newAutomationExecutionPool(t)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := svc.runAutomationAction(ctx, func(context.Context) error { panic("broken executor") })
	require.ErrorContains(t, err, "broken executor")
}

type failingExecutionStart struct {
	domain.AutomationExecutionRepository
}

func (failingExecutionStart) Start(context.Context, *domain.AutomationExecution) (bool, *domain.AutomationExecution, error) {
	return false, nil, errors.New("database unavailable")
}

func TestAutomationDoesNotBypassFailedIdempotencyClaim(t *testing.T) {
	backup := &automationBackupStub{}
	svc := NewAutomationService(nil, nil, backup, nil, nil, nil, zap.NewNop(), failingExecutionStart{}).(*automationService)
	trigger := &domain.AutomationTrigger{ID: 1, UID: 42, VaultID: 7, Actions: []domain.AutomationAction{{Type: domain.AutomationTargetBackup, ConfigID: 1}}}
	err := svc.executeTrigger(context.Background(), trigger, &domain.AutomationEvent{ID: "event", UID: 42, VaultID: 7})
	require.ErrorContains(t, err, "database unavailable")
	require.Zero(t, backup.calls.Load())
}

func TestBackupBaselineRequiresSuccessForEveryDestination(t *testing.T) {
	repo := new(domainmocks.MockBackupRepository)
	old := time.Now().Add(-time.Hour)
	newer := old.Add(30 * time.Minute)
	repo.On("ListHistory", mock.Anything, int64(1), int64(2), 1, 1000).Return([]*domain.BackupHistory{
		{TriggerID: 3, VaultID: 4, StorageID: 5, Status: domain.BackupStatusSuccess, StartTime: newer},
		{TriggerID: 3, VaultID: 4, StorageID: 6, Status: domain.BackupStatusFailed, StartTime: newer},
		{TriggerID: 3, VaultID: 4, StorageID: 6, Status: domain.BackupStatusSuccess, StartTime: old},
		{TriggerID: 99, VaultID: 4, StorageID: 6, Status: domain.BackupStatusSuccess, StartTime: newer},
	}, int64(4), nil)
	svc := &backupService{backupRepo: repo}
	config := &domain.BackupConfig{ID: 2, UID: 1, StorageIds: "[5,6]"}
	execution := &domain.AutomationExecutionContext{UID: 1, TriggerID: 3, VaultID: 4}
	require.Equal(t, old, svc.previousBackupRun(context.Background(), config, execution))
	config.StorageIds = "[5,6,7]"
	require.True(t, svc.previousBackupRun(context.Background(), config, execution).IsZero())
	execution.VaultID = 8
	require.True(t, svc.previousBackupRun(context.Background(), config, execution).IsZero())
}

func TestBackupConfigPreservesRandomPasswordMode(t *testing.T) {
	repo := new(domainmocks.MockBackupRepository)
	repo.On("SaveConfig", mock.Anything, mock.MatchedBy(func(config *domain.BackupConfig) bool {
		return config.PasswordMode == 2 && config.PasswordValue == ""
	}), int64(1)).Return(&domain.BackupConfig{ID: 1, UID: 1, PasswordMode: 2}, nil)
	svc := newBackupSvc(repo, nil, &backupStorageStub{storages: map[int64]*dto.StorageDTO{2: {ID: 2}}})
	_, err := svc.UpdateConfig(context.Background(), 1, &dto.BackupConfigRequest{Type: "full", StorageIds: "[2]", PasswordMode: 2})
	require.NoError(t, err)
	repo.AssertExpectations(t)
}

func TestBackupRejectsUnavailableStorageTargets(t *testing.T) {
	svc := &backupService{storageService: &backupStorageStub{storages: map[int64]*dto.StorageDTO{1: {ID: 1}}}}
	_, err := svc.UpdateConfig(context.Background(), 1, &dto.BackupConfigRequest{StorageIds: "[]"})
	require.Error(t, err)
	_, err = svc.UpdateConfig(context.Background(), 1, &dto.BackupConfigRequest{StorageIds: "[2]"})
	require.Error(t, err)
	_, err = svc.loadBackupStorageTargets(context.Background(), &domain.BackupConfig{UID: 1, Type: "full", StorageIds: "[1]"})
	require.ErrorContains(t, err, "at least one enabled")
}

type cancellationExecutor struct{ started chan struct{} }

func (e cancellationExecutor) Execute(ctx context.Context, _ domain.AutomationAction, _ *domain.AutomationExecutionContext, _ *domain.AutomationEvent) error {
	close(e.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestAutomationShutdownWaitsForManualRun(t *testing.T) {
	trigger := &domain.AutomationTrigger{ID: 1, UID: 1, VaultID: 1, Enabled: true,
		Events:  []domain.AutomationEventRule{{Type: domain.AutomationEventManual}},
		Actions: []domain.AutomationAction{{Type: domain.AutomationTargetBackup, ConfigID: 1}},
	}
	registry := NewAutomationActionExecutorRegistry()
	started := make(chan struct{})
	require.NoError(t, registry.Register(domain.AutomationTargetBackup, cancellationExecutor{started: started}))
	svc := NewAutomationServiceWithExecutorRegistry(&automationRetryTriggerRepository{trigger: trigger}, nil, nil, nil, nil, nil, zap.NewNop(), registry)
	done := make(chan error, 1)
	go func() { done <- svc.Trigger(context.Background(), 1, &dto.AutomationRunRequest{ID: 1}) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("manual run did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))
	require.ErrorIs(t, <-done, context.Canceled)
	require.ErrorIs(t, svc.Trigger(context.Background(), 1, &dto.AutomationRunRequest{ID: 1}), context.Canceled)
}

type pausedGitRepository struct {
	domain.GitSyncRepository
	started chan context.Context
	release chan struct{}
}

func (r *pausedGitRepository) GetByID(_ context.Context, id, uid int64) (*domain.GitSyncConfig, error) {
	return &domain.GitSyncConfig{ID: id, UID: uid, RepoURL: "not a valid git URL", Branch: "main"}, nil
}
func (r *pausedGitRepository) ListHistory(context.Context, int64, int64, int, int) ([]*domain.GitSyncHistory, int64, error) {
	return nil, 0, nil
}
func (r *pausedGitRepository) Save(ctx context.Context, conf *domain.GitSyncConfig, _ int64) (*domain.GitSyncConfig, error) {
	if conf.LastStatus == domain.GitSyncStatusRunning {
		r.started <- ctx
		<-r.release
	}
	return conf, nil
}
func (r *pausedGitRepository) CreateHistory(_ context.Context, history *domain.GitSyncHistory, _ int64) (*domain.GitSyncHistory, error) {
	return history, nil
}

func TestGitConcurrentUsersDoNotCancelEachOther(t *testing.T) {
	t.Chdir(t.TempDir())
	repo := &pausedGitRepository{started: make(chan context.Context, 2), release: make(chan struct{})}
	svc := NewGitSyncService(repo, nil, nil, nil, nil, nil, nil, zap.NewNop()).(*gitSyncService)
	done := make(chan error, 2)
	for _, uid := range []int64{1, 2} {
		go func() {
			done <- svc.ExecuteSync(context.Background(), uid, 1, &domain.AutomationExecutionContext{UID: uid, TriggerID: 1, VaultID: 1})
		}()
	}
	contexts := make([]context.Context, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case ctx := <-repo.started:
			contexts = append(contexts, ctx)
		case <-time.After(time.Second):
			close(repo.release)
			t.Fatal("independent user did not start")
		}
	}
	for _, ctx := range contexts {
		if ctx.Err() != nil {
			t.Error("another user with the same IDs cancelled this task")
		}
	}
	close(repo.release)
	for i := 0; i < 2; i++ {
		require.Error(t, <-done)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))
}

type pausedBackupRepository struct {
	domain.BackupRepository
	started chan context.Context
	release chan struct{}
}

func (r *pausedBackupRepository) GetByID(_ context.Context, id, uid int64) (*domain.BackupConfig, error) {
	return &domain.BackupConfig{ID: id, UID: uid, Type: "full", StorageIds: "[1]"}, nil
}
func (r *pausedBackupRepository) ListHistory(context.Context, int64, int64, int, int) ([]*domain.BackupHistory, int64, error) {
	return nil, 0, nil
}
func (r *pausedBackupRepository) SaveConfig(ctx context.Context, config *domain.BackupConfig, _ int64) (*domain.BackupConfig, error) {
	if config.LastStatus == domain.BackupStatusRunning {
		r.started <- ctx
		<-r.release
	}
	return config, nil
}

type unavailableBackupVaults struct{ domain.VaultRepository }

func (unavailableBackupVaults) GetByID(context.Context, int64, int64) (*domain.Vault, error) {
	return nil, errors.New("vault unavailable")
}

func TestBackupConcurrentUsersDoNotSkipEachOther(t *testing.T) {
	repo := &pausedBackupRepository{started: make(chan context.Context, 2), release: make(chan struct{})}
	svc := NewBackupService(repo, nil, nil, nil, unavailableBackupVaults{}, &backupStorageStub{storages: map[int64]*dto.StorageDTO{1: {ID: 1, IsEnabled: true}}}, nil, t.TempDir(), zap.NewNop())
	done := make(chan error, 2)
	for _, uid := range []int64{1, 2} {
		go func() {
			done <- svc.ExecuteUserBackup(context.Background(), uid, 1, &domain.AutomationExecutionContext{UID: uid, TriggerID: 1, VaultID: 1})
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case ctx := <-repo.started:
			if ctx.Err() != nil {
				t.Error("another user's backup was cancelled")
			}
		case <-time.After(time.Second):
			close(repo.release)
			t.Fatal("another user's backup was incorrectly skipped")
		}
	}
	close(repo.release)
	for i := 0; i < 2; i++ {
		require.ErrorContains(t, <-done, "vault unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, svc.Shutdown(ctx))
}
