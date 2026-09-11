package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	domainmocks "github.com/haierkeys/fast-note-sync-service/internal/domain/mocks"
	"github.com/haierkeys/fast-note-sync-service/pkg/workerpool"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type automationBackupStub struct {
	BackupService
	err   error
	calls atomic.Int32
}

func (s *automationBackupStub) ExecuteUserBackup(context.Context, int64, int64, *domain.AutomationExecutionContext) error {
	s.calls.Add(1)
	return s.err
}

type automationCronRepository struct {
	domain.AutomationRepository
	trigger   *domain.AutomationTrigger
	marked    atomic.Int32
	attempted atomic.Int32
}

func (r *automationCronRepository) ListEnabledByType(context.Context, domain.AutomationEventType) ([]*domain.AutomationTrigger, error) {
	return []*domain.AutomationTrigger{r.trigger}, nil
}

func (r *automationCronRepository) MarkRun(_ context.Context, _ int64, _ int64, at time.Time) error {
	r.trigger.LastRunAt = at
	r.trigger.LastAttemptAt = at
	r.marked.Add(1)
	return nil
}

func (r *automationCronRepository) MarkAttempt(_ context.Context, _ int64, _ int64, at time.Time) error {
	r.trigger.LastAttemptAt = at
	r.attempted.Add(1)
	return nil
}

func newAutomationExecutionPool(t *testing.T) *workerpool.Pool {
	t.Helper()
	pool := workerpool.New(&workerpool.Config{MaxWorkers: 1, QueueSize: 2}, zap.NewNop())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = pool.Shutdown(ctx)
	})
	return pool
}

func TestDispatchTriggerWaitsForActionResult(t *testing.T) {
	wantErr := errors.New("backup unavailable")
	backup := &automationBackupStub{err: wantErr}
	svc := &automationService{
		backupService: backup,
		pool:          newAutomationExecutionPool(t),
		logger:        zap.NewNop(),
	}

	err := svc.dispatchTrigger(context.Background(), &domain.AutomationTrigger{
		ID: 1, Enabled: true, VaultID: 7,
		Actions: []domain.AutomationAction{{Type: domain.AutomationTargetBackup, ConfigID: 2}},
	}, &domain.AutomationEvent{ID: "event-1", UID: 42, VaultID: 7})

	require.ErrorIs(t, err, wantErr)
	require.EqualValues(t, 1, backup.calls.Load())
}

func TestPollTimeTriggersMarksOnlySuccessfulDispatch(t *testing.T) {
	for _, test := range []struct {
		name      string
		actionErr error
		wantMark  int32
	}{
		{name: "failure", actionErr: errors.New("backup failed"), wantMark: 0},
		{name: "success", wantMark: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &automationCronRepository{trigger: &domain.AutomationTrigger{
				ID: 1, UID: 42, VaultID: 7, Enabled: true, Timezone: "UTC",
				Events:    []domain.AutomationEventRule{{Type: domain.AutomationEventCron, Schedule: "* * * * *"}},
				Actions:   []domain.AutomationAction{{Type: domain.AutomationTargetBackup, ConfigID: 2}},
				LastRunAt: time.Now().Add(-2 * time.Minute),
			}}
			backup := &automationBackupStub{err: test.actionErr}
			svc := NewAutomationService(repo, nil, backup, nil, nil, newAutomationExecutionPool(t), zap.NewNop()).(*automationService)

			svc.pollTimeTriggers()
			// A failed attempt advances only the attempt cursor, so the same cron
			// occurrence is not retried on every minute tick.
			svc.pollTimeTriggers()

			require.Equal(t, test.wantMark, repo.marked.Load())
			require.Equal(t, int32(1)-test.wantMark, repo.attempted.Load())
			require.EqualValues(t, 1, backup.calls.Load())
		})
	}
}

func TestGitExecuteSyncReturnsTheActualTaskError(t *testing.T) {
	t.Chdir(t.TempDir())
	repo := new(domainmocks.MockGitSyncRepository)
	conf := &domain.GitSyncConfig{ID: 2, UID: 42, RepoURL: "not a valid git URL", Branch: "main"}
	repo.On("GetByID", mock.Anything, int64(2), int64(42)).Return(conf, nil).Once()
	repo.On("ListHistory", mock.Anything, int64(42), int64(2), 1, 1000).Return([]*domain.GitSyncHistory(nil), int64(0), nil).Once()
	repo.On("Save", mock.Anything, mock.AnythingOfType("*domain.GitSyncConfig"), int64(42)).Return(conf, nil).Maybe()
	repo.On("CreateHistory", mock.Anything, mock.AnythingOfType("*domain.GitSyncHistory"), int64(42)).Return(&domain.GitSyncHistory{}, nil).Maybe()

	service := NewGitSyncService(repo, nil, nil, nil, nil, nil, nil, zap.NewNop()).(*gitSyncService)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, service.Shutdown(ctx))
	}()

	err := service.ExecuteSync(context.Background(), 42, 2, &domain.AutomationExecutionContext{UID: 42, TriggerID: 1, VaultID: 7, EventID: "event-1"})
	require.Error(t, err, "ExecuteSync must not report success before the Git task has completed")
	repo.AssertExpectations(t)
}
