package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

type automationBlockingRetryBackupStub struct {
	BackupService
	calls     atomic.Int32
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

type automationSequenceBackupStub struct {
	BackupService
	errs  []error
	calls atomic.Int32
}

func (s *automationSequenceBackupStub) ExecuteUserBackup(context.Context, int64, int64, *domain.AutomationExecutionContext) error {
	index := int(s.calls.Add(1) - 1)
	if index < len(s.errs) {
		return s.errs[index]
	}
	return nil
}

func (s *automationBackupStub) ExecuteUserBackup(context.Context, int64, int64, *domain.AutomationExecutionContext) error {
	s.calls.Add(1)
	return s.err
}

func (s *automationBlockingRetryBackupStub) ExecuteUserBackup(context.Context, int64, int64, *domain.AutomationExecutionContext) error {
	call := s.calls.Add(1)
	if call == 1 {
		return errors.New("initial failure")
	}
	if call == 2 {
		s.startOnce.Do(func() { close(s.started) })
		<-s.release
	}
	return nil
}

type automationCronRepository struct {
	domain.AutomationRepository
	trigger   *domain.AutomationTrigger
	markErr   error
	marked    atomic.Int32
	attempted atomic.Int32
}

type automationExecutionRepositoryStub struct {
	mu         sync.Mutex
	nextID     int64
	executions map[string]*domain.AutomationExecution
}

func newAutomationExecutionRepositoryStub() *automationExecutionRepositoryStub {
	return &automationExecutionRepositoryStub{executions: make(map[string]*domain.AutomationExecution)}
}

func (r *automationExecutionRepositoryStub) Start(_ context.Context, execution *domain.AutomationExecution) (bool, *domain.AutomationExecution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%d:%d:%s", execution.UID, execution.TriggerID, execution.EventID)
	if current, ok := r.executions[key]; ok {
		return false, cloneAutomationExecution(current), nil
	}
	r.nextID++
	value := cloneAutomationExecution(execution)
	value.ID = r.nextID
	value.CreatedAt = time.Now().UTC()
	value.UpdatedAt = value.CreatedAt
	r.executions[key] = value
	return true, cloneAutomationExecution(value), nil
}

func (r *automationExecutionRepositoryStub) ClaimRetry(_ context.Context, execution *domain.AutomationExecution) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, current := range r.executions {
		if current.UID != execution.UID || current.ID != execution.ID {
			continue
		}
		if current.Status != domain.AutomationExecutionFailed && current.Status != domain.AutomationExecutionCancelled {
			return false, nil
		}
		r.executions[key] = cloneAutomationExecution(execution)
		return true, nil
	}
	return false, nil
}

func (r *automationExecutionRepositoryStub) Update(_ context.Context, execution *domain.AutomationExecution) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%d:%d:%s", execution.UID, execution.TriggerID, execution.EventID)
	r.executions[key] = cloneAutomationExecution(execution)
	return nil
}

func (r *automationExecutionRepositoryStub) GetByID(_ context.Context, uid, id int64) (*domain.AutomationExecution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, execution := range r.executions {
		if execution.UID == uid && execution.ID == id {
			return cloneAutomationExecution(execution), nil
		}
	}
	return nil, nil
}

func (r *automationExecutionRepositoryStub) List(_ context.Context, uid, triggerID int64, _, _ int) ([]*domain.AutomationExecution, int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []*domain.AutomationExecution
	for _, execution := range r.executions {
		if execution.UID == uid && (triggerID <= 0 || execution.TriggerID == triggerID) {
			result = append(result, cloneAutomationExecution(execution))
		}
	}
	return result, int64(len(result)), nil
}

func (r *automationExecutionRepositoryStub) Cleanup(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func cloneAutomationExecution(execution *domain.AutomationExecution) *domain.AutomationExecution {
	if execution == nil {
		return nil
	}
	value := *execution
	value.Actions = append([]domain.AutomationActionExecution(nil), execution.Actions...)
	return &value
}

type automationRetryTriggerRepository struct {
	domain.AutomationRepository
	trigger *domain.AutomationTrigger
}

func (r *automationRetryTriggerRepository) GetByID(context.Context, int64, int64) (*domain.AutomationTrigger, error) {
	return r.trigger, nil
}

func (r *automationCronRepository) ListEnabledByType(context.Context, domain.AutomationEventType) ([]*domain.AutomationTrigger, error) {
	return []*domain.AutomationTrigger{r.trigger}, nil
}

func (r *automationCronRepository) MarkRun(_ context.Context, _ int64, _ int64, at time.Time) error {
	if r.markErr != nil {
		r.marked.Add(1)
		return r.markErr
	}
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

func TestExecuteTriggerRecordsResultsAndDeduplicatesEvent(t *testing.T) {
	backup := &automationBackupStub{}
	executions := newAutomationExecutionRepositoryStub()
	svc := NewAutomationService(nil, nil, backup, nil, nil, newAutomationExecutionPool(t), zap.NewNop(), executions).(*automationService)
	trigger := &domain.AutomationTrigger{ID: 1, UID: 42, VaultID: 7, Enabled: true, Actions: []domain.AutomationAction{{Type: domain.AutomationTargetBackup, ConfigID: 2}}}
	event := &domain.AutomationEvent{ID: "event-1", UID: 42, VaultID: 7, Type: domain.AutomationEventManual}

	require.NoError(t, svc.executeTrigger(context.Background(), trigger, event))
	require.NoError(t, svc.executeTrigger(context.Background(), trigger, event))
	require.EqualValues(t, 1, backup.calls.Load(), "a duplicate event must not repeat a successful action")

	rows, total, err := executions.List(context.Background(), 42, 1, 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Status)
	require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Actions[0].Status)
}

func TestExecuteTriggerRecordsActionFailure(t *testing.T) {
	wantErr := errors.New("backup failed")
	backup := &automationBackupStub{err: wantErr}
	executions := newAutomationExecutionRepositoryStub()
	svc := NewAutomationService(nil, nil, backup, nil, nil, newAutomationExecutionPool(t), zap.NewNop(), executions).(*automationService)
	trigger := &domain.AutomationTrigger{ID: 1, UID: 42, VaultID: 7, Enabled: true, Actions: []domain.AutomationAction{{Type: domain.AutomationTargetBackup, ConfigID: 2}}}
	event := &domain.AutomationEvent{ID: "event-failed", UID: 42, VaultID: 7, Type: domain.AutomationEventManual}

	require.ErrorIs(t, svc.executeTrigger(context.Background(), trigger, event), wantErr)
	rows, _, err := executions.List(context.Background(), 42, 1, 1, 20)
	require.NoError(t, err)
	require.Equal(t, domain.AutomationExecutionFailed, rows[0].Status)
	require.Equal(t, domain.AutomationExecutionFailed, rows[0].Actions[0].Status)
	require.EqualError(t, errors.New(rows[0].Actions[0].Error), wantErr.Error())
}

func TestRetryExecutionOnlyRunsFailedActions(t *testing.T) {
	wantErr := errors.New("second action failed")
	backup := &automationSequenceBackupStub{errs: []error{nil, wantErr, nil}}
	executions := newAutomationExecutionRepositoryStub()
	trigger := &domain.AutomationTrigger{
		ID: 1, UID: 42, VaultID: 7, Enabled: true,
		Actions: []domain.AutomationAction{
			{Type: domain.AutomationTargetBackup, ConfigID: 2},
			{Type: domain.AutomationTargetBackup, ConfigID: 3},
		},
	}
	svc := NewAutomationService(&automationRetryTriggerRepository{trigger: trigger}, nil, backup, nil, nil, newAutomationExecutionPool(t), zap.NewNop(), executions).(*automationService)
	event := &domain.AutomationEvent{ID: "event-partial", UID: 42, VaultID: 7, Type: domain.AutomationEventManual}

	require.ErrorIs(t, svc.executeTrigger(context.Background(), trigger, event), wantErr)
	rows, _, err := executions.List(context.Background(), 42, 1, 1, 20)
	require.NoError(t, err)
	require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Actions[0].Status)
	require.Equal(t, domain.AutomationExecutionFailed, rows[0].Actions[1].Status)

	require.NoError(t, svc.RetryExecution(context.Background(), 42, rows[0].ID))
	require.EqualValues(t, 3, backup.calls.Load(), "retry must skip the action that already succeeded")
	rows, _, err = executions.List(context.Background(), 42, 1, 1, 20)
	require.NoError(t, err)
	require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Status)
	require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Actions[0].Status)
	require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Actions[1].Status)
}

func TestConcurrentRetryExecutionClaimsAnExecutionOnlyOnce(t *testing.T) {
	backup := &automationBlockingRetryBackupStub{started: make(chan struct{}), release: make(chan struct{})}
	executions := newAutomationExecutionRepositoryStub()
	trigger := &domain.AutomationTrigger{
		ID: 1, UID: 42, VaultID: 7, Enabled: true,
		Actions: []domain.AutomationAction{{Type: domain.AutomationTargetBackup, ConfigID: 2}},
	}
	svc := NewAutomationService(&automationRetryTriggerRepository{trigger: trigger}, nil, backup, nil, nil, newAutomationExecutionPool(t), zap.NewNop(), executions).(*automationService)
	event := &domain.AutomationEvent{ID: "event-concurrent-retry", UID: 42, VaultID: 7, Type: domain.AutomationEventManual}

	require.Error(t, svc.executeTrigger(context.Background(), trigger, event))
	rows, _, err := executions.List(context.Background(), 42, 1, 1, 20)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	firstDone := make(chan error, 1)
	go func() { firstDone <- svc.RetryExecution(context.Background(), 42, rows[0].ID) }()
	select {
	case <-backup.started:
	case <-time.After(time.Second):
		t.Fatal("first retry did not start its action")
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- svc.RetryExecution(context.Background(), 42, rows[0].ID) }()
	select {
	case secondErr := <-secondDone:
		require.Error(t, secondErr)
	case <-time.After(time.Second):
		t.Fatal("second retry was not rejected while the first retry was running")
	}
	require.EqualValues(t, 2, backup.calls.Load(), "the second retry must not execute the action")

	close(backup.release)
	require.NoError(t, <-firstDone)
}

func TestCronOccurrenceIDPreventsDuplicateAfterCursorWriteFailure(t *testing.T) {
	markErr := errors.New("cursor write failed")
	repo := &automationCronRepository{
		markErr: markErr,
		trigger: &domain.AutomationTrigger{
			ID: 1, UID: 42, VaultID: 7, Enabled: true, Timezone: "UTC",
			Events:    []domain.AutomationEventRule{{Type: domain.AutomationEventCron, Schedule: "* * * * *"}},
			Actions:   []domain.AutomationAction{{Type: domain.AutomationTargetBackup, ConfigID: 2}},
			LastRunAt: time.Now().Add(-2 * time.Minute),
		},
	}
	backup := &automationBackupStub{}
	executions := newAutomationExecutionRepositoryStub()
	svc := NewAutomationService(repo, nil, backup, nil, nil, newAutomationExecutionPool(t), zap.NewNop(), executions).(*automationService)

	svc.pollTimeTriggers()
	svc.pollTimeTriggers()

	require.EqualValues(t, 1, backup.calls.Load(), "a cursor write failure must not repeat the same cron occurrence")
	require.EqualValues(t, 2, repo.marked.Load(), "the cursor write should be retried while the occurrence remains uncommitted")
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
