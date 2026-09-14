package dao

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAutomationExecutionRepositoryIsIdempotentAndQueryable(t *testing.T) {
	queue := false
	cfg := config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(t.TempDir(), "db.sqlite3"), EnableWriteQueue: &queue}
	db, err := NewEngine(cfg, zap.NewNop())
	require.NoError(t, err)
	d := New(db, context.Background(), WithConfig(&cfg), WithUserDatabaseConfig(&cfg), WithLogger(zap.NewNop()))
	t.Cleanup(func() {
		d.CleanupConnections(-time.Second)
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})
	require.NoError(t, db.AutoMigrate(&model.User{}))
	require.NoError(t, db.Create(&model.User{UID: 42}).Error)

	repo := NewAutomationExecutionRepository(d)
	userDB := d.ResolveDB("user_automation_42")
	require.NotNil(t, userDB)
	first := &domain.AutomationExecution{
		UID: 42, TriggerID: 7, VaultID: 3, EventID: "event-1", EventType: domain.AutomationEventCron,
		Status:  domain.AutomationExecutionRunning,
		Actions: []domain.AutomationActionExecution{{Type: domain.AutomationTargetWebhook, ConfigID: 9, Status: domain.AutomationExecutionPending}},
	}
	created, current, err := repo.Start(context.Background(), first)
	require.NoError(t, err)
	require.True(t, created)
	require.NotZero(t, current.ID)
	require.True(t, userDB.Migrator().HasIndex(&model.AutomationExecution{}, "idx_automation_execution_event"))

	duplicateRow := &model.AutomationExecution{
		UID: 42, TriggerID: 7, VaultID: 3, EventID: "event-1", EventType: "cron", Status: "running",
	}
	require.Error(t, userDB.Create(duplicateRow).Error, "the database must reject a duplicate trigger/event pair")
	otherTrigger := *first
	otherTrigger.TriggerID = 8
	created, _, err = repo.Start(context.Background(), &otherTrigger)
	require.NoError(t, err)
	require.True(t, created, "the same event may execute once for another trigger")

	created, duplicate, err := repo.Start(context.Background(), first)
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, current.ID, duplicate.ID)

	current.Status = domain.AutomationExecutionSucceeded
	current.Actions[0].Status = domain.AutomationExecutionSucceeded
	current.Event = domain.AutomationEvent{ID: "event-1", UID: 42, VaultID: 3, Type: domain.AutomationEventCron, Action: "cron"}
	require.NoError(t, repo.Update(context.Background(), current))
	rows, total, err := repo.List(context.Background(), 42, 7, 1, 20)
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Status)
	require.Equal(t, domain.AutomationExecutionSucceeded, rows[0].Actions[0].Status)
	require.Equal(t, "event-1", rows[0].Event.ID)
}

func TestAutomationExecutionRepositoryClaimRetryIsAtomic(t *testing.T) {
	queue := false
	cfg := config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(t.TempDir(), "db.sqlite3"), EnableWriteQueue: &queue}
	db, err := NewEngine(cfg, zap.NewNop())
	require.NoError(t, err)
	d := New(db, context.Background(), WithConfig(&cfg), WithUserDatabaseConfig(&cfg), WithLogger(zap.NewNop()))
	t.Cleanup(func() {
		d.CleanupConnections(-time.Second)
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})
	require.NoError(t, db.AutoMigrate(&model.User{}))
	require.NoError(t, db.Create(&model.User{UID: 42}).Error)

	repo := NewAutomationExecutionRepository(d)
	failed := &domain.AutomationExecution{
		UID: 42, TriggerID: 7, VaultID: 3, EventID: "retry-event", EventType: domain.AutomationEventManual,
		Status:  domain.AutomationExecutionFailed,
		Actions: []domain.AutomationActionExecution{{Type: domain.AutomationTargetWebhook, ConfigID: 9, Status: domain.AutomationExecutionPending}},
	}
	created, current, err := repo.Start(context.Background(), failed)
	require.NoError(t, err)
	require.True(t, created)
	current.Status = domain.AutomationExecutionFailed
	require.NoError(t, repo.Update(context.Background(), current))

	type claimResult struct {
		claimed bool
		err     error
	}
	claims := make(chan claimResult, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			candidate := *current
			candidate.Actions = append([]domain.AutomationActionExecution(nil), current.Actions...)
			candidate.Status = domain.AutomationExecutionRunning
			candidate.StartedAt = time.Now().UTC()
			claimed, claimErr := repo.ClaimRetry(context.Background(), &candidate)
			claims <- claimResult{claimed: claimed, err: claimErr}
		}()
	}
	wg.Wait()
	close(claims)
	var claimCount int
	for result := range claims {
		require.NoError(t, result.err)
		if result.claimed {
			claimCount++
		}
	}
	require.Equal(t, 1, claimCount, "only one database compare-and-swap may claim the retry")

	latest, err := repo.GetByID(context.Background(), 42, current.ID)
	require.NoError(t, err)
	require.Equal(t, domain.AutomationExecutionRunning, latest.Status)
}

func TestAutomationExecutionRepositoryCleanupExpiresAndBoundsHistory(t *testing.T) {
	queue := false
	cfg := config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(t.TempDir(), "db.sqlite3"), EnableWriteQueue: &queue}
	db, err := NewEngine(cfg, zap.NewNop())
	require.NoError(t, err)
	d := New(db, context.Background(), WithConfig(&cfg), WithUserDatabaseConfig(&cfg), WithLogger(zap.NewNop()))
	t.Cleanup(func() {
		d.CleanupConnections(-time.Second)
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})
	require.NoError(t, db.AutoMigrate(&model.User{}))
	require.NoError(t, db.Create(&model.User{UID: 42}).Error)

	repo := NewAutomationExecutionRepository(d)
	add := func(id string, status domain.AutomationExecutionStatus) int64 {
		t.Helper()
		created, execution, startErr := repo.Start(context.Background(), &domain.AutomationExecution{
			UID: 42, TriggerID: 7, VaultID: 3, EventID: id, EventType: domain.AutomationEventManual, Status: status,
		})
		require.NoError(t, startErr)
		require.True(t, created)
		execution.Status = status
		require.NoError(t, repo.Update(context.Background(), execution))
		return execution.ID
	}
	oldID := add("old", domain.AutomationExecutionSucceeded)
	add("new-1", domain.AutomationExecutionSucceeded)
	add("new-2", domain.AutomationExecutionFailed)
	add("new-3", domain.AutomationExecutionCancelled)
	add("new-4", domain.AutomationExecutionSucceeded)
	runningID := add("running", domain.AutomationExecutionRunning)

	userDB := d.ResolveDB("user_automation_42")
	old := time.Now().Add(-48 * time.Hour)
	require.NoError(t, userDB.Model(&model.AutomationExecution{}).Where("id = ?", oldID).Update("created_at", old).Error)
	deleted, err := repo.Cleanup(context.Background(), time.Now().Add(-24*time.Hour), 2)
	require.NoError(t, err)
	require.EqualValues(t, 3, deleted, "one expired row and one excess terminal row should be deleted")

	var remaining []model.AutomationExecution
	require.NoError(t, userDB.Order("id").Find(&remaining).Error)
	require.Len(t, remaining, 3)
	var ids []int64
	for _, row := range remaining {
		ids = append(ids, row.ID)
	}
	require.NotContains(t, ids, oldID)
	require.Contains(t, ids, runningID)
}
