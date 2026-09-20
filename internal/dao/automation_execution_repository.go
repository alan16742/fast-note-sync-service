package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type automationExecutionRepository struct {
	dao         *Dao
	mu          sync.Mutex
	initialized map[int64]bool
}

func NewAutomationExecutionRepository(d *Dao) domain.AutomationExecutionRepository {
	return &automationExecutionRepository{dao: d, initialized: make(map[int64]bool)}
}

func (r *automationExecutionRepository) GetKey(uid int64) string {
	return "user_automation_" + strconv.FormatInt(uid, 10)
}

func init() {
	RegisterModel(ModelConfig{Name: "AutomationExecution", RepoFactory: func(d *Dao) daoDBCustomKey {
		return NewAutomationExecutionRepository(d).(daoDBCustomKey)
	}})
}

func (r *automationExecutionRepository) db(ctx context.Context, uid int64) (*gorm.DB, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.initialized[uid] {
		if err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
			return db.AutoMigrate(&model.AutomationExecution{})
		}); err != nil {
			return nil, err
		}
		r.initialized[uid] = true
	}
	return r.dao.ResolveDB(r.GetKey(uid)).WithContext(ctx), nil
}

func automationExecutionToDomain(item *model.AutomationExecution) (*domain.AutomationExecution, error) {
	if item == nil {
		return nil, nil
	}
	var actions []domain.AutomationActionExecution
	if item.Actions != "" {
		if err := json.Unmarshal([]byte(item.Actions), &actions); err != nil {
			return nil, err
		}
	}
	var event domain.AutomationEvent
	if item.Event != "" {
		if err := json.Unmarshal([]byte(item.Event), &event); err != nil {
			return nil, err
		}
	}
	return &domain.AutomationExecution{
		ID: item.ID, Revision: item.Revision, UID: item.UID, TriggerID: item.TriggerID, VaultID: item.VaultID,
		EventID: item.EventID, EventType: domain.AutomationEventType(item.EventType),
		Event:  event,
		Status: domain.AutomationExecutionStatus(item.Status), Error: item.Error, Actions: actions,
		StartedAt: time.Time(item.StartedAt), FinishedAt: time.Time(item.FinishedAt),
		CreatedAt: time.Time(item.CreatedAt), UpdatedAt: time.Time(item.UpdatedAt),
	}, nil
}

func automationExecutionToModel(item *domain.AutomationExecution) (*model.AutomationExecution, error) {
	if item == nil {
		return nil, nil
	}
	actions, err := json.Marshal(item.Actions)
	if err != nil {
		return nil, err
	}
	eventData := ""
	if item.Event.ID != "" {
		event, err := json.Marshal(item.Event)
		if err != nil {
			return nil, err
		}
		eventData = string(event)
	}
	return &model.AutomationExecution{
		ID: item.ID, Revision: item.Revision, UID: item.UID, TriggerID: item.TriggerID, VaultID: item.VaultID,
		EventID: item.EventID, EventType: string(item.EventType), Status: string(item.Status),
		Event: eventData, Error: item.Error, Actions: string(actions), StartedAt: timex.Time(item.StartedAt),
		FinishedAt: timex.Time(item.FinishedAt), CreatedAt: timex.Time(item.CreatedAt),
		UpdatedAt: timex.Time(item.UpdatedAt),
	}, nil
}

func (r *automationExecutionRepository) GetByID(ctx context.Context, uid, id int64) (*domain.AutomationExecution, error) {
	if uid <= 0 || id <= 0 {
		return nil, gorm.ErrInvalidData
	}
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	var item model.AutomationExecution
	if err := db.Where("id = ? AND uid = ?", id, uid).First(&item).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return automationExecutionToDomain(&item)
}

func (r *automationExecutionRepository) Start(ctx context.Context, execution *domain.AutomationExecution) (bool, *domain.AutomationExecution, error) {
	if execution == nil || execution.UID <= 0 || execution.TriggerID <= 0 || execution.EventID == "" {
		return false, nil, gorm.ErrInvalidData
	}
	if _, err := r.db(ctx, execution.UID); err != nil {
		return false, nil, err
	}
	var current *domain.AutomationExecution
	created := false
	err := r.dao.ExecuteWrite(ctx, execution.UID, r, func(db *gorm.DB) error {
		item, err := automationExecutionToModel(execution)
		if err != nil {
			return err
		}
		now := timex.Time(time.Now().UTC())
		if item.StartedAt.IsZero() {
			item.StartedAt = now
		}
		if item.CreatedAt.IsZero() {
			item.CreatedAt = now
		}
		item.UpdatedAt = now
		result := db.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "uid"}, {Name: "trigger_id"}, {Name: "event_id"}},
			DoNothing: true,
		}).Create(item)
		if result.Error != nil {
			return result.Error
		}
		created = result.RowsAffected == 1
		if !created {
			if err := db.Where("uid = ? AND trigger_id = ? AND event_id = ?", execution.UID, execution.TriggerID, execution.EventID).First(item).Error; err != nil {
				return err
			}
		}
		current, err = automationExecutionToDomain(item)
		return err
	})
	return created, current, err
}

// ClaimRetry performs the retry state transition with a compare-and-swap on
// the persisted status. This is the database-level guard for duplicate retry
// requests; an in-memory mutex alone would not protect two service processes.
func (r *automationExecutionRepository) ClaimRetry(ctx context.Context, execution *domain.AutomationExecution) (bool, error) {
	if execution == nil || execution.ID <= 0 || execution.UID <= 0 || execution.EventID == "" || execution.Status != domain.AutomationExecutionRunning {
		return false, gorm.ErrInvalidData
	}
	if _, err := r.db(ctx, execution.UID); err != nil {
		return false, err
	}
	item, err := automationExecutionToModel(execution)
	if err != nil {
		return false, err
	}
	now := timex.Time(time.Now().UTC())
	if item.StartedAt.IsZero() {
		item.StartedAt = now
		execution.StartedAt = time.Time(now)
	}
	item.UpdatedAt = now
	claimed := false
	err = r.dao.ExecuteWrite(ctx, execution.UID, r, func(db *gorm.DB) error {
		result := db.Model(&model.AutomationExecution{}).
			Where("id = ? AND uid = ? AND revision = ? AND status IN ?", execution.ID, execution.UID, execution.Revision, []string{
				string(domain.AutomationExecutionFailed), string(domain.AutomationExecutionCancelled),
			}).
			Updates(map[string]any{
				"revision":    gorm.Expr("revision + 1"),
				"status":      string(domain.AutomationExecutionRunning),
				"error":       "",
				"actions":     item.Actions,
				"started_at":  item.StartedAt,
				"finished_at": nil,
				"updated_at":  item.UpdatedAt,
			})
		claimed = result.RowsAffected == 1
		return result.Error
	})
	if err == nil && claimed {
		execution.Revision++
		execution.UpdatedAt = time.Time(now)
	}
	return claimed, err
}

func (r *automationExecutionRepository) Update(ctx context.Context, execution *domain.AutomationExecution) error {
	if execution == nil || execution.ID <= 0 || execution.UID <= 0 {
		return gorm.ErrInvalidData
	}
	if _, err := r.db(ctx, execution.UID); err != nil {
		return err
	}
	item, err := automationExecutionToModel(execution)
	if err != nil {
		return err
	}
	item.UpdatedAt = timex.Time(time.Now().UTC())
	err = r.dao.ExecuteWrite(ctx, execution.UID, r, func(db *gorm.DB) error {
		result := db.Model(&model.AutomationExecution{}).
			Where("id = ? AND uid = ? AND revision = ?", execution.ID, execution.UID, execution.Revision).
			Updates(map[string]any{
				"revision": gorm.Expr("revision + 1"),
				"status":   item.Status, "error": item.Error, "actions": item.Actions, "event": item.Event,
				"started_at": item.StartedAt, "finished_at": item.FinishedAt, "updated_at": item.UpdatedAt,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("automation execution %d changed or was removed", execution.ID)
		}
		return nil
	})
	if err == nil {
		execution.Revision++
		execution.UpdatedAt = time.Time(item.UpdatedAt)
	}
	return err
}

// LatestByTrigger is independent of the global history page. A busy rule must
// not hide the last result of a less frequently executed rule.
func (r *automationExecutionRepository) LatestByTrigger(ctx context.Context, uid int64) ([]*domain.AutomationExecution, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	latest := db.Model(&model.AutomationExecution{}).Select("MAX(id)").Where("uid = ?", uid).Group("trigger_id")
	var items []*model.AutomationExecution
	if err := db.Omit("event").Where("uid = ? AND id IN (?)", uid, latest).Find(&items).Error; err != nil {
		return nil, err
	}
	result := make([]*domain.AutomationExecution, 0, len(items))
	for _, item := range items {
		execution, err := automationExecutionToDomain(item)
		if err != nil {
			return nil, err
		}
		result = append(result, execution)
	}
	return result, nil
}

func (r *automationExecutionRepository) List(ctx context.Context, uid, triggerID int64, page, pageSize int) ([]*domain.AutomationExecution, int64, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, 0, err
	}
	query := db.Model(&model.AutomationExecution{}).Where("uid = ?", uid)
	if triggerID > 0 {
		query = query.Where("trigger_id = ?", triggerID)
	}
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	var items []*model.AutomationExecution
	if err := query.Omit("event").Order("id desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&items).Error; err != nil {
		return nil, 0, err
	}
	result := make([]*domain.AutomationExecution, 0, len(items))
	for _, item := range items {
		value, err := automationExecutionToDomain(item)
		if err != nil {
			return nil, 0, err
		}
		result = append(result, value)
	}
	return result, total, nil
}

var automationExecutionTerminalStatuses = []string{
	string(domain.AutomationExecutionSucceeded),
	string(domain.AutomationExecutionFailed),
	string(domain.AutomationExecutionCancelled),
}

// Cleanup removes all records older than before, including abandoned active
// records left by a process crash. It then retains only the newest keep
// terminal records per user. Running executions newer than the retention
// cutoff are never selected by the second pass.
func (r *automationExecutionRepository) Cleanup(ctx context.Context, before time.Time, keep int) (int64, error) {
	if before.IsZero() || keep < 0 {
		return 0, gorm.ErrInvalidData
	}
	uids, err := r.dao.GetAllUserUIDs()
	if err != nil {
		return 0, err
	}
	var deleted int64
	for _, uid := range uids {
		count, err := r.cleanupUser(ctx, uid, before, keep)
		deleted += count
		if err != nil {
			return deleted, fmt.Errorf("cleanup automation executions for uid %d: %w", uid, err)
		}
	}
	return deleted, nil
}

func (r *automationExecutionRepository) cleanupUser(ctx context.Context, uid int64, before time.Time, keep int) (int64, error) {
	if uid <= 0 {
		return 0, gorm.ErrInvalidData
	}
	if _, err := r.db(ctx, uid); err != nil {
		return 0, err
	}
	var deleted int64
	err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		result := db.Where("uid = ? AND (created_at IS NULL OR created_at < ?)", uid, before).
			Delete(&model.AutomationExecution{})
		if result.Error != nil {
			return result.Error
		}
		deleted += result.RowsAffected

		var terminalIDs []int64
		if err := db.Model(&model.AutomationExecution{}).
			Where("uid = ? AND status IN ?", uid, automationExecutionTerminalStatuses).
			Order("created_at DESC, id DESC").Pluck("id", &terminalIDs).Error; err != nil {
			return err
		}
		if len(terminalIDs) <= keep {
			return nil
		}
		for start := keep; start < len(terminalIDs); start += 500 {
			end := start + 500
			if end > len(terminalIDs) {
				end = len(terminalIDs)
			}
			result = db.Where("uid = ? AND id IN ? AND status IN ?", uid, terminalIDs[start:end], automationExecutionTerminalStatuses).Delete(&model.AutomationExecution{})
			if result.Error != nil {
				return result.Error
			}
			deleted += result.RowsAffected
		}
		return nil
	})
	return deleted, err
}

var _ domain.AutomationExecutionRepository = (*automationExecutionRepository)(nil)
