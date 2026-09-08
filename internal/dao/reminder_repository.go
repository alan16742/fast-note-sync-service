package dao

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type reminderRepository struct {
	dao         *Dao
	mu          sync.Mutex
	initialized map[int64]bool
}

func NewReminderRepository(d *Dao) domain.ReminderRepository {
	return &reminderRepository{dao: d, initialized: map[int64]bool{}}
}
func (r *reminderRepository) GetKey(uid int64) string {
	return "user_reminder_" + strconv.FormatInt(uid, 10)
}

func init() {
	RegisterModel(ModelConfig{Name: "ReminderJob", RepoFactory: func(d *Dao) daoDBCustomKey { return NewReminderRepository(d).(daoDBCustomKey) }})
}

func (r *reminderRepository) db(ctx context.Context, uid int64) (*gorm.DB, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.initialized[uid] {
		if err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error { return db.AutoMigrate(&model.ReminderJob{}) }); err != nil {
			return nil, err
		}
		r.initialized[uid] = true
	}
	return r.dao.ResolveDB(r.GetKey(uid)).WithContext(ctx), nil
}

func (r *reminderRepository) SyncNote(ctx context.Context, uid, subscriptionID, noteID int64, jobs []domain.ReminderJob) error {
	if _, err := r.db(ctx, uid); err != nil {
		return err
	}
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		return db.Transaction(func(tx *gorm.DB) error {
			keys := make([]string, 0, len(jobs))
			for _, job := range jobs {
				schedule, err := json.Marshal(job.Task)
				if err != nil {
					return err
				}
				item := model.ReminderJob{UID: uid, SubscriptionID: subscriptionID, NoteID: noteID, TaskKey: job.Task.Key, Schedule: string(schedule), Active: true, NextAt: job.NextAt, OccurrenceAt: job.OccurrenceAt}
				keys = append(keys, item.TaskKey)
				if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "uid"}, {Name: "subscription_id"}, {Name: "note_id"}, {Name: "task_key"}}, DoNothing: true}).Create(&item).Error; err != nil {
					return err
				}
				// Reopened tasks start from their new effective time. Live tasks
				// retain delivery progress across repeated scans and restarts.
				if err := tx.Model(&model.ReminderJob{}).Where("uid = ? AND subscription_id = ? AND note_id = ? AND task_key = ? AND active = ?", uid, subscriptionID, noteID, item.TaskKey, false).Updates(map[string]any{"active": true, "schedule": item.Schedule, "next_at": item.NextAt, "occurrence_at": item.OccurrenceAt, "retry_at": 0, "attempts": 0, "lease_until": 0, "claim_token": ""}).Error; err != nil {
					return err
				}
			}
			query := tx.Model(&model.ReminderJob{}).Where("uid = ? AND subscription_id = ? AND note_id = ?", uid, subscriptionID, noteID)
			if len(keys) > 0 {
				query = query.Where("task_key NOT IN ?", keys)
			}
			return query.Updates(map[string]any{"active": false, "next_at": 0, "claim_token": "", "lease_until": 0}).Error
		})
	})
}

func (r *reminderRepository) ListDue(ctx context.Context, uid, subscriptionID, now int64, limit int) ([]domain.ReminderJob, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	var rows []model.ReminderJob
	if err := db.Where("uid = ? AND subscription_id = ? AND active = ? AND next_at > 0 AND next_at <= ? AND retry_at <= ? AND lease_until <= ?", uid, subscriptionID, true, now, now, now).Order("next_at, id").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]domain.ReminderJob, 0, len(rows))
	for _, row := range rows {
		job := domain.ReminderJob{ID: row.ID, UID: uid, SubscriptionID: subscriptionID, NoteID: row.NoteID, NextAt: row.NextAt, OccurrenceAt: row.OccurrenceAt, Attempts: row.Attempts}
		if err := json.Unmarshal([]byte(row.Schedule), &job.Task); err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, nil
}

func (r *reminderRepository) Claim(ctx context.Context, uid, id, now int64, token string) (bool, error) {
	if _, err := r.db(ctx, uid); err != nil {
		return false, err
	}
	claimed := false
	err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		result := db.Model(&model.ReminderJob{}).Where("uid = ? AND id = ? AND active = ? AND next_at > 0 AND next_at <= ? AND retry_at <= ? AND lease_until <= ?", uid, id, true, now, now, now).Updates(map[string]any{"claim_token": token, "lease_until": now + 60})
		claimed = result.RowsAffected == 1
		return result.Error
	})
	return claimed, err
}

func (r *reminderRepository) update(ctx context.Context, uid, id int64, token string, values map[string]any) error {
	if _, err := r.db(ctx, uid); err != nil {
		return err
	}
	values["claim_token"] = ""
	values["lease_until"] = 0
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		return db.Model(&model.ReminderJob{}).Where("uid = ? AND id = ? AND claim_token = ?", uid, id, token).Updates(values).Error
	})
}
func (r *reminderRepository) Finish(ctx context.Context, uid, id int64, token string, nextAt, occurrenceAt int64) error {
	return r.update(ctx, uid, id, token, map[string]any{"next_at": nextAt, "occurrence_at": occurrenceAt, "retry_at": 0, "attempts": 0})
}
func (r *reminderRepository) Retry(ctx context.Context, uid, id int64, token string, retryAt int64) error {
	return r.update(ctx, uid, id, token, map[string]any{"retry_at": retryAt, "attempts": gorm.Expr("attempts + 1")})
}
func (r *reminderRepository) Cancel(ctx context.Context, uid, id int64, token string) error {
	return r.update(ctx, uid, id, token, map[string]any{"active": false, "next_at": 0})
}
