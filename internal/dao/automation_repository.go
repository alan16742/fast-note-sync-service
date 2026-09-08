package dao

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"gorm.io/gorm"
)

type automationRepository struct {
	dao         *Dao
	mu          sync.Mutex
	initialized map[int64]bool
}

func NewAutomationRepository(d *Dao) domain.AutomationRepository {
	return &automationRepository{dao: d, initialized: make(map[int64]bool)}
}

func (r *automationRepository) GetKey(uid int64) string {
	return "user_automation_" + strconv.FormatInt(uid, 10)
}

func init() {
	factory := func(d *Dao) daoDBCustomKey { return NewAutomationRepository(d).(daoDBCustomKey) }
	RegisterModel(ModelConfig{Name: "AutomationTrigger", RepoFactory: factory})
}

func (r *automationRepository) db(ctx context.Context, uid int64) (*gorm.DB, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.initialized[uid] {
		if err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
			return db.AutoMigrate(&model.AutomationTrigger{})
		}); err != nil {
			return nil, err
		}
		r.initialized[uid] = true
	}
	return r.dao.ResolveDB(r.GetKey(uid)).WithContext(ctx), nil
}

func automationToDomain(item *model.AutomationTrigger) (*domain.AutomationTrigger, error) {
	if item == nil {
		return nil, nil
	}
	var eventActions []string
	if item.EventActions != "" {
		if err := json.Unmarshal([]byte(item.EventActions), &eventActions); err != nil {
			return nil, err
		}
	}
	var actions []domain.AutomationAction
	if item.Actions != "" {
		if err := json.Unmarshal([]byte(item.Actions), &actions); err != nil {
			return nil, err
		}
	}
	lastRunAt := time.Time{}
	if item.LastRunAt > 0 {
		lastRunAt = time.Unix(item.LastRunAt, 0)
	}
	return &domain.AutomationTrigger{
		ID: item.ID, UID: item.UID, Name: item.Name, Enabled: item.Enabled == 1,
		EventType: domain.AutomationEventType(item.EventType), VaultID: item.VaultID,
		Timezone: item.Timezone, Schedule: item.Schedule, ContentContains: item.ContentContains,
		PathPrefix: item.PathPrefix, PathGlob: item.PathGlob, EventActions: eventActions,
		Actions: actions, LastRunAt: lastRunAt,
		CreatedAt: time.Time(item.CreatedAt), UpdatedAt: time.Time(item.UpdatedAt),
	}, nil
}

func automationToModel(item *domain.AutomationTrigger) (*model.AutomationTrigger, error) {
	if item == nil {
		return nil, nil
	}
	eventActions, err := json.Marshal(item.EventActions)
	if err != nil {
		return nil, err
	}
	actions, err := json.Marshal(item.Actions)
	if err != nil {
		return nil, err
	}
	enabled := int64(0)
	if item.Enabled {
		enabled = 1
	}
	lastRunAt := int64(0)
	if !item.LastRunAt.IsZero() {
		lastRunAt = item.LastRunAt.Unix()
	}
	return &model.AutomationTrigger{
		ID: item.ID, UID: item.UID, Name: item.Name, Enabled: enabled,
		EventType: string(item.EventType), VaultID: item.VaultID, Timezone: item.Timezone,
		Schedule: item.Schedule, ContentContains: item.ContentContains, PathPrefix: item.PathPrefix,
		PathGlob: item.PathGlob, EventActions: string(eventActions), Actions: string(actions),
		LastRunAt: lastRunAt, CreatedAt: timex.Time(item.CreatedAt), UpdatedAt: timex.Time(item.UpdatedAt),
	}, nil
}

func automationDomains(items []*model.AutomationTrigger) ([]*domain.AutomationTrigger, error) {
	result := make([]*domain.AutomationTrigger, 0, len(items))
	for _, item := range items {
		value, err := automationToDomain(item)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (r *automationRepository) List(ctx context.Context, uid int64) ([]*domain.AutomationTrigger, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	var items []*model.AutomationTrigger
	if err := db.Where("uid = ?", uid).Order("id desc").Find(&items).Error; err != nil {
		return nil, err
	}
	return automationDomains(items)
}

func (r *automationRepository) ListEnabled(ctx context.Context, uid int64, eventType domain.AutomationEventType) ([]*domain.AutomationTrigger, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	var items []*model.AutomationTrigger
	if err := db.Where("uid = ? AND enabled = ? AND event_type = ?", uid, 1, string(eventType)).Order("id asc").Find(&items).Error; err != nil {
		return nil, err
	}
	return automationDomains(items)
}

func (r *automationRepository) ListEnabledByType(ctx context.Context, eventType domain.AutomationEventType) ([]*domain.AutomationTrigger, error) {
	uids, err := r.dao.GetAllUserUIDs()
	if err != nil {
		return nil, err
	}
	result := make([]*domain.AutomationTrigger, 0)
	for _, uid := range uids {
		db, err := r.db(ctx, uid)
		if err != nil {
			continue
		}
		var items []*model.AutomationTrigger
		if err := db.Where("uid = ? AND enabled = ? AND event_type = ?", uid, 1, string(eventType)).Order("id asc").Find(&items).Error; err != nil {
			continue
		}
		values, err := automationDomains(items)
		if err != nil {
			return nil, err
		}
		result = append(result, values...)
	}
	return result, nil
}

func (r *automationRepository) GetByID(ctx context.Context, id, uid int64) (*domain.AutomationTrigger, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	var item model.AutomationTrigger
	if err := db.Where("id = ? AND uid = ?", id, uid).First(&item).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return automationToDomain(&item)
}

func (r *automationRepository) Save(ctx context.Context, trigger *domain.AutomationTrigger, uid int64) (*domain.AutomationTrigger, error) {
	if _, err := r.db(ctx, uid); err != nil {
		return nil, err
	}
	var result *domain.AutomationTrigger
	err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		item, err := automationToModel(trigger)
		if err != nil {
			return err
		}
		item.UID = uid
		now := timex.Now()
		item.UpdatedAt = now
		if item.ID > 0 {
			var old model.AutomationTrigger
			if err := db.Where("id = ? AND uid = ?", item.ID, uid).First(&old).Error; err != nil {
				return err
			}
			item.CreatedAt = old.CreatedAt
			if err := db.Save(item).Error; err != nil {
				return err
			}
		} else {
			item.CreatedAt = now
			if err := db.Create(item).Error; err != nil {
				return err
			}
		}
		result, err = automationToDomain(item)
		return err
	})
	return result, err
}

func (r *automationRepository) Delete(ctx context.Context, id, uid int64) error {
	if _, err := r.db(ctx, uid); err != nil {
		return err
	}
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		return db.Where("id = ? AND uid = ?", id, uid).Delete(&model.AutomationTrigger{}).Error
	})
}

func (r *automationRepository) MarkRun(ctx context.Context, id, uid int64, at time.Time) error {
	if _, err := r.db(ctx, uid); err != nil {
		return err
	}
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		return db.Model(&model.AutomationTrigger{}).Where("id = ? AND uid = ?", id, uid).Update("last_run_at", at.Unix()).Error
	})
}

var _ domain.AutomationRepository = (*automationRepository)(nil)
