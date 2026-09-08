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

type webhookRepository struct {
	dao         *Dao
	mu          sync.Mutex
	initialized map[int64]bool
}

// NewWebhookRepository creates a user webhook repository.
func NewWebhookRepository(d *Dao) domain.WebhookRepository {
	return &webhookRepository{dao: d, initialized: map[int64]bool{}}
}

func (r *webhookRepository) GetKey(uid int64) string {
	return "user_webhook_" + strconv.FormatInt(uid, 10)
}

func init() {
	factory := func(d *Dao) daoDBCustomKey { return NewWebhookRepository(d).(daoDBCustomKey) }
	RegisterModel(ModelConfig{Name: "WebhookSubscription", RepoFactory: factory})
}

func (r *webhookRepository) db(ctx context.Context, uid int64) (*gorm.DB, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.initialized[uid] {
		if err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error { return db.AutoMigrate(&model.WebhookSubscription{}) }); err != nil {
			return nil, err
		}
		r.initialized[uid] = true
	}
	return r.dao.ResolveDB(r.GetKey(uid)).WithContext(ctx), nil
}

func webhookToDomain(item *model.WebhookSubscription) (*domain.WebhookSubscription, error) {
	if item == nil {
		return nil, nil
	}
	var actions []domain.WebhookAction
	if item.Actions != "" {
		if err := json.Unmarshal([]byte(item.Actions), &actions); err != nil {
			return nil, err
		}
	}
	var headers map[string]string
	if item.Headers != "" {
		if err := json.Unmarshal([]byte(item.Headers), &headers); err != nil {
			return nil, err
		}
	}
	return &domain.WebhookSubscription{
		ID: item.ID, UID: item.UID, Enabled: item.Enabled == 1, Provider: item.Provider, Mode: item.Mode, Timezone: item.Timezone, URL: item.URL, Method: item.Method, Headers: headers, Secret: item.Secret,
		VaultID: item.VaultID, Actions: actions, PathPrefix: item.PathPrefix, PathGlob: item.PathGlob,
		BodyMatcher:   domain.WebhookBodyMatcher{Substring: item.BodySubstring, Regex: item.BodyRegex, MaxBytes: int(item.BodyMaxBytes)},
		TitleTemplate: item.TitleTemplate, BodyTemplate: item.BodyTemplate,
		CreatedAt: time.Time(item.CreatedAt), UpdatedAt: time.Time(item.UpdatedAt),
	}, nil
}

func webhookToModel(item *domain.WebhookSubscription) (*model.WebhookSubscription, error) {
	if item == nil {
		return nil, nil
	}
	actions, err := json.Marshal(item.Actions)
	if err != nil {
		return nil, err
	}
	headers, err := json.Marshal(item.Headers)
	if err != nil {
		return nil, err
	}
	enabled := int64(0)
	if item.Enabled {
		enabled = 1
	}
	return &model.WebhookSubscription{
		ID: item.ID, UID: item.UID, Enabled: enabled, Provider: item.Provider, Mode: item.Mode, Timezone: item.Timezone, URL: item.URL, Method: item.Method, Headers: string(headers), Secret: item.Secret, VaultID: item.VaultID,
		Actions: string(actions), PathPrefix: item.PathPrefix, PathGlob: item.PathGlob,
		BodySubstring: item.BodyMatcher.Substring, BodyRegex: item.BodyMatcher.Regex, BodyMaxBytes: int64(item.BodyMatcher.MaxBytes),
		TitleTemplate: item.TitleTemplate, BodyTemplate: item.BodyTemplate,
		CreatedAt: timex.Time(item.CreatedAt), UpdatedAt: timex.Time(item.UpdatedAt),
	}, nil
}

func (r *webhookRepository) List(ctx context.Context, uid int64) ([]*domain.WebhookSubscription, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	var items []*model.WebhookSubscription
	if err := db.WithContext(ctx).Where("uid = ?", uid).Order("id desc").Find(&items).Error; err != nil {
		return nil, err
	}
	return webhookDomains(items)
}

func (r *webhookRepository) ListEnabled(ctx context.Context, uid int64) ([]*domain.WebhookSubscription, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	var items []*model.WebhookSubscription
	if err := db.WithContext(ctx).Where("uid = ? AND enabled = ?", uid, 1).Order("id desc").Find(&items).Error; err != nil {
		return nil, err
	}
	return webhookDomains(items)
}

func (r *webhookRepository) GetByID(ctx context.Context, id, uid int64) (*domain.WebhookSubscription, error) {
	db, err := r.db(ctx, uid)
	if err != nil {
		return nil, err
	}
	var item model.WebhookSubscription
	if err := db.WithContext(ctx).Where("id = ? AND uid = ?", id, uid).First(&item).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return webhookToDomain(&item)
}

func (r *webhookRepository) Save(ctx context.Context, subscription *domain.WebhookSubscription, uid int64) (*domain.WebhookSubscription, error) {
	if _, err := r.db(ctx, uid); err != nil {
		return nil, err
	}
	var result *domain.WebhookSubscription
	err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		item, err := webhookToModel(subscription)
		if err != nil {
			return err
		}
		item.UID = uid
		now := timex.Now()
		item.UpdatedAt = now
		if item.ID > 0 {
			var old model.WebhookSubscription
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
		result, err = webhookToDomain(item)
		return err
	})
	return result, err
}

func (r *webhookRepository) Delete(ctx context.Context, id, uid int64) error {
	if _, err := r.db(ctx, uid); err != nil {
		return err
	}
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		return db.Where("id = ? AND uid = ?", id, uid).Delete(&model.WebhookSubscription{}).Error
	})
}

func webhookDomains(items []*model.WebhookSubscription) ([]*domain.WebhookSubscription, error) {
	result := make([]*domain.WebhookSubscription, 0, len(items))
	for _, item := range items {
		value, err := webhookToDomain(item)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}
