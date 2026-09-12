package dao

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
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
	var headers map[string]string
	if item.Headers != "" {
		if err := json.Unmarshal([]byte(item.Headers), &headers); err != nil {
			return nil, err
		}
	}
	return &domain.WebhookSubscription{
		ID: item.ID, UID: item.UID, Provider: item.Provider, URL: item.URL, Method: item.Method, Headers: headers, Secret: item.Secret,
		TitleTemplate: item.TitleTemplate, BodyTemplate: item.BodyTemplate,
		CreatedAt: time.Time(item.CreatedAt), UpdatedAt: time.Time(item.UpdatedAt),
	}, nil
}

func webhookToModel(item *domain.WebhookSubscription) (*model.WebhookSubscription, error) {
	if item == nil {
		return nil, nil
	}
	headers := make(map[string]string, len(item.Headers))
	for key, value := range item.Headers {
		headers[key] = value
	}
	headerData, err := json.Marshal(headers)
	if err != nil {
		return nil, err
	}
	return &model.WebhookSubscription{
		ID: item.ID, UID: item.UID, Provider: item.Provider, URL: item.URL, Method: item.Method, Headers: string(headerData), Secret: item.Secret,
		TitleTemplate: item.TitleTemplate, BodyTemplate: item.BodyTemplate,
		CreatedAt: timex.Time(item.CreatedAt), UpdatedAt: timex.Time(item.UpdatedAt),
	}, nil
}

func (r *webhookRepository) transformSecrets(m *model.WebhookSubscription, transform ValueTransformer) error {
	if m == nil {
		return nil
	}
	if err := transformStrings(transform, &m.URL, &m.Secret); err != nil {
		return err
	}

	headers := make(map[string]string)
	if strings.TrimSpace(m.Headers) != "" {
		if err := json.Unmarshal([]byte(m.Headers), &headers); err != nil {
			return err
		}
	}
	for key, value := range headers {
		if !util.IsSensitiveHeaderName(key) {
			continue
		}
		if err := transformStrings(transform, &value); err != nil {
			return err
		}
		headers[key] = value
	}
	data, err := json.Marshal(headers)
	if err != nil {
		return err
	}
	m.Headers = string(data)
	return nil
}

func (r *webhookRepository) fromModel(m *model.WebhookSubscription) (*domain.WebhookSubscription, error) {
	plain, err := cloneAndTransform(m, func(copy *model.WebhookSubscription) error {
		return r.transformSecrets(copy, r.dao.DataProtector().Decrypt)
	})
	if err != nil {
		return nil, err
	}
	return webhookToDomain(plain)
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
	result := make([]*domain.WebhookSubscription, 0, len(items))
	for _, item := range items {
		value, err := r.fromModel(item)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
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
	value, err := r.fromModel(&item)
	if err != nil {
		return nil, err
	}
	return value, nil
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
		if err := r.transformSecrets(item, r.dao.DataProtector().Encrypt); err != nil {
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
		saved := *subscription
		saved.ID = item.ID
		saved.UID = item.UID
		saved.CreatedAt = time.Time(item.CreatedAt)
		saved.UpdatedAt = time.Time(item.UpdatedAt)
		result = &saved
		return nil
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
