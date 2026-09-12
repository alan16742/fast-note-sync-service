package dao

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/internal/query"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"gorm.io/gorm"
)

type gitSyncRepository struct {
	dao             *Dao
	customPrefixKey string
}

// NewGitSyncRepository creates GitSyncRepository instance
// NewGitSyncRepository 创建 GitSyncRepository 实例
func NewGitSyncRepository(dao *Dao) domain.GitSyncRepository {
	return &gitSyncRepository{dao: dao, customPrefixKey: "user_git_sync_"}
}

func (r *gitSyncRepository) GetKey(uid int64) string {
	return r.customPrefixKey + strconv.FormatInt(uid, 10)
}

func init() {
	factory := func(d *Dao) daoDBCustomKey {
		return NewGitSyncRepository(d).(daoDBCustomKey)
	}
	RegisterModel(ModelConfig{
		Name:        "GitSyncConfig",
		RepoFactory: factory,
	})
	RegisterModel(ModelConfig{
		Name:        "GitSyncHistory",
		RepoFactory: factory,
	})
}

func (r *gitSyncRepository) gitSync(uid int64) *query.Query {
	return r.dao.QueryWithOnceInit(func(g *gorm.DB) error {
		if err := model.AutoMigrate(g, "GitSyncConfig"); err != nil {
			return err
		}
		return model.AutoMigrate(g, "GitSyncHistory")
	}, r.GetKey(uid)+"#git_sync", r.GetKey(uid))
}

func (r *gitSyncRepository) historyToDomain(m *model.GitSyncHistory) *domain.GitSyncHistory {
	if m == nil {
		return nil
	}
	return &domain.GitSyncHistory{
		ID:        m.ID,
		UID:       m.UID,
		ConfigID:  m.ConfigID,
		TriggerID: m.TriggerID,
		VaultID:   m.VaultID,
		StartTime: m.StartTime,
		EndTime:   m.EndTime,
		Status:    m.Status,
		Message:   m.Message,
		CreatedAt: time.Time(m.CreatedAt),
		UpdatedAt: time.Time(m.UpdatedAt),
	}
}

func (r *gitSyncRepository) historyToModel(d *domain.GitSyncHistory) *model.GitSyncHistory {
	if d == nil {
		return nil
	}
	return &model.GitSyncHistory{
		ID:        d.ID,
		UID:       d.UID,
		ConfigID:  d.ConfigID,
		TriggerID: d.TriggerID,
		VaultID:   d.VaultID,
		StartTime: d.StartTime,
		EndTime:   d.EndTime,
		Status:    d.Status,
		Message:   d.Message,
		CreatedAt: timex.Time(d.CreatedAt),
		UpdatedAt: timex.Time(d.UpdatedAt),
	}
}

// ... existing config methods ...

func (r *gitSyncRepository) CreateHistory(ctx context.Context, history *domain.GitSyncHistory, uid int64) (*domain.GitSyncHistory, error) {
	var result *domain.GitSyncHistory
	err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.gitSync(uid).GitSyncHistory
		m := r.historyToModel(history)
		m.UID = uid
		m.CreatedAt = timex.Now()
		m.UpdatedAt = timex.Now()
		if err := q.WithContext(ctx).Save(m); err != nil {
			return err
		}
		result = r.historyToDomain(m)
		return nil
	})
	return result, err
}

func (r *gitSyncRepository) ListHistory(ctx context.Context, uid int64, configID int64, page, pageSize int) ([]*domain.GitSyncHistory, int64, error) {
	q := r.gitSync(uid).GitSyncHistory
	offset := (page - 1) * pageSize
	db := q.WithContext(ctx).Where(q.UID.Eq(uid))
	if configID > 0 {
		db = db.Where(q.ConfigID.Eq(configID))
	}
	modelList, count, err := db.Order(q.ID.Desc()).FindByPage(offset, pageSize)
	if err != nil {
		return nil, 0, err
	}

	var list []*domain.GitSyncHistory
	for _, m := range modelList {
		list = append(list, r.historyToDomain(m))
	}
	return list, count, nil
}

func (r *gitSyncRepository) configToDomain(m *model.GitSyncConfig) *domain.GitSyncConfig {
	if m == nil {
		return nil
	}
	var lastSyncTime *time.Time
	if !m.LastSyncTime.IsZero() {
		t := m.LastSyncTime
		lastSyncTime = &t
	}
	return &domain.GitSyncConfig{
		ID:            m.ID,
		UID:           m.UID,
		VaultID:       m.VaultID,
		RepoURL:       m.RepoURL,
		Username:      m.Username,
		Password:      m.Password,
		Branch:        m.Branch,
		RetentionDays: m.RetentionDays,
		LastSyncTime:  lastSyncTime,
		LastStatus:    m.LastStatus,
		LastMessage:   m.LastMessage,
		IncludeConfig: m.IncludeConfig == 1,
		ConfigSyncRules: func() []string {
			var rules []string
			_ = json.Unmarshal([]byte(m.ConfigSyncRules), &rules)
			return rules
		}(),
		CreatedAt: time.Time(m.CreatedAt),
		UpdatedAt: time.Time(m.UpdatedAt),
	}
}

func (r *gitSyncRepository) configToModel(d *domain.GitSyncConfig) *model.GitSyncConfig {
	if d == nil {
		return nil
	}
	var lastSyncTime time.Time
	if d.LastSyncTime != nil {
		lastSyncTime = *d.LastSyncTime
	}
	return &model.GitSyncConfig{
		ID:            d.ID,
		UID:           d.UID,
		VaultID:       d.VaultID,
		RepoURL:       d.RepoURL,
		Username:      d.Username,
		Password:      d.Password,
		Branch:        d.Branch,
		RetentionDays: d.RetentionDays,
		LastSyncTime:  lastSyncTime,
		LastStatus:    d.LastStatus,
		LastMessage:   d.LastMessage,
		IncludeConfig: func() int64 {
			if d.IncludeConfig {
				return 1
			}
			return 0
		}(),
		ConfigSyncRules: func() string {
			b, _ := json.Marshal(d.ConfigSyncRules)
			return string(b)
		}(),
		CreatedAt: timex.Time(d.CreatedAt),
		UpdatedAt: timex.Time(d.UpdatedAt),
	}
}

func (r *gitSyncRepository) transformSecrets(m *model.GitSyncConfig, transform ValueTransformer) error {
	if m == nil {
		return nil
	}
	return transformStrings(transform, &m.Password)
}

func (r *gitSyncRepository) configFromModel(m *model.GitSyncConfig) (*domain.GitSyncConfig, error) {
	plain, err := cloneAndTransform(m, func(copy *model.GitSyncConfig) error {
		return r.transformSecrets(copy, r.dao.DataProtector().Decrypt)
	})
	if err != nil {
		return nil, err
	}
	return r.configToDomain(plain), nil
}

func (r *gitSyncRepository) GetByID(ctx context.Context, id, uid int64) (*domain.GitSyncConfig, error) {
	q := r.gitSync(uid).GitSyncConfig
	m, err := q.WithContext(ctx).Where(q.ID.Eq(id), q.UID.Eq(uid)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return r.configFromModel(m)
}

func (r *gitSyncRepository) Save(ctx context.Context, config *domain.GitSyncConfig, uid int64) (*domain.GitSyncConfig, error) {
	var result *domain.GitSyncConfig
	err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.gitSync(uid).GitSyncConfig
		m := r.configToModel(config)
		if err := r.transformSecrets(m, r.dao.DataProtector().Encrypt); err != nil {
			return err
		}
		m.UID = uid

		if config.ID > 0 {
			old, err := q.WithContext(ctx).Where(q.ID.Eq(config.ID), q.UID.Eq(uid)).First()
			if err != nil {
				return err
			}
			m.CreatedAt = old.CreatedAt
			m.UpdatedAt = timex.Now()
			if err := q.WithContext(ctx).Save(m); err != nil {
				return err
			}
		} else {
			m.CreatedAt = timex.Now()

			if err := q.WithContext(ctx).Create(m); err != nil {
				return err
			}
		}
		saved := *config
		saved.ID = m.ID
		saved.UID = m.UID
		saved.CreatedAt = time.Time(m.CreatedAt)
		saved.UpdatedAt = time.Time(m.UpdatedAt)
		result = &saved
		return nil
	})
	return result, err
}

func (r *gitSyncRepository) Delete(ctx context.Context, id, uid int64) error {
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.gitSync(uid).GitSyncConfig
		_, err := q.WithContext(ctx).Where(q.ID.Eq(id), q.UID.Eq(uid)).Delete()
		return err
	})
}

func (r *gitSyncRepository) List(ctx context.Context, uid int64) ([]*domain.GitSyncConfig, error) {
	q := r.gitSync(uid).GitSyncConfig
	ms, err := q.WithContext(ctx).Where(q.UID.Eq(uid)).Order(q.ID.Desc()).Find()
	if err != nil {
		return nil, err
	}
	var res []*domain.GitSyncConfig
	for _, m := range ms {
		item, err := r.configFromModel(m)
		if err != nil {
			return nil, err
		}
		res = append(res, item)
	}
	return res, nil
}

func (r *gitSyncRepository) DeleteHistory(ctx context.Context, uid int64, configID int64) error {
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.gitSync(uid).GitSyncHistory
		query := q.WithContext(ctx).Where(q.UID.Eq(uid))
		if configID > 0 {
			query = query.Where(q.ConfigID.Eq(configID))
		}
		_, err := query.Delete()
		return err
	})
}

func (r *gitSyncRepository) DeleteOldHistory(ctx context.Context, uid int64, configID int64, cutoffTime time.Time) error {
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.gitSync(uid).GitSyncHistory
		query := q.WithContext(ctx).Where(q.UID.Eq(uid), q.CreatedAt.Lt(timex.Time(cutoffTime)))
		if configID > 0 {
			query = query.Where(q.ConfigID.Eq(configID))
		}
		_, err := query.Delete()
		return err
	})
}

// Ensure gitSyncRepository implements domain.GitSyncRepository interface
// 确保 gitSyncRepository 实现了 domain.GitSyncRepository 接口
var _ domain.GitSyncRepository = (*gitSyncRepository)(nil)
