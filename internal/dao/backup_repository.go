package dao

import (
	"context"
	"fmt"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/internal/query"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"gorm.io/gorm"
)

type backupRepository struct {
	dao *Dao
}

// NewBackupRepository creates BackupRepository instance
// NewBackupRepository 创建 BackupRepository 实例
func NewBackupRepository(dao *Dao) domain.BackupRepository {
	return &backupRepository{dao: dao}
}

func (r *backupRepository) GetKey(uid int64) string {
	return "user_backup_" + fmt.Sprintf("%d", uid)
}

func init() {
	factory := func(d *Dao) daoDBCustomKey {
		return NewBackupRepository(d).(daoDBCustomKey)
	}
	RegisterModel(ModelConfig{
		Name:        "BackupConfig",
		RepoFactory: factory,
	})
	RegisterModel(ModelConfig{
		Name:        "BackupHistory",
		RepoFactory: factory,
	})
}

func (r *backupRepository) backup(uid int64) *query.Query {
	return r.dao.QueryWithOnceInit(func(g *gorm.DB) {
		model.AutoMigrate(g, "BackupConfig")
		model.AutoMigrate(g, "BackupHistory")
	}, r.GetKey(uid)+"#backup", r.GetKey(uid))
}

func configToDomain(m *model.BackupConfig) *domain.BackupConfig {
	if m == nil {
		return nil
	}
	return &domain.BackupConfig{
		ID:               m.ID,
		UID:              m.UID,
		VaultID:          m.VaultID,
		Type:             m.Type,
		StorageIds:       m.StorageIds,
		IncludeVaultName: m.IncludeVaultName == 1,
		RetentionDays:    int(m.RetentionDays),
		LastRunTime:      m.LastRunTime,
		LastStatus:       int(m.LastStatus),
		LastMessage:      m.LastMessage,
		PasswordMode:     int(m.PasswordMode),
		PasswordValue:    m.PasswordValue,
		CreatedAt:        time.Time(m.CreatedAt),
		UpdatedAt:        time.Time(m.UpdatedAt),
	}
}

func configToModel(d *domain.BackupConfig) *model.BackupConfig {
	if d == nil {
		return nil
	}
	includeVaultName := int64(0)
	if d.IncludeVaultName {
		includeVaultName = 1
	}
	return &model.BackupConfig{
		ID:               d.ID,
		UID:              d.UID,
		VaultID:          d.VaultID,
		Type:             d.Type,
		StorageIds:       d.StorageIds,
		IncludeVaultName: includeVaultName,
		RetentionDays:    int64(d.RetentionDays),
		LastRunTime:      d.LastRunTime,
		LastStatus:       int64(d.LastStatus),
		LastMessage:      d.LastMessage,
		PasswordMode:     int64(d.PasswordMode),
		PasswordValue:    d.PasswordValue,
		CreatedAt:        timex.Time(d.CreatedAt),
		UpdatedAt:        timex.Time(d.UpdatedAt),
	}
}

func historyToDomain(m *model.BackupHistory) *domain.BackupHistory {
	if m == nil {
		return nil
	}
	return &domain.BackupHistory{
		ID:        m.ID,
		UID:       m.UID,
		ConfigID:  m.ConfigID,
		TriggerID: m.TriggerID,
		VaultID:   m.VaultID,
		StorageID: m.StorageID,
		Type:      m.Type,
		StartTime: m.StartTime,
		EndTime:   m.EndTime,
		Status:    int(m.Status),
		FileSize:  m.FileSize,
		FileCount: m.FileCount,
		Message:   m.Message,
		FilePath:  m.FilePath,
		Password:  m.Password,
		CreatedAt: time.Time(m.CreatedAt),
		UpdatedAt: time.Time(m.UpdatedAt),
	}
}

func historyToModel(d *domain.BackupHistory) *model.BackupHistory {
	if d == nil {
		return nil
	}
	return &model.BackupHistory{
		ID:        d.ID,
		UID:       d.UID,
		ConfigID:  d.ConfigID,
		TriggerID: d.TriggerID,
		VaultID:   d.VaultID,
		StorageID: d.StorageID,
		Type:      d.Type,
		StartTime: d.StartTime,
		EndTime:   d.EndTime,
		Status:    int64(d.Status),
		FileSize:  d.FileSize,
		FileCount: d.FileCount,
		Message:   d.Message,
		FilePath:  d.FilePath,
		Password:  d.Password,
		CreatedAt: timex.Time(d.CreatedAt),
		UpdatedAt: timex.Time(d.UpdatedAt),
	}
}

func (r *backupRepository) transformConfigSecrets(m *model.BackupConfig, transform ValueTransformer) error {
	if m == nil {
		return nil
	}
	return transformStrings(transform, &m.PasswordValue)
}

func (r *backupRepository) transformHistorySecrets(m *model.BackupHistory, transform ValueTransformer) error {
	if m == nil {
		return nil
	}
	return transformStrings(transform, &m.Password)
}

func (r *backupRepository) configFromModel(m *model.BackupConfig) (*domain.BackupConfig, error) {
	plain, err := cloneAndTransform(m, func(copy *model.BackupConfig) error {
		return r.transformConfigSecrets(copy, r.dao.DataProtector().Decrypt)
	})
	if err != nil {
		return nil, err
	}
	return configToDomain(plain), nil
}

func (r *backupRepository) historyFromModel(m *model.BackupHistory) (*domain.BackupHistory, error) {
	plain, err := cloneAndTransform(m, func(copy *model.BackupHistory) error {
		return r.transformHistorySecrets(copy, r.dao.DataProtector().Decrypt)
	})
	if err != nil {
		return nil, err
	}
	return historyToDomain(plain), nil
}

func (r *backupRepository) GetByID(ctx context.Context, id, uid int64) (*domain.BackupConfig, error) {
	q := r.backup(uid).BackupConfig
	m, err := q.WithContext(ctx).Where(q.UID.Eq(uid), q.ID.Eq(id)).First()
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return r.configFromModel(m)
}

func (r *backupRepository) DeleteConfig(ctx context.Context, id, uid int64) error {
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.backup(uid).BackupConfig
		// Limit to UID for safety
		_, err := q.WithContext(ctx).Where(q.UID.Eq(uid), q.ID.Eq(id)).Delete()
		return err
	})
}

func (r *backupRepository) ListConfigs(ctx context.Context, uid int64) ([]*domain.BackupConfig, error) {
	q := r.backup(uid).BackupConfig
	configs, err := q.WithContext(ctx).Where(q.UID.Eq(uid)).Order(q.ID.Desc()).Find()
	if err != nil {
		return nil, err
	}
	var result []*domain.BackupConfig
	for _, m := range configs {
		item, err := r.configFromModel(m)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, nil
}

func (r *backupRepository) SaveConfig(ctx context.Context, config *domain.BackupConfig, uid int64) (*domain.BackupConfig, error) {
	var result *domain.BackupConfig
	err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.backup(uid).BackupConfig
		m := configToModel(config)
		if err := r.transformConfigSecrets(m, r.dao.DataProtector().Encrypt); err != nil {
			return err
		}
		m.UID = uid

		// If ID > 0, execute update logic
		// 如果 ID > 0，执行更新逻辑
		if config.ID > 0 {
			// Check if ID belongs to the current user
			// 检查 ID 是否属于当前用户
			old, err := q.WithContext(ctx).Where(q.UID.Eq(uid), q.ID.Eq(config.ID)).First()
			if err != nil {
				return err // RecordNotFound or other error
			}
			m.CreatedAt = old.CreatedAt
			m.UpdatedAt = timex.Now()
			// Update columns
			if err := q.WithContext(ctx).Save(m); err != nil {
				return err
			}
		} else {
			// ID == 0, execute create new config logic
			// ID == 0，执行创建新配置逻辑
			m.CreatedAt = timex.Now()
			m.UpdatedAt = timex.Now()
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

// Modify interface definition to support calls without UID (if ID is included in Config)
// 修改接口定义以支持无 UID 调用 (如果 ID 包含在 Config 中)
func (r *backupRepository) CreateHistory(ctx context.Context, history *domain.BackupHistory, uid int64) (*domain.BackupHistory, error) {
	var result *domain.BackupHistory
	err := r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.backup(uid).BackupHistory
		m := historyToModel(history)
		if err := r.transformHistorySecrets(m, r.dao.DataProtector().Encrypt); err != nil {
			return err
		}
		m.UID = uid
		m.CreatedAt = timex.Now()
		m.UpdatedAt = timex.Now()
		if err := q.WithContext(ctx).Save(m); err != nil {
			return err
		}
		saved := *history
		saved.ID = m.ID
		saved.UID = m.UID
		saved.CreatedAt = time.Time(m.CreatedAt)
		saved.UpdatedAt = time.Time(m.UpdatedAt)
		result = &saved
		return nil
	})
	return result, err
}

func (r *backupRepository) ListHistory(ctx context.Context, uid int64, configID int64, page, pageSize int) ([]*domain.BackupHistory, int64, error) {
	q := r.backup(uid).BackupHistory
	offset := (page - 1) * pageSize
	modelList, count, err := q.WithContext(ctx).Where(q.UID.Eq(uid), q.ConfigID.Eq(configID)).Order(q.ID.Desc()).FindByPage(offset, pageSize)
	if err != nil {
		return nil, 0, err
	}

	var list []*domain.BackupHistory
	for _, m := range modelList {
		item, err := r.historyFromModel(m)
		if err != nil {
			return nil, 0, err
		}
		list = append(list, item)
	}
	return list, count, nil
}

func (r *backupRepository) ListOldHistory(ctx context.Context, uid int64, configID int64, cutoffTime time.Time) ([]*domain.BackupHistory, error) {
	q := r.backup(uid).BackupHistory
	modelList, err := q.WithContext(ctx).Where(q.ConfigID.Eq(configID), q.UID.Eq(uid), q.CreatedAt.Lt(timex.Time(cutoffTime))).Find()
	if err != nil {
		return nil, err
	}

	var list []*domain.BackupHistory
	for _, m := range modelList {
		item, err := r.historyFromModel(m)
		if err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, nil
}

func (r *backupRepository) DeleteOldHistory(ctx context.Context, uid int64, configID int64, cutoffTime time.Time) error {
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.backup(uid).BackupHistory
		// Delete history records created before cutoffTime
		_, err := q.WithContext(ctx).Where(q.ConfigID.Eq(configID), q.UID.Eq(uid), q.CreatedAt.Lt(timex.Time(cutoffTime))).Delete()
		return err
	})
}

func (r *backupRepository) ListOldHistoryForStorage(ctx context.Context, uid, configID, storageID int64, cutoffTime time.Time) ([]*domain.BackupHistory, error) {
	q := r.backup(uid).BackupHistory
	items, err := q.WithContext(ctx).Where(q.UID.Eq(uid), q.ConfigID.Eq(configID), q.StorageID.Eq(storageID), q.CreatedAt.Lt(timex.Time(cutoffTime))).Find()
	if err != nil {
		return nil, err
	}
	result := make([]*domain.BackupHistory, 0, len(items))
	for _, item := range items {
		value, err := r.historyFromModel(item)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (r *backupRepository) DeleteOldHistoryForStorage(ctx context.Context, uid, configID, storageID int64, cutoffTime time.Time) error {
	return r.dao.ExecuteWrite(ctx, uid, r, func(db *gorm.DB) error {
		q := r.backup(uid).BackupHistory
		_, err := q.WithContext(ctx).Where(q.UID.Eq(uid), q.ConfigID.Eq(configID), q.StorageID.Eq(storageID), q.CreatedAt.Lt(timex.Time(cutoffTime))).Delete()
		return err
	})
}

// Ensure backupRepository implements domain.BackupRepository interface
// 确保 backupRepository 实现了 domain.BackupRepository 接口
var _ domain.BackupRepository = (*backupRepository)(nil)
