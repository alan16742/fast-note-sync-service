package upgrade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"gorm.io/gorm"
)

const release370Version = "3.7.0"

// Release370Migrate is the single 3.7.0 release migration. The automation
// migration was already part of the unreleased 3.7.0 change set; keeping both
// operations under one version avoids registering two migrations with the same
// unique schema-version key.
type Release370Migrate struct{}

func (m *Release370Migrate) Version() string { return release370Version }

func (m *Release370Migrate) Description() string {
	return "Encrypt sensitive database values and convert legacy automation settings"
}

func (m *Release370Migrate) Up(db *gorm.DB, ctx context.Context, mc *MigrationContext) error {
	if err := (&SensitiveDataMigrate{}).Up(db, ctx, mc); err != nil {
		return err
	}
	return (&AutomationRuleMigrate{}).Up(db, ctx, mc)
}

// SensitiveDataMigrate converts the pre-3.7.0 plaintext credential columns to
// authenticated ciphertext. It uses the existing migration/version framework;
// the per-user database schema_version row is written in the same transaction
// as that database's data update. This is necessary because a deployment may
// route different model tables to separate physical databases.
type SensitiveDataMigrate struct{}

func (m *SensitiveDataMigrate) Version() string { return release370Version }

func (m *SensitiveDataMigrate) Description() string {
	return "Encrypt sensitive database values"
}

func (m *SensitiveDataMigrate) Up(_ *gorm.DB, ctx context.Context, mc *MigrationContext) error {
	if mc == nil || mc.Dao == nil {
		return errors.New("dao is nil in sensitive data migration context")
	}
	protector := mc.DataProtector
	if protector == nil {
		protector = mc.Dao.DataProtector()
	}
	if !protector.Configured() {
		return util.ErrDataEncryptionKeyRequired
	}

	uids, err := mc.Dao.GetAllUserUIDs()
	if err != nil {
		return fmt.Errorf("failed to get all user UIDs: %w", err)
	}
	for _, uid := range uids {
		userDatabaseType := strings.ToLower(strings.TrimSpace(mc.UserDatabaseType))
		if userDatabaseType == "" {
			userDatabaseType = strings.ToLower(strings.TrimSpace(mc.DatabaseType))
		}
		if userDatabaseType == "mysql" || userDatabaseType == "postgres" {
			if err := migrateSensitiveDatabase(ctx, mc, uid, fmt.Sprintf("user_backup_%d", uid), protector, migrateAllSensitiveValues); err != nil {
				return err
			}
			continue
		}

		for _, database := range []struct {
			key    string
			update func(*gorm.DB, *dao.DataProtector) error
		}{
			{key: fmt.Sprintf("user_backup_%d", uid), update: migrateBackupSensitiveValues},
			{key: fmt.Sprintf("user_git_sync_%d", uid), update: migrateGitSensitiveValues},
			{key: fmt.Sprintf("user_storage_%d", uid), update: migrateStorageSensitiveValues},
			{key: fmt.Sprintf("user_webhook_%d", uid), update: migrateWebhookSensitiveValues},
		} {
			if err := migrateSensitiveDatabase(ctx, mc, uid, database.key, protector, database.update); err != nil {
				return err
			}
		}
	}
	return nil
}

func migrateAllSensitiveValues(db *gorm.DB, protector *dao.DataProtector) error {
	if err := migrateBackupSensitiveValues(db, protector); err != nil {
		return err
	}
	if err := migrateGitSensitiveValues(db, protector); err != nil {
		return err
	}
	if err := migrateStorageSensitiveValues(db, protector); err != nil {
		return err
	}
	return migrateWebhookSensitiveValues(db, protector)
}

func migrateSensitiveDatabase(ctx context.Context, mc *MigrationContext, uid int64, key string, protector *dao.DataProtector, update func(*gorm.DB, *dao.DataProtector) error) error {
	db := mc.Dao.ResolveDB(key)
	if db == nil {
		return fmt.Errorf("cannot open sensitive data database %s for uid %d", key, uid)
	}
	if err := db.WithContext(ctx).AutoMigrate(&SchemaVersion{}); err != nil {
		return fmt.Errorf("failed to create migration history in %s: %w", key, err)
	}

	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var applied SchemaVersion
		err := tx.WithContext(ctx).Where("version = ?", release370Version).First(&applied).Error
		if err == nil {
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("failed to read migration history in %s: %w", key, err)
		}

		if err := update(tx, protector); err != nil {
			return fmt.Errorf("failed to encrypt sensitive values in %s: %w", key, err)
		}
		record := &SchemaVersion{
			Version:     release370Version,
			Description: (&SensitiveDataMigrate{}).Description(),
			AppliedAt:   time.Now(),
		}
		if err := tx.WithContext(ctx).Create(record).Error; err != nil {
			return fmt.Errorf("failed to record migration history in %s: %w", key, err)
		}
		return nil
	})
}

func encryptMigrationValue(protector *dao.DataProtector, value string) (string, error) {
	if value == "" {
		return "", nil
	}
	return protector.Encrypt(value)
}

func migrateBackupSensitiveValues(db *gorm.DB, protector *dao.DataProtector) error {
	if db.Migrator().HasTable("backup_config") {
		if !db.Migrator().HasColumn("backup_config", "password_value") {
			return errors.New("backup_config.password_value column is missing")
		}
		var configs []struct {
			ID            int64
			PasswordValue string
		}
		if err := db.Table("backup_config").Select("id, password_value").Find(&configs).Error; err != nil {
			return err
		}
		for _, config := range configs {
			value, err := encryptMigrationValue(protector, config.PasswordValue)
			if err != nil {
				return err
			}
			if err := db.Table("backup_config").Where("id = ?", config.ID).Update("password_value", value).Error; err != nil {
				return err
			}
		}
	}

	if !db.Migrator().HasTable("backup_history") {
		return nil
	}
	if !db.Migrator().HasColumn("backup_history", "password") {
		return errors.New("backup_history.password column is missing")
	}
	var histories []struct {
		ID       int64
		Password string
	}
	if err := db.Table("backup_history").Select("id, password").Find(&histories).Error; err != nil {
		return err
	}
	for _, history := range histories {
		value, err := encryptMigrationValue(protector, history.Password)
		if err != nil {
			return err
		}
		if err := db.Table("backup_history").Where("id = ?", history.ID).Update("password", value).Error; err != nil {
			return err
		}
	}
	return nil
}

func migrateGitSensitiveValues(db *gorm.DB, protector *dao.DataProtector) error {
	if !db.Migrator().HasTable("git_sync_config") {
		return nil
	}
	if !db.Migrator().HasColumn("git_sync_config", "password") {
		return errors.New("git_sync_config.password column is missing")
	}
	var configs []struct {
		ID       int64
		Password string
	}
	if err := db.Table("git_sync_config").Select("id, password").Find(&configs).Error; err != nil {
		return err
	}
	for _, config := range configs {
		value, err := encryptMigrationValue(protector, config.Password)
		if err != nil {
			return err
		}
		if err := db.Table("git_sync_config").Where("id = ?", config.ID).Update("password", value).Error; err != nil {
			return err
		}
	}
	return nil
}

func migrateStorageSensitiveValues(db *gorm.DB, protector *dao.DataProtector) error {
	if !db.Migrator().HasTable("storage") {
		return nil
	}
	if !db.Migrator().HasColumn("storage", "access_key_secret") || !db.Migrator().HasColumn("storage", "password") {
		return errors.New("storage protected credential column is missing")
	}
	var storages []struct {
		ID              int64
		AccessKeySecret string
		Password        string
	}
	if err := db.Table("storage").Select("id, access_key_secret, password").Find(&storages).Error; err != nil {
		return err
	}
	for _, storage := range storages {
		accessKeySecret, err := encryptMigrationValue(protector, storage.AccessKeySecret)
		if err != nil {
			return err
		}
		password, err := encryptMigrationValue(protector, storage.Password)
		if err != nil {
			return err
		}
		if err := db.Table("storage").Where("id = ?", storage.ID).Updates(map[string]any{
			"access_key_secret": accessKeySecret,
			"password":          password,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func migrateWebhookSensitiveValues(db *gorm.DB, protector *dao.DataProtector) error {
	if !db.Migrator().HasTable("notification_channel") {
		return nil
	}
	if !db.Migrator().HasColumn("notification_channel", "url") ||
		!db.Migrator().HasColumn("notification_channel", "secret") ||
		!db.Migrator().HasColumn("notification_channel", "headers") {
		return errors.New("notification_channel protected column is missing")
	}
	var subscriptions []struct {
		ID      int64
		URL     string
		Secret  string
		Headers string
	}
	if err := db.Table("notification_channel").Select("id, url, secret, headers").Find(&subscriptions).Error; err != nil {
		return err
	}
	for _, subscription := range subscriptions {
		endpoint, err := encryptMigrationValue(protector, subscription.URL)
		if err != nil {
			return err
		}
		secret, err := encryptMigrationValue(protector, subscription.Secret)
		if err != nil {
			return err
		}

		headers := make(map[string]string)
		if strings.TrimSpace(subscription.Headers) != "" {
			if err := json.Unmarshal([]byte(subscription.Headers), &headers); err != nil {
				return fmt.Errorf("decode notification headers for id %d: %w", subscription.ID, err)
			}
		}
		for key, value := range headers {
			if util.IsSensitiveHeaderName(key) {
				value, err = encryptMigrationValue(protector, value)
				if err != nil {
					return err
				}
				headers[key] = value
			}
		}
		headerData, err := json.Marshal(headers)
		if err != nil {
			return err
		}
		if err := db.Table("notification_channel").Where("id = ?", subscription.ID).Updates(map[string]any{
			"url":     endpoint,
			"secret":  secret,
			"headers": string(headerData),
		}).Error; err != nil {
			return err
		}
	}
	return nil
}
