package upgrade

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type sensitiveMigrationFixture struct {
	db        *gorm.DB
	dao       *dao.Dao
	encryptor *util.DataEncryptor
}

func newSensitiveMigrationFixture(t *testing.T) *sensitiveMigrationFixture {
	t.Helper()
	ctx := context.Background()
	queue := false
	cfg := &config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(t.TempDir(), "db.sqlite3"), EnableWriteQueue: &queue}
	db, err := dao.NewEngine(*cfg, zap.NewNop())
	require.NoError(t, err)
	encryptor, err := util.NewDataEncryptor("sensitive migration test key-fixture-0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	d := dao.New(db, ctx, dao.WithConfig(cfg), dao.WithUserDatabaseConfig(cfg), dao.WithLogger(zap.NewNop()), dao.WithDataEncryptor(encryptor))
	require.NoError(t, db.AutoMigrate(&model.User{}))
	require.NoError(t, db.Create(&model.User{UID: 1}).Error)

	for _, key := range []string{"user_backup_1", "user_git_sync_1", "user_storage_1", "user_webhook_1"} {
		userDB := d.ResolveDB(key)
		require.NotNil(t, userDB)
		switch key {
		case "user_backup_1":
			require.NoError(t, userDB.AutoMigrate(&model.BackupConfig{}, &model.BackupHistory{}))
		case "user_git_sync_1":
			require.NoError(t, userDB.AutoMigrate(&model.GitSyncConfig{}))
		case "user_storage_1":
			require.NoError(t, userDB.AutoMigrate(&model.Storage{}))
		case "user_webhook_1":
			require.NoError(t, userDB.AutoMigrate(&model.WebhookSubscription{}))
		}
	}
	t.Cleanup(func() {
		d.CleanupConnections(-1)
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return &sensitiveMigrationFixture{db: db, dao: d, encryptor: encryptor}
}

func seedSensitivePlaintext(t *testing.T, fixture *sensitiveMigrationFixture) {
	t.Helper()
	backupDB := fixture.dao.ResolveDB("user_backup_1")
	require.NoError(t, backupDB.Create(&model.BackupConfig{UID: 1, PasswordValue: "backup-password"}).Error)
	require.NoError(t, backupDB.Create(&model.BackupHistory{UID: 1, Password: "history-password"}).Error)

	gitDB := fixture.dao.ResolveDB("user_git_sync_1")
	require.NoError(t, gitDB.Create(&model.GitSyncConfig{UID: 1, RepoURL: "https://example.com/repo.git", Password: "git-password"}).Error)

	storageDB := fixture.dao.ResolveDB("user_storage_1")
	require.NoError(t, storageDB.Create(&model.Storage{UID: 1, Type: "webdav", AccessKeySecret: "storage-access-secret", Password: "storage-password"}).Error)

	headers, err := json.Marshal(map[string]string{"Authorization": "Bearer webhook-header-secret", "X-Source": "fast-note-sync"})
	require.NoError(t, err)
	webhookDB := fixture.dao.ResolveDB("user_webhook_1")
	require.NoError(t, webhookDB.Create(&model.WebhookSubscription{UID: 1, Provider: "custom", URL: "https://example.com/hook?token=query-secret", Secret: "webhook-secret", Headers: string(headers)}).Error)
}

func sensitiveMigrationContext(fixture *sensitiveMigrationFixture) *MigrationContext {
	return &MigrationContext{
		Logger:           zap.NewNop(),
		DatabaseType:     "sqlite",
		UserDatabaseType: "sqlite",
		Dao:              fixture.dao,
		DataProtector:    fixture.dao.DataProtector(),
	}
}

func TestSensitiveDataMigrateEncryptsAllFieldsAndIsVersionIdempotent(t *testing.T) {
	fixture := newSensitiveMigrationFixture(t)
	seedSensitivePlaintext(t, fixture)
	migration := &SensitiveDataMigrate{}
	ctx := context.Background()
	require.NoError(t, migration.Up(fixture.db, ctx, sensitiveMigrationContext(fixture)))

	backupDB := fixture.dao.ResolveDB("user_backup_1")
	var backupConfig model.BackupConfig
	var backupHistory model.BackupHistory
	require.NoError(t, backupDB.First(&backupConfig).Error)
	require.NoError(t, backupDB.First(&backupHistory).Error)
	assertEncryptedValue(t, fixture.encryptor, backupConfig.PasswordValue, "backup-password")
	assertEncryptedValue(t, fixture.encryptor, backupHistory.Password, "history-password")

	gitDB := fixture.dao.ResolveDB("user_git_sync_1")
	var gitConfig model.GitSyncConfig
	require.NoError(t, gitDB.First(&gitConfig).Error)
	assertEncryptedValue(t, fixture.encryptor, gitConfig.Password, "git-password")

	storageDB := fixture.dao.ResolveDB("user_storage_1")
	var storage model.Storage
	require.NoError(t, storageDB.First(&storage).Error)
	assertEncryptedValue(t, fixture.encryptor, storage.AccessKeySecret, "storage-access-secret")
	assertEncryptedValue(t, fixture.encryptor, storage.Password, "storage-password")

	webhookDB := fixture.dao.ResolveDB("user_webhook_1")
	var webhookRow model.WebhookSubscription
	require.NoError(t, webhookDB.First(&webhookRow).Error)
	assertEncryptedValue(t, fixture.encryptor, webhookRow.URL, "https://example.com/hook?token=query-secret")
	assertEncryptedValue(t, fixture.encryptor, webhookRow.Secret, "webhook-secret")
	var headers map[string]string
	require.NoError(t, json.Unmarshal([]byte(webhookRow.Headers), &headers))
	assertEncryptedValue(t, fixture.encryptor, headers["Authorization"], "Bearer webhook-header-secret")
	require.Equal(t, "fast-note-sync", headers["X-Source"])

	backupConfigCiphertext := backupConfig.PasswordValue
	webhookCiphertext := webhookRow.Secret
	require.NoError(t, migration.Up(fixture.db, ctx, sensitiveMigrationContext(fixture)))
	var backupConfigAgain model.BackupConfig
	var webhookAgain model.WebhookSubscription
	require.NoError(t, backupDB.First(&backupConfigAgain).Error)
	require.NoError(t, webhookDB.First(&webhookAgain).Error)
	require.Equal(t, backupConfigCiphertext, backupConfigAgain.PasswordValue, "versioned migration must not re-encrypt")
	require.Equal(t, webhookCiphertext, webhookAgain.Secret, "versioned migration must not re-encrypt")

	for _, key := range []string{"user_backup_1", "user_git_sync_1", "user_storage_1", "user_webhook_1"} {
		var count int64
		require.NoError(t, fixture.dao.ResolveDB(key).Model(&SchemaVersion{}).Where("version = ?", migration.Version()).Count(&count).Error)
		require.EqualValues(t, 1, count, "migration history for %s", key)
	}

	backupRepo := dao.NewBackupRepository(fixture.dao)
	loadedBackup, err := backupRepo.GetByID(ctx, backupConfig.ID, 1)
	require.NoError(t, err)
	require.Equal(t, "backup-password", loadedBackup.PasswordValue)
	gitRepo := dao.NewGitSyncRepository(fixture.dao)
	loadedGit, err := gitRepo.GetByID(ctx, gitConfig.ID, 1)
	require.NoError(t, err)
	require.Equal(t, "git-password", loadedGit.Password)
	storageRepo := dao.NewStorageRepository(fixture.dao)
	loadedStorage, err := storageRepo.GetByID(ctx, storage.ID, 1)
	require.NoError(t, err)
	require.Equal(t, "storage-password", loadedStorage.Password)
	webhookRepo := dao.NewWebhookRepository(fixture.dao)
	loadedWebhook, err := webhookRepo.GetByID(ctx, webhookRow.ID, 1)
	require.NoError(t, err)
	require.Equal(t, "webhook-secret", loadedWebhook.Secret)
	require.Equal(t, "Bearer webhook-header-secret", loadedWebhook.Headers["Authorization"])
}

func TestSensitiveDataMigrateRollsBackFailedDatabaseAndDoesNotRecordVersion(t *testing.T) {
	fixture := newSensitiveMigrationFixture(t)
	seedSensitivePlaintext(t, fixture)
	webhookDB := fixture.dao.ResolveDB("user_webhook_1")
	require.NoError(t, webhookDB.Exec(`CREATE TRIGGER fail_notification_update BEFORE UPDATE ON notification_channel BEGIN SELECT RAISE(ABORT, 'forced migration failure'); END`).Error)

	err := (&SensitiveDataMigrate{}).Up(fixture.db, context.Background(), sensitiveMigrationContext(fixture))
	require.Error(t, err)
	var row model.WebhookSubscription
	require.NoError(t, webhookDB.First(&row).Error)
	require.Equal(t, "https://example.com/hook?token=query-secret", row.URL)
	require.Equal(t, "webhook-secret", row.Secret)
	var versionCount int64
	require.NoError(t, webhookDB.Model(&SchemaVersion{}).Where("version = ?", release370Version).Count(&versionCount).Error)
	require.Zero(t, versionCount)

	require.NoError(t, webhookDB.Exec(`DROP TRIGGER fail_notification_update`).Error)
	require.NoError(t, (&SensitiveDataMigrate{}).Up(fixture.db, context.Background(), sensitiveMigrationContext(fixture)))
	require.NoError(t, webhookDB.First(&row).Error)
	assertEncryptedValue(t, fixture.encryptor, row.Secret, "webhook-secret")
}

func TestRelease370MigrationIsRecordedOnceByMigrationManager(t *testing.T) {
	t.Chdir(t.TempDir())
	require.NoError(t, os.Mkdir("config", 0o755))
	require.NoError(t, os.WriteFile("config/lastVersion", []byte("3.6.1"), 0o644))

	fixture := newSensitiveMigrationFixture(t)
	seedSensitivePlaintext(t, fixture)
	// The manager must use the fixture's already-open main database while its
	// config still describes the same user-database routing.
	cfg := fixture.dao.Config()
	manager := NewMigrationManager(fixture.db, zap.NewNop(), release370Version, cfg, cfg, fixture.encryptor)
	require.NoError(t, manager.Run(context.Background()))

	var count int64
	require.NoError(t, fixture.db.Model(&SchemaVersion{}).Where("version = ?", release370Version).Count(&count).Error)
	require.EqualValues(t, 1, count)
	webhookDB := fixture.dao.ResolveDB("user_webhook_1")
	var row model.WebhookSubscription
	require.NoError(t, webhookDB.First(&row).Error)
	secretCiphertext := row.Secret

	require.NoError(t, manager.Run(context.Background()))
	require.NoError(t, fixture.db.Model(&SchemaVersion{}).Where("version = ?", release370Version).Count(&count).Error)
	require.EqualValues(t, 1, count)
	require.NoError(t, webhookDB.First(&row).Error)
	require.Equal(t, secretCiphertext, row.Secret)
}

func assertEncryptedValue(t *testing.T, encryptor *util.DataEncryptor, value, expected string) {
	t.Helper()
	require.NotEqual(t, expected, value)
	plaintext, err := encryptor.Decrypt(value)
	require.NoError(t, err)
	require.Equal(t, expected, plaintext)
}
