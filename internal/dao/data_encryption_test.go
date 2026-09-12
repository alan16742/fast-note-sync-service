package dao

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newProtectedDataTestDAO(t *testing.T) (*Dao, *util.DataEncryptor) {
	t.Helper()
	queue := false
	cfg := config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(t.TempDir(), "db.sqlite3"), EnableWriteQueue: &queue}
	db, err := NewEngine(cfg, zap.NewNop())
	require.NoError(t, err)
	encryptor, err := util.NewDataEncryptor("test protected database key-fixture-0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	d := New(db, context.Background(), WithConfig(&cfg), WithUserDatabaseConfig(&cfg), WithLogger(zap.NewNop()), WithDataEncryptor(encryptor))
	t.Cleanup(func() {
		for _, key := range []string{"user_storage_1", "user_git_sync_1", "user_backup_1", "user_webhook_1"} {
			if entry := d.ResolveDB(key); entry != nil {
				if sqlDB, err := entry.DB(); err == nil {
					_ = sqlDB.Close()
				}
			}
		}
	})
	return d, encryptor
}

func TestProtectedRepositoriesEncryptSensitiveValuesAtRest(t *testing.T) {
	d, encryptor := newProtectedDataTestDAO(t)
	ctx := context.Background()

	storageRepo := NewStorageRepository(d)
	storageResult, err := storageRepo.Create(ctx, &domain.Storage{Type: "webdav", User: "user", Password: "storage-password", AccessKeySecret: "access-secret"}, 1)
	require.NoError(t, err)
	require.Equal(t, "storage-password", storageResult.Password)
	require.Equal(t, "access-secret", storageResult.AccessKeySecret)
	var storageRow model.Storage
	require.NoError(t, d.ResolveDB("user_storage_1").Table("storage").First(&storageRow).Error)
	require.NotEqual(t, "storage-password", storageRow.Password)
	require.NotEqual(t, "access-secret", storageRow.AccessKeySecret)
	password, err := encryptor.Decrypt(storageRow.Password)
	require.NoError(t, err)
	require.Equal(t, "storage-password", password)
	rawStoragePassword := storageRow.Password
	rawStorageSecret := storageRow.AccessKeySecret
	decodedStorage, err := storageRepo.(*storageRepository).fromModel(&storageRow)
	require.NoError(t, err)
	require.Equal(t, "storage-password", decodedStorage.Password)
	require.Equal(t, rawStoragePassword, storageRow.Password, "read transform must not mutate the model")
	require.Equal(t, rawStorageSecret, storageRow.AccessKeySecret, "read transform must not mutate the model")

	gitRepo := NewGitSyncRepository(d)
	gitResult, err := gitRepo.Save(ctx, &domain.GitSyncConfig{RepoURL: "https://example.com/repo.git", Password: "git-password"}, 1)
	require.NoError(t, err)
	require.Equal(t, "git-password", gitResult.Password)
	var gitRow model.GitSyncConfig
	require.NoError(t, d.ResolveDB("user_git_sync_1").Table("git_sync_config").First(&gitRow).Error)
	require.NotEqual(t, "git-password", gitRow.Password)
	rawGitPassword := gitRow.Password
	decodedGit, err := gitRepo.(*gitSyncRepository).configFromModel(&gitRow)
	require.NoError(t, err)
	require.Equal(t, "git-password", decodedGit.Password)
	require.Equal(t, rawGitPassword, gitRow.Password, "read transform must not mutate the model")

	backupRepo := NewBackupRepository(d)
	backupResult, err := backupRepo.SaveConfig(ctx, &domain.BackupConfig{Type: "full", PasswordMode: 1, PasswordValue: "backup-password"}, 1)
	require.NoError(t, err)
	require.Equal(t, "backup-password", backupResult.PasswordValue)
	var backupRow model.BackupConfig
	require.NoError(t, d.ResolveDB("user_backup_1").Table("backup_config").First(&backupRow).Error)
	require.NotEqual(t, "backup-password", backupRow.PasswordValue)
	rawBackupPassword := backupRow.PasswordValue
	decodedBackup, err := backupRepo.(*backupRepository).configFromModel(&backupRow)
	require.NoError(t, err)
	require.Equal(t, "backup-password", decodedBackup.PasswordValue)
	require.Equal(t, rawBackupPassword, backupRow.PasswordValue, "read transform must not mutate the model")
	historyResult, err := backupRepo.CreateHistory(ctx, &domain.BackupHistory{ConfigID: backupResult.ID, Password: "history-password"}, 1)
	require.NoError(t, err)
	require.Equal(t, "history-password", historyResult.Password)
	var historyRow model.BackupHistory
	require.NoError(t, d.ResolveDB("user_backup_1").Table("backup_history").First(&historyRow).Error)
	require.NotEqual(t, "history-password", historyRow.Password)
	rawHistoryPassword := historyRow.Password
	decodedHistory, err := backupRepo.(*backupRepository).historyFromModel(&historyRow)
	require.NoError(t, err)
	require.Equal(t, "history-password", decodedHistory.Password)
	require.Equal(t, rawHistoryPassword, historyRow.Password, "read transform must not mutate the model")

	webhookRepo := NewWebhookRepository(d)
	webhookResult, err := webhookRepo.Save(ctx, &domain.WebhookSubscription{
		Provider: "custom", URL: "https://example.com/hook", Method: "POST",
		Headers: map[string]string{"Authorization": "Bearer header-secret", "X-Source": "fast-note-sync"}, Secret: "channel-secret",
	}, 1)
	require.NoError(t, err)
	require.Equal(t, "channel-secret", webhookResult.Secret)
	require.Equal(t, "Bearer header-secret", webhookResult.Headers["Authorization"])
	var webhookRow model.WebhookSubscription
	require.NoError(t, d.ResolveDB("user_webhook_1").Table("notification_channel").First(&webhookRow).Error)
	require.NotEqual(t, "https://example.com/hook", webhookRow.URL)
	require.NotEqual(t, "channel-secret", webhookRow.Secret)
	var storedHeaders map[string]string
	require.NoError(t, json.Unmarshal([]byte(webhookRow.Headers), &storedHeaders))
	require.NotEqual(t, "Bearer header-secret", storedHeaders["Authorization"])
	decryptedHeader, err := encryptor.Decrypt(storedHeaders["Authorization"])
	require.NoError(t, err)
	require.Equal(t, "Bearer header-secret", decryptedHeader)
	require.Equal(t, "fast-note-sync", storedHeaders["X-Source"])
	rawWebhookSecret := webhookRow.Secret
	rawWebhookHeaders := webhookRow.Headers
	decodedWebhook, err := webhookRepo.(*webhookRepository).fromModel(&webhookRow)
	require.NoError(t, err)
	require.Equal(t, "channel-secret", decodedWebhook.Secret)
	require.Equal(t, "Bearer header-secret", decodedWebhook.Headers["Authorization"])
	require.Equal(t, rawWebhookSecret, webhookRow.Secret, "read transform must not mutate the model")
	require.Equal(t, rawWebhookHeaders, webhookRow.Headers, "read transform must not mutate the model")

	loaded, err := webhookRepo.GetByID(ctx, 1, 1)
	require.NoError(t, err)
	require.Equal(t, "channel-secret", loaded.Secret)
	require.Equal(t, "Bearer header-secret", loaded.Headers["Authorization"])
}

func TestProtectedRepositoriesRejectUnexpectedPlaintext(t *testing.T) {
	d, _ := newProtectedDataTestDAO(t)
	ctx := context.Background()
	repo := NewWebhookRepository(d)

	_, err := repo.Save(ctx, &domain.WebhookSubscription{Provider: "custom", URL: "https://example.com/hook", Headers: map[string]string{"Authorization": "stored-secret"}, Secret: "stored-channel"}, 1)
	require.NoError(t, err)
	db := d.ResolveDB("user_webhook_1")
	require.NoError(t, db.Model(&model.WebhookSubscription{}).Where("id = ?", 1).Updates(map[string]any{
		"url":     "https://example.com/hook",
		"secret":  "unexpected-plaintext",
		"headers": `{"Authorization":"unexpected-header-plaintext"}`,
	}).Error)

	_, err = repo.GetByID(ctx, 1, 1)
	require.ErrorIs(t, err, util.ErrInvalidDataCiphertext)
}
