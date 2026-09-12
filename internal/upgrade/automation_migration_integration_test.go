package upgrade

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func automationMigrationFixture(t *testing.T) (*gorm.DB, *config.DatabaseConfig, *MigrationContext) {
	t.Helper()
	cfg := &config.DatabaseConfig{
		Type:              "sqlite",
		Path:              filepath.Join(t.TempDir(), "db.sqlite3"),
		DataEncryptionKey: "automation migration test key-0123456789abcdef",
	}
	db, err := dao.NewEngine(*cfg, zap.NewNop())
	require.NoError(t, err)
	d := dao.New(db, context.Background(), dao.WithConfig(cfg), dao.WithLogger(zap.NewNop()))
	t.Cleanup(func() {
		d.CleanupConnections(-time.Second)
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})
	require.NoError(t, db.AutoMigrate(&model.User{}))
	require.NoError(t, db.Create(&model.User{UID: 1}).Error)
	backup := d.ResolveDB("user_backup_1")
	require.NotNil(t, backup)
	require.NoError(t, backup.Exec(`CREATE TABLE backup_config (
		id INTEGER PRIMARY KEY, uid INTEGER, vault_id INTEGER, type TEXT,
		is_enabled INTEGER, cron_strategy TEXT, cron_expression TEXT)`).Error)
	git := d.ResolveDB("user_git_sync_1")
	require.NotNil(t, git)
	require.NoError(t, git.Exec(`CREATE TABLE git_sync_config (
		id INTEGER PRIMARY KEY, uid INTEGER, vault_id INTEGER, is_enabled INTEGER)`).Error)
	return db, cfg, &MigrationContext{Dao: d, Logger: zap.NewNop()}
}

func TestAutomationMigrationConvertsLegacyTargetsAndIsIdempotent(t *testing.T) {
	db, _, mc := automationMigrationFixture(t)
	ctx := context.Background()
	backup := mc.Dao.ResolveDB("user_backup_1")
	require.NoError(t, backup.Exec(`INSERT INTO backup_config VALUES
		(1,1,7,'sync',1,'',''), (2,1,7,'full',1,'daily',''),
		(3,1,7,'sync',0,'',''), (4,1,7,'full',1,'custom','invalid')`).Error)
	require.NoError(t, mc.Dao.ResolveDB("user_git_sync_1").Exec(`INSERT INTO git_sync_config VALUES (1,1,7,1)`).Error)
	rulesDB := mc.Dao.ResolveDB("user_automation_1")
	require.NoError(t, rulesDB.AutoMigrate(&model.AutomationTrigger{}))
	migration := &AutomationRuleMigrate{}
	require.NoError(t, migration.Up(db, ctx, mc))
	var rows []model.AutomationTrigger
	require.NoError(t, rulesDB.Order("id").Find(&rows).Error)
	require.Len(t, rows, 3)
	var backupEvents, gitEvents []domain.AutomationEventRule
	for _, row := range rows {
		var events []domain.AutomationEventRule
		require.NoError(t, json.Unmarshal([]byte(row.Events), &events))
		switch row.Name {
		case "Migrated backup #1":
			backupEvents = events
		case "Migrated git sync #1":
			gitEvents = events
		case "Migrated backup #2":
			require.Equal(t, time.Local.String(), row.Timezone)
			require.Equal(t, []domain.AutomationEventRule{{Type: domain.AutomationEventCron, Schedule: "0 0 * * *"}}, events)
		default:
			t.Fatalf("unexpected migrated rule: %+v", row)
		}
	}
	require.Equal(t, legacySyncEvents(), backupEvents)
	require.Equal(t, legacySyncEvents(), gitEvents)
	require.NoError(t, migration.Up(db, ctx, mc))
	var count int64
	require.NoError(t, rulesDB.Model(&model.AutomationTrigger{}).Count(&count).Error)
	require.EqualValues(t, 3, count)
}

func TestAutomationMigrationRollsBackUserOnFailure(t *testing.T) {
	db, _, mc := automationMigrationFixture(t)
	require.NoError(t, mc.Dao.ResolveDB("user_backup_1").Exec(`INSERT INTO backup_config VALUES (1,1,7,'sync',1,'','')`).Error)
	git := mc.Dao.ResolveDB("user_git_sync_1")
	// Force the second target read to fail after the first rule was inserted.
	require.NoError(t, git.Exec(`ALTER TABLE git_sync_config RENAME COLUMN vault_id TO broken_vault_id`).Error)
	migration := &AutomationRuleMigrate{}
	require.Error(t, migration.Up(db, context.Background(), mc))
	var count int64
	rulesDB := mc.Dao.ResolveDB("user_automation_1")
	require.NoError(t, rulesDB.Model(&model.AutomationTrigger{}).Count(&count).Error)
	require.Zero(t, count)
	require.NoError(t, git.Exec(`ALTER TABLE git_sync_config RENAME COLUMN broken_vault_id TO vault_id`).Error)
	require.NoError(t, migration.Up(db, context.Background(), mc))
	require.NoError(t, rulesDB.Model(&model.AutomationTrigger{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}

func TestMigrationManagerRunsLegacyMigrationAfterPreviousRelease(t *testing.T) {
	t.Chdir(t.TempDir())
	require.NoError(t, os.Mkdir("config", 0755))
	require.NoError(t, os.WriteFile("config/lastVersion", []byte("3.6.0"), 0644))
	db, cfg, mc := automationMigrationFixture(t)
	require.NoError(t, mc.Dao.ResolveDB("user_backup_1").Exec(`INSERT INTO backup_config VALUES (1,1,7,'sync',1,'','')`).Error)
	manager := NewMigrationManager(db, zap.NewNop(), "3.7.0", cfg, nil)
	manager.migrations = []Migration{&AutomationRuleMigrate{}}
	require.NoError(t, manager.Run(context.Background()))
	require.NoError(t, manager.Run(context.Background()))
	var count int64
	require.NoError(t, mc.Dao.ResolveDB("user_automation_1").Model(&model.AutomationTrigger{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
	require.NoError(t, db.Model(&SchemaVersion{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
	watermark, err := os.ReadFile("config/lastVersion")
	require.NoError(t, err)
	require.Equal(t, "3.7.0", string(watermark))
}
