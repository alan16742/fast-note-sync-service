package upgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/model"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// AutomationRuleMigrate converts the pre-automation auto-run settings into
// automation_rule entries. Those columns (backup_config.is_enabled/cron_*,
// git_sync_config.is_enabled/delay) were dropped from the runtime without a
// migration, so upgraded installs silently lost their scheduled backups and
// auto Git syncs, with no UI switch left to restore them.
// AutomationRuleMigrate 将自动化重构前的自动运行设置转换为 automation_rule 记录。
// 这些列（backup_config.is_enabled/cron_*、git_sync_config.is_enabled/delay）
// 在无迁移的情况下被移除，导致升级后的实例定时备份与自动 Git 同步静默失效，
// 且 UI 已无开关可恢复。
type AutomationRuleMigrate struct{}

// legacyNotifyUpdatedActions mirrors the actions the pre-automation runtime
// forwarded to backup and Git sync. Delete/rename/restore fired NotifyUpdated
// too, so subscribing to create/modify alone would silently drop them.
// legacyNotifyUpdatedActions 对应重构前运行时会转发给备份与 Git 同步的操作。
// 删除/重命名/恢复同样会触发 NotifyUpdated，只订阅 create/modify 会让它们静默丢失。
var legacyNotifyUpdatedActions = []string{"create", "modify", "delete", "rename", "restore"}

// Version identifies the release introducing automation and the legacy settings
// migration. It must be newer than the last release containing the old columns.
func (m *AutomationRuleMigrate) Version() string {
	return "3.7.0"
}

// Description returns the migration description
// Description 返回升级描述
func (m *AutomationRuleMigrate) Description() string {
	return "Convert legacy backup_config/git_sync_config auto-run settings into automation_rule entries"
}

// legacyBackupConfigRow mirrors the dropped backup_config columns.
type legacyBackupConfigRow struct {
	ID             int64
	VaultID        int64
	Type           string
	IsEnabled      int64
	CronStrategy   string
	CronExpression string
}

// legacyGitSyncConfigRow mirrors the dropped git_sync_config columns.
type legacyGitSyncConfigRow struct {
	ID        int64
	VaultID   int64
	IsEnabled int64
}

// Up runs the migration
// Up 执行升级操作
func (m *AutomationRuleMigrate) Up(_ *gorm.DB, ctx context.Context, mc *MigrationContext) error {
	if mc.Dao == nil {
		return fmt.Errorf("dao is nil in migration context")
	}

	uids, err := mc.Dao.GetAllUserUIDs()
	if err != nil {
		return fmt.Errorf("failed to get all user UIDs: %w", err)
	}

	for _, uid := range uids {
		automationDB := mc.Dao.ResolveDB("user_automation_" + fmt.Sprintf("%d", uid))
		if automationDB == nil {
			return fmt.Errorf("cannot open automation database for uid %d", uid)
		}
		if err := automationDB.WithContext(ctx).AutoMigrate(&model.AutomationTrigger{}); err != nil {
			return fmt.Errorf("failed to migrate automation_rule table for uid %d: %w", uid, err)
		}

		// The manager's main-database transaction cannot roll back user databases.
		// Keep the two legacy target conversions atomic within this user's database.
		if err := automationDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := m.migrateBackupConfigs(mc, tx, ctx, uid); err != nil {
				return err
			}
			return m.migrateGitSyncConfigs(mc, tx, ctx, uid)
		}); err != nil {
			return err
		}
	}

	return nil
}

func (m *AutomationRuleMigrate) migrateBackupConfigs(mc *MigrationContext, automationDB *gorm.DB, ctx context.Context, uid int64) error {
	backupDB := mc.Dao.ResolveDB("user_backup_" + fmt.Sprintf("%d", uid))
	if backupDB == nil {
		return fmt.Errorf("cannot open backup database for uid %d", uid)
	}
	if !backupDB.Migrator().HasTable("backup_config") || !backupDB.Migrator().HasColumn("backup_config", "is_enabled") {
		return nil
	}

	var rows []legacyBackupConfigRow
	if err := backupDB.WithContext(ctx).Table("backup_config").
		Select("id, vault_id, type, is_enabled, cron_strategy, cron_expression").
		Where("is_enabled = ?", 1).Find(&rows).Error; err != nil {
		return fmt.Errorf("failed to read legacy backup_config for uid %d: %w", uid, err)
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	for _, row := range rows {
		if row.VaultID <= 0 {
			mc.Logger.Warn("legacy backup config has no vault, skipping", zap.Int64("uid", uid), zap.Int64("configId", row.ID))
			continue
		}
		var events []domain.AutomationEventRule
		if row.Type == "sync" {
			// Legacy behavior: debounced sync on every note change.
			// 旧行为：每次笔记变更后防抖执行同步。
			events = legacySyncEvents()
		} else {
			schedule := legacyCronSchedule(row.CronStrategy, row.CronExpression)
			if schedule == "" {
				continue
			}
			if _, err := parser.Parse(schedule); err != nil {
				mc.Logger.Warn("legacy backup cron expression is invalid, skipping",
					zap.Int64("uid", uid), zap.Int64("configId", row.ID), zap.String("schedule", schedule), zap.Error(err))
				continue
			}
			events = []domain.AutomationEventRule{{Type: domain.AutomationEventCron, Schedule: schedule}}
		}

		name := fmt.Sprintf("Migrated backup #%d", row.ID)
		if err := m.insertRule(automationDB, ctx, uid, row.VaultID, name, events, domain.AutomationAction{Type: domain.AutomationTargetBackup, ConfigID: row.ID}); err != nil {
			return err
		}
		mc.Logger.Info("migrated legacy backup config to automation rule",
			zap.Int64("uid", uid), zap.Int64("configId", row.ID), zap.String("type", row.Type))
	}
	return nil
}

func (m *AutomationRuleMigrate) migrateGitSyncConfigs(mc *MigrationContext, automationDB *gorm.DB, ctx context.Context, uid int64) error {
	gitDB := mc.Dao.ResolveDB("user_git_sync_" + fmt.Sprintf("%d", uid))
	if gitDB == nil {
		return fmt.Errorf("cannot open git sync database for uid %d", uid)
	}
	if !gitDB.Migrator().HasTable("git_sync_config") || !gitDB.Migrator().HasColumn("git_sync_config", "is_enabled") {
		return nil
	}

	var rows []legacyGitSyncConfigRow
	if err := gitDB.WithContext(ctx).Table("git_sync_config").
		Select("id, vault_id, is_enabled").
		Where("is_enabled = ?", 1).Find(&rows).Error; err != nil {
		return fmt.Errorf("failed to read legacy git_sync_config for uid %d: %w", uid, err)
	}

	for _, row := range rows {
		if row.VaultID <= 0 {
			mc.Logger.Warn("legacy git sync config has no vault, skipping", zap.Int64("uid", uid), zap.Int64("configId", row.ID))
			continue
		}
		// Legacy behavior: debounced sync on every note change. The delay has no
		// automation equivalent, so note changes dispatch directly.
		// 旧行为：笔记变更后按 delay 防抖同步；自动化无防抖等价物，变更直接派发。
		name := fmt.Sprintf("Migrated git sync #%d", row.ID)
		if err := m.insertRule(automationDB, ctx, uid, row.VaultID, name, legacySyncEvents(), domain.AutomationAction{Type: domain.AutomationTargetGit, ConfigID: row.ID}); err != nil {
			return err
		}
		mc.Logger.Info("migrated legacy git sync config to automation rule",
			zap.Int64("uid", uid), zap.Int64("configId", row.ID))
	}
	return nil
}

func legacySyncEvents() []domain.AutomationEventRule {
	return []domain.AutomationEventRule{
		{Type: domain.AutomationEventNoteContent},
		{Type: domain.AutomationEventFileBehavior, EventActions: legacyNotifyUpdatedActions},
	}
}

func (m *AutomationRuleMigrate) insertRule(automationDB *gorm.DB, ctx context.Context, uid, vaultID int64, name string, events []domain.AutomationEventRule, action domain.AutomationAction) error {
	// Legacy cron evaluated time.Now() in the server's local timezone.
	timezone := time.Local.String()
	var existing int64
	if err := automationDB.WithContext(ctx).Model(&model.AutomationTrigger{}).
		Where("uid = ? AND vault_id = ? AND name = ?", uid, vaultID, name).Count(&existing).Error; err != nil {
		return fmt.Errorf("failed to check existing migrated rule for uid %d: %w", uid, err)
	}
	if existing > 0 {
		return nil
	}
	eventsJSON, err := json.Marshal(events)
	if err != nil {
		return err
	}
	actions, err := json.Marshal([]domain.AutomationAction{action})
	if err != nil {
		return err
	}
	now := timex.Now()
	rule := &model.AutomationTrigger{
		UID: uid, Name: name, Enabled: 1, VaultID: vaultID,
		Timezone: timezone, MatchMode: string(domain.AutomationMatchAny),
		Events: string(eventsJSON), Actions: string(actions),
		CreatedAt: now, UpdatedAt: now,
	}
	if err := automationDB.WithContext(ctx).Create(rule).Error; err != nil {
		return fmt.Errorf("failed to insert automation rule for uid %d: %w", uid, err)
	}
	return nil
}

// legacyCronSchedule maps the dropped backup_config.cron_strategy values to the
// cron expressions the old runtime used (see legacy calculateNextRunTime).
func legacyCronSchedule(strategy, expression string) string {
	switch strategy {
	case "daily":
		return "0 0 * * *"
	case "weekly":
		return "0 0 * * 0"
	case "monthly":
		return "0 0 1 * *"
	case "custom", "cron":
		return expression
	default:
		return ""
	}
}
