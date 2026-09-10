package upgrade

import (
	"strings"
	"testing"

	"golang.org/x/mod/semver"
)

func TestLegacyAutomationRuleMigrateHasSeparateRepairIdentity(t *testing.T) {
	version := (&LegacyAutomationRuleMigrate{}).Version()
	if version == "" {
		t.Fatal("migration version must not be empty")
	}
	current := version
	if !strings.HasPrefix(current, "v") {
		current = "v" + current
	}
	if !semver.IsValid(current) {
		t.Fatalf("migration version %q is not valid semver", version)
	}

	key := migrationRecordKey(&LegacyAutomationRuleMigrate{})
	if key == "" || key == "3.6.0" || key == "3.7.0" || key == version {
		t.Fatalf("repair must have a distinct identity from previous incomplete migrations: %q", key)
	}
}

// TestLegacyAutomationRuleMigrate_RunsUnconditionally pins the escape hatch for
// instances that started this build before the migration shipped: their
// lastVersion already equals the running version, so without the opt-out the
// migration would be skipped forever.
// TestLegacyAutomationRuleMigrate_RunsUnconditionally 固定逃生通道：在本迁移落地前就启动
// 过该版本的实例，其 lastVersion 已等于运行版本，若不声明无视门控会被永久跳过。
func TestLegacyAutomationRuleMigrate_RunsUnconditionally(t *testing.T) {
	var migration Migration = &LegacyAutomationRuleMigrate{}
	unconditional, ok := migration.(unconditionalMigration)
	if !ok {
		t.Fatal("legacy automation migration must implement unconditionalMigration")
	}
	if !unconditional.RunsUnconditionally() {
		t.Fatal("legacy automation migration must run despite an equal lastVersion")
	}
}

func TestMigrationManager_HasPendingUnconditionalMigration(t *testing.T) {
	manager := &MigrationManager{migrations: []Migration{&LegacyAutomationRuleMigrate{}}}

	if !manager.hasPendingUnconditionalMigration(map[string]bool{}) {
		t.Fatal("an unrecorded unconditional migration must keep the version gate open")
	}
	if manager.hasPendingUnconditionalMigration(map[string]bool{migrationRecordKey(&LegacyAutomationRuleMigrate{}): true}) {
		t.Fatal("a recorded unconditional migration must not keep the version gate open")
	}

	gated := &MigrationManager{migrations: []Migration{&BackupRetentionDefaultMigrate{}}}
	if gated.hasPendingUnconditionalMigration(map[string]bool{}) {
		t.Fatal("gated migrations must never bypass the lastVersion gate")
	}
}

// TestLegacyNotifyUpdatedActionsCoverAllLegacyTriggers guards the second half of
// the migration bug: delete/rename/restore also fired NotifyUpdated, so
// subscribing to create/modify alone would silently drop them.
// TestLegacyNotifyUpdatedActionsCoverAllLegacyTriggers 保护迁移缺陷的另一半：删除/重命名/
// 恢复同样会触发 NotifyUpdated，只订阅 create/modify 会让它们静默丢失。
func TestLegacyNotifyUpdatedActionsCoverAllLegacyTriggers(t *testing.T) {
	has := func(want string) bool {
		for _, action := range legacyNotifyUpdatedActions {
			if action == want {
				return true
			}
		}
		return false
	}

	for _, want := range []string{"create", "modify", "delete", "rename", "restore"} {
		if !has(want) {
			t.Fatalf("legacy action %q is missing from %v", want, legacyNotifyUpdatedActions)
		}
	}

	// The legacy runtime never called NotifyUpdated for permanent deletion, so
	// adding it would fire rules that never fired before the upgrade.
	// 旧运行时在彻底删除时从不调用 NotifyUpdated，加入它会在升级后触发以前从不触发的规则。
	if has("permanent_delete") {
		t.Fatalf("permanent_delete must not be part of %v", legacyNotifyUpdatedActions)
	}
}

func TestLegacyCronSchedule(t *testing.T) {
	for _, test := range []struct {
		strategy   string
		expression string
		want       string
	}{
		{"daily", "", "0 0 * * *"},
		{"weekly", "", "0 0 * * 0"},
		{"monthly", "", "0 0 1 * *"},
		{"custom", "*/15 * * * *", "*/15 * * * *"},
		{"cron", "0 3 * * *", "0 3 * * *"},
		{"", "", ""},
		{"unknown", "0 3 * * *", ""},
	} {
		if got := legacyCronSchedule(test.strategy, test.expression); got != test.want {
			t.Fatalf("legacyCronSchedule(%q, %q) = %q, want %q", test.strategy, test.expression, got, test.want)
		}
	}
}
