package upgrade

import "testing"

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
