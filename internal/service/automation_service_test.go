package service

import (
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
)

func TestAutomationTriggerMatchesOrEventBranches(t *testing.T) {
	trigger := &domain.AutomationTrigger{
		Enabled: true, VaultID: 7,
		Events: []domain.AutomationEventRule{
			{Type: domain.AutomationEventNoteContent, ContentContains: "release", EventActions: []string{"modify"}},
			{Type: domain.AutomationEventFileBehavior, PathGlob: "Projects/*.md", EventActions: []string{"create"}},
		},
	}
	if !automationTriggerMatches(trigger, &domain.AutomationEvent{
		Type: domain.AutomationEventNoteContent, VaultID: 7, Action: "modify", Content: "prepare release notes",
	}) {
		t.Fatal("expected note content branch to match")
	}
	if !automationTriggerMatches(trigger, &domain.AutomationEvent{
		Type: domain.AutomationEventFileBehavior, VaultID: 7, Action: "create", Path: "Projects/readme.md",
	}) {
		t.Fatal("expected file behavior branch to match")
	}
	if automationTriggerMatches(trigger, &domain.AutomationEvent{
		Type: domain.AutomationEventNoteContent, VaultID: 7, Action: "modify", Content: "meeting notes",
	}) {
		t.Fatal("unmatched branch should not trigger")
	}
}

func TestAutomationTriggerMatchesRenameByOldOrNewPath(t *testing.T) {
	trigger := &domain.AutomationTrigger{
		Enabled: true, VaultID: 1,
		Events: []domain.AutomationEventRule{{Type: domain.AutomationEventFileBehavior, PathGlob: "Projects/*.md", EventActions: []string{"rename"}}},
	}
	if !automationTriggerMatches(trigger, &domain.AutomationEvent{
		Type: domain.AutomationEventFileBehavior, VaultID: 1, Action: "rename", Path: "Archive/notes.txt", OldPath: "Projects/notes.md",
	}) {
		t.Fatal("rename should match either the old or new path")
	}
}

func TestAutomationTriggerMatchesAllBranchesAndNeverDefaultsActions(t *testing.T) {
	trigger := &domain.AutomationTrigger{
		Enabled: true, VaultID: 1, MatchMode: domain.AutomationMatchAll,
		Events: []domain.AutomationEventRule{
			{Type: domain.AutomationEventNoteContent, ContentContains: "release", EventActions: []string{"modify"}},
			{Type: domain.AutomationEventNoteContent, ContentContains: "notes", EventActions: []string{"modify"}},
		},
	}
	if !automationTriggerMatches(trigger, &domain.AutomationEvent{
		Type: domain.AutomationEventNoteContent, VaultID: 1, Action: "modify", Content: "release notes",
	}) {
		t.Fatal("expected all note conditions to match")
	}
	if automationTriggerMatches(trigger, &domain.AutomationEvent{
		Type: domain.AutomationEventNoteContent, VaultID: 1, Action: "modify", Content: "release draft",
	}) {
		t.Fatal("all mode should require every branch")
	}
	if automationTriggerMatches(&domain.AutomationTrigger{
		Enabled: true, VaultID: 1,
		Events: []domain.AutomationEventRule{{Type: domain.AutomationEventNoteContent, ContentContains: "release"}},
	}, &domain.AutomationEvent{Type: domain.AutomationEventNoteContent, VaultID: 1, Action: "modify", Content: "release"}) {
		t.Fatal("an empty action selection must not match every action")
	}
}

func TestAutomationFromRequestValidatesBranchesAndTargets(t *testing.T) {
	trigger, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "nightly git", Enabled: true, VaultID: 9, Timezone: "Asia/Shanghai",
		Events:  []dto.AutomationEventRuleDTO{{Type: domain.AutomationEventCron, Schedule: "0 2 * * *"}},
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetGit, ConfigID: 4}},
	}, 9)
	if err != nil {
		t.Fatalf("valid trigger rejected: %v", err)
	}
	if trigger.Events[0].Type != domain.AutomationEventCron || trigger.Actions[0].ConfigID != 4 {
		t.Fatalf("unexpected trigger: %#v", trigger)
	}

	if _, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "broken", Enabled: true, VaultID: 9,
		Events:  []dto.AutomationEventRuleDTO{{Type: domain.AutomationEventCron, Schedule: "not cron"}},
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetBackup, ConfigID: 1}},
	}, 9); err == nil {
		t.Fatal("invalid cron should be rejected")
	}
}

func TestAutomationTodoBranchOnlyAllowsNotificationTargets(t *testing.T) {
	trigger, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "todo reminders", Enabled: true, VaultID: 3, Timezone: "UTC",
		Events:  []dto.AutomationEventRuleDTO{{Type: domain.AutomationEventTodoReminder}},
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetWebhook, ConfigID: 8}},
	}, 9)
	if err != nil {
		t.Fatalf("valid todo trigger rejected: %v", err)
	}
	if trigger.Timezone != "UTC" || len(trigger.Events) != 1 || len(trigger.Actions) != 1 {
		t.Fatalf("unexpected todo trigger: %#v", trigger)
	}

	if _, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "invalid todo", Enabled: true, VaultID: 3,
		Events:  []dto.AutomationEventRuleDTO{{Type: domain.AutomationEventTodoReminder}},
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetBackup, ConfigID: 1}},
	}, 9); err == nil {
		t.Fatal("todo trigger should reject non-notification targets")
	}
}

func TestAutomationFromRequestRequiresExplicitActionsAndValidMatchMode(t *testing.T) {
	if _, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "all note conditions", Enabled: true, VaultID: 1, MatchMode: "all",
		Events: []dto.AutomationEventRuleDTO{
			{Type: domain.AutomationEventNoteContent, ContentContains: "a", EventActions: []string{"modify"}},
			{Type: domain.AutomationEventNoteContent, ContentContains: "b", EventActions: []string{"modify"}},
		},
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetWebhook, ConfigID: 1}},
	}, 1); err != nil {
		t.Fatalf("valid all-mode trigger rejected: %v", err)
	}
	if _, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "missing actions", Enabled: true, VaultID: 1,
		Events:  []dto.AutomationEventRuleDTO{{Type: domain.AutomationEventFileBehavior, PathGlob: "*.md"}},
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetWebhook, ConfigID: 1}},
	}, 1); err == nil {
		t.Fatal("file behavior should require explicit action selections")
	}
	if _, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "mixed all-mode", Enabled: true, VaultID: 1, MatchMode: "all",
		Events: []dto.AutomationEventRuleDTO{
			{Type: domain.AutomationEventCron, Schedule: "0 * * * *"},
			{Type: domain.AutomationEventManual},
		},
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetWebhook, ConfigID: 1}},
	}, 1); err == nil {
		t.Fatal("all-mode should reject mixed event types")
	}
}

func TestAutomationEventFromSyncLogNormalizesFileActions(t *testing.T) {
	for _, test := range []struct {
		action domain.SyncLogAction
		want   string
	}{
		{domain.SyncLogActionCreate, "create"},
		{domain.SyncLogActionModify, "modify"},
		{domain.SyncLogActionSoftDelete, "delete"},
		{domain.SyncLogActionDelete, "permanent_delete"},
	} {
		event := automationEventFromSyncLog(&domain.SyncLog{UID: 1, VaultID: 2, Action: test.action, Path: "a.bin"})
		if event.Type != domain.AutomationEventFileBehavior || event.Action != test.want {
			t.Fatalf("action %q mapped to %#v, want %q", test.action, event, test.want)
		}
	}
}

// TestAutomationEventFromSyncLogPreservesOldPathOnRename guards the rename path:
// the source path must reach the derived event, otherwise a rule scoped to the
// folder a resource is moved out of silently stops matching.
// TestAutomationEventFromSyncLogPreservesOldPathOnRename 保护重命名链路：源路径必须传递到
// 派生事件，否则限定在资源被移出目录上的规则会静默失去匹配。
func TestAutomationEventFromSyncLogPreservesOldPathOnRename(t *testing.T) {
	event := automationEventFromSyncLog(&domain.SyncLog{
		UID: 1, VaultID: 2, Action: domain.SyncLogActionRename,
		Path: "Archive/notes.md", OldPath: "Projects/notes.md",
	})
	if event.Action != "rename" {
		t.Fatalf("action = %q, want %q", event.Action, "rename")
	}
	if event.OldPath != "Projects/notes.md" {
		t.Fatalf("OldPath = %q, want the pre-rename path", event.OldPath)
	}

	rule := &domain.AutomationTrigger{
		Enabled: true, VaultID: 2,
		Events: []domain.AutomationEventRule{{
			Type: domain.AutomationEventFileBehavior, EventActions: []string{"rename"}, PathPrefix: "Projects/",
		}},
	}
	if !automationTriggerMatches(rule, event) {
		t.Fatal("rule scoped to the source path did not match the rename")
	}

	// Without the source path the same rule must not match — the regression this
	// test exists to prevent.
	// 缺少源路径时同一规则不应匹配——这正是本测试要防止的回归。
	event.OldPath = ""
	if automationTriggerMatches(rule, event) {
		t.Fatal("rule scoped to the source path matched even though no OldPath was set")
	}
}
