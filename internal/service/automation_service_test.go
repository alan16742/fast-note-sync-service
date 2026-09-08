package service

import (
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
)

func TestAutomationTriggerMatchesConditions(t *testing.T) {
	trigger := &domain.AutomationTrigger{
		Enabled: true, EventType: domain.AutomationEventContent, VaultID: 7,
		EventActions: []string{"modify"}, ContentContains: "release",
		PathGlob: "Projects/*.md",
	}

	matching := &domain.AutomationEvent{
		Type: domain.AutomationEventContent, VaultID: 7, Action: "modify",
		Path: "Projects/release.md", Content: "prepare release notes",
	}
	if !automationTriggerMatches(trigger, matching) {
		t.Fatal("expected content event to match trigger")
	}

	matching.Content = "meeting notes"
	if automationTriggerMatches(trigger, matching) {
		t.Fatal("content matcher should reject unrelated content")
	}

	matching.Content = "prepare release notes"
	matching.VaultID = 8
	if automationTriggerMatches(trigger, matching) {
		t.Fatal("vault matcher should reject another vault")
	}
}

func TestAutomationTriggerMatchesUnrestrictedFileEvent(t *testing.T) {
	trigger := &domain.AutomationTrigger{Enabled: true, EventType: domain.AutomationEventFile}
	if !automationTriggerMatches(trigger, &domain.AutomationEvent{
		Type: domain.AutomationEventFile, VaultID: 1, Action: "delete", Path: "assets/image.png",
	}) {
		t.Fatal("expected unrestricted file trigger to match")
	}
}

func TestAutomationTriggerMatchesRenameByOldOrNewPath(t *testing.T) {
	trigger := &domain.AutomationTrigger{
		Enabled: true, EventType: domain.AutomationEventFile, PathGlob: "Projects/*.md",
	}
	if !automationTriggerMatches(trigger, &domain.AutomationEvent{
		Type: domain.AutomationEventFile, Action: "rename", Path: "Archive/notes.txt", OldPath: "Projects/notes.md",
	}) {
		t.Fatal("rename should match either the old or new path")
	}
}

func TestAutomationFromRequestValidatesTimeAndTargets(t *testing.T) {
	trigger, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "nightly git", Enabled: true, EventType: domain.AutomationEventTime,
		Timezone: "Asia/Shanghai", Schedule: "0 2 * * *",
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetGit, ConfigID: 4}},
	}, 9)
	if err != nil {
		t.Fatalf("valid trigger rejected: %v", err)
	}
	if trigger.EventType != domain.AutomationEventTime || trigger.Actions[0].ConfigID != 4 {
		t.Fatalf("unexpected trigger: %#v", trigger)
	}

	if _, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "broken", EventType: domain.AutomationEventTime, Schedule: "not cron",
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetBackup, ConfigID: 1}},
	}, 9); err == nil {
		t.Fatal("invalid cron should be rejected")
	}
}

func TestAutomationFromRequestTodoKeepsTriggerConditionsAndOnlyNotificationTargets(t *testing.T) {
	trigger, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "todo reminders", Enabled: true, EventType: domain.AutomationEventTodo,
		VaultID: 3, Timezone: "UTC", ContentContains: "@(", PathGlob: "Projects/*.md",
		EventActions: []string{"create"},
		Actions:      []dto.AutomationActionDTO{{Type: domain.AutomationTargetWebhook, ConfigID: 8}},
	}, 9)
	if err != nil {
		t.Fatalf("valid todo trigger rejected: %v", err)
	}
	if trigger.Timezone != "UTC" || trigger.ContentContains != "@(" || trigger.PathGlob != "Projects/*.md" {
		t.Fatalf("todo trigger conditions were not preserved: %#v", trigger)
	}
	if len(trigger.EventActions) != 0 || len(trigger.Actions) != 1 || trigger.Actions[0].Type != domain.AutomationTargetWebhook {
		t.Fatalf("unexpected todo trigger routing: %#v", trigger)
	}

	if _, err := automationFromRequest(&dto.AutomationTriggerRequest{
		Name: "invalid todo", Enabled: true, EventType: domain.AutomationEventTodo,
		Actions: []dto.AutomationActionDTO{{Type: domain.AutomationTargetBackup, ConfigID: 1}},
	}, 9); err == nil {
		t.Fatal("todo trigger should reject non-notification targets")
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
		if event.Type != domain.AutomationEventFile || event.Action != test.want {
			t.Fatalf("action %q mapped to %#v, want %q", test.action, event, test.want)
		}
	}
}
