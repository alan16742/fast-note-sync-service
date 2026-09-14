package model

import "github.com/haierkeys/fast-note-sync-service/pkg/timex"

const TableNameAutomationExecution = "automation_execution"

// AutomationExecution stores one trigger/event execution and its action
// results. The unique index is scoped to uid + trigger_id + event_id: one
// event may legitimately match multiple triggers, while each trigger/event
// pair must be idempotent. Actions are JSON because their target-specific
// details are small and the execution table is an audit surface rather than a
// query boundary.
type AutomationExecution struct {
	ID         int64      `gorm:"column:id;primaryKey"`
	UID        int64      `gorm:"column:uid;not null;index:idx_automation_execution_uid_created,priority:1;uniqueIndex:idx_automation_execution_event,priority:1"`
	TriggerID  int64      `gorm:"column:trigger_id;not null;index:idx_automation_execution_trigger;uniqueIndex:idx_automation_execution_event,priority:2"`
	VaultID    int64      `gorm:"column:vault_id;not null;index:idx_automation_execution_vault"`
	EventID    string     `gorm:"column:event_id;type:varchar(80);not null;uniqueIndex:idx_automation_execution_event,priority:3"`
	EventType  string     `gorm:"column:event_type;type:varchar(32);not null;index:idx_automation_execution_type"`
	Event      string     `gorm:"column:event;type:TEXT;not null;default:''"`
	Status     string     `gorm:"column:status;type:varchar(16);not null;index:idx_automation_execution_status"`
	Error      string     `gorm:"column:error;type:TEXT;not null;default:''"`
	Actions    string     `gorm:"column:actions;type:TEXT;not null;default:'[]'"`
	StartedAt  timex.Time `gorm:"column:started_at;default:NULL"`
	FinishedAt timex.Time `gorm:"column:finished_at;default:NULL"`
	CreatedAt  timex.Time `gorm:"column:created_at;index:idx_automation_execution_uid_created,priority:2;default:NULL;autoCreateTime:false"`
	UpdatedAt  timex.Time `gorm:"column:updated_at;default:NULL;autoUpdateTime:false"`
}

func (*AutomationExecution) TableName() string { return TableNameAutomationExecution }
