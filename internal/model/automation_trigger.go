package model

import "github.com/haierkeys/fast-note-sync-service/pkg/timex"

const TableNameAutomationTrigger = "automation_rule"

// AutomationTrigger is the persisted common scope, event branches, and target
// bindings.
type AutomationTrigger struct {
	ID            int64      `gorm:"column:id;primaryKey"`
	UID           int64      `gorm:"column:uid;not null;index:idx_automation_rule_uid"`
	Name          string     `gorm:"column:name;type:varchar(120);not null;default:''"`
	Enabled       int64      `gorm:"column:enabled;not null;default:0;index:idx_automation_rule_enabled"`
	VaultID       int64      `gorm:"column:vault_id;not null"`
	Timezone      string     `gorm:"column:timezone;type:varchar(64);not null;default:'Asia/Shanghai'"`
	MatchMode     string     `gorm:"column:match_mode;type:varchar(8);not null;default:'any'"`
	Events        string     `gorm:"column:events;type:TEXT;not null;default:'[]'"`
	Actions       string     `gorm:"column:actions;type:TEXT;not null;default:'[]'"`
	LastRunAt     int64      `gorm:"column:last_run_at;not null;default:0"`
	LastAttemptAt int64      `gorm:"column:last_attempt_at;not null;default:0"`
	CreatedAt     timex.Time `gorm:"column:created_at;default:NULL;autoCreateTime:false"`
	UpdatedAt     timex.Time `gorm:"column:updated_at;default:NULL;autoUpdateTime:false"`
}

func (*AutomationTrigger) TableName() string { return TableNameAutomationTrigger }
