package model

import "github.com/haierkeys/fast-note-sync-service/pkg/timex"

const TableNameAutomationTrigger = "automation_trigger"

// AutomationTrigger is the persisted event condition and target binding.
// JSON columns are used for the small, user-owned lists so this table can be
// migrated without introducing a second relation for every target binding.
type AutomationTrigger struct {
	ID              int64      `gorm:"column:id;primaryKey"`
	UID             int64      `gorm:"column:uid;not null;index:idx_automation_trigger_uid"`
	Name            string     `gorm:"column:name;type:varchar(120);not null;default:''"`
	Enabled         int64      `gorm:"column:enabled;not null;default:0;index:idx_automation_trigger_enabled"`
	EventType       string     `gorm:"column:event_type;type:varchar(32);not null;index:idx_automation_trigger_type"`
	VaultID         int64      `gorm:"column:vault_id;not null;default:0"`
	Timezone        string     `gorm:"column:timezone;type:varchar(64);not null;default:'Asia/Shanghai'"`
	Schedule        string     `gorm:"column:schedule;type:TEXT;not null;default:''"`
	ContentContains string     `gorm:"column:content_contains;type:TEXT;not null;default:''"`
	PathPrefix      string     `gorm:"column:path_prefix;type:TEXT;not null;default:''"`
	PathGlob        string     `gorm:"column:path_glob;type:TEXT;not null;default:''"`
	EventActions    string     `gorm:"column:event_actions;type:TEXT;not null;default:'[]'"`
	Actions         string     `gorm:"column:actions;type:TEXT;not null;default:'[]'"`
	LastRunAt       int64      `gorm:"column:last_run_at;not null;default:0"`
	CreatedAt       timex.Time `gorm:"column:created_at;default:NULL;autoCreateTime:false"`
	UpdatedAt       timex.Time `gorm:"column:updated_at;default:NULL;autoUpdateTime:false"`
}

func (*AutomationTrigger) TableName() string { return TableNameAutomationTrigger }
