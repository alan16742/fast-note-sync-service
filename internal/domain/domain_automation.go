package domain

import (
	"context"
	"time"
)

// AutomationEventType identifies the source of an automation event.
// A trigger subscribes to one event type; target configurations remain owned by
// their respective services and are referenced by ID from AutomationAction.
type AutomationEventType string

const (
	AutomationEventTime    AutomationEventType = "time"
	AutomationEventContent AutomationEventType = "content"
	AutomationEventManual  AutomationEventType = "manual"
	AutomationEventFile    AutomationEventType = "file"
)

const (
	AutomationTargetGit     = "git"
	AutomationTargetBackup  = "backup"
	AutomationTargetWebhook = "webhook"
)

// AutomationAction binds a trigger to an existing target configuration.
// ConfigID is interpreted by the target service (Git config, backup config, or
// webhook subscription), so credentials never get copied into this record.
type AutomationAction struct {
	Type     string `json:"type"`
	ConfigID int64  `json:"configId"`
}

// AutomationEvent is the transport-neutral event contract used by all
// automation consumers.
type AutomationEvent struct {
	ID            string
	OccurredAt    time.Time
	UID           int64
	VaultID       int64
	VaultName     string
	Type          AutomationEventType
	Action        string
	Path          string
	OldPath       string
	PathHash      string
	ChangedFields []string
	Content       string
	ContentHash   string
	Size          int64
	ClientType    string
	ClientName    string
	ClientVersion string
	Source        string
}

// AutomationTrigger stores only event conditions and references to target
// configurations. Target-specific secrets are deliberately not part of this
// model.
type AutomationTrigger struct {
	ID              int64
	UID             int64
	Name            string
	Enabled         bool
	EventType       AutomationEventType
	VaultID         int64
	Timezone        string
	Schedule        string
	ContentContains string
	PathPrefix      string
	PathGlob        string
	EventActions    []string
	Actions         []AutomationAction
	LastRunAt       time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// AutomationRepository stores user-owned automation triggers.
type AutomationRepository interface {
	List(ctx context.Context, uid int64) ([]*AutomationTrigger, error)
	ListEnabled(ctx context.Context, uid int64, eventType AutomationEventType) ([]*AutomationTrigger, error)
	ListEnabledByType(ctx context.Context, eventType AutomationEventType) ([]*AutomationTrigger, error)
	GetByID(ctx context.Context, id, uid int64) (*AutomationTrigger, error)
	Save(ctx context.Context, trigger *AutomationTrigger, uid int64) (*AutomationTrigger, error)
	Delete(ctx context.Context, id, uid int64) error
	MarkRun(ctx context.Context, id, uid int64, at time.Time) error
}
