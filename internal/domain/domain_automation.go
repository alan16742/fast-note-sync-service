package domain

import (
	"context"
	"time"
)

// AutomationEventType identifies one event branch in an automation trigger.
type AutomationEventType string

const (
	AutomationEventCron         AutomationEventType = "cron"
	AutomationEventNoteContent  AutomationEventType = "note_content"
	AutomationEventFileBehavior AutomationEventType = "file_behavior"
	AutomationEventTodoReminder AutomationEventType = "todo_reminder"
	AutomationEventManual       AutomationEventType = "manual"
)

// AutomationMatchMode controls how event branches are combined.
type AutomationMatchMode string

const (
	AutomationMatchAny AutomationMatchMode = "any"
	AutomationMatchAll AutomationMatchMode = "all"
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

// AutomationEventRule is one branch of a trigger. Fields not applicable to the
// branch type must be empty.
type AutomationEventRule struct {
	Type            AutomationEventType `json:"type"`
	Schedule        string              `json:"schedule,omitempty"`
	ContentContains string              `json:"contentContains,omitempty"`
	PathPrefix      string              `json:"pathPrefix,omitempty"`
	PathGlob        string              `json:"pathGlob,omitempty"`
	EventActions    []string            `json:"eventActions,omitempty"`
}

// AutomationExecutionContext carries the trigger scope into a target service.
// Target configurations deliberately do not own a notebook/vault scope.
type AutomationExecutionContext struct {
	UID        int64
	TriggerID  int64
	VaultID    int64
	EventID    string
	OccurredAt time.Time
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

// AutomationTrigger stores common scope, event branches, their match mode, and
// target references. Target-specific secrets are deliberately not part of this model.
type AutomationTrigger struct {
	ID        int64
	UID       int64
	Name      string
	Enabled   bool
	VaultID   int64
	Timezone  string
	MatchMode AutomationMatchMode
	Events    []AutomationEventRule
	Actions   []AutomationAction
	LastRunAt time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
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
