package domain

import (
	"context"
	"time"
)

// AutomationExecutionStatus is the lifecycle state of one trigger execution.
type AutomationExecutionStatus string

const (
	AutomationExecutionPending   AutomationExecutionStatus = "pending"
	AutomationExecutionRunning   AutomationExecutionStatus = "running"
	AutomationExecutionSucceeded AutomationExecutionStatus = "succeeded"
	AutomationExecutionFailed    AutomationExecutionStatus = "failed"
	AutomationExecutionCancelled AutomationExecutionStatus = "cancelled"
)

// AutomationActionExecution records the result of one target within an
// AutomationExecution. Keeping this at action granularity makes partial
// success visible and gives a future retry worker a stable starting point.
type AutomationActionExecution struct {
	Type       string
	ConfigID   int64
	Status     AutomationExecutionStatus
	Error      string
	StartedAt  time.Time
	FinishedAt time.Time
}

// AutomationExecution is the durable audit record for one trigger/event pair.
// EventID is the idempotency key supplied by the event producer. Event contains
// the original payload only for failed executions that may be retried.
type AutomationExecution struct {
	ID         int64
	Revision   int64
	UID        int64
	TriggerID  int64
	VaultID    int64
	EventID    string
	EventType  AutomationEventType
	Event      AutomationEvent
	Status     AutomationExecutionStatus
	Error      string
	Actions    []AutomationActionExecution
	StartedAt  time.Time
	FinishedAt time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// AutomationExecutionRepository stores execution audit records in the same
// user-scoped database as automation rules.
type AutomationExecutionRepository interface {
	Start(ctx context.Context, execution *AutomationExecution) (created bool, current *AutomationExecution, err error)
	// ClaimRetry atomically changes a failed/cancelled execution to running.
	// Only one caller may claim the same execution, including callers in
	// separate service processes that share the database.
	ClaimRetry(ctx context.Context, execution *AutomationExecution) (claimed bool, err error)
	GetByID(ctx context.Context, uid, id int64) (*AutomationExecution, error)
	Update(ctx context.Context, execution *AutomationExecution) error
	List(ctx context.Context, uid, triggerID int64, page, pageSize int) ([]*AutomationExecution, int64, error)
	LatestByTrigger(ctx context.Context, uid int64) ([]*AutomationExecution, error)
	// Cleanup removes expired execution history and bounds retained terminal
	// records for every user. Active executions are not removed before expiry.
	Cleanup(ctx context.Context, before time.Time, keep int) (deleted int64, err error)
}
