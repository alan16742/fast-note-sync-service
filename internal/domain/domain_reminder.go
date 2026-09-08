package domain

import (
	"context"
	"github.com/haierkeys/fast-note-sync-service/pkg/reminder"
)

// ReminderJob keeps a single next delivery for a task, rather than expanding
// recurring schedules into an unbounded queue.
type ReminderJob struct {
	ID             int64
	UID            int64
	SubscriptionID int64
	NoteID         int64
	Task           reminder.Task
	NextAt         int64
	OccurrenceAt   int64
	Attempts       int
}

type ReminderRepository interface {
	SyncNote(ctx context.Context, uid, subscriptionID, noteID int64, jobs []ReminderJob) error
	ListDue(ctx context.Context, uid, subscriptionID, now int64, limit int) ([]ReminderJob, error)
	Claim(ctx context.Context, uid, id, now int64, token string) (bool, error)
	Finish(ctx context.Context, uid, id int64, token string, nextAt, occurrenceAt int64) error
	Retry(ctx context.Context, uid, id int64, token string, retryAt int64) error
	Cancel(ctx context.Context, uid, id int64, token string) error
}
