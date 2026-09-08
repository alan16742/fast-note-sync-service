package model

type ReminderJob struct {
	ID             int64  `gorm:"primaryKey"`
	UID            int64  `gorm:"not null;uniqueIndex:idx_reminder_task;index:idx_reminder_due"`
	SubscriptionID int64  `gorm:"not null;uniqueIndex:idx_reminder_task;index:idx_reminder_due"`
	NoteID         int64  `gorm:"not null;uniqueIndex:idx_reminder_task"`
	TaskKey        string `gorm:"type:varchar(80);not null;uniqueIndex:idx_reminder_task"`
	Schedule       string `gorm:"type:TEXT;not null"`
	Active         bool   `gorm:"not null;index:idx_reminder_due"`
	NextAt         int64  `gorm:"not null;index:idx_reminder_due"`
	OccurrenceAt   int64  `gorm:"not null"`
	RetryAt        int64  `gorm:"not null;default:0"`
	Attempts       int    `gorm:"not null;default:0"`
	LeaseUntil     int64  `gorm:"not null;default:0"`
	ClaimToken     string `gorm:"type:varchar(36);not null;default:''"`
}

func (*ReminderJob) TableName() string { return "reminder_job" }
