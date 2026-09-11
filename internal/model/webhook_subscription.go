package model

import "github.com/haierkeys/fast-note-sync-service/pkg/timex"

// Webhook subscriptions are stored in their own user-scoped table.
const TableNameWebhookSubscription = "notification_channel"

// WebhookSubscription is the persisted user webhook configuration.
type WebhookSubscription struct {
	ID            int64      `gorm:"column:id;primaryKey"`
	UID           int64      `gorm:"column:uid;not null;index:idx_notification_channel_uid"`
	Provider      string     `gorm:"column:provider;type:varchar(32);not null;default:'serverchan'"`
	URL           string     `gorm:"column:url;type:TEXT;not null"`
	Method        string     `gorm:"column:method;type:varchar(8);not null;default:'POST'"`
	Headers       string     `gorm:"column:headers;type:TEXT;not null;default:'{}'"`
	Secret        string     `gorm:"column:secret;type:TEXT;not null;default:''"`
	TitleTemplate string     `gorm:"column:title_template;type:TEXT;not null;default:''"`
	BodyTemplate  string     `gorm:"column:body_template;type:TEXT;not null;default:''"`
	CreatedAt     timex.Time `gorm:"column:created_at;default:NULL;autoCreateTime:false"`
	UpdatedAt     timex.Time `gorm:"column:updated_at;default:NULL;autoUpdateTime:false"`
}

func (*WebhookSubscription) TableName() string { return TableNameWebhookSubscription }
