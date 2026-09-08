package model

import "github.com/haierkeys/fast-note-sync-service/pkg/timex"

const TableNameWebhookSubscription = "webhook_subscription"

// WebhookSubscription is the persisted user webhook configuration.
type WebhookSubscription struct {
	ID            int64      `gorm:"column:id;primaryKey"`
	UID           int64      `gorm:"column:uid;not null;index:idx_webhook_subscription_uid"`
	Enabled       int64      `gorm:"column:enabled;not null;default:0"`
	Provider      string     `gorm:"column:provider;type:varchar(32);not null;default:'serverchan'"`
	Mode          string     `gorm:"column:mode;type:varchar(32);not null;default:'note_change'"`
	Timezone      string     `gorm:"column:timezone;type:varchar(64);not null;default:'Asia/Shanghai'"`
	URL           string     `gorm:"column:url;type:TEXT;not null"`
	Method        string     `gorm:"column:method;type:varchar(8);not null;default:'POST'"`
	Headers       string     `gorm:"column:headers;type:TEXT;not null;default:'{}'"`
	Secret        string     `gorm:"column:secret;type:TEXT;not null;default:''"`
	VaultID       int64      `gorm:"column:vault_id;not null;default:0"`
	Actions       string     `gorm:"column:actions;type:TEXT;not null;default:''"`
	PathPrefix    string     `gorm:"column:path_prefix;type:TEXT;not null;default:''"`
	PathGlob      string     `gorm:"column:path_glob;type:TEXT;not null;default:''"`
	BodySubstring string     `gorm:"column:body_substring;type:TEXT;not null;default:''"`
	BodyRegex     string     `gorm:"column:body_regex;type:TEXT;not null;default:''"`
	BodyMaxBytes  int64      `gorm:"column:body_max_bytes;not null;default:65536"`
	TitleTemplate string     `gorm:"column:title_template;type:TEXT;not null;default:''"`
	BodyTemplate  string     `gorm:"column:body_template;type:TEXT;not null;default:''"`
	CreatedAt     timex.Time `gorm:"column:created_at;default:NULL;autoCreateTime:false"`
	UpdatedAt     timex.Time `gorm:"column:updated_at;default:NULL;autoUpdateTime:false"`
}

func (*WebhookSubscription) TableName() string { return TableNameWebhookSubscription }
