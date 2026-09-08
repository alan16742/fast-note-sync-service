package domain

import (
	"context"
	"time"
)

// WebhookResource identifies the resource represented by a webhook event.
type WebhookResource string

const (
	WebhookResourceNote WebhookResource = "note"
)

// WebhookAction identifies the business operation that changed a resource.
type WebhookAction string

const (
	WebhookActionCreate          WebhookAction = "create"
	WebhookActionModify          WebhookAction = "modify"
	WebhookActionRename          WebhookAction = "rename"
	WebhookActionDelete          WebhookAction = "delete"
	WebhookActionRestore         WebhookAction = "restore"
	WebhookActionPermanentDelete WebhookAction = "permanent_delete"
)

const (
	WebhookProviderServerChan = "serverchan"
	WebhookProviderBark       = "bark"
	WebhookProviderCustom     = "custom"
)

// ContentChangeEvent is the stable event contract shared by webhook channels.
type ContentChangeEvent struct {
	ID            string
	OccurredAt    time.Time
	UID           int64
	VaultID       int64
	VaultName     string
	Resource      WebhookResource
	Action        WebhookAction
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

// WebhookSubscription contains one user's notification channel settings.
// Event conditions belong to AutomationTrigger; this record only owns the
// provider, credentials, transport, and rendered message templates.
type WebhookSubscription struct {
	ID            int64
	UID           int64
	Enabled       bool
	Provider      string
	URL           string
	Method        string
	Headers       map[string]string
	Secret        string
	TitleTemplate string
	BodyTemplate  string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// WebhookRepository stores user-owned webhook subscriptions.
type WebhookRepository interface {
	List(ctx context.Context, uid int64) ([]*WebhookSubscription, error)
	GetByID(ctx context.Context, id, uid int64) (*WebhookSubscription, error)
	Save(ctx context.Context, subscription *WebhookSubscription, uid int64) (*WebhookSubscription, error)
	Delete(ctx context.Context, id, uid int64) error
}
