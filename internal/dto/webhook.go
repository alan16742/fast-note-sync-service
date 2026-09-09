package dto

// WebhookSubscriptionRequest creates or updates a user's webhook subscription.
type WebhookSubscriptionRequest struct {
	ID            int64             `json:"id" form:"id"`
	Provider      string            `json:"provider" form:"provider"`
	URL           string            `json:"url" form:"url"`
	Method        string            `json:"method" form:"method"`
	Headers       map[string]string `json:"headers" form:"headers"`
	Secret        string            `json:"secret" form:"secret"`
	TitleTemplate string            `json:"titleTemplate" form:"titleTemplate"`
	BodyTemplate  string            `json:"bodyTemplate" form:"bodyTemplate"`
}

// WebhookSubscriptionDTO is the safe API representation; the secret is never returned.
type WebhookSubscriptionDTO struct {
	ID            int64             `json:"id"`
	UID           int64             `json:"uid"`
	Provider      string            `json:"provider"`
	URL           string            `json:"url"`
	Method        string            `json:"method"`
	Headers       map[string]string `json:"headers"`
	HasSecret     bool              `json:"hasSecret"`
	TitleTemplate string            `json:"titleTemplate"`
	BodyTemplate  string            `json:"bodyTemplate"`
	CreatedAt     string            `json:"createdAt"`
	UpdatedAt     string            `json:"updatedAt"`
}
