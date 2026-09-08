package dto

import "github.com/haierkeys/fast-note-sync-service/internal/domain"

// WebhookSubscriptionRequest creates or updates a user's webhook subscription.
type WebhookSubscriptionRequest struct {
	ID            int64                  `json:"id" form:"id"`
	Enabled       bool                   `json:"enabled" form:"enabled"`
	Provider      string                 `json:"provider" form:"provider"`
	Mode          string                 `json:"mode" form:"mode"`
	Timezone      string                 `json:"timezone" form:"timezone"`
	URL           string                 `json:"url" form:"url"`
	Method        string                 `json:"method" form:"method"`
	Headers       map[string]string      `json:"headers" form:"headers"`
	Secret        string                 `json:"secret" form:"secret"`
	VaultID       int64                  `json:"vaultId" form:"vaultId"`
	Actions       []domain.WebhookAction `json:"actions" form:"actions"`
	PathPrefix    string                 `json:"pathPrefix" form:"pathPrefix"`
	PathGlob      string                 `json:"pathGlob" form:"pathGlob"`
	BodySubstring string                 `json:"bodySubstring" form:"bodySubstring"`
	BodyRegex     string                 `json:"bodyRegex" form:"bodyRegex"`
	BodyMaxBytes  int                    `json:"bodyMaxBytes" form:"bodyMaxBytes"`
	TitleTemplate string                 `json:"titleTemplate" form:"titleTemplate"`
	BodyTemplate  string                 `json:"bodyTemplate" form:"bodyTemplate"`
}

// WebhookSubscriptionDTO is the safe API representation; the secret is never returned.
type WebhookSubscriptionDTO struct {
	ID            int64                  `json:"id"`
	UID           int64                  `json:"uid"`
	Enabled       bool                   `json:"enabled"`
	Provider      string                 `json:"provider"`
	Mode          string                 `json:"mode"`
	Timezone      string                 `json:"timezone"`
	URL           string                 `json:"url"`
	Method        string                 `json:"method"`
	Headers       map[string]string      `json:"headers"`
	HasSecret     bool                   `json:"hasSecret"`
	VaultID       int64                  `json:"vaultId"`
	Actions       []domain.WebhookAction `json:"actions"`
	PathPrefix    string                 `json:"pathPrefix"`
	PathGlob      string                 `json:"pathGlob"`
	BodySubstring string                 `json:"bodySubstring"`
	BodyRegex     string                 `json:"bodyRegex"`
	BodyMaxBytes  int                    `json:"bodyMaxBytes"`
	TitleTemplate string                 `json:"titleTemplate"`
	BodyTemplate  string                 `json:"bodyTemplate"`
	CreatedAt     string                 `json:"createdAt"`
	UpdatedAt     string                 `json:"updatedAt"`
}
