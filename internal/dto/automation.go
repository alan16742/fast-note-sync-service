package dto

import "github.com/haierkeys/fast-note-sync-service/internal/domain"

// AutomationActionDTO binds an automation trigger to an existing target
// configuration. Secrets belong to the target configuration APIs.
type AutomationActionDTO struct {
	Type     string `json:"type"`
	ConfigID int64  `json:"configId"`
}

// AutomationTriggerRequest creates or updates an automation trigger.
type AutomationTriggerRequest struct {
	ID              int64                      `json:"id" form:"id"`
	Name            string                     `json:"name" form:"name"`
	Enabled         bool                       `json:"enabled" form:"enabled"`
	EventType       domain.AutomationEventType `json:"eventType" form:"eventType"`
	VaultID         int64                      `json:"vaultId" form:"vaultId"`
	Timezone        string                     `json:"timezone" form:"timezone"`
	Schedule        string                     `json:"schedule" form:"schedule"`
	ContentContains string                     `json:"contentContains" form:"contentContains"`
	PathPrefix      string                     `json:"pathPrefix" form:"pathPrefix"`
	PathGlob        string                     `json:"pathGlob" form:"pathGlob"`
	EventActions    []string                   `json:"eventActions" form:"eventActions"`
	Actions         []AutomationActionDTO      `json:"actions" form:"actions"`
}

// AutomationTriggerDTO is the safe API representation of an automation
// trigger. It intentionally contains no target credentials.
type AutomationTriggerDTO struct {
	ID              int64                      `json:"id"`
	UID             int64                      `json:"uid"`
	Name            string                     `json:"name"`
	Enabled         bool                       `json:"enabled"`
	EventType       domain.AutomationEventType `json:"eventType"`
	VaultID         int64                      `json:"vaultId"`
	Timezone        string                     `json:"timezone"`
	Schedule        string                     `json:"schedule"`
	ContentContains string                     `json:"contentContains"`
	PathPrefix      string                     `json:"pathPrefix"`
	PathGlob        string                     `json:"pathGlob"`
	EventActions    []string                   `json:"eventActions"`
	Actions         []AutomationActionDTO      `json:"actions"`
	LastRunAt       string                     `json:"lastRunAt,omitempty"`
	CreatedAt       string                     `json:"createdAt"`
	UpdatedAt       string                     `json:"updatedAt"`
}

// AutomationRunRequest identifies a manual trigger to execute.
type AutomationRunRequest struct {
	ID      int64 `json:"id" form:"id" binding:"required"`
	VaultID int64 `json:"vaultId" form:"vaultId"`
}
