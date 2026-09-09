package dto

import "github.com/haierkeys/fast-note-sync-service/internal/domain"

// AutomationActionDTO binds an automation trigger to an existing target
// configuration. Secrets belong to the target configuration APIs.
type AutomationActionDTO struct {
	Type     string `json:"type"`
	ConfigID int64  `json:"configId"`
}

type AutomationEventRuleDTO struct {
	Type            domain.AutomationEventType `json:"type"`
	Schedule        string                     `json:"schedule,omitempty"`
	ContentContains string                     `json:"contentContains,omitempty"`
	PathPrefix      string                     `json:"pathPrefix,omitempty"`
	PathGlob        string                     `json:"pathGlob,omitempty"`
	EventActions    []string                   `json:"eventActions,omitempty"`
}

// AutomationTriggerRequest creates or updates an automation trigger.
type AutomationTriggerRequest struct {
	ID        int64                    `json:"id" form:"id"`
	Name      string                   `json:"name" form:"name"`
	Enabled   bool                     `json:"enabled" form:"enabled"`
	VaultID   int64                    `json:"vaultId" form:"vaultId"`
	Timezone  string                   `json:"timezone" form:"timezone"`
	MatchMode string                   `json:"matchMode" form:"matchMode"`
	Events    []AutomationEventRuleDTO `json:"events" form:"events"`
	Actions   []AutomationActionDTO    `json:"actions" form:"actions"`
}

// AutomationTriggerDTO is the safe API representation of an automation
// trigger. It intentionally contains no target credentials.
type AutomationTriggerDTO struct {
	ID        int64                    `json:"id"`
	UID       int64                    `json:"uid"`
	Name      string                   `json:"name"`
	Enabled   bool                     `json:"enabled"`
	VaultID   int64                    `json:"vaultId"`
	Timezone  string                   `json:"timezone"`
	MatchMode string                   `json:"matchMode"`
	Events    []AutomationEventRuleDTO `json:"events"`
	Actions   []AutomationActionDTO    `json:"actions"`
	Warnings  []string                 `json:"warnings,omitempty"`
	LastRunAt string                   `json:"lastRunAt,omitempty"`
	CreatedAt string                   `json:"createdAt"`
	UpdatedAt string                   `json:"updatedAt"`
}

// AutomationRunRequest identifies a manual trigger to execute.
type AutomationRunRequest struct {
	ID int64 `json:"id" form:"id" binding:"required"`
}
