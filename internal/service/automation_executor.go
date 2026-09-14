package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
)

// ErrAutomationActionExecutorNotRegistered indicates that an action has no
// executor in the registry.
var ErrAutomationActionExecutorNotRegistered = errors.New("automation action executor is not registered")

// AutomationActionExecutor executes one automation action against a target
// configuration. The event is kept separate from the execution context because
// webhook delivery needs the original event payload while Git and Backup use
// the stable execution scope.
type AutomationActionExecutor interface {
	Execute(ctx context.Context, action domain.AutomationAction, execution *domain.AutomationExecutionContext, event *domain.AutomationEvent) error
}

// AutomationActionExecutorRegistry maps action types to their executors.
// Registry access is safe while actions are being dispatched concurrently.
type AutomationActionExecutorRegistry struct {
	mu        sync.RWMutex
	executors map[string]AutomationActionExecutor
}

// NewAutomationActionExecutorRegistry creates an empty action executor
// registry.
func NewAutomationActionExecutorRegistry() *AutomationActionExecutorRegistry {
	return &AutomationActionExecutorRegistry{
		executors: make(map[string]AutomationActionExecutor),
	}
}

// Register associates actionType with executor. Registering an existing type
// replaces its executor, which allows an application to override a default
// implementation during initialization.
func (r *AutomationActionExecutorRegistry) Register(actionType string, executor AutomationActionExecutor) error {
	if r == nil {
		return errors.New("automation action executor registry is nil")
	}
	key := normalizeAutomationActionType(actionType)
	if key == "" {
		return errors.New("automation action type is required")
	}
	if executor == nil {
		return errors.New("automation action executor is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.executors == nil {
		r.executors = make(map[string]AutomationActionExecutor)
	}
	r.executors[key] = executor
	return nil
}

// Get returns the executor registered for actionType.
func (r *AutomationActionExecutorRegistry) Get(actionType string) (AutomationActionExecutor, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	executor, ok := r.executors[normalizeAutomationActionType(actionType)]
	return executor, ok
}

func normalizeAutomationActionType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// GitExecutor dispatches an action to the Git synchronization service.
type GitExecutor struct {
	service GitSyncService
}

// NewGitExecutor creates a Git action executor.
func NewGitExecutor(service GitSyncService) *GitExecutor {
	return &GitExecutor{service: service}
}

func (e *GitExecutor) Execute(ctx context.Context, action domain.AutomationAction, execution *domain.AutomationExecutionContext, _ *domain.AutomationEvent) error {
	if e == nil || e.service == nil {
		return errors.New("git sync service is unavailable")
	}
	if execution == nil {
		return errors.New("automation execution context is required")
	}
	return e.service.ExecuteSync(ctx, execution.UID, action.ConfigID, execution)
}

// BackupExecutor dispatches an action to the backup service.
type BackupExecutor struct {
	service BackupService
}

// NewBackupExecutor creates a Backup action executor.
func NewBackupExecutor(service BackupService) *BackupExecutor {
	return &BackupExecutor{service: service}
}

func (e *BackupExecutor) Execute(ctx context.Context, action domain.AutomationAction, execution *domain.AutomationExecutionContext, _ *domain.AutomationEvent) error {
	if e == nil || e.service == nil {
		return errors.New("backup service is unavailable")
	}
	if execution == nil {
		return errors.New("automation execution context is required")
	}
	return e.service.ExecuteUserBackup(ctx, execution.UID, action.ConfigID, execution)
}

// WebhookExecutor dispatches an action to the webhook service.
type WebhookExecutor struct {
	service WebhookService
}

// NewWebhookExecutor creates a Webhook action executor.
func NewWebhookExecutor(service WebhookService) *WebhookExecutor {
	return &WebhookExecutor{service: service}
}

func (e *WebhookExecutor) Execute(ctx context.Context, action domain.AutomationAction, execution *domain.AutomationExecutionContext, event *domain.AutomationEvent) error {
	if e == nil || e.service == nil {
		return errors.New("webhook service is unavailable")
	}
	if event == nil {
		return errors.New("automation event is required")
	}
	return e.service.DeliverEvent(ctx, event.UID, action.ConfigID, contentChangeEventFromAutomation(event))
}

func defaultAutomationActionExecutorRegistry(backupService BackupService, gitSyncService GitSyncService, webhookService WebhookService) *AutomationActionExecutorRegistry {
	executors := NewAutomationActionExecutorRegistry()
	// These registrations are fixed application defaults. A caller that needs
	// custom implementations can build a registry and pass it to the explicit
	// constructor instead.
	_ = executors.Register(domain.AutomationTargetGit, NewGitExecutor(gitSyncService))
	_ = executors.Register(domain.AutomationTargetBackup, NewBackupExecutor(backupService))
	_ = executors.Register(domain.AutomationTargetWebhook, NewWebhookExecutor(webhookService))
	return executors
}

func automationActionExecutorError(actionType string) error {
	return fmt.Errorf("%w for type %q", ErrAutomationActionExecutorNotRegistered, actionType)
}

var _ AutomationActionExecutor = (*GitExecutor)(nil)
var _ AutomationActionExecutor = (*BackupExecutor)(nil)
var _ AutomationActionExecutor = (*WebhookExecutor)(nil)
