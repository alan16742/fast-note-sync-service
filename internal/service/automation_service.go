package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/safego"
	"github.com/haierkeys/fast-note-sync-service/pkg/timex"
	"github.com/haierkeys/fast-note-sync-service/pkg/workerpool"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

const automationDefaultTimezone = "Asia/Shanghai"

const (
	// Execution history is an audit/idempotency surface, not an unbounded event
	// store. Keep recent history for retries while bounding terminal rows.
	automationExecutionRetentionPeriod = 30 * 24 * time.Hour
	automationExecutionMaxRetained     = 1000
	automationExecutionCleanupInterval = 24 * time.Hour
)

// automationRulesCacheTTL bounds how long a per-user enabled-rule snapshot may be
// reused, so a burst of note saves doesn't repeat the same ListEnabled query.
// automationRulesCacheTTL 限制每个用户启用规则快照可复用的时长，避免一次笔记保存风暴
// 重复执行相同的 ListEnabled 查询。
const automationRulesCacheTTL = 3 * time.Second

// AutomationService is the central event-to-target coordinator. Events carry
// no target-specific configuration; triggers decide which existing targets are
// invoked after their conditions match.
type AutomationService interface {
	List(ctx context.Context, uid int64) ([]*dto.AutomationTriggerDTO, error)
	Save(ctx context.Context, uid int64, request *dto.AutomationTriggerRequest) (*dto.AutomationTriggerDTO, error)
	Delete(ctx context.Context, uid, id int64) error
	Trigger(ctx context.Context, uid int64, request *dto.AutomationRunRequest) error
	ListExecutions(ctx context.Context, uid, triggerID int64, page, pageSize int) ([]*dto.AutomationExecutionDTO, int64, error)
	RetryExecution(ctx context.Context, uid, executionID int64) error
	Publish(ctx context.Context, event *domain.AutomationEvent)
	PublishNoteChange(ctx context.Context, event *domain.ContentChangeEvent)
	Start()
	Shutdown(ctx context.Context) error
}

// AutomationEventPublisher is the small dependency used by write/audit
// services. Keeping this interface narrow makes the event layer independent of
// the HTTP and persistence APIs.
type AutomationEventPublisher interface {
	Publish(ctx context.Context, event *domain.AutomationEvent)
}

type automationRulesCacheEntry struct {
	triggers  []*domain.AutomationTrigger
	expiresAt time.Time
}

type automationUserRulesCache struct {
	mu     sync.Mutex
	byType map[domain.AutomationEventType]automationRulesCacheEntry
}

type automationService struct {
	repo           domain.AutomationRepository
	vaultRepo      domain.VaultRepository
	backupService  BackupService
	gitSyncService GitSyncService
	webhookService WebhookService
	executors      *AutomationActionExecutorRegistry
	pool           *workerpool.Pool
	executionRepo  domain.AutomationExecutionRepository
	logger         *zap.Logger
	ctx            context.Context
	cancel         context.CancelFunc
	startOnce      sync.Once
	shutdownOnce   sync.Once
	eventMu        sync.Mutex
	stopped        bool
	eventWg        sync.WaitGroup
	doneCh         chan struct{}
	cacheMu        sync.Mutex
	rulesCache     map[int64]*automationUserRulesCache
}

// NewAutomationService creates the central automation coordinator.
func NewAutomationService(
	repo domain.AutomationRepository,
	vaultRepo domain.VaultRepository,
	backupService BackupService,
	gitSyncService GitSyncService,
	webhookService WebhookService,
	pool *workerpool.Pool,
	logger *zap.Logger,
	executionRepos ...domain.AutomationExecutionRepository,
) AutomationService {
	return NewAutomationServiceWithExecutorRegistry(
		repo, vaultRepo, backupService, gitSyncService, webhookService, pool, logger,
		defaultAutomationActionExecutorRegistry(backupService, gitSyncService, webhookService), executionRepos...,
	)
}

// NewAutomationServiceWithExecutorRegistry creates the central automation
// coordinator with an explicit action executor registry. The target services
// remain separate dependencies for Save-time ownership validation; action
// execution itself only goes through executors.
func NewAutomationServiceWithExecutorRegistry(
	repo domain.AutomationRepository,
	vaultRepo domain.VaultRepository,
	backupService BackupService,
	gitSyncService GitSyncService,
	webhookService WebhookService,
	pool *workerpool.Pool,
	logger *zap.Logger,
	executors *AutomationActionExecutorRegistry,
	executionRepos ...domain.AutomationExecutionRepository,
) AutomationService {
	if logger == nil {
		logger = zap.NewNop()
	}
	if executors == nil {
		executors = defaultAutomationActionExecutorRegistry(backupService, gitSyncService, webhookService)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var executionRepo domain.AutomationExecutionRepository
	if len(executionRepos) > 0 {
		executionRepo = executionRepos[0]
	}
	return &automationService{
		repo: repo, vaultRepo: vaultRepo, backupService: backupService, gitSyncService: gitSyncService,
		webhookService: webhookService, executors: executors,
		pool: pool, executionRepo: executionRepo, logger: logger, ctx: ctx, cancel: cancel, doneCh: make(chan struct{}),
		rulesCache: make(map[int64]*automationUserRulesCache),
	}
}

func (s *automationService) List(ctx context.Context, uid int64) ([]*dto.AutomationTriggerDTO, error) {
	triggers, err := s.repo.List(ctx, uid)
	if err != nil {
		return nil, err
	}
	result := make([]*dto.AutomationTriggerDTO, 0, len(triggers))
	for _, trigger := range triggers {
		result = append(result, automationToDTO(trigger))
	}
	return result, nil
}

func (s *automationService) Save(ctx context.Context, uid int64, request *dto.AutomationTriggerRequest) (*dto.AutomationTriggerDTO, error) {
	trigger, err := automationFromRequest(request, uid)
	if err != nil {
		return nil, err
	}
	if err := s.validateAutomationTargets(ctx, uid, trigger.Actions); err != nil {
		return nil, err
	}
	// Validate the vault in the same user scope before persisting the rule.
	// Execution already performs this lookup, but validating here avoids saving
	// rules that can never run because their vault ID belongs to another user
	// or no longer exists.
	if s.vaultRepo != nil {
		vault, err := s.vaultRepo.GetByID(ctx, trigger.VaultID, uid)
		if err != nil {
			return nil, err
		}
		if vault == nil {
			return nil, code.ErrorVaultNotFound
		}
	}
	if trigger.ID > 0 {
		old, err := s.repo.GetByID(ctx, trigger.ID, uid)
		if err != nil {
			return nil, err
		}
		if old == nil {
			return nil, errors.New("automation trigger not found")
		}
		trigger.CreatedAt = old.CreatedAt
		trigger.LastRunAt = old.LastRunAt
		trigger.LastAttemptAt = old.LastAttemptAt
	}
	saved, err := s.repo.Save(ctx, trigger, uid)
	if err != nil {
		return nil, err
	}
	s.invalidateRulesCache(uid)
	result := automationToDTO(saved)
	result.Warnings = s.targetReuseWarnings(ctx, uid, saved)
	return result, nil
}

func (s *automationService) targetReuseWarnings(ctx context.Context, uid int64, trigger *domain.AutomationTrigger) []string {
	if trigger == nil {
		return nil
	}
	triggers, err := s.repo.List(ctx, uid)
	if err != nil {
		return nil
	}
	seen := map[string]struct{}{}
	var warnings []string
	for _, existing := range triggers {
		if existing.ID == trigger.ID || existing.VaultID == trigger.VaultID {
			continue
		}
		for _, action := range trigger.Actions {
			for _, other := range existing.Actions {
				if action.Type != other.Type || action.ConfigID != other.ConfigID {
					continue
				}
				key := fmt.Sprintf("%s:%d:%d", action.Type, action.ConfigID, existing.VaultID)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				warnings = append(warnings, fmt.Sprintf("target %s#%d is also used by vault %d", action.Type, action.ConfigID, existing.VaultID))
			}
		}
	}
	return warnings
}

func (s *automationService) validateAutomationTargets(ctx context.Context, uid int64, actions []domain.AutomationAction) error {
	needBackup, needGit, needWebhook := false, false, false
	for _, action := range actions {
		switch action.Type {
		case domain.AutomationTargetBackup:
			needBackup = true
		case domain.AutomationTargetGit:
			needGit = true
		case domain.AutomationTargetWebhook:
			needWebhook = true
		}
	}

	if needBackup {
		if s.backupService == nil {
			return errors.New("backup service is unavailable")
		}
		configs, err := s.backupService.GetConfigs(ctx, uid)
		if err != nil {
			return fmt.Errorf("load backup targets: %w", err)
		}
		for _, action := range actions {
			if action.Type != domain.AutomationTargetBackup {
				continue
			}
			found := false
			for _, config := range configs {
				if config != nil && config.ID == action.ConfigID {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("backup target #%d was not found for this user", action.ConfigID)
			}
		}
	}

	if needGit {
		if s.gitSyncService == nil {
			return errors.New("git sync service is unavailable")
		}
		configs, err := s.gitSyncService.GetConfigs(ctx, uid)
		if err != nil {
			return fmt.Errorf("load git targets: %w", err)
		}
		for _, action := range actions {
			if action.Type != domain.AutomationTargetGit {
				continue
			}
			found := false
			for _, config := range configs {
				if config != nil && config.ID == action.ConfigID {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("git target #%d was not found for this user", action.ConfigID)
			}
		}
	}

	if needWebhook {
		if s.webhookService == nil {
			return errors.New("webhook service is unavailable")
		}
		channels, err := s.webhookService.List(ctx, uid)
		if err != nil {
			return fmt.Errorf("load webhook targets: %w", err)
		}
		for _, action := range actions {
			if action.Type != domain.AutomationTargetWebhook {
				continue
			}
			found := false
			for _, channel := range channels {
				if channel != nil && channel.ID == action.ConfigID {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("webhook target #%d was not found for this user", action.ConfigID)
			}
		}
	}
	return nil
}

func (s *automationService) Delete(ctx context.Context, uid, id int64) error {
	if id <= 0 {
		return errors.New("automation trigger id is required")
	}
	if err := s.repo.Delete(ctx, id, uid); err != nil {
		return err
	}
	s.invalidateRulesCache(uid)
	return nil
}

// Trigger emits a manual event for one manual trigger. It is deliberately
// separate from Save so a UI/API caller can run the same configured rule
// without knowing anything about its target credentials.
func (s *automationService) Trigger(ctx context.Context, uid int64, request *dto.AutomationRunRequest) error {
	if request == nil || request.ID <= 0 {
		return errors.New("automation trigger id is required")
	}
	trigger, err := s.repo.GetByID(ctx, request.ID, uid)
	if err != nil {
		return err
	}
	if trigger == nil {
		return errors.New("automation trigger not found")
	}
	if !trigger.Enabled {
		return errors.New("automation trigger is disabled")
	}
	if !automationTriggerHasEvent(trigger, domain.AutomationEventManual) {
		return errors.New("only manual triggers can be run manually")
	}
	event := &domain.AutomationEvent{
		ID: uuid.NewString(), OccurredAt: time.Now().UTC(), UID: uid,
		VaultID: trigger.VaultID, Type: domain.AutomationEventManual,
		Action: "manual", Source: "manual_api",
	}
	s.enrichEvent(ctx, event)
	if !automationTriggerMatches(trigger, event) {
		return errors.New("manual event does not match automation trigger conditions")
	}
	return s.executeTrigger(ctx, trigger, event)
}

func (s *automationService) ListExecutions(ctx context.Context, uid, triggerID int64, page, pageSize int) ([]*dto.AutomationExecutionDTO, int64, error) {
	if uid <= 0 {
		return nil, 0, errors.New("user id is required")
	}
	if s.executionRepo == nil {
		return []*dto.AutomationExecutionDTO{}, 0, nil
	}
	executions, total, err := s.executionRepo.List(ctx, uid, triggerID, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	result := make([]*dto.AutomationExecutionDTO, 0, len(executions))
	for _, execution := range executions {
		result = append(result, automationExecutionToDTO(execution))
	}
	return result, total, nil
}

// Publish queues all enabled triggers for an event. It intentionally has no
// error return and performs the repository lookup asynchronously: event
// consumers must never make note/file writes fail or add database latency.
func (s *automationService) Publish(_ context.Context, event *domain.AutomationEvent) {
	if s == nil || s.repo == nil || event == nil {
		return
	}
	// Events are published after a write and can outlive the request context.
	// Copy the small event payload before handing it to the background worker so
	// callers cannot mutate it while matching is in progress.
	eventCopy := *event
	eventCopy.ChangedFields = append([]string(nil), event.ChangedFields...)
	s.eventMu.Lock()
	if s.stopped {
		s.eventMu.Unlock()
		return
	}
	s.eventWg.Add(1)
	s.eventMu.Unlock()
	safego.Go(s.logger, func() {
		defer s.eventWg.Done()
		s.publishNow(s.ctx, &eventCopy)
	})
}

func (s *automationService) publishNow(ctx context.Context, event *domain.AutomationEvent) {
	triggers, err := s.listEnabledCached(ctx, event.UID, event.Type)
	if err != nil {
		s.logger.Warn("load automation triggers failed", zap.Int64("uid", event.UID), zap.String("eventType", string(event.Type)), zap.Error(err))
		return
	}
	for _, trigger := range triggers {
		if !automationTriggerMatches(trigger, event) {
			continue
		}
		s.enrichEvent(ctx, event)
		if err := s.executeTrigger(ctx, trigger, event); err != nil {
			s.logger.Warn("automation trigger dispatch failed", zap.Int64("uid", event.UID), zap.Int64("triggerID", trigger.ID), zap.String("eventID", event.ID), zap.Error(err))
		}
	}
}

// listEnabledCached returns the enabled triggers for a user/event type, reusing
// a snapshot for automationRulesCacheTTL. Cached triggers are treated as
// read-only by matching and dispatch, so sharing the slice is safe.
func (s *automationService) listEnabledCached(ctx context.Context, uid int64, eventType domain.AutomationEventType) ([]*domain.AutomationTrigger, error) {
	cache := s.userRulesCache(uid)
	// Serialize cache misses and invalidation per user. A query started before
	// Save/Delete must not repopulate the cache after invalidation completed.
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if entry, ok := cache.byType[eventType]; ok && time.Now().Before(entry.expiresAt) {
		return entry.triggers, nil
	}

	triggers, err := s.repo.ListEnabled(ctx, uid, eventType)
	if err != nil {
		return nil, err
	}

	cache.byType[eventType] = automationRulesCacheEntry{triggers: triggers, expiresAt: time.Now().Add(automationRulesCacheTTL)}
	return triggers, nil
}

func (s *automationService) userRulesCache(uid int64) *automationUserRulesCache {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if s.rulesCache == nil {
		s.rulesCache = make(map[int64]*automationUserRulesCache)
	}
	if s.rulesCache[uid] == nil {
		s.rulesCache[uid] = &automationUserRulesCache{byType: make(map[domain.AutomationEventType]automationRulesCacheEntry)}
	}
	return s.rulesCache[uid]
}

func (s *automationService) invalidateRulesCache(uid int64) {
	cache := s.userRulesCache(uid)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	clear(cache.byType)
}

func (s *automationService) enrichEvent(ctx context.Context, event *domain.AutomationEvent) {
	if s == nil || s.vaultRepo == nil || event == nil || event.VaultID <= 0 || event.VaultName != "" {
		return
	}
	vault, err := s.vaultRepo.GetByID(ctx, event.VaultID, event.UID)
	if err == nil && vault != nil {
		event.VaultName = vault.Name
	}
}

// PublishNoteChange converts a persisted note change into the central event
// contract. Notification delivery is performed only by matching automation
// triggers; a channel never subscribes to events by itself.
func (s *automationService) PublishNoteChange(ctx context.Context, event *domain.ContentChangeEvent) {
	if s == nil || event == nil {
		return
	}
	s.Publish(ctx, automationEventFromNote(event))
}

func (s *automationService) dispatchTrigger(ctx context.Context, trigger *domain.AutomationTrigger, event *domain.AutomationEvent) error {
	return s.dispatchTriggerObserved(ctx, trigger, event, nil)
}

type automationActionObserver func(index int, action domain.AutomationAction, status domain.AutomationExecutionStatus, cause error, at time.Time)

func (s *automationService) dispatchTriggerObserved(ctx context.Context, trigger *domain.AutomationTrigger, event *domain.AutomationEvent, observe automationActionObserver) error {
	return s.dispatchTriggerSelected(ctx, trigger, event, nil, observe)
}

func (s *automationService) dispatchTriggerSelected(ctx context.Context, trigger *domain.AutomationTrigger, event *domain.AutomationEvent, selected map[int]struct{}, observe automationActionObserver) error {
	if trigger == nil || event == nil {
		return nil
	}
	var firstErr error
	for index, action := range trigger.Actions {
		if selected != nil {
			if _, ok := selected[index]; !ok {
				continue
			}
		}
		action := action
		if observe != nil {
			observe(index, action, domain.AutomationExecutionRunning, nil, time.Now().UTC())
		}
		submit := func(taskCtx context.Context) error {
			return s.executeAction(taskCtx, trigger.ID, event, action)
		}
		var actionErr error
		if s.pool == nil {
			actionErr = submit(ctx)
		} else {
			// Publish already runs outside the write request. Waiting here is
			// intentional: a successful dispatch means every target action finished
			// successfully, so cron can safely advance LastRunAt and manual callers
			// receive the actual action error.
			actionErr = s.pool.Submit(ctx, submit)
		}
		if actionErr != nil {
			if firstErr == nil {
				firstErr = actionErr
			}
			s.logger.Warn("automation action failed",
				zap.Int64("uid", event.UID), zap.Int64("triggerID", trigger.ID), zap.String("eventID", event.ID),
				zap.String("targetType", action.Type), zap.Int64("configID", action.ConfigID), zap.Error(actionErr))
			if observe != nil {
				observe(index, action, automationExecutionStatusForError(actionErr), actionErr, time.Now().UTC())
			}
			continue
		}
		if observe != nil {
			observe(index, action, domain.AutomationExecutionSucceeded, nil, time.Now().UTC())
		}
	}
	return firstErr
}

func (s *automationService) executeTrigger(ctx context.Context, trigger *domain.AutomationTrigger, event *domain.AutomationEvent) error {
	if trigger == nil || event == nil {
		return nil
	}
	if s.executionRepo == nil {
		return s.dispatchTrigger(ctx, trigger, event)
	}

	execution := &domain.AutomationExecution{
		UID: trigger.UID, TriggerID: trigger.ID, VaultID: trigger.VaultID,
		EventID: event.ID, EventType: event.Type, Status: domain.AutomationExecutionRunning,
		StartedAt: time.Now().UTC(), Actions: make([]domain.AutomationActionExecution, len(trigger.Actions)),
	}
	for index, action := range trigger.Actions {
		execution.Actions[index] = domain.AutomationActionExecution{
			Type: action.Type, ConfigID: action.ConfigID, Status: domain.AutomationExecutionPending,
		}
	}

	created, current, err := s.executionRepo.Start(ctx, execution)
	if err != nil {
		// Execution history must not make a note/file write fail. The action is
		// still attempted, but the failure is visible in the service log.
		s.logger.Warn("start automation execution record failed",
			zap.Int64("uid", event.UID), zap.Int64("triggerID", trigger.ID), zap.String("eventID", event.ID), zap.Error(err))
		return s.dispatchTrigger(ctx, trigger, event)
	}
	if current != nil {
		execution = current
	}
	if !created {
		if current != nil && current.Status == domain.AutomationExecutionSucceeded {
			// The same event was delivered more than once. A successful execution
			// is already complete, so do not repeat side effects.
			return nil
		}
		if current != nil && current.Status == domain.AutomationExecutionRunning {
			return errors.New("automation execution is already running")
		}
		if current != nil && current.Error != "" {
			return fmt.Errorf("automation execution already completed: %s", current.Error)
		}
		return errors.New("automation execution already completed")
	}

	observe := s.automationActionObserver(ctx, execution, "update automation action execution failed")

	err = s.dispatchTriggerObserved(ctx, trigger, event, observe)
	execution.Status = domain.AutomationExecutionSucceeded
	if err != nil {
		execution.Status = automationExecutionStatusForError(err)
		execution.Error = err.Error()
		// Retain the payload only when a retry may need it. Successful executions
		// keep metadata and action results without duplicating full note content.
		execution.Event = *event
	}
	execution.FinishedAt = time.Now().UTC()
	if updateErr := s.updateAutomationExecution(ctx, execution); updateErr != nil {
		s.logger.Warn("finish automation execution record failed",
			zap.Int64("executionID", execution.ID), zap.Int64("triggerID", trigger.ID), zap.String("eventID", event.ID), zap.Error(updateErr))
	}
	return err
}

func automationExecutionStatusForError(err error) domain.AutomationExecutionStatus {
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, workerpool.ErrTaskCancelled) || errors.Is(err, workerpool.ErrWorkerPoolClosed)) {
		return domain.AutomationExecutionCancelled
	}
	return domain.AutomationExecutionFailed
}

func (s *automationService) updateAutomationExecution(ctx context.Context, execution *domain.AutomationExecution) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.executionRepo.Update(persistCtx, execution)
	}
	return s.executionRepo.Update(ctx, execution)
}

func (s *automationService) automationActionObserver(ctx context.Context, execution *domain.AutomationExecution, logMessage string) automationActionObserver {
	return func(index int, action domain.AutomationAction, status domain.AutomationExecutionStatus, cause error, at time.Time) {
		if index < 0 || index >= len(execution.Actions) {
			return
		}
		actionState := &execution.Actions[index]
		actionState.Type = action.Type
		actionState.ConfigID = action.ConfigID
		actionState.Status = status
		if status == domain.AutomationExecutionRunning {
			actionState.StartedAt = at
			actionState.Error = ""
		} else {
			actionState.FinishedAt = at
			if cause != nil {
				actionState.Error = cause.Error()
			} else {
				actionState.Error = ""
			}
		}
		if err := s.updateAutomationExecution(ctx, execution); err != nil {
			s.logger.Warn(logMessage, zap.Int64("executionID", execution.ID), zap.Int("actionIndex", index), zap.Error(err))
		}
	}
}

func (s *automationService) RetryExecution(ctx context.Context, uid, executionID int64) error {
	if s.executionRepo == nil {
		return errors.New("automation execution history is unavailable")
	}
	if s.repo == nil {
		return errors.New("automation repository is unavailable")
	}
	if uid <= 0 || executionID <= 0 {
		return errors.New("automation execution id is required")
	}
	execution, err := s.executionRepo.GetByID(ctx, uid, executionID)
	if err != nil {
		return err
	}
	if execution == nil {
		return errors.New("automation execution not found")
	}
	if execution.Status != domain.AutomationExecutionFailed && execution.Status != domain.AutomationExecutionCancelled {
		return errors.New("only failed or cancelled automation executions can be retried")
	}
	trigger, err := s.repo.GetByID(ctx, execution.TriggerID, uid)
	if err != nil {
		return err
	}
	if trigger == nil {
		return errors.New("automation trigger not found")
	}
	if !trigger.Enabled {
		return errors.New("automation trigger is disabled")
	}
	if len(trigger.Actions) != len(execution.Actions) {
		return errors.New("automation trigger actions changed; run the trigger again")
	}
	for index, action := range trigger.Actions {
		if execution.Actions[index].Type != action.Type || execution.Actions[index].ConfigID != action.ConfigID {
			return errors.New("automation trigger actions changed; run the trigger again")
		}
	}
	event := execution.Event
	if event.ID == "" {
		return errors.New("automation execution event payload is unavailable")
	}
	event.UID = uid
	event.VaultID = execution.VaultID
	selected := make(map[int]struct{})
	for index := range trigger.Actions {
		if index >= len(execution.Actions) || execution.Actions[index].Status != domain.AutomationExecutionSucceeded {
			selected[index] = struct{}{}
			if index < len(execution.Actions) {
				execution.Actions[index].Status = domain.AutomationExecutionPending
				execution.Actions[index].Error = ""
				execution.Actions[index].StartedAt = time.Time{}
				execution.Actions[index].FinishedAt = time.Time{}
			}
		}
	}
	if len(selected) == 0 {
		return errors.New("automation execution has no retryable actions")
	}
	execution.Status = domain.AutomationExecutionRunning
	execution.Error = ""
	execution.StartedAt = time.Now().UTC()
	execution.FinishedAt = time.Time{}
	claimed, err := s.executionRepo.ClaimRetry(ctx, execution)
	if err != nil {
		return err
	}
	if !claimed {
		return errors.New("automation execution is already running or no longer retryable")
	}
	observe := s.automationActionObserver(ctx, execution, "update retried automation action execution failed")
	err = s.dispatchTriggerSelected(ctx, trigger, &event, selected, observe)
	execution.Status = domain.AutomationExecutionSucceeded
	if err != nil {
		execution.Status = automationExecutionStatusForError(err)
		execution.Error = err.Error()
	}
	execution.FinishedAt = time.Now().UTC()
	if updateErr := s.updateAutomationExecution(ctx, execution); updateErr != nil {
		s.logger.Warn("finish retried automation execution record failed", zap.Int64("executionID", execution.ID), zap.Error(updateErr))
	}
	if execution.EventType == domain.AutomationEventCron {
		cursorAt := time.Now()
		var cursorErr error
		if err == nil {
			cursorErr = s.repo.MarkRun(ctx, trigger.ID, uid, cursorAt)
		} else {
			cursorErr = s.repo.MarkAttempt(ctx, trigger.ID, uid, cursorAt)
		}
		if cursorErr != nil {
			s.logger.Warn("update cron cursor after automation retry failed", zap.Int64("triggerID", trigger.ID), zap.Int64("executionID", execution.ID), zap.Error(cursorErr))
		}
	}
	return err
}

func (s *automationService) executeAction(ctx context.Context, triggerID int64, event *domain.AutomationEvent, action domain.AutomationAction) error {
	if action.ConfigID <= 0 {
		return errors.New("automation target config id is required")
	}
	if event == nil {
		return errors.New("automation event is required")
	}
	executor, ok := s.executors.Get(action.Type)
	if !ok {
		return automationActionExecutorError(action.Type)
	}
	return executor.Execute(ctx, action, &domain.AutomationExecutionContext{
		UID: event.UID, TriggerID: triggerID, VaultID: event.VaultID, EventID: event.ID, OccurredAt: event.OccurredAt,
	}, event)
}

func (s *automationService) Start() {
	if s == nil || s.repo == nil {
		return
	}
	s.startOnce.Do(func() {
		go s.runClock()
	})
}

func (s *automationService) runClock() {
	defer close(s.doneCh)
	s.cleanupAutomationExecutions()
	s.pollTimeTriggers()
	nextCleanupAt := time.Now().Add(automationExecutionCleanupInterval)
	for {
		// Recalculate the next wall-clock minute after every poll.
		next := nextAutomationMinuteBoundary(time.Now())
		timer := time.NewTimer(time.Until(next))
		select {
		case <-timer.C:
			now := time.Now()
			if !now.Before(nextCleanupAt) {
				s.cleanupAutomationExecutions()
				nextCleanupAt = now.Add(automationExecutionCleanupInterval)
			}
			s.pollTimeTriggers()
		case <-s.ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (s *automationService) cleanupAutomationExecutions() {
	if s == nil || s.executionRepo == nil {
		return
	}
	cutoff := time.Now().UTC().Add(-automationExecutionRetentionPeriod)
	deleted, err := s.executionRepo.Cleanup(s.ctx, cutoff, automationExecutionMaxRetained)
	if err != nil {
		s.logger.Warn("cleanup automation execution history failed", zap.Error(err))
		return
	}
	if deleted > 0 {
		s.logger.Info("cleaned up automation execution history", zap.Int64("deleted", deleted))
	}
}

func nextAutomationMinuteBoundary(now time.Time) time.Time {
	return now.Truncate(time.Minute).Add(time.Minute)
}

func (s *automationService) pollTimeTriggers() {
	triggers, err := s.repo.ListEnabledByType(s.ctx, domain.AutomationEventCron)
	if err != nil {
		s.logger.Warn("load cron automation triggers failed", zap.Error(err))
		return
	}
	now := time.Now()
	for _, trigger := range triggers {
		location, err := time.LoadLocation(trigger.Timezone)
		if err != nil {
			location = time.Local
		}
		localNow := now.In(location)
		last := trigger.LastAttemptAt
		if last.IsZero() {
			last = trigger.LastRunAt
		}
		last = last.In(location)
		if last.IsZero() {
			last = localNow.Add(-time.Minute)
		}
		due := false
		cronRuleCount := 0
		cronMatches := 0
		var matchedOccurrences []time.Time
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		for _, eventRule := range trigger.Events {
			if eventRule.Type != domain.AutomationEventCron {
				continue
			}
			cronRuleCount++
			schedule, parseErr := parser.Parse(eventRule.Schedule)
			if parseErr != nil {
				s.logger.Warn("invalid automation schedule", zap.Int64("triggerID", trigger.ID), zap.Error(parseErr))
				continue
			}
			next := schedule.Next(last)
			if !next.After(localNow) {
				cronMatches++
				matchedOccurrences = append(matchedOccurrences, next)
			}
		}
		if trigger.MatchMode == domain.AutomationMatchAll {
			due = cronRuleCount > 0 && cronMatches == cronRuleCount
		} else {
			due = cronMatches > 0
		}
		if !due {
			continue
		}
		occurrence := localNow.Truncate(time.Minute)
		if len(matchedOccurrences) > 0 {
			occurrence = matchedOccurrences[0]
			for _, candidate := range matchedOccurrences[1:] {
				if trigger.MatchMode == domain.AutomationMatchAll && candidate.After(occurrence) {
					occurrence = candidate
				}
				if trigger.MatchMode != domain.AutomationMatchAll && candidate.Before(occurrence) {
					occurrence = candidate
				}
			}
		}
		event := &domain.AutomationEvent{
			ID: automationCronEventID(trigger.ID, occurrence), OccurredAt: occurrence.UTC(), UID: trigger.UID,
			VaultID: trigger.VaultID, Type: domain.AutomationEventCron,
			Action: "cron", Source: "automation_clock",
		}
		s.enrichEvent(s.ctx, event)
		if err := s.executeTrigger(s.ctx, trigger, event); err != nil {
			s.logger.Warn("dispatch time automation trigger failed", zap.Int64("triggerID", trigger.ID), zap.String("eventID", event.ID), zap.Error(err))
			// Queue saturation/closure means no action was attempted; leave the
			// cursor untouched so the next scheduler tick can retry promptly.
			if !errors.Is(err, workerpool.ErrWorkerPoolFull) && !errors.Is(err, workerpool.ErrWorkerPoolClosed) && !errors.Is(err, workerpool.ErrTaskCancelled) && !errors.Is(err, context.Canceled) {
				if markErr := s.repo.MarkAttempt(s.ctx, trigger.ID, trigger.UID, now); markErr != nil {
					s.logger.Warn("mark automation trigger attempt failed", zap.Int64("triggerID", trigger.ID), zap.Error(markErr))
				}
			}
			continue
		}
		if err := s.repo.MarkRun(s.ctx, trigger.ID, trigger.UID, now); err != nil {
			s.logger.Warn("mark automation trigger run failed", zap.Int64("triggerID", trigger.ID), zap.Error(err))
		}
	}
}

func automationCronEventID(triggerID int64, occurrence time.Time) string {
	return fmt.Sprintf("cron:%d:%d", triggerID, occurrence.Unix())
}

func (s *automationService) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.shutdownOnce.Do(func() {
		s.eventMu.Lock()
		s.stopped = true
		s.eventMu.Unlock()
		s.cancel()
		// App normally starts the clock during construction, but closing the
		// done channel here makes the service safe to shut down in isolated
		// tests or partial initialization paths too.
		s.startOnce.Do(func() { close(s.doneCh) })
	})
	eventsDone := make(chan struct{})
	go func() {
		s.eventWg.Wait()
		close(eventsDone)
	}()
	select {
	case <-eventsDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func automationFromRequest(request *dto.AutomationTriggerRequest, uid int64) (*domain.AutomationTrigger, error) {
	if request == nil {
		return nil, errors.New("automation trigger request is required")
	}
	if request.ID < 0 || request.VaultID <= 0 {
		return nil, errors.New("invalid automation trigger or vault id")
	}
	name := strings.TrimSpace(request.Name)
	if name == "" {
		return nil, errors.New("automation trigger name is required")
	}
	if len(name) > 120 {
		return nil, errors.New("automation trigger name is too long")
	}
	matchMode := normalizeAutomationMatchMode(request.MatchMode)
	if matchMode == "" {
		return nil, errors.New("automation trigger match mode must be any or all")
	}
	if len(request.Events) == 0 {
		return nil, errors.New("automation trigger requires at least one event branch")
	}
	events := make([]domain.AutomationEventRule, 0, len(request.Events))
	seenEvents := make(map[string]struct{}, len(request.Events))
	needsTimezone := false
	hasTodo := false
	for _, item := range request.Events {
		eventType := normalizeAutomationEventType(item.Type)
		rule := domain.AutomationEventRule{Type: eventType}
		var err error
		switch eventType {
		case domain.AutomationEventCron:
			rule.Schedule = strings.TrimSpace(item.Schedule)
			if rule.Schedule == "" {
				return nil, errors.New("cron event requires a schedule")
			}
			parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
			if _, err := parser.Parse(rule.Schedule); err != nil {
				return nil, fmt.Errorf("invalid cron schedule: %w", err)
			}
			needsTimezone = true
		case domain.AutomationEventNoteContent:
			rule.ContentContains = strings.TrimSpace(item.ContentContains)
		case domain.AutomationEventFileBehavior:
			rule.PathPrefix = strings.TrimSpace(item.PathPrefix)
			rule.PathGlob = strings.TrimSpace(item.PathGlob)
			if rule.PathGlob != "" {
				if _, err := path.Match(rule.PathGlob, ""); err != nil {
					return nil, errors.New("invalid path glob")
				}
			}
			rule.EventActions, err = normalizeEventActions(item.EventActions)
			if err != nil {
				return nil, err
			}
			if len(rule.EventActions) == 0 {
				return nil, errors.New("file behavior event requires at least one event action")
			}
		case domain.AutomationEventTodoReminder:
			hasTodo = true
			needsTimezone = true
		case domain.AutomationEventManual:
		default:
			return nil, fmt.Errorf("unsupported automation event type: %s", item.Type)
		}
		if len(rule.ContentContains) > 4096 || len(rule.PathPrefix) > 4096 || len(rule.PathGlob) > 4096 {
			return nil, errors.New("automation condition is too long")
		}
		key, _ := json.Marshal(rule)
		if _, exists := seenEvents[string(key)]; exists {
			continue
		}
		seenEvents[string(key)] = struct{}{}
		events = append(events, rule)
	}
	if err := validateAutomationEventCombination(matchMode, events); err != nil {
		return nil, err
	}
	timezone := strings.TrimSpace(request.Timezone)
	if needsTimezone {
		if timezone == "" {
			timezone = automationDefaultTimezone
		}
		if _, err := time.LoadLocation(timezone); err != nil {
			return nil, errors.New("invalid IANA timezone")
		}
	} else {
		timezone = ""
	}
	actions := make([]domain.AutomationAction, 0, len(request.Actions))
	seenTargets := make(map[string]struct{}, len(request.Actions))
	for _, item := range request.Actions {
		targetType := strings.ToLower(strings.TrimSpace(item.Type))
		switch targetType {
		case domain.AutomationTargetGit, domain.AutomationTargetBackup, domain.AutomationTargetWebhook:
		default:
			return nil, fmt.Errorf("unsupported automation target: %s", item.Type)
		}
		if item.ConfigID <= 0 {
			return nil, errors.New("automation target config id is required")
		}
		targetKey := fmt.Sprintf("%s:%d", targetType, item.ConfigID)
		if _, exists := seenTargets[targetKey]; exists {
			continue
		}
		seenTargets[targetKey] = struct{}{}
		actions = append(actions, domain.AutomationAction{Type: targetType, ConfigID: item.ConfigID})
	}
	if hasTodo {
		for _, action := range actions {
			if action.Type != domain.AutomationTargetWebhook {
				return nil, errors.New("triggers containing todo reminders can only target notification channels")
			}
		}
	}
	if len(actions) == 0 {
		return nil, errors.New("automation trigger requires at least one target")
	}
	return &domain.AutomationTrigger{
		ID: request.ID, UID: uid, Name: name, Enabled: request.Enabled,
		VaultID: request.VaultID, Timezone: timezone, MatchMode: domain.AutomationMatchMode(matchMode), Events: events, Actions: actions,
	}, nil
}

// validateAutomationEventCombination keeps the stateless event matcher honest:
// one AutomationEvent represents one event type, so ALL can only combine
// predicates evaluated against that same event. Different event types are
// alternatives and must use ANY; supporting cross-type ALL would require an
// explicit correlation/window state machine.
func validateAutomationEventCombination(matchMode string, events []domain.AutomationEventRule) error {
	if matchMode != string(domain.AutomationMatchAll) || len(events) < 2 {
		return nil
	}
	first := events[0].Type
	for _, event := range events[1:] {
		if event.Type != first {
			return errors.New("all mode only supports conditions of one event type; use any (OR) for different event types")
		}
	}
	if first == domain.AutomationEventFileBehavior {
		commonActions := make(map[string]struct{}, len(events[0].EventActions))
		for _, action := range events[0].EventActions {
			commonActions[action] = struct{}{}
		}
		for _, event := range events[1:] {
			next := make(map[string]struct{}, len(event.EventActions))
			for _, action := range event.EventActions {
				if _, ok := commonActions[action]; ok {
					next[action] = struct{}{}
				}
			}
			commonActions = next
		}
		if len(commonActions) == 0 {
			return errors.New("all mode file conditions must share at least one event action")
		}
	}
	return nil
}

func normalizeAutomationMatchMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "any":
		return string(domain.AutomationMatchAny)
	case "all":
		return string(domain.AutomationMatchAll)
	default:
		return ""
	}
}

func normalizeAutomationEventType(eventType domain.AutomationEventType) domain.AutomationEventType {
	return domain.AutomationEventType(strings.ToLower(strings.TrimSpace(string(eventType))))
}

func normalizeEventActions(actions []string) ([]string, error) {
	result := make([]string, 0, len(actions))
	seen := make(map[string]struct{}, len(actions))
	for _, action := range actions {
		value := strings.ToLower(strings.TrimSpace(action))
		if value == "" {
			continue
		}
		switch value {
		case "create", "modify", "delete", "rename", "restore", "permanent_delete":
		default:
			return nil, fmt.Errorf("unsupported automation event action: %s", action)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func automationTriggerMatches(trigger *domain.AutomationTrigger, event *domain.AutomationEvent) bool {
	if trigger == nil || event == nil || !trigger.Enabled || trigger.VaultID != event.VaultID {
		return false
	}
	matchMode := trigger.MatchMode
	if matchMode == "" {
		matchMode = domain.AutomationMatchAny
	}
	matched := 0
	for _, rule := range trigger.Events {
		if rule.Type != normalizeAutomationEventType(event.Type) {
			if matchMode == domain.AutomationMatchAll {
				return false
			}
			continue
		}
		if automationEventRuleMatches(rule, event) {
			matched++
			if matchMode == domain.AutomationMatchAny {
				return true
			}
		} else if matchMode == domain.AutomationMatchAll {
			return false
		}
	}
	return matchMode == domain.AutomationMatchAll && matched == len(trigger.Events) && matched > 0
}

func automationTriggerHasEvent(trigger *domain.AutomationTrigger, eventType domain.AutomationEventType) bool {
	if trigger == nil {
		return false
	}
	for _, rule := range trigger.Events {
		if rule.Type == eventType {
			return true
		}
	}
	return false
}

func automationEventRuleMatches(rule domain.AutomationEventRule, event *domain.AutomationEvent) bool {
	if rule.Type == domain.AutomationEventNoteContent {
		// A content predicate must be tied to a content change. NoteService also
		// publishes mtime-only, delete, restore, and rename events with the
		// current note body attached; matching the body alone would retrigger a
		// "content contains" rule for those unrelated changes. Empty
		// ChangedFields is treated as legacy/unknown input for compatibility.
		if rule.ContentContains != "" && len(event.ChangedFields) > 0 && !containsString(event.ChangedFields, "content") {
			return false
		}
		if rule.ContentContains != "" && !strings.Contains(event.Content, rule.ContentContains) {
			return false
		}
		return true
	}
	if rule.Type == domain.AutomationEventFileBehavior {
		if len(rule.EventActions) == 0 || !containsString(rule.EventActions, event.Action) {
			return false
		}
		if rule.PathPrefix == "" && rule.PathGlob == "" {
			return true
		}
		paths := []string{event.Path}
		if event.OldPath != "" && event.OldPath != event.Path {
			paths = append(paths, event.OldPath)
		}
		matchedPath := false
		for _, pathValue := range paths {
			if rule.PathPrefix != "" && !strings.HasPrefix(pathValue, rule.PathPrefix) {
				continue
			}
			if rule.PathGlob != "" {
				matched, err := path.Match(rule.PathGlob, pathValue)
				if err != nil || !matched {
					continue
				}
			}
			matchedPath = true
			break
		}
		return matchedPath
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}

func automationToDTO(trigger *domain.AutomationTrigger) *dto.AutomationTriggerDTO {
	if trigger == nil {
		return nil
	}
	actions := make([]dto.AutomationActionDTO, 0, len(trigger.Actions))
	for _, action := range trigger.Actions {
		actions = append(actions, dto.AutomationActionDTO{Type: action.Type, ConfigID: action.ConfigID})
	}
	events := make([]dto.AutomationEventRuleDTO, 0, len(trigger.Events))
	for _, event := range trigger.Events {
		var eventActions []string
		if event.Type == domain.AutomationEventFileBehavior {
			eventActions = append([]string(nil), event.EventActions...)
		}
		events = append(events, dto.AutomationEventRuleDTO{Type: event.Type, Schedule: event.Schedule, ContentContains: event.ContentContains, PathPrefix: event.PathPrefix, PathGlob: event.PathGlob, EventActions: eventActions})
	}
	result := &dto.AutomationTriggerDTO{
		ID: trigger.ID, UID: trigger.UID, Name: trigger.Name, Enabled: trigger.Enabled,
		VaultID: trigger.VaultID, Timezone: trigger.Timezone, MatchMode: string(trigger.MatchMode), Events: events, Actions: actions,
		CreatedAt: trigger.CreatedAt.Format(time.RFC3339), UpdatedAt: trigger.UpdatedAt.Format(time.RFC3339),
	}
	if !trigger.LastRunAt.IsZero() {
		result.LastRunAt = trigger.LastRunAt.Format(time.RFC3339)
	}
	if !trigger.LastAttemptAt.IsZero() {
		result.LastAttemptAt = trigger.LastAttemptAt.Format(time.RFC3339)
	}
	return result
}

func automationExecutionToDTO(execution *domain.AutomationExecution) *dto.AutomationExecutionDTO {
	if execution == nil {
		return nil
	}
	actions := make([]dto.AutomationActionExecutionDTO, 0, len(execution.Actions))
	for _, action := range execution.Actions {
		actions = append(actions, dto.AutomationActionExecutionDTO{
			Type: action.Type, ConfigID: action.ConfigID, Status: string(action.Status), Error: action.Error,
			StartedAt: timex.Time(action.StartedAt), FinishedAt: timex.Time(action.FinishedAt),
		})
	}
	return &dto.AutomationExecutionDTO{
		ID: execution.ID, UID: execution.UID, TriggerID: execution.TriggerID, VaultID: execution.VaultID,
		EventID: execution.EventID, EventType: execution.EventType, Status: execution.Status, Error: execution.Error,
		Actions: actions, StartedAt: timex.Time(execution.StartedAt), FinishedAt: timex.Time(execution.FinishedAt),
		CreatedAt: timex.Time(execution.CreatedAt), UpdatedAt: timex.Time(execution.UpdatedAt),
	}
}

func automationEventFromNote(event *domain.ContentChangeEvent) *domain.AutomationEvent {
	return &domain.AutomationEvent{
		ID: event.ID, OccurredAt: event.OccurredAt, UID: event.UID, VaultID: event.VaultID,
		VaultName: event.VaultName, Type: domain.AutomationEventNoteContent, Action: string(event.Action),
		Path: event.Path, OldPath: event.OldPath, PathHash: event.PathHash, ChangedFields: append([]string(nil), event.ChangedFields...),
		Content: event.Content, ContentHash: event.ContentHash, Size: event.Size,
		ClientType: event.ClientType, ClientName: event.ClientName, ClientVersion: event.ClientVersion, Source: event.Source,
	}
}

func automationEventFromSyncLog(log *domain.SyncLog) *domain.AutomationEvent {
	if log == nil {
		return nil
	}
	action := string(log.Action)
	switch log.Action {
	case domain.SyncLogActionSoftDelete:
		action = "delete"
	case domain.SyncLogActionDelete:
		action = "permanent_delete"
	}
	eventID := log.EventID
	if eventID == "" {
		if log.ID > 0 {
			eventID = fmt.Sprintf("sync-log:%d", log.ID)
		} else {
			eventID = uuid.NewString()
		}
	}
	return &domain.AutomationEvent{
		ID: eventID, OccurredAt: time.Time(log.CreatedAt), UID: log.UID, VaultID: log.VaultID,
		Type: domain.AutomationEventFileBehavior, Action: action, Path: log.Path, OldPath: log.OldPath, PathHash: log.PathHash,
		ChangedFields: splitChangedFields(log.ChangedFields), Size: log.Size,
		ClientType: log.ClientType, ClientName: log.ClientName, ClientVersion: log.ClientVersion,
		Source: "sync_log",
	}
}

func splitChangedFields(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func contentChangeEventFromAutomation(event *domain.AutomationEvent) *domain.ContentChangeEvent {
	return &domain.ContentChangeEvent{
		ID: event.ID, OccurredAt: event.OccurredAt, UID: event.UID, VaultID: event.VaultID, VaultName: event.VaultName,
		Resource: domain.WebhookResourceNote, Action: domain.WebhookAction(event.Action), Path: event.Path, OldPath: event.OldPath,
		PathHash: event.PathHash, ChangedFields: append([]string(nil), event.ChangedFields...), Content: event.Content,
		ContentHash: event.ContentHash, Size: event.Size, ClientType: event.ClientType, ClientName: event.ClientName,
		ClientVersion: event.ClientVersion, Source: event.Source,
	}
}

var _ AutomationService = (*automationService)(nil)
