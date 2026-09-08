package service

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/safego"
	"github.com/haierkeys/fast-note-sync-service/pkg/workerpool"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

const automationDefaultTimezone = "Asia/Shanghai"

// AutomationService is the central event-to-target coordinator. Events carry
// no target-specific configuration; triggers decide which existing targets are
// invoked after their conditions match.
type AutomationService interface {
	List(ctx context.Context, uid int64) ([]*dto.AutomationTriggerDTO, error)
	Save(ctx context.Context, uid int64, request *dto.AutomationTriggerRequest) (*dto.AutomationTriggerDTO, error)
	Delete(ctx context.Context, uid, id int64) error
	Trigger(ctx context.Context, uid int64, request *dto.AutomationRunRequest) error
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

type automationService struct {
	repo              domain.AutomationRepository
	backupService     BackupService
	gitSyncService    GitSyncService
	webhookService    WebhookService
	webhookDispatcher *WebhookDispatcher
	pool              *workerpool.Pool
	logger            *zap.Logger
	ctx               context.Context
	cancel            context.CancelFunc
	startOnce         sync.Once
	shutdownOnce      sync.Once
	eventMu           sync.Mutex
	stopped           bool
	eventWg           sync.WaitGroup
	doneCh            chan struct{}
}

// NewAutomationService creates the central automation coordinator.
func NewAutomationService(
	repo domain.AutomationRepository,
	backupService BackupService,
	gitSyncService GitSyncService,
	webhookService WebhookService,
	webhookDispatcher *WebhookDispatcher,
	pool *workerpool.Pool,
	logger *zap.Logger,
) AutomationService {
	if logger == nil {
		logger = zap.NewNop()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &automationService{
		repo: repo, backupService: backupService, gitSyncService: gitSyncService,
		webhookService: webhookService, webhookDispatcher: webhookDispatcher,
		pool: pool, logger: logger, ctx: ctx, cancel: cancel, doneCh: make(chan struct{}),
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
	}
	saved, err := s.repo.Save(ctx, trigger, uid)
	if err != nil {
		return nil, err
	}
	return automationToDTO(saved), nil
}

func (s *automationService) Delete(ctx context.Context, uid, id int64) error {
	if id <= 0 {
		return errors.New("automation trigger id is required")
	}
	return s.repo.Delete(ctx, id, uid)
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
	if trigger.EventType != domain.AutomationEventManual {
		return errors.New("only manual triggers can be run manually")
	}
	event := &domain.AutomationEvent{
		ID: uuid.NewString(), OccurredAt: time.Now().UTC(), UID: uid,
		VaultID: request.VaultID, Type: domain.AutomationEventManual,
		Action: "manual", Source: "manual_api",
	}
	if event.VaultID == 0 {
		event.VaultID = trigger.VaultID
	}
	if !automationTriggerMatches(trigger, event) {
		return errors.New("manual event does not match automation trigger conditions")
	}
	return s.dispatchTrigger(ctx, trigger, event)
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
	triggers, err := s.repo.ListEnabled(ctx, event.UID, event.Type)
	if err != nil {
		s.logger.Warn("load automation triggers failed", zap.Int64("uid", event.UID), zap.String("eventType", string(event.Type)), zap.Error(err))
		return
	}
	for _, trigger := range triggers {
		if !automationTriggerMatches(trigger, event) {
			continue
		}
		if err := s.dispatchTrigger(context.Background(), trigger, event); err != nil {
			s.logger.Warn("automation trigger dispatch failed", zap.Int64("uid", event.UID), zap.Int64("triggerID", trigger.ID), zap.Error(err))
		}
	}
}

// PublishNoteChange keeps the existing notification dispatcher behind the
// central publisher while also exposing the same note event to automation
// triggers.
func (s *automationService) PublishNoteChange(ctx context.Context, event *domain.ContentChangeEvent) {
	if s == nil || event == nil {
		return
	}
	if s.webhookDispatcher != nil {
		s.webhookDispatcher.PublishNoteChange(ctx, event)
	}
	s.Publish(ctx, automationEventFromNote(event))
}

func (s *automationService) dispatchTrigger(ctx context.Context, trigger *domain.AutomationTrigger, event *domain.AutomationEvent) error {
	if trigger == nil || event == nil {
		return nil
	}
	var firstErr error
	for _, action := range trigger.Actions {
		action := action
		submit := func(taskCtx context.Context) error {
			return s.executeAction(taskCtx, event, action)
		}
		if s.pool == nil {
			if err := submit(ctx); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			}
			continue
		}
		if err := s.pool.SubmitAsync(context.Background(), submit); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (s *automationService) executeAction(ctx context.Context, event *domain.AutomationEvent, action domain.AutomationAction) error {
	if action.ConfigID <= 0 {
		return errors.New("automation target config id is required")
	}
	switch strings.ToLower(strings.TrimSpace(action.Type)) {
	case domain.AutomationTargetBackup:
		if s.backupService == nil {
			return errors.New("backup service is unavailable")
		}
		return s.backupService.ExecuteUserBackup(ctx, event.UID, action.ConfigID)
	case domain.AutomationTargetGit:
		if s.gitSyncService == nil {
			return errors.New("git sync service is unavailable")
		}
		return s.gitSyncService.ExecuteSync(ctx, event.UID, action.ConfigID)
	case domain.AutomationTargetWebhook:
		if s.webhookService == nil {
			return errors.New("webhook service is unavailable")
		}
		return s.webhookService.DeliverEvent(ctx, event.UID, action.ConfigID, contentChangeEventFromAutomation(event))
	default:
		return fmt.Errorf("unsupported automation target: %s", action.Type)
	}
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
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	defer close(s.doneCh)
	s.pollTimeTriggers()
	for {
		select {
		case <-ticker.C:
			s.pollTimeTriggers()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *automationService) pollTimeTriggers() {
	triggers, err := s.repo.ListEnabledByType(s.ctx, domain.AutomationEventTime)
	if err != nil {
		s.logger.Warn("load time automation triggers failed", zap.Error(err))
		return
	}
	now := time.Now()
	for _, trigger := range triggers {
		location, err := time.LoadLocation(trigger.Timezone)
		if err != nil {
			location = time.Local
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		schedule, err := parser.Parse(trigger.Schedule)
		if err != nil {
			s.logger.Warn("invalid automation schedule", zap.Int64("triggerID", trigger.ID), zap.Error(err))
			continue
		}
		localNow := now.In(location)
		last := trigger.LastRunAt.In(location)
		if trigger.LastRunAt.IsZero() {
			last = localNow.Add(-time.Minute)
		}
		if schedule.Next(last).After(localNow) {
			continue
		}
		event := &domain.AutomationEvent{
			ID: uuid.NewString(), OccurredAt: now.UTC(), UID: trigger.UID,
			VaultID: trigger.VaultID, Type: domain.AutomationEventTime,
			Action: "time", Source: "automation_clock",
		}
		if err := s.dispatchTrigger(context.Background(), trigger, event); err != nil {
			s.logger.Warn("dispatch time automation trigger failed", zap.Int64("triggerID", trigger.ID), zap.Error(err))
			continue
		}
		if err := s.repo.MarkRun(s.ctx, trigger.ID, trigger.UID, now); err != nil {
			s.logger.Warn("mark automation trigger run failed", zap.Int64("triggerID", trigger.ID), zap.Error(err))
		}
	}
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
	if request.ID < 0 || request.VaultID < 0 {
		return nil, errors.New("invalid automation trigger or vault id")
	}
	eventType := domain.AutomationEventType(strings.ToLower(strings.TrimSpace(string(request.EventType))))
	switch eventType {
	case domain.AutomationEventTime, domain.AutomationEventContent, domain.AutomationEventManual, domain.AutomationEventFile:
	default:
		return nil, fmt.Errorf("unsupported automation event type: %s", request.EventType)
	}
	name := strings.TrimSpace(request.Name)
	if name == "" {
		return nil, errors.New("automation trigger name is required")
	}
	if len(name) > 120 {
		return nil, errors.New("automation trigger name is too long")
	}
	timezone := strings.TrimSpace(request.Timezone)
	if timezone == "" {
		timezone = automationDefaultTimezone
	}
	if _, err := time.LoadLocation(timezone); err != nil {
		return nil, errors.New("invalid IANA timezone")
	}
	schedule := strings.TrimSpace(request.Schedule)
	if eventType == domain.AutomationEventTime {
		if schedule == "" {
			return nil, errors.New("time triggers require a cron schedule")
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if _, err := parser.Parse(schedule); err != nil {
			return nil, fmt.Errorf("invalid cron schedule: %w", err)
		}
	} else {
		schedule = ""
	}
	pathGlob := strings.TrimSpace(request.PathGlob)
	if eventType != domain.AutomationEventContent && eventType != domain.AutomationEventFile {
		pathGlob = ""
	}
	if pathGlob != "" {
		if _, err := path.Match(pathGlob, ""); err != nil {
			return nil, errors.New("invalid path glob")
		}
	}
	contentContains := strings.TrimSpace(request.ContentContains)
	pathPrefix := strings.TrimSpace(request.PathPrefix)
	if eventType != domain.AutomationEventContent {
		contentContains = ""
	}
	if eventType != domain.AutomationEventContent && eventType != domain.AutomationEventFile {
		pathPrefix = ""
	}
	eventActions, err := normalizeEventActions(request.EventActions)
	if err != nil {
		return nil, err
	}
	if eventType != domain.AutomationEventContent && eventType != domain.AutomationEventFile {
		eventActions = nil
	}
	if len(contentContains) > 4096 || len(pathPrefix) > 4096 || len(pathGlob) > 4096 {
		return nil, errors.New("automation condition is too long")
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
	if len(actions) == 0 {
		return nil, errors.New("automation trigger requires at least one target")
	}
	return &domain.AutomationTrigger{
		ID: request.ID, UID: uid, Name: name, Enabled: request.Enabled, EventType: eventType,
		VaultID: request.VaultID, Timezone: timezone, Schedule: schedule,
		ContentContains: contentContains, PathPrefix: pathPrefix,
		PathGlob: pathGlob, EventActions: eventActions, Actions: actions,
	}, nil
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
	if trigger == nil || event == nil || !trigger.Enabled || trigger.EventType != event.Type {
		return false
	}
	if trigger.VaultID > 0 && trigger.VaultID != event.VaultID {
		return false
	}
	if len(trigger.EventActions) > 0 && !containsString(trigger.EventActions, event.Action) {
		return false
	}
	if trigger.ContentContains != "" && !strings.Contains(event.Content, trigger.ContentContains) {
		return false
	}
	pathValue := event.Path
	if pathValue == "" {
		pathValue = event.OldPath
	}
	if trigger.PathPrefix != "" && !strings.HasPrefix(pathValue, trigger.PathPrefix) {
		return false
	}
	if trigger.PathGlob != "" {
		matched, err := path.Match(trigger.PathGlob, pathValue)
		if err != nil || !matched {
			return false
		}
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
	result := &dto.AutomationTriggerDTO{
		ID: trigger.ID, UID: trigger.UID, Name: trigger.Name, Enabled: trigger.Enabled,
		EventType: trigger.EventType, VaultID: trigger.VaultID, Timezone: trigger.Timezone,
		Schedule: trigger.Schedule, ContentContains: trigger.ContentContains, PathPrefix: trigger.PathPrefix,
		PathGlob: trigger.PathGlob, EventActions: append([]string(nil), trigger.EventActions...), Actions: actions,
		CreatedAt: trigger.CreatedAt.Format(time.RFC3339), UpdatedAt: trigger.UpdatedAt.Format(time.RFC3339),
	}
	if !trigger.LastRunAt.IsZero() {
		result.LastRunAt = trigger.LastRunAt.Format(time.RFC3339)
	}
	return result
}

func automationEventFromNote(event *domain.ContentChangeEvent) *domain.AutomationEvent {
	return &domain.AutomationEvent{
		ID: event.ID, OccurredAt: event.OccurredAt, UID: event.UID, VaultID: event.VaultID,
		VaultName: event.VaultName, Type: domain.AutomationEventContent, Action: string(event.Action),
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
	return &domain.AutomationEvent{
		ID: uuid.NewString(), OccurredAt: time.Time(log.CreatedAt), UID: log.UID, VaultID: log.VaultID,
		Type: domain.AutomationEventFile, Action: action, Path: log.Path, PathHash: log.PathHash,
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
