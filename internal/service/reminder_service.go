package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/reminder"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type reminderScan struct {
	fingerprint [32]byte
	timestamp   int64
}

// ReminderService implements the specialized todo-content trigger. The
// automation trigger owns the vault/path/timezone conditions and references
// notification channels as execution targets.
type ReminderService struct {
	users    domain.UserRepository
	vaults   domain.VaultRepository
	notes    domain.NoteRepository
	triggers domain.AutomationRepository
	webhooks WebhookService
	jobs     domain.ReminderRepository
	logger   *zap.Logger
	scans    map[string]reminderScan
	tickMu   sync.Mutex
	lifeMu   sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
}

func NewReminderService(users domain.UserRepository, vaults domain.VaultRepository, notes domain.NoteRepository, triggers domain.AutomationRepository, webhooks WebhookService, jobs domain.ReminderRepository, logger *zap.Logger) *ReminderService {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ReminderService{users: users, vaults: vaults, notes: notes, triggers: triggers, webhooks: webhooks, jobs: jobs, logger: logger, scans: map[string]reminderScan{}}
}

func (s *ReminderService) Start() {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			if err := s.Tick(ctx, time.Now()); err != nil && ctx.Err() == nil {
				s.logger.Warn("process todo triggers failed", zap.Error(err))
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *ReminderService) Shutdown(ctx context.Context) error {
	s.lifeMu.Lock()
	cancel, done := s.cancel, s.done
	s.lifeMu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ReminderService) Tick(ctx context.Context, now time.Time) error {
	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	users, err := s.users.GetAllUIDs(ctx)
	if err != nil {
		return err
	}
	activeScans := map[string]bool{}
	for _, uid := range users {
		if err := ctx.Err(); err != nil {
			return err
		}
		triggers, err := s.triggers.ListEnabled(ctx, uid, domain.AutomationEventTodoReminder)
		if err != nil {
			s.logger.Warn("load todo triggers failed", zap.Int64("uid", uid), zap.Error(err))
			continue
		}
		vaults, err := s.vaults.List(ctx, uid)
		if err != nil {
			s.logger.Warn("load todo vaults failed", zap.Int64("uid", uid), zap.Error(err))
			continue
		}
		for _, trigger := range triggers {
			for _, vault := range vaults {
				if vault.IsDeleted || (trigger.VaultID != 0 && trigger.VaultID != vault.ID) {
					continue
				}
				scanKey := formatReminderScanKey(uid, trigger.ID, vault.ID)
				activeScans[scanKey] = true
				if err := s.indexVault(ctx, uid, trigger, vault, scanKey, now); err != nil {
					s.logger.Warn("index todo trigger failed", zap.Int64("uid", uid), zap.Int64("triggerID", trigger.ID), zap.Error(err))
				}
			}
			jobs, err := s.jobs.ListDue(ctx, uid, trigger.ID, now.Unix(), 100)
			if err != nil {
				s.logger.Warn("load due todo reminders failed", zap.Int64("uid", uid), zap.Int64("triggerID", trigger.ID), zap.Error(err))
				continue
			}
			for _, job := range jobs {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := s.deliver(ctx, job, now); err != nil {
					s.logger.Warn("deliver todo reminder failed", zap.Int64("uid", uid), zap.Int64("job", job.ID), zap.Error(err))
				}
			}
		}
	}
	for key := range s.scans {
		if !activeScans[key] {
			delete(s.scans, key)
		}
	}
	return nil
}

func formatReminderScanKey(uid, triggerID, vaultID int64) string {
	return fmtInt(uid) + "/" + fmtInt(triggerID) + "/" + fmtInt(vaultID)
}

func fmtInt(value int64) string {
	// Avoid pulling formatting into the hot scan loop through fmt.Sprintf.
	return strconv.FormatInt(value, 10)
}

func (s *ReminderService) indexVault(ctx context.Context, uid int64, trigger *domain.AutomationTrigger, vault *domain.Vault, key string, now time.Time) error {
	encoded, _ := json.Marshal(trigger)
	fingerprint := sha256.Sum256(encoded)
	state := s.scans[key]
	if state.fingerprint != fingerprint {
		state.timestamp = 0
	}
	notes, err := s.notes.ListByUpdatedTimestampMeta(ctx, state.timestamp, vault.ID, uid)
	if err != nil {
		return err
	}
	for _, meta := range notes {
		if err := ctx.Err(); err != nil {
			return err
		}
		var jobs []domain.ReminderJob
		if !meta.IsDeleted() && strings.HasSuffix(strings.ToLower(meta.Path), ".md") {
			note, err := s.notes.GetByID(ctx, meta.ID, uid)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			if note != nil && !note.IsDeleted() && automationTriggerMatches(trigger, &domain.AutomationEvent{Type: domain.AutomationEventTodoReminder, UID: uid, VaultID: vault.ID, Path: note.Path, Content: note.Content}) {
				tasks, issues := reminder.Parse(note.Content, trigger.Timezone)
				for _, issue := range issues {
					s.logger.Warn("invalid todo annotation", zap.Int64("uid", uid), zap.Int64("note", note.ID), zap.Error(issue))
				}
				effective := trigger.CreatedAt
				if updated := time.UnixMilli(note.UpdatedTimestamp); updated.After(effective) {
					effective = updated
				}
				if effective.IsZero() {
					effective = now
				}
				for _, task := range tasks {
					if task.Completed {
						continue
					}
					job := domain.ReminderJob{Task: task}
					if next, ok := task.Next(effective.Truncate(time.Second).Add(-time.Second)); ok {
						job.NextAt, job.OccurrenceAt = next.At.Unix(), next.Occurrence.Unix()
					}
					jobs = append(jobs, job)
				}
			}
		}
		if err := s.jobs.SyncNote(ctx, uid, trigger.ID, meta.ID, jobs); err != nil {
			return err
		}
	}
	s.scans[key] = reminderScan{fingerprint: fingerprint, timestamp: now.Add(-2 * time.Second).UnixMilli()}
	return nil
}

func (s *ReminderService) deliver(ctx context.Context, job domain.ReminderJob, now time.Time) error {
	token := uuid.NewString()
	claimed, err := s.jobs.Claim(ctx, job.UID, job.ID, now.Unix(), token)
	if err != nil || !claimed {
		return err
	}
	cancel := func() error { return s.jobs.Cancel(ctx, job.UID, job.ID, token) }
	trigger, err := s.triggers.GetByID(ctx, job.TriggerID, job.UID)
	if err != nil {
		return s.retry(ctx, job, token, now, err)
	}
	if trigger == nil || !trigger.Enabled || !automationTriggerHasEvent(trigger, domain.AutomationEventTodoReminder) {
		return cancel()
	}
	note, err := s.notes.GetByID(ctx, job.NoteID, job.UID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return cancel()
	}
	if err != nil {
		return s.retry(ctx, job, token, now, err)
	}
	if note == nil || note.IsDeleted() || !strings.HasSuffix(strings.ToLower(note.Path), ".md") || !automationTriggerMatches(trigger, &domain.AutomationEvent{Type: domain.AutomationEventTodoReminder, UID: job.UID, VaultID: note.VaultID, Path: note.Path, Content: note.Content}) {
		return cancel()
	}
	vault, err := s.vaults.GetByID(ctx, note.VaultID, job.UID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return cancel()
	}
	if err != nil {
		return s.retry(ctx, job, token, now, err)
	}
	if vault == nil || vault.IsDeleted {
		return cancel()
	}
	tasks, _ := reminder.Parse(note.Content, trigger.Timezone)
	var current *reminder.Task
	for i := range tasks {
		if tasks[i].Key == job.Task.Key && !tasks[i].Completed {
			current = &tasks[i]
			break
		}
	}
	if current == nil || (current.Until != nil && now.After(*current.Until)) {
		return cancel()
	}
	location, _ := time.LoadLocation(current.Timezone)
	due := time.Unix(job.OccurrenceAt, 0).In(location).Format(reminder.DateLayout)
	link := "obsidian://open?" + url.Values{"vault": {vault.Name}, "file": {note.Path}}.Encode()
	var firstErr error
	for _, action := range trigger.Actions {
		if action.Type != domain.AutomationTargetWebhook {
			continue
		}
		if err := s.webhooks.DeliverReminder(ctx, job.UID, action.ConfigID, current.Title, due, current.Timezone, vault.Name, note.Path, note.Content, link); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return s.retry(ctx, job, token, now, firstErr)
	}
	var nextAt, occurrenceAt int64
	if next, ok := current.Next(now); ok {
		nextAt, occurrenceAt = next.At.Unix(), next.Occurrence.Unix()
	}
	return s.jobs.Finish(ctx, job.UID, job.ID, token, nextAt, occurrenceAt)
}

func (s *ReminderService) retry(ctx context.Context, job domain.ReminderJob, token string, now time.Time, cause error) error {
	attempts := min(job.Attempts, 7)
	delay := min(int64(30)<<attempts, int64(3600))
	if err := s.jobs.Retry(ctx, job.UID, job.ID, token, now.Unix()+delay); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}
