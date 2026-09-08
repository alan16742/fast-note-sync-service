package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/bark"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/serverchan"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/webhook"
	"github.com/haierkeys/fast-note-sync-service/pkg/reminder"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type reminderScan struct {
	fingerprint [32]byte
	timestamp   int64
}

// ReminderService indexes changed notes, stores one next delivery per task, and
// rechecks the source immediately before sending. Provider calls stay outside DB
// transactions and the note synchronization path.
type ReminderService struct {
	users         domain.UserRepository
	vaults        domain.VaultRepository
	notes         domain.NoteRepository
	subscriptions domain.WebhookRepository
	jobs          domain.ReminderRepository
	senders       map[string]notification.Sender
	logger        *zap.Logger
	scans         map[string]reminderScan
	tickMu        sync.Mutex
	lifeMu        sync.Mutex
	cancel        context.CancelFunc
	done          chan struct{}
}

func NewReminderService(users domain.UserRepository, vaults domain.VaultRepository, notes domain.NoteRepository, subscriptions domain.WebhookRepository, jobs domain.ReminderRepository, logger *zap.Logger) *ReminderService {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ReminderService{users: users, vaults: vaults, notes: notes, subscriptions: subscriptions, jobs: jobs, logger: logger, scans: map[string]reminderScan{}, senders: map[string]notification.Sender{
		domain.WebhookProviderServerChan: serverchan.NewClient(nil),
		domain.WebhookProviderBark:       bark.NewClient(nil),
		domain.WebhookProviderCustom:     webhook.NewClient(nil),
	}}
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
				s.logger.Warn("process task reminders failed", zap.Error(err))
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
		subscriptions, err := s.subscriptions.ListEnabled(ctx, uid)
		if err != nil {
			s.logger.Warn("load reminder channels failed", zap.Int64("uid", uid), zap.Error(err))
			continue
		}
		var vaults []*domain.Vault
		for _, subscription := range subscriptions {
			if subscription.Mode != domain.NotificationModeReminder {
				continue
			}
			if vaults == nil {
				vaults, err = s.vaults.List(ctx, uid)
				if err != nil {
					s.logger.Warn("load reminder vaults failed", zap.Int64("uid", uid), zap.Error(err))
					break
				}
			}
			for _, vault := range vaults {
				if vault.IsDeleted || (subscription.VaultID != 0 && subscription.VaultID != vault.ID) {
					continue
				}
				scanKey := fmt.Sprintf("%d/%d/%d", uid, subscription.ID, vault.ID)
				activeScans[scanKey] = true
				if err := s.indexVault(ctx, uid, subscription, vault, scanKey, now); err != nil {
					s.logger.Warn("index reminders failed", zap.Int64("uid", uid), zap.Int64("subscription", subscription.ID), zap.Error(err))
				}
			}
			jobs, err := s.jobs.ListDue(ctx, uid, subscription.ID, now.Unix(), 100)
			if err != nil {
				s.logger.Warn("load due reminders failed", zap.Int64("uid", uid), zap.Error(err))
				continue
			}
			for _, job := range jobs {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := s.deliver(ctx, job, now); err != nil {
					s.logger.Warn("deliver reminder failed", zap.Int64("uid", uid), zap.Int64("job", job.ID), zap.Error(err))
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

func (s *ReminderService) indexVault(ctx context.Context, uid int64, subscription *domain.WebhookSubscription, vault *domain.Vault, key string, now time.Time) error {
	encoded, _ := json.Marshal(subscription)
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
		if !meta.IsDeleted() && strings.HasSuffix(strings.ToLower(meta.Path), ".md") && matchesPath(&domain.ContentChangeEvent{Path: meta.Path}, subscription) {
			note, err := s.notes.GetByID(ctx, meta.ID, uid)
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			if note == nil || note.IsDeleted() {
				continue
			}
			tasks, issues := reminder.Parse(note.Content, subscription.Timezone)
			for _, issue := range issues {
				s.logger.Warn("invalid task reminder", zap.Int64("uid", uid), zap.Int64("note", note.ID), zap.Error(issue))
			}
			effective := subscription.CreatedAt
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
		if err := s.jobs.SyncNote(ctx, uid, subscription.ID, meta.ID, jobs); err != nil {
			return err
		}
	}
	// Revisit the boundary to cover writes sharing a timestamp with this scan.
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
	subscription, err := s.subscriptions.GetByID(ctx, job.SubscriptionID, job.UID)
	if err != nil {
		return s.retry(ctx, job, token, now, err)
	}
	if subscription == nil || !subscription.Enabled || subscription.Mode != domain.NotificationModeReminder {
		return cancel()
	}
	note, err := s.notes.GetByID(ctx, job.NoteID, job.UID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return cancel()
	}
	if err != nil {
		return s.retry(ctx, job, token, now, err)
	}
	if note == nil || note.IsDeleted() || !strings.HasSuffix(strings.ToLower(note.Path), ".md") || (subscription.VaultID != 0 && subscription.VaultID != note.VaultID) || !matchesPath(&domain.ContentChangeEvent{Path: note.Path}, subscription) {
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
	tasks, _ := reminder.Parse(note.Content, subscription.Timezone)
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
	sender := s.senders[subscription.Provider]
	if sender == nil {
		return cancel()
	}
	location, _ := time.LoadLocation(current.Timezone)
	due := time.Unix(job.OccurrenceAt, 0).In(location).Format(reminder.DateLayout)
	link := "obsidian://open?" + url.Values{"vault": {vault.Name}, "file": {note.Path}}.Encode()
	message := messageForReminder(subscription, current.Title, due, current.Timezone, vault.Name, note.Path, note.Content, link)
	sendCtx, stop := context.WithTimeout(ctx, notification.Timeout)
	err = sendNotification(sendCtx, sender, subscription, message)
	stop()
	if err != nil {
		return s.retry(ctx, job, token, now, err)
	}
	var nextAt, occurrenceAt int64
	// A restart catches up at most one pending reminder per task, then resumes
	// from now; this avoids replaying hours of overdue notifications in a burst.
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
