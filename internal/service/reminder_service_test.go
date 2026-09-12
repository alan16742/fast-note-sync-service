package service

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/haierkeys/fast-note-sync-service/internal/dao"
	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type reminderUsers struct{ domain.UserRepository }

func (reminderUsers) GetAllUIDs(context.Context) ([]int64, error) { return []int64{1}, nil }

type reminderVaults struct {
	domain.VaultRepository
	deleted bool
}

func (v *reminderVaults) List(context.Context, int64) ([]*domain.Vault, error) {
	return []*domain.Vault{{ID: 1, Name: "vault", IsDeleted: v.deleted}}, nil
}
func (v *reminderVaults) GetByID(context.Context, int64, int64) (*domain.Vault, error) {
	return &domain.Vault{ID: 1, Name: "vault", IsDeleted: v.deleted}, nil
}

type reminderNotes struct {
	domain.NoteRepository
	note *domain.Note
}

func (n *reminderNotes) ListByUpdatedTimestampMeta(_ context.Context, stamp, _, _ int64) ([]*domain.Note, error) {
	if n.note == nil || n.note.UpdatedTimestamp <= stamp {
		return nil, nil
	}
	return []*domain.Note{n.note}, nil
}
func (n *reminderNotes) GetByID(context.Context, int64, int64) (*domain.Note, error) {
	if n.note == nil {
		return nil, gorm.ErrRecordNotFound
	}
	return n.note, nil
}

type reminderSender struct {
	calls []notification.Message
	err   error
}

func (s *reminderSender) Send(_ context.Context, _, _ string, message notification.Message) error {
	s.calls = append(s.calls, message)
	return s.err
}

type reminderTestEnv struct {
	svc      *ReminderService
	source   *reminderNotes
	vaults   *reminderVaults
	triggers domain.AutomationRepository
	webhooks domain.WebhookRepository
	jobs     domain.ReminderRepository
	sender   *reminderSender
	start    time.Time
	trigger  *domain.AutomationTrigger
}

func newReminderTestEnv(t *testing.T) *reminderTestEnv {
	t.Helper()
	ctx := context.Background()
	queue := false
	cfg := config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(t.TempDir(), "db.sqlite3"), EnableWriteQueue: &queue}
	db, err := dao.NewEngine(cfg, zap.NewNop())
	require.NoError(t, err)
	encryptor, err := util.NewDataEncryptor("reminder test database key-fixture-0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	d := dao.New(db, ctx, dao.WithConfig(&cfg), dao.WithUserDatabaseConfig(&cfg), dao.WithLogger(zap.NewNop()), dao.WithDataEncryptor(encryptor))
	t.Cleanup(func() {
		for _, key := range []string{"", "user_webhook_1", "user_automation_1", "user_reminder_1", "user_webhook_2", "user_automation_2", "user_reminder_2"} {
			if sqlDB, err := d.ResolveDB(key).DB(); err == nil {
				_ = sqlDB.Close()
			}
		}
	})
	webhooks := dao.NewWebhookRepository(d)
	target, err := webhooks.Save(ctx, &domain.WebhookSubscription{UID: 1, Provider: "bark", Secret: "test-key"}, 1)
	require.NoError(t, err, "first operation must migrate before saving")
	start := time.Date(2026, 9, 6, 14, 40, 0, 0, time.UTC)
	triggers := dao.NewAutomationRepository(d)
	trigger, err := triggers.Save(ctx, &domain.AutomationTrigger{UID: 1, Enabled: true, VaultID: 1, Timezone: "Asia/Shanghai", Events: []domain.AutomationEventRule{{Type: domain.AutomationEventTodoReminder}}, Actions: []domain.AutomationAction{{Type: domain.AutomationTargetWebhook, ConfigID: target.ID}}}, 1)
	require.NoError(t, err)
	require.NoError(t, d.ResolveDB("user_automation_1").Table("automation_rule").Where("id = ?", trigger.ID).Update("created_at", start).Error)
	trigger, err = triggers.GetByID(ctx, trigger.ID, 1)
	require.NoError(t, err)
	notes := &reminderNotes{note: &domain.Note{ID: 1, VaultID: 1, Path: "todo.md", Content: "- [ ] todo @(2026-09-06 22:45; remind=0,+1h)", UpdatedTimestamp: start.UnixMilli()}}
	vaults := &reminderVaults{}
	jobs := dao.NewReminderRepository(d)
	sender := &reminderSender{}
	webhookSvc := NewWebhookService(webhooks).(*webhookService)
	webhookSvc.senders["bark"] = sender
	svc := NewReminderService(reminderUsers{}, vaults, notes, triggers, webhookSvc, jobs, zap.NewNop())
	return &reminderTestEnv{svc: svc, source: notes, vaults: vaults, triggers: triggers, webhooks: webhooks, jobs: jobs, sender: sender, start: start, trigger: trigger}
}

func TestReminderPersistenceCompletionAndTenantIsolation(t *testing.T) {
	env := newReminderTestEnv(t)
	ctx := context.Background()
	require.NoError(t, env.svc.Tick(ctx, env.start))
	require.Empty(t, env.sender.calls)
	due := env.start.Add(5 * time.Minute)
	require.NoError(t, env.svc.Tick(ctx, due))
	require.Len(t, env.sender.calls, 1)
	require.Equal(t, "todo", env.sender.calls[0].Title)
	require.Contains(t, env.sender.calls[0].ObsidianURI, "obsidian://open?")
	restarted := NewReminderService(reminderUsers{}, env.vaults, env.source, env.triggers, NewWebhookService(env.webhooks), env.jobs, zap.NewNop())
	restarted.webhooks.(*webhookService).senders["bark"] = env.sender
	require.NoError(t, restarted.Tick(ctx, due.Add(time.Second)))
	require.Len(t, env.sender.calls, 1, "restart must preserve delivery progress")
	env.source.note.Content = "- [x] todo @(2026-09-06 22:45; remind=0,+1h)"
	env.source.note.UpdatedTimestamp = due.Add(time.Minute).UnixMilli()
	require.NoError(t, restarted.Tick(ctx, due.Add(time.Hour)))
	require.Len(t, env.sender.calls, 1, "completion cancels future sends")
	rows, err := env.jobs.ListDue(ctx, 2, env.trigger.ID, due.Add(time.Hour).Unix(), 100)
	require.NoError(t, err)
	require.Empty(t, rows)
	other, err := env.triggers.GetByID(ctx, env.trigger.ID, 2)
	require.NoError(t, err)
	require.Nil(t, other)
}

func TestReminderRetriesAndRechecksSource(t *testing.T) {
	for _, reason := range []string{"completed", "deleted", "disabled", "vault_deleted"} {
		t.Run(reason, func(t *testing.T) {
			env := newReminderTestEnv(t)
			ctx := context.Background()
			require.NoError(t, env.svc.Tick(ctx, env.start))
			due := env.start.Add(5 * time.Minute)
			env.sender.err = errors.New("temporary failure")
			require.NoError(t, env.svc.Tick(ctx, due))
			require.Len(t, env.sender.calls, 1)
			require.NoError(t, env.svc.Tick(ctx, due.Add(10*time.Second)))
			require.Len(t, env.sender.calls, 1, "retry must back off")
			switch reason {
			case "completed":
				env.source.note.Content = "- [x] todo @(2026-09-06 22:45; remind=0,+1h)"
			case "deleted":
				env.source.note = nil
			case "disabled":
				env.trigger.Enabled = false
				_, err := env.triggers.Save(ctx, env.trigger, 1)
				require.NoError(t, err)
			case "vault_deleted":
				env.vaults.deleted = true
			}
			require.NoError(t, env.svc.Tick(ctx, due.Add(time.Minute)))
			require.Len(t, env.sender.calls, 1, "must recheck the source even when its index has not changed")
		})
	}
}

func TestReminderLeasePreventsConcurrentDelivery(t *testing.T) {
	env := newReminderTestEnv(t)
	ctx := context.Background()
	require.NoError(t, env.svc.Tick(ctx, env.start))
	now := env.start.Add(5 * time.Minute).Unix()
	rows, err := env.jobs.ListDue(ctx, 1, env.trigger.ID, now, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	claimed, err := env.jobs.Claim(ctx, 1, rows[0].ID, now, "first")
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = env.jobs.Claim(ctx, 1, rows[0].ID, now, "second")
	require.NoError(t, err)
	require.False(t, claimed)
	require.NoError(t, env.jobs.Finish(ctx, 1, rows[0].ID, "wrong-token", 0, 0))
	claimed, err = env.jobs.Claim(ctx, 1, rows[0].ID, now, "third")
	require.NoError(t, err)
	require.False(t, claimed)
	require.NoError(t, env.jobs.Finish(ctx, 1, rows[0].ID, "first", 0, 0))
	rows, err = env.jobs.ListDue(ctx, 1, env.trigger.ID, now+61, 10)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestWebhookRepositoryContextAndRoundTrip(t *testing.T) {
	env := newReminderTestEnv(t)
	item, err := NewWebhookService(env.webhooks).Save(context.Background(), 1, &dto.WebhookSubscriptionRequest{Provider: "bark", Secret: "new-key", TitleTemplate: "{{task}}", BodyTemplate: "{{vault}}/{{path}}"})
	require.NoError(t, err)
	require.Equal(t, "{{task}}", item.TitleTemplate)
	require.Equal(t, "{{vault}}/{{path}}", item.BodyTemplate)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = env.webhooks.List(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
}
