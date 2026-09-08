package service

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/bark"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/serverchan"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification/webhook"
	"github.com/haierkeys/fast-note-sync-service/pkg/workerpool"
	"go.uber.org/zap"
)

// WebhookSubscriptionProvider supplies enabled subscriptions for one user.
type WebhookSubscriptionProvider interface {
	ListEnabled(ctx context.Context, uid int64) ([]*domain.WebhookSubscription, error)
}

// WebhookDispatcher delivers matching note-change events through notification providers.
type WebhookDispatcher struct {
	provider WebhookSubscriptionProvider
	pool     *workerpool.Pool
	logger   *zap.Logger
	senders  map[string]notification.Sender
}

// NewWebhookDispatcher creates a bounded asynchronous webhook dispatcher.
func NewWebhookDispatcher(provider WebhookSubscriptionProvider, pool *workerpool.Pool, client *http.Client, logger *zap.Logger) *WebhookDispatcher {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	client = notification.HTTPClient(client)
	return &WebhookDispatcher{provider: provider, pool: pool, logger: logger, senders: map[string]notification.Sender{
		domain.WebhookProviderServerChan: serverchan.NewClient(client),
		domain.WebhookProviderBark:       bark.NewClient(client),
		domain.WebhookProviderCustom:     webhook.NewClient(client),
	}}
}

// PublishNoteChange queues matching deliveries and intentionally has no error return.
func (d *WebhookDispatcher) PublishNoteChange(ctx context.Context, event *domain.ContentChangeEvent) {
	if d == nil || d.provider == nil || d.pool == nil || event == nil {
		return
	}

	subscriptions, err := d.provider.ListEnabled(ctx, event.UID)
	if err != nil {
		d.logger.Warn("load webhook subscriptions failed", zap.Int64("uid", event.UID), zap.Error(err))
		return
	}
	for _, subscription := range subscriptions {
		if !MatchWebhookSubscription(event, subscription) {
			continue
		}
		subscription := subscription
		err := d.pool.SubmitAsync(context.Background(), func(taskCtx context.Context) error {
			return d.deliver(taskCtx, subscription, event)
		})
		if err != nil {
			d.logger.Warn("webhook delivery dropped", zap.Int64("uid", event.UID), zap.Error(err))
		}
	}
}

func (d *WebhookDispatcher) deliver(ctx context.Context, subscription *domain.WebhookSubscription, event *domain.ContentChangeEvent) error {
	sender, ok := d.senders[subscription.Provider]
	if !ok {
		return fmt.Errorf("unsupported webhook provider: %s", subscription.Provider)
	}
	return sendNotification(ctx, sender, subscription, messageForNoteEvent(event, subscription))
}
