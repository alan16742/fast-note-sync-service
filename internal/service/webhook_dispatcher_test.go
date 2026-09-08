package service

import (
	"context"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/haierkeys/fast-note-sync-service/pkg/workerpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type webhookSubscriptionProvider struct {
	subscriptions []*domain.WebhookSubscription
}

type notificationSenderStub struct {
	endpoint   string
	credential string
	message    notification.Message
	delivered  chan struct{}
}

func (s *notificationSenderStub) Send(_ context.Context, endpoint, credential string, message notification.Message) error {
	s.endpoint, s.credential, s.message = endpoint, credential, message
	if s.delivered != nil {
		s.delivered <- struct{}{}
	}
	return nil
}

func TestWebhookDispatcher_DeliverUsesProviderSender(t *testing.T) {
	dispatcher := NewWebhookDispatcher(nil, nil, nil, zap.NewNop())
	stub := &notificationSenderStub{}
	dispatcher.senders[domain.WebhookProviderBark] = stub

	err := dispatcher.deliver(context.Background(), &domain.WebhookSubscription{Provider: domain.WebhookProviderBark, URL: "https://bark.example", Secret: "device-key"}, &domain.ContentChangeEvent{Action: domain.WebhookActionModify, Path: "note.md"})

	require.NoError(t, err)
	assert.Equal(t, "https://bark.example", stub.endpoint)
	assert.Equal(t, "device-key", stub.credential)
	assert.Equal(t, "Fast Note Sync: modify note.md", stub.message.Title)
}

func (p webhookSubscriptionProvider) ListEnabled(context.Context, int64) ([]*domain.WebhookSubscription, error) {
	return p.subscriptions, nil
}

func TestWebhookDispatcher_PublishQueuesOnlyMatchingSubscriptions(t *testing.T) {
	delivered := make(chan struct{}, 1)

	pool := workerpool.New(&workerpool.Config{MaxWorkers: 1, QueueSize: 1}, zap.NewNop())
	defer pool.Shutdown(context.Background())
	dispatcher := NewWebhookDispatcher(webhookSubscriptionProvider{subscriptions: []*domain.WebhookSubscription{
		{UID: 1, Enabled: true, Provider: domain.WebhookProviderBark, Actions: []domain.WebhookAction{domain.WebhookActionModify}},
		{UID: 1, Enabled: true, Provider: domain.WebhookProviderBark, Actions: []domain.WebhookAction{domain.WebhookActionDelete}},
	}}, pool, nil, zap.NewNop())
	dispatcher.senders[domain.WebhookProviderBark] = &notificationSenderStub{delivered: delivered}

	dispatcher.PublishNoteChange(context.Background(), &domain.ContentChangeEvent{UID: 1, Resource: domain.WebhookResourceNote, Action: domain.WebhookActionModify})
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for webhook delivery")
	}
}
