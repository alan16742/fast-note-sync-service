package service

import (
	"context"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/notification"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateWebhookRequest(t *testing.T) {
	tests := []struct {
		name    string
		request *dto.WebhookSubscriptionRequest
		valid   bool
	}{
		{name: "valid https", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderBark, Secret: "device-key", URL: "https://example.com/push"}, valid: true},
		{name: "reject unsupported scheme", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderBark, Secret: "device-key", URL: "ftp://example.com/push"}},
		{name: "reject localhost", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderBark, Secret: "device-key", URL: "http://localhost/push"}},
		{name: "reject private ip", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderBark, Secret: "device-key", URL: "http://127.0.0.1/push"}},
		{name: "reject invalid regex", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderBark, Secret: "device-key", URL: "https://example.com/push", BodyRegex: "["}},
		{name: "reject unknown action", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderBark, Secret: "device-key", URL: "https://example.com/push", Actions: []domain.WebhookAction{"unknown"}}},
		{name: "serverchan uses credential instead of url", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderServerChan, Secret: "sctp123tTestKey"}, valid: true},
		{name: "custom webhook", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderCustom, URL: "https://example.com/hook", Method: "POST", Headers: map[string]string{"Authorization": "Bearer token"}}, valid: true},
		{name: "custom webhook requires url", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderCustom, Method: "GET"}},
		{name: "custom webhook rejects restricted header", request: &dto.WebhookSubscriptionRequest{Provider: domain.WebhookProviderCustom, URL: "https://example.com/hook", Method: "POST", Headers: map[string]string{"Host": "internal"}}},
		{name: "reject unknown provider", request: &dto.WebhookSubscriptionRequest{Provider: "unknown", URL: "https://example.com/hook"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.valid, validateWebhookRequest(test.request) == nil)
		})
	}
}

type webhookRepositoryStub struct {
	item *domain.WebhookSubscription
}

func (s *webhookRepositoryStub) List(context.Context, int64) ([]*domain.WebhookSubscription, error) {
	return []*domain.WebhookSubscription{s.item}, nil
}
func (s *webhookRepositoryStub) ListEnabled(context.Context, int64) ([]*domain.WebhookSubscription, error) {
	return []*domain.WebhookSubscription{s.item}, nil
}
func (s *webhookRepositoryStub) GetByID(context.Context, int64, int64) (*domain.WebhookSubscription, error) {
	return s.item, nil
}
func (s *webhookRepositoryStub) Save(_ context.Context, item *domain.WebhookSubscription, _ int64) (*domain.WebhookSubscription, error) {
	s.item = item
	return item, nil
}
func (s *webhookRepositoryStub) Delete(context.Context, int64, int64) error { return nil }

type webhookTestSender struct {
	message notification.Message
}

func (s *webhookTestSender) Send(_ context.Context, _, _ string, message notification.Message) error {
	s.message = message
	return nil
}

func TestWebhookService_ListMasksSecret(t *testing.T) {
	service := NewWebhookService(&webhookRepositoryStub{item: &domain.WebhookSubscription{ID: 1, UID: 2, URL: "https://example.com", Secret: "hidden"}})
	items, err := service.List(context.Background(), 2)

	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.True(t, items[0].HasSecret)
}

func TestWebhookSaveDefaultsAndCredentialSwitch(t *testing.T) {
	ctx := context.Background()
	repo := &webhookRepositoryStub{}
	svc := NewWebhookService(repo)
	request := &dto.WebhookSubscriptionRequest{Provider: "bark", Mode: "reminder", Secret: "device-key"}
	item, err := svc.Save(ctx, 2, request)
	require.NoError(t, err)
	assert.Equal(t, "https://api.day.app", item.URL)
	assert.Equal(t, "Asia/Shanghai", item.Timezone)
	assert.Equal(t, defaultReminderTitleTemplate, item.TitleTemplate)
	assert.Equal(t, defaultReminderBodyTemplate, item.BodyTemplate)
	assert.Equal(t, "", request.URL, "normalization must not mutate the caller")
	repo.item.ID = 1
	_, err = svc.Save(ctx, 2, &dto.WebhookSubscriptionRequest{ID: 1, Provider: "serverchan"})
	require.ErrorContains(t, err, "new credential")
	_, err = svc.Save(ctx, 2, &dto.WebhookSubscriptionRequest{ID: 1, Provider: "bark", Mode: "reminder"})
	require.NoError(t, err)
	assert.Equal(t, "device-key", repo.item.Secret)
	for _, request := range []*dto.WebhookSubscriptionRequest{
		{Provider: "bark", Mode: "reminder", Secret: "key", Timezone: "not/a/timezone"},
		{Provider: "bark", Secret: "key", PathGlob: "["},
		{Provider: "serverchan", Secret: "invalid"},
	} {
		_, err := svc.Save(ctx, 2, request)
		require.Error(t, err)
	}
}

func TestWebhookServiceTestUsesSavedTemplates(t *testing.T) {
	repo := &webhookRepositoryStub{item: &domain.WebhookSubscription{
		ID: 1, UID: 2, Provider: domain.WebhookProviderBark, Secret: "device-key", Mode: domain.NotificationModeNoteChange,
		TitleTemplate: "Test {{action}} {{path}}", BodyTemplate: "{{vault}}/{{path}}: {{content}}",
	}}
	svc := NewWebhookService(repo).(*webhookService)
	sender := &webhookTestSender{}
	svc.senders[domain.WebhookProviderBark] = sender

	require.NoError(t, svc.Test(context.Background(), 2, 1))
	assert.Equal(t, "Test modify test-note.md", sender.message.Title)
	assert.Equal(t, "测试笔记库/test-note.md: 这是一条测试通知。", sender.message.Body)
}

func TestWebhookServiceTestRequestUsesDraftTemplates(t *testing.T) {
	repo := &webhookRepositoryStub{}
	svc := NewWebhookService(repo).(*webhookService)
	sender := &webhookTestSender{}
	svc.senders[domain.WebhookProviderBark] = sender

	require.NoError(t, svc.TestRequest(context.Background(), 2, &dto.WebhookSubscriptionRequest{
		Provider: domain.WebhookProviderBark, Mode: domain.NotificationModeReminder, URL: "https://example.com", Secret: "device-key",
		Timezone: "UTC", TitleTemplate: "{{title}}", BodyTemplate: "{{due}}|{{vault}}|{{path}}",
	}))
	assert.Equal(t, "测试待办", sender.message.Title)
	assert.Equal(t, "2026-01-01 09:00|测试笔记库|test-note.md", sender.message.Body)
}
