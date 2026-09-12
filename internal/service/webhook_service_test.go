package service

import (
	"context"
	"encoding/json"
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
	service := NewWebhookService(&webhookRepositoryStub{item: &domain.WebhookSubscription{
		ID: 1, UID: 2, URL: "https://example.com/hook?token=query-secret&title={{content}}", Secret: "hidden",
		Headers: map[string]string{"Authorization": "Bearer hidden", "X-Source": "fast-note-sync"},
	}})
	items, err := service.List(context.Background(), 2)

	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.True(t, items[0].HasSecret)
	assert.Equal(t, "https://example.com/hook?token=&title={{content}}", items[0].URL)
	assert.Equal(t, "", items[0].Headers["Authorization"])
	assert.Equal(t, "fast-note-sync", items[0].Headers["X-Source"])
	encodedHeaders, err := json.Marshal(items[0].Headers)
	require.NoError(t, err)
	assert.NotContains(t, string(encodedHeaders), "Bearer hidden")
	assert.NotContains(t, items[0].URL, "query-secret")
}

func TestWebhookSavePreservesRedactedCredentials(t *testing.T) {
	repo := &webhookRepositoryStub{item: &domain.WebhookSubscription{
		ID: 1, UID: 2, Provider: domain.WebhookProviderCustom,
		URL:     "https://example.com/hook?token=query-secret&title={{content}}",
		Headers: map[string]string{"Authorization": "Bearer header-secret"},
	}}
	svc := NewWebhookService(repo)
	_, err := svc.Save(context.Background(), 2, &dto.WebhookSubscriptionRequest{
		ID: 1, Provider: domain.WebhookProviderCustom,
		URL: "https://example.com/hook?token=&title={{content}}", Method: "POST",
		Headers: map[string]string{"Authorization": "", "X-Source": "fast-note-sync"},
	})
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/hook?token=query-secret&title={{content}}", repo.item.URL)
	assert.Equal(t, "Bearer header-secret", repo.item.Headers["Authorization"])
	assert.Equal(t, "fast-note-sync", repo.item.Headers["X-Source"])
}

func TestWebhookSaveDefaultsAndCredentialSwitch(t *testing.T) {
	ctx := context.Background()
	repo := &webhookRepositoryStub{}
	svc := NewWebhookService(repo)
	request := &dto.WebhookSubscriptionRequest{Provider: "bark", Secret: "device-key"}
	item, err := svc.Save(ctx, 2, request)
	require.NoError(t, err)
	assert.Equal(t, "https://api.day.app", item.URL)
	assert.Equal(t, defaultNoteTitleTemplate, item.TitleTemplate)
	assert.Equal(t, defaultNoteBodyTemplate, item.BodyTemplate)
	assert.Equal(t, "", request.URL, "normalization must not mutate the caller")
	repo.item.ID = 1
	_, err = svc.Save(ctx, 2, &dto.WebhookSubscriptionRequest{ID: 1, Provider: "serverchan"})
	require.ErrorContains(t, err, "new credential")
	_, err = svc.Save(ctx, 2, &dto.WebhookSubscriptionRequest{ID: 1, Provider: "bark"})
	require.NoError(t, err)
	assert.Equal(t, "device-key", repo.item.Secret)
	for _, request := range []*dto.WebhookSubscriptionRequest{
		{Provider: domain.WebhookProviderCustom, URL: "https://example.com", Method: "PATCH"},
		{Provider: "serverchan"},
	} {
		_, err := svc.Save(ctx, 2, request)
		require.Error(t, err)
	}
}

func TestWebhookServiceTestUsesSavedTemplates(t *testing.T) {
	repo := &webhookRepositoryStub{item: &domain.WebhookSubscription{
		ID: 1, UID: 2, Provider: domain.WebhookProviderBark, Secret: "device-key",
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
		Provider: domain.WebhookProviderBark, URL: "https://example.com", Secret: "device-key",
		TitleTemplate: "{{action}}", BodyTemplate: "{{vault}}|{{path}}|{{content}}",
	}))
	assert.Equal(t, "modify", sender.message.Title)
	assert.Equal(t, "测试笔记库|test-note.md|这是一条测试通知。", sender.message.Body)
}

func TestWebhookHeaderCredentialLifecycle(t *testing.T) {
	repo := &webhookRepositoryStub{item: &domain.WebhookSubscription{
		ID: 1, UID: 2, Provider: "custom", URL: "https://example.com/hook",
		Headers: map[string]string{"Authorization": "old-secret", "X-Source": "source", "X-Token": ""},
	}}
	svc := NewWebhookService(repo)
	items, err := svc.List(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, []string{"Authorization"}, items[0].ProtectedHeaders)
	request := &dto.WebhookSubscriptionRequest{ID: 1, Provider: "custom", URL: repo.item.URL, Headers: map[string]string{"authorization": ""}}
	_, err = svc.Save(context.Background(), 2, request)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"authorization": "old-secret"}, repo.item.Headers)
	require.Empty(t, request.Headers["authorization"], "must not put secrets in the caller's request")
	request.Headers["authorization"] = "replacement"
	item, err := svc.Save(context.Background(), 2, request)
	require.NoError(t, err)
	require.Empty(t, item.Headers["authorization"])
	require.Equal(t, "replacement", repo.item.Headers["authorization"])
	request.Headers = map[string]string{}
	_, err = svc.Save(context.Background(), 2, request)
	require.NoError(t, err)
	require.Empty(t, repo.item.Headers)
}

func TestWebhookSaveAndTestRejectUnsafeHeadersBeforeNormalization(t *testing.T) {
	for name, headers := range map[string]map[string]string{
		"case duplicate": {"Authorization": "one", "authorization": "two"},
		"trim collision": {"Authorization": "one", " Authorization ": "two"},
		"newline":        {"Authorization": "secret\r\n"},
		"nul":            {"Authorization": "secret\x00"},
		"del":            {"Authorization": "secret\x7f"},
	} {
		t.Run(name, func(t *testing.T) {
			repo := &webhookRepositoryStub{}
			svc := NewWebhookService(repo)
			request := &dto.WebhookSubscriptionRequest{Provider: "custom", URL: "https://example.com", Headers: headers}
			_, err := svc.Save(context.Background(), 2, request)
			require.Error(t, err)
			require.Error(t, svc.TestRequest(context.Background(), 2, request))
			require.Nil(t, repo.item)
		})
	}
}

func TestWebhookCannotForwardSavedHeadersToChangedEndpoint(t *testing.T) {
	for _, endpoint := range []string{"https://other.example/hook", "http://example.com/hook", "https://example.com/other"} {
		t.Run(endpoint, func(t *testing.T) {
			repo := &webhookRepositoryStub{item: &domain.WebhookSubscription{ID: 1, UID: 2, Provider: "custom", URL: "https://example.com/hook", Headers: map[string]string{"Authorization": "secret"}}}
			svc := NewWebhookService(repo)
			request := &dto.WebhookSubscriptionRequest{ID: 1, Provider: "custom", URL: endpoint, Headers: map[string]string{"Authorization": ""}}
			_, err := svc.Save(context.Background(), 2, request)
			require.ErrorContains(t, err, "re-enter")
			require.ErrorContains(t, svc.TestRequest(context.Background(), 2, request), "re-enter")
			require.Equal(t, "https://example.com/hook", repo.item.URL)
			request.Headers["Authorization"] = "new-secret"
			_, err = svc.Save(context.Background(), 2, request)
			require.NoError(t, err)
			require.Equal(t, "new-secret", repo.item.Headers["Authorization"])
		})
	}
}
