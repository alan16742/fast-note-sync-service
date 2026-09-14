package service

import (
	"context"
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/stretchr/testify/require"
)

type automationBackupTargetStub struct {
	BackupService
	configs []*dto.BackupConfigDTO
}

func (s *automationBackupTargetStub) GetConfigs(context.Context, int64) ([]*dto.BackupConfigDTO, error) {
	return s.configs, nil
}

type automationGitTargetStub struct {
	GitSyncService
	configs []*dto.GitSyncConfigDTO
}

func (s *automationGitTargetStub) GetConfigs(context.Context, int64) ([]*dto.GitSyncConfigDTO, error) {
	return s.configs, nil
}

type automationWebhookTargetStub struct {
	WebhookService
	channels []*dto.WebhookSubscriptionDTO
}

func (s *automationWebhookTargetStub) List(context.Context, int64) ([]*dto.WebhookSubscriptionDTO, error) {
	return s.channels, nil
}

func TestValidateAutomationTargetsRequiresExistingUserOwnedTargets(t *testing.T) {
	svc := &automationService{
		backupService:  &automationBackupTargetStub{configs: []*dto.BackupConfigDTO{{ID: 1}}},
		gitSyncService: &automationGitTargetStub{configs: []*dto.GitSyncConfigDTO{{ID: 2}}},
		webhookService: &automationWebhookTargetStub{channels: []*dto.WebhookSubscriptionDTO{{ID: 3}}},
	}
	actions := []domain.AutomationAction{
		{Type: domain.AutomationTargetBackup, ConfigID: 1},
		{Type: domain.AutomationTargetGit, ConfigID: 2},
		{Type: domain.AutomationTargetWebhook, ConfigID: 3},
	}
	require.NoError(t, svc.validateAutomationTargets(context.Background(), 42, actions))

	err := svc.validateAutomationTargets(context.Background(), 42, append(actions[:2], domain.AutomationAction{Type: domain.AutomationTargetWebhook, ConfigID: 99}))
	require.EqualError(t, err, "webhook target #99 was not found for this user")
}
