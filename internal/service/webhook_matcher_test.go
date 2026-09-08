package service

import (
	"testing"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestMatchWebhookSubscription(t *testing.T) {
	event := &domain.ContentChangeEvent{
		UID:      7,
		VaultID:  3,
		Resource: domain.WebhookResourceNote,
		Action:   domain.WebhookActionRename,
		Path:     "docs/new.md",
		OldPath:  "notes/old.md",
		Content:  "deploy after review",
	}

	tests := []struct {
		name         string
		subscription domain.WebhookSubscription
		want         bool
	}{
		{
			name: "matches new path and body",
			subscription: domain.WebhookSubscription{
				UID: 7, Enabled: true, VaultID: 3,
				Actions:     []domain.WebhookAction{domain.WebhookActionRename},
				PathGlob:    "docs/*.md",
				BodyMatcher: domain.WebhookBodyMatcher{Substring: "deploy"},
			},
			want: true,
		},
		{
			name: "matches old path on rename",
			subscription: domain.WebhookSubscription{
				UID: 7, Enabled: true, PathPrefix: "notes/",
			},
			want: true,
		},
		{
			name: "body is limited before matching",
			subscription: domain.WebhookSubscription{
				UID: 7, Enabled: true,
				BodyMatcher: domain.WebhookBodyMatcher{Substring: "review", MaxBytes: 8},
			},
			want: false,
		},
		{
			name: "invalid regex does not match",
			subscription: domain.WebhookSubscription{
				UID: 7, Enabled: true,
				BodyMatcher: domain.WebhookBodyMatcher{Regex: "["},
			},
			want: false,
		},
		{
			name:         "disabled subscription does not match",
			subscription: domain.WebhookSubscription{UID: 7},
			want:         false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, MatchWebhookSubscription(event, &test.subscription))
		})
	}
}

func TestMatchWebhookSubscription_RejectsDifferentUserOrResource(t *testing.T) {
	subscription := &domain.WebhookSubscription{UID: 7, Enabled: true}

	assert.False(t, MatchWebhookSubscription(&domain.ContentChangeEvent{
		UID: 8, Resource: domain.WebhookResourceNote,
	}, subscription))
	assert.False(t, MatchWebhookSubscription(&domain.ContentChangeEvent{
		UID: 7, Resource: domain.WebhookResource("file"),
	}, subscription))
}
