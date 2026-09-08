package service

import (
	"path"
	"regexp"
	"strings"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
)

const defaultWebhookBodyMaxBytes = 64 * 1024

// MatchWebhookSubscription reports whether an event satisfies a subscription.
// It is intentionally independent of delivery so every future channel can reuse it.
func MatchWebhookSubscription(event *domain.ContentChangeEvent, subscription *domain.WebhookSubscription) bool {
	if event == nil || subscription == nil || !subscription.Enabled {
		return false
	}
	if subscription.Mode == domain.NotificationModeReminder {
		return false
	}
	if event.Resource != domain.WebhookResourceNote || event.UID != subscription.UID {
		return false
	}
	if subscription.VaultID != 0 && event.VaultID != subscription.VaultID {
		return false
	}
	if !matchesAction(event.Action, subscription.Actions) {
		return false
	}
	if !matchesPath(event, subscription) {
		return false
	}
	return matchesBody(event.Content, subscription.BodyMatcher)
}

func matchesAction(action domain.WebhookAction, actions []domain.WebhookAction) bool {
	if len(actions) == 0 {
		return true
	}
	for _, candidate := range actions {
		if candidate == action {
			return true
		}
	}
	return false
}

func matchesPath(event *domain.ContentChangeEvent, subscription *domain.WebhookSubscription) bool {
	paths := []string{event.Path}
	if event.OldPath != "" && event.OldPath != event.Path {
		paths = append(paths, event.OldPath)
	}
	for _, candidate := range paths {
		if subscription.PathPrefix != "" && strings.HasPrefix(candidate, subscription.PathPrefix) {
			return true
		}
		if subscription.PathGlob != "" {
			matched, err := path.Match(subscription.PathGlob, candidate)
			if err == nil && matched {
				return true
			}
		}
	}
	return subscription.PathPrefix == "" && subscription.PathGlob == ""
}

func matchesBody(content string, matcher domain.WebhookBodyMatcher) bool {
	if matcher.Substring == "" && matcher.Regex == "" {
		return true
	}
	maxBytes := matcher.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultWebhookBodyMaxBytes
	}
	limitedContent := content
	if len(limitedContent) > maxBytes {
		limitedContent = limitedContent[:maxBytes]
	}
	if matcher.Substring != "" && strings.Contains(limitedContent, matcher.Substring) {
		return true
	}
	if matcher.Regex != "" {
		compiled, err := regexp.Compile(matcher.Regex)
		return err == nil && compiled.MatchString(limitedContent)
	}
	return false
}
