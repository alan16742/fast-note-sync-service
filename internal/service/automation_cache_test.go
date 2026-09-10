package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/stretchr/testify/require"
)

type automationCacheRepository struct {
	domain.AutomationRepository
	list func() ([]*domain.AutomationTrigger, error)
}

func (r *automationCacheRepository) ListEnabled(context.Context, int64, domain.AutomationEventType) ([]*domain.AutomationTrigger, error) {
	return r.list()
}

func TestAutomationCacheCoalescesConcurrentMisses(t *testing.T) {
	t.Run("concurrent readers", func(t *testing.T) {
		var calls atomic.Int32
		release := make(chan struct{})
		repo := &automationCacheRepository{list: func() ([]*domain.AutomationTrigger, error) {
			calls.Add(1)
			<-release
			return []*domain.AutomationTrigger{{ID: 1}}, nil
		}}
		svc := NewAutomationService(repo, nil, nil, nil, nil, nil, nil).(*automationService)
		var wg sync.WaitGroup
		for range 32 {
			wg.Go(func() {
				_, _ = svc.listEnabledCached(context.Background(), 1, domain.AutomationEventNoteContent)
			})
		}
		time.AfterFunc(20*time.Millisecond, func() { close(release) })
		wg.Wait()
		require.EqualValues(t, 1, calls.Load())
	})
}

func TestAutomationCacheInvalidationCannotBeOverwrittenByInflightQuery(t *testing.T) {
	t.Run("invalidation waits for query", func(t *testing.T) {
		var calls atomic.Int32
		release := make(chan struct{})
		started := make(chan struct{})
		repo := &automationCacheRepository{list: func() ([]*domain.AutomationTrigger, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
				return []*domain.AutomationTrigger{{ID: 1}}, nil
			}
			return nil, nil // The rule was deleted while the first query was running.
		}}
		svc := NewAutomationService(repo, nil, nil, nil, nil, nil, nil).(*automationService)
		queried := make(chan struct{})
		go func() {
			_, _ = svc.listEnabledCached(context.Background(), 1, domain.AutomationEventNoteContent)
			close(queried)
		}()
		<-started
		time.AfterFunc(20*time.Millisecond, func() { close(release) })
		svc.invalidateRulesCache(1)
		<-queried
		rows, err := svc.listEnabledCached(context.Background(), 1, domain.AutomationEventNoteContent)
		require.NoError(t, err)
		require.Empty(t, rows)
		require.EqualValues(t, 2, calls.Load())
	})
}
