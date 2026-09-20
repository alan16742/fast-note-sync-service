package service

import (
	"context"
	"fmt"
	"sync"
)

// Manual runs and retries belong to the same lifecycle as published events.
func (s *automationService) beginAutomationRequest(ctx context.Context) (context.Context, func(), error) {
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if s.stopped {
		return nil, nil, context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.eventWg.Add(1)
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	return requestCtx, func() {
		stop()
		cancel()
		s.eventWg.Done()
	}, nil
}

// A cancelled Submit can return while its worker is still executing. Do not
// expose a retryable result until that action has actually stopped, and prevent
// a queued action from starting after its caller has already returned.
func (s *automationService) runAutomationAction(ctx context.Context, action func(context.Context) error) error {
	run := func(ctx context.Context) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("automation action panicked: %v", recovered)
			}
		}()
		if err := ctx.Err(); err != nil {
			return err
		}
		return action(ctx)
	}
	if s.pool == nil {
		return run(ctx)
	}
	var mu sync.Mutex
	started, abandoned := false, false
	done := make(chan error, 1)
	err := s.pool.Submit(ctx, func(taskCtx context.Context) error {
		mu.Lock()
		if abandoned {
			mu.Unlock()
			return context.Canceled
		}
		started = true
		mu.Unlock()
		result := run(taskCtx)
		done <- result
		return result
	})
	mu.Lock()
	if !started {
		abandoned = true
		mu.Unlock()
		return err
	}
	mu.Unlock()
	return <-done
}
