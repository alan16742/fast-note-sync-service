package service

import (
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/robfig/cron/v3"
)

// AND schedules must match the same occurrence, including after downtime.
// Independent past occurrences are not evidence that an AND rule is due.
func nextAutomationCronOccurrence(trigger *domain.AutomationTrigger, after, now time.Time) (time.Time, bool) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	var schedules []cron.Schedule
	for _, rule := range trigger.Events {
		if rule.Type != domain.AutomationEventCron {
			continue
		}
		schedule, err := parser.Parse(rule.Schedule)
		if err != nil {
			if trigger.MatchMode == domain.AutomationMatchAll {
				return time.Time{}, false
			}
			continue
		}
		schedules = append(schedules, schedule)
	}
	if len(schedules) == 0 {
		return time.Time{}, false
	}
	if trigger.MatchMode != domain.AutomationMatchAll {
		var earliest time.Time
		for _, schedule := range schedules {
			next := schedule.Next(after)
			if !next.IsZero() && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
		return earliest, !earliest.IsZero() && !earliest.After(now)
	}
	candidate := schedules[0].Next(after)
	for !candidate.IsZero() && !candidate.After(now) {
		matched := true
		for _, schedule := range schedules {
			next := schedule.Next(candidate.Add(-time.Nanosecond))
			if next.IsZero() || next.After(now) {
				return time.Time{}, false
			}
			if next.After(candidate) {
				candidate, matched = next, false
				break
			}
		}
		if matched {
			return candidate, true
		}
	}
	return time.Time{}, false
}
