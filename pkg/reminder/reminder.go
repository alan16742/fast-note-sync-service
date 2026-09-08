// Package reminder turns Markdown task metadata into timezone-aware schedules.
package reminder

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	extast "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/text"
)

const DateLayout = "2006-01-02 15:04"
const DefaultTimezone = "Asia/Shanghai"
const maxOffset = 366 * 24 * 60 * 60

var scheduleMetadata = regexp.MustCompile(`@\(([^()]*)\)`)
var offsetPattern = regexp.MustCompile(`^([+-]?)([0-9]+)([dhm])$`)

type Task struct {
	Key       string     `json:"key"`
	Title     string     `json:"title"`
	Completed bool       `json:"completed"`
	Due       time.Time  `json:"due"`
	Remind    []int64    `json:"remind"`
	Until     *time.Time `json:"until,omitempty"`
	Timezone  string     `json:"tz"`
}

type Notification struct {
	At         time.Time
	Occurrence time.Time
	Offset     int64
}

// Parse only visits actual task checkboxes, ignoring fenced code and quotations.
// Invalid metadata is reported separately so one bad task cannot hide valid ones.
func Parse(markdown, timezone string) ([]Task, []error) {
	if timezone == "" {
		timezone = DefaultTimezone
	}
	source := []byte(markdown)
	doc := goldmark.New(goldmark.WithExtensions(extension.TaskList)).Parser().Parse(text.NewReader(source))
	tasks := make([]Task, 0)
	var issues []error
	duplicates := map[string]int{}
	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		if n.Kind() == ast.KindBlockquote {
			return ast.WalkSkipChildren, nil
		}
		checkbox, ok := n.(*extast.TaskCheckBox)
		if !ok {
			return ast.WalkContinue, nil
		}
		var content strings.Builder
		_ = ast.Walk(n.Parent(), func(inline ast.Node, enter bool) (ast.WalkStatus, error) {
			if !enter {
				return ast.WalkContinue, nil
			}
			if inline.Kind() == ast.KindCodeSpan || inline.Kind() == ast.KindRawHTML {
				return ast.WalkSkipChildren, nil
			}
			switch value := inline.(type) {
			case *ast.Text:
				content.Write(value.Segment.Value(source))
				if value.SoftLineBreak() || value.HardLineBreak() {
					content.WriteByte(' ')
				}
			case *ast.String:
				content.Write(value.Value)
			}
			return ast.WalkContinue, nil
		})
		value := content.String()
		scheduleMatches := scheduleMetadata.FindAllStringSubmatchIndex(value, -1)
		if len(scheduleMatches) == 0 {
			return ast.WalkContinue, nil
		}
		if len(scheduleMatches) != 1 {
			issues = append(issues, errors.New("task must contain one schedule annotation"))
			return ast.WalkContinue, nil
		}
		match := scheduleMatches[0]
		title := removeRanges(value, []textRange{{start: match[0], end: match[1]}})
		task, err := parseFields(value[match[2]:match[3]], title, timezone)
		if err != nil {
			issues = append(issues, fmt.Errorf("task %q: %w", title, err))
			return ast.WalkContinue, nil
		}
		keyData, _ := json.Marshal(task)
		hash := sha256.Sum256(keyData)
		key := hex.EncodeToString(hash[:])
		duplicates[key]++
		task.Key = fmt.Sprintf("%s:%d", key, duplicates[key])
		task.Completed = checkbox.IsChecked
		tasks = append(tasks, task)
		return ast.WalkContinue, nil
	})
	return tasks, issues
}

type textRange struct{ start, end int }

func removeRanges(value string, ranges []textRange) string {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start > ranges[j].start })
	for _, item := range ranges {
		value = value[:item.start] + value[item.end:]
	}
	return strings.TrimSpace(value)
}

func parseFields(raw, title, timezone string) (Task, error) {
	fields := map[string]string{}
	parts := strings.Split(raw, ";")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return Task{}, errors.New("task date is required")
	}
	dateValue := strings.TrimSpace(parts[0])
	if strings.Contains(dateValue, "=") {
		return Task{}, errors.New("task date must be the first value after @(")
	}
	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		if part == "" {
			return Task{}, errors.New("task field must not be empty")
		}
		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 {
			return Task{}, errors.New("task fields must use key=value")
		}
		key := strings.TrimSpace(pair[0])
		switch key {
		case "remind", "until", "tz":
		default:
			return Task{}, fmt.Errorf("unknown field %q", key)
		}
		if _, exists := fields[key]; exists {
			return Task{}, fmt.Errorf("duplicate field %q", key)
		}
		fields[key] = strings.TrimSpace(pair[1])
	}
	if fields["tz"] != "" {
		timezone = fields["tz"]
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return Task{}, errors.New("invalid IANA timezone")
	}
	due, err := parseDate(dateValue, location)
	if err != nil {
		return Task{}, fmt.Errorf("date: %w", err)
	}
	task := Task{Title: title, Due: due, Timezone: timezone, Remind: []int64{0}}
	if rawOffsets, exists := fields["remind"]; exists {
		if rawOffsets == "" {
			return Task{}, errors.New("remind must not be empty")
		}
		task.Remind = nil
		for _, rawOffset := range strings.Split(rawOffsets, ",") {
			offset, err := parseOffset(strings.TrimSpace(rawOffset))
			if err != nil {
				return Task{}, err
			}
			task.Remind = append(task.Remind, offset)
		}
		if len(task.Remind) > 128 {
			return Task{}, errors.New("too many reminder offsets")
		}
		sort.Slice(task.Remind, func(i, j int) bool { return task.Remind[i] < task.Remind[j] })
		task.Remind = unique(task.Remind)
	}
	if rawUntil, exists := fields["until"]; exists {
		until, err := parseDate(rawUntil, location)
		if err != nil || until.Before(due) {
			return Task{}, errors.New("until must be a valid time at or after due")
		}
		task.Until = &until
	}
	return task, nil
}

func parseDate(value string, location *time.Location) (time.Time, error) {
	parsed, err := time.ParseInLocation(DateLayout, value, location)
	if err != nil || parsed.Format(DateLayout) != value {
		return time.Time{}, errors.New("expected YYYY-MM-DD HH:mm in the selected timezone")
	}
	return parsed, nil
}

func parseOffset(value string) (int64, error) {
	if value == "0" {
		return 0, nil
	}
	match := offsetPattern.FindStringSubmatch(value)
	if match == nil {
		return 0, fmt.Errorf("invalid reminder offset %q", value)
	}
	count, err := strconv.ParseInt(match[2], 10, 64)
	unit := map[string]int64{"d": 86400, "h": 3600, "m": 60}[match[3]]
	if err != nil || count > maxOffset/unit {
		return 0, errors.New("reminder offset exceeds 366 days")
	}
	if match[1] == "-" {
		count = -count
	}
	return count * unit, nil
}

func unique(values []int64) []int64 {
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

// Next returns the first notification strictly after after. until extends the
// last positive offset.
func (t Task) Next(after time.Time) (Notification, bool) {
	if t.Completed || len(t.Remind) == 0 || (t.Until != nil && !t.Until.After(after)) {
		return Notification{}, false
	}
	location, err := time.LoadLocation(t.Timezone)
	if err != nil {
		return Notification{}, false
	}
	due := t.Due.In(location)
	best := Notification{}
	consider := func(at, occurrence time.Time) {
		if !at.After(after) || (t.Until != nil && at.After(*t.Until)) {
			return
		}
		if best.At.IsZero() || at.Before(best.At) {
			best = Notification{At: at, Occurrence: occurrence, Offset: int64(at.Sub(occurrence) / time.Second)}
		}
	}
	for _, offset := range t.Remind {
		occurrence := due
		duration := time.Duration(offset) * time.Second
		consider(occurrence.Add(duration), occurrence)
	}
	last := t.Remind[len(t.Remind)-1]
	if t.Until != nil && last > 0 {
		occurrence := due
		elapsed := int64(after.Sub(occurrence) / time.Second)
		step := elapsed/last + 1
		if step < 1 {
			step = 1
		}
		at := occurrence.Add(time.Duration(step*last) * time.Second)
		consider(at, occurrence)
	}
	return best, !best.At.IsZero()
}
