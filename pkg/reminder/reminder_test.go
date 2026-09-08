package reminder

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func parseOne(t *testing.T, content string) Task {
	t.Helper()
	tasks, issues := Parse(content, "Asia/Shanghai")
	require.Empty(t, issues)
	require.Len(t, tasks, 1)
	return tasks[0]
}
func at(t *testing.T, value string) time.Time {
	t.Helper()
	result, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)
	return result
}

func TestParseTaskSemantics(t *testing.T) {
	task := parseOne(t, "* [ ] 测试 **待办** @(2026-09-06 22:45; remind=+1h,0,-1h,0; tz=Asia/Shanghai)")
	require.Equal(t, []int64{-3600, 0, 3600}, task.Remind)
	require.Equal(t, "测试 待办", task.Title)
	require.True(t, at(t, "2026-09-06T22:45:00+08:00").Equal(task.Due))
	tasks, issues := Parse("```text\n- [ ] example @(2026-09-06 22:45)\n```\n\n> - [ ] quoted @(2026-09-06 22:45)\n\n- [ ] Inline `@(2026-09-06 22:45)`\n\n- [x] done @(2026-09-06 22:45)", "Asia/Shanghai")
	require.Empty(t, issues)
	require.Len(t, tasks, 1)
	require.True(t, tasks[0].Completed)
	_, ok := tasks[0].Next(time.Time{})
	require.False(t, ok)
}

func TestDefaultTimezone(t *testing.T) {
	task := parseOne(t, "- [ ] test @(2026-09-06 22:45)")
	require.Equal(t, []int64{0}, task.Remind)
	tasks, issues := Parse("- [ ] test @(2026-09-06 22:45; tz=UTC)", "Asia/Shanghai")
	require.Empty(t, issues)
	require.Equal(t, at(t, "2026-09-06T22:45:00Z"), tasks[0].Due)
}

func TestUntilAndOffsetSequence(t *testing.T) {
	task := parseOne(t, "- [ ] test @(2026-09-06 22:45; remind=-1h,0,+1h; until=2026-09-07 01:00)")
	cursor := at(t, "2026-09-06T20:00:00+08:00")
	for _, expected := range []string{"2026-09-06T21:45:00+08:00", "2026-09-06T22:45:00+08:00", "2026-09-06T23:45:00+08:00", "2026-09-07T00:45:00+08:00"} {
		next, ok := task.Next(cursor)
		require.True(t, ok)
		require.True(t, at(t, expected).Equal(next.At), "got %s", next.At)
		cursor = next.At
	}
	_, ok := task.Next(cursor)
	require.False(t, ok)
	task.Until = nil
	_, ok = task.Next(at(t, "2026-09-06T23:45:00+08:00"))
	require.False(t, ok)
}

func TestInvalidMetadataDoesNotHideValidTasks(t *testing.T) {
	for _, field := range []string{"bad", "2026-09-06 22:45; remind=+1w", "2026-09-06 22:45; remind=999999999999999999d", "2026-09-06 22:45; 2026-09-07 22:45", "2026-09-06 22:45; tz=bad", "2026-09-06 22:45; until=2026-09-05 22:45", "due=bad"} {
		tasks, issues := Parse("- [ ] bad @("+field+")\n- [ ] good @(2026-09-06 22:45)", "Asia/Shanghai")
		require.Len(t, issues, 1, field)
		require.Len(t, tasks, 1, field)
	}
}
