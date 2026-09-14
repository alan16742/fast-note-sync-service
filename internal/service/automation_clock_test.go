package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextAutomationMinuteBoundary(t *testing.T) {
	location := time.FixedZone("CST", 8*60*60)
	tests := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{
			name: "during minute",
			now:  time.Date(2026, time.September, 14, 12, 34, 3, 500000000, location),
			want: time.Date(2026, time.September, 14, 12, 35, 0, 0, location),
		},
		{
			name: "at minute boundary",
			now:  time.Date(2026, time.September, 14, 12, 34, 0, 0, location),
			want: time.Date(2026, time.September, 14, 12, 35, 0, 0, location),
		},
		{
			name: "last second",
			now:  time.Date(2026, time.September, 14, 12, 34, 59, 999999999, location),
			want: time.Date(2026, time.September, 14, 12, 35, 0, 0, location),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, nextAutomationMinuteBoundary(test.now))
		})
	}
}
