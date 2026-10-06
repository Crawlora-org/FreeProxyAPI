package monitor

import (
	"testing"
	"time"
)

func TestBudgetWindowDelayWaitsForNextMinuteWithJitter(t *testing.T) {
	base := time.Date(2026, 9, 14, 10, 30, 0, 0, time.UTC)
	cases := []struct {
		name   string
		now    time.Time
		jitter time.Duration
		want   time.Duration
	}{
		{name: "start of window", now: base, jitter: 0, want: time.Minute},
		{name: "mid window", now: base.Add(45 * time.Second), jitter: 0, want: 15 * time.Second},
		{name: "jitter is added", now: base.Add(45 * time.Second), jitter: 3 * time.Second, want: 18 * time.Second},
		{name: "floor applies at boundary", now: base.Add(time.Minute - 10*time.Millisecond), jitter: 0, want: 100 * time.Millisecond},
		{name: "jitter lifts delay past floor", now: base.Add(time.Minute - 10*time.Millisecond), jitter: time.Second, want: time.Second + 10*time.Millisecond},
		{name: "non-UTC input", now: base.Add(20 * time.Second).In(time.FixedZone("UTC+7", 7*3600)), jitter: 0, want: 40 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := budgetWindowDelay(tc.now, tc.jitter); got != tc.want {
				t.Fatalf("budgetWindowDelay = %v, want %v", got, tc.want)
			}
		})
	}
}
