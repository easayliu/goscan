package dateutils

import (
	"testing"
	"time"
)

// Every month length, including the 31-day months the old loop never left.
func TestGenerateDatesInMonthStaysInTheMonth(t *testing.T) {
	// Past months only: a billing cycle in the future does not validate.
	cases := map[string]int{
		"2026-01": 31,
		"2026-02": 28,
		"2024-02": 29,
		"2026-04": 30,
		"2026-08": 31,
		"2025-12": 31,
	}
	for cycle, days := range cases {
		done := make(chan []string, 1)
		go func() {
			dates, err := GenerateDatesInMonth(cycle)
			if err != nil {
				t.Errorf("%s: %v", cycle, err)
			}
			done <- dates
		}()

		select {
		case dates := <-done:
			if len(dates) != days {
				t.Errorf("%s: %d dates, want %d", cycle, len(dates), days)
				continue
			}
			if dates[0] != cycle+"-01" {
				t.Errorf("%s: starts at %s", cycle, dates[0])
			}
			if last := dates[len(dates)-1]; last[:7] != cycle {
				t.Errorf("%s: ends at %s, outside the month", cycle, last)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: did not return — the loop runs past the end of the month", cycle)
		}
	}
}
