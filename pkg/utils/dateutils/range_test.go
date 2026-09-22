package dateutils

import (
	"strings"
	"testing"
)

func TestExpandPeriodRange(t *testing.T) {
	cases := []struct {
		name       string
		start, end string
		want       []string
	}{
		{"months are walked inclusively", "2026-04", "2026-06", []string{"2026-04", "2026-05", "2026-06"}},
		{"a single month is itself", "2026-04", "2026-04", []string{"2026-04"}},
		{"days are walked across the month boundary", "2026-04-29", "2026-05-01", []string{"2026-04-29", "2026-04-30", "2026-05-01"}},
		{"an open start is the end alone", "", "2026-07", []string{"2026-07"}},
		{"an open end is the start alone", "2026-07", "", []string{"2026-07"}},
		{"no range at all is no periods", "", "", nil},
		{"surrounding space is not part of the period", " 2026-04 ", "2026-05 ", []string{"2026-04", "2026-05"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExpandPeriodRange(tc.start, tc.end)
			if err != nil {
				t.Fatalf("ExpandPeriodRange(%q, %q): %v", tc.start, tc.end, err)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// Every rejection here would otherwise turn into a sync over some other set of
// periods than the one that was asked for.
func TestExpandPeriodRangeRejects(t *testing.T) {
	cases := []struct {
		name       string
		start, end string
	}{
		{"an end before the start", "2026-09", "2026-04"},
		{"a month paired with a day", "2026-04", "2026-09-15"},
		{"a day paired with a month", "2026-04-01", "2026-09"},
		{"something that is not a period", "2026/04", "2026/09"},
		{"more periods than one task should hold", "2019-01-01", "2025-12-31"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ExpandPeriodRange(tc.start, tc.end); err == nil {
				t.Errorf("ExpandPeriodRange(%q, %q) should have failed", tc.start, tc.end)
			}
		})
	}
}
