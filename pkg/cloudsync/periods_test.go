package cloudsync

import (
	"context"
	"testing"
	"time"
)

func pairsOf(periods []*PeriodInfo) []string {
	out := make([]string, 0, len(periods))
	for _, p := range periods {
		out = append(out, p.Period+" "+p.Granularity)
	}
	return out
}

func equal(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// The bug this guards: a run asked for a range over both granularities used to
// collapse into the current month, monthly, twice — so the daily table stayed
// empty however the request was written.
func TestDeterminePeriodsCrossesTheRangeWithTheGranularities(t *testing.T) {
	executor := &BaseCloudSyncExecutor{provider: &stubProvider{}}

	periods, err := executor.determinePeriods(&SyncConfig{
		StartPeriod: "2026-04",
		EndPeriod:   "2026-06",
		Granularity: "both",
	})
	if err != nil {
		t.Fatalf("determinePeriods: %v", err)
	}

	want := []string{
		"2026-04 monthly", "2026-04 daily",
		"2026-05 monthly", "2026-05 daily",
		"2026-06 monthly", "2026-06 daily",
	}
	if got := pairsOf(periods); !equal(got, want) {
		t.Errorf("periods = %v, want %v", got, want)
	}
}

func TestDeterminePeriodsSources(t *testing.T) {
	thisMonth := time.Now().Format("2006-01")

	cases := []struct {
		name   string
		config SyncConfig
		want   []string
	}{
		{
			name:   "a single period beats the default",
			config: SyncConfig{BillPeriod: "2026-08", Granularity: "monthly"},
			want:   []string{"2026-08 monthly"},
		},
		{
			name:   "nothing given means the current month",
			config: SyncConfig{Granularity: "monthly"},
			want:   []string{thisMonth + " monthly"},
		},
		{
			// A day has no monthly bill, so "both" must not queue one.
			name:   "a dated period is daily whatever the field says",
			config: SyncConfig{BillPeriod: "2026-08-15", Granularity: "both"},
			want:   []string{"2026-08-15 daily"},
		},
		{
			name:   "an explicit list wins over the range",
			config: SyncConfig{Periods: []string{"2026-01"}, StartPeriod: "2026-05", EndPeriod: "2026-09", Granularity: "daily"},
			want:   []string{"2026-01 daily"},
		},
		{
			name:   "a one-sided range is that one period",
			config: SyncConfig{StartPeriod: "2026-03", Granularity: "monthly"},
			want:   []string{"2026-03 monthly"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor := &BaseCloudSyncExecutor{provider: &stubProvider{}}
			periods, err := executor.determinePeriods(&tc.config)
			if err != nil {
				t.Fatalf("determinePeriods: %v", err)
			}
			if got := pairsOf(periods); !equal(got, tc.want) {
				t.Errorf("periods = %v, want %v", got, tc.want)
			}
		})
	}
}

// A provider with one table says so by answering nil for the granularities it
// does not have, and must then be asked for that period once, not twice.
func TestDeterminePeriodsSkipsGranularitiesTheProviderLacks(t *testing.T) {
	executor := &BaseCloudSyncExecutor{provider: &stubProvider{granularities: []string{"monthly"}}}

	periods, err := executor.determinePeriods(&SyncConfig{BillPeriod: "2026-08", Granularity: "both"})
	if err != nil {
		t.Fatalf("determinePeriods: %v", err)
	}
	if got := pairsOf(periods); !equal(got, []string{"2026-08 monthly"}) {
		t.Errorf("periods = %v, want one monthly pull", got)
	}

	// Asking such a provider for the table it does not have is a mistake worth
	// reporting, not an empty run that reports success.
	if _, err := executor.determinePeriods(&SyncConfig{BillPeriod: "2026-08-15", Granularity: "daily"}); err == nil {
		t.Error("a daily-only request to a monthly-only provider should fail")
	}
}

// A range nobody can serve has to come back as an error: syncing a silently
// different set of periods than the one asked for is worse than refusing.
func TestDeterminePeriodsRejectsABadRange(t *testing.T) {
	executor := &BaseCloudSyncExecutor{provider: &stubProvider{}}

	for _, tc := range []struct {
		name       string
		start, end string
	}{
		{"end before start", "2026-09", "2026-04"},
		{"mixed formats", "2026-04", "2026-09-15"},
		{"not a period at all", "april", "2026-09"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := executor.determinePeriods(&SyncConfig{StartPeriod: tc.start, EndPeriod: tc.end}); err == nil {
				t.Errorf("range %s..%s should have been rejected", tc.start, tc.end)
			}
		})
	}
}

// The granularity has to reach the provider, because for AliCloud that is what
// picks the table.
func TestSyncPeriodsPassesTheGranularityToTheProvider(t *testing.T) {
	provider := &stubProvider{}
	executor := &BaseCloudSyncExecutor{provider: provider}

	periods := []*PeriodInfo{
		{Period: "2026-08", Granularity: "monthly"},
		{Period: "2026-08", Granularity: "daily"},
	}
	if _, err := executor.syncPeriods(context.Background(), periods, &SyncConfig{}); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	want := []string{"2026-08 monthly", "2026-08 daily"}
	if !equal(provider.syncedPairs, want) {
		t.Errorf("provider saw %v, want %v", provider.syncedPairs, want)
	}
}
