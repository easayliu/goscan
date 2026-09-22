package cloudsync

import (
	"context"
	"fmt"
	"testing"
)

// stubProvider is a CloudProvider that does nothing but remember which periods
// it was asked to sync, and fail the ones it is told to.
type stubProvider struct {
	synced []string
	failOn map[string]bool
}

func (p *stubProvider) GetProviderName() string                          { return "stub" }
func (p *stubProvider) ValidateCredentials(context.Context) error        { return nil }
func (p *stubProvider) Close() error                                     { return nil }
func (p *stubProvider) GetPeriodField() string                           { return "period" }
func (p *stubProvider) GetTableConfig(string) *TableConfig               { return &TableConfig{} }
func (p *stubProvider) CreateTables(context.Context, *TableConfig) error { return nil }

func (p *stubProvider) GetAPIDataCount(context.Context, string, string) (int64, error) {
	return 0, nil
}

func (p *stubProvider) FetchBillData(context.Context, *FetchRequest) (*FetchResult, error) {
	return &FetchResult{}, nil
}

func (p *stubProvider) SyncPeriodData(_ context.Context, period string, _ *SyncOptions) error {
	p.synced = append(p.synced, period)
	if p.failOn[period] {
		return fmt.Errorf("period %s failed", period)
	}
	return nil
}

func periodsOf(names ...string) []*PeriodInfo {
	out := make([]*PeriodInfo, 0, len(names))
	for _, name := range names {
		out = append(out, &PeriodInfo{Period: name, Granularity: "monthly"})
	}
	return out
}

// A manual pull takes minutes, so the caller needs to know where it is. The
// report goes out *before* each period starts — the interesting number is the
// one in flight, not the last one finished — and once more at the end so the
// bar reaches full.
func TestSyncPeriodsReportsProgress(t *testing.T) {
	executor := &BaseCloudSyncExecutor{provider: &stubProvider{}}
	var seen []SyncProgress
	config := &SyncConfig{Progress: func(p SyncProgress) { seen = append(seen, p) }}

	if _, err := executor.syncPeriods(context.Background(), periodsOf("2026-07", "2026-08", "2026-09"), config); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	want := []SyncProgress{
		{Period: "2026-07", Done: 0, Total: 3},
		{Period: "2026-08", Done: 1, Total: 3},
		{Period: "2026-09", Done: 2, Total: 3},
		{Period: "", Done: 3, Total: 3},
	}
	if len(seen) != len(want) {
		t.Fatalf("got %d reports, want %d: %+v", len(seen), len(want), seen)
	}
	for i, w := range want {
		if seen[i] != w {
			t.Errorf("report %d = %+v, want %+v", i, seen[i], w)
		}
	}
}

// A period that fails must not stall the bar: the sync moves on to the next
// one, and so does the progress.
func TestProgressContinuesPastAFailedPeriod(t *testing.T) {
	provider := &stubProvider{failOn: map[string]bool{"2026-08": true}}
	executor := &BaseCloudSyncExecutor{provider: provider}
	var last SyncProgress
	config := &SyncConfig{Progress: func(p SyncProgress) { last = p }}

	result, err := executor.syncPeriods(context.Background(), periodsOf("2026-08", "2026-09"), config)
	if err != nil {
		t.Fatalf("one bad period should not fail the run: %v", err)
	}
	if result.Success {
		t.Error("a failed period must leave Success false")
	}
	if last.Done != 2 || last.Total != 2 {
		t.Errorf("final progress = %+v, want 2/2", last)
	}
}

// Nobody watching is the normal case (the cron does not): the loop must not
// dereference a nil reporter.
func TestSyncPeriodsWithoutAReporter(t *testing.T) {
	executor := &BaseCloudSyncExecutor{provider: &stubProvider{}}
	if _, err := executor.syncPeriods(context.Background(), periodsOf("2026-09"), &SyncConfig{}); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
}
