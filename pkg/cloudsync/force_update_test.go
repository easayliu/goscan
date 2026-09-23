package cloudsync

import (
	"context"
	"testing"
)

// stubChecker answers every consistency question the same way and remembers
// what it was asked.
type stubChecker struct {
	consistent bool
	checked    []string
	cleaned    []string
}

func (c *stubChecker) CheckPeriodConsistency(_ context.Context, period *PeriodInfo) (bool, error) {
	c.checked = append(c.checked, period.Period+" "+period.Granularity)
	period.DBCount = 1
	period.NeedCleanup = !c.consistent
	return c.consistent, nil
}

func (c *stubChecker) FindInconsistentPeriods(context.Context, *SyncConfig) ([]*PeriodInfo, error) {
	return nil, nil
}

func (c *stubChecker) CleanInconsistentData(_ context.Context, periods []*PeriodInfo) error {
	for _, period := range periods {
		c.cleaned = append(c.cleaned, period.Period+" "+period.Granularity)
	}
	return nil
}

// force_update says "已有数据也重新拉取". It used to do the opposite: the flag
// gated the consistency check, so ticking it skipped every period that already
// had data and leaving it off pulled everything.
func TestForceUpdateDecidesWhetherAPeriodMayBeSkipped(t *testing.T) {
	cases := []struct {
		name       string
		force      bool
		consistent bool
		wantSynced []string
		wantCheck  bool
	}{
		{
			name:       "a period that already matches is skipped",
			consistent: true,
			wantSynced: nil,
			wantCheck:  true,
		},
		{
			name:       "a period that does not match is pulled",
			consistent: false,
			wantSynced: []string{"2026-08 monthly"},
			wantCheck:  true,
		},
		{
			name:       "force pulls it however complete it looks",
			force:      true,
			consistent: true,
			wantSynced: []string{"2026-08 monthly"},
			// Nothing to decide, so nothing to count — a forced pull must not
			// spend API quota counting rows it is going to replace anyway.
			wantCheck: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			provider := &stubProvider{}
			checker := &stubChecker{consistent: tc.consistent}
			executor := &BaseCloudSyncExecutor{provider: provider, consistencyChecker: checker}

			result, err := executor.executeStandardSync(context.Background(), &SyncConfig{
				BillPeriod:  "2026-08",
				Granularity: "monthly",
				ForceUpdate: tc.force,
				AutoClean:   true,
			})
			if err != nil {
				t.Fatalf("executeStandardSync: %v", err)
			}
			if !result.Success {
				t.Errorf("result = %+v, want a successful run", result)
			}
			if !equal(provider.syncedPairs, tc.wantSynced) {
				t.Errorf("provider pulled %v, want %v", provider.syncedPairs, tc.wantSynced)
			}
			if checked := len(checker.checked) > 0; checked != tc.wantCheck {
				t.Errorf("consistency checked = %v, want %v", checked, tc.wantCheck)
			}
		})
	}
}

// A period whose rows are stale is cleaned before it is pulled again, so the
// new rows replace the old ones rather than joining them.
func TestAnInconsistentPeriodIsCleanedFirst(t *testing.T) {
	provider := &stubProvider{}
	checker := &stubChecker{consistent: false}
	executor := &BaseCloudSyncExecutor{provider: provider, consistencyChecker: checker}

	if _, err := executor.executeStandardSync(context.Background(), &SyncConfig{
		BillPeriod:  "2026-08",
		Granularity: "monthly",
		AutoClean:   true,
	}); err != nil {
		t.Fatalf("executeStandardSync: %v", err)
	}

	if !equal(checker.cleaned, []string{"2026-08 monthly"}) {
		t.Errorf("cleaned %v, want the stale period", checker.cleaned)
	}
}

// sync-optimal skips what is already complete; force says to pull it anyway.
// The two cannot both hold, and the explicit flag is the one the caller just
// ticked, so it wins rather than being quietly ignored.
func TestForceUpdateOverridesSyncOptimal(t *testing.T) {
	provider := &stubProvider{}
	checker := &stubChecker{consistent: true}
	executor := &BaseCloudSyncExecutor{provider: provider, consistencyChecker: checker}

	if _, err := executor.ExecuteSync(context.Background(), &SyncConfig{
		SyncMode:    "sync-optimal",
		BillPeriod:  "2026-08",
		Granularity: "monthly",
		ForceUpdate: true,
		AutoClean:   true,
	}); err != nil {
		t.Fatalf("ExecuteSync: %v", err)
	}

	if !equal(provider.syncedPairs, []string{"2026-08 monthly"}) {
		t.Errorf("provider pulled %v, want the period it was forced to pull", provider.syncedPairs)
	}
}

// A forced re-pull empties each period first. Replacing in place only reaches
// lines whose key comes back; a line the provider has since dropped would
// otherwise stay behind and be counted on top of the fresh pull.
func TestForceUpdateClearsEachPeriodBeforePulling(t *testing.T) {
	provider := &stubProvider{}
	checker := &stubChecker{consistent: true}
	executor := &BaseCloudSyncExecutor{provider: provider, consistencyChecker: checker}

	if _, err := executor.executeStandardSync(context.Background(), &SyncConfig{
		StartPeriod: "2026-07",
		EndPeriod:   "2026-08",
		Granularity: "monthly",
		ForceUpdate: true,
		AutoClean:   true,
	}); err != nil {
		t.Fatalf("executeStandardSync: %v", err)
	}

	want := []string{"2026-07 monthly", "2026-08 monthly"}
	if !equal(checker.cleaned, want) {
		t.Errorf("cleaned %v, want %v", checker.cleaned, want)
	}
	if !equal(provider.syncedPairs, want) {
		t.Errorf("pulled %v, want %v", provider.syncedPairs, want)
	}
}
