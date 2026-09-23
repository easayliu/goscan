package cloudsync

import (
	"context"
	"testing"
)

// eventLog records clears and pulls in the one order they happened.
type eventLog struct{ events []string }

// orderedProvider logs each pull, reports rows as it goes, and can run a hook
// mid-pass — which is where a cancel from the UI lands.
type orderedProvider struct {
	stubProvider
	log    *eventLog
	during func(period string)
}

func (p *orderedProvider) SyncPeriodData(ctx context.Context, period, granularity string, opts *SyncOptions) error {
	if p.during != nil {
		p.during(period)
	}
	if opts.ProgressCallback != nil {
		opts.ProgressCallback(100, 100, "")
	}
	p.log.events = append(p.log.events, "pull "+period)
	return p.stubProvider.SyncPeriodData(ctx, period, granularity, opts)
}

// orderedChecker says every period is stale (so each needs clearing) and logs
// each clear.
type orderedChecker struct {
	stubChecker
	log *eventLog
}

func (c *orderedChecker) CheckPeriodConsistency(ctx context.Context, period *PeriodInfo) (bool, error) {
	c.checked = append(c.checked, period.Period)
	period.DBCount = 5
	return false, nil
}

func (c *orderedChecker) CleanInconsistentData(_ context.Context, periods []*PeriodInfo) error {
	for _, period := range periods {
		c.log.events = append(c.log.events, "clear "+period.Period)
	}
	return nil
}

func rangeConfig(force bool, stop <-chan struct{}) *SyncConfig {
	return &SyncConfig{
		StartPeriod: "2026-05",
		EndPeriod:   "2026-08",
		Granularity: "monthly",
		ForceUpdate: force,
		AutoClean:   true,
		Stop:        stop,
	}
}

// Each period is cleared right before it is pulled, never all of them up
// front: a run that stops early has to leave the periods it did not reach as
// they were, not emptied.
func TestPeriodsAreClearedOneAtATimeRightBeforeTheirPull(t *testing.T) {
	for _, force := range []bool{true, false} {
		log := &eventLog{}
		executor := &BaseCloudSyncExecutor{
			provider:           &orderedProvider{log: log},
			consistencyChecker: &orderedChecker{log: log},
		}
		if _, err := executor.executeStandardSync(context.Background(), rangeConfig(force, nil)); err != nil {
			t.Fatal(err)
		}

		want := []string{
			"clear 2026-05", "pull 2026-05",
			"clear 2026-06", "pull 2026-06",
			"clear 2026-07", "pull 2026-07",
			"clear 2026-08", "pull 2026-08",
		}
		if !equal(log.events, want) {
			t.Errorf("force=%v: %v, want %v", force, log.events, want)
		}
	}
}

// A cancel that arrives mid-pass lets that pass finish and starts no other.
// The passes never started are reported, and were neither cleared nor pulled.
func TestStopFinishesThePassInFlightAndStartsNoOther(t *testing.T) {
	log := &eventLog{}
	stop := make(chan struct{})
	provider := &orderedProvider{log: log, during: func(period string) {
		if period == "2026-06" {
			close(stop) // the user clicks while 2026-06 is being pulled
		}
	}}
	var seen []SyncProgress
	config := rangeConfig(true, stop)
	config.Progress = func(p SyncProgress) { seen = append(seen, p) }

	executor := &BaseCloudSyncExecutor{provider: provider, consistencyChecker: &orderedChecker{log: log}}
	result, err := executor.executeStandardSync(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}

	wantEvents := []string{"clear 2026-05", "pull 2026-05", "clear 2026-06", "pull 2026-06"}
	if !equal(log.events, wantEvents) {
		t.Errorf("events %v, want %v — 2026-06 finished, nothing after it touched", log.events, wantEvents)
	}
	if !result.Cancelled {
		t.Error("result not marked cancelled")
	}
	if want := []string{"2026-07 monthly", "2026-08 monthly"}; !equal(result.NotRun, want) {
		t.Errorf("not run = %v, want %v", result.NotRun, want)
	}
	if !result.Success {
		t.Error("a stop is not a failure; both passes that ran succeeded")
	}
	if result.RecordsProcessed != 200 {
		t.Errorf("records processed = %d, want the 200 rows the two passes wrote", result.RecordsProcessed)
	}
	if last := seen[len(seen)-1]; last.Done != 2 || last.Total != 4 {
		t.Errorf("final progress = %+v, want 2 of 4 passes done", last)
	}
}

// Stopped before anything was written — while periods were still being
// counted — the run touches nothing and reports every pass as not run.
func TestStopDuringTheChecksWritesNothing(t *testing.T) {
	log := &eventLog{}
	stop := make(chan struct{})
	close(stop)
	checker := &orderedChecker{log: log}
	executor := &BaseCloudSyncExecutor{provider: &orderedProvider{log: log}, consistencyChecker: checker}

	result, err := executor.executeStandardSync(context.Background(), rangeConfig(false, stop))
	if err != nil {
		t.Fatal(err)
	}
	if len(log.events) != 0 || len(checker.checked) != 0 {
		t.Errorf("stopped run still acted: events %v, checked %v", log.events, checker.checked)
	}
	if !result.Cancelled || len(result.NotRun) != 4 {
		t.Errorf("result = %+v, want cancelled with all 4 passes not run", result)
	}
}

// records_processed used to be a counter nothing ever added to, so every
// result said 0 rows. It now adds up what each pass wrote.
func TestRecordsProcessedAddsUpThePasses(t *testing.T) {
	log := &eventLog{}
	executor := &BaseCloudSyncExecutor{provider: &orderedProvider{log: log}, consistencyChecker: &orderedChecker{log: log}}

	result, err := executor.executeStandardSync(context.Background(), rangeConfig(true, nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.RecordsProcessed != 400 || result.Cancelled {
		t.Errorf("result = %+v, want 400 rows over 4 passes and not cancelled", result)
	}
}
