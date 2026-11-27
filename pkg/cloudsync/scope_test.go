package cloudsync

import (
	"context"
	"testing"
)

// scopedProvider claims the VolcEngine bill table with a Scope, the way an
// account does when several accounts share the table.
type scopedProvider struct {
	stubProvider
	scope string
}

func (p *scopedProvider) GetProviderName() string { return "volcengine" }

func (p *scopedProvider) GetTableConfig(string) *TableConfig {
	return &TableConfig{TableName: "volcengine_bill_local", PeriodField: "BillPeriod", Scope: p.scope}
}

type recordingCleaner struct {
	periodCalls []string
	tableCalls  []string
}

func (c *recordingCleaner) CleanPeriodData(_ context.Context, table, period, _, _ string) error {
	c.periodCalls = append(c.periodCalls, table+" "+period)
	return nil
}

func (c *recordingCleaner) CleanTableData(_ context.Context, table, condition string) error {
	c.tableCalls = append(c.tableCalls, table+" WHERE "+condition)
	return nil
}

// Clearing a period before re-pulling it drops the whole month partition. In a
// table several accounts share that takes the other accounts' month with it,
// so a scoped provider has to delete its own rows only.
func TestCleanScopedPeriodDeletesOnlyThatAccount(t *testing.T) {
	cleaner := &recordingCleaner{}
	c := NewDefaultConsistencyChecker(&scopedProvider{scope: "PayerID = '2134017190'"}, nil, cleaner)

	err := c.CleanInconsistentData(context.Background(), []*PeriodInfo{{Period: "2026-11", Granularity: "monthly", NeedCleanup: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cleaner.periodCalls) != 0 {
		t.Errorf("scoped cleanup dropped whole periods: %v", cleaner.periodCalls)
	}
	want := "volcengine_bill_local WHERE BillPeriod = '2026-11' AND PayerID = '2134017190'"
	if len(cleaner.tableCalls) != 1 || cleaner.tableCalls[0] != want {
		t.Errorf("cleanup = %v, want [%s]", cleaner.tableCalls, want)
	}
}

// Without a scope nothing changes: the period is dropped as before.
func TestCleanUnscopedPeriodDropsThePeriod(t *testing.T) {
	cleaner := &recordingCleaner{}
	c := NewDefaultConsistencyChecker(&scopedProvider{}, nil, cleaner)

	if err := c.CleanInconsistentData(context.Background(), []*PeriodInfo{{Period: "2026-11", Granularity: "monthly", NeedCleanup: true}}); err != nil {
		t.Fatal(err)
	}
	if len(cleaner.tableCalls) != 0 || len(cleaner.periodCalls) != 1 {
		t.Errorf("period calls %v, table calls %v; want one period drop", cleaner.periodCalls, cleaner.tableCalls)
	}
}
