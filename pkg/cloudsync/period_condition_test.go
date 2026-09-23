package cloudsync

import "testing"

func TestPeriodConditionSelectsOnePeriod(t *testing.T) {
	aliMonthly := &TableConfig{TableName: "m", PeriodField: "billing_cycle", DateField: "billing_date"}
	aliDaily := &TableConfig{TableName: "d", PeriodField: "billing_date", DateField: "billing_date", CycleField: "billing_cycle"}
	volc := &TableConfig{TableName: "v", PeriodField: "BillPeriod"}

	cases := []struct {
		name        string
		tc          *TableConfig
		period      string
		granularity string
		want        string
	}{
		{"alicloud monthly", aliMonthly, "2026-08", "monthly", "billing_cycle = ? AND billing_date IS NULL"},
		{"volcengine monthly", volc, "2026-08", "monthly", "BillPeriod = ?"},
		{"alicloud one day", aliDaily, "2026-08-15", "daily", "billing_date = ?"},
		// Comparing the Date column to '2026-08' fails instead of selecting the
		// month, which is how a daily backfill over months used to go uncounted.
		{"alicloud whole cycle at daily granularity", aliDaily, "2026-08", "daily", "billing_cycle = ?"},
	}
	for _, c := range cases {
		got, err := periodCondition(c.tc, c.period, c.granularity)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}

	if _, err := periodCondition(&TableConfig{TableName: "x", PeriodField: "d", DateField: "d"}, "2026-08", "daily"); err == nil {
		t.Error("a daily table with no cycle column accepted a whole-cycle period")
	}
}
