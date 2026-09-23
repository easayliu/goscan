package tasks

import "testing"

// Clearing a whole cycle from the daily table has to name every day's
// partition. The single value '202608' is no day at all: dropping it matches
// nothing and succeeds, leaving the month in place.
func TestPartitionValuesForAWholeCycleOfTheDailyTable(t *testing.T) {
	c := &CommonDataCleaner{}

	values, ok := c.calculatePartitionValues("2026-02", "billing_date", "alicloud")
	if !ok {
		t.Fatal("no partitions for a whole cycle")
	}
	if len(values) != 28 || values[0] != "20260201" || values[27] != "20260228" {
		t.Errorf("partitions = %v, want 20260201..20260228", values)
	}

	values, ok = c.calculatePartitionValues("2026-02-14", "billing_date", "alicloud")
	if !ok || len(values) != 1 || values[0] != "20260214" {
		t.Errorf("one day: %v %v, want [20260214]", values, ok)
	}

	values, ok = c.calculatePartitionValues("2026-02", "billing_cycle", "alicloud")
	if !ok || len(values) != 1 || values[0] != "202602" {
		t.Errorf("monthly table: %v %v, want [202602]", values, ok)
	}
}
