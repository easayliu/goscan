package alicloud

import (
	"strings"
	"testing"
	"time"

	"goscan/pkg/ddl"
)

func monthlyLine(instance, item string) *BillDetailForDB {
	return &BillDetailForDB{
		BillingCycle:     "2026-08",
		ProductCode:      "ecs",
		InstanceID:       instance,
		BillAccountID:    "1001",
		SubscriptionType: "Subscription",
		BillingType:      "Normal",
		Item:             item,
		Region:           "cn-hangzhou",
	}
}

func seqs(records []*BillDetailForDB) []uint32 {
	out := make([]uint32, len(records))
	for i, r := range records {
		out[i] = r.LineSeq
	}
	return out
}

func equalSeqs(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Two lines that agree on every key column must not share a key, or the merge
// keeps one of them. Lines that already differ keep line_seq 0, which is what
// lets a corrected line replace its old version.
func TestAssignLineSeqNumbersLinesWithinAGroup(t *testing.T) {
	p := &Processor{}
	records := []*BillDetailForDB{
		monthlyLine("i-1", "SubscriptionOrder"),
		monthlyLine("i-1", "Refund"),            // differs in item: its own group
		monthlyLine("i-1", "SubscriptionOrder"), // same as the first: seq 1
		monthlyLine("i-2", "SubscriptionOrder"),
	}
	p.assignLineSeq(records, false)

	if got, want := seqs(records), []uint32{0, 0, 1, 0}; !equalSeqs(got, want) {
		t.Fatalf("line_seq = %v, want %v", got, want)
	}
}

// A period arrives page by page; the numbering has to carry across pages or a
// group split over two pages would hand out 0 twice.
func TestAssignLineSeqCarriesAcrossPages(t *testing.T) {
	p := &Processor{}
	page1 := []*BillDetailForDB{monthlyLine("-", "PayAsYouGoBill")}
	page2 := []*BillDetailForDB{monthlyLine("-", "PayAsYouGoBill")}
	p.assignLineSeq(page1, false)
	p.assignLineSeq(page2, false)

	if page1[0].LineSeq != 0 || page2[0].LineSeq != 1 {
		t.Fatalf("line_seq across pages = %d, %d, want 0, 1", page1[0].LineSeq, page2[0].LineSeq)
	}
}

// A re-pull is a new Processor and must land on the same slots, so the
// ReplacingMergeTree replaces the old lines instead of adding to them.
func TestAssignLineSeqRestartsForEachPull(t *testing.T) {
	first := []*BillDetailForDB{monthlyLine("-", "PayAsYouGoBill"), monthlyLine("-", "PayAsYouGoBill")}
	again := []*BillDetailForDB{monthlyLine("-", "PayAsYouGoBill"), monthlyLine("-", "PayAsYouGoBill")}
	(&Processor{}).assignLineSeq(first, false)
	(&Processor{}).assignLineSeq(again, false)

	if !equalSeqs(seqs(first), seqs(again)) {
		t.Fatalf("re-pull line_seq %v differs from first pull %v", seqs(again), seqs(first))
	}
}

// The daily table keys on billing_date, so the same line on two days is two
// groups, each starting at 0.
func TestAssignLineSeqGroupsDailyLinesByDate(t *testing.T) {
	d1 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	d2 := d1.AddDate(0, 0, 1)
	a, b := monthlyLine("i-1", "PayAsYouGoBill"), monthlyLine("i-1", "PayAsYouGoBill")
	a.BillingDate, b.BillingDate = &d1, &d2
	records := []*BillDetailForDB{a, b}
	(&Processor{}).assignLineSeq(records, true)

	if got, want := seqs(records), []uint32{0, 0}; !equalSeqs(got, want) {
		t.Fatalf("line_seq = %v, want %v", got, want)
	}
}

// lineGroupKey has to be the sorting key minus line_seq. Finer, and two lines
// in one key could both get 0; coarser only wastes numbers. Equal is the goal.
func TestLineGroupKeyMatchesTheSortingKey(t *testing.T) {
	for _, tc := range []struct {
		table  ddl.Table
		period string
	}{
		{ddl.AliCloudMonthlyTable("m"), "billing_cycle"},
		{ddl.AliCloudDailyTable("d"), "billing_date"},
	} {
		want := "(" + strings.Join(append(append([]string{tc.period}, lineGroupColumns...), "line_seq"), ", ") + ")"
		if tc.table.OrderBy != want {
			t.Errorf("%s: ORDER BY %s, but line_seq is assigned over %s", tc.table.Name, tc.table.OrderBy, want)
		}
	}
}

// Amounts leave the SDK as float64; what goes into the Decimal column has to be
// the number the API printed, and sums over it must not drift.
func TestMoneyKeepsThePrintedAmount(t *testing.T) {
	if got := money(0.1).String(); got != "0.1" {
		t.Errorf("money(0.1) = %s, want 0.1", got)
	}
	if got := money(-12.34).String(); got != "-12.34" {
		t.Errorf("money(-12.34) = %s, want -12.34", got)
	}
	if got := money(0.1).Add(money(0.2)).String(); got != "0.3" {
		t.Errorf("0.1 + 0.2 = %s, want 0.3", got)
	}
}
