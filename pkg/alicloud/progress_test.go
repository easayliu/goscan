package alicloud

import (
	"context"
	"testing"
)

// The API's TotalCount includes lines that are never written (filtered out,
// failing validation). Left in the denominator they keep the bar short of
// 100% for good; they are taken out as they are dropped.
func TestDroppedLinesComeOffTheTotal(t *testing.T) {
	p := NewProcessor(nil, &SyncOptions{})
	p.options.RecordFilter = func(bill *BillDetail) bool { return bill.InstanceID != "drop" }
	p.SetTotalRecords(10)

	bills := []BillDetail{{InstanceID: "drop"}, {InstanceID: "drop"}, {InstanceID: "drop"}}
	if err := p.ProcessBatchWithBillingCycle(context.Background(), "alicloud_bill_monthly", bills, "2026-08"); err != nil {
		t.Fatalf("ProcessBatchWithBillingCycle: %v", err)
	}

	if got := p.GetTotalRecords(); got != 7 {
		t.Errorf("total = %d, want 7 (10 reported, 3 dropped)", got)
	}
}

// A total that was never known stays unknown rather than going negative.
func TestDiscountLeavesAnUnknownTotalAlone(t *testing.T) {
	p := NewProcessor(nil, &SyncOptions{})
	p.discountTotal(5)
	if got := p.GetTotalRecords(); got != 0 {
		t.Errorf("total = %d, want 0", got)
	}

	p.SetTotalRecords(3)
	p.discountTotal(5)
	if got := p.GetTotalRecords(); got != 0 {
		t.Errorf("total = %d, want 0, not negative", got)
	}
}
