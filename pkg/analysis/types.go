package analysis

import (
	"time"
)

// CostAnalysisRequest represents a cost analysis request
type CostAnalysisRequest struct {
	Date           time.Time `json:"date"`            // analysis date (default to yesterday, compared with the day before)
	Providers      []string  `json:"providers"`       // list of cloud providers to analyze
	AlertThreshold float64   `json:"alert_threshold"` // anomaly alert threshold (percentage)
}

// CostAnalysisResult represents cost analysis results
type CostAnalysisResult struct {
	Date          time.Time             `json:"date"`           // analysis date
	YesterdayDate time.Time             `json:"yesterday_date"` // yesterday's date
	TotalCost     *CostMetric           `json:"total_cost"`     // total cost metrics
	Providers     []*ProviderCostMetric `json:"providers"`      // cost metrics for each provider
	Alerts        []string              `json:"alerts"`         // anomaly alert list
	GeneratedAt   time.Time             `json:"generated_at"`   // generation time
}

// ProviderCostMetric represents cost metrics for a single cloud provider
type ProviderCostMetric struct {
	Provider    string        `json:"provider"`     // provider identifier
	DisplayName string        `json:"display_name"` // display name
	TotalCost   *CostMetric   `json:"total_cost"`   // total cost for this provider
	Products    []*CostMetric `json:"products"`     // product cost list
}

// CostMetric represents cost metrics
type CostMetric struct {
	Name          string  `json:"name"`           // name (product name or total)
	YesterdayCost float64 `json:"yesterday_cost"` // yesterday's cost
	TodayCost     float64 `json:"today_cost"`     // today's cost
	ChangeAmount  float64 `json:"change_amount"`  // change amount
	ChangePercent float64 `json:"change_percent"` // change percentage
	Currency      string  `json:"currency"`       // currency unit
	IsSignificant bool    `json:"is_significant"` // whether it's a significant change
	RecordCount   uint64  `json:"record_count"`   // record count (for debugging)
}

// RawCostData represents raw cost data from ClickHouse queries
type RawCostData struct {
	Provider    string    `json:"provider"`     // cloud provider
	Product     string    `json:"product"`      // product name
	ExpenseDate time.Time `json:"expense_date"` // expense date
	TotalAmount float64   `json:"total_amount"` // total amount
	Currency    string    `json:"currency"`     // currency
	RecordCount uint64    `json:"record_count"` // record count
}

// DatabaseTableInfo represents database table information
type DatabaseTableInfo struct {
	Provider       string `json:"provider"`        // cloud provider
	TableName      string `json:"table_name"`      // table name
	DateColumn     string `json:"date_column"`     // date column name
	AmountColumn   string `json:"amount_column"`   // amount column name
	ProductColumn  string `json:"product_column"`  // product column name
	CurrencyColumn string `json:"currency_column"` // currency column name
}

// GetProviderTableInfo gets table information for various cloud providers
func GetProviderTableInfo() map[string]DatabaseTableInfo {
	return map[string]DatabaseTableInfo{
		"volcengine": {
			Provider:       "volcengine",
			TableName:      "volcengine_bill_details",
			DateColumn:     "ExpenseDate",
			AmountColumn:   "PayableAmount", // use payable amount
			ProductColumn:  "ProductZh",     // use Chinese product name
			CurrencyColumn: "Currency",
		},
		"alicloud_monthly": {
			Provider:       "alicloud",
			TableName:      "alicloud_bill_monthly",
			DateColumn:     "billing_date",
			AmountColumn:   "pretax_amount",
			ProductColumn:  "product_name",
			CurrencyColumn: "currency",
		},
		"alicloud_daily": {
			Provider:       "alicloud",
			TableName:      "alicloud_bill_daily",
			DateColumn:     "billing_date",
			AmountColumn:   "pretax_amount",
			ProductColumn:  "product_name",
			CurrencyColumn: "currency",
		},
	}
}

// GetProviderDisplayName gets cloud provider display name
func GetProviderDisplayName(provider string) string {
	names := map[string]string{
		"volcengine": "火山引擎",
		"alicloud":   "阿里云",
		"aws":        "亚马逊云",
		"azure":      "微软云",
		"gcp":        "谷歌云",
	}

	if displayName, exists := names[provider]; exists {
		return displayName
	}
	return provider
}

// IsIncrease determines if it's a cost increase
func (cm *CostMetric) IsIncrease() bool {
	return cm.ChangeAmount > 0
}

// IsDecrease determines if it's a cost decrease
func (cm *CostMetric) IsDecrease() bool {
	return cm.ChangeAmount < 0
}

// CalculateChange calculates cost changes
func (cm *CostMetric) CalculateChange() {
	cm.ChangeAmount = cm.TodayCost - cm.YesterdayCost

	if cm.YesterdayCost != 0 {
		cm.ChangePercent = (cm.ChangeAmount / cm.YesterdayCost) * 100
	} else if cm.TodayCost > 0 {
		cm.ChangePercent = 100 // growth from 0 is considered 100% growth
	} else {
		cm.ChangePercent = 0
	}
}

// SetSignificant sets whether it's a significant change
func (cm *CostMetric) SetSignificant(threshold float64) {
	cm.IsSignificant = cm.ChangePercent >= threshold || cm.ChangePercent <= -threshold
}
