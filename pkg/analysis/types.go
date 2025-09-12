package analysis

import (
	"time"
)

// CostAnalysisRequest 费用分析请求
type CostAnalysisRequest struct {
	Date           time.Time `json:"date"`            // 分析日期（默认为昨天，与前天对比）
	Providers      []string  `json:"providers"`       // 要分析的云服务商列表
	AlertThreshold float64   `json:"alert_threshold"` // 异常告警阈值（百分比）
}

// CostAnalysisResult 费用分析结果
type CostAnalysisResult struct {
	Date          time.Time             `json:"date"`           // 分析日期
	YesterdayDate time.Time             `json:"yesterday_date"` // 昨日日期
	TotalCost     *CostMetric           `json:"total_cost"`     // 总费用指标
	Providers     []*ProviderCostMetric `json:"providers"`      // 各服务商费用指标
	Alerts        []string              `json:"alerts"`         // 异常告警列表
	GeneratedAt   time.Time             `json:"generated_at"`   // 生成时间
}

// ProviderCostMetric 单个云服务商费用指标
type ProviderCostMetric struct {
	Provider    string        `json:"provider"`     // 服务商标识
	DisplayName string        `json:"display_name"` // 显示名称
	TotalCost   *CostMetric   `json:"total_cost"`   // 该服务商总费用
	Products    []*CostMetric `json:"products"`     // 产品费用列表
}

// CostMetric 费用指标
type CostMetric struct {
	Name          string  `json:"name"`           // 名称（产品名或总计）
	YesterdayCost float64 `json:"yesterday_cost"` // 昨日费用
	TodayCost     float64 `json:"today_cost"`     // 今日费用
	ChangeAmount  float64 `json:"change_amount"`  // 变化金额
	ChangePercent float64 `json:"change_percent"` // 变化百分比
	Currency      string  `json:"currency"`       // 货币单位
	IsSignificant bool    `json:"is_significant"` // 是否为显著变化
	RecordCount   uint64  `json:"record_count"`   // 记录数（用于调试）
}

// RawCostData ClickHouse查询的原始费用数据
type RawCostData struct {
	Provider    string    `json:"provider"`     // 云服务商
	Product     string    `json:"product"`      // 产品名称
	ExpenseDate time.Time `json:"expense_date"` // 费用日期
	TotalAmount float64   `json:"total_amount"` // 总金额
	Currency    string    `json:"currency"`     // 货币
	RecordCount uint64    `json:"record_count"` // 记录数
}

// DatabaseTableInfo 数据库表信息
type DatabaseTableInfo struct {
	Provider       string `json:"provider"`        // 云服务商
	TableName      string `json:"table_name"`      // 表名
	DateColumn     string `json:"date_column"`     // 日期字段名
	AmountColumn   string `json:"amount_column"`   // 金额字段名
	ProductColumn  string `json:"product_column"`  // 产品字段名
	CurrencyColumn string `json:"currency_column"` // 货币字段名
}

// GetProviderTableInfo 获取各云服务商的表信息
func GetProviderTableInfo() map[string]DatabaseTableInfo {
	return map[string]DatabaseTableInfo{
		"volcengine": {
			Provider:       "volcengine",
			TableName:      "volcengine_bill_details",
			DateColumn:     "expense_date",
			AmountColumn:   "round_amount",
			ProductColumn:  "product",
			CurrencyColumn: "currency",
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

// GetProviderDisplayName 获取云服务商显示名称
func GetProviderDisplayName(provider string) string {
	names := map[string]string{
		"volcengine": "火山云",
		"alicloud":   "阿里云",
		"aws":        "亚马逊云科技",
		"azure":      "微软云",
		"gcp":        "谷歌云",
	}

	if displayName, exists := names[provider]; exists {
		return displayName
	}
	return provider
}

// IsIncrease 判断是否为费用增长
func (cm *CostMetric) IsIncrease() bool {
	return cm.ChangeAmount > 0
}

// IsDecrease 判断是否为费用降低
func (cm *CostMetric) IsDecrease() bool {
	return cm.ChangeAmount < 0
}

// CalculateChange 计算费用变化
func (cm *CostMetric) CalculateChange() {
	cm.ChangeAmount = cm.TodayCost - cm.YesterdayCost

	if cm.YesterdayCost != 0 {
		cm.ChangePercent = (cm.ChangeAmount / cm.YesterdayCost) * 100
	} else if cm.TodayCost > 0 {
		cm.ChangePercent = 100 // 从0增长视为100%增长
	} else {
		cm.ChangePercent = 0
	}
}

// SetSignificant 设置是否为显著变化
func (cm *CostMetric) SetSignificant(threshold float64) {
	cm.IsSignificant = cm.ChangePercent >= threshold || cm.ChangePercent <= -threshold
}
