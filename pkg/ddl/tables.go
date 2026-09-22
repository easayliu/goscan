package ddl

import "goscan/pkg/config"

// Default table names. They are also the defaults of the matching config
// fields; a deployment that renames a table in its config gets the renamed
// table out of `--ddl` as well.
const (
	DefaultVolcEngineBillTable  = "volcengine_bill_details"
	DefaultAliCloudMonthlyTable = "alicloud_bill_monthly"
	DefaultAliCloudDailyTable   = "alicloud_bill_daily"
)

// Tables returns every table goscan writes, named after the given config.
func Tables(cfg *config.Config) []Table {
	volc := cfg.GetVolcEngineConfig()
	ali := cfg.GetAliCloudConfig()

	return []Table{
		VolcEngineBillTable(orDefault(volc.BillTable, DefaultVolcEngineBillTable)),
		AliCloudMonthlyTable(orDefault(ali.MonthlyTable, DefaultAliCloudMonthlyTable)),
		AliCloudDailyTable(orDefault(ali.DailyTable, DefaultAliCloudDailyTable)),
	}
}

// OptionsFrom reads the ClickHouse side of the config: which database to write
// into, and whether this is a cluster.
func OptionsFrom(cfg *config.Config) Options {
	ch := cfg.ClickHouse
	if ch == nil {
		ch = config.NewClickHouseConfig()
	}
	return Options{
		Database:   orDefault(ch.Database, "default"),
		Cluster:    ch.Cluster,
		Replicated: ch.Replicated,
	}
}

// VolcEngineBillTable is the VolcEngine bill detail table. Its columns keep the
// API's own PascalCase names so a row can be matched against the raw response.
func VolcEngineBillTable(name string) Table {
	return Table{
		Name:        name,
		Comment:     "VolcEngine bill details, one row per billing item",
		Columns:     volcEngineBillColumns,
		Engine:      "ReplacingMergeTree",
		PartitionBy: "toYYYYMM(toDate(ExpenseDate))",
		OrderBy:     "(BillPeriod, ExpenseDate, InstanceNo, ExpenseBeginTime, Product, ElementCode, PayableAmount)",
		Settings:    "index_granularity = 8192",
	}
}

// AliCloudMonthlyTable is the Alibaba Cloud monthly bill table.
func AliCloudMonthlyTable(name string) Table {
	return Table{
		Name:        name,
		Comment:     "Alibaba Cloud bills at monthly granularity",
		Columns:     aliCloudMonthlyColumns,
		Engine:      "ReplacingMergeTree()",
		PartitionBy: "toYYYYMM(parseDateTimeBestEffort(billing_cycle || '-01'))",
		OrderBy:     "(billing_cycle, product_code, instance_id, bill_account_id, subscription_type, payment_amount)",
	}
}

// AliCloudDailyTable is the Alibaba Cloud daily bill table.
func AliCloudDailyTable(name string) Table {
	return Table{
		Name:        name,
		Comment:     "Alibaba Cloud bills at daily granularity",
		Columns:     aliCloudDailyColumns,
		Engine:      "ReplacingMergeTree()",
		PartitionBy: "toYYYYMMDD(billing_date)",
		OrderBy:     "(billing_date, product_code, instance_id, bill_account_id, subscription_type, payment_amount)",
	}
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

var volcEngineBillColumns = []Column{
	{Name: "BillDetailId", Type: "String", Section: "核心标识字段"},
	{Name: "BillID", Type: "String"},
	{Name: "InstanceNo", Type: "String"},
	{Name: "BillPeriod", Type: "String", Section: "时间字段"},
	{Name: "BusiPeriod", Type: "String"},
	{Name: "ExpenseDate", Type: "String"},
	{Name: "ExpenseBeginTime", Type: "String"},
	{Name: "ExpenseEndTime", Type: "String"},
	{Name: "TradeTime", Type: "String"},
	{Name: "PayerID", Type: "String", Section: "用户信息字段"},
	{Name: "PayerUserName", Type: "String"},
	{Name: "PayerCustomerName", Type: "String"},
	{Name: "SellerID", Type: "String"},
	{Name: "SellerUserName", Type: "String"},
	{Name: "SellerCustomerName", Type: "String"},
	{Name: "OwnerID", Type: "String"},
	{Name: "OwnerUserName", Type: "String"},
	{Name: "OwnerCustomerName", Type: "String"},
	{Name: "Product", Type: "String", Section: "产品信息字段"},
	{Name: "ProductZh", Type: "String"},
	{Name: "SolutionZh", Type: "String"},
	{Name: "Element", Type: "String"},
	{Name: "ElementCode", Type: "String"},
	{Name: "Factor", Type: "String"},
	{Name: "FactorCode", Type: "String"},
	{Name: "ConfigName", Type: "String", Section: "配置信息字段"},
	{Name: "ConfigurationCode", Type: "String"},
	{Name: "InstanceName", Type: "String"},
	{Name: "Region", Type: "String", Section: "地域信息字段"},
	{Name: "RegionCode", Type: "String"},
	{Name: "Zone", Type: "String"},
	{Name: "ZoneCode", Type: "String"},
	{Name: "CountryRegion", Type: "String"},
	{Name: "BillingMode", Type: "String", Section: "计费模式信息"},
	{Name: "BusinessMode", Type: "String"},
	{Name: "BillingFunction", Type: "String"},
	{Name: "BillingMethodCode", Type: "String"},
	{Name: "SellingMode", Type: "String"},
	{Name: "SettlementType", Type: "String"},
	{Name: "Count", Type: "String", Section: "用量信息字段"},
	{Name: "Unit", Type: "String"},
	{Name: "UseDuration", Type: "String"},
	{Name: "UseDurationUnit", Type: "String"},
	{Name: "DeductionCount", Type: "String"},
	{Name: "DeductionUseDuration", Type: "String"},
	{Name: "Price", Type: "String", Section: "价格信息字段"},
	{Name: "PriceUnit", Type: "String"},
	{Name: "PriceInterval", Type: "String"},
	{Name: "MarketPrice", Type: "String"},
	{Name: "MeasureInterval", Type: "String"},
	{Name: "Formula", Type: "String"},
	{Name: "OriginalBillAmount", Type: "String", Section: "金额信息字段"},
	{Name: "PreferentialBillAmount", Type: "String"},
	{Name: "DiscountBillAmount", Type: "String"},
	{Name: "RoundAmount", Type: "Float64"},
	{Name: "PayableAmount", Type: "String"},
	{Name: "PreTaxPayableAmount", Type: "String"},
	{Name: "SettlePayableAmount", Type: "String"},
	{Name: "SettlePreTaxPayableAmount", Type: "String"},
	{Name: "PretaxAmount", Type: "String"},
	{Name: "PosttaxAmount", Type: "String"},
	{Name: "SettlePretaxAmount", Type: "String"},
	{Name: "SettlePosttaxAmount", Type: "String"},
	{Name: "Tax", Type: "String"},
	{Name: "SettleTax", Type: "String"},
	{Name: "TaxRate", Type: "String"},
	{Name: "PaidAmount", Type: "String"},
	{Name: "UnpaidAmount", Type: "String"},
	{Name: "CreditCarriedAmount", Type: "String"},
	{Name: "RealValue", Type: "String", Section: "实际价值和结算信息"},
	{Name: "PretaxRealValue", Type: "String"},
	{Name: "SettleRealValue", Type: "String"},
	{Name: "SettlePretaxRealValue", Type: "String"},
	{Name: "CouponAmount", Type: "String", Section: "优惠和抵扣信息"},
	{Name: "DiscountInfo", Type: "String"},
	{Name: "SavingPlanDeductionDiscountAmount", Type: "String"},
	{Name: "SavingPlanDeductionSpID", Type: "String"},
	{Name: "SavingPlanOriginalAmount", Type: "String"},
	{Name: "ReservationInstance", Type: "String"},
	{Name: "Currency", Type: "String", Section: "货币信息"},
	{Name: "CurrencySettlement", Type: "String"},
	{Name: "ExchangeRate", Type: "String"},
	{Name: "Project", Type: "String", Section: "项目和分类信息"},
	{Name: "ProjectDisplayName", Type: "String"},
	{Name: "BillCategory", Type: "String"},
	{Name: "SubjectName", Type: "String"},
	{Name: "Tag", Type: "String"},
	{Name: "DiscountBizBillingFunction", Type: "String", Section: "折扣相关业务信息"},
	{Name: "DiscountBizMeasureInterval", Type: "String"},
	{Name: "DiscountBizUnitPrice", Type: "String"},
	{Name: "DiscountBizUnitPriceInterval", Type: "String"},
	{Name: "MainContractNumber", Type: "String", Section: "其他业务信息"},
	{Name: "OriginalOrderNo", Type: "String"},
	{Name: "EffectiveFactor", Type: "String"},
	{Name: "ExpandField", Type: "String"},
	{Name: "created_at", Type: "DateTime64(3)", Default: "now()", Section: "系统字段"},
	{Name: "updated_at", Type: "DateTime64(3)", Default: "now()"},
}

var aliCloudMonthlyColumns = []Column{
	{Name: "instance_id", Type: "String"},
	{Name: "instance_name", Type: "String"},
	{Name: "bill_account_id", Type: "String"},
	{Name: "bill_account_name", Type: "String"},
	{Name: "billing_date", Type: "Nullable(Date)", Comment: "NULL for monthly table"},
	{Name: "billing_cycle", Type: "String"},
	{Name: "product_code", Type: "String"},
	{Name: "product_name", Type: "String"},
	{Name: "product_type", Type: "String"},
	{Name: "product_detail", Type: "String"},
	{Name: "subscription_type", Type: "String"},
	{Name: "pricing_unit", Type: "String"},
	{Name: "currency", Type: "String"},
	{Name: "billing_type", Type: "String"},
	{Name: "usage", Type: "String"},
	{Name: "usage_unit", Type: "String"},
	{Name: "pretax_gross_amount", Type: "Float64"},
	{Name: "invoice_discount", Type: "Float64"},
	{Name: "deducted_by_coupons", Type: "Float64"},
	{Name: "pretax_amount", Type: "Float64"},
	{Name: "currency_amount", Type: "Float64"},
	{Name: "payment_amount", Type: "Float64"},
	{Name: "outstanding_amount", Type: "Float64"},
	{Name: "region", Type: "String"},
	{Name: "zone", Type: "String"},
	{Name: "instance_spec", Type: "String"},
	{Name: "internet_ip", Type: "String"},
	{Name: "intranet_ip", Type: "String"},
	{Name: "resource_group", Type: "String"},
	{Name: "tags", Type: "Map(String, String)"},
	{Name: "cost_unit", Type: "String"},
	{Name: "service_period", Type: "String"},
	{Name: "service_period_unit", Type: "String"},
	{Name: "list_price", Type: "String"},
	{Name: "list_price_unit", Type: "String"},
	{Name: "owner_id", Type: "String"},
	{Name: "split_item_id", Type: "String"},
	{Name: "split_item_name", Type: "String"},
	{Name: "split_account_id", Type: "String"},
	{Name: "split_account_name", Type: "String"},
	{Name: "nick_name", Type: "String"},
	{Name: "product_detail_code", Type: "String"},
	{Name: "biz_type", Type: "String"},
	{Name: "adjust_type", Type: "String"},
	{Name: "adjust_amount", Type: "Float64"},
	{Name: "granularity", Type: "String", Default: "'MONTHLY'"},
	{Name: "created_at", Type: "DateTime64(3)", Default: "now()"},
	{Name: "updated_at", Type: "DateTime64(3)", Default: "now()"},
}

var aliCloudDailyColumns = []Column{
	{Name: "instance_id", Type: "String"},
	{Name: "instance_name", Type: "String"},
	{Name: "bill_account_id", Type: "String"},
	{Name: "bill_account_name", Type: "String"},
	{Name: "billing_date", Type: "Date", Comment: "Daily table must have value"},
	{Name: "billing_cycle", Type: "String"},
	{Name: "product_code", Type: "String"},
	{Name: "product_name", Type: "String"},
	{Name: "product_type", Type: "String"},
	{Name: "product_detail", Type: "String"},
	{Name: "subscription_type", Type: "String"},
	{Name: "pricing_unit", Type: "String"},
	{Name: "currency", Type: "String"},
	{Name: "billing_type", Type: "String"},
	{Name: "usage", Type: "String"},
	{Name: "usage_unit", Type: "String"},
	{Name: "pretax_gross_amount", Type: "Float64"},
	{Name: "invoice_discount", Type: "Float64"},
	{Name: "deducted_by_coupons", Type: "Float64"},
	{Name: "pretax_amount", Type: "Float64"},
	{Name: "currency_amount", Type: "Float64"},
	{Name: "payment_amount", Type: "Float64"},
	{Name: "outstanding_amount", Type: "Float64"},
	{Name: "region", Type: "String"},
	{Name: "zone", Type: "String"},
	{Name: "instance_spec", Type: "String"},
	{Name: "internet_ip", Type: "String"},
	{Name: "intranet_ip", Type: "String"},
	{Name: "resource_group", Type: "String"},
	{Name: "tags", Type: "Map(String, String)"},
	{Name: "cost_unit", Type: "String"},
	{Name: "service_period", Type: "String"},
	{Name: "service_period_unit", Type: "String"},
	{Name: "list_price", Type: "String"},
	{Name: "list_price_unit", Type: "String"},
	{Name: "owner_id", Type: "String"},
	{Name: "split_item_id", Type: "String"},
	{Name: "split_item_name", Type: "String"},
	{Name: "split_account_id", Type: "String"},
	{Name: "split_account_name", Type: "String"},
	{Name: "nick_name", Type: "String"},
	{Name: "product_detail_code", Type: "String"},
	{Name: "biz_type", Type: "String"},
	{Name: "adjust_type", Type: "String"},
	{Name: "adjust_amount", Type: "Float64"},
	{Name: "granularity", Type: "String", Default: "'DAILY'"},
	{Name: "created_at", Type: "DateTime64(3)", Default: "now()"},
	{Name: "updated_at", Type: "DateTime64(3)", Default: "now()"},
}
