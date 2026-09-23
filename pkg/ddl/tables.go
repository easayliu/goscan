package ddl

import "goscan/pkg/config"

// MoneyType is what every amount column is stored as.
//
// Not String: the API sends decimal text and keeping it verbatim meant every
// sum had to go through toFloat64OrZero, no index or ORDER BY could use the
// column, and it compressed badly. Not Float64 either — money summed as binary
// floating point drifts, and a monthly total that is off by a cent is a bill
// nobody can reconcile. Decimal(20, 8) holds twelve integer digits, which is
// more headroom than any invoice we will see, and eight decimals, enough for
// the per-unit prices billed at 0.00000001.
const MoneyType = "Decimal(20, 8)"

// Default table names. They are also the defaults of the matching config
// fields; a deployment that renames a table in its config gets the renamed
// table out of `--ddl` as well.
const (
	DefaultVolcEngineBillTable  = "volcengine_bill"
	DefaultAliCloudMonthlyTable = "alicloud_bill_monthly"
	DefaultAliCloudDailyTable   = "alicloud_bill_daily"
)

// Tables returns every table goscan writes, named after the given config.
func Tables(cfg *config.Config) []Table {
	return []Table{
		VolcEngineBillTable(VolcEngineBillTableName(cfg.GetVolcEngineConfig())),
		AliCloudMonthlyTable(AliCloudMonthlyTableName(cfg.GetAliCloudConfig())),
		AliCloudDailyTable(AliCloudDailyTableName(cfg.GetAliCloudConfig())),
	}
}

// The three functions below are the only place a bill table's name is decided.
// Everything that touches those tables — the DDL, the sync, the consistency
// check, the cost report — has to ask here: a name resolved anywhere else means
// a renamed table gets created in one place and read from another.

// VolcEngineBillTableName is the table VolcEngine bills live in.
func VolcEngineBillTableName(cfg *config.VolcEngineConfig) string {
	if cfg == nil {
		return DefaultVolcEngineBillTable
	}
	return orDefault(cfg.BillTable, DefaultVolcEngineBillTable)
}

// AliCloudMonthlyTableName is the table Alibaba Cloud monthly bills live in.
func AliCloudMonthlyTableName(cfg *config.AliCloudConfig) string {
	if cfg == nil {
		return DefaultAliCloudMonthlyTable
	}
	return orDefault(cfg.MonthlyTable, DefaultAliCloudMonthlyTable)
}

// AliCloudDailyTableName is the table Alibaba Cloud daily bills live in.
func AliCloudDailyTableName(cfg *config.AliCloudConfig) string {
	if cfg == nil {
		return DefaultAliCloudDailyTable
	}
	return orDefault(cfg.DailyTable, DefaultAliCloudDailyTable)
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

// volcEngineBillKey identifies one billing item: the period it belongs to, what
// was billed and which line of the bill it is. BillDetailId is the API's own id
// for that line; the columns before it keep the key a useful query prefix (every
// query starts from a period) and keep rows apart should the id ever come back
// empty.
//
// No amount takes part in it. Amounts are what gets corrected — a refund, a
// recomputed discount, an invoice adjustment — and a corrected row must replace
// the old one instead of settling next to it.
const volcEngineBillKey = "BillPeriod, ExpenseDate, InstanceNo, ExpenseBeginTime, Product, ElementCode, BillDetailId"

// VolcEngineBillTable is the VolcEngine bill detail table. Its columns keep the
// API's own PascalCase names so a row can be matched against the raw response.
func VolcEngineBillTable(name string) Table {
	return Table{
		Name:    name,
		Comment: "VolcEngine bill details, one row per billing item",
		Columns: volcEngineBillColumns,
		// updated_at is the version: the row pulled last wins, which is what
		// makes a re-pull of a corrected period converge instead of double count.
		Engine: "ReplacingMergeTree(updated_at)",
		// ExpenseDate is a String from the API. toDate() on an empty one throws
		// and takes the whole INSERT with it, so parse leniently: a row the
		// provider sent without a date lands in the 1970-01 partition instead of
		// failing the batch.
		PartitionBy: "toYYYYMM(toDate(parseDateTimeBestEffortOrZero(ExpenseDate)))",
		OrderBy:     "(" + volcEngineBillKey + ")",
		ShardBy:     "cityHash64(" + volcEngineBillKey + ")",
		Settings:    "index_granularity = 8192",
	}
}

// aliCloudBillKey identifies one Alibaba Cloud bill line within a period. There
// is no single id in the response, so the identity is spelled out: whose account,
// which product and instance, under which billing arrangement, what kind of line
// it is, where it ran and which split record it belongs to. As with VolcEngine,
// no amount is part of it.
//
// item is what tells an order from its refund or an adjustment: those lines
// agree on every other column, so leaving it out collapsed a charge and its
// refund into whichever merged last. region / zone keep apart the lines that
// have no instance at all (resource packages, marketplace, SMS), where the
// instance id cannot. adjust_type is not here because the SDK never fills it.
//
// line_seq closes the gap no business column can: two lines that still agree on
// everything above. The sync numbers the lines of each such group 0, 1, 2… as
// it pulls a period, so a re-pull lands on the same slots and replaces them,
// while two genuinely different lines no longer share a key. It relies on the
// sync clearing a period before re-pulling it — otherwise a group that shrank
// between pulls would keep its old tail.
const aliCloudBillKey = "product_code, instance_id, bill_account_id, subscription_type, billing_type, item, biz_type, product_detail_code, region, zone, split_item_id, line_seq"

// AliCloudMonthlyTable is the Alibaba Cloud monthly bill table.
func AliCloudMonthlyTable(name string) Table {
	return Table{
		Name:    name,
		Comment: "Alibaba Cloud bills at monthly granularity",
		Columns: aliCloudMonthlyColumns,
		Engine:  "ReplacingMergeTree(updated_at)",
		// billing_cycle is a String; an empty one used to blow up
		// parseDateTimeBestEffort and with it the whole INSERT.
		PartitionBy: "toYYYYMM(parseDateTimeBestEffortOrZero(concat(billing_cycle, '-01')))",
		OrderBy:     "(billing_cycle, " + aliCloudBillKey + ")",
		ShardBy:     "cityHash64(billing_cycle, " + aliCloudBillKey + ")",
	}
}

// AliCloudDailyTable is the Alibaba Cloud daily bill table.
func AliCloudDailyTable(name string) Table {
	return Table{
		Name:        name,
		Comment:     "Alibaba Cloud bills at daily granularity",
		Columns:     aliCloudDailyColumns,
		Engine:      "ReplacingMergeTree(updated_at)",
		PartitionBy: "toYYYYMMDD(billing_date)",
		OrderBy:     "(billing_date, " + aliCloudBillKey + ")",
		ShardBy:     "cityHash64(billing_date, " + aliCloudBillKey + ")",
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
	{Name: "Price", Type: MoneyType, Section: "价格信息字段"},
	{Name: "PriceUnit", Type: "String"},
	{Name: "PriceInterval", Type: "String"},
	{Name: "MarketPrice", Type: MoneyType},
	{Name: "MeasureInterval", Type: "String"},
	{Name: "Formula", Type: "String"},
	{Name: "OriginalBillAmount", Type: MoneyType, Section: "金额信息字段"},
	{Name: "PreferentialBillAmount", Type: MoneyType},
	{Name: "DiscountBillAmount", Type: MoneyType},
	{Name: "RoundAmount", Type: MoneyType},
	{Name: "PayableAmount", Type: MoneyType},
	{Name: "PreTaxPayableAmount", Type: MoneyType},
	{Name: "SettlePayableAmount", Type: MoneyType},
	{Name: "SettlePreTaxPayableAmount", Type: MoneyType},
	{Name: "PretaxAmount", Type: MoneyType},
	{Name: "PosttaxAmount", Type: MoneyType},
	{Name: "SettlePretaxAmount", Type: MoneyType},
	{Name: "SettlePosttaxAmount", Type: MoneyType},
	{Name: "Tax", Type: MoneyType},
	{Name: "SettleTax", Type: MoneyType},
	{Name: "TaxRate", Type: "String"},
	{Name: "PaidAmount", Type: MoneyType},
	{Name: "UnpaidAmount", Type: MoneyType},
	{Name: "CreditCarriedAmount", Type: MoneyType},
	{Name: "RealValue", Type: MoneyType, Section: "实际价值和结算信息"},
	{Name: "PretaxRealValue", Type: MoneyType},
	{Name: "SettleRealValue", Type: MoneyType},
	{Name: "SettlePretaxRealValue", Type: MoneyType},
	{Name: "CouponAmount", Type: MoneyType, Section: "优惠和抵扣信息"},
	{Name: "DiscountInfo", Type: "String"},
	{Name: "SavingPlanDeductionDiscountAmount", Type: MoneyType},
	{Name: "SavingPlanDeductionSpID", Type: "String"},
	{Name: "SavingPlanOriginalAmount", Type: MoneyType},
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
	{Name: "DiscountBizUnitPrice", Type: MoneyType},
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
	{Name: "item", Type: "String", Comment: "bill line type: SubscriptionOrder / PayAsYouGoBill / Refund / Adjustment"},
	{Name: "usage", Type: "String"},
	{Name: "usage_unit", Type: "String"},
	{Name: "pretax_gross_amount", Type: MoneyType},
	{Name: "invoice_discount", Type: MoneyType},
	{Name: "deducted_by_coupons", Type: MoneyType},
	{Name: "pretax_amount", Type: MoneyType},
	{Name: "currency_amount", Type: MoneyType},
	{Name: "payment_amount", Type: MoneyType},
	{Name: "outstanding_amount", Type: MoneyType},
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
	{Name: "adjust_amount", Type: MoneyType},
	{Name: "line_seq", Type: "UInt32", Comment: "ordinal among lines sharing the rest of the sorting key in one pull"},
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
	{Name: "item", Type: "String", Comment: "bill line type: SubscriptionOrder / PayAsYouGoBill / Refund / Adjustment"},
	{Name: "usage", Type: "String"},
	{Name: "usage_unit", Type: "String"},
	{Name: "pretax_gross_amount", Type: MoneyType},
	{Name: "invoice_discount", Type: MoneyType},
	{Name: "deducted_by_coupons", Type: MoneyType},
	{Name: "pretax_amount", Type: MoneyType},
	{Name: "currency_amount", Type: MoneyType},
	{Name: "payment_amount", Type: MoneyType},
	{Name: "outstanding_amount", Type: MoneyType},
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
	{Name: "adjust_amount", Type: MoneyType},
	{Name: "line_seq", Type: "UInt32", Comment: "ordinal among lines sharing the rest of the sorting key in one pull"},
	{Name: "granularity", Type: "String", Default: "'DAILY'"},
	{Name: "created_at", Type: "DateTime64(3)", Default: "now()"},
	{Name: "updated_at", Type: "DateTime64(3)", Default: "now()"},
}
