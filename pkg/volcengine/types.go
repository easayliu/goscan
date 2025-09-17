package volcengine

import (
	"fmt"
	"time"
)

// ListBillDetailRequest 查询账单明细请求
type ListBillDetailRequest struct {
	// 必需参数
	BillPeriod string `json:"BillPeriod" validate:"required"`           // 账期，格式：YYYY-MM
	Limit      int32  `json:"Limit" validate:"required,min=1,max=1000"` // 每页数量，范围1-1000

	// 可选参数
	BillCategory  []string `json:"BillCategory,omitempty"`  // 账单分类
	BillingMode   []string `json:"BillingMode,omitempty"`   // 计费模式列表
	ExpenseDate   string   `json:"ExpenseDate,omitempty"`   // 费用日期
	GroupPeriod   int32    `json:"GroupPeriod,omitempty"`   // 分组周期，1-按天，2-按月
	GroupTerm     int32    `json:"GroupTerm,omitempty"`     // 分组条件
	IgnoreZero    int32    `json:"IgnoreZero,omitempty"`    // 是否忽略0元账单
	InstanceNo    string   `json:"InstanceNo,omitempty"`    // 实例编号
	NeedRecordNum int32    `json:"NeedRecordNum,omitempty"` // 是否需要总记录数
	Offset        int32    `json:"Offset,omitempty"`        // 偏移量
	OwnerID       []int64  `json:"OwnerID,omitempty"`       // 所有者ID列表
	PayerID       []int64  `json:"PayerID,omitempty"`       // 付款方ID列表
	Product       []string `json:"Product,omitempty"`       // 产品名称列表
}

// Validate 验证请求参数
func (req *ListBillDetailRequest) Validate() error {
	if req.BillPeriod == "" {
		return fmt.Errorf("BillPeriod is required")
	}

	if _, err := time.Parse("2006-01", req.BillPeriod); err != nil {
		return fmt.Errorf("BillPeriod must be in YYYY-MM format, got: %s", req.BillPeriod)
	}

	if req.Limit <= 0 {
		return fmt.Errorf("Limit is required and must be greater than 0")
	}
	if req.Limit > 1000 {
		return fmt.Errorf("Limit must not exceed 1000, got: %d", req.Limit)
	}

	if req.Offset < 0 {
		return fmt.Errorf("Offset must be non-negative, got: %d", req.Offset)
	}

	return nil
}

// SetDefaults 设置默认值
func (req *ListBillDetailRequest) SetDefaults() {
	if req.Limit == 0 {
		req.Limit = 50
	}
	if req.GroupPeriod == 0 {
		req.GroupPeriod = 1
	}
	if req.NeedRecordNum == 0 {
		req.NeedRecordNum = 1
	}
}

// ListBillDetailResponse 账单明细响应
type ListBillDetailResponse struct {
	ResponseMetadata ResponseMetadata `json:"ResponseMetadata"`
	Result           BillDetailResult `json:"Result"`
}

// BillDetailResult 账单明细结果
type BillDetailResult struct {
	List   []BillDetail `json:"List"`
	Total  int32        `json:"Total"`
	Limit  int32        `json:"Limit"`
	Offset int32        `json:"Offset"`
}

// ResponseMetadata 响应元数据
type ResponseMetadata struct {
	RequestID string `json:"RequestId"`
	Action    string `json:"Action"`
	Version   string `json:"Version"`
	Service   string `json:"Service"`
	Region    string `json:"Region"`
	Error     *Error `json:"Error,omitempty"`
}

// Error 错误信息
type Error struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// BillDetail 账单明细
type BillDetail struct {
	// 核心标识字段
	BillDetailID string `json:"BillDetailId" db:"BillDetailId"`
	BillID       string `json:"BillID" db:"BillID"`
	InstanceNo   string `json:"InstanceNo" db:"InstanceNo"`

	// 账期和时间字段
	BillPeriod       string `json:"BillPeriod" db:"BillPeriod"`
	BusiPeriod       string `json:"BusiPeriod" db:"BusiPeriod"`
	ExpenseDate      string `json:"ExpenseDate" db:"ExpenseDate"`
	ExpenseBeginTime string `json:"ExpenseBeginTime" db:"ExpenseBeginTime"`
	ExpenseEndTime   string `json:"ExpenseEndTime" db:"ExpenseEndTime"`
	TradeTime        string `json:"TradeTime" db:"TradeTime"`

	// 产品和服务信息
	Product     string `json:"Product" db:"Product"`
	ProductZh   string `json:"ProductZh" db:"ProductZh"`
	SolutionZh  string `json:"SolutionZh" db:"SolutionZh"`
	Element     string `json:"Element" db:"Element"`
	ElementCode string `json:"ElementCode" db:"ElementCode"`
	Factor      string `json:"Factor" db:"Factor"`
	FactorCode  string `json:"FactorCode" db:"FactorCode"`

	// 配置信息
	ConfigName        string `json:"ConfigName" db:"ConfigName"`
	ConfigurationCode string `json:"ConfigurationCode" db:"ConfigurationCode"`
	InstanceName      string `json:"InstanceName" db:"InstanceName"`

	// 地域信息
	Region        string `json:"Region" db:"Region"`
	RegionCode    string `json:"RegionCode" db:"RegionCode"`
	Zone          string `json:"Zone" db:"Zone"`
	ZoneCode      string `json:"ZoneCode" db:"ZoneCode"`
	CountryRegion string `json:"CountryRegion" db:"CountryRegion"`

	// 用量和计费信息
	Count                string `json:"Count" db:"Count"`
	Unit                 string `json:"Unit" db:"Unit"`
	UseDuration          string `json:"UseDuration" db:"UseDuration"`
	UseDurationUnit      string `json:"UseDurationUnit" db:"UseDurationUnit"`
	DeductionCount       string `json:"DeductionCount" db:"DeductionCount"`
	DeductionUseDuration string `json:"DeductionUseDuration" db:"DeductionUseDuration"`

	// 价格信息
	Price           string `json:"Price" db:"Price"`
	PriceUnit       string `json:"PriceUnit" db:"PriceUnit"`
	PriceInterval   string `json:"PriceInterval" db:"PriceInterval"`
	MarketPrice     string `json:"MarketPrice" db:"MarketPrice"`
	Formula         string `json:"Formula" db:"Formula"`
	MeasureInterval string `json:"MeasureInterval" db:"MeasureInterval"`

	// 金额信息
	OriginalBillAmount     string  `json:"OriginalBillAmount" db:"OriginalBillAmount"`
	PreferentialBillAmount string  `json:"PreferentialBillAmount" db:"PreferentialBillAmount"`
	DiscountBillAmount     string  `json:"DiscountBillAmount" db:"DiscountBillAmount"`
	RoundAmount            float64 `json:"RoundAmount" db:"RoundAmount"`

	// 实际价值和结算信息
	RealValue             string `json:"RealValue" db:"RealValue"`
	PretaxRealValue       string `json:"PretaxRealValue" db:"PretaxRealValue"`
	SettleRealValue       string `json:"SettleRealValue" db:"SettleRealValue"`
	SettlePretaxRealValue string `json:"SettlePretaxRealValue" db:"SettlePretaxRealValue"`

	// 应付金额信息
	PayableAmount             string `json:"PayableAmount" db:"PayableAmount"`
	PreTaxPayableAmount       string `json:"PreTaxPayableAmount" db:"PreTaxPayableAmount"`
	SettlePayableAmount       string `json:"SettlePayableAmount" db:"SettlePayableAmount"`
	SettlePreTaxPayableAmount string `json:"SettlePreTaxPayableAmount" db:"SettlePreTaxPayableAmount"`

	// 税费信息
	PretaxAmount        string `json:"PretaxAmount" db:"PretaxAmount"`
	PosttaxAmount       string `json:"PosttaxAmount" db:"PosttaxAmount"`
	SettlePretaxAmount  string `json:"SettlePretaxAmount" db:"SettlePretaxAmount"`
	SettlePosttaxAmount string `json:"SettlePosttaxAmount" db:"SettlePosttaxAmount"`
	Tax                 string `json:"Tax" db:"Tax"`
	SettleTax           string `json:"SettleTax" db:"SettleTax"`
	TaxRate             string `json:"TaxRate" db:"TaxRate"`

	// 付款信息
	PaidAmount          string `json:"PaidAmount" db:"PaidAmount"`
	UnpaidAmount        string `json:"UnpaidAmount" db:"UnpaidAmount"`
	CreditCarriedAmount string `json:"CreditCarriedAmount" db:"CreditCarriedAmount"`

	// 优惠和抵扣信息
	CouponAmount                      string `json:"CouponAmount" db:"CouponAmount"`
	DiscountInfo                      string `json:"DiscountInfo" db:"DiscountInfo"`
	SavingPlanDeductionDiscountAmount string `json:"SavingPlanDeductionDiscountAmount" db:"SavingPlanDeductionDiscountAmount"`
	SavingPlanDeductionSpID           string `json:"SavingPlanDeductionSpID" db:"SavingPlanDeductionSpID"`
	SavingPlanOriginalAmount          string `json:"SavingPlanOriginalAmount" db:"SavingPlanOriginalAmount"`
	ReservationInstance               string `json:"ReservationInstance" db:"ReservationInstance"`

	// 货币信息
	Currency           string `json:"Currency" db:"Currency"`
	CurrencySettlement string `json:"CurrencySettlement" db:"CurrencySettlement"`
	ExchangeRate       string `json:"ExchangeRate" db:"ExchangeRate"`

	// 计费模式信息
	BillingMode       string `json:"BillingMode" db:"BillingMode"`
	BillingMethodCode string `json:"BillingMethodCode" db:"BillingMethodCode"`
	BillingFunction   string `json:"BillingFunction" db:"BillingFunction"`
	BusinessMode      string `json:"BusinessMode" db:"BusinessMode"`
	SellingMode       string `json:"SellingMode" db:"SellingMode"`
	SettlementType    string `json:"SettlementType" db:"SettlementType"`

	// 折扣相关业务信息
	DiscountBizBillingFunction   string `json:"DiscountBizBillingFunction" db:"DiscountBizBillingFunction"`
	DiscountBizMeasureInterval   string `json:"DiscountBizMeasureInterval" db:"DiscountBizMeasureInterval"`
	DiscountBizUnitPrice         string `json:"DiscountBizUnitPrice" db:"DiscountBizUnitPrice"`
	DiscountBizUnitPriceInterval string `json:"DiscountBizUnitPriceInterval" db:"DiscountBizUnitPriceInterval"`

	// 用户和组织信息
	OwnerID            string `json:"OwnerID" db:"OwnerID"`
	OwnerUserName      string `json:"OwnerUserName" db:"OwnerUserName"`
	OwnerCustomerName  string `json:"OwnerCustomerName" db:"OwnerCustomerName"`
	PayerID            string `json:"PayerID" db:"PayerID"`
	PayerUserName      string `json:"PayerUserName" db:"PayerUserName"`
	PayerCustomerName  string `json:"PayerCustomerName" db:"PayerCustomerName"`
	SellerID           string `json:"SellerID" db:"SellerID"`
	SellerUserName     string `json:"SellerUserName" db:"SellerUserName"`
	SellerCustomerName string `json:"SellerCustomerName" db:"SellerCustomerName"`

	// 项目和分类信息
	Project            string `json:"Project" db:"Project"`
	ProjectDisplayName string `json:"ProjectDisplayName" db:"ProjectDisplayName"`
	BillCategory       string `json:"BillCategory" db:"BillCategory"`
	SubjectName        string `json:"SubjectName" db:"SubjectName"`
	Tag                string `json:"Tag" db:"Tag"`

	// 其他业务信息
	MainContractNumber string `json:"MainContractNumber" db:"MainContractNumber"`
	OriginalOrderNo    string `json:"OriginalOrderNo" db:"OriginalOrderNo"`
	EffectiveFactor    string `json:"EffectiveFactor" db:"EffectiveFactor"`
	ExpandField        string `json:"ExpandField" db:"ExpandField"`
}

// ToDBMap 转换为数据库插入映射
func (bd *BillDetail) ToDBMap() map[string]interface{} {
	data := make(map[string]interface{})

	// 使用反射或手动映射关键字段
	data["BillDetailId"] = bd.BillDetailID
	data["BillID"] = bd.BillID
	data["InstanceNo"] = bd.InstanceNo
	data["BillPeriod"] = bd.BillPeriod
	data["BusiPeriod"] = bd.BusiPeriod
	data["ExpenseDate"] = bd.ExpenseDate
	data["ExpenseBeginTime"] = bd.ExpenseBeginTime
	data["ExpenseEndTime"] = bd.ExpenseEndTime
	data["TradeTime"] = bd.TradeTime
	data["Product"] = bd.Product
	data["ProductZh"] = bd.ProductZh
	data["SolutionZh"] = bd.SolutionZh
	data["Element"] = bd.Element
	data["ElementCode"] = bd.ElementCode
	data["Factor"] = bd.Factor
	data["FactorCode"] = bd.FactorCode
	data["ConfigName"] = bd.ConfigName
	data["ConfigurationCode"] = bd.ConfigurationCode
	data["InstanceName"] = bd.InstanceName
	data["Region"] = bd.Region
	data["RegionCode"] = bd.RegionCode
	data["Zone"] = bd.Zone
	data["ZoneCode"] = bd.ZoneCode
	data["CountryRegion"] = bd.CountryRegion
	data["Count"] = bd.Count
	data["Unit"] = bd.Unit
	data["UseDuration"] = bd.UseDuration
	data["UseDurationUnit"] = bd.UseDurationUnit
	data["DeductionCount"] = bd.DeductionCount
	data["DeductionUseDuration"] = bd.DeductionUseDuration
	data["Price"] = bd.Price
	data["PriceUnit"] = bd.PriceUnit
	data["PriceInterval"] = bd.PriceInterval
	data["MarketPrice"] = bd.MarketPrice
	data["Formula"] = bd.Formula
	data["MeasureInterval"] = bd.MeasureInterval
	data["OriginalBillAmount"] = bd.OriginalBillAmount
	data["PreferentialBillAmount"] = bd.PreferentialBillAmount
	data["DiscountBillAmount"] = bd.DiscountBillAmount
	data["RoundAmount"] = bd.RoundAmount
	data["RealValue"] = bd.RealValue
	data["PretaxRealValue"] = bd.PretaxRealValue
	data["SettleRealValue"] = bd.SettleRealValue
	data["SettlePretaxRealValue"] = bd.SettlePretaxRealValue
	data["PayableAmount"] = bd.PayableAmount
	data["PreTaxPayableAmount"] = bd.PreTaxPayableAmount
	data["SettlePayableAmount"] = bd.SettlePayableAmount
	data["SettlePreTaxPayableAmount"] = bd.SettlePreTaxPayableAmount
	data["PretaxAmount"] = bd.PretaxAmount
	data["PosttaxAmount"] = bd.PosttaxAmount
	data["SettlePretaxAmount"] = bd.SettlePretaxAmount
	data["SettlePosttaxAmount"] = bd.SettlePosttaxAmount
	data["Tax"] = bd.Tax
	data["SettleTax"] = bd.SettleTax
	data["TaxRate"] = bd.TaxRate
	data["PaidAmount"] = bd.PaidAmount
	data["UnpaidAmount"] = bd.UnpaidAmount
	data["CreditCarriedAmount"] = bd.CreditCarriedAmount
	data["CouponAmount"] = bd.CouponAmount
	data["DiscountInfo"] = bd.DiscountInfo
	data["SavingPlanDeductionDiscountAmount"] = bd.SavingPlanDeductionDiscountAmount
	data["SavingPlanDeductionSpID"] = bd.SavingPlanDeductionSpID
	data["SavingPlanOriginalAmount"] = bd.SavingPlanOriginalAmount
	data["ReservationInstance"] = bd.ReservationInstance
	data["Currency"] = bd.Currency
	data["CurrencySettlement"] = bd.CurrencySettlement
	data["ExchangeRate"] = bd.ExchangeRate
	data["BillingMode"] = bd.BillingMode
	data["BillingMethodCode"] = bd.BillingMethodCode
	data["BillingFunction"] = bd.BillingFunction
	data["BusinessMode"] = bd.BusinessMode
	data["SellingMode"] = bd.SellingMode
	data["SettlementType"] = bd.SettlementType
	data["DiscountBizBillingFunction"] = bd.DiscountBizBillingFunction
	data["DiscountBizMeasureInterval"] = bd.DiscountBizMeasureInterval
	data["DiscountBizUnitPrice"] = bd.DiscountBizUnitPrice
	data["DiscountBizUnitPriceInterval"] = bd.DiscountBizUnitPriceInterval
	data["OwnerID"] = bd.OwnerID
	data["OwnerUserName"] = bd.OwnerUserName
	data["OwnerCustomerName"] = bd.OwnerCustomerName
	data["PayerID"] = bd.PayerID
	data["PayerUserName"] = bd.PayerUserName
	data["PayerCustomerName"] = bd.PayerCustomerName
	data["SellerID"] = bd.SellerID
	data["SellerUserName"] = bd.SellerUserName
	data["SellerCustomerName"] = bd.SellerCustomerName
	data["Project"] = bd.Project
	data["ProjectDisplayName"] = bd.ProjectDisplayName
	data["BillCategory"] = bd.BillCategory
	data["SubjectName"] = bd.SubjectName
	data["Tag"] = bd.Tag
	data["MainContractNumber"] = bd.MainContractNumber
	data["OriginalOrderNo"] = bd.OriginalOrderNo
	data["EffectiveFactor"] = bd.EffectiveFactor
	data["ExpandField"] = bd.ExpandField

	return data
}

// TableHealthStatus 表健康状态
type TableHealthStatus struct {
	LocalTableExists       bool   `json:"local_table_exists"`
	DistributedTableExists bool   `json:"distributed_table_exists"`
	IsHealthy              bool   `json:"is_healthy"`
	NeedsRepair            bool   `json:"needs_repair"`
	ErrorMessage           string `json:"error_message,omitempty"`
}

// String 实现 Stringer 接口
func (ths *TableHealthStatus) String() string {
	status := "健康"
	if !ths.IsHealthy {
		status = "异常"
	}

	return fmt.Sprintf("表状态: %s, 本地表: %v, 分布式表: %v, 需要修复: %v",
		status, ths.LocalTableExists, ths.DistributedTableExists, ths.NeedsRepair)
}

// DataComparisonResult 数据对比结果
type DataComparisonResult struct {
	APICount      int32  `json:"api_count"`
	DatabaseCount int64  `json:"database_count"`
	BillPeriod    string `json:"bill_period"`
	Granularity   string `json:"granularity"`
	NeedSync      bool   `json:"need_sync"`
	Reason        string `json:"reason"`
}

// IntelligentSyncOptions 智能同步选项
type IntelligentSyncOptions struct {
	TableName            string `json:"table_name"`
	IsDistributed        bool   `json:"is_distributed"`
	EnablePreCheck       bool   `json:"enable_pre_check"`
	EnableProgressReport bool   `json:"enable_progress_report"`
	CleanupCondition     string `json:"cleanup_condition,omitempty"`
}

// InitialPullResult 初始拉取结果
type InitialPullResult struct {
	PullResults     []*SyncResult `json:"pull_results"`
	TotalPeriods    int           `json:"total_periods"`
	SuccessfulPulls int           `json:"successful_pulls"`
	FailedPulls     int           `json:"failed_pulls"`
	TotalRecords    int           `json:"total_records"`
	Duration        time.Duration `json:"duration"`
	Strategy        *PullStrategy `json:"strategy"`
}

// ValidationResult 验证结果
type ValidationResult struct {
	IsValid     bool     `json:"is_valid"`
	Issues      []string `json:"issues"`
	Suggestions []string `json:"suggestions"`
}

// PullStrategy 拉取策略
type PullStrategy struct {
	BatchSize       int           `json:"batch_size"`
	ConcurrentLimit int           `json:"concurrent_limit"`
	RetryAttempts   int           `json:"retry_attempts"`
	DelayBetween    time.Duration `json:"delay_between"`
	EstimatedTime   time.Duration `json:"estimated_time"`
}

// SmartBillResponse 智能账单响应
type SmartBillResponse struct {
	Bills        []BillDetail `json:"bills"`
	Summary      BillSummary  `json:"summary"`
	FilteredFrom []string     `json:"filtered_from"`
	DateRange    string       `json:"date_range"`
}

// BillSummary 账单汇总
type BillSummary struct {
	TotalRecords   int      `json:"total_records"`
	TotalAmount    string   `json:"total_amount"`
	PeriodsCovered []string `json:"periods_covered"`
	TopProducts    []string `json:"top_products"`
}
