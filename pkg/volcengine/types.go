package volcengine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ListBillDetailRequest 查询账单明细请求
// 严格按照 github.com/volcengine/volcengine-go-sdk/service/billing.ListBillDetailInput 结构定义
type ListBillDetailRequest struct {
	// === 必需参数 ===
	BillPeriod string `json:"BillPeriod" validate:"required"`           // 账期，格式：YYYY-MM，如2025-08（必需参数）
	Limit      int32  `json:"Limit" validate:"required,min=1,max=1000"` // 每页数量，范围1-1000（必需参数）

	// === 可选参数 ===
	BillCategory  []string `json:"BillCategory,omitempty"`  // 账单分类
	BillingMode   []string `json:"BillingMode,omitempty"`   // 计费模式列表
	ExpenseDate   string   `json:"ExpenseDate,omitempty"`   // 费用日期
	GroupPeriod   int32    `json:"GroupPeriod,omitempty"`   // 分组周期，1-按天，2-按月
	GroupTerm     int32    `json:"GroupTerm,omitempty"`     // 分组条件
	IgnoreZero    int32    `json:"IgnoreZero,omitempty"`    // 是否忽略0元账单，1-忽略，0-不忽略
	InstanceNo    string   `json:"InstanceNo,omitempty"`    // 实例编号
	NeedRecordNum int32    `json:"NeedRecordNum,omitempty"` // 是否需要总记录数，1-需要，0-不需要
	Offset        int32    `json:"Offset,omitempty"`        // 偏移量，默认0
	OwnerID       []int64  `json:"OwnerID,omitempty"`       // 所有者ID列表
	PayerID       []int64  `json:"PayerID,omitempty"`       // 付款方ID列表
	Product       []string `json:"Product,omitempty"`       // 产品名称列表
}

// Validate 验证请求参数（严格按照VolcEngine API规范）
func (req *ListBillDetailRequest) Validate() error {
	// BillPeriod是必需参数
	if req.BillPeriod == "" {
		return fmt.Errorf("BillPeriod is required")
	}

	// 验证BillPeriod格式（YYYY-MM）
	if _, err := time.Parse("2006-01", req.BillPeriod); err != nil {
		return fmt.Errorf("BillPeriod must be in YYYY-MM format, got: %s", req.BillPeriod)
	}

	// Limit是必需参数
	if req.Limit <= 0 {
		return fmt.Errorf("Limit is required and must be greater than 0")
	}
	if req.Limit > 1000 {
		return fmt.Errorf("Limit must not exceed 1000, got: %d", req.Limit)
	}

	// Offset不能为负数
	if req.Offset < 0 {
		return fmt.Errorf("Offset must be non-negative, got: %d", req.Offset)
	}

	// GroupPeriod验证
	if req.GroupPeriod != 0 && req.GroupPeriod != 1 && req.GroupPeriod != 2 {
		return fmt.Errorf("GroupPeriod must be 1 (daily) or 2 (monthly), got: %d", req.GroupPeriod)
	}

	// IgnoreZero验证
	if req.IgnoreZero != 0 && req.IgnoreZero != 1 {
		return fmt.Errorf("IgnoreZero must be 0 or 1, got: %d", req.IgnoreZero)
	}

	// NeedRecordNum验证
	if req.NeedRecordNum != 0 && req.NeedRecordNum != 1 {
		return fmt.Errorf("NeedRecordNum must be 0 or 1, got: %d", req.NeedRecordNum)
	}

	return nil
}

// SetDefaults 设置默认值（按照VolcEngine API默认值）
func (req *ListBillDetailRequest) SetDefaults() {
	// 设置默认Limit
	if req.Limit == 0 {
		req.Limit = 50 // 改为更保守的默认值，避免限流
	}

	// 设置默认Offset
	if req.Offset == 0 {
		req.Offset = 0
	}

	// 设置默认GroupPeriod（按天分组）
	if req.GroupPeriod == 0 {
		req.GroupPeriod = 1
	}

	// 默认需要总记录数
	if req.NeedRecordNum == 0 {
		req.NeedRecordNum = 1
	}

	// 默认不忽略零元账单
	if req.IgnoreZero == 0 {
		req.IgnoreZero = 0
	}
}

// ListBillDetailResponse 查询账单明细响应
// 严格按照 github.com/volcengine/volcengine-go-sdk/service/billing.ListBillDetailOutput 结构定义
type ListBillDetailResponse struct {
	ResponseMetadata ResponseMetadata `json:"ResponseMetadata"` // API响应元数据
	Result           BillDetailResult `json:"Result"`           // 账单详情结果
}

// BillDetailResult 账单明细结果
// 对应SDK中的分页和列表数据
type BillDetailResult struct {
	List   []BillDetail `json:"List"`   // 账单详情列表
	Total  int32        `json:"Total"`  // 总记录数（对应SDK的*int32）
	Limit  int32        `json:"Limit"`  // 分页大小限制
	Offset int32        `json:"Offset"` // 分页偏移量
}

// ResponseMetadata API响应元数据
type ResponseMetadata struct {
	RequestID string `json:"RequestId"`
	Action    string `json:"Action"`
	Version   string `json:"Version"`
	Service   string `json:"Service"`
	Region    string `json:"Region"`
	Error     *Error `json:"Error,omitempty"`
}

// Error API错误信息
type Error struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// Error 实现error接口
func (e *Error) Error() string {
	return fmt.Sprintf("VolcEngine API Error: %s - %s", e.Code, e.Message)
}

// IsRetryable 判断错误是否可重试
func (e *Error) IsRetryable() bool {
	retryableCodes := []string{
		"InternalError",
		"ServiceUnavailable",
		"Throttling",
		"RequestTimeout",
	}

	for _, code := range retryableCodes {
		if strings.Contains(e.Code, code) {
			return true
		}
	}
	return false
}

// IsAuthError 判断是否为认证错误
func (e *Error) IsAuthError() bool {
	authCodes := []string{
		"InvalidAccessKey",
		"SignatureNotMatch",
		"InvalidToken",
		"Forbidden",
	}

	for _, code := range authCodes {
		if strings.Contains(e.Code, code) {
			return true
		}
	}
	return false
}

// BillDetail 账单明细项
// 严格按照 github.com/volcengine/volcengine-go-sdk/service/billing.ListForListBillDetailOutput 结构定义
// 包含SDK中的所有字段，确保完整性和准确性
type BillDetail struct {
	// === 核心标识字段 ===
	BillDetailID string `json:"BillDetailId"` // 账单详情ID（主键）
	BillID       string `json:"BillID"`       // 账单ID
	InstanceNo   string `json:"InstanceNo"`   // 实例编号

	// === 账期和时间字段 ===
	BillPeriod       string `json:"BillPeriod"`       // 账期（YYYY-MM）
	BusiPeriod       string `json:"BusiPeriod"`       // 业务周期
	ExpenseDate      string `json:"ExpenseDate"`      // 费用日期
	ExpenseBeginTime string `json:"ExpenseBeginTime"` // 费用开始时间
	ExpenseEndTime   string `json:"ExpenseEndTime"`   // 费用结束时间
	TradeTime        string `json:"TradeTime"`        // 交易时间

	// === 产品和服务信息 ===
	Product     string `json:"Product"`     // 产品名称（英文）
	ProductZh   string `json:"ProductZh"`   // 产品名称（中文）
	SolutionZh  string `json:"SolutionZh"`  // 解决方案中文名
	Element     string `json:"Element"`     // 计费项（英文）
	ElementCode string `json:"ElementCode"` // 计费项编码
	Factor      string `json:"Factor"`      // 计费因子
	FactorCode  string `json:"FactorCode"`  // 计费因子编码

	// === 配置信息 ===
	ConfigName        string `json:"ConfigName"`        // 配置名称
	ConfigurationCode string `json:"ConfigurationCode"` // 配置编码
	InstanceName      string `json:"InstanceName"`      // 实例名称

	// === 地域信息 ===
	Region        string `json:"Region"`        // 地域
	RegionCode    string `json:"RegionCode"`    // 地域编码
	Zone          string `json:"Zone"`          // 可用区
	ZoneCode      string `json:"ZoneCode"`      // 可用区编码
	CountryRegion string `json:"CountryRegion"` // 国家地域

	// === 用量和计费信息 ===
	Count                string `json:"Count"`                // 用量数量
	Unit                 string `json:"Unit"`                 // 用量单位
	UseDuration          string `json:"UseDuration"`          // 使用时长
	UseDurationUnit      string `json:"UseDurationUnit"`      // 使用时长单位
	DeductionCount       string `json:"DeductionCount"`       // 抵扣数量
	DeductionUseDuration string `json:"DeductionUseDuration"` // 抵扣使用时长

	// === 价格信息 ===
	Price           string `json:"Price"`           // 单价
	PriceUnit       string `json:"PriceUnit"`       // 价格单位
	PriceInterval   string `json:"PriceInterval"`   // 价格区间
	MarketPrice     string `json:"MarketPrice"`     // 市场价格
	Formula         string `json:"Formula"`         // 计费公式
	MeasureInterval string `json:"MeasureInterval"` // 计量区间

	// === 金额信息（核心） ===
	OriginalBillAmount     string  `json:"OriginalBillAmount"`     // 原始账单金额
	PreferentialBillAmount string  `json:"PreferentialBillAmount"` // 优惠后账单金额
	DiscountBillAmount     string  `json:"DiscountBillAmount"`     // 折扣账单金额
	RoundAmount            float64 `json:"RoundAmount"`            // 四舍五入金额

	// === 实际价值和结算信息 ===
	RealValue             string `json:"RealValue"`             // 实际价值
	PretaxRealValue       string `json:"PretaxRealValue"`       // 税前实际价值
	SettleRealValue       string `json:"SettleRealValue"`       // 结算实际价值
	SettlePretaxRealValue string `json:"SettlePretaxRealValue"` // 结算税前实际价值

	// === 应付金额信息 ===
	PayableAmount             string `json:"PayableAmount"`             // 应付金额
	PreTaxPayableAmount       string `json:"PreTaxPayableAmount"`       // 税前应付金额
	SettlePayableAmount       string `json:"SettlePayableAmount"`       // 结算应付金额
	SettlePreTaxPayableAmount string `json:"SettlePreTaxPayableAmount"` // 结算税前应付金额

	// === 税费信息 ===
	PretaxAmount        string `json:"PretaxAmount"`        // 税前金额
	PosttaxAmount       string `json:"PosttaxAmount"`       // 税后金额
	SettlePretaxAmount  string `json:"SettlePretaxAmount"`  // 结算税前金额
	SettlePosttaxAmount string `json:"SettlePosttaxAmount"` // 结算税后金额
	Tax                 string `json:"Tax"`                 // 税额
	SettleTax           string `json:"SettleTax"`           // 结算税额
	TaxRate             string `json:"TaxRate"`             // 税率

	// === 付款信息 ===
	PaidAmount          string `json:"PaidAmount"`          // 已付金额
	UnpaidAmount        string `json:"UnpaidAmount"`        // 未付金额
	CreditCarriedAmount string `json:"CreditCarriedAmount"` // 信用结转金额

	// === 优惠和抵扣信息 ===
	CouponAmount                      string `json:"CouponAmount"`                      // 代金券金额
	DiscountInfo                      string `json:"DiscountInfo"`                      // 折扣信息
	SavingPlanDeductionDiscountAmount string `json:"SavingPlanDeductionDiscountAmount"` // 节省计划抵扣折扣金额
	SavingPlanDeductionSpID           string `json:"SavingPlanDeductionSpID"`           // 节省计划抵扣SPID
	SavingPlanOriginalAmount          string `json:"SavingPlanOriginalAmount"`          // 节省计划原始金额
	ReservationInstance               string `json:"ReservationInstance"`               // 预留实例

	// === 货币信息 ===
	Currency           string `json:"Currency"`           // 货币类型
	CurrencySettlement string `json:"CurrencySettlement"` // 结算货币
	ExchangeRate       string `json:"ExchangeRate"`       // 汇率

	// === 计费模式信息 ===
	BillingMode       string `json:"BillingMode"`       // 计费模式
	BillingMethodCode string `json:"BillingMethodCode"` // 计费方法编码
	BillingFunction   string `json:"BillingFunction"`   // 计费函数
	BusinessMode      string `json:"BusinessMode"`      // 业务模式
	SellingMode       string `json:"SellingMode"`       // 销售模式
	SettlementType    string `json:"SettlementType"`    // 结算类型

	// === 折扣相关业务信息 ===
	DiscountBizBillingFunction   string `json:"DiscountBizBillingFunction"`   // 折扣业务计费函数
	DiscountBizMeasureInterval   string `json:"DiscountBizMeasureInterval"`   // 折扣业务计量区间
	DiscountBizUnitPrice         string `json:"DiscountBizUnitPrice"`         // 折扣业务单价
	DiscountBizUnitPriceInterval string `json:"DiscountBizUnitPriceInterval"` // 折扣业务单价区间

	// === 用户和组织信息 ===
	OwnerID            string `json:"OwnerID"`            // 所有者ID
	OwnerUserName      string `json:"OwnerUserName"`      // 所有者用户名
	OwnerCustomerName  string `json:"OwnerCustomerName"`  // 所有者客户名
	PayerID            string `json:"PayerID"`            // 付款方ID
	PayerUserName      string `json:"PayerUserName"`      // 付款方用户名
	PayerCustomerName  string `json:"PayerCustomerName"`  // 付款方客户名
	SellerID           string `json:"SellerID"`           // 销售方ID
	SellerUserName     string `json:"SellerUserName"`     // 销售方用户名
	SellerCustomerName string `json:"SellerCustomerName"` // 销售方客户名

	// === 项目和分类信息 ===
	Project            string `json:"Project"`            // 项目
	ProjectDisplayName string `json:"ProjectDisplayName"` // 项目显示名称
	BillCategory       string `json:"BillCategory"`       // 账单分类
	SubjectName        string `json:"SubjectName"`        // 主题名称
	Tag                string `json:"Tag"`                // 标签（JSON字符串）

	// === 其他业务信息 ===
	MainContractNumber string `json:"MainContractNumber"` // 主合同号
	OriginalOrderNo    string `json:"OriginalOrderNo"`    // 原始订单号
	EffectiveFactor    string `json:"EffectiveFactor"`    // 有效因子
	ExpandField        string `json:"ExpandField"`        // 扩展字段
}

// String 返回账单详情的字符串表示（使用新字段）
func (bd *BillDetail) String() string {
	return fmt.Sprintf("BillDetail{BillDetailID: %s, Product: %s, PreferentialBillAmount: %s %s, ExpenseDate: %s}",
		bd.BillDetailID, bd.Product, bd.PreferentialBillAmount, bd.Currency, bd.ExpenseDate)
}

// GetID 获取账单的主要标识ID（优先级：BillDetailID > InstanceNo > BillID）
func (bd *BillDetail) GetID() string {
	if bd.BillDetailID != "" && bd.BillDetailID != "-" {
		return bd.BillDetailID
	}
	if bd.InstanceNo != "" && bd.InstanceNo != "-" {
		return bd.InstanceNo
	}
	if bd.BillID != "" && bd.BillID != "-" {
		return bd.BillID
	}
	return ""
}

// GetAmount 获取主要金额（优惠后账单金额）
func (bd *BillDetail) GetAmount() float64 {
	if amount, err := strconv.ParseFloat(bd.PreferentialBillAmount, 64); err == nil {
		return amount
	}
	return 0.0
}

// IsZeroAmount 检查是否为零金额账单
func (bd *BillDetail) IsZeroAmount() bool {
	amount := bd.GetAmount()
	return amount == 0.0 || amount < 0.001 // 考虑浮点精度
}

// GetResourceIdentifier 获取资源标识符
func (bd *BillDetail) GetResourceIdentifier() string {
	if bd.InstanceNo != "" && bd.InstanceNo != "-" {
		return bd.InstanceNo
	}
	if bd.InstanceName != "" && bd.InstanceName != "-" {
		return bd.InstanceName
	}
	return fmt.Sprintf("%s-%s", bd.Product, bd.Region)
}

// ParseTags 解析标签JSON字符串为map
func (bd *BillDetail) ParseTags() (map[string]string, error) {
	if bd.Tag == "" {
		return make(map[string]string), nil
	}

	var tags map[string]string
	if err := json.Unmarshal([]byte(bd.Tag), &tags); err != nil {
		// 如果JSON解析失败，返回空map和错误
		return make(map[string]string), fmt.Errorf("failed to parse tags JSON: %w", err)
	}
	return tags, nil
}

// GetTagValue 获取指定标签的值
func (bd *BillDetail) GetTagValue(key string) string {
	tags, err := bd.ParseTags()
	if err != nil {
		return ""
	}
	return tags[key]
}

// HasTag 检查是否包含指定标签
func (bd *BillDetail) HasTag(key, value string) bool {
	tags, err := bd.ParseTags()
	if err != nil {
		return false
	}
	tagValue, exists := tags[key]
	return exists && tagValue == value
}

// Tag 结构已废弃，现在使用 BillDetail.Tag 字段（JSON字符串格式）
// 使用 BillDetail.ParseTags() 方法解析标签

// BillDetailForDB 用于数据库存储的账单明细结构
type BillDetailForDB struct {
	ID                 string                 `json:"id"`
	OwnerID            string                 `json:"owner_id"`
	OwnerUserName      string                 `json:"owner_user_name"`
	Product            string                 `json:"product"`
	ProductZh          string                 `json:"product_zh"`
	BillingMode        string                 `json:"billing_mode"`
	ExpenseTime        time.Time              `json:"expense_time"`
	ExpenseDate        string                 `json:"expense_date"`
	BillPeriod         string                 `json:"bill_period"`
	Amount             float64                `json:"amount"`
	Currency           string                 `json:"currency"`
	Region             string                 `json:"region"`
	Zone               string                 `json:"zone"`
	InstanceID         string                 `json:"instance_id"`
	InstanceName       string                 `json:"instance_name"`
	ResourceID         string                 `json:"resource_id"`
	ResourceName       string                 `json:"resource_name"`
	ConfigName         string                 `json:"config_name"`
	ConfigNameZh       string                 `json:"config_name_zh"`
	Element            string                 `json:"element"`
	ElementZh          string                 `json:"element_zh"`
	Price              float64                `json:"price"`
	PriceUnit          string                 `json:"price_unit"`
	Count              float64                `json:"count"`
	Unit               string                 `json:"unit"`
	DeductionAmount    float64                `json:"deduction_amount"`
	PreferentialInfo   string                 `json:"preferential_info"`
	Project            string                 `json:"project"`
	ProjectDisplayName string                 `json:"project_display_name"`
	Tags               map[string]interface{} `json:"tags"`
	RoundAmount        float64                `json:"round_amount"`
	UsageStartTime     time.Time              `json:"usage_start_time"`
	UsageEndTime       time.Time              `json:"usage_end_time"`
	CreatedAt          time.Time              `json:"created_at"`
	UpdatedAt          time.Time              `json:"updated_at"`
}

// ToDBFormat 转换为数据库格式（优化版本）
func (bd *BillDetail) ToDBFormat() *BillDetailForDB {
	return bd.ToDBFormatWithTime(time.Now())
}

// ToDBFormatWithTime 使用指定时间转换为数据库格式（重构版本）
// 适配新的BillDetail结构，保留核心字段用于数据库存储
func (bd *BillDetail) ToDBFormatWithTime(timestamp time.Time) *BillDetailForDB {
	// 解析标签JSON为map
	tags := make(map[string]interface{})
	if tagMap, err := bd.ParseTags(); err == nil {
		for k, v := range tagMap {
			tags[k] = v
		}
	}

	// 解析费用时间（从ExpenseDate字符串）
	var expenseTime time.Time
	if bd.ExpenseDate != "" {
		if t, err := time.Parse("2006-01-02", bd.ExpenseDate); err == nil {
			expenseTime = t
		} else if t, err := time.Parse("2006-01", bd.ExpenseDate); err == nil {
			expenseTime = t
		}
	}

	return &BillDetailForDB{
		ID:                 bd.GetID(), // 使用GetID()方法获取主ID
		OwnerID:            bd.OwnerID,
		OwnerUserName:      bd.OwnerUserName,
		Product:            bd.Product,
		ProductZh:          bd.ProductZh,
		BillingMode:        bd.BillingMode,
		ExpenseTime:        expenseTime, // 解析后的时间
		ExpenseDate:        bd.ExpenseDate,
		BillPeriod:         bd.BillPeriod,
		Amount:             bd.GetAmount(), // 使用GetAmount()获取主要金额
		Currency:           bd.Currency,
		Region:             bd.Region,
		Zone:               bd.Zone,
		InstanceID:         bd.InstanceNo, // 使用InstanceNo作为InstanceID
		InstanceName:       bd.InstanceName,
		ResourceID:         bd.InstanceNo,   // 使用InstanceNo作为ResourceID
		ResourceName:       bd.InstanceName, // 使用InstanceName作为ResourceName
		ConfigName:         bd.ConfigName,
		ConfigNameZh:       "", // 新结构中没有中文配置名
		Element:            bd.Element,
		ElementZh:          "",                     // 新结构中没有中文计费项名
		Price:              parseFloat64(bd.Price), // 转换字符串到float64
		PriceUnit:          bd.PriceUnit,
		Count:              parseFloat64(bd.Count), // 转换字符串到float64
		Unit:               bd.Unit,
		DeductionAmount:    parseFloat64(bd.CouponAmount), // 使用CouponAmount作为抵扣金额
		PreferentialInfo:   bd.DiscountInfo,               // 使用DiscountInfo作为优惠信息
		Project:            bd.Project,
		ProjectDisplayName: bd.ProjectDisplayName,
		Tags:               tags,
		RoundAmount:        bd.RoundAmount,
		UsageStartTime:     parseTimeFromString(bd.ExpenseBeginTime), // 解析开始时间
		UsageEndTime:       parseTimeFromString(bd.ExpenseEndTime),   // 解析结束时间
		CreatedAt:          timestamp,
		UpdatedAt:          timestamp,
	}
}

// parseTimeFromString 辅助函数：解析时间字符串
func parseTimeFromString(timeStr string) time.Time {
	if timeStr == "" {
		return time.Time{}
	}

	// 尝试不同的时间格式
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006-01",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t
		}
	}

	return time.Time{}
}

// ProcessingStats 处理统计信息
type ProcessingStats struct {
	StartTime        time.Time     `json:"start_time"`
	LastUpdateTime   time.Time     `json:"last_update_time"`
	TotalRecords     int           `json:"total_records"`
	ProcessedRecords int           `json:"processed_records"`
	CurrentBatch     int           `json:"current_batch"`
	TotalBatches     int           `json:"total_batches"`
	AverageSpeed     float64       `json:"average_speed"`  // records per second
	EstimatedTime    time.Duration `json:"estimated_time"` // remaining time
}

// Update 更新统计信息
func (ps *ProcessingStats) Update(processedRecords int) {
	now := time.Now()
	ps.LastUpdateTime = now
	ps.ProcessedRecords = processedRecords

	// 计算平均速度
	elapsed := now.Sub(ps.StartTime)
	if elapsed > 0 {
		ps.AverageSpeed = float64(processedRecords) / elapsed.Seconds()
	}

	// 估算剩余时间
	if ps.AverageSpeed > 0 && ps.TotalRecords > 0 {
		remaining := ps.TotalRecords - processedRecords
		if remaining > 0 {
			ps.EstimatedTime = time.Duration(float64(remaining)/ps.AverageSpeed) * time.Second
		}
	}
}

// GetProgress 获取进度百分比
func (ps *ProcessingStats) GetProgress() float64 {
	if ps.TotalRecords == 0 {
		return 0
	}
	return float64(ps.ProcessedRecords) / float64(ps.TotalRecords) * 100
}

// String 返回统计信息的字符串表示
func (ps *ProcessingStats) String() string {
	progress := ps.GetProgress()
	return fmt.Sprintf("Progress: %.1f%% (%d/%d), Speed: %.1f records/s, ETA: %v",
		progress, ps.ProcessedRecords, ps.TotalRecords, ps.AverageSpeed, ps.EstimatedTime)
}

// TransformationStats 数据转换统计信息
type TransformationStats struct {
	StartTime           time.Time     `json:"start_time"`
	EndTime             time.Time     `json:"end_time"`
	TotalRecords        int           `json:"total_records"`
	TransformedRecords  int           `json:"transformed_records"`
	FailedRecords       int           `json:"failed_records"`
	TransformationSpeed float64       `json:"transformation_speed"` // records per second
	Duration            time.Duration `json:"duration"`
	MemoryUsage         int64         `json:"memory_usage"`        // bytes
	AverageRecordSize   int64         `json:"average_record_size"` // bytes per record
	Errors              []string      `json:"errors,omitempty"`
}

// NewTransformationStats 创建新的转换统计信息
func NewTransformationStats() *TransformationStats {
	return &TransformationStats{
		StartTime: time.Now(),
		Errors:    make([]string, 0),
	}
}

// Finish 完成统计
func (ts *TransformationStats) Finish() {
	ts.EndTime = time.Now()
	ts.Duration = ts.EndTime.Sub(ts.StartTime)
	if ts.Duration > 0 {
		ts.TransformationSpeed = float64(ts.TransformedRecords) / ts.Duration.Seconds()
	}
	if ts.TransformedRecords > 0 {
		ts.AverageRecordSize = ts.MemoryUsage / int64(ts.TransformedRecords)
	}
}

// AddError 添加错误信息
func (ts *TransformationStats) AddError(err error) {
	if err != nil {
		ts.Errors = append(ts.Errors, err.Error())
		ts.FailedRecords++
	}
}

// UpdateMemoryUsage 更新内存使用情况
func (ts *TransformationStats) UpdateMemoryUsage(bytes int64) {
	ts.MemoryUsage += bytes
}

// IncrementTransformed 增加转换成功的记录数
func (ts *TransformationStats) IncrementTransformed() {
	ts.TransformedRecords++
}

// GetSuccessRate 获取转换成功率
func (ts *TransformationStats) GetSuccessRate() float64 {
	if ts.TotalRecords == 0 {
		return 0
	}
	return float64(ts.TransformedRecords) / float64(ts.TotalRecords) * 100
}

// String 返回转换统计信息的字符串表示
func (ts *TransformationStats) String() string {
	successRate := ts.GetSuccessRate()
	return fmt.Sprintf("TransformationStats{Transformed: %d/%d (%.1f%%), Speed: %.1f records/s, Duration: %v, MemoryUsage: %s}",
		ts.TransformedRecords, ts.TotalRecords, successRate, ts.TransformationSpeed, ts.Duration, formatBytes(ts.MemoryUsage))
}

// formatBytes 格式化字节数为可读字符串
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(bytes)/float64(div), "KMGTPE"[exp])
}

// ValidationError 数据验证错误
type ValidationError struct {
	Field   string `json:"field"`
	Value   string `json:"value"`
	Message string `json:"message"`
}

// Error 实现error接口
func (ve *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s' with value '%s': %s", ve.Field, ve.Value, ve.Message)
}

// Validate 验证BillDetail数据（重构版本）
// 适配新的BillDetail结构进行验证
func (bd *BillDetail) Validate() error {
	// 必需字段验证 - 使用新的字段和方法
	if bd.GetID() == "" {
		return &ValidationError{Field: "BillDetailID", Value: bd.GetID(), Message: "BillDetail ID cannot be empty"}
	}

	if bd.BillPeriod == "" {
		return &ValidationError{Field: "BillPeriod", Value: bd.BillPeriod, Message: "BillPeriod cannot be empty"}
	}

	if bd.Product == "" {
		return &ValidationError{Field: "Product", Value: bd.Product, Message: "Product cannot be empty"}
	}

	// 数值字段验证（现在都是字符串）
	amount := bd.GetAmount()
	if amount < 0 {
		return &ValidationError{Field: "PreferentialBillAmount", Value: strconv.FormatFloat(amount, 'f', 2, 64), Message: "Amount cannot be negative"}
	}

	// 验证Price字符串是否为有效数字
	if bd.Price != "" {
		if price, err := strconv.ParseFloat(bd.Price, 64); err != nil {
			return &ValidationError{Field: "Price", Value: bd.Price, Message: "Price must be a valid number"}
		} else if price < 0 {
			return &ValidationError{Field: "Price", Value: bd.Price, Message: "Price cannot be negative"}
		}
	}

	// 验证Count字符串是否为有效数字
	if bd.Count != "" {
		if count, err := strconv.ParseFloat(bd.Count, 64); err != nil {
			return &ValidationError{Field: "Count", Value: bd.Count, Message: "Count must be a valid number"}
		} else if count < 0 {
			return &ValidationError{Field: "Count", Value: bd.Count, Message: "Count cannot be negative"}
		}
	}

	// 时间字段验证（现在是字符串格式）
	if bd.ExpenseDate == "" {
		return &ValidationError{Field: "ExpenseDate", Value: bd.ExpenseDate, Message: "ExpenseDate cannot be empty"}
	}

	// 标签JSON验证
	if bd.Tag != "" {
		if _, err := bd.ParseTags(); err != nil {
			return &ValidationError{Field: "Tag", Value: bd.Tag, Message: fmt.Sprintf("Invalid JSON format: %v", err)}
		}
	}

	return nil
}

// BatchTransformationOptions 批量转换选项
type BatchTransformationOptions struct {
	BatchSize        int            `json:"batch_size"`         // 批次大小
	WorkerCount      int            `json:"worker_count"`       // 工作协程数
	EnableValidation bool           `json:"enable_validation"`  // 是否启用数据验证
	EnableStats      bool           `json:"enable_stats"`       // 是否启用统计信息
	SkipErrorRecords bool           `json:"skip_error_records"` // 是否跳过错误记录
	MemoryLimitMB    int            `json:"memory_limit_mb"`    // 内存限制（MB）
	TimeoutPerBatch  time.Duration  `json:"timeout_per_batch"`  // 每批次超时时间
	ProgressCallback func(int, int) `json:"-"`                  // 进度回调函数
}

// DefaultBatchTransformationOptions 返回默认的批量转换选项
func DefaultBatchTransformationOptions() *BatchTransformationOptions {
	return &BatchTransformationOptions{
		BatchSize:        1000,
		WorkerCount:      4,
		EnableValidation: true,
		EnableStats:      true,
		SkipErrorRecords: true,
		MemoryLimitMB:    100,
		TimeoutPerBatch:  30 * time.Second,
	}
}

// BatchTransformationResult 批量转换结果
type BatchTransformationResult struct {
	TransformedRecords []*BillDetailForDB   `json:"transformed_records"`
	Stats              *TransformationStats `json:"stats"`
	Errors             []error              `json:"errors,omitempty"`
}

// IsSuccess 检查批量转换是否成功
func (btr *BatchTransformationResult) IsSuccess() bool {
	return len(btr.Errors) == 0 && btr.Stats.FailedRecords == 0
}

// GetSuccessRate 获取成功率
func (btr *BatchTransformationResult) GetSuccessRate() float64 {
	return btr.Stats.GetSuccessRate()
}

// BatchTransformer 批量数据转换器
type BatchTransformer struct {
	options *BatchTransformationOptions
	mu      sync.RWMutex
}

// NewBatchTransformer 创建批量转换器
func NewBatchTransformer(options *BatchTransformationOptions) *BatchTransformer {
	if options == nil {
		options = DefaultBatchTransformationOptions()
	}
	return &BatchTransformer{
		options: options,
	}
}

// Transform 批量转换数据
func (bt *BatchTransformer) Transform(bills []BillDetail) (*BatchTransformationResult, error) {
	bt.mu.RLock()
	opts := bt.options
	bt.mu.RUnlock()

	stats := NewTransformationStats()
	stats.TotalRecords = len(bills)
	defer stats.Finish()

	result := &BatchTransformationResult{
		TransformedRecords: make([]*BillDetailForDB, 0, len(bills)),
		Stats:              stats,
		Errors:             make([]error, 0),
	}

	if len(bills) == 0 {
		return result, nil
	}

	// 创建工作池
	batchChan := make(chan []BillDetail, opts.WorkerCount)
	resultChan := make(chan *batchTransformResult, opts.WorkerCount)

	// 启动工作协程
	for i := 0; i < opts.WorkerCount; i++ {
		go bt.transformWorker(batchChan, resultChan, opts)
	}

	// 分批发送数据
	go func() {
		defer close(batchChan)
		for i := 0; i < len(bills); i += opts.BatchSize {
			end := i + opts.BatchSize
			if end > len(bills) {
				end = len(bills)
			}
			batchChan <- bills[i:end]
		}
	}()

	// 收集结果
	expectedBatches := (len(bills) + opts.BatchSize - 1) / opts.BatchSize
	for i := 0; i < expectedBatches; i++ {
		batchResult := <-resultChan

		// 合并结果
		result.TransformedRecords = append(result.TransformedRecords, batchResult.Records...)
		result.Errors = append(result.Errors, batchResult.Errors...)

		// 更新统计信息
		stats.TransformedRecords += batchResult.TransformedCount
		stats.FailedRecords += batchResult.FailedCount
		stats.UpdateMemoryUsage(batchResult.MemoryUsage)

		// 进度回调
		if opts.ProgressCallback != nil {
			opts.ProgressCallback(stats.TransformedRecords+stats.FailedRecords, stats.TotalRecords)
		}
	}

	return result, nil
}

// batchTransformResult 批量转换工作结果
type batchTransformResult struct {
	Records          []*BillDetailForDB
	TransformedCount int
	FailedCount      int
	MemoryUsage      int64
	Errors           []error
}

// transformWorker 转换工作协程
func (bt *BatchTransformer) transformWorker(batchChan <-chan []BillDetail, resultChan chan<- *batchTransformResult, opts *BatchTransformationOptions) {
	for batch := range batchChan {
		result := &batchTransformResult{
			Records: make([]*BillDetailForDB, 0, len(batch)),
			Errors:  make([]error, 0),
		}

		// 批量转换时间戳，减少时间调用
		timestamp := time.Now()

		for _, bill := range batch {
			// 数据验证
			if opts.EnableValidation {
				if err := bill.Validate(); err != nil {
					result.FailedCount++
					if !opts.SkipErrorRecords {
						result.Errors = append(result.Errors, err)
						continue
					}
					// 跳过错误记录但记录错误
					result.Errors = append(result.Errors, err)
					continue
				}
			}

			// 转换数据
			dbBill := bill.ToDBFormatWithTime(timestamp)
			result.Records = append(result.Records, dbBill)
			result.TransformedCount++

			// 估算内存使用（简单估算）
			if opts.EnableStats {
				result.MemoryUsage += bt.estimateRecordSize(dbBill)
			}
		}

		resultChan <- result
	}
}

// estimateRecordSize 估算记录大小
func (bt *BatchTransformer) estimateRecordSize(record *BillDetailForDB) int64 {
	// 简单估算：字符串长度 + 固定开销
	size := int64(0)
	size += int64(len(record.ID))
	size += int64(len(record.OwnerID))
	size += int64(len(record.OwnerUserName))
	size += int64(len(record.Product))
	size += int64(len(record.ProductZh))
	size += int64(len(record.BillingMode))
	size += int64(len(record.Currency))
	size += int64(len(record.Region))
	size += int64(len(record.Zone))
	size += int64(len(record.InstanceName))
	size += int64(len(record.ResourceName))
	size += int64(len(record.ConfigName))

	// 标签map大小
	for k, v := range record.Tags {
		size += int64(len(k))
		if str, ok := v.(string); ok {
			size += int64(len(str))
		}
	}

	// 固定字段开销（数字、时间等）
	size += 200 // 估算值

	return size
}

// SetOptions 设置转换选项
func (bt *BatchTransformer) SetOptions(options *BatchTransformationOptions) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	if options != nil {
		bt.options = options
	}
}

// GetOptions 获取转换选项
func (bt *BatchTransformer) GetOptions() *BatchTransformationOptions {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.options
}
