package alicloud

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DescribeInstanceBillRequest 查询实例账单请求
// 严格按照阿里云 BSS OpenAPI DescribeInstanceBill 接口定义
type DescribeInstanceBillRequest struct {
	// === 必需参数 ===
	BillingCycle string `json:"BillingCycle" validate:"required"` // 账期，格式：YYYY-MM（必需参数）

	// === 粒度控制 ===
	Granularity string `json:"Granularity,omitempty"` // MONTHLY(默认) 或 DAILY
	BillingDate string `json:"BillingDate,omitempty"` // 当 Granularity=DAILY 时必需，格式 YYYY-MM-DD

	// === 分页参数 ===
	MaxResults int32  `json:"MaxResults,omitempty"` // 每页条数，范围1-300，默认20
	NextToken  string `json:"NextToken,omitempty"`  // 分页标记

	// === 过滤参数 ===
	ProductCode            string   `json:"ProductCode,omitempty"`            // 产品代码
	ProductType            string   `json:"ProductType,omitempty"`            // 产品类型
	SubscriptionType       string   `json:"SubscriptionType,omitempty"`       // 付费方式
	InstanceIDs            []string `json:"InstanceIDs,omitempty"`            // 实例ID列表
	BillOwnerId            int64    `json:"BillOwnerId,omitempty"`            // 账单归属用户ID
	IsHideZeroCharge       bool     `json:"IsHideZeroCharge,omitempty"`       // 是否隐藏0元账单
	IsDisplayLocalCurrency bool     `json:"IsDisplayLocalCurrency,omitempty"` // 是否显示本币
}

// Validate 验证请求参数
func (req *DescribeInstanceBillRequest) Validate() error {
	// BillingCycle是必需参数
	if req.BillingCycle == "" {
		return fmt.Errorf("BillingCycle is required")
	}

	// 验证BillingCycle格式（YYYY-MM）
	if _, err := time.Parse("2006-01", req.BillingCycle); err != nil {
		return fmt.Errorf("BillingCycle must be in YYYY-MM format, got: %s", req.BillingCycle)
	}

	// 验证粒度参数
	if req.Granularity != "" && req.Granularity != "MONTHLY" && req.Granularity != "DAILY" {
		return fmt.Errorf("Granularity must be MONTHLY or DAILY, got: %s", req.Granularity)
	}

	// 当粒度为DAILY时，BillingDate是必需的
	if req.Granularity == "DAILY" && req.BillingDate == "" {
		return fmt.Errorf("BillingDate is required when Granularity is DAILY")
	}

	// 验证BillingDate格式和一致性
	if req.BillingDate != "" {
		billingDate, err := time.Parse("2006-01-02", req.BillingDate)
		if err != nil {
			return fmt.Errorf("BillingDate must be in YYYY-MM-DD format, got: %s", req.BillingDate)
		}

		// 验证BillingDate与BillingCycle的月份一致性
		expectedMonth := billingDate.Format("2006-01")
		if expectedMonth != req.BillingCycle {
			return fmt.Errorf("BillingDate (%s) must be in the same month as BillingCycle (%s)", req.BillingDate, req.BillingCycle)
		}
	}

	// MaxResults范围验证
	if req.MaxResults != 0 && (req.MaxResults < 1 || req.MaxResults > 300) {
		return fmt.Errorf("MaxResults must be between 1 and 300, got: %d", req.MaxResults)
	}

	return nil
}

// SetDefaults 设置默认值
func (req *DescribeInstanceBillRequest) SetDefaults() {
	// 设置默认粒度
	if req.Granularity == "" {
		req.Granularity = "MONTHLY"
	}

	// 设置默认分页大小
	if req.MaxResults == 0 {
		req.MaxResults = 100 // 设置较大的默认值以减少API调用次数
	}
}

// IsDaily 判断是否为按天粒度
func (req *DescribeInstanceBillRequest) IsDaily() bool {
	return req.Granularity == "DAILY"
}

// IsMonthly 判断是否为按月粒度
func (req *DescribeInstanceBillRequest) IsMonthly() bool {
	return req.Granularity == "MONTHLY" || req.Granularity == ""
}

// DescribeInstanceBillResponse 查询实例账单响应
type DescribeInstanceBillResponse struct {
	RequestId string             `json:"RequestId"` // 请求ID
	Success   bool               `json:"Success"`   // 是否成功
	Code      string             `json:"Code"`      // 响应代码
	Message   string             `json:"Message"`   // 响应消息
	Data      BillInstanceResult `json:"Data"`      // 账单数据
}

// BillInstanceResult 账单实例结果
type BillInstanceResult struct {
	BillingCycle string       `json:"BillingCycle"` // 账期
	AccountID    string       `json:"AccountID"`    // 账号ID
	AccountName  string       `json:"AccountName"`  // 账号名称
	TotalCount   int32        `json:"TotalCount"`   // 总记录数
	NextToken    string       `json:"NextToken"`    // 下一页标记
	MaxResults   int32        `json:"MaxResults"`   // 页大小
	Items        []BillDetail `json:"Items"`        // 账单明细列表
}

// BillDetail 账单明细项
// 严格按照阿里云 BSS OpenAPI 返回字段定义
type BillDetail struct {
	// === 核心标识字段 ===
	InstanceID      string `json:"InstanceID"`      // 实例ID
	InstanceName    string `json:"InstanceName"`    // 实例名称
	BillAccountID   string `json:"BillAccountID"`   // 账单归属账号ID
	BillAccountName string `json:"BillAccountName"` // 账单归属账号名称

	// === 时间字段 ===
	BillingDate string `json:"BillingDate"` // 账单日期（YYYY-MM-DD，仅DAILY粒度有值）

	// === 产品信息 ===
	ProductCode   string `json:"ProductCode"`   // 产品代码
	ProductName   string `json:"ProductName"`   // 产品名称
	ProductType   string `json:"ProductType"`   // 产品类型
	ProductDetail string `json:"ProductDetail"` // 产品明细

	// === 计费信息 ===
	SubscriptionType string `json:"SubscriptionType"` // 付费方式：Subscription-包年包月，PayAsYouGo-按量付费
	PricingUnit      string `json:"PricingUnit"`      // 计费单位
	Currency         string `json:"Currency"`         // 币种
	BillingType      string `json:"BillingType"`      // 账单类型

	// === 用量信息 ===
	Usage     string `json:"Usage"`     // 用量
	UsageUnit string `json:"UsageUnit"` // 用量单位

	// === 金额信息（核心） ===
	PretaxGrossAmount float64 `json:"PretaxGrossAmount"` // 税前原价
	InvoiceDiscount   float64 `json:"InvoiceDiscount"`   // 开票折扣金额
	DeductedByCoupons float64 `json:"DeductedByCoupons"` // 代金券抵扣金额
	PretaxAmount      float64 `json:"PretaxAmount"`      // 税前应付金额
	Currency_Amount   float64 `json:"Currency_Amount"`   // 本币金额
	PaymentAmount     float64 `json:"PaymentAmount"`     // 应付金额
	OutstandingAmount float64 `json:"OutstandingAmount"` // 未结算金额

	// === 地域信息 ===
	Region string `json:"Region"` // 地域
	Zone   string `json:"Zone"`   // 可用区

	// === 规格信息 ===
	InstanceSpec string `json:"InstanceSpec"` // 实例规格
	InternetIP   string `json:"InternetIP"`   // 公网IP
	IntranetIP   string `json:"IntranetIP"`   // 内网IP

	// === 资源组和标签 ===
	ResourceGroup string `json:"ResourceGroup"` // 资源组
	Tag           string `json:"Tag"`           // 标签（JSON字符串）

	// === 其他信息 ===
	ServicePeriod     string `json:"ServicePeriod"`     // 服务周期
	ServicePeriodUnit string `json:"ServicePeriodUnit"` // 服务周期单位
	ListPrice         string `json:"ListPrice"`         // 官网价格
	ListPriceUnit     string `json:"ListPriceUnit"`     // 官网价格单位
	OwnerID           string `json:"OwnerID"`           // 资源拥有者ID

	// === 成本分摊 ===
	SplitItemID      string `json:"SplitItemID"`      // 分拆项ID
	SplitItemName    string `json:"SplitItemName"`    // 分拆项名称
	SplitAccountID   string `json:"SplitAccountID"`   // 分拆账号ID
	SplitAccountName string `json:"SplitAccountName"` // 分拆账号名称

	// === 成本单元 ===
	CostUnit string `json:"CostUnit"` // 成本单元（阿里云特有）

	// === 订单信息 ===
	NickName          string `json:"NickName"`          // 用户昵称
	ProductDetailCode string `json:"ProductDetailCode"` // 产品明细代码

	// === 账单归属 ===
	BizType      string  `json:"BizType"`      // 业务类型
	AdjustType   string  `json:"AdjustType"`   // 调整类型
	AdjustAmount float64 `json:"AdjustAmount"` // 调整金额
}

// String 返回账单详情的字符串表示
func (bd *BillDetail) String() string {
	granularityInfo := ""
	if bd.BillingDate != "" {
		granularityInfo = fmt.Sprintf(", BillingDate: %s", bd.BillingDate)
	}

	return fmt.Sprintf("BillDetail{InstanceID: %s, ProductCode: %s, PaymentAmount: %.2f %s%s}",
		bd.InstanceID, bd.ProductCode, bd.PaymentAmount, bd.Currency, granularityInfo)
}

// GetID 获取账单的主要标识ID
func (bd *BillDetail) GetID() string {
	if bd.InstanceID != "" && bd.InstanceID != "-" {
		return bd.InstanceID
	}
	return fmt.Sprintf("%s-%s", bd.ProductCode, bd.BillAccountID)
}

// GetAmount 获取主要金额（应付金额）
func (bd *BillDetail) GetAmount() float64 {
	return bd.PaymentAmount
}

// IsZeroAmount 检查是否为零金额账单
func (bd *BillDetail) IsZeroAmount() bool {
	return bd.PaymentAmount == 0.0 || bd.PaymentAmount < 0.001 // 考虑浮点精度
}

// GetResourceIdentifier 获取资源标识符
func (bd *BillDetail) GetResourceIdentifier() string {
	if bd.InstanceID != "" && bd.InstanceID != "-" {
		return bd.InstanceID
	}
	if bd.InstanceName != "" && bd.InstanceName != "-" {
		return bd.InstanceName
	}
	return fmt.Sprintf("%s-%s", bd.ProductCode, bd.Region)
}

// ParseTags 解析标签字符串为map
// 支持两种格式：
// 1. JSON格式：{"key1":"value1","key2":"value2"}
// 2. 阿里云格式：key1:value1; key2:value2
func (bd *BillDetail) ParseTags() (map[string]string, error) {
	if bd.Tag == "" {
		return make(map[string]string), nil
	}

	// 尝试JSON格式解析
	var jsonTags map[string]string
	if err := json.Unmarshal([]byte(bd.Tag), &jsonTags); err == nil {
		return jsonTags, nil
	}

	// 尝试阿里云格式解析: key:value; key:value
	tags := make(map[string]string)
	tagPairs := strings.Split(bd.Tag, ";")
	for _, pair := range tagPairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// 查找第一个冒号作为分隔符
		colonIndex := strings.Index(pair, ":")
		if colonIndex == -1 {
			continue
		}

		key := strings.TrimSpace(pair[:colonIndex])
		value := strings.TrimSpace(pair[colonIndex+1:])

		if key != "" {
			tags[key] = value
		}
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

// IsDaily 判断是否为按天粒度的数据
func (bd *BillDetail) IsDaily() bool {
	return bd.BillingDate != ""
}

// IsMonthly 判断是否为按月粒度的数据
func (bd *BillDetail) IsMonthly() bool {
	return bd.BillingDate == ""
}

// GetTableSuffix 根据粒度获取表后缀
func (bd *BillDetail) GetTableSuffix() string {
	if bd.IsDaily() {
		return "daily"
	}
	return "monthly"
}

// BillDetailForDB 用于数据库存储的账单明细结构
type BillDetailForDB struct {
	ID              string `json:"id"`
	InstanceID      string `json:"instance_id"`
	InstanceName    string `json:"instance_name"`
	BillAccountID   string `json:"bill_account_id"`
	BillAccountName string `json:"bill_account_name"`

	// 时间字段
	BillingDate  *time.Time `json:"billing_date"` // 仅按天粒度有值
	BillingCycle string     `json:"billing_cycle"`

	// 产品信息
	ProductCode   string `json:"product_code"`
	ProductName   string `json:"product_name"`
	ProductType   string `json:"product_type"`
	ProductDetail string `json:"product_detail"`

	// 计费信息
	SubscriptionType string `json:"subscription_type"`
	PricingUnit      string `json:"pricing_unit"`
	Currency         string `json:"currency"`
	BillingType      string `json:"billing_type"`

	// 用量信息
	Usage     string `json:"usage"`
	UsageUnit string `json:"usage_unit"`

	// 金额信息
	PretaxGrossAmount float64 `json:"pretax_gross_amount"`
	InvoiceDiscount   float64 `json:"invoice_discount"`
	DeductedByCoupons float64 `json:"deducted_by_coupons"`
	PretaxAmount      float64 `json:"pretax_amount"`
	CurrencyAmount    float64 `json:"currency_amount"`
	PaymentAmount     float64 `json:"payment_amount"`
	OutstandingAmount float64 `json:"outstanding_amount"`

	// 地域信息
	Region string `json:"region"`
	Zone   string `json:"zone"`

	// 规格信息
	InstanceSpec string `json:"instance_spec"`
	InternetIP   string `json:"internet_ip"`
	IntranetIP   string `json:"intranet_ip"`

	// 标签和分组
	ResourceGroup string            `json:"resource_group"`
	Tags          map[string]string `json:"tags"`
	CostUnit      string            `json:"cost_unit"`

	// 服务期间和价格信息
	ServicePeriod     string `json:"service_period"`
	ServicePeriodUnit string `json:"service_period_unit"`
	ListPrice         string `json:"list_price"`
	ListPriceUnit     string `json:"list_price_unit"`
	OwnerID           string `json:"owner_id"`

	// 拆分账单信息
	SplitItemID      string `json:"split_item_id"`
	SplitItemName    string `json:"split_item_name"`
	SplitAccountID   string `json:"split_account_id"`
	SplitAccountName string `json:"split_account_name"`

	// 订单信息
	NickName          string `json:"nick_name"`
	ProductDetailCode string `json:"product_detail_code"`

	// 账单归属
	BizType      string  `json:"biz_type"`
	AdjustType   string  `json:"adjust_type"`
	AdjustAmount float64 `json:"adjust_amount"`

	// 系统字段
	Granularity string    `json:"granularity"` // MONTHLY 或 DAILY
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ToDBFormat 转换为数据库格式
func (bd *BillDetail) ToDBFormat() *BillDetailForDB {
	return bd.ToDBFormatWithTime(time.Now())
}

// ToDBFormatWithBillingCycle 使用指定的账期转换为数据库格式
func (bd *BillDetail) ToDBFormatWithBillingCycle(billingCycle string) *BillDetailForDB {
	// 完全依赖API返回的原始数据，不进行任何推导
	timestamp := time.Now()

	// 解析标签为map
	tags, err := bd.ParseTags()
	if err != nil {
		tags = make(map[string]string)
	}

	// 直接使用API返回的时间字段值，空就是空
	var billingDate *time.Time
	if bd.BillingDate != "" {
		if t, err := time.Parse("2006-01-02", bd.BillingDate); err == nil {
			billingDate = &t
		}
	}

	// 确定粒度
	granularity := "MONTHLY"
	if bd.IsDaily() {
		granularity = "DAILY"
	}

	return &BillDetailForDB{
		ID:              bd.GetID(),
		InstanceID:      bd.InstanceID,
		InstanceName:    bd.InstanceName,
		BillAccountID:   bd.BillAccountID,
		BillAccountName: bd.BillAccountName,

		BillingDate:  billingDate,
		BillingCycle: billingCycle, // 直接使用API返回的BillingCycle

		ProductCode:   bd.ProductCode,
		ProductName:   bd.ProductName,
		ProductType:   bd.ProductType,
		ProductDetail: bd.ProductDetail,

		SubscriptionType: bd.SubscriptionType,
		PricingUnit:      bd.PricingUnit,
		Currency:         bd.Currency,
		BillingType:      bd.BillingType,

		Usage:     bd.Usage,
		UsageUnit: bd.UsageUnit,

		PretaxGrossAmount: bd.PretaxGrossAmount,
		InvoiceDiscount:   bd.InvoiceDiscount,
		DeductedByCoupons: bd.DeductedByCoupons,
		PretaxAmount:      bd.PretaxAmount,
		CurrencyAmount:    bd.Currency_Amount,
		PaymentAmount:     bd.PaymentAmount,
		OutstandingAmount: bd.OutstandingAmount,

		Region: bd.Region,
		Zone:   bd.Zone,

		InstanceSpec: bd.InstanceSpec,
		InternetIP:   bd.InternetIP,
		IntranetIP:   bd.IntranetIP,

		ResourceGroup: bd.ResourceGroup,
		Tags:          tags,
		CostUnit:      bd.CostUnit,

		ServicePeriod:     bd.ServicePeriod,
		ServicePeriodUnit: bd.ServicePeriodUnit,
		ListPrice:         bd.ListPrice,
		ListPriceUnit:     bd.ListPriceUnit,
		OwnerID:           bd.OwnerID,

		SplitItemID:      bd.SplitItemID,
		SplitItemName:    bd.SplitItemName,
		SplitAccountID:   bd.SplitAccountID,
		SplitAccountName: bd.SplitAccountName,

		NickName:          bd.NickName,
		ProductDetailCode: bd.ProductDetailCode,

		BizType:      bd.BizType,
		AdjustType:   bd.AdjustType,
		AdjustAmount: bd.AdjustAmount,

		Granularity: granularity,
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
	}
}

// ToDBFormatWithTime 使用指定时间转换为数据库格式
func (bd *BillDetail) ToDBFormatWithTime(timestamp time.Time) *BillDetailForDB {
	// 完全依赖API返回的原始数据，不进行任何推导
	// 解析标签为map
	tags, err := bd.ParseTags()
	if err != nil {
		tags = make(map[string]string)
	}

	// 直接使用API返回的时间字段值，空就是空
	var billingDate *time.Time
	if bd.BillingDate != "" {
		if t, err := time.Parse("2006-01-02", bd.BillingDate); err == nil {
			billingDate = &t
		}
	}

	// 确定粒度
	granularity := "MONTHLY"
	if bd.IsDaily() {
		granularity = "DAILY"
	}

	// BillingCycle 留空，因为这个方法不传入BillingCycle
	billingCycle := ""

	return &BillDetailForDB{
		ID:              bd.GetID(),
		InstanceID:      bd.InstanceID,
		InstanceName:    bd.InstanceName,
		BillAccountID:   bd.BillAccountID,
		BillAccountName: bd.BillAccountName,

		BillingDate:  billingDate,
		BillingCycle: billingCycle,

		ProductCode:   bd.ProductCode,
		ProductName:   bd.ProductName,
		ProductType:   bd.ProductType,
		ProductDetail: bd.ProductDetail,

		SubscriptionType: bd.SubscriptionType,
		PricingUnit:      bd.PricingUnit,
		Currency:         bd.Currency,
		BillingType:      bd.BillingType,

		Usage:     bd.Usage,
		UsageUnit: bd.UsageUnit,

		PretaxGrossAmount: bd.PretaxGrossAmount,
		InvoiceDiscount:   bd.InvoiceDiscount,
		DeductedByCoupons: bd.DeductedByCoupons,
		PretaxAmount:      bd.PretaxAmount,
		CurrencyAmount:    bd.Currency_Amount,
		PaymentAmount:     bd.PaymentAmount,
		OutstandingAmount: bd.OutstandingAmount,

		Region: bd.Region,
		Zone:   bd.Zone,

		InstanceSpec: bd.InstanceSpec,
		InternetIP:   bd.InternetIP,
		IntranetIP:   bd.IntranetIP,

		ResourceGroup: bd.ResourceGroup,
		Tags:          tags,
		CostUnit:      bd.CostUnit,

		ServicePeriod:     bd.ServicePeriod,
		ServicePeriodUnit: bd.ServicePeriodUnit,
		ListPrice:         bd.ListPrice,
		ListPriceUnit:     bd.ListPriceUnit,
		OwnerID:           bd.OwnerID,

		SplitItemID:      bd.SplitItemID,
		SplitItemName:    bd.SplitItemName,
		SplitAccountID:   bd.SplitAccountID,
		SplitAccountName: bd.SplitAccountName,

		NickName:          bd.NickName,
		ProductDetailCode: bd.ProductDetailCode,

		BizType:      bd.BizType,
		AdjustType:   bd.AdjustType,
		AdjustAmount: bd.AdjustAmount,

		Granularity: granularity,
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
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
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
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

// Validate 验证BillDetail数据
func (bd *BillDetail) Validate() error {
	// 必需字段验证
	if bd.GetID() == "" {
		return &ValidationError{Field: "InstanceID", Value: bd.GetID(), Message: "Instance ID cannot be empty"}
	}

	if bd.ProductCode == "" {
		return &ValidationError{Field: "ProductCode", Value: bd.ProductCode, Message: "ProductCode cannot be empty"}
	}

	// 金额字段验证（允许负数，如退款、调账等场景）
	// PaymentAmount 可以为负数，表示退款、调账、欠费核销等场景

	// 时间字段验证
	if bd.IsDaily() && bd.BillingDate == "" {
		return &ValidationError{Field: "BillingDate", Value: bd.BillingDate, Message: "BillingDate cannot be empty for daily granularity"}
	}

	// 标签JSON验证
	if bd.Tag != "" {
		if _, err := bd.ParseTags(); err != nil {
			return &ValidationError{Field: "Tag", Value: bd.Tag, Message: fmt.Sprintf("Invalid JSON format: %v", err)}
		}
	}

	return nil
}

// ProcessingStats 处理统计信息（复用火山云的实现）
type ProcessingStats struct {
	StartTime        time.Time     `json:"start_time"`
	LastUpdateTime   time.Time     `json:"last_update_time"`
	TotalRecords     int           `json:"total_records"`
	ProcessedRecords int           `json:"processed_records"`
	CurrentBatch     int           `json:"current_batch"`
	TotalBatches     int           `json:"total_batches"`
	AverageSpeed     float64       `json:"average_speed"`  // records per second
	EstimatedTime    time.Duration `json:"estimated_time"` // remaining time
	Granularity      string        `json:"granularity"`    // MONTHLY 或 DAILY
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
	granularityInfo := ""
	if ps.Granularity != "" {
		granularityInfo = fmt.Sprintf(" [%s]", ps.Granularity)
	}
	return fmt.Sprintf("Progress: %.1f%% (%d/%d)%s, Speed: %.1f records/s, ETA: %v",
		progress, ps.ProcessedRecords, ps.TotalRecords, granularityInfo, ps.AverageSpeed, ps.EstimatedTime)
}

// parseFloat64 辅助函数：安全解析字符串为float64
func parseFloat64(s string) float64 {
	if s == "" || s == "-" {
		return 0.0
	}
	if val, err := strconv.ParseFloat(s, 64); err == nil {
		return val
	}
	return 0.0
}

// BatchTransformationOptions 批量转换选项（复用火山云的实现）
type BatchTransformationOptions struct {
	BatchSize        int            `json:"batch_size"`         // 批次大小
	WorkerCount      int            `json:"worker_count"`       // 工作协程数
	EnableValidation bool           `json:"enable_validation"`  // 是否启用数据验证
	EnableStats      bool           `json:"enable_stats"`       // 是否启用统计信息
	SkipErrorRecords bool           `json:"skip_error_records"` // 是否跳过错误记录
	MemoryLimitMB    int            `json:"memory_limit_mb"`    // 内存限制（MB）
	TimeoutPerBatch  time.Duration  `json:"timeout_per_batch"`  // 每批次超时时间
	ProgressCallback func(int, int) `json:"-"`                  // 进度回调函数
	Granularity      string         `json:"granularity"`        // 粒度类型
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
		Granularity:      "MONTHLY",
	}
}

// BatchTransformationResult 批量转换结果
type BatchTransformationResult struct {
	TransformedRecords []*BillDetailForDB `json:"transformed_records"`
	Stats              *ProcessingStats   `json:"stats"`
	Errors             []error            `json:"errors,omitempty"`
	Granularity        string             `json:"granularity"`
}

// IsSuccess 检查批量转换是否成功
func (btr *BatchTransformationResult) IsSuccess() bool {
	return len(btr.Errors) == 0
}

// GetSuccessRate 获取成功率
func (btr *BatchTransformationResult) GetSuccessRate() float64 {
	if btr.Stats.TotalRecords == 0 {
		return 0
	}
	return float64(btr.Stats.ProcessedRecords) / float64(btr.Stats.TotalRecords) * 100
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
	return bt.TransformWithBillingCycle(bills, "")
}

// TransformWithBillingCycle 批量转换数据，传入账期信息
func (bt *BatchTransformer) TransformWithBillingCycle(bills []BillDetail, billingCycle string) (*BatchTransformationResult, error) {
	bt.mu.RLock()
	opts := bt.options
	bt.mu.RUnlock()

	stats := &ProcessingStats{
		StartTime:    time.Now(),
		TotalRecords: len(bills),
		Granularity:  opts.Granularity,
	}

	result := &BatchTransformationResult{
		TransformedRecords: make([]*BillDetailForDB, 0, len(bills)),
		Stats:              stats,
		Errors:             make([]error, 0),
		Granularity:        opts.Granularity,
	}

	if len(bills) == 0 {
		return result, nil
	}

	// 批量转换时间戳，减少时间调用
	timestamp := time.Now()

	for _, bill := range bills {
		// 数据验证
		if opts.EnableValidation {
			if err := bill.Validate(); err != nil {
				if !opts.SkipErrorRecords {
					result.Errors = append(result.Errors, err)
					continue
				}
				// 跳过错误记录但记录错误
				result.Errors = append(result.Errors, err)
				continue
			}
		}

		// 转换数据，优先使用传入的BillingCycle
		var dbBill *BillDetailForDB
		if billingCycle != "" {
			// 使用API返回的BillingCycle
			dbBill = bill.ToDBFormatWithBillingCycle(billingCycle)
		} else {
			// 不传入BillingCycle的情况
			dbBill = bill.ToDBFormatWithTime(timestamp)
		}
		result.TransformedRecords = append(result.TransformedRecords, dbBill)
		stats.ProcessedRecords++

		// 更新进度回调
		if opts.ProgressCallback != nil {
			opts.ProgressCallback(stats.ProcessedRecords, stats.TotalRecords)
		}
	}

	// 更新最终统计
	stats.Update(stats.ProcessedRecords)

	return result, nil
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

// APIError 阿里云API错误
type APIError struct {
	RequestId string `json:"RequestId"`
	Code      string `json:"Code"`
	Message   string `json:"Message"`
}

// Error 实现error接口
func (e *APIError) Error() string {
	return fmt.Sprintf("AliCloud API Error: %s - %s (RequestId: %s)", e.Code, e.Message, e.RequestId)
}

// IsRetryable 判断错误是否可重试
func (e *APIError) IsRetryable() bool {
	retryableCodes := []string{
		"InternalError",
		"ServiceUnavailable",
		"Throttling",
		"RequestTimeout",
		"QpsLimitExceeded",
		"FlowLimitExceeded",
	}

	for _, code := range retryableCodes {
		if strings.Contains(e.Code, code) {
			return true
		}
	}
	return false
}

// IsAuthError 判断是否为认证错误
func (e *APIError) IsAuthError() bool {
	authCodes := []string{
		"InvalidAccessKeyId",
		"SignatureDoesNotMatch",
		"InvalidSecurityToken",
		"Forbidden",
		"Unauthorized",
	}

	for _, code := range authCodes {
		if strings.Contains(e.Code, code) {
			return true
		}
	}
	return false
}

// ValidateBillingCycle 验证账期格式和范围
func ValidateBillingCycle(billingCycle string) error {
	if billingCycle == "" {
		return fmt.Errorf("billing cycle cannot be empty")
	}

	// 验证格式
	cycleTime, err := time.Parse("2006-01", billingCycle)
	if err != nil {
		return fmt.Errorf("invalid billing cycle format, expected YYYY-MM: %s", billingCycle)
	}

	// 验证范围：阿里云支持18个月历史数据
	now := time.Now()
	earliestTime := now.AddDate(0, -18, 0)
	futureTime := now.AddDate(0, 1, 0) // 允许查询下个月

	if cycleTime.Before(earliestTime) {
		return fmt.Errorf("billing cycle %s is too old, only supports 18 months history", billingCycle)
	}

	if cycleTime.After(futureTime) {
		return fmt.Errorf("billing cycle %s is in the future", billingCycle)
	}

	return nil
}

// GenerateDatesInMonth 生成指定月份的所有日期
func GenerateDatesInMonth(billingCycle string) ([]string, error) {
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return nil, err
	}

	// 解析月份
	monthTime, err := time.Parse("2006-01", billingCycle)
	if err != nil {
		return nil, err
	}

	var dates []string

	// 获取该月的第一天和最后一天
	firstDay := time.Date(monthTime.Year(), monthTime.Month(), 1, 0, 0, 0, 0, monthTime.Location())
	lastDay := firstDay.AddDate(0, 1, -1)

	// 如果是当前月份，不要超过今天
	now := time.Now()
	if monthTime.Year() == now.Year() && monthTime.Month() == now.Month() {
		if lastDay.After(now) {
			lastDay = now
		}
	}

	// 生成日期列表
	for d := firstDay; !d.After(lastDay); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d.Format("2006-01-02"))
	}

	return dates, nil
}
