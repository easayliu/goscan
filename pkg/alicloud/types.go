package alicloud

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// 注意：请求和响应相关类型已迁移到 request.go 和 response.go 文件
// 这里保留对外的类型别名以保持向后兼容性

// 类型别名（保持向后兼容性）
type (
	// DescribeInstanceBillRequest 请求类型已迁移到 request.go
	// DescribeInstanceBillResponse 响应类型已迁移到 response.go
	// BillInstanceResult 结果类型已迁移到 response.go

	// 为保持向后兼容性，保留这些别名
	AliCloudBillRequest  = DescribeInstanceBillRequest
	AliCloudBillResponse = DescribeInstanceBillResponse
	AliCloudBillResult   = BillInstanceResult
)

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


// 注意：ValidationError 类型已迁移到 errors.go 文件

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

// 注意：ProcessingStats 类型已迁移到 response.go 文件


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

// 注意：APIError 类型已迁移到 errors.go 文件

// 注意：验证相关函数已迁移到 validator.go 文件
