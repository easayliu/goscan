package alicloud

import (
	"fmt"
	"goscan/pkg/logger"
	"time"

	"go.uber.org/zap"
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
	validator := NewValidator()
	return validator.ValidateRequest(req)
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

// GetEffectiveGranularity 获取有效的粒度
func (req *DescribeInstanceBillRequest) GetEffectiveGranularity() string {
	if req.Granularity == "" {
		return "MONTHLY"
	}
	return req.Granularity
}

// GetKey 获取请求的唯一标识
func (req *DescribeInstanceBillRequest) GetKey() string {
	key := fmt.Sprintf("%s-%s", req.BillingCycle, req.GetEffectiveGranularity())
	if req.BillingDate != "" {
		key += "-" + req.BillingDate
	}
	if req.ProductCode != "" {
		key += "-" + req.ProductCode
	}
	return key
}

// Clone 克隆请求对象
func (req *DescribeInstanceBillRequest) Clone() *DescribeInstanceBillRequest {
	cloned := &DescribeInstanceBillRequest{
		BillingCycle:           req.BillingCycle,
		Granularity:            req.Granularity,
		BillingDate:            req.BillingDate,
		MaxResults:             req.MaxResults,
		NextToken:              req.NextToken,
		ProductCode:            req.ProductCode,
		ProductType:            req.ProductType,
		SubscriptionType:       req.SubscriptionType,
		BillOwnerId:            req.BillOwnerId,
		IsHideZeroCharge:       req.IsHideZeroCharge,
		IsDisplayLocalCurrency: req.IsDisplayLocalCurrency,
	}

	// 深拷贝切片
	if req.InstanceIDs != nil {
		cloned.InstanceIDs = make([]string, len(req.InstanceIDs))
		copy(cloned.InstanceIDs, req.InstanceIDs)
	}

	return cloned
}

// ResetPagination 重置分页参数
func (req *DescribeInstanceBillRequest) ResetPagination() {
	req.NextToken = ""
}

// SetNextToken 设置下一页标记
func (req *DescribeInstanceBillRequest) SetNextToken(token string) {
	req.NextToken = token
}

// HasMorePages 判断是否还有更多页
func (req *DescribeInstanceBillRequest) HasMorePages() bool {
	return req.NextToken != ""
}

// GetTimeRange 获取时间范围信息
func (req *DescribeInstanceBillRequest) GetTimeRange() (start, end time.Time, err error) {
	if req.IsDaily() && req.BillingDate != "" {
		// 按天粒度，返回具体日期
		start, err = time.Parse("2006-01-02", req.BillingDate)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid billing date: %w", err)
		}
		end = start.AddDate(0, 0, 1) // 下一天
		return start, end, nil
	}

	// 按月粒度，返回整个月份
	start, err = time.Parse("2006-01", req.BillingCycle)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid billing cycle: %w", err)
	}
	end = start.AddDate(0, 1, 0) // 下个月
	return start, end, nil
}

// String 返回请求的字符串表示
func (req *DescribeInstanceBillRequest) String() string {
	granularityInfo := req.GetEffectiveGranularity()
	if req.BillingDate != "" {
		granularityInfo += fmt.Sprintf("(%s)", req.BillingDate)
	}

	return fmt.Sprintf("DescribeInstanceBillRequest{BillingCycle: %s, Granularity: %s, MaxResults: %d}",
		req.BillingCycle, granularityInfo, req.MaxResults)
}

// RequestBuilder 请求构建器
type RequestBuilder struct {
	request *DescribeInstanceBillRequest
}

// NewRequestBuilder 创建请求构建器
func NewRequestBuilder(billingCycle string) *RequestBuilder {
	return &RequestBuilder{
		request: &DescribeInstanceBillRequest{
			BillingCycle: billingCycle,
		},
	}
}

// WithGranularity 设置粒度
func (rb *RequestBuilder) WithGranularity(granularity string) *RequestBuilder {
	rb.request.Granularity = granularity
	return rb
}

// WithBillingDate 设置账单日期
func (rb *RequestBuilder) WithBillingDate(billingDate string) *RequestBuilder {
	rb.request.BillingDate = billingDate
	return rb
}

// WithMaxResults 设置分页大小
func (rb *RequestBuilder) WithMaxResults(maxResults int32) *RequestBuilder {
	rb.request.MaxResults = maxResults
	return rb
}

// WithProductCode 设置产品代码
func (rb *RequestBuilder) WithProductCode(productCode string) *RequestBuilder {
	rb.request.ProductCode = productCode
	return rb
}

// WithProductType 设置产品类型
func (rb *RequestBuilder) WithProductType(productType string) *RequestBuilder {
	rb.request.ProductType = productType
	return rb
}

// WithSubscriptionType 设置付费方式
func (rb *RequestBuilder) WithSubscriptionType(subscriptionType string) *RequestBuilder {
	rb.request.SubscriptionType = subscriptionType
	return rb
}

// WithInstanceIDs 设置实例ID列表
func (rb *RequestBuilder) WithInstanceIDs(instanceIDs []string) *RequestBuilder {
	rb.request.InstanceIDs = instanceIDs
	return rb
}

// WithBillOwnerId 设置账单归属用户ID
func (rb *RequestBuilder) WithBillOwnerId(billOwnerId int64) *RequestBuilder {
	rb.request.BillOwnerId = billOwnerId
	return rb
}

// WithHideZeroCharge 设置是否隐藏0元账单
func (rb *RequestBuilder) WithHideZeroCharge(hideZeroCharge bool) *RequestBuilder {
	rb.request.IsHideZeroCharge = hideZeroCharge
	return rb
}

// WithDisplayLocalCurrency 设置是否显示本币
func (rb *RequestBuilder) WithDisplayLocalCurrency(displayLocalCurrency bool) *RequestBuilder {
	rb.request.IsDisplayLocalCurrency = displayLocalCurrency
	return rb
}

// Build 构建请求
func (rb *RequestBuilder) Build() (*DescribeInstanceBillRequest, error) {
	req := rb.request.Clone()
	req.SetDefaults()

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request parameters: %w", err)
	}

	return req, nil
}

// MustBuild builds request, exits with fatal error if failed
func (rb *RequestBuilder) MustBuild() *DescribeInstanceBillRequest {
	req, err := rb.Build()
	if err != nil {
		logger.Fatal("failed to build request", zap.Error(err))
	}
	return req
}

// 便利构造函数

// NewMonthlyRequest 创建按月请求
func NewMonthlyRequest(billingCycle string) *DescribeInstanceBillRequest {
	return NewRequestBuilder(billingCycle).
		WithGranularity("MONTHLY").
		MustBuild()
}

// NewDailyRequest 创建按天请求
func NewDailyRequest(billingCycle, billingDate string) *DescribeInstanceBillRequest {
	return NewRequestBuilder(billingCycle).
		WithGranularity("DAILY").
		WithBillingDate(billingDate).
		MustBuild()
}

// NewRequestWithDefaults 创建带默认设置的请求
func NewRequestWithDefaults(billingCycle string) *DescribeInstanceBillRequest {
	return NewRequestBuilder(billingCycle).
		WithMaxResults(100).
		MustBuild()
}
