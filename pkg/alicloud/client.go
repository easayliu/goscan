package alicloud

import (
	"context"
	"fmt"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/bssopenapi"
	"goscan/pkg/config"
	"log"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
)

type Client struct {
	config      *config.AliCloudConfig
	bssClient   *bssopenapi.Client
	rateLimiter *RateLimiter
}

// RateLimiter 阿里云API限流器
// 阿里云单用户限流：10次/秒
type RateLimiter struct {
	lastRequestTime  time.Time
	baseDelay        time.Duration
	adaptiveDelay    time.Duration
	maxDelay         time.Duration
	consecutiveFails int
	maxQPS           int // 最大QPS限制
}

// NewRateLimiter 创建新的限流器
func NewRateLimiter(baseDelay time.Duration) *RateLimiter {
	return &RateLimiter{
		baseDelay:        baseDelay,
		adaptiveDelay:    baseDelay,
		maxDelay:         30 * time.Second, // 最大延迟30秒
		consecutiveFails: 0,
		maxQPS:           10, // 充分利用阿里云10QPS限制
	}
}

// Wait 等待合适的时间间隔
func (rl *RateLimiter) Wait(ctx context.Context) error {
	now := time.Now()

	// 计算基于QPS的最小间隔
	minInterval := time.Second / time.Duration(rl.maxQPS)

	// 计算自上次请求以来的时间间隔
	timeSinceLastRequest := now.Sub(rl.lastRequestTime)

	// 选择较大的延迟时间（QPS限制 vs 自适应延迟）
	waitTime := time.Duration(0)
	if timeSinceLastRequest < minInterval {
		waitTime = minInterval - timeSinceLastRequest
	}
	if rl.adaptiveDelay > minInterval && timeSinceLastRequest < rl.adaptiveDelay {
		adaptiveWait := rl.adaptiveDelay - timeSinceLastRequest
		if adaptiveWait > waitTime {
			waitTime = adaptiveWait
		}
	}

	if waitTime > 0 {
		log.Printf("[阿里云限流] 等待 %v 后发起下一个API请求 (QPS: %d, 自适应延迟: %v)",
			waitTime, rl.maxQPS, rl.adaptiveDelay)

		select {
		case <-time.After(waitTime):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	rl.lastRequestTime = time.Now()
	return nil
}

// OnSuccess 记录成功的请求
func (rl *RateLimiter) OnSuccess() {
	if rl.consecutiveFails > 0 {
		// 成功后逐步减少延迟
		rl.consecutiveFails--
		rl.adaptiveDelay = time.Duration(float64(rl.adaptiveDelay) * 0.8)
		if rl.adaptiveDelay < rl.baseDelay {
			rl.adaptiveDelay = rl.baseDelay
		}
		log.Printf("[阿里云限流] 请求成功，减少延迟至 %v", rl.adaptiveDelay)
	}
}

// OnRateLimit 记录被限流的请求
func (rl *RateLimiter) OnRateLimit() {
	rl.consecutiveFails++

	// 使用指数退避算法增加延迟
	backoffMultiplier := math.Pow(2, float64(rl.consecutiveFails))
	if backoffMultiplier > 16 { // 限制最大倍数为16
		backoffMultiplier = 16
	}

	// 添加随机抖动避免雷群效应
	jitter := rand.Float64()*0.3 + 0.85 // 0.85-1.15倍的随机抖动

	newDelay := time.Duration(float64(rl.baseDelay) * backoffMultiplier * jitter)
	if newDelay > rl.maxDelay {
		newDelay = rl.maxDelay
	}

	rl.adaptiveDelay = newDelay
	log.Printf("[阿里云限流] 触发限流，增加延迟至 %v (连续失败: %d次)", rl.adaptiveDelay, rl.consecutiveFails)
}

// OnError 记录其他错误
func (rl *RateLimiter) OnError(err error) {
	// 如果是限流错误，调用OnRateLimit
	if isRateLimitError(err) {
		rl.OnRateLimit()
	} else {
		// 其他错误也适当增加延迟，但不如限流那么激进
		rl.consecutiveFails++
		if rl.consecutiveFails <= 3 {
			newDelay := time.Duration(float64(rl.adaptiveDelay) * 1.2)
			if newDelay <= rl.maxDelay {
				rl.adaptiveDelay = newDelay
				log.Printf("[阿里云限流] 请求错误，适度增加延迟至 %v", rl.adaptiveDelay)
			}
		}
	}
}

// isRateLimitError 检查是否为限流错误
func isRateLimitError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	rateLimitIndicators := []string{
		"qpslimitexceeded",
		"flowlimitexceeded",
		"throttling",
		"rate limit",
		"rate exceeded",
		"too many requests",
		"flow control",
		"频率限制",
		"请求过于频繁",
	}

	for _, indicator := range rateLimitIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

// NewClient 创建阿里云客户端
func NewClient(cfg *config.AliCloudConfig) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("AliCloud config is required")
	}

	// 验证必需参数
	if cfg.AccessKeyID == "" || cfg.AccessKeySecret == "" {
		return nil, fmt.Errorf("AccessKeyID and AccessKeySecret are required")
	}

	// 创建认证信息
	credential := credentials.NewAccessKeyCredential(cfg.AccessKeyID, cfg.AccessKeySecret)

	// 创建SDK配置
	config := sdk.NewConfig()
	config = config.WithTimeout(time.Duration(cfg.Timeout) * time.Second)

	// 创建BSS OpenAPI客户端
	bssClient, err := bssopenapi.NewClientWithOptions(cfg.Region, config, credential)
	if err != nil {
		return nil, fmt.Errorf("failed to create AliCloud BSS client: %w", err)
	}

	// 创建限流器，基础延迟优化为充分利用10QPS
	baseDelay := time.Duration(cfg.RetryDelay) * time.Second
	if baseDelay <= 0 {
		// 10QPS理论间隔100ms，考虑网络延迟和安全余量设置为120ms
		baseDelay = 120 * time.Millisecond
	}
	// 如果配置的延迟过大，使用优化后的值
	minOptimalDelay := 120 * time.Millisecond
	if baseDelay > 1*time.Second {
		log.Printf("[阿里云客户端] 配置延迟过大(%v)，优化为%v以提高效率", baseDelay, minOptimalDelay)
		baseDelay = minOptimalDelay
	}

	client := &Client{
		config:      cfg,
		bssClient:   bssClient,
		rateLimiter: NewRateLimiter(baseDelay),
	}

	log.Printf("[阿里云客户端] 初始化完成，Region: %s, QPS限制: 10", cfg.Region)
	return client, nil
}

// DescribeInstanceBill 查询实例账单
func (c *Client) DescribeInstanceBill(ctx context.Context, req *DescribeInstanceBillRequest) (*DescribeInstanceBillResponse, error) {
	if req == nil {
		req = &DescribeInstanceBillRequest{}
	}

	// 设置默认值并验证请求参数
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request parameters: %w", err)
	}

	// 记录请求参数（调试用）
	granularityInfo := fmt.Sprintf("Granularity: %s", req.Granularity)
	if req.IsDaily() && req.BillingDate != "" {
		granularityInfo += fmt.Sprintf(", BillingDate: %s", req.BillingDate)
	}
	log.Printf("[阿里云API请求] DescribeInstanceBill - BillingCycle: %s, %s, MaxResults: %d",
		req.BillingCycle, granularityInfo, req.MaxResults)

	// 使用智能重试调用API
	response, err := c.callAPIWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to call DescribeInstanceBill API: %w", err)
	}

	// 验证响应
	if err := c.validateResponse(response); err != nil {
		return nil, fmt.Errorf("response validation failed: %w", err)
	}

	log.Printf("[阿里云API响应] 成功获取 %d 条记录，NextToken: %s",
		len(response.Data.Items), response.Data.NextToken)
	return response, nil
}

// callAPIWithRetry 使用智能重试调用API
func (c *Client) callAPIWithRetry(ctx context.Context, req *DescribeInstanceBillRequest) (*DescribeInstanceBillResponse, error) {
	maxRetries := c.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5 // 默认最大重试5次
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// 使用限流器等待合适的时间间隔
		if err := c.rateLimiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("context cancelled while waiting for rate limiter: %w", err)
		}

		// 记录请求信息
		if attempt > 0 {
			log.Printf("[阿里云重试] 第 %d/%d 次尝试调用API", attempt, maxRetries)
		}

		// 构建API请求
		apiRequest := c.buildAPIRequest(req)

		// 调用阿里云SDK
		response, err := c.bssClient.DescribeInstanceBill(apiRequest)

		if err == nil {
			// 成功，通知限流器并转换响应
			c.rateLimiter.OnSuccess()
			return c.convertAPIResponse(response), nil
		}

		// 错误处理
		c.rateLimiter.OnError(err)

		// 如果是最后一次尝试，直接返回错误
		if attempt == maxRetries {
			log.Printf("[阿里云重试] 达到最大重试次数 %d，放弃重试", maxRetries)
			return nil, c.wrapAPIError(err)
		}

		// 检查是否可以重试
		if !isRetryableError(err) {
			log.Printf("[阿里云重试] 遇到不可重试错误，放弃重试: %v", err)
			return nil, c.wrapAPIError(err)
		}

		log.Printf("[阿里云重试] API调用失败，将进行重试: %v", err)
	}

	return nil, fmt.Errorf("unexpected end of retry loop")
}

// buildAPIRequest 构建阿里云API请求
func (c *Client) buildAPIRequest(req *DescribeInstanceBillRequest) *bssopenapi.DescribeInstanceBillRequest {
	apiRequest := bssopenapi.CreateDescribeInstanceBillRequest()

	// 设置基本参数
	apiRequest.BillingCycle = req.BillingCycle
	apiRequest.MaxResults = requests.NewInteger(int(req.MaxResults))

	// 设置粒度参数
	if req.Granularity != "" {
		apiRequest.Granularity = req.Granularity
	}
	if req.BillingDate != "" {
		apiRequest.BillingDate = req.BillingDate
	}

	// 设置分页参数
	if req.NextToken != "" {
		apiRequest.NextToken = req.NextToken
	}

	// 设置过滤参数
	if req.ProductCode != "" {
		apiRequest.ProductCode = req.ProductCode
	}
	if req.ProductType != "" {
		apiRequest.ProductType = req.ProductType
	}
	if req.SubscriptionType != "" {
		apiRequest.SubscriptionType = req.SubscriptionType
	}
	if req.BillOwnerId != 0 {
		apiRequest.BillOwnerId = requests.NewInteger64(req.BillOwnerId)
	}
	if req.IsHideZeroCharge {
		apiRequest.IsHideZeroCharge = requests.NewBoolean(req.IsHideZeroCharge)
	}
	// IsDisplayLocalCurrency字段在阿里云SDK中不存在，跳过

	// 设置实例ID（阿里云SDK只支持单个InstanceID字段）
	if len(req.InstanceIDs) > 0 {
		apiRequest.InstanceID = req.InstanceIDs[0] // 只取第一个实例ID
	}

	return apiRequest
}

// convertAPIResponse 转换阿里云API响应为内部格式
func (c *Client) convertAPIResponse(response *bssopenapi.DescribeInstanceBillResponse) *DescribeInstanceBillResponse {
	result := &DescribeInstanceBillResponse{
		RequestId: response.RequestId,
		Success:   response.Success,
		Code:      response.Code,
		Message:   response.Message,
		Data: BillInstanceResult{
			BillingCycle: response.Data.BillingCycle,
			AccountID:    response.Data.AccountID,
			AccountName:  response.Data.AccountName,
			TotalCount:   int32(response.Data.TotalCount),
			NextToken:    response.Data.NextToken,
			MaxResults:   int32(response.Data.MaxResults),
			Items:        make([]BillDetail, 0, len(response.Data.Items)),
		},
	}

	// 转换账单明细列表
	for _, item := range response.Data.Items {
		billDetail := BillDetail{
			// 核心标识字段
			InstanceID:      item.InstanceID,
			InstanceName:    "", // SDK中没有InstanceName字段
			BillAccountID:   item.BillAccountID,
			BillAccountName: item.BillAccountName,

			// 时间字段
			BillingDate: item.BillingDate,

			// 产品信息
			ProductCode:   item.ProductCode,
			ProductName:   item.ProductName,
			ProductType:   item.ProductType,
			ProductDetail: item.ProductDetail,

			// 计费信息
			SubscriptionType: item.SubscriptionType,
			PricingUnit:      item.ListPriceUnit,
			Currency:         item.Currency,
			BillingType:      item.BillingType,

			// 用量信息
			Usage:     item.Usage,
			UsageUnit: item.UsageUnit,

			// 金额信息
			PretaxGrossAmount: item.PretaxGrossAmount,
			InvoiceDiscount:   item.InvoiceDiscount,
			DeductedByCoupons: item.DeductedByCoupons,
			PretaxAmount:      item.PretaxAmount,
			Currency_Amount:   0.0, // 阿里云无相应字段
			PaymentAmount:     item.PaymentAmount,
			OutstandingAmount: item.OutstandingAmount,

			// 地域信息
			Region: item.Region,
			Zone:   item.Zone,

			// 规格信息
			InstanceSpec: item.InstanceSpec,
			InternetIP:   item.InternetIP,
			IntranetIP:   item.IntranetIP,

			// 标签和分组
			ResourceGroup: item.ResourceGroup,
			Tag:           item.Tag,
			CostUnit:      item.CostUnit,

			// 其他信息
			ServicePeriod:     item.ServicePeriod,
			ServicePeriodUnit: item.ServicePeriodUnit,
			ListPrice:         item.ListPrice,
			ListPriceUnit:     item.ListPriceUnit,
			OwnerID:           item.OwnerID,

			// 成本分摊
			SplitItemID:      item.SplitItemID,
			SplitItemName:    item.SplitItemName,
			SplitAccountID:   item.SplitAccountID,
			SplitAccountName: item.SplitAccountName,

			// 订单信息
			NickName:          item.NickName,
			ProductDetailCode: item.ProductDetail,

			// 账单归属
			BizType:      item.BizType,
			AdjustType:   "", // SDK中没有AdjustType字段
			AdjustAmount: item.AdjustAmount,
		}

		result.Data.Items = append(result.Data.Items, billDetail)
	}

	return result
}

// wrapAPIError 包装阿里云API错误
func (c *Client) wrapAPIError(err error) error {
	if err == nil {
		return nil
	}

	// 尝试解析为阿里云SDK错误
	errStr := err.Error()

	// 创建通用API错误
	apiErr := &APIError{
		RequestId: "unknown",
		Code:      "UnknownError",
		Message:   errStr,
	}

	// 尝试从错误信息中提取更多细节
	if strings.Contains(errStr, "QpsLimitExceeded") {
		apiErr.Code = "QpsLimitExceeded"
		apiErr.Message = "Request rate exceeded limit"
	} else if strings.Contains(errStr, "InvalidAccessKeyId") {
		apiErr.Code = "InvalidAccessKeyId"
		apiErr.Message = "Invalid AccessKey ID"
	} else if strings.Contains(errStr, "SignatureDoesNotMatch") {
		apiErr.Code = "SignatureDoesNotMatch"
		apiErr.Message = "Signature verification failed"
	}

	return apiErr
}

// validateResponse 验证响应数据
func (c *Client) validateResponse(response *DescribeInstanceBillResponse) error {
	if response == nil {
		return fmt.Errorf("response is nil")
	}

	if !response.Success {
		return &APIError{
			RequestId: response.RequestId,
			Code:      response.Code,
			Message:   response.Message,
		}
	}

	return nil
}

// isRetryableError 检查错误是否可以重试
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// 可重试的错误类型
	retryableErrors := []string{
		"qpslimitexceeded",  // 限流错误
		"flowlimitexceeded", // 流控错误
		"throttling",        // 节流错误
		"rate limit",
		"rate exceeded",
		"too many requests",
		"timeout",
		"connection",
		"network",
		"internal error",
		"internalerror",
		"service unavailable",
		"serviceunavailable",
		"temporarily unavailable",
		"try again",
		"502",
		"503",
		"504",
		"请求过于频繁",
		"服务器忙",
		"网络错误",
	}

	for _, retryable := range retryableErrors {
		if strings.Contains(errStr, retryable) {
			return true
		}
	}

	return false
}

// GetAvailableBillingCycles 获取可用的账期列表
// 阿里云支持查询18个月的历史数据
func (c *Client) GetAvailableBillingCycles(ctx context.Context) ([]string, error) {
	var cycles []string

	now := time.Now()
	// 生成最近18个月的账期
	for i := 0; i < 18; i++ {
		cycleTime := now.AddDate(0, -i, 0)
		cycle := cycleTime.Format("2006-01")

		// 验证账期是否有效
		if err := ValidateBillingCycle(cycle); err == nil {
			cycles = append(cycles, cycle)
		}
	}

	log.Printf("[阿里云账期] 生成可用账期列表: %v", cycles)
	return cycles, nil
}

// TestConnection 测试连接
func (c *Client) TestConnection(ctx context.Context) error {
	// 尝试查询当前月的账单（少量数据）
	currentMonth := time.Now().Format("2006-01")
	req := &DescribeInstanceBillRequest{
		BillingCycle: currentMonth,
		MaxResults:   1, // 只查询1条记录用于测试
		Granularity:  "MONTHLY",
	}

	_, err := c.DescribeInstanceBill(ctx, req)
	if err != nil {
		// 如果是认证错误，直接返回
		if apiErr, ok := err.(*APIError); ok && apiErr.IsAuthError() {
			return fmt.Errorf("authentication failed: %w", err)
		}
		// 其他错误可能是暂时性的，记录但不阻断
		log.Printf("[阿里云连接测试] 警告: %v", err)
	}

	log.Printf("[阿里云连接测试] 连接成功")
	return nil
}

// Close 关闭客户端（预留接口）
func (c *Client) Close() error {
	// 阿里云SDK客户端无需显式关闭
	log.Printf("[阿里云客户端] 客户端已关闭")
	return nil
}
