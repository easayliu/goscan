package alicloud

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/bssopenapi"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"go.uber.org/zap"
)

type Client struct {
	config      *config.AliCloudConfig
	bssClient   *bssopenapi.Client
	rateLimiter RateLimiterInterface
}

// RetryHandler retry handler
type RetryHandler struct {
	maxRetries   int
	baseDelay    time.Duration
	maxDelay     time.Duration
	enableJitter bool
}

// NewRetryHandler creates retry handler
func NewRetryHandler(maxRetries int, baseDelay, maxDelay time.Duration) *RetryHandler {
	return &RetryHandler{
		maxRetries:   maxRetries,
		baseDelay:    baseDelay,
		maxDelay:     maxDelay,
		enableJitter: true,
	}
}

// ShouldRetry determines if retry should be attempted
func (rh *RetryHandler) ShouldRetry(err error, attempt int) bool {
	if attempt >= rh.maxRetries {
		return false
	}

	return IsRetryableError(err)
}

// GetRetryDelay gets retry delay
func (rh *RetryHandler) GetRetryDelay(attempt int) time.Duration {
	// Exponential backoff
	delay := time.Duration(float64(rh.baseDelay) * math.Pow(2, float64(attempt)))

	if delay > rh.maxDelay {
		delay = rh.maxDelay
	}

	// Add random jitter to avoid thundering herd effect
	if rh.enableJitter {
		jitter := rand.Float64()*0.3 + 0.85 // 0.85-1.15x jitter
		delay = time.Duration(float64(delay) * jitter)
	}

	return delay
}

// OnRetry 重试时的回调
func (rh *RetryHandler) OnRetry(attempt int, err error) {
	logger.Warn("API call retry",
		zap.String("provider", "alicloud"),
		zap.Int("attempt", attempt),
		zap.Error(err))
}

// 实现 BillProvider 接口
var _ BillProvider = (*Client)(nil)

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
		logger.Debug("rate limiter waiting",
			zap.String("provider", "alicloud"),
			zap.Duration("wait_time", waitTime),
			zap.Int("max_qps", rl.maxQPS),
			zap.Duration("adaptive_delay", rl.adaptiveDelay))

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
		logger.Debug("rate limiter adjusted",
			zap.String("provider", "alicloud"),
			zap.String("action", "reduce_delay"),
			zap.Duration("delay", rl.adaptiveDelay))
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
	logger.Warn("rate limit triggered",
		zap.String("provider", "alicloud"),
		zap.Duration("delay", rl.adaptiveDelay),
		zap.Int("consecutive_fails", rl.consecutiveFails))
}

// GetCurrentDelay 获取当前延迟
func (rl *RateLimiter) GetCurrentDelay() time.Duration {
	return rl.adaptiveDelay
}

// SetQPS 设置QPS限制
func (rl *RateLimiter) SetQPS(qps int) {
	if qps > 0 {
		rl.maxQPS = qps
		logger.Debug("rate limiter configuration updated",
			zap.String("provider", "alicloud"),
			zap.Int("qps_limit", qps))
	}
}

// 实现 RateLimiterInterface 接口
var _ RateLimiterInterface = (*RateLimiter)(nil)

// OnError 记录其他错误
func (rl *RateLimiter) OnError(err error) {
	// 如果是限流错误，调用OnRateLimit
	if IsRateLimitError(err) {
		rl.OnRateLimit()
	} else {
		// 其他错误也适当增加延迟，但不如限流那么激进
		rl.consecutiveFails++
		if rl.consecutiveFails <= 3 {
			newDelay := time.Duration(float64(rl.adaptiveDelay) * 1.2)
			if newDelay <= rl.maxDelay {
				rl.adaptiveDelay = newDelay
				logger.Debug("rate limiter adjusted for error",
					zap.String("provider", "alicloud"),
					zap.Duration("delay", rl.adaptiveDelay))
			}
		}
	}
}

// 注意：isRateLimitError 函数已移动到 errors.go 中作为 IsRateLimitError

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
		logger.Info("client configuration optimized",
			zap.String("provider", "alicloud"),
			zap.Duration("original_delay", baseDelay),
			zap.Duration("optimized_delay", minOptimalDelay))
		baseDelay = minOptimalDelay
	}

	client := &Client{
		config:      cfg,
		bssClient:   bssClient,
		rateLimiter: NewRateLimiter(baseDelay),
	}

	logger.Info("client initialized",
		zap.String("provider", "alicloud"),
		zap.String("region", cfg.Region),
		zap.Int("qps_limit", 10))
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
	logger.Debug("API request",
		zap.String("provider", "alicloud"),
		zap.String("api", "DescribeInstanceBill"),
		zap.String("cycle", req.BillingCycle),
		zap.String("granularity", granularityInfo),
		zap.Int32("max_results", req.MaxResults))

	// 使用智能重试调用API
	response, err := c.callAPIWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to call DescribeInstanceBill API: %w", err)
	}

	// 验证响应
	if err := c.validateResponse(response); err != nil {
		return nil, fmt.Errorf("response validation failed: %w", err)
	}

	logger.Debug("API response received",
		zap.String("provider", "alicloud"),
		zap.String("api", "DescribeInstanceBill"),
		zap.Int("records", len(response.Data.Items)),
		zap.String("next_token", response.Data.NextToken))
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
			logger.Debug("API retry attempt",
				zap.String("provider", "alicloud"),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", maxRetries))
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
			logger.Error("maximum retry attempts reached",
				zap.String("provider", "alicloud"),
				zap.Int("max_retries", maxRetries),
				zap.Error(err))
			return nil, c.wrapAPIError(err)
		}

		// 检查是否可以重试
		if !isRetryableError(err) {
			logger.Error("non-retryable error encountered",
				zap.String("provider", "alicloud"),
				zap.Error(err))
			return nil, c.wrapAPIError(err)
		}

		logger.Warn("API call failed, retrying",
			zap.String("provider", "alicloud"),
			zap.Error(err))
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
		Code:     "UnknownError",
		Message:  errStr,
		Details:  "",
		HTTPCode: 0,
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
			Code:     response.Code,
			Message:  response.Message,
			Details:  response.RequestId,
			HTTPCode: 400,
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

	logger.Debug("billing cycles generated",
		zap.String("provider", "alicloud"),
		zap.Int("count", len(cycles)),
		zap.Strings("cycles", cycles))
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
		if apiErr, ok := err.(*APIError); ok && isAuthError(apiErr) {
			return fmt.Errorf("authentication failed: %w", err)
		}
		// 其他错误可能是暂时性的，记录但不阻断
		logger.Warn("connection test warning",
			zap.String("provider", "alicloud"),
			zap.Error(err))
	}

	logger.Info("connection test successful",
		zap.String("provider", "alicloud"))
	return nil
}

// isAuthError 检查是否为认证错误
func isAuthError(apiErr *APIError) bool {
	authErrorCodes := []string{
		"InvalidAccessKeyId",
		"SignatureDoesNotMatch",
		"Forbidden",
		"Unauthorized",
		"AccessDenied",
	}

	for _, code := range authErrorCodes {
		if apiErr.Code == code {
			return true
		}
	}
	return false
}

// Close 关闭客户端（预留接口）
func (c *Client) Close() error {
	// 阿里云SDK客户端无需显式关闭
	logger.Debug("client closed",
		zap.String("provider", "alicloud"))
	return nil
}
