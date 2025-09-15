package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/volcengine/volcengine-go-sdk/service/billing"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
)

type Client struct {
	config         *config.VolcEngineConfig
	billingService *billing.BILLING
	rateLimiter    *RateLimiter
}

// RateLimiter 限流器
type RateLimiter struct {
	lastRequestTime  time.Time
	baseDelay        time.Duration
	adaptiveDelay    time.Duration
	maxDelay         time.Duration
	consecutiveFails int
}

// NewRateLimiter 创建新的限流器
func NewRateLimiter(baseDelay time.Duration) *RateLimiter {
	return &RateLimiter{
		baseDelay:        baseDelay,
		adaptiveDelay:    baseDelay,
		maxDelay:         30 * time.Second, // 最大延迟30秒
		consecutiveFails: 0,
	}
}

// Wait 等待合适的时间间隔
func (rl *RateLimiter) Wait(ctx context.Context) error {
	now := time.Now()

	// 计算自上次请求以来的时间间隔
	timeSinceLastRequest := now.Sub(rl.lastRequestTime)

	// 如果还没到下次请求的时间，需要等待
	if timeSinceLastRequest < rl.adaptiveDelay {
		waitTime := rl.adaptiveDelay - timeSinceLastRequest
		log.Printf("[限流] 等待 %v 后发起下一个API请求", waitTime)

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
		log.Printf("[限流] 请求成功，减少延迟至 %v", rl.adaptiveDelay)
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
	log.Printf("[限流] 触发限流，增加延迟至 %v (连续失败: %d次)", rl.adaptiveDelay, rl.consecutiveFails)
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
				log.Printf("[限流] 请求错误，适度增加延迟至 %v", rl.adaptiveDelay)
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
		"accountflowlimitexceeded",
		"rate limit",
		"rate exceeded",
		"too many requests",
		"throttling",
		"flow control limit",
	}

	for _, indicator := range rateLimitIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	return false
}

func NewClient(cfg *config.VolcEngineConfig) *Client {
	if cfg == nil {
		cfg = config.NewVolcEngineConfig()
	}

	// 使用火山引擎官方SDK创建会话
	sdkConfig := volcengine.NewConfig().
		WithRegion(cfg.Region).
		WithCredentials(credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""))

	sess, err := session.NewSession(sdkConfig)
	if err != nil {
		panic(fmt.Errorf("failed to create volcengine session: %w", err))
	}

	svc := billing.New(sess)

	// 创建限流器，基础延迟为配置的重试延迟，如果没有配置则使用1秒
	baseDelay := time.Duration(cfg.RetryDelay) * time.Second
	if baseDelay <= 0 {
		baseDelay = 1 * time.Second
	}

	return &Client{
		config:         cfg,
		billingService: svc,
		rateLimiter:    NewRateLimiter(baseDelay),
	}
}

func (c *Client) ListBillDetail(ctx context.Context, req *ListBillDetailRequest) (*ListBillDetailResponse, error) {
	if req == nil {
		req = &ListBillDetailRequest{}
	}

	// BillPeriod智能处理：如果未提供，使用智能时间范围（上个月到昨天）
	if req.BillPeriod == "" {
		smartPeriod, dateRange := c.calculateSmartBillPeriod()
		req.BillPeriod = smartPeriod
		log.Printf("🧠 [智能账期] BillPeriod未设置，智能选择: %s (覆盖%s)", smartPeriod, dateRange)
	} else {
		// 添加调试信息，显示接收到的BillPeriod
		log.Printf("🔍 [智能账期] 接收到的BillPeriod: '%s' (长度: %d)", req.BillPeriod, len(req.BillPeriod))
		// 验证提供的BillPeriod格式和有效性
		if err := ValidateBillPeriod(req.BillPeriod); err != nil {
			// 不再强制替换，直接返回错误
			return nil, fmt.Errorf("无效的BillPeriod: %w", err)
		}
	}

	// 设置默认值并验证请求参数
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request parameters: %w", err)
	}

	// 构造官方SDK的输入参数
	input := c.buildSDKInput(req)

	// 记录请求参数（调试用）
	log.Printf("[API请求] ListBillDetail - BillPeriod: %s, Limit: %d, Offset: %d",
		req.BillPeriod, req.Limit, req.Offset)

	// 使用智能重试调用API
	output, err := c.callAPIWithRetry(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to call ListBillDetail API: %w", err)
	}

	// 检查API响应错误
	if output.Metadata.Error != nil {
		return nil, &Error{
			Code:    output.Metadata.Error.Code,
			Message: output.Metadata.Error.Message,
		}
	}

	// 转换响应格式并验证数据一致性
	response := c.convertBillDetailSDKResponse(output)
	if err := c.validateResponse(response); err != nil {
		return nil, fmt.Errorf("response validation failed: %w", err)
	}

	log.Printf("[API响应] 成功获取 %d 条记录，总计: %d", len(response.Result.List), response.Result.Total)
	return response, nil
}

// callAPIWithRetry 使用智能重试调用API
func (c *Client) callAPIWithRetry(ctx context.Context, input *billing.ListBillDetailInput) (*billing.ListBillDetailOutput, error) {
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
			log.Printf("[重试] 第 %d/%d 次尝试调用API", attempt, maxRetries)
		}

		// 调用官方SDK
		output, err := c.billingService.ListBillDetailWithContext(ctx, input)

		if err == nil {
			// 成功，通知限流器
			c.rateLimiter.OnSuccess()
			return output, nil
		}

		// 错误处理
		c.rateLimiter.OnError(err)

		// 如果是最后一次尝试，直接返回错误
		if attempt == maxRetries {
			log.Printf("[重试] 达到最大重试次数 %d，放弃重试", maxRetries)
			return nil, err
		}

		// 检查是否可以重试
		if !isRetryableError(err) {
			log.Printf("[重试] 遇到不可重试错误，放弃重试: %v", err)
			return nil, err
		}

		log.Printf("[重试] API调用失败，将进行重试: %v", err)
	}

	return nil, fmt.Errorf("unexpected end of retry loop")
}

// isRetryableError 检查错误是否可以重试
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// 可重试的错误类型
	retryableErrors := []string{
		"accountflowlimitexceeded", // 限流错误
		"rate limit",
		"rate exceeded",
		"too many requests",
		"throttling",
		"flow control limit",
		"timeout",
		"connection",
		"network",
		"internal error",
		"service unavailable",
		"temporarily unavailable",
		"try again",
		"502",
		"503",
		"504",
	}

	for _, retryable := range retryableErrors {
		if strings.Contains(errStr, retryable) {
			return true
		}
	}

	return false
}

// convertBillDetailSDKResponse 将官方SDK响应转换为内部格式（完全重构版本）
// 严格按照 SDK ListForListBillDetailOutput 字段进行1:1映射，确保数据完整性
func (c *Client) convertBillDetailSDKResponse(output *billing.ListBillDetailOutput) *ListBillDetailResponse {
	response := &ListBillDetailResponse{
		ResponseMetadata: ResponseMetadata{
			RequestID: output.Metadata.RequestId,
			Action:    output.Metadata.Action,
			Version:   output.Metadata.Version,
			Service:   output.Metadata.Service,
			Region:    output.Metadata.Region,
		},
		Result: BillDetailResult{
			Total:  getInt32Value(output.Total),
			Limit:  getInt32Value(output.Limit),
			Offset: getInt32Value(output.Offset),
		},
	}

	// 转换账单详情列表 - 按照SDK字段完整映射
	if output.List != nil {
		response.Result.List = make([]BillDetail, 0, len(output.List))

		for _, item := range output.List {
			// 直接按照SDK字段1:1映射，不做任何转换逻辑
			billDetail := BillDetail{
				// === 核心标识字段 ===
				BillDetailID: getStringValue(item.BillDetailId),
				BillID:       getStringValue(item.BillID),
				InstanceNo:   getStringValue(item.InstanceNo),

				// === 账期和时间字段 ===
				BillPeriod:       getStringValue(item.BillPeriod),
				BusiPeriod:       getStringValue(item.BusiPeriod),
				ExpenseDate:      getStringValue(item.ExpenseDate),
				ExpenseBeginTime: getStringValue(item.ExpenseBeginTime),
				ExpenseEndTime:   getStringValue(item.ExpenseEndTime),
				TradeTime:        getStringValue(item.TradeTime),

				// === 产品和服务信息 ===
				Product:     getStringValue(item.Product),
				ProductZh:   getStringValue(item.ProductZh),
				SolutionZh:  getStringValue(item.SolutionZh),
				Element:     getStringValue(item.Element),
				ElementCode: getStringValue(item.ElementCode),
				Factor:      getStringValue(item.Factor),
				FactorCode:  getStringValue(item.FactorCode),

				// === 配置信息 ===
				ConfigName:        getStringValue(item.ConfigName),
				ConfigurationCode: getStringValue(item.ConfigurationCode),
				InstanceName:      getStringValue(item.InstanceName),

				// === 地域信息 ===
				Region:        getStringValue(item.Region),
				RegionCode:    getStringValue(item.RegionCode),
				Zone:          getStringValue(item.Zone),
				ZoneCode:      getStringValue(item.ZoneCode),
				CountryRegion: getStringValue(item.CountryRegion),

				// === 用量和计费信息 ===
				Count:                getStringValue(item.Count),
				Unit:                 getStringValue(item.Unit),
				UseDuration:          getStringValue(item.UseDuration),
				UseDurationUnit:      getStringValue(item.UseDurationUnit),
				DeductionCount:       getStringValue(item.DeductionCount),
				DeductionUseDuration: getStringValue(item.DeductionUseDuration),

				// === 价格信息 ===
				Price:           getStringValue(item.Price),
				PriceUnit:       getStringValue(item.PriceUnit),
				PriceInterval:   getStringValue(item.PriceInterval),
				MarketPrice:     getStringValue(item.MarketPrice),
				Formula:         getStringValue(item.Formula),
				MeasureInterval: getStringValue(item.MeasureInterval),

				// === 金额信息（核心） ===
				OriginalBillAmount:     getStringValue(item.OriginalBillAmount),
				PreferentialBillAmount: getStringValue(item.PreferentialBillAmount),
				DiscountBillAmount:     getStringValue(item.DiscountBillAmount),
				RoundAmount:            getFloat64Value(item.RoundAmount),

				// === 实际价值和结算信息 ===
				RealValue:             getStringValue(item.RealValue),
				PretaxRealValue:       getStringValue(item.PretaxRealValue),
				SettleRealValue:       getStringValue(item.SettleRealValue),
				SettlePretaxRealValue: getStringValue(item.SettlePretaxRealValue),

				// === 应付金额信息 ===
				PayableAmount:             getStringValue(item.PayableAmount),
				PreTaxPayableAmount:       getStringValue(item.PreTaxPayableAmount),
				SettlePayableAmount:       getStringValue(item.SettlePayableAmount),
				SettlePreTaxPayableAmount: getStringValue(item.SettlePreTaxPayableAmount),

				// === 税费信息 ===
				PretaxAmount:        getStringValue(item.PretaxAmount),
				PosttaxAmount:       getStringValue(item.PosttaxAmount),
				SettlePretaxAmount:  getStringValue(item.SettlePretaxAmount),
				SettlePosttaxAmount: getStringValue(item.SettlePosttaxAmount),
				Tax:                 getStringValue(item.Tax),
				SettleTax:           getStringValue(item.SettleTax),
				TaxRate:             getStringValue(item.TaxRate),

				// === 付款信息 ===
				PaidAmount:          getStringValue(item.PaidAmount),
				UnpaidAmount:        getStringValue(item.UnpaidAmount),
				CreditCarriedAmount: getStringValue(item.CreditCarriedAmount),

				// === 优惠和抵扣信息 ===
				CouponAmount:                      getStringValue(item.CouponAmount),
				DiscountInfo:                      getStringValue(item.DiscountInfo),
				SavingPlanDeductionDiscountAmount: getStringValue(item.SavingPlanDeductionDiscountAmount),
				SavingPlanDeductionSpID:           getStringValue(item.SavingPlanDeductionSpID),
				SavingPlanOriginalAmount:          getStringValue(item.SavingPlanOriginalAmount),
				ReservationInstance:               getStringValue(item.ReservationInstance),

				// === 货币信息 ===
				Currency:           getStringValue(item.Currency),
				CurrencySettlement: getStringValue(item.CurrencySettlement),
				ExchangeRate:       getStringValue(item.ExchangeRate),

				// === 计费模式信息 ===
				BillingMode:       getStringValue(item.BillingMode),
				BillingMethodCode: getStringValue(item.BillingMethodCode),
				BillingFunction:   getStringValue(item.BillingFunction),
				BusinessMode:      getStringValue(item.BusinessMode),
				SellingMode:       getStringValue(item.SellingMode),
				SettlementType:    getStringValue(item.SettlementType),

				// === 折扣相关业务信息 ===
				DiscountBizBillingFunction:   getStringValue(item.DiscountBizBillingFunction),
				DiscountBizMeasureInterval:   getStringValue(item.DiscountBizMeasureInterval),
				DiscountBizUnitPrice:         getStringValue(item.DiscountBizUnitPrice),
				DiscountBizUnitPriceInterval: getStringValue(item.DiscountBizUnitPriceInterval),

				// === 用户和组织信息 ===
				OwnerID:            getStringValue(item.OwnerID),
				OwnerUserName:      getStringValue(item.OwnerUserName),
				OwnerCustomerName:  getStringValue(item.OwnerCustomerName),
				PayerID:            getStringValue(item.PayerID),
				PayerUserName:      getStringValue(item.PayerUserName),
				PayerCustomerName:  getStringValue(item.PayerCustomerName),
				SellerID:           getStringValue(item.SellerID),
				SellerUserName:     getStringValue(item.SellerUserName),
				SellerCustomerName: getStringValue(item.SellerCustomerName),

				// === 项目和分类信息 ===
				Project:            getStringValue(item.Project),
				ProjectDisplayName: getStringValue(item.ProjectDisplayName),
				BillCategory:       getStringValue(item.BillCategory),
				SubjectName:        getStringValue(item.SubjectName),
				Tag:                getStringValue(item.Tag), // 保持JSON字符串格式

				// === 其他业务信息 ===
				MainContractNumber: getStringValue(item.MainContractNumber),
				OriginalOrderNo:    getStringValue(item.OriginalOrderNo),
				EffectiveFactor:    getStringValue(item.EffectiveFactor),
				ExpandField:        getStringValue(item.ExpandField),
			}

			response.Result.List = append(response.Result.List, billDetail)
		}
	}

	log.Printf("[SDK转换] 成功转换 %d 条账单记录，总计: %d",
		len(response.Result.List), response.Result.Total)

	return response
}

// GetValidBillPeriods 获取推荐的账期选项（当月和上月）
// 注意：这仅是推荐选项，API实际支持任何有效的历史月份
// Deprecated: 建议直接使用具体的YYYY-MM格式，不受此列表限制
func GetValidBillPeriods() []string {
	now := time.Now()
	currentMonth := now.Format("2006-01")
	lastMonth := now.AddDate(0, -1, 0).Format("2006-01")

	return []string{currentMonth, lastMonth}
}

// ValidateBillPeriod 验证BillPeriod格式和有效性（按照火山引擎API文档）
func ValidateBillPeriod(billPeriod string) error {
	if billPeriod == "" {
		return fmt.Errorf("BillPeriod不能为空")
	}

	// 检查格式（YYYY-MM）
	parsedTime, err := time.Parse("2006-01", billPeriod)
	if err != nil {
		return fmt.Errorf("BillPeriod格式错误，应为YYYY-MM格式，如2024-08")
	}

	// 基本合理性检查：不能是未来月份（但允许当前月份）
	now := time.Now()
	// 获取下个月的第一天作为限制
	nextMonth := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, now.Location())
	billMonth := time.Date(parsedTime.Year(), parsedTime.Month(), 1, 0, 0, 0, 0, parsedTime.Location())
	
	// 只有当账期在下个月或更晚时才报错（即允许当前月份）
	if billMonth.After(nextMonth) || billMonth.Equal(nextMonth) {
		return fmt.Errorf("BillPeriod不能是未来月份，当前最大可查询月份: %s", now.Format("2006-01"))
	}
	
	// 历史数据合理性检查：不能太久远（如2000年之前）
	earliestAllowed := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC) // 火山引擎大约2018年开始服务
	if billMonth.Before(earliestAllowed) {
		return fmt.Errorf("BillPeriod不能早于2018-01（火山引擎服务开始时间）")
	}

	return nil
}

func getStringValue(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return *ptr
}

func getInt32Value(ptr *int32) int32 {
	if ptr == nil {
		return 0
	}
	return *ptr
}

func getFloat64Value(ptr *float64) float64 {
	if ptr == nil {
		return 0.0
	}
	return *ptr
}

// parseFloat64 安全解析字符串为float64
func parseFloat64(str string) float64 {
	if str == "" {
		return 0.0
	}
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0.0
	}
	return val
}

// buildSDKInput 构造SDK输入参数（完全重构版本）
// 严格按照 github.com/volcengine/volcengine-go-sdk/service/billing.ListBillDetailInput 结构映射
func (c *Client) buildSDKInput(req *ListBillDetailRequest) *billing.ListBillDetailInput {
	input := &billing.ListBillDetailInput{}

	// === 设置必需参数 ===
	input.BillPeriod = &req.BillPeriod
	input.Limit = &req.Limit

	// === 设置可选参数（完全按照SDK字段映射）===

	// Offset - 默认为0
	if req.Offset > 0 {
		input.Offset = &req.Offset
	}

	// NeedRecordNum - 是否需要总记录数
	if req.NeedRecordNum > 0 {
		input.NeedRecordNum = &req.NeedRecordNum
	}

	// IgnoreZero - 是否忽略零元账单
	if req.IgnoreZero > 0 {
		input.IgnoreZero = &req.IgnoreZero
	}

	// GroupPeriod - 分组周期
	if req.GroupPeriod > 0 {
		input.GroupPeriod = &req.GroupPeriod
	}

	// GroupTerm - 分组条件
	if req.GroupTerm > 0 {
		input.GroupTerm = &req.GroupTerm
	}

	// ExpenseDate - 费用日期
	if req.ExpenseDate != "" {
		input.ExpenseDate = &req.ExpenseDate
	}

	// InstanceNo - 实例编号
	if req.InstanceNo != "" {
		input.InstanceNo = &req.InstanceNo
	}

	// BillCategory - 账单分类列表
	if len(req.BillCategory) > 0 {
		input.BillCategory = make([]*string, len(req.BillCategory))
		for i, category := range req.BillCategory {
			input.BillCategory[i] = &category
		}
	}

	// BillingMode - 计费模式列表
	if len(req.BillingMode) > 0 {
		input.BillingMode = make([]*string, len(req.BillingMode))
		for i, mode := range req.BillingMode {
			input.BillingMode[i] = &mode
		}
	}

	// Product - 产品名称列表
	if len(req.Product) > 0 {
		input.Product = make([]*string, len(req.Product))
		for i, product := range req.Product {
			input.Product[i] = &product
		}
	}

	// OwnerID - 所有者ID列表
	if len(req.OwnerID) > 0 {
		input.OwnerID = make([]*int64, len(req.OwnerID))
		for i, ownerID := range req.OwnerID {
			input.OwnerID[i] = &ownerID
		}
	}

	// PayerID - 付款方ID列表
	if len(req.PayerID) > 0 {
		input.PayerID = make([]*int64, len(req.PayerID))
		for i, payerID := range req.PayerID {
			input.PayerID[i] = &payerID
		}
	}

	log.Printf("[SDK映射] 成功构造输入参数: BillPeriod=%s, Limit=%d, 可选参数数量=%d",
		req.BillPeriod, req.Limit, c.countOptionalParams(req))

	return input
}

// countOptionalParams 计算设置的可选参数数量（用于调试）
func (c *Client) countOptionalParams(req *ListBillDetailRequest) int {
	count := 0
	if req.Offset > 0 {
		count++
	}
	if req.NeedRecordNum > 0 {
		count++
	}
	if req.IgnoreZero > 0 {
		count++
	}
	if req.GroupPeriod > 0 {
		count++
	}
	if req.GroupTerm > 0 {
		count++
	}
	if req.ExpenseDate != "" {
		count++
	}
	if req.InstanceNo != "" {
		count++
	}
	if len(req.BillCategory) > 0 {
		count++
	}
	if len(req.BillingMode) > 0 {
		count++
	}
	if len(req.Product) > 0 {
		count++
	}
	if len(req.OwnerID) > 0 {
		count++
	}
	if len(req.PayerID) > 0 {
		count++
	}
	return count
}

// validateResponse 验证API响应的数据一致性
func (c *Client) validateResponse(response *ListBillDetailResponse) error {
	if response == nil {
		return fmt.Errorf("response is nil")
	}

	// 验证基本响应结构
	if response.ResponseMetadata.RequestID == "" {
		return fmt.Errorf("missing RequestID in response metadata")
	}

	// 验证分页数据一致性
	actualCount := int32(len(response.Result.List))
	if actualCount > response.Result.Limit && response.Result.Limit > 0 {
		return fmt.Errorf("response count (%d) exceeds limit (%d)", actualCount, response.Result.Limit)
	}

	// 验证账单详情数据
	for i, bill := range response.Result.List {
		if bill.GetID() == "" {
			log.Printf("[响应验证] 警告: 第 %d 条记录缺少ID字段", i+1)
		}
		if bill.ExpenseDate == "" {
			log.Printf("[响应验证] 警告: 第 %d 条记录缺少ExpenseDate字段", i+1)
		}
		if bill.BillPeriod == "" {
			log.Printf("[响应验证] 警告: 第 %d 条记录缺少BillPeriod字段", i+1)
		}
	}

	log.Printf("[响应验证] 数据验证完成，共验证 %d 条记录", actualCount)
	return nil
}

// enhancedErrorHandling 增强的错误处理，提供更详细的错误信息
func (c *Client) enhancedErrorHandling(err error, context string) error {
	if err == nil {
		return nil
	}

	// 包装错误信息，提供更多上下文
	wrappedErr := fmt.Errorf("%s: %w", context, err)

	// 记录详细的错误信息
	if isRateLimitError(err) {
		log.Printf("[错误处理] 限流错误 - %s: %v", context, err)
	} else if isRetryableError(err) {
		log.Printf("[错误处理] 可重试错误 - %s: %v", context, err)
	} else {
		log.Printf("[错误处理] 不可重试错误 - %s: %v", context, err)
	}

	return wrappedErr
}

// formatRequestContext 格式化请求上下文信息
func formatRequestContext(req *ListBillDetailRequest) string {
	if req == nil {
		return "request=nil"
	}

	return fmt.Sprintf("BillPeriod=%s,Limit=%d,Offset=%d,Product=%s",
		req.BillPeriod, req.Limit, req.Offset, req.Product)
}

// CalculateSmartBillPeriod 计算智能账期（上个月到昨天的最优账期选择）- 导出版本
func (c *Client) CalculateSmartBillPeriod() (string, string) {
	return c.calculateSmartBillPeriod()
}

// calculateSmartBillPeriod 计算智能账期（上个月到昨天的最优账期选择）
func (c *Client) calculateSmartBillPeriod() (string, string) {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	
	// 计算上个月第一天
	firstDayOfThisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	firstDayOfLastMonth := firstDayOfThisMonth.AddDate(0, -1, 0)
	
	startDate := firstDayOfLastMonth  // 上个月第一天
	endDate := yesterday              // 昨天
	
	// 分析时间跨度
	lastMonthPeriod := startDate.Format("2006-01")
	currentMonthPeriod := now.Format("2006-01")
	
	// 计算各账期覆盖的天数
	lastMonthDays := 0
	currentMonthDays := 0
	
	// 上个月的天数：从上个月第一天到上个月最后一天（或昨天，取较小者）
	lastMonthEnd := firstDayOfThisMonth.AddDate(0, 0, -1) // 上个月最后一天
	if endDate.Before(lastMonthEnd) {
		lastMonthDays = int(endDate.Sub(startDate).Hours()/24) + 1
	} else {
		lastMonthDays = int(lastMonthEnd.Sub(startDate).Hours()/24) + 1
	}
	
	// 当前月的天数：从当前月第一天到昨天
	if endDate.After(firstDayOfThisMonth) || endDate.Equal(firstDayOfThisMonth) {
		currentMonthDays = int(endDate.Sub(firstDayOfThisMonth).Hours()/24) + 1
	}
	
	// 智能选择策略
	var selectedPeriod string
	var reason string
	
	if lastMonthDays > currentMonthDays {
		// 上个月天数更多，选择上个月
		selectedPeriod = lastMonthPeriod
		reason = fmt.Sprintf("上个月占主导(%d天 vs %d天)", lastMonthDays, currentMonthDays)
	} else if currentMonthDays > lastMonthDays {
		// 当前月天数更多，选择当前月
		selectedPeriod = currentMonthPeriod  
		reason = fmt.Sprintf("当前月占主导(%d天 vs %d天)", currentMonthDays, lastMonthDays)
	} else {
		// 天数相等或其他情况，默认选择上个月（数据更稳定）
		selectedPeriod = lastMonthPeriod
		reason = "默认选择上个月（数据更稳定）"
	}
	
	dateRange := fmt.Sprintf("%s至%s, %s", 
		startDate.Format("2006-01-02"), 
		endDate.Format("2006-01-02"),
		reason)
	
	return selectedPeriod, dateRange
}
