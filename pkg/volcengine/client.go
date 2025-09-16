package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"log"
	"time"

	"github.com/volcengine/volcengine-go-sdk/service/billing"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
	"golang.org/x/time/rate"
)

// Client 火山引擎客户端
type Client struct {
	config         *config.VolcEngineConfig
	billingService *billing.BILLING
	rateLimiter    *rate.Limiter
	retryPolicy    RetryPolicy
}

// NewClient 创建火山引擎客户端
func NewClient(cfg *config.VolcEngineConfig) (*Client, error) {
	if cfg == nil {
		cfg = config.NewVolcEngineConfig()
	}

	// 验证配置
	if err := cfg.Validate(); err != nil {
		return nil, WrapError(err, "invalid config")
	}

	// 创建 SDK 会话
	sdkConfig := volcengine.NewConfig().
		WithRegion(cfg.Region).
		WithCredentials(credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""))

	sess, err := session.NewSession(sdkConfig)
	if err != nil {
		return nil, WrapError(err, "create session failed")
	}

	// 创建限流器
	qps := cfg.RateLimit
	if qps <= 0 {
		qps = 10 // 默认 QPS
	}
	rateLimiter := rate.NewLimiter(rate.Limit(qps), 1)

	return &Client{
		config:         cfg,
		billingService: billing.New(sess),
		rateLimiter:    rateLimiter,
		retryPolicy:    DefaultExponentialBackoff(),
	}, nil
}

// ListBillDetail 获取账单明细
func (c *Client) ListBillDetail(ctx context.Context, req *ListBillDetailRequest) (*ListBillDetailResponse, error) {
	if req == nil {
		req = &ListBillDetailRequest{}
	}

	// 处理账期
	if req.BillPeriod == "" {
		req.BillPeriod, _ = c.CalculateSmartPeriod()
		if c.config.EnableDebug {
			log.Printf("[DEBUG] 使用智能账期: %s", req.BillPeriod)
		}
	}

	// 验证账期
	if err := c.ValidatePeriod(req.BillPeriod); err != nil {
		return nil, WrapError(err, "validate period failed")
	}

	// 设置默认值并验证
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		return nil, WrapError(err, "validate request failed")
	}

	// 构建 SDK 输入
	input := c.buildSDKInput(req)

	if c.config.EnableDebug {
		log.Printf("[DEBUG] API请求 - Period: %s, Limit: %d, Offset: %d",
			req.BillPeriod, req.Limit, req.Offset)
	}

	// 调用 API
	output, err := c.callAPIWithRetry(ctx, input)
	if err != nil {
		return nil, WrapError(err, "api call failed")
	}

	// 检查响应错误
	if output.Metadata.Error != nil {
		return nil, NewAPIError(
			output.Metadata.Error.Code,
			output.Metadata.Error.Message,
		)
	}

	// 转换响应
	response := c.convertSDKResponse(output)

	if c.config.EnableDebug {
		log.Printf("[DEBUG] 获取 %d 条记录，总计: %d",
			len(response.Result.List), response.Result.Total)
	}

	return response, nil
}

// callAPIWithRetry 使用重试策略调用 API
func (c *Client) callAPIWithRetry(ctx context.Context, input *billing.ListBillDetailInput) (*billing.ListBillDetailOutput, error) {
	var lastErr error

	for attempt := 0; attempt < c.retryPolicy.MaxAttempts(); attempt++ {
		// 限流控制
		if err := c.rateLimiter.Wait(ctx); err != nil {
			return nil, WrapError(err, "rate limit wait failed")
		}

		if attempt > 0 && c.config.EnableDebug {
			log.Printf("[DEBUG] 重试 %d/%d", attempt, c.retryPolicy.MaxAttempts())
		}

		// 调用 API
		output, err := c.billingService.ListBillDetailWithContext(ctx, input)
		if err == nil {
			return output, nil
		}

		lastErr = err

		// 检查是否应该重试
		if !c.retryPolicy.ShouldRetry(err, attempt) {
			break
		}

		// 等待重试
		delay := c.retryPolicy.GetDelay(attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, WrapError(lastErr, "all retry attempts failed")
}

// ValidatePeriod 验证账期格式
func (c *Client) ValidatePeriod(billPeriod string) error {
	if billPeriod == "" {
		return ErrEmptyBillPeriod
	}

	// 检查格式（YYYY-MM）
	parsedTime, err := time.Parse("2006-01", billPeriod)
	if err != nil {
		return ErrInvalidBillPeriod
	}

	// 检查是否为未来月份
	now := time.Now()
	nextMonth := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, now.Location())
	billMonth := time.Date(parsedTime.Year(), parsedTime.Month(), 1, 0, 0, 0, 0, parsedTime.Location())
	
	if billMonth.After(nextMonth) || billMonth.Equal(nextMonth) {
		return ErrFutureBillPeriod
	}
	
	// 检查是否太过久远
	earliestAllowed := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	if billMonth.Before(earliestAllowed) {
		return ErrTooOldBillPeriod
	}

	return nil
}

// CalculateSmartPeriod 计算智能账期
func (c *Client) CalculateSmartPeriod() (string, string) {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	
	// 计算上个月第一天
	firstDayOfThisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	firstDayOfLastMonth := firstDayOfThisMonth.AddDate(0, -1, 0)
	
	startDate := firstDayOfLastMonth
	endDate := yesterday
	
	// 分析时间跨度
	lastMonthPeriod := startDate.Format("2006-01")
	currentMonthPeriod := now.Format("2006-01")
	
	// 计算各账期覆盖的天数
	lastMonthDays := 0
	currentMonthDays := 0
	
	lastMonthEnd := firstDayOfThisMonth.AddDate(0, 0, -1)
	if endDate.Before(lastMonthEnd) {
		lastMonthDays = int(endDate.Sub(startDate).Hours()/24) + 1
	} else {
		lastMonthDays = int(lastMonthEnd.Sub(startDate).Hours()/24) + 1
	}
	
	if endDate.After(firstDayOfThisMonth) || endDate.Equal(firstDayOfThisMonth) {
		currentMonthDays = int(endDate.Sub(firstDayOfThisMonth).Hours()/24) + 1
	}
	
	var selectedPeriod string
	var reason string
	
	if lastMonthDays > currentMonthDays {
		selectedPeriod = lastMonthPeriod
		reason = fmt.Sprintf("上个月占主导(%d天 vs %d天)", lastMonthDays, currentMonthDays)
	} else if currentMonthDays > lastMonthDays {
		selectedPeriod = currentMonthPeriod  
		reason = fmt.Sprintf("当前月占主导(%d天 vs %d天)", currentMonthDays, lastMonthDays)
	} else {
		selectedPeriod = lastMonthPeriod
		reason = "默认选择上个月（数据更稳定）"
	}
	
	dateRange := fmt.Sprintf("%s至%s, %s", 
		startDate.Format("2006-01-02"), 
		endDate.Format("2006-01-02"),
		reason)
	
	return selectedPeriod, dateRange
}

// convertSDKResponse 转换 SDK 响应为内部格式
func (c *Client) convertSDKResponse(output *billing.ListBillDetailOutput) *ListBillDetailResponse {
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

	// 转换账单详情列表
	if output.List != nil {
		response.Result.List = make([]BillDetail, 0, len(output.List))
		for _, item := range output.List {
			response.Result.List = append(response.Result.List, c.convertBillDetail(item))
		}
	}

	return response
}

// convertBillDetail 转换单个账单详情
func (c *Client) convertBillDetail(item *billing.ListForListBillDetailOutput) BillDetail {
	return BillDetail{
		// 核心标识字段
		BillDetailID: getStringValue(item.BillDetailId),
		BillID:       getStringValue(item.BillID),
		InstanceNo:   getStringValue(item.InstanceNo),

		// 账期和时间字段
		BillPeriod:       getStringValue(item.BillPeriod),
		BusiPeriod:       getStringValue(item.BusiPeriod),
		ExpenseDate:      getStringValue(item.ExpenseDate),
		ExpenseBeginTime: getStringValue(item.ExpenseBeginTime),
		ExpenseEndTime:   getStringValue(item.ExpenseEndTime),
		TradeTime:        getStringValue(item.TradeTime),

		// 产品和服务信息
		Product:     getStringValue(item.Product),
		ProductZh:   getStringValue(item.ProductZh),
		SolutionZh:  getStringValue(item.SolutionZh),
		Element:     getStringValue(item.Element),
		ElementCode: getStringValue(item.ElementCode),
		Factor:      getStringValue(item.Factor),
		FactorCode:  getStringValue(item.FactorCode),

		// 配置信息
		ConfigName:        getStringValue(item.ConfigName),
		ConfigurationCode: getStringValue(item.ConfigurationCode),
		InstanceName:      getStringValue(item.InstanceName),

		// 地域信息
		Region:        getStringValue(item.Region),
		RegionCode:    getStringValue(item.RegionCode),
		Zone:          getStringValue(item.Zone),
		ZoneCode:      getStringValue(item.ZoneCode),
		CountryRegion: getStringValue(item.CountryRegion),

		// 用量和计费信息
		Count:                getStringValue(item.Count),
		Unit:                 getStringValue(item.Unit),
		UseDuration:          getStringValue(item.UseDuration),
		UseDurationUnit:      getStringValue(item.UseDurationUnit),
		DeductionCount:       getStringValue(item.DeductionCount),
		DeductionUseDuration: getStringValue(item.DeductionUseDuration),

		// 价格信息
		Price:           getStringValue(item.Price),
		PriceUnit:       getStringValue(item.PriceUnit),
		PriceInterval:   getStringValue(item.PriceInterval),
		MarketPrice:     getStringValue(item.MarketPrice),
		Formula:         getStringValue(item.Formula),
		MeasureInterval: getStringValue(item.MeasureInterval),

		// 金额信息
		OriginalBillAmount:     getStringValue(item.OriginalBillAmount),
		PreferentialBillAmount: getStringValue(item.PreferentialBillAmount),
		DiscountBillAmount:     getStringValue(item.DiscountBillAmount),
		RoundAmount:            getFloat64Value(item.RoundAmount),

		// 实际价值和结算信息
		RealValue:             getStringValue(item.RealValue),
		PretaxRealValue:       getStringValue(item.PretaxRealValue),
		SettleRealValue:       getStringValue(item.SettleRealValue),
		SettlePretaxRealValue: getStringValue(item.SettlePretaxRealValue),

		// 应付金额信息
		PayableAmount:             getStringValue(item.PayableAmount),
		PreTaxPayableAmount:       getStringValue(item.PreTaxPayableAmount),
		SettlePayableAmount:       getStringValue(item.SettlePayableAmount),
		SettlePreTaxPayableAmount: getStringValue(item.SettlePreTaxPayableAmount),

		// 税费信息
		PretaxAmount:        getStringValue(item.PretaxAmount),
		PosttaxAmount:       getStringValue(item.PosttaxAmount),
		SettlePretaxAmount:  getStringValue(item.SettlePretaxAmount),
		SettlePosttaxAmount: getStringValue(item.SettlePosttaxAmount),
		Tax:                 getStringValue(item.Tax),
		SettleTax:           getStringValue(item.SettleTax),
		TaxRate:             getStringValue(item.TaxRate),

		// 付款信息
		PaidAmount:          getStringValue(item.PaidAmount),
		UnpaidAmount:        getStringValue(item.UnpaidAmount),
		CreditCarriedAmount: getStringValue(item.CreditCarriedAmount),

		// 优惠和抵扣信息
		CouponAmount:                      getStringValue(item.CouponAmount),
		DiscountInfo:                      getStringValue(item.DiscountInfo),
		SavingPlanDeductionDiscountAmount: getStringValue(item.SavingPlanDeductionDiscountAmount),
		SavingPlanDeductionSpID:           getStringValue(item.SavingPlanDeductionSpID),
		SavingPlanOriginalAmount:          getStringValue(item.SavingPlanOriginalAmount),
		ReservationInstance:               getStringValue(item.ReservationInstance),

		// 货币信息
		Currency:           getStringValue(item.Currency),
		CurrencySettlement: getStringValue(item.CurrencySettlement),
		ExchangeRate:       getStringValue(item.ExchangeRate),

		// 计费模式信息
		BillingMode:       getStringValue(item.BillingMode),
		BillingMethodCode: getStringValue(item.BillingMethodCode),
		BillingFunction:   getStringValue(item.BillingFunction),
		BusinessMode:      getStringValue(item.BusinessMode),
		SellingMode:       getStringValue(item.SellingMode),
		SettlementType:    getStringValue(item.SettlementType),

		// 折扣相关业务信息
		DiscountBizBillingFunction:   getStringValue(item.DiscountBizBillingFunction),
		DiscountBizMeasureInterval:   getStringValue(item.DiscountBizMeasureInterval),
		DiscountBizUnitPrice:         getStringValue(item.DiscountBizUnitPrice),
		DiscountBizUnitPriceInterval: getStringValue(item.DiscountBizUnitPriceInterval),

		// 用户和组织信息
		OwnerID:            getStringValue(item.OwnerID),
		OwnerUserName:      getStringValue(item.OwnerUserName),
		OwnerCustomerName:  getStringValue(item.OwnerCustomerName),
		PayerID:            getStringValue(item.PayerID),
		PayerUserName:      getStringValue(item.PayerUserName),
		PayerCustomerName:  getStringValue(item.PayerCustomerName),
		SellerID:           getStringValue(item.SellerID),
		SellerUserName:     getStringValue(item.SellerUserName),
		SellerCustomerName: getStringValue(item.SellerCustomerName),

		// 项目和分类信息
		Project:            getStringValue(item.Project),
		ProjectDisplayName: getStringValue(item.ProjectDisplayName),
		BillCategory:       getStringValue(item.BillCategory),
		SubjectName:        getStringValue(item.SubjectName),
		Tag:                getStringValue(item.Tag),

		// 其他业务信息
		MainContractNumber: getStringValue(item.MainContractNumber),
		OriginalOrderNo:    getStringValue(item.OriginalOrderNo),
		EffectiveFactor:    getStringValue(item.EffectiveFactor),
		ExpandField:        getStringValue(item.ExpandField),
	}
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

	if c.config.EnableDebug {
		log.Printf("[SDK映射] 成功构造输入参数: BillPeriod=%s, Limit=%d",
			req.BillPeriod, req.Limit)
	}

	return input
}





