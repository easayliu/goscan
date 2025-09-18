package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"time"

	"github.com/volcengine/volcengine-go-sdk/service/billing"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	"github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Client VolcEngine client
type Client struct {
	config         *config.VolcEngineConfig
	billingService *billing.BILLING
	rateLimiter    *rate.Limiter
	retryPolicy    RetryPolicy
}

// NewClient creates a VolcEngine client
func NewClient(cfg *config.VolcEngineConfig) (*Client, error) {
	if cfg == nil {
		cfg = config.NewVolcEngineConfig()
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, WrapError(err, "invalid config")
	}

	// Create SDK session
	sdkConfig := volcengine.NewConfig().
		WithRegion(cfg.Region).
		WithCredentials(credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""))

	sess, err := session.NewSession(sdkConfig)
	if err != nil {
		return nil, WrapError(err, "create session failed")
	}

	// Create rate limiter
	qps := cfg.RateLimit
	if qps <= 0 {
		qps = 10 // Default QPS
	}
	rateLimiter := rate.NewLimiter(rate.Limit(qps), 1)

	return &Client{
		config:         cfg,
		billingService: billing.New(sess),
		rateLimiter:    rateLimiter,
		retryPolicy:    DefaultExponentialBackoff(),
	}, nil
}

// ListBillDetail retrieves bill details
func (c *Client) ListBillDetail(ctx context.Context, req *ListBillDetailRequest) (*ListBillDetailResponse, error) {
	if req == nil {
		req = &ListBillDetailRequest{}
	}

	// Handle billing period
	if req.BillPeriod == "" {
		req.BillPeriod, _ = c.CalculateSmartPeriod()
		if c.config.EnableDebug {
			logger.Debug("Volcengine using intelligent period",
				zap.String("provider", "volcengine"),
				zap.String("period", req.BillPeriod))
		}
	}

	// Validate billing period
	if err := c.ValidatePeriod(req.BillPeriod); err != nil {
		return nil, WrapError(err, "validate period failed")
	}

	// Set default values and validate
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		return nil, WrapError(err, "validate request failed")
	}

	// Build SDK input
	input := c.buildSDKInput(req)

	if c.config.EnableDebug {
		logger.Debug("Volcengine API request",
			zap.String("provider", "volcengine"),
			zap.String("period", req.BillPeriod),
			zap.Int32("limit", req.Limit),
			zap.Int32("offset", req.Offset))
	}

	// Call API
	output, err := c.callAPIWithRetry(ctx, input)
	if err != nil {
		return nil, WrapError(err, "api call failed")
	}

	// Check response error
	if output.Metadata.Error != nil {
		return nil, NewAPIError(
			output.Metadata.Error.Code,
			output.Metadata.Error.Message,
		)
	}

	// Convert response
	response := c.convertSDKResponse(output)

	if c.config.EnableDebug {
		logger.Debug("Volcengine API response",
			zap.String("provider", "volcengine"),
			zap.Int("record_count", len(response.Result.List)),
			zap.Int32("total", response.Result.Total))
	}

	return response, nil
}

// callAPIWithRetry calls API with retry strategy
func (c *Client) callAPIWithRetry(ctx context.Context, input *billing.ListBillDetailInput) (*billing.ListBillDetailOutput, error) {
	var lastErr error

	for attempt := 0; attempt < c.retryPolicy.MaxAttempts(); attempt++ {
		// Rate limiting control
		if err := c.rateLimiter.Wait(ctx); err != nil {
			return nil, WrapError(err, "rate limit wait failed")
		}

		if attempt > 0 && c.config.EnableDebug {
			logger.Debug("Volcengine API retry",
				zap.String("provider", "volcengine"),
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", c.retryPolicy.MaxAttempts()))
		}

		// Call API
		output, err := c.billingService.ListBillDetailWithContext(ctx, input)
		if err == nil {
			return output, nil
		}

		lastErr = err

		// Check if should retry
		if !c.retryPolicy.ShouldRetry(err, attempt) {
			break
		}

		// Wait for retry
		delay := c.retryPolicy.GetDelay(attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, WrapError(lastErr, "all retry attempts failed")
}

// ValidatePeriod validates billing period format
func (c *Client) ValidatePeriod(billPeriod string) error {
	if billPeriod == "" {
		return ErrEmptyBillPeriod
	}

	// Check format (YYYY-MM)
	parsedTime, err := time.Parse("2006-01", billPeriod)
	if err != nil {
		return ErrInvalidBillPeriod
	}

	// Check if it's a future month
	now := time.Now()
	nextMonth := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, now.Location())
	billMonth := time.Date(parsedTime.Year(), parsedTime.Month(), 1, 0, 0, 0, 0, parsedTime.Location())

	if billMonth.After(nextMonth) || billMonth.Equal(nextMonth) {
		return ErrFutureBillPeriod
	}

	// Check if it's too old
	earliestAllowed := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	if billMonth.Before(earliestAllowed) {
		return ErrTooOldBillPeriod
	}

	return nil
}

// CalculateSmartPeriod calculates intelligent billing period
func (c *Client) CalculateSmartPeriod() (string, string) {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)

	// Calculate the first day of last month
	firstDayOfThisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	firstDayOfLastMonth := firstDayOfThisMonth.AddDate(0, -1, 0)

	startDate := firstDayOfLastMonth
	endDate := yesterday

	// Analyze time span
	lastMonthPeriod := startDate.Format("2006-01")
	currentMonthPeriod := now.Format("2006-01")

	// Calculate days covered by each billing period
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
		reason = fmt.Sprintf("last month dominates (%d days vs %d days)", lastMonthDays, currentMonthDays)
	} else if currentMonthDays > lastMonthDays {
		selectedPeriod = currentMonthPeriod
		reason = fmt.Sprintf("current month dominates (%d days vs %d days)", currentMonthDays, lastMonthDays)
	} else {
		selectedPeriod = lastMonthPeriod
		reason = "default to last month (data is more stable)"
	}

	dateRange := fmt.Sprintf("%s to %s, %s",
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"),
		reason)

	return selectedPeriod, dateRange
}

// convertSDKResponse converts SDK response to internal format
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

	// Convert bill detail list
	if output.List != nil {
		response.Result.List = make([]BillDetail, 0, len(output.List))
		for _, item := range output.List {
			response.Result.List = append(response.Result.List, c.convertBillDetail(item))
		}
	}

	return response
}

// convertBillDetail converts a single bill detail
func (c *Client) convertBillDetail(item *billing.ListForListBillDetailOutput) BillDetail {
	return BillDetail{
		// Core identification fields
		BillDetailID: getStringValue(item.BillDetailId),
		BillID:       getStringValue(item.BillID),
		InstanceNo:   getStringValue(item.InstanceNo),

		// Billing period and time fields
		BillPeriod:       getStringValue(item.BillPeriod),
		BusiPeriod:       getStringValue(item.BusiPeriod),
		ExpenseDate:      getStringValue(item.ExpenseDate),
		ExpenseBeginTime: getStringValue(item.ExpenseBeginTime),
		ExpenseEndTime:   getStringValue(item.ExpenseEndTime),
		TradeTime:        getStringValue(item.TradeTime),

		// Product and service information
		Product:     getStringValue(item.Product),
		ProductZh:   getStringValue(item.ProductZh),
		SolutionZh:  getStringValue(item.SolutionZh),
		Element:     getStringValue(item.Element),
		ElementCode: getStringValue(item.ElementCode),
		Factor:      getStringValue(item.Factor),
		FactorCode:  getStringValue(item.FactorCode),

		// Configuration information
		ConfigName:        getStringValue(item.ConfigName),
		ConfigurationCode: getStringValue(item.ConfigurationCode),
		InstanceName:      getStringValue(item.InstanceName),

		// Region information
		Region:        getStringValue(item.Region),
		RegionCode:    getStringValue(item.RegionCode),
		Zone:          getStringValue(item.Zone),
		ZoneCode:      getStringValue(item.ZoneCode),
		CountryRegion: getStringValue(item.CountryRegion),

		// Usage and billing information
		Count:                getStringValue(item.Count),
		Unit:                 getStringValue(item.Unit),
		UseDuration:          getStringValue(item.UseDuration),
		UseDurationUnit:      getStringValue(item.UseDurationUnit),
		DeductionCount:       getStringValue(item.DeductionCount),
		DeductionUseDuration: getStringValue(item.DeductionUseDuration),

		// Price information
		Price:           getStringValue(item.Price),
		PriceUnit:       getStringValue(item.PriceUnit),
		PriceInterval:   getStringValue(item.PriceInterval),
		MarketPrice:     getStringValue(item.MarketPrice),
		Formula:         getStringValue(item.Formula),
		MeasureInterval: getStringValue(item.MeasureInterval),

		// Amount information
		OriginalBillAmount:     getStringValue(item.OriginalBillAmount),
		PreferentialBillAmount: getStringValue(item.PreferentialBillAmount),
		DiscountBillAmount:     getStringValue(item.DiscountBillAmount),
		RoundAmount:            getFloat64Value(item.RoundAmount),

		// Actual value and settlement information
		RealValue:             getStringValue(item.RealValue),
		PretaxRealValue:       getStringValue(item.PretaxRealValue),
		SettleRealValue:       getStringValue(item.SettleRealValue),
		SettlePretaxRealValue: getStringValue(item.SettlePretaxRealValue),

		// Payable amount information
		PayableAmount:             getStringValue(item.PayableAmount),
		PreTaxPayableAmount:       getStringValue(item.PreTaxPayableAmount),
		SettlePayableAmount:       getStringValue(item.SettlePayableAmount),
		SettlePreTaxPayableAmount: getStringValue(item.SettlePreTaxPayableAmount),

		// Tax information
		PretaxAmount:        getStringValue(item.PretaxAmount),
		PosttaxAmount:       getStringValue(item.PosttaxAmount),
		SettlePretaxAmount:  getStringValue(item.SettlePretaxAmount),
		SettlePosttaxAmount: getStringValue(item.SettlePosttaxAmount),
		Tax:                 getStringValue(item.Tax),
		SettleTax:           getStringValue(item.SettleTax),
		TaxRate:             getStringValue(item.TaxRate),

		// Payment information
		PaidAmount:          getStringValue(item.PaidAmount),
		UnpaidAmount:        getStringValue(item.UnpaidAmount),
		CreditCarriedAmount: getStringValue(item.CreditCarriedAmount),

		// Discount and deduction information
		CouponAmount:                      getStringValue(item.CouponAmount),
		DiscountInfo:                      getStringValue(item.DiscountInfo),
		SavingPlanDeductionDiscountAmount: getStringValue(item.SavingPlanDeductionDiscountAmount),
		SavingPlanDeductionSpID:           getStringValue(item.SavingPlanDeductionSpID),
		SavingPlanOriginalAmount:          getStringValue(item.SavingPlanOriginalAmount),
		ReservationInstance:               getStringValue(item.ReservationInstance),

		// Currency information
		Currency:           getStringValue(item.Currency),
		CurrencySettlement: getStringValue(item.CurrencySettlement),
		ExchangeRate:       getStringValue(item.ExchangeRate),

		// Billing mode information
		BillingMode:       getStringValue(item.BillingMode),
		BillingMethodCode: getStringValue(item.BillingMethodCode),
		BillingFunction:   getStringValue(item.BillingFunction),
		BusinessMode:      getStringValue(item.BusinessMode),
		SellingMode:       getStringValue(item.SellingMode),
		SettlementType:    getStringValue(item.SettlementType),

		// Discount related business information
		DiscountBizBillingFunction:   getStringValue(item.DiscountBizBillingFunction),
		DiscountBizMeasureInterval:   getStringValue(item.DiscountBizMeasureInterval),
		DiscountBizUnitPrice:         getStringValue(item.DiscountBizUnitPrice),
		DiscountBizUnitPriceInterval: getStringValue(item.DiscountBizUnitPriceInterval),

		// User and organization information
		OwnerID:            getStringValue(item.OwnerID),
		OwnerUserName:      getStringValue(item.OwnerUserName),
		OwnerCustomerName:  getStringValue(item.OwnerCustomerName),
		PayerID:            getStringValue(item.PayerID),
		PayerUserName:      getStringValue(item.PayerUserName),
		PayerCustomerName:  getStringValue(item.PayerCustomerName),
		SellerID:           getStringValue(item.SellerID),
		SellerUserName:     getStringValue(item.SellerUserName),
		SellerCustomerName: getStringValue(item.SellerCustomerName),

		// Project and category information
		Project:            getStringValue(item.Project),
		ProjectDisplayName: getStringValue(item.ProjectDisplayName),
		BillCategory:       getStringValue(item.BillCategory),
		SubjectName:        getStringValue(item.SubjectName),
		Tag:                getStringValue(item.Tag),

		// Other business information
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

// buildSDKInput constructs SDK input parameters (fully refactored version)
// Strictly mapped according to github.com/volcengine/volcengine-go-sdk/service/billing.ListBillDetailInput structure
func (c *Client) buildSDKInput(req *ListBillDetailRequest) *billing.ListBillDetailInput {
	input := &billing.ListBillDetailInput{}

	// === Set required parameters ===
	input.BillPeriod = &req.BillPeriod
	input.Limit = &req.Limit

	// === Set optional parameters (fully mapped according to SDK fields) ===

	// Offset - defaults to 0
	if req.Offset > 0 {
		input.Offset = &req.Offset
	}

	// NeedRecordNum - whether total record count is needed
	if req.NeedRecordNum > 0 {
		input.NeedRecordNum = &req.NeedRecordNum
	}

	// IgnoreZero - whether to ignore zero amount bills
	if req.IgnoreZero > 0 {
		input.IgnoreZero = &req.IgnoreZero
	}

	// GroupPeriod - grouping period
	if req.GroupPeriod > 0 {
		input.GroupPeriod = &req.GroupPeriod
	}

	// GroupTerm - grouping condition
	if req.GroupTerm > 0 {
		input.GroupTerm = &req.GroupTerm
	}

	// ExpenseDate - expense date
	if req.ExpenseDate != "" {
		input.ExpenseDate = &req.ExpenseDate
	}

	// InstanceNo - instance number
	if req.InstanceNo != "" {
		input.InstanceNo = &req.InstanceNo
	}

	// BillCategory - bill category list
	if len(req.BillCategory) > 0 {
		input.BillCategory = make([]*string, len(req.BillCategory))
		for i, category := range req.BillCategory {
			input.BillCategory[i] = &category
		}
	}

	// BillingMode - billing mode list
	if len(req.BillingMode) > 0 {
		input.BillingMode = make([]*string, len(req.BillingMode))
		for i, mode := range req.BillingMode {
			input.BillingMode[i] = &mode
		}
	}

	// Product - product name list
	if len(req.Product) > 0 {
		input.Product = make([]*string, len(req.Product))
		for i, product := range req.Product {
			input.Product[i] = &product
		}
	}

	// OwnerID - owner ID list
	if len(req.OwnerID) > 0 {
		input.OwnerID = make([]*int64, len(req.OwnerID))
		for i, ownerID := range req.OwnerID {
			input.OwnerID[i] = &ownerID
		}
	}

	// PayerID - payer ID list
	if len(req.PayerID) > 0 {
		input.PayerID = make([]*int64, len(req.PayerID))
		for i, payerID := range req.PayerID {
			input.PayerID[i] = &payerID
		}
	}

	if c.config.EnableDebug {
		logger.Debug("Volcengine SDK mapping successful",
			zap.String("provider", "volcengine"),
			zap.String("bill_period", req.BillPeriod),
			zap.Int32("limit", req.Limit))
	}

	return input
}
