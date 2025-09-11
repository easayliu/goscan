package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"log"
	"os"
	"strconv"
	"time"
)

// ProcessorOptions 处理器选项
type ProcessorOptions struct {
	BatchSize           int           // 批次大小
	MaxRetries          int           // 最大重试次数
	RetryDelay          time.Duration // 重试延迟
	EnableAsync         bool          // 是否启用异步插入
	Timeout             time.Duration // 超时时间
	ProgressLog         bool          // 是否显示进度日志
	DryRunCleanup       bool          // 清理时是否仅预览
	OptimizeAfterInsert bool          // 插入后是否优化表
}

// DefaultProcessorOptions 返回默认的处理器选项
func DefaultProcessorOptions() *ProcessorOptions {
	batchSize := 500
	if size := os.Getenv("CLICKHOUSE_BATCH_SIZE"); size != "" {
		if parsed, err := strconv.Atoi(size); err == nil && parsed > 0 {
			batchSize = parsed
		}
	}

	return &ProcessorOptions{
		BatchSize:           batchSize,
		MaxRetries:          3,
		RetryDelay:          2 * time.Second,
		EnableAsync:         false,
		Timeout:             30 * time.Second,
		ProgressLog:         true,
		DryRunCleanup:       false,
		OptimizeAfterInsert: false,
	}
}

// ClickHouseProcessor ClickHouse数据处理器
type ClickHouseProcessor struct {
	client            *clickhouse.Client
	tableName         string
	isDistributed     bool
	cleanBeforeInsert bool
	cleanCondition    string
	cleanArgs         []interface{}
	options           *ProcessorOptions
}

// NewClickHouseProcessor 创建ClickHouse处理器
func NewClickHouseProcessor(client *clickhouse.Client, tableName string, isDistributed bool) *ClickHouseProcessor {
	return &ClickHouseProcessor{
		client:            client,
		tableName:         tableName,
		isDistributed:     isDistributed,
		cleanBeforeInsert: false,
		options:           DefaultProcessorOptions(),
	}
}

// NewClickHouseProcessorWithOptions 使用选项创建ClickHouse处理器
func NewClickHouseProcessorWithOptions(client *clickhouse.Client, tableName string, isDistributed bool, options *ProcessorOptions) *ClickHouseProcessor {
	if options == nil {
		options = DefaultProcessorOptions()
	}
	return &ClickHouseProcessor{
		client:            client,
		tableName:         tableName,
		isDistributed:     isDistributed,
		cleanBeforeInsert: false,
		options:           options,
	}
}

// SetCleanup 设置数据清理选项
func (p *ClickHouseProcessor) SetCleanup(condition string, args ...interface{}) {
	p.cleanBeforeInsert = true
	p.cleanCondition = condition
	p.cleanArgs = args
}

// SetCleanupWithDryRun 设置数据清理选项（支持预览模式）
func (p *ClickHouseProcessor) SetCleanupWithDryRun(condition string, dryRun bool, args ...interface{}) {
	p.cleanBeforeInsert = true
	p.cleanCondition = condition
	p.cleanArgs = args
	p.options.DryRunCleanup = dryRun
}

// SetBatchSize 设置批次大小
func (p *ClickHouseProcessor) SetBatchSize(size int) {
	if size > 0 {
		p.options.BatchSize = size
	}
}

// SetOptions 设置处理器选项
func (p *ClickHouseProcessor) SetOptions(options *ProcessorOptions) {
	if options != nil {
		p.options = options
	}
}

// GetOptions 获取处理器选项
func (p *ClickHouseProcessor) GetOptions() *ProcessorOptions {
	return p.options
}

// ProcessResult 处理结果
type ProcessResult struct {
	TotalRecords    int                           `json:"total_records"`
	InsertedRecords int                           `json:"inserted_records"`
	FailedRecords   int                           `json:"failed_records"`
	Duration        time.Duration                 `json:"duration"`
	AverageSpeed    float64                       `json:"average_speed"` // records per second
	CleanupResult   *clickhouse.CleanupResult     `json:"cleanup_result,omitempty"`
	BatchResult     *clickhouse.BatchInsertResult `json:"batch_result,omitempty"`
	Errors          []error                       `json:"errors,omitempty"`
}

// Process 处理数据批次
func (p *ClickHouseProcessor) Process(ctx context.Context, data []BillDetail) error {
	result, err := p.ProcessWithResult(ctx, data)
	if p.options.ProgressLog {
		log.Printf("[处理器] 处理完成: %s", result.String())
	}
	return err
}

// ProcessWithResult 处理数据批次并返回详细结果
func (p *ClickHouseProcessor) ProcessWithResult(ctx context.Context, data []BillDetail) (*ProcessResult, error) {
	result := &ProcessResult{
		TotalRecords: len(data),
		Errors:       make([]error, 0),
	}

	if len(data) == 0 {
		return result, nil
	}

	startTime := time.Now()
	defer func() {
		result.Duration = time.Since(startTime)
		if result.Duration > 0 {
			result.AverageSpeed = float64(result.InsertedRecords) / result.Duration.Seconds()
		}
	}()

	if p.options.ProgressLog {
		log.Printf("[处理器] 开始处理 %d 条记录到表 %s", len(data), p.tableName)
	}

	// 数据清理
	if p.cleanBeforeInsert {
		cleanupOpts := &clickhouse.CleanupOptions{
			Condition:   p.cleanCondition,
			Args:        p.cleanArgs,
			DryRun:      p.options.DryRunCleanup,
			ProgressLog: p.options.ProgressLog,
		}

		if p.options.ProgressLog {
			if p.options.DryRunCleanup {
				log.Printf("[处理器] 预览清理表 %s 中的数据，条件: %s", p.tableName, p.cleanCondition)
			} else {
				log.Printf("[处理器] 清理表 %s 中的数据，条件: %s", p.tableName, p.cleanCondition)
			}
		}

		cleanupResult, err := p.client.EnhancedCleanTableData(ctx, p.tableName, cleanupOpts)
		result.CleanupResult = cleanupResult
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("数据清理失败: %w", err))
			return result, err
		}

		if p.options.ProgressLog {
			if p.options.DryRunCleanup {
				log.Printf("[处理器] 清理预览完成: %s", cleanupResult.String())
			} else {
				log.Printf("[处理器] 数据清理完成: %s", cleanupResult.String())
			}
		}

		// 如果是预览模式，直接返回不执行插入
		if p.options.DryRunCleanup {
			return result, nil
		}

		// 清理完成后重置标志
		p.cleanBeforeInsert = false
	}

	// 转换数据格式（直接使用原始API数据）
	records := make([]map[string]interface{}, 0, len(data))
	for _, bill := range data {
		record := p.convertBillToRecordDirect(bill)
		records = append(records, record)
	}

	// 转换完成，直接使用记录

	// 使用优化的批量插入
	batchOpts := &clickhouse.BatchInsertOptions{
		BatchSize:   p.options.BatchSize,
		MaxRetries:  p.options.MaxRetries,
		RetryDelay:  p.options.RetryDelay,
		EnableAsync: p.options.EnableAsync,
		Timeout:     p.options.Timeout,
	}

	var batchResult *clickhouse.BatchInsertResult
	var err error

	if p.isDistributed {
		batchResult, err = p.client.OptimizedBatchInsertToDistributed(ctx, p.tableName, records, batchOpts)
	} else {
		batchResult, err = p.client.OptimizedBatchInsert(ctx, p.tableName, records, batchOpts)
	}

	result.BatchResult = batchResult
	if err != nil {
		result.Errors = append(result.Errors, err)
		return result, err
	}

	result.InsertedRecords = batchResult.InsertedRecords
	result.FailedRecords = batchResult.FailedRecords

	// 如果有失败的批次，添加到错误列表
	if len(batchResult.Errors) > 0 {
		result.Errors = append(result.Errors, batchResult.Errors...)
	}

	// 插入后优化表（可选）
	if p.options.OptimizeAfterInsert && batchResult.IsSuccess() {
		if p.options.ProgressLog {
			log.Printf("[处理器] 开始优化表 %s", p.tableName)
		}
		if err := p.client.OptimizeTable(ctx, p.tableName, false); err != nil {
			if p.options.ProgressLog {
				log.Printf("[处理器] 表优化失败: %v", err)
			}
			// 优化失败不影响主流程
		} else if p.options.ProgressLog {
			log.Printf("[处理器] 表优化完成")
		}
	}

	return result, nil
}

// String 返回处理结果的字符串表示
func (r *ProcessResult) String() string {
	status := "SUCCESS"
	if r.FailedRecords > 0 {
		if r.InsertedRecords > 0 {
			status = "PARTIAL"
		} else {
			status = "FAILED"
		}
	}

	return fmt.Sprintf("ProcessResult{Status: %s, Total: %d, Inserted: %d, Failed: %d, Duration: %v, Speed: %.1f records/s}",
		status, r.TotalRecords, r.InsertedRecords, r.FailedRecords, r.Duration, r.AverageSpeed)
}

// IsSuccess 检查处理是否完全成功
func (r *ProcessResult) IsSuccess() bool {
	return r.FailedRecords == 0 && len(r.Errors) == 0
}

// GetSuccessRate 获取成功率
func (r *ProcessResult) GetSuccessRate() float64 {
	if r.TotalRecords == 0 {
		return 0
	}
	return float64(r.InsertedRecords) / float64(r.TotalRecords) * 100
}

// WithCleanupPreview 创建带清理预览的处理器副本
func (p *ClickHouseProcessor) WithCleanupPreview(condition string, args ...interface{}) *ClickHouseProcessor {
	newProcessor := *p
	newProcessor.SetCleanupWithDryRun(condition, true, args...)
	return &newProcessor
}

// WithBatchSize 创建带指定批次大小的处理器副本
func (p *ClickHouseProcessor) WithBatchSize(size int) *ClickHouseProcessor {
	newProcessor := *p
	newOptions := *p.options
	newOptions.BatchSize = size
	newProcessor.options = &newOptions
	return &newProcessor
}

// WithAsyncInsert 创建带异步插入的处理器副本
func (p *ClickHouseProcessor) WithAsyncInsert(enabled bool) *ClickHouseProcessor {
	newProcessor := *p
	newOptions := *p.options
	newOptions.EnableAsync = enabled
	newProcessor.options = &newOptions
	return &newProcessor
}

// EnableProgressLogging 启用进度日志
func (p *ClickHouseProcessor) EnableProgressLogging() {
	p.options.ProgressLog = true
}

// DisableProgressLogging 禁用进度日志
func (p *ClickHouseProcessor) DisableProgressLogging() {
	p.options.ProgressLog = false
}

// SetRetryOptions 设置重试选项
func (p *ClickHouseProcessor) SetRetryOptions(maxRetries int, retryDelay time.Duration) {
	p.options.MaxRetries = maxRetries
	p.options.RetryDelay = retryDelay
}

// SetTimeout 设置超时时间
func (p *ClickHouseProcessor) SetTimeout(timeout time.Duration) {
	p.options.Timeout = timeout
}

// convertBillToRecord 转换账单数据为数据库记录
func (p *ClickHouseProcessor) convertBillToRecord(bill BillDetail) map[string]interface{} {
	dbBill := bill.ToDBFormat()

	// 转换标签为ClickHouse Map格式
	tagsMap := make(map[string]string)
	for k, v := range dbBill.Tags {
		if str, ok := v.(string); ok {
			tagsMap[k] = str
		} else {
			tagsMap[k] = fmt.Sprintf("%v", v)
		}
	}

	return map[string]interface{}{
		"id":               dbBill.ID,
		"owner_id":         dbBill.OwnerID,
		"owner_user_name":  dbBill.OwnerUserName,
		"product":          dbBill.Product,
		"product_zh":       dbBill.ProductZh,
		"billing_mode":     dbBill.BillingMode,
		"bill_period":      dbBill.BillPeriod,
		"amount":           dbBill.Amount,
		"currency":         dbBill.Currency,
		"region":           dbBill.Region,
		"zone":             dbBill.Zone,
		"instance_name":    dbBill.InstanceName,
		"config_name":      dbBill.ConfigName,
		"element":          dbBill.Element,
		"price":            dbBill.Price,
		"price_unit":       dbBill.PriceUnit,
		"count":            dbBill.Count,
		"unit":             dbBill.Unit,
		"project":          dbBill.Project,
		"round_amount":     dbBill.RoundAmount,
		"expense_date":     dbBill.ExpenseDate,
		"expense_time":     dbBill.ExpenseTime,
		"usage_start_time": dbBill.UsageStartTime,
		"usage_end_time":   dbBill.UsageEndTime,
		"tags":             tagsMap,
		"created_at":       time.Now(),
		"updated_at":       time.Now(),
	}
}

// convertBillToRecordDirect 直接转换API原始数据到数据库记录（无数据转换）
func (p *ClickHouseProcessor) convertBillToRecordDirect(bill BillDetail) map[string]interface{} {
	return map[string]interface{}{
		// 核心标识字段
		"bill_detail_id": bill.BillDetailID,
		"bill_id":        bill.BillID,
		"instance_no":    bill.InstanceNo,

		// 账期和时间字段
		"bill_period":        bill.BillPeriod,
		"busi_period":        bill.BusiPeriod,
		"expense_date":       bill.ExpenseDate,
		"expense_begin_time": bill.ExpenseBeginTime,
		"expense_end_time":   bill.ExpenseEndTime,
		"trade_time":         bill.TradeTime,

		// 产品和服务信息
		"product":      bill.Product,
		"product_zh":   bill.ProductZh,
		"solution_zh":  bill.SolutionZh,
		"element":      bill.Element,
		"element_code": bill.ElementCode,
		"factor":       bill.Factor,
		"factor_code":  bill.FactorCode,

		// 配置信息
		"config_name":        bill.ConfigName,
		"configuration_code": bill.ConfigurationCode,
		"instance_name":      bill.InstanceName,

		// 地域信息
		"region":         bill.Region,
		"region_code":    bill.RegionCode,
		"zone":           bill.Zone,
		"zone_code":      bill.ZoneCode,
		"country_region": bill.CountryRegion,

		// 用量和计费信息
		"count":                  bill.Count,
		"unit":                   bill.Unit,
		"use_duration":           bill.UseDuration,
		"use_duration_unit":      bill.UseDurationUnit,
		"deduction_count":        bill.DeductionCount,
		"deduction_use_duration": bill.DeductionUseDuration,

		// 价格信息
		"price":            bill.Price,
		"price_unit":       bill.PriceUnit,
		"price_interval":   bill.PriceInterval,
		"market_price":     bill.MarketPrice,
		"formula":          bill.Formula,
		"measure_interval": bill.MeasureInterval,

		// 金额信息（核心）- 保持原始字符串格式
		"original_bill_amount":     bill.OriginalBillAmount,
		"preferential_bill_amount": bill.PreferentialBillAmount,
		"discount_bill_amount":     bill.DiscountBillAmount,
		"round_amount":             bill.RoundAmount,

		// 实际价值和结算信息
		"real_value":               bill.RealValue,
		"pretax_real_value":        bill.PretaxRealValue,
		"settle_real_value":        bill.SettleRealValue,
		"settle_pretax_real_value": bill.SettlePretaxRealValue,

		// 应付金额信息
		"payable_amount":                bill.PayableAmount,
		"pre_tax_payable_amount":        bill.PreTaxPayableAmount,
		"settle_payable_amount":         bill.SettlePayableAmount,
		"settle_pre_tax_payable_amount": bill.SettlePreTaxPayableAmount,

		// 税费信息
		"pretax_amount":         bill.PretaxAmount,
		"posttax_amount":        bill.PosttaxAmount,
		"settle_pretax_amount":  bill.SettlePretaxAmount,
		"settle_posttax_amount": bill.SettlePosttaxAmount,
		"tax":                   bill.Tax,
		"settle_tax":            bill.SettleTax,
		"tax_rate":              bill.TaxRate,

		// 付款信息
		"paid_amount":           bill.PaidAmount,
		"unpaid_amount":         bill.UnpaidAmount,
		"credit_carried_amount": bill.CreditCarriedAmount,

		// 优惠和抵扣信息
		"coupon_amount":                         bill.CouponAmount,
		"discount_info":                         bill.DiscountInfo,
		"saving_plan_deduction_discount_amount": bill.SavingPlanDeductionDiscountAmount,
		"saving_plan_deduction_sp_id":           bill.SavingPlanDeductionSpID,
		"saving_plan_original_amount":           bill.SavingPlanOriginalAmount,
		"reservation_instance":                  bill.ReservationInstance,

		// 货币信息
		"currency":            bill.Currency,
		"currency_settlement": bill.CurrencySettlement,
		"exchange_rate":       bill.ExchangeRate,

		// 计费模式信息
		"billing_mode":        bill.BillingMode,
		"billing_method_code": bill.BillingMethodCode,
		"billing_function":    bill.BillingFunction,
		"business_mode":       bill.BusinessMode,
		"selling_mode":        bill.SellingMode,
		"settlement_type":     bill.SettlementType,

		// 折扣相关业务信息
		"discount_biz_billing_function":    bill.DiscountBizBillingFunction,
		"discount_biz_measure_interval":    bill.DiscountBizMeasureInterval,
		"discount_biz_unit_price":          bill.DiscountBizUnitPrice,
		"discount_biz_unit_price_interval": bill.DiscountBizUnitPriceInterval,

		// 用户和组织信息
		"owner_id":             bill.OwnerID,
		"owner_user_name":      bill.OwnerUserName,
		"owner_customer_name":  bill.OwnerCustomerName,
		"payer_id":             bill.PayerID,
		"payer_user_name":      bill.PayerUserName,
		"payer_customer_name":  bill.PayerCustomerName,
		"seller_id":            bill.SellerID,
		"seller_user_name":     bill.SellerUserName,
		"seller_customer_name": bill.SellerCustomerName,

		// 项目和分类信息
		"project":              bill.Project,
		"project_display_name": bill.ProjectDisplayName,
		"bill_category":        bill.BillCategory,
		"subject_name":         bill.SubjectName,
		"tag":                  bill.Tag, // JSON字符串，保持原格式

		// 其他业务信息
		"main_contract_number": bill.MainContractNumber,
		"original_order_no":    bill.OriginalOrderNo,
		"effective_factor":     bill.EffectiveFactor,
		"expand_field":         bill.ExpandField,

		// 系统字段
		"created_at": time.Now(),
		"updated_at": time.Now(),
	}
}

// BatchProcessor 批处理器，支持多种数据源
type BatchProcessor struct {
	processors []DataProcessor
}

// NewBatchProcessor 创建批处理器
func NewBatchProcessor(processors ...DataProcessor) *BatchProcessor {
	return &BatchProcessor{processors: processors}
}

// Process 并行处理数据
func (bp *BatchProcessor) Process(ctx context.Context, data []BillDetail) error {
	errCh := make(chan error, len(bp.processors))

	for _, processor := range bp.processors {
		go func(p DataProcessor) {
			errCh <- p.Process(ctx, data)
		}(processor)
	}

	var errors []error
	for i := 0; i < len(bp.processors); i++ {
		if err := <-errCh; err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("批处理失败: %v", errors)
	}

	return nil
}

// MockProcessor 模拟处理器，用于测试
type MockProcessor struct {
	processedCount int
	delay          time.Duration
}

// NewMockProcessor 创建模拟处理器
func NewMockProcessor(delay time.Duration) *MockProcessor {
	return &MockProcessor{delay: delay}
}

// Process 模拟处理数据
func (mp *MockProcessor) Process(ctx context.Context, data []BillDetail) error {
	if mp.delay > 0 {
		time.Sleep(mp.delay)
	}
	mp.processedCount += len(data)
	log.Printf("[模拟处理器] 处理了 %d 条记录，累计: %d", len(data), mp.processedCount)
	return nil
}

// GetProcessedCount 获取已处理数量
func (mp *MockProcessor) GetProcessedCount() int {
	return mp.processedCount
}
