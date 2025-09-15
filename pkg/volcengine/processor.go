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
	EnableDeduplication bool          // 是否启用去重检查（借鉴阿里云）
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
		EnableDeduplication: true, // 默认启用去重检查
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
	log.Printf("📊 [处理器] 开始转换 %d 条账单数据", len(data))
	records := make([]map[string]interface{}, 0, len(data))
	for i, bill := range data {
		if i > 0 && i%100 == 0 {
			log.Printf("⏳ [处理器] 转换进度: %d/%d", i, len(data))
		}
		record := p.convertBillToRecordDirect(bill)
		records = append(records, record)
	}
	log.Printf("✅ [处理器] 数据转换完成，准备批量插入 %d 条记录", len(records))


	// 使用优化的批量插入
	log.Printf("💾 [处理器] 准备批量插入到表: %s (分布式: %v)", p.tableName, p.isDistributed)
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
		log.Printf("📤 [处理器] 开始分布式批量插入...")
		batchResult, err = p.client.OptimizedBatchInsertToDistributed(ctx, p.tableName, records, batchOpts)
	} else {
		log.Printf("📤 [处理器] 开始本地批量插入...")
		batchResult, err = p.client.OptimizedBatchInsert(ctx, p.tableName, records, batchOpts)
	}

	result.BatchResult = batchResult
	if err != nil {
		log.Printf("❌ [处理器] 批量插入失败: %v", err)
		result.Errors = append(result.Errors, err)
		return result, err
	}
	
	log.Printf("✅ [处理器] 批量插入成功，插入 %d 条，失败 %d 条", 
		batchResult.InsertedRecords, batchResult.FailedRecords)

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
		"usage_start_time": dbBill.UsageStartTime,
		"usage_end_time":   dbBill.UsageEndTime,
		"tags":             tagsMap,
		"created_at":       time.Now(),
		"updated_at":       time.Now(),
	}
}

// convertBillToRecordDirect 直接转换API原始数据到数据库记录（使用Pascal case字段名与表结构一致）
func (p *ClickHouseProcessor) convertBillToRecordDirect(bill BillDetail) map[string]interface{} {
	return map[string]interface{}{
		// 直接使用API字段名（Pascal case），与表结构保持一致
		"BillDetailId": bill.BillDetailID,
		"BillID":       bill.BillID,
		"InstanceNo":   bill.InstanceNo,

		// 时间字段
		"BillPeriod":       bill.BillPeriod,
		"BusiPeriod":       bill.BusiPeriod,
		"ExpenseDate":      bill.ExpenseDate,
		"ExpenseBeginTime": bill.ExpenseBeginTime,
		"ExpenseEndTime":   bill.ExpenseEndTime,
		"TradeTime":        bill.TradeTime,

		// 用户信息字段
		"PayerID":           bill.PayerID,
		"PayerUserName":     bill.PayerUserName,
		"PayerCustomerName": bill.PayerCustomerName,
		"SellerID":          bill.SellerID,
		"SellerUserName":    bill.SellerUserName,
		"SellerCustomerName": bill.SellerCustomerName,
		"OwnerID":           bill.OwnerID,
		"OwnerUserName":     bill.OwnerUserName,
		"OwnerCustomerName": bill.OwnerCustomerName,
		
		// 产品信息字段
		"Product":     bill.Product,
		"ProductZh":   bill.ProductZh,
		"SolutionZh":  bill.SolutionZh,
		"Element":     bill.Element,
		"ElementCode": bill.ElementCode,
		"Factor":      bill.Factor,
		"FactorCode":  bill.FactorCode,

		// 配置信息字段
		"ConfigName":        bill.ConfigName,
		"ConfigurationCode": bill.ConfigurationCode,
		"InstanceName":      bill.InstanceName,

		// 地域信息字段
		"Region":     bill.Region,
		"RegionCode": bill.RegionCode,
		"Zone":       bill.Zone,
		"ZoneCode":   bill.ZoneCode,

		// 计费模式信息
		"BillingMode":       bill.BillingMode,
		"BusinessMode":      bill.BusinessMode,
		"BillingFunction":   bill.BillingFunction,
		"BillingMethodCode": bill.BillingMethodCode,
		"SellingMode":       bill.SellingMode,
		"SettlementType":    bill.SettlementType,
		
		// 用量信息字段
		"Count":                bill.Count,
		"Unit":                 bill.Unit,
		"UseDuration":          bill.UseDuration,
		"UseDurationUnit":      bill.UseDurationUnit,
		"DeductionCount":       bill.DeductionCount,
		"DeductionUseDuration": bill.DeductionUseDuration,

		// 价格信息字段
		"Price":           bill.Price,
		"PriceUnit":       bill.PriceUnit,
		"PriceInterval":   bill.PriceInterval,
		"MarketPrice":     bill.MarketPrice,
		"MeasureInterval": bill.MeasureInterval,

		// 金额信息字段
		"OriginalBillAmount":     bill.OriginalBillAmount,
		"PreferentialBillAmount": bill.PreferentialBillAmount,
		"DiscountBillAmount":     bill.DiscountBillAmount,
		"RoundAmount":            bill.RoundAmount,
		"PayableAmount":          bill.PayableAmount,
		"PaidAmount":             bill.PaidAmount,
		"UnpaidAmount":           bill.UnpaidAmount,
		"CouponAmount":           bill.CouponAmount,
		"CreditCarriedAmount":    bill.CreditCarriedAmount,

		// 其他信息字段
		"Currency":            bill.Currency,
		"Project":             bill.Project,
		"ProjectDisplayName":  bill.ProjectDisplayName,
		"Tag":                 bill.Tag,
		"BillCategory":        bill.BillCategory,
		"SubjectName":         bill.SubjectName,
		"ReservationInstance": bill.ReservationInstance,
		"ExpandField":         bill.ExpandField,
		"EffectiveFactor":     bill.EffectiveFactor,
		
		// 折扣相关字段
		"DiscountBizBillingFunction":   bill.DiscountBizBillingFunction,
		"DiscountBizUnitPrice":         bill.DiscountBizUnitPrice,
		"DiscountBizUnitPriceInterval": bill.DiscountBizUnitPriceInterval,
		"DiscountBizMeasureInterval":   bill.DiscountBizMeasureInterval,
		
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

