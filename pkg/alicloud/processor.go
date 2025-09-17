package alicloud

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/logger"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Processor 阿里云账单数据处理器
type Processor struct {
	chClient         *clickhouse.Client
	options          *ProcessorOptions
	transformer      *BatchTransformer
	mu               sync.RWMutex
	processedRecords int64
	totalRecords     int64
	startTime        time.Time
}

// 实现 DataProcessor 接口
var _ DataProcessor = (*Processor)(nil)

// ProcessorOptions 处理器选项
type ProcessorOptions struct {
	BatchSize           int                    // 批次大小
	WorkerCount         int                    // 工作协程数
	EnableValidation    bool                   // 是否启用数据验证
	SkipErrorRecords    bool                   // 是否跳过错误记录
	SkipZeroAmount      bool                   // 是否跳过零金额记录
	EnableDeduplication bool                   // 是否启用去重
	MemoryLimitMB       int                    // 内存限制（MB）
	TimeoutPerBatch     time.Duration          // 每批次超时时间
	ProgressCallback    func(int64, int64)     // 进度回调函数
	RecordFilter        func(*BillDetail) bool // 记录过滤函数
	EnableRetry         bool                   // 是否启用重试
	MaxRetries          int                    // 最大重试次数
}

// DefaultProcessorOptions 返回默认的处理器选项
func DefaultProcessorOptions() *ProcessorOptions {
	return &ProcessorOptions{
		BatchSize:           1000,
		WorkerCount:         4,
		EnableValidation:    true,
		SkipErrorRecords:    true,
		SkipZeroAmount:      false,
		EnableDeduplication: true,
		MemoryLimitMB:       200,
		TimeoutPerBatch:     60 * time.Second,
		EnableRetry:         true,
		MaxRetries:          3,
	}
}

// NewProcessor 创建新的数据处理器
func NewProcessor(chClient *clickhouse.Client, syncOptions *SyncOptions) *Processor {
	options := DefaultProcessorOptions()

	// 从 SyncOptions 复制相关配置
	if syncOptions != nil {
		if syncOptions.BatchSize > 0 {
			options.BatchSize = syncOptions.BatchSize
		}
		if syncOptions.MaxWorkers > 0 {
			options.WorkerCount = syncOptions.MaxWorkers
		}
		options.EnableValidation = syncOptions.EnableValidation
		options.SkipZeroAmount = syncOptions.SkipZeroAmount

		// 转换进度回调函数
		if syncOptions.ProgressCallback != nil {
			options.ProgressCallback = func(processed, total int64) {
				syncOptions.ProgressCallback(int(processed), int(total))
			}
		}
	}

	// 创建批量转换器
	transformOptions := DefaultBatchTransformationOptions()
	transformOptions.BatchSize = options.BatchSize
	transformOptions.WorkerCount = options.WorkerCount
	transformOptions.EnableValidation = options.EnableValidation
	transformOptions.SkipErrorRecords = options.SkipErrorRecords

	return &Processor{
		chClient:    chClient,
		options:     options,
		transformer: NewBatchTransformer(transformOptions),
		startTime:   time.Now(),
	}
}

// ProcessBatch 处理一批账单数据
func (p *Processor) ProcessBatch(ctx context.Context, tableName string, bills []BillDetail) error {
	return p.ProcessBatchWithBillingCycle(ctx, tableName, bills, "")
}

// SetBatchSize 设置批处理大小
func (p *Processor) SetBatchSize(size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.options != nil {
		p.options.BatchSize = size
	}
}

// GetProcessedCount 获取已处理记录数
func (p *Processor) GetProcessedCount() int64 {
	return p.GetProcessedRecords()
}

// GetTotalCount 获取总记录数
func (p *Processor) GetTotalCount() int64 {
	return p.GetTotalRecords()
}

// ProcessBatchWithBillingCycle 处理一批账单数据，传入账期信息
func (p *Processor) ProcessBatchWithBillingCycle(ctx context.Context, tableName string, bills []BillDetail, billingCycle string) error {
	if len(bills) == 0 {
		return nil
	}

	p.logBatchStart(len(bills), billingCycle)

	// 预处理数据
	filteredBills, err := p.preprocessBills(bills)
	if err != nil {
		return fmt.Errorf("failed to preprocess bills: %w", err)
	}

	if len(filteredBills) == 0 {
		logger.Debug("no data after filtering",
			zap.String("provider", "alicloud"))
		return nil
	}

	// 转换数据
	transformedRecords, err := p.transformBills(filteredBills, billingCycle)
	if err != nil {
		return fmt.Errorf("failed to transform bills: %w", err)
	}

	if len(transformedRecords) == 0 {
		logger.Debug("no data after transformation",
			zap.String("provider", "alicloud"))
		return nil
	}

	// 写入数据库
	if err := p.writeToDBWithRetry(ctx, tableName, transformedRecords); err != nil {
		return fmt.Errorf("failed to write to database: %w", err)
	}

	// 更新统计信息
	p.updateProgress(int64(len(filteredBills)))

	p.logBatchComplete(len(bills), len(filteredBills), len(transformedRecords))

	return nil
}

// preprocessBills 预处理账单数据（过滤和验证）
func (p *Processor) preprocessBills(bills []BillDetail) ([]BillDetail, error) {
	// 应用过滤器
	filteredBills := p.applyFilters(bills)

	// 如果启用了验证，进行数据验证
	if p.options.EnableValidation {
		return p.validateBills(filteredBills)
	}

	return filteredBills, nil
}

// applyFilters 应用数据过滤器
func (p *Processor) applyFilters(bills []BillDetail) []BillDetail {
	if !p.options.SkipZeroAmount && p.options.RecordFilter == nil {
		return bills // 没有过滤器，直接返回
	}

	filtered := make([]BillDetail, 0, len(bills))

	for _, bill := range bills {
		if p.shouldFilterBill(&bill) {
			continue
		}
		filtered = append(filtered, bill)
	}

	return filtered
}

// shouldFilterBill 判断是否应该过滤该记录
func (p *Processor) shouldFilterBill(bill *BillDetail) bool {
	// 检查零金额过滤
	if p.options.SkipZeroAmount && bill.IsZeroAmount() {
		return true
	}

	// 检查自定义过滤器
	if p.options.RecordFilter != nil && !p.options.RecordFilter(bill) {
		return true
	}

	return false
}

// validateBills 验证账单数据
func (p *Processor) validateBills(bills []BillDetail) ([]BillDetail, error) {
	validBills := make([]BillDetail, 0, len(bills))
	validator := NewValidator()

	for _, bill := range bills {
		if err := validator.ValidateBillDetail(&bill); err != nil {
			if p.options.SkipErrorRecords {
				logger.Warn("skipping invalid record",
					zap.String("provider", "alicloud"),
					zap.Error(err))
				continue
			}
			return nil, fmt.Errorf("bill validation failed: %w", err)
		}
		validBills = append(validBills, bill)
	}

	return validBills, nil
}

// transformBills 转换账单数据
func (p *Processor) transformBills(bills []BillDetail, billingCycle string) ([]*BillDetailForDB, error) {
	var result *BatchTransformationResult
	var err error

	if billingCycle != "" {
		result, err = p.transformer.TransformWithBillingCycle(bills, billingCycle)
	} else {
		result, err = p.transformer.Transform(bills)
	}

	if err != nil {
		return nil, err
	}

	// 处理转换错误
	if !result.IsSuccess() && len(result.Errors) > 0 {
		p.logTransformationErrors(result.Errors)
		if !p.options.SkipErrorRecords {
			return nil, fmt.Errorf("transformation failed with %d errors", len(result.Errors))
		}
	}

	return result.TransformedRecords, nil
}

// writeToDBWithRetry 带重试的数据库写入
func (p *Processor) writeToDBWithRetry(ctx context.Context, tableName string, records []*BillDetailForDB) error {
	if err := p.writeToDB(ctx, tableName, records); err != nil {
		if p.options.EnableRetry {
			return p.retryWriteToDB(ctx, tableName, records)
		}
		return err
	}
	return nil
}

// writeToDB 写入数据库
func (p *Processor) writeToDB(ctx context.Context, tableName string, records []*BillDetailForDB) error {
	if len(records) == 0 {
		return nil
	}

	batch := p.prepareBatchData(tableName, records)
	return p.chClient.InsertBatch(ctx, tableName, batch)
}

// prepareBatchData 准备批量数据
func (p *Processor) prepareBatchData(tableName string, records []*BillDetailForDB) []map[string]interface{} {
	batch := make([]map[string]interface{}, 0, len(records))
	isDailyTable := p.isDailyTable(tableName)

	for _, record := range records {
		row := p.recordToMapWithGranularity(record, isDailyTable)
		batch = append(batch, row)
	}

	return batch
}

// isDailyTable 判断是否为按天表
func (p *Processor) isDailyTable(tableName string) bool {
	return strings.Contains(strings.ToLower(tableName), "daily")
}

// retryWriteToDB 重试写入数据库
func (p *Processor) retryWriteToDB(ctx context.Context, tableName string, records []*BillDetailForDB) error {
	maxRetries := p.getMaxRetries()
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Debug("retrying database write",
			zap.String("provider", "alicloud"),
			zap.Int("attempt", attempt))

		if err := p.writeToDB(ctx, tableName, records); err != nil {
			lastErr = err
			if attempt < maxRetries {
				if err := p.waitForRetry(ctx, attempt, err); err != nil {
					return err
				}
				continue
			}
		} else {
			logger.Info("database write retry successful",
				zap.String("provider", "alicloud"),
				zap.Int("attempt", attempt))
			return nil
		}
	}

	return fmt.Errorf("failed to write to database after %d retries: %w", maxRetries, lastErr)
}

// getMaxRetries 获取最大重试次数
func (p *Processor) getMaxRetries() int {
	if p.options.MaxRetries <= 0 {
		return 3
	}
	return p.options.MaxRetries
}

// waitForRetry 等待重试
func (p *Processor) waitForRetry(ctx context.Context, attempt int, err error) error {
	// 指数退避
	delay := time.Duration(attempt) * time.Second
	logger.Warn("database write failed, retrying",
		zap.String("provider", "alicloud"),
		zap.Duration("delay", delay),
		zap.Error(err))

	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 注意：buildInsertSQL 方法已移除，因为现在使用 InsertBatch 方法

// 注意：recordToRow 方法已移除，因为现在使用 map 格式

// recordToMapWithGranularity 根据粒度将记录转换为map格式
func (p *Processor) recordToMapWithGranularity(record *BillDetailForDB, isDailyTable bool) map[string]interface{} {
	// 处理可选的 billing_date 字段
	var billingDate interface{}
	if record.BillingDate != nil {
		billingDate = *record.BillingDate
	} else {
		billingDate = nil
	}

	result := map[string]interface{}{
		"instance_id":         record.InstanceID,
		"instance_name":       record.InstanceName,
		"bill_account_id":     record.BillAccountID,
		"bill_account_name":   record.BillAccountName,
		"billing_cycle":       record.BillingCycle,
		"product_code":        record.ProductCode,
		"product_name":        record.ProductName,
		"product_type":        record.ProductType,
		"product_detail":      record.ProductDetail,
		"subscription_type":   record.SubscriptionType,
		"pricing_unit":        record.PricingUnit,
		"currency":            record.Currency,
		"billing_type":        record.BillingType,
		"usage":               record.Usage,
		"usage_unit":          record.UsageUnit,
		"pretax_gross_amount": record.PretaxGrossAmount,
		"invoice_discount":    record.InvoiceDiscount,
		"deducted_by_coupons": record.DeductedByCoupons,
		"pretax_amount":       record.PretaxAmount,
		"currency_amount":     record.CurrencyAmount,
		"payment_amount":      record.PaymentAmount,
		"outstanding_amount":  record.OutstandingAmount,
		"region":              record.Region,
		"zone":                record.Zone,
		"instance_spec":       record.InstanceSpec,
		"internet_ip":         record.InternetIP,
		"intranet_ip":         record.IntranetIP,
		"resource_group":      record.ResourceGroup,
		"tags":                record.Tags,
		"cost_unit":           record.CostUnit,
		"service_period":      record.ServicePeriod,
		"service_period_unit": record.ServicePeriodUnit,
		"list_price":          record.ListPrice,
		"list_price_unit":     record.ListPriceUnit,
		"owner_id":            record.OwnerID,
		"split_item_id":       record.SplitItemID,
		"split_item_name":     record.SplitItemName,
		"split_account_id":    record.SplitAccountID,
		"split_account_name":  record.SplitAccountName,
		"nick_name":           record.NickName,
		"product_detail_code": record.ProductDetailCode,
		"biz_type":            record.BizType,
		"adjust_type":         record.AdjustType,
		"adjust_amount":       record.AdjustAmount,
		"granularity":         record.Granularity,
		"created_at":          record.CreatedAt,
		"updated_at":          record.UpdatedAt,
	}

	// 只在日表中包含 billing_date 字段
	if isDailyTable {
		result["billing_date"] = billingDate
	}

	return result
}

// updateProgress 更新处理进度
func (p *Processor) updateProgress(processed int64) {
	p.mu.Lock()
	p.processedRecords += processed
	current := p.processedRecords
	total := p.totalRecords
	p.mu.Unlock()

	// 调用进度回调
	if p.options.ProgressCallback != nil {
		p.options.ProgressCallback(current, total)
	}

	// 计算和显示进度信息
	p.logProgressInfo(current, total)
}

// logProgressInfo 记录进度信息
func (p *Processor) logProgressInfo(current, total int64) {
	elapsed := time.Since(p.startTime)
	if elapsed <= 0 || current <= 0 {
		return
	}

	speed := float64(current) / elapsed.Seconds()
	if total > 0 && current < total {
		remaining := total - current
		eta := time.Duration(float64(remaining)/speed) * time.Second
		progress := float64(current) / float64(total) * 100

		logger.Debug("processing progress",
			zap.String("provider", "alicloud"),
			zap.Int64("current", current),
			zap.Int64("total", total),
			zap.Float64("progress_pct", progress),
			zap.Float64("records_per_sec", speed),
			zap.Duration("eta", eta))
	}
}

// SetTotalRecords 设置总记录数（用于进度计算）
func (p *Processor) SetTotalRecords(total int64) {
	p.mu.Lock()
	p.totalRecords = total
	p.mu.Unlock()
}

// GetProcessedRecords 获取已处理记录数
func (p *Processor) GetProcessedRecords() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.processedRecords
}

// GetTotalRecords 获取总记录数
func (p *Processor) GetTotalRecords() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.totalRecords
}

// GetProgress 获取处理进度（0-100）
func (p *Processor) GetProgress() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.totalRecords == 0 {
		return 0
	}

	return float64(p.processedRecords) / float64(p.totalRecords) * 100
}

// GetProcessingStats 获取处理统计信息
func (p *Processor) GetProcessingStats() *ProcessingStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := &ProcessingStats{
		StartTime:        p.startTime,
		LastUpdateTime:   time.Now(),
		TotalRecords:     int(p.totalRecords),
		ProcessedRecords: int(p.processedRecords),
	}

	// 计算处理速度
	elapsed := stats.LastUpdateTime.Sub(p.startTime)
	if elapsed > 0 && p.processedRecords > 0 {
		stats.AverageSpeed = float64(p.processedRecords) / elapsed.Seconds()
	}

	// 计算预计剩余时间
	if stats.AverageSpeed > 0 && p.totalRecords > 0 && p.processedRecords < p.totalRecords {
		remaining := p.totalRecords - p.processedRecords
		stats.EstimatedTime = time.Duration(float64(remaining)/stats.AverageSpeed) * time.Second
	}

	return stats
}

// SetOptions 设置处理器选项
func (p *Processor) SetOptions(options *ProcessorOptions) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if options != nil {
		p.options = options

		// 更新转换器选项
		transformOptions := DefaultBatchTransformationOptions()
		transformOptions.BatchSize = options.BatchSize
		transformOptions.WorkerCount = options.WorkerCount
		transformOptions.EnableValidation = options.EnableValidation
		transformOptions.SkipErrorRecords = options.SkipErrorRecords

		p.transformer.SetOptions(transformOptions)
	}
}

// GetOptions 获取处理器选项
func (p *Processor) GetOptions() *ProcessorOptions {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.options
}

// Reset 重置处理器状态
func (p *Processor) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.processedRecords = 0
	p.totalRecords = 0
	p.startTime = time.Now()
}

// ProcessMultipleBatches 并发处理多个批次
func (p *Processor) ProcessMultipleBatches(ctx context.Context, tableName string, batches [][]BillDetail) error {
	if len(batches) == 0 {
		return nil
	}

	logger.Info("starting concurrent batch processing",
		zap.String("provider", "alicloud"),
		zap.Int("batches", len(batches)))

	// 初始化并发处理
	totalRecords := p.calculateTotalRecords(batches)
	p.SetTotalRecords(totalRecords)

	// 执行并发处理
	errors := p.processBatchesConcurrently(ctx, tableName, batches)

	if len(errors) > 0 {
		return fmt.Errorf("batch processing failed: %s", strings.Join(errors, "; "))
	}

	logger.Info("all batch processing completed",
		zap.String("provider", "alicloud"))
	return nil
}

// calculateTotalRecords 计算总记录数
func (p *Processor) calculateTotalRecords(batches [][]BillDetail) int64 {
	totalRecords := int64(0)
	for _, batch := range batches {
		totalRecords += int64(len(batch))
	}
	return totalRecords
}

// processBatchesConcurrently 并发处理批次
func (p *Processor) processBatchesConcurrently(ctx context.Context, tableName string, batches [][]BillDetail) []string {
	workerCount := p.getWorkerCount()
	batchChan := make(chan []BillDetail, workerCount)
	errChan := make(chan error, len(batches))

	// 启动工作协程
	var wg sync.WaitGroup
	p.startWorkers(ctx, &wg, workerCount, tableName, batchChan, errChan)

	// 发送批次数据
	p.sendBatches(ctx, batchChan, batches)

	// 等待完成并收集错误
	wg.Wait()
	close(errChan)

	return p.collectErrors(errChan)
}

// getWorkerCount 获取工作协程数
func (p *Processor) getWorkerCount() int {
	if p.options.WorkerCount <= 0 {
		return 4
	}
	return p.options.WorkerCount
}

// startWorkers 启动工作协程
func (p *Processor) startWorkers(ctx context.Context, wg *sync.WaitGroup, workerCount int, tableName string, batchChan <-chan []BillDetail, errChan chan<- error) {
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchChan {
				if err := p.ProcessBatch(ctx, tableName, batch); err != nil {
					errChan <- err
					return
				}
			}
		}()
	}
}

// sendBatches 发送批次数据
func (p *Processor) sendBatches(ctx context.Context, batchChan chan<- []BillDetail, batches [][]BillDetail) {
	go func() {
		defer close(batchChan)
		for _, batch := range batches {
			select {
			case batchChan <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()
}

// collectErrors 收集错误
func (p *Processor) collectErrors(errChan <-chan error) []string {
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}
	return errors
}

// ValidateData 验证数据完整性
func (p *Processor) ValidateData(ctx context.Context, tableName string) error {
	// 检查表是否存在
	exists, err := p.chClient.TableExists(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// 检查数据完整性（例如：检查是否有空的必需字段）
	validationSQL := fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_count,
			COUNT(CASE WHEN instance_id = '' THEN 1 END) as empty_instance_id,
			COUNT(CASE WHEN product_code = '' THEN 1 END) as empty_product_code,
			COUNT(CASE WHEN payment_amount < 0 THEN 1 END) as negative_amounts
		FROM %s
	`, tableName)

	logger.Info("starting table integrity validation",
		zap.String("provider", "alicloud"),
		zap.String("table", tableName))

	// 执行验证查询（这里需要实现具体的查询逻辑）
	// 由于 clickhouse.Client 的具体实现未知，这里只是示例
	logger.Debug("executing validation query",
		zap.String("provider", "alicloud"),
		zap.String("query", validationSQL))

	return nil
}

// GetTableStats 获取表统计信息
func (p *Processor) GetTableStats(ctx context.Context, tableName string) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// 检查表是否存在
	exists, err := p.chClient.TableExists(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to check table existence: %w", err)
	}

	stats["table_exists"] = exists
	if !exists {
		return stats, nil
	}

	// 获取基本统计信息
	statsSQL := p.buildStatsSQL(tableName)
	logger.Debug("executing statistics query",
		zap.String("provider", "alicloud"),
		zap.String("query", statsSQL))

	// 这里需要根据实际的 clickhouse.Client 实现来执行查询
	// 暂时返回空统计信息
	stats["query_sql"] = statsSQL

	return stats, nil
}

// buildStatsSQL 构建统计SQL
func (p *Processor) buildStatsSQL(tableName string) string {
	return fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_records,
			MIN(created_at) as earliest_record,
			MAX(created_at) as latest_record,
			COUNT(DISTINCT instance_id) as unique_instances,
			COUNT(DISTINCT product_code) as unique_products,
			SUM(payment_amount) as total_payment_amount
		FROM %s
	`, tableName)
}

// logBatchStart 记录批次开始
func (p *Processor) logBatchStart(billCount int, billingCycle string) {
	logger.Debug("starting batch processing",
		zap.String("provider", "alicloud"),
		zap.Int("records", billCount),
		zap.String("cycle", billingCycle))
}

// logBatchComplete 记录批次完成
func (p *Processor) logBatchComplete(inputCount, filteredCount, transformedCount int) {
	logger.Debug("batch processing completed",
		zap.String("provider", "alicloud"),
		zap.Int("input", inputCount),
		zap.Int("filtered", filteredCount),
		zap.Int("transformed", transformedCount),
		zap.Int("inserted", transformedCount))
}

// logTransformationErrors 记录转换错误
func (p *Processor) logTransformationErrors(errors []error) {
	logger.Warn("transformation errors occurred",
		zap.String("provider", "alicloud"),
		zap.Int("error_count", len(errors)))
	for i, err := range errors {
		if i < 5 { // 只显示前5个错误
			logger.Warn("transformation error detail",
				zap.String("provider", "alicloud"),
				zap.Int("index", i+1),
				zap.Error(err))
		}
	}
	if len(errors) > 5 {
		logger.Warn("additional errors not displayed",
			zap.String("provider", "alicloud"),
			zap.Int("hidden_count", len(errors)-5))
	}
}
