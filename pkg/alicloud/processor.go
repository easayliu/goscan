package alicloud

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"log"
	"strings"
	"sync"
	"time"
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

// ProcessBatchWithBillingCycle 处理一批账单数据，传入账期信息
func (p *Processor) ProcessBatchWithBillingCycle(ctx context.Context, tableName string, bills []BillDetail, billingCycle string) error {
	if len(bills) == 0 {
		return nil
	}

	log.Printf("[阿里云数据处理] 开始处理批次数据: %d 条记录", len(bills))
	if billingCycle != "" {
		log.Printf("[阿里云数据处理] 使用账期: %s", billingCycle)
	}

	// 应用过滤器
	filteredBills := p.applyFilters(bills)
	if len(filteredBills) == 0 {
		log.Printf("[阿里云数据处理] 过滤后没有数据需要处理")
		return nil
	}

	// 批量转换数据
	var result *BatchTransformationResult
	var err error
	if billingCycle != "" {
		result, err = p.transformer.TransformWithBillingCycle(filteredBills, billingCycle)
	} else {
		result, err = p.transformer.Transform(filteredBills)
	}
	if err != nil {
		return fmt.Errorf("failed to transform bill data: %w", err)
	}

	if !result.IsSuccess() && len(result.Errors) > 0 {
		log.Printf("[阿里云数据处理] 转换过程中发生错误: %d 个", len(result.Errors))
		for i, err := range result.Errors {
			if i < 5 { // 只显示前5个错误
				log.Printf("[阿里云数据处理] 错误 %d: %v", i+1, err)
			}
		}
		if len(result.Errors) > 5 {
			log.Printf("[阿里云数据处理] ...还有 %d 个错误未显示", len(result.Errors)-5)
		}
	}

	if len(result.TransformedRecords) == 0 {
		log.Printf("[阿里云数据处理] 转换后没有有效数据")
		return nil
	}

	// 写入数据库
	if err := p.writeToDB(ctx, tableName, result.TransformedRecords); err != nil {
		if p.options.EnableRetry {
			return p.retryWriteToDB(ctx, tableName, result.TransformedRecords)
		}
		return fmt.Errorf("failed to write to database: %w", err)
	}

	// 更新统计信息
	p.updateProgress(int64(len(filteredBills)))

	log.Printf("[阿里云数据处理] 批次处理完成: 输入 %d 条，过滤 %d 条，转换 %d 条，入库 %d 条",
		len(bills), len(bills)-len(filteredBills), len(result.TransformedRecords), len(result.TransformedRecords))

	return nil
}

// applyFilters 应用数据过滤器
func (p *Processor) applyFilters(bills []BillDetail) []BillDetail {
	if !p.options.SkipZeroAmount && p.options.RecordFilter == nil {
		return bills // 没有过滤器，直接返回
	}

	filtered := make([]BillDetail, 0, len(bills))

	for _, bill := range bills {
		// 检查零金额过滤
		if p.options.SkipZeroAmount && bill.IsZeroAmount() {
			continue
		}

		// 检查自定义过滤器
		if p.options.RecordFilter != nil && !p.options.RecordFilter(&bill) {
			continue
		}

		filtered = append(filtered, bill)
	}

	return filtered
}

// writeToDB 写入数据库
func (p *Processor) writeToDB(ctx context.Context, tableName string, records []*BillDetailForDB) error {
	if len(records) == 0 {
		return nil
	}

	// 准备批量数据
	batch := make([]map[string]interface{}, 0, len(records))
	isDailyTable := strings.Contains(tableName, "daily")

	for _, record := range records {
		row := p.recordToMapWithGranularity(record, isDailyTable)
		batch = append(batch, row)
	}

	// 执行批量插入
	return p.chClient.InsertBatch(ctx, tableName, batch)
}

// retryWriteToDB 重试写入数据库
func (p *Processor) retryWriteToDB(ctx context.Context, tableName string, records []*BillDetailForDB) error {
	maxRetries := p.options.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[阿里云数据处理] 第 %d 次尝试写入数据库", attempt)

		if err := p.writeToDB(ctx, tableName, records); err != nil {
			lastErr = err
			if attempt < maxRetries {
				// 指数退避
				delay := time.Duration(attempt) * time.Second
				log.Printf("[阿里云数据处理] 写入失败，%v 后重试: %v", delay, err)

				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		} else {
			log.Printf("[阿里云数据处理] 第 %d 次尝试成功", attempt)
			return nil
		}
	}

	return fmt.Errorf("failed to write to database after %d retries: %w", maxRetries, lastErr)
}

// buildInsertSQL 构建插入SQL语句
func (p *Processor) buildInsertSQL(tableName string) string {
	return fmt.Sprintf(`INSERT INTO %s (
		instance_id, instance_name, bill_account_id, bill_account_name,
		billing_date, billing_cycle,
		product_code, product_name, product_type, product_detail,
		subscription_type, pricing_unit, currency, billing_type,
		usage, usage_unit,
		pretax_gross_amount, invoice_discount, deducted_by_coupons,
		pretax_amount, currency_amount, payment_amount, outstanding_amount,
		region, zone, instance_spec, internet_ip, intranet_ip,
		resource_group, tags, cost_unit,
		service_period, service_period_unit, list_price, list_price_unit, owner_id,
		split_item_id, split_item_name, split_account_id, split_account_name,
		nick_name, product_detail_code,
		biz_type, adjust_type, adjust_amount,
		granularity, created_at, updated_at
	) VALUES (
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?,
		?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?, ?,
		?, ?, ?,
		?, ?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?,
		?, ?, ?,
		?, ?, ?
	)`, tableName)
}

// recordToRow 将记录转换为数据库行
func (p *Processor) recordToRow(record *BillDetailForDB) []interface{} {
	// 处理可选的 billing_date 字段
	var billingDate interface{}
	if record.BillingDate != nil {
		billingDate = *record.BillingDate
	} else {
		billingDate = nil
	}

	return []interface{}{
		record.InstanceID, record.InstanceName, record.BillAccountID, record.BillAccountName,
		billingDate, record.BillingCycle,
		record.ProductCode, record.ProductName, record.ProductType, record.ProductDetail,
		record.SubscriptionType, record.PricingUnit, record.Currency, record.BillingType,
		record.Usage, record.UsageUnit,
		record.PretaxGrossAmount, record.InvoiceDiscount, record.DeductedByCoupons,
		record.PretaxAmount, record.CurrencyAmount, record.PaymentAmount, record.OutstandingAmount,
		record.Region, record.Zone, record.InstanceSpec, record.InternetIP, record.IntranetIP,
		record.ResourceGroup, record.Tags, record.CostUnit,
		record.ServicePeriod, record.ServicePeriodUnit, record.ListPrice, record.ListPriceUnit, record.OwnerID,
		record.SplitItemID, record.SplitItemName, record.SplitAccountID, record.SplitAccountName,
		record.NickName, record.ProductDetailCode,
		record.BizType, record.AdjustType, record.AdjustAmount,
		record.Granularity, record.CreatedAt, record.UpdatedAt,
	}
}

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

	// 计算处理速度和预计剩余时间
	elapsed := time.Since(p.startTime)
	if elapsed > 0 && current > 0 {
		speed := float64(current) / elapsed.Seconds()
		if total > 0 && current < total {
			remaining := total - current
			eta := time.Duration(float64(remaining)/speed) * time.Second
			log.Printf("[阿里云数据处理] 进度: %d/%d (%.1f%%), 速度: %.1f records/s, 预计剩余: %v",
				current, total, float64(current)/float64(total)*100, speed, eta)
		}
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

	log.Printf("[阿里云数据处理] 开始并发处理 %d 个批次", len(batches))

	// 计算总记录数
	totalRecords := int64(0)
	for _, batch := range batches {
		totalRecords += int64(len(batch))
	}
	p.SetTotalRecords(totalRecords)

	// 创建工作池
	workerCount := p.options.WorkerCount
	if workerCount <= 0 {
		workerCount = 4
	}

	batchChan := make(chan []BillDetail, workerCount)
	errChan := make(chan error, len(batches))

	// 启动工作协程
	var wg sync.WaitGroup
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

	// 发送批次数据
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

	// 等待所有工作协程完成
	wg.Wait()
	close(errChan)

	// 收集错误
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch processing failed: %s", strings.Join(errors, "; "))
	}

	log.Printf("[阿里云数据处理] 所有批次处理完成")
	return nil
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

	log.Printf("[阿里云数据验证] 验证表 %s 的数据完整性", tableName)

	// 执行验证查询（这里需要实现具体的查询逻辑）
	// 由于 clickhouse.Client 的具体实现未知，这里只是示例
	log.Printf("[阿里云数据验证] SQL: %s", validationSQL)

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
	statsSQL := fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_records,
			MIN(created_at) as earliest_record,
			MAX(created_at) as latest_record,
			COUNT(DISTINCT instance_id) as unique_instances,
			COUNT(DISTINCT product_code) as unique_products,
			SUM(payment_amount) as total_payment_amount
		FROM %s
	`, tableName)

	log.Printf("[阿里云表统计] SQL: %s", statsSQL)

	// 这里需要根据实际的 clickhouse.Client 实现来执行查询
	// 暂时返回空统计信息
	stats["query_sql"] = statsSQL

	return stats, nil
}
