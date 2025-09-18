package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/logger"
	"time"

	"go.uber.org/zap"
)

// ProcessorOptions 处理器选项
type ProcessorOptions struct {
	BatchSize     int           `json:"batch_size"`      // 批次大小
	MaxRetries    int           `json:"max_retries"`     // 最大重试次数
	RetryDelay    time.Duration `json:"retry_delay"`     // 重试延迟
	EnableAsync   bool          `json:"enable_async"`    // 启用异步插入
	ProgressLog   bool          `json:"progress_log"`    // Whether to emit progress logs
	DryRunCleanup bool          `json:"dry_run_cleanup"` // 干预式清理预览
	Timeout       time.Duration `json:"timeout"`         // 超时时间
}

// ProcessResult 处理结果
type ProcessResult struct {
	TotalRecords    int                           `json:"total_records"`
	InsertedRecords int                           `json:"inserted_records"`
	FailedRecords   int                           `json:"failed_records"`
	Errors          []error                       `json:"errors"`
	Duration        time.Duration                 `json:"duration"`
	AverageSpeed    float64                       `json:"average_speed"` // 记录/秒
	CleanupResult   *clickhouse.CleanupResult     `json:"cleanup_result,omitempty"`
	BatchResult     *clickhouse.BatchInsertResult `json:"batch_result,omitempty"`
}

// DefaultProcessorOptions 创建默认处理器选项
func DefaultProcessorOptions() *ProcessorOptions {
	return &ProcessorOptions{
		BatchSize:     500,
		MaxRetries:    3,
		RetryDelay:    2 * time.Second,
		EnableAsync:   false,
		ProgressLog:   true,
		DryRunCleanup: false,
		Timeout:       30 * time.Minute,
	}
}

// ClickHouseProcessor ClickHouse数据处理器
type ClickHouseProcessor struct {
	client            *clickhouse.Client
	tableName         string
	isDistributed     bool
	options           *ProcessorOptions
	cleanBeforeInsert bool
	cleanCondition    string
	cleanArgs         []interface{}
}

// NewClickHouseProcessor 创建ClickHouse处理器
func NewClickHouseProcessor(client *clickhouse.Client, tableName string, isDistributed bool) *ClickHouseProcessor {
	return NewClickHouseProcessorWithOptions(client, tableName, isDistributed, DefaultProcessorOptions())
}

// NewClickHouseProcessorWithOptions 使用选项创建ClickHouse处理器
func NewClickHouseProcessorWithOptions(client *clickhouse.Client, tableName string, isDistributed bool, options *ProcessorOptions) *ClickHouseProcessor {
	if options == nil {
		options = DefaultProcessorOptions()
	}

	return &ClickHouseProcessor{
		client:        client,
		tableName:     tableName,
		isDistributed: isDistributed,
		options:       options,
	}
}

// SetCleanup 设置清理条件
func (p *ClickHouseProcessor) SetCleanup(condition string, args ...interface{}) {
	p.SetCleanupWithDryRun(condition, false, args...)
}

// SetCleanupWithDryRun 设置清理条件（支持干运行）
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

// Process 实现 DataProcessor 接口
func (p *ClickHouseProcessor) Process(ctx context.Context, data []BillDetail) error {
	_, err := p.ProcessWithResult(ctx, data)
	return err
}

// ProcessWithResult 处理数据并返回详细结果
func (p *ClickHouseProcessor) ProcessWithResult(ctx context.Context, data []BillDetail) (*ProcessResult, error) {
	result := &ProcessResult{
		TotalRecords: len(data),
		Errors:       make([]error, 0),
	}

	startTime := time.Now()
	defer func() {
		result.Duration = time.Since(startTime)
		if result.Duration > 0 {
			result.AverageSpeed = float64(result.InsertedRecords) / result.Duration.Seconds()
		}
	}()

	p.logProgress("Processing started",
		zap.Int("records", len(data)),
		zap.String("table", p.tableName))

	// 执行数据清理 - 即使数据为空也需要执行清理操作
	if err := p.performCleanup(ctx, result); err != nil {
		return result, err
	}

	// 如果数据为空且已完成清理，直接返回
	if len(data) == 0 {
		p.logProgress("Processing completed with cleanup only",
			zap.String("table", p.tableName),
			zap.Bool("cleanup_performed", p.cleanBeforeInsert))
		return result, nil
	}

	// 如果是预览模式，直接返回
	if p.options.DryRunCleanup {
		return result, nil
	}

	// 转换和插入数据
	if err := p.processDataInsert(ctx, data, result); err != nil {
		return result, err
	}

	p.logProcessResult(result)
	return result, nil
}

// performCleanup 执行数据清理
func (p *ClickHouseProcessor) performCleanup(ctx context.Context, result *ProcessResult) error {
	if !p.cleanBeforeInsert {
		return nil
	}

	cleanupOpts := &clickhouse.CleanupOptions{
		Condition:   p.cleanCondition,
		Args:        p.cleanArgs,
		DryRun:      p.options.DryRunCleanup,
		ProgressLog: p.options.ProgressLog,
	}

	action := "cleanup"
	if p.options.DryRunCleanup {
		action = "cleanup preview"
	}
	p.logProgress("Preparing cleanup",
		zap.String("action", action),
		zap.String("table", p.tableName),
		zap.String("condition", p.cleanCondition))

	cleanupResult, err := p.client.EnhancedCleanTableData(ctx, p.tableName, cleanupOpts)
	result.CleanupResult = cleanupResult

	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("cleanup failed: %w", err))
		return err
	}

	p.logProgress("Cleanup finished",
		zap.String("table", p.tableName),
		zap.String("summary", cleanupResult.String()))

	// 清理完成后重置标志
	if !p.options.DryRunCleanup {
		p.cleanBeforeInsert = false
	}

	return nil
}

// processDataInsert 处理数据插入
func (p *ClickHouseProcessor) processDataInsert(ctx context.Context, data []BillDetail, result *ProcessResult) error {
	// 转换数据格式
	records := p.convertBillsToRecords(data)

	// 批量插入
	return p.performBatchInsert(ctx, records, result)
}

// convertBillsToRecords 批量转换账单数据
func (p *ClickHouseProcessor) convertBillsToRecords(data []BillDetail) []map[string]interface{} {
	p.logProgress("Conversion started",
		zap.Int("records", len(data)),
		zap.String("table", p.tableName))

	records := make([]map[string]interface{}, 0, len(data))
	for i, bill := range data {
		if i > 0 && i%100 == 0 {
			p.logProgress("Conversion progress",
				zap.Int("processed", i),
				zap.Int("total", len(data)),
				zap.String("table", p.tableName))
		}
		records = append(records, bill.ToDBMap())
	}

	p.logProgress("Conversion completed",
		zap.Int("records", len(records)),
		zap.String("table", p.tableName))
	return records
}

// performBatchInsert 执行批量插入
func (p *ClickHouseProcessor) performBatchInsert(ctx context.Context, records []map[string]interface{}, result *ProcessResult) error {
	// Retrieve resolver info for debugging
	resolverInfo := p.client.GetTableNameResolver().GetTableInfo(p.tableName)
	p.logProgress("Preparing batch insert",
		zap.String("table", p.tableName),
		zap.Bool("distributed", p.isDistributed),
		zap.Any("resolver_info", resolverInfo))

	// Check whether the target table exists
	targetTable := p.tableName
	if p.isDistributed {
		targetTable = p.client.GetTableNameResolver().ResolveInsertTarget(p.tableName)
	}

	exists, err := p.client.TableExists(ctx, targetTable)
	if err != nil {
		p.logProgress("Table existence check failed",
			zap.String("table", targetTable),
			zap.Error(err))
		// Do not return immediately; attempt insert so ClickHouse returns a concrete error
	} else if !exists {
		p.logProgress("Target table missing",
			zap.String("table", targetTable))
	}

	batchOpts := &clickhouse.BatchInsertOptions{
		BatchSize:   p.options.BatchSize,
		MaxRetries:  p.options.MaxRetries,
		RetryDelay:  p.options.RetryDelay,
		EnableAsync: p.options.EnableAsync,
		Timeout:     p.options.Timeout,
	}

	var batchResult *clickhouse.BatchInsertResult

	if p.isDistributed {
		p.logProgress("Batch insert started",
			zap.String("table", targetTable),
			zap.String("mode", "distributed"))
		batchResult, err = p.client.OptimizedBatchInsertToDistributed(ctx, p.tableName, records, batchOpts)
	} else {
		p.logProgress("Batch insert started",
			zap.String("table", targetTable),
			zap.String("mode", "local"))
		batchResult, err = p.client.OptimizedBatchInsert(ctx, p.tableName, records, batchOpts)
	}

	result.BatchResult = batchResult

	if err != nil {
		p.logProgress("Batch insert failed",
			zap.String("table", targetTable),
			zap.Error(err))
		result.Errors = append(result.Errors, fmt.Errorf("batch insert failed: %w", err))
		return err
	}

	// 更新结果统计
	result.InsertedRecords = batchResult.InsertedRecords
	result.FailedRecords = batchResult.FailedRecords
	if len(batchResult.Errors) > 0 {
		result.Errors = append(result.Errors, batchResult.Errors...)
	}

	p.logProgress("Batch insert completed",
		zap.String("table", targetTable),
		zap.Int("inserted_records", batchResult.InsertedRecords),
		zap.Int("failed_records", batchResult.FailedRecords))
	return nil
}

// logProgress writes progress logs following the standard structured style
func (p *ClickHouseProcessor) logProgress(message string, fields ...zap.Field) {
	if !p.options.ProgressLog {
		return
	}

	base := []zap.Field{zap.String("provider", "volcengine")}
	base = append(base, fields...)
	logger.Info(message, base...)
}

// logProcessResult logs the processing summary
func (p *ClickHouseProcessor) logProcessResult(result *ProcessResult) {
	if p.options.ProgressLog {
		logger.Info("Volcengine data processing completed",
			zap.String("provider", "volcengine"),
			zap.String("result", result.String()))
	}
}

// String implements fmt.Stringer for structured summaries
func (r *ProcessResult) String() string {
	successRate := r.GetSuccessRate()
	status := "success"
	if !r.IsSuccess() {
		status = "partial failure"
	}

	return fmt.Sprintf("status=%s, total=%d, succeeded=%d, failed=%d, success_rate=%.1f%%, avg_speed=%.1f records/s, duration=%v",
		status, r.TotalRecords, r.InsertedRecords, r.FailedRecords, successRate, r.AverageSpeed, r.Duration)
}

// IsSuccess 判断处理是否成功
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

// 以下是链式方法，提供更友好的API

// ExecuteCleanupOnly 仅执行数据清理操作，不进行数据插入
func (p *ClickHouseProcessor) ExecuteCleanupOnly(ctx context.Context) (*ProcessResult, error) {
	result := &ProcessResult{
		TotalRecords: 0,
		Errors:       make([]error, 0),
	}

	startTime := time.Now()
	defer func() {
		result.Duration = time.Since(startTime)
	}()

	p.logProgress("Executing cleanup only",
		zap.String("table", p.tableName),
		zap.String("condition", p.cleanCondition))

	// 执行清理操作
	if err := p.performCleanup(ctx, result); err != nil {
		return result, err
	}

	p.logProgress("Cleanup only completed",
		zap.String("table", p.tableName),
		zap.Bool("cleanup_performed", p.cleanBeforeInsert))

	return result, nil
}

// WithCleanupPreview 设置清理预览
func (p *ClickHouseProcessor) WithCleanupPreview(condition string, args ...interface{}) *ClickHouseProcessor {
	p.SetCleanupWithDryRun(condition, true, args...)
	return p
}

// WithBatchSize 设置批次大小
func (p *ClickHouseProcessor) WithBatchSize(size int) *ClickHouseProcessor {
	p.SetBatchSize(size)
	return p
}

// WithAsyncInsert 设置异步插入
func (p *ClickHouseProcessor) WithAsyncInsert(enabled bool) *ClickHouseProcessor {
	p.options.EnableAsync = enabled
	return p
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

// BatchProcessor 批处理器，可以组合多个处理器
type BatchProcessor struct {
	processors []DataProcessor
}

// NewBatchProcessor 创建批处理器
func NewBatchProcessor(processors ...DataProcessor) *BatchProcessor {
	return &BatchProcessor{processors: processors}
}

// Process 实现 DataProcessor 接口
func (bp *BatchProcessor) Process(ctx context.Context, data []BillDetail) error {
	for _, processor := range bp.processors {
		if err := processor.Process(ctx, data); err != nil {
			return err
		}
	}
	return nil
}

// ProcessWithResult 处理数据并返回结果
func (bp *BatchProcessor) ProcessWithResult(ctx context.Context, data []BillDetail) (*ProcessResult, error) {
	combinedResult := &ProcessResult{
		TotalRecords: len(data),
		Errors:       make([]error, 0),
	}

	startTime := time.Now()
	defer func() {
		combinedResult.Duration = time.Since(startTime)
	}()

	for i, processor := range bp.processors {
		logger.Info("Volcengine batch processor executing processor",
			zap.String("provider", "volcengine"),
			zap.Int("current", i+1),
			zap.Int("total", len(bp.processors)))

		if chProcessor, ok := processor.(*ClickHouseProcessor); ok {
			result, err := chProcessor.ProcessWithResult(ctx, data)
			if err != nil {
				combinedResult.Errors = append(combinedResult.Errors, err)
				combinedResult.FailedRecords += result.FailedRecords
			} else {
				combinedResult.InsertedRecords += result.InsertedRecords
			}
		} else {
			if err := processor.Process(ctx, data); err != nil {
				combinedResult.Errors = append(combinedResult.Errors, err)
				combinedResult.FailedRecords += len(data)
			} else {
				combinedResult.InsertedRecords += len(data)
			}
		}
	}

	return combinedResult, nil
}

// SetBatchSize 设置批次大小
func (bp *BatchProcessor) SetBatchSize(size int) {
	for _, processor := range bp.processors {
		if chProcessor, ok := processor.(*ClickHouseProcessor); ok {
			chProcessor.SetBatchSize(size)
		}
	}
}
