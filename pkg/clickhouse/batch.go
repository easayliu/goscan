package clickhouse

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// BatchInsertOptions 批量插入选项
type BatchInsertOptions struct {
	BatchSize   int           // 每批次大小
	MaxRetries  int           // 最大重试次数
	RetryDelay  time.Duration // 重试延迟
	EnableAsync bool          // 是否启用异步插入
	Timeout     time.Duration // 超时时间
}

// DefaultBatchInsertOptions 返回默认的批量插入选项
func DefaultBatchInsertOptions() *BatchInsertOptions {
	return &BatchInsertOptions{
		BatchSize:   500,
		MaxRetries:  3,
		RetryDelay:  2 * time.Second,
		EnableAsync: false,
		Timeout:     30 * time.Second,
	}
}

// BatchInsertResult 批量插入结果
type BatchInsertResult struct {
	TotalRecords     int           `json:"total_records"`
	ProcessedBatches int           `json:"processed_batches"`
	FailedBatches    int           `json:"failed_batches"`
	InsertedRecords  int           `json:"inserted_records"`
	FailedRecords    int           `json:"failed_records"`
	Duration         time.Duration `json:"duration"`
	AverageSpeed     float64       `json:"average_speed"` // records per second
	Errors           []error       `json:"errors,omitempty"`
}

// batchManager 批量操作管理器
type batchManager struct {
	conn         driver.Conn
	nameResolver *TableNameResolver
}

// NewBatchManager 创建批量操作管理器
func NewBatchManager(conn driver.Conn, nameResolver *TableNameResolver) BatchManager {
	return &batchManager{
		conn:         conn,
		nameResolver: nameResolver,
	}
}

// PrepareBatch 准备批量操作
func (bm *batchManager) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	if bm.conn == nil {
		return nil, WrapConnectionError(fmt.Errorf("connection is nil"))
	}
	
	batch, err := bm.conn.PrepareBatch(ctx, query)
	if err != nil {
		return nil, WrapError("prepare batch", "", err)
	}
	return batch, nil
}

// InsertBatch 批量插入数据
func (bm *batchManager) InsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return ErrEmptyData
	}

	// 自动解析表名
	resolvedTableName := bm.nameResolver.ResolveInsertTarget(tableName)

	columns := ExtractColumnsFromData(data)
	if len(columns) == 0 {
		return WrapError("insert batch", tableName, fmt.Errorf("no columns found in data"))
	}

	query := BuildInsertQuery(resolvedTableName, columns)
	
	batch, err := bm.PrepareBatch(ctx, query)
	if err != nil {
		return WrapError("prepare batch", tableName, err)
	}

	for _, row := range data {
		values := PrepareValues(row, columns)
		if err := batch.Append(values...); err != nil {
			return WrapError("append to batch", tableName, err)
		}
	}

	if err := batch.Send(); err != nil {
		return WrapError("send batch", tableName, err)
	}

	return nil
}

// AsyncInsertBatch 异步批量插入数据
func (bm *batchManager) AsyncInsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return ErrEmptyData
	}

	// 启用异步插入
	ctxWithSettings := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":                 1,
		"wait_for_async_insert":        1,
		"async_insert_max_data_size":   10485760, // 10MB
		"async_insert_busy_timeout_ms": 200,
	}))

	return bm.InsertBatch(ctxWithSettings, tableName, data)
}

// OptimizedBatchInsert 优化的批量插入方法，支持分块、重试、性能监控
func (bm *batchManager) OptimizedBatchInsert(ctx context.Context, tableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error) {
	if opts == nil {
		opts = DefaultBatchInsertOptions()
	}

	result := &BatchInsertResult{
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

	// 验证批次大小
	if opts.BatchSize <= 0 {
		return result, WrapError("optimized batch insert", tableName, ErrInvalidBatchSize)
	}

	// 分块处理数据
	totalBatches := int(math.Ceil(float64(len(data)) / float64(opts.BatchSize)))

	for i := 0; i < len(data); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		batchNum := (i / opts.BatchSize) + 1

		log.Printf("处理批次 %d/%d, 记录数: %d", batchNum, totalBatches, len(batch))

		// 重试机制
		var batchErr error
		for retry := 0; retry <= opts.MaxRetries; retry++ {
			// 创建带超时的上下文
			batchCtx, cancel := context.WithTimeout(ctx, opts.Timeout)

			if opts.EnableAsync {
				batchErr = bm.AsyncInsertBatch(batchCtx, tableName, batch)
			} else {
				batchErr = bm.InsertBatch(batchCtx, tableName, batch)
			}

			cancel()

			if batchErr == nil {
				// 成功
				result.ProcessedBatches++
				result.InsertedRecords += len(batch)
				break
			}

			// 失败，记录错误
			if retry == opts.MaxRetries {
				// 最后一次重试也失败了
				result.FailedBatches++
				result.FailedRecords += len(batch)
				wrappedErr := WrapBatchError(batchNum, batchErr)
				result.Errors = append(result.Errors, fmt.Errorf("批次失败（重试 %d 次后）: %w", opts.MaxRetries, wrappedErr))
				log.Printf("批次 %d 最终失败: %v", batchNum, batchErr)
			} else {
				// 还有重试机会，等待后重试
				log.Printf("批次 %d 失败，%v 后重试（第 %d/%d 次）: %v", batchNum, opts.RetryDelay, retry+1, opts.MaxRetries, batchErr)
				time.Sleep(opts.RetryDelay)
			}
		}
	}

	return result, nil
}

// InsertBatchToDistributed 分布式表批量插入
func (bm *batchManager) InsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return bm.InsertBatch(ctx, distributedTableName, data)
}

// AsyncInsertBatchToDistributed 异步分布式表批量插入
func (bm *batchManager) AsyncInsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return bm.AsyncInsertBatch(ctx, distributedTableName, data)
}

// OptimizedBatchInsertToDistributed 优化的分布式表批量插入
func (bm *batchManager) OptimizedBatchInsertToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error) {
	if opts == nil {
		opts = DefaultBatchInsertOptions()
	}

	result := &BatchInsertResult{
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

	// 验证批次大小
	if opts.BatchSize <= 0 {
		return result, WrapError("optimized distributed batch insert", distributedTableName, ErrInvalidBatchSize)
	}

	// 分块处理数据
	totalBatches := int(math.Ceil(float64(len(data)) / float64(opts.BatchSize)))

	for i := 0; i < len(data); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		batchNum := (i / opts.BatchSize) + 1

		log.Printf("处理分布式表批次 %d/%d, 记录数: %d", batchNum, totalBatches, len(batch))

		// 重试机制
		var batchErr error
		for retry := 0; retry <= opts.MaxRetries; retry++ {
			// 创建带超时的上下文
			batchCtx, cancel := context.WithTimeout(ctx, opts.Timeout)

			if opts.EnableAsync {
				batchErr = bm.AsyncInsertBatchToDistributed(batchCtx, distributedTableName, batch)
			} else {
				batchErr = bm.InsertBatchToDistributed(batchCtx, distributedTableName, batch)
			}

			cancel()

			if batchErr == nil {
				// 成功
				result.ProcessedBatches++
				result.InsertedRecords += len(batch)
				break
			}

			// 失败，记录错误
			if retry == opts.MaxRetries {
				// 最后一次重试也失败了
				result.FailedBatches++
				result.FailedRecords += len(batch)
				wrappedErr := WrapBatchError(batchNum, batchErr)
				result.Errors = append(result.Errors, fmt.Errorf("分布式表批次失败（重试 %d 次后）: %w", opts.MaxRetries, wrappedErr))
				log.Printf("分布式表批次 %d 最终失败: %v", batchNum, batchErr)
			} else {
				// 还有重试机会，等待后重试
				log.Printf("分布式表批次 %d 失败，%v 后重试（第 %d/%d 次）: %v", batchNum, opts.RetryDelay, retry+1, opts.MaxRetries, batchErr)
				time.Sleep(opts.RetryDelay)
			}
		}
	}

	return result, nil
}

// ValidateBatchData 验证批量数据
func ValidateBatchData(data []map[string]interface{}) error {
	if len(data) == 0 {
		return ErrEmptyData
	}

	// 检查所有行是否有相同的列
	firstRowColumns := make(map[string]bool)
	for column := range data[0] {
		firstRowColumns[column] = true
	}

	for i, row := range data {
		if len(row) != len(firstRowColumns) {
			return fmt.Errorf("row %d has different number of columns", i)
		}

		for column := range row {
			if !firstRowColumns[column] {
				return fmt.Errorf("row %d has unexpected column: %s", i, column)
			}
		}
	}

	return nil
}

// String 返回批量插入结果的字符串表示
func (r *BatchInsertResult) String() string {
	status := "SUCCESS"
	if r.FailedBatches > 0 || r.FailedRecords > 0 {
		if r.InsertedRecords > 0 {
			status = "PARTIAL"
		} else {
			status = "FAILED"
		}
	}

	return fmt.Sprintf("BatchInsertResult{Status: %s, Duration: %v, Total: %d, Inserted: %d, Failed: %d, Batches: %d/%d, Speed: %.1f records/s}",
		status, r.Duration, r.TotalRecords, r.InsertedRecords, r.FailedRecords,
		r.ProcessedBatches, r.ProcessedBatches+r.FailedBatches, r.AverageSpeed)
}

// IsSuccess 检查是否全部成功
func (r *BatchInsertResult) IsSuccess() bool {
	return r.FailedRecords == 0 && r.FailedBatches == 0
}

// GetSuccessRate 获取成功率
func (r *BatchInsertResult) GetSuccessRate() float64 {
	if r.TotalRecords == 0 {
		return 0
	}
	return float64(r.InsertedRecords) / float64(r.TotalRecords) * 100
}

// HasErrors 检查是否有错误
func (r *BatchInsertResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// GetErrorSummary 获取错误摘要
func (r *BatchInsertResult) GetErrorSummary() string {
	if !r.HasErrors() {
		return "No errors"
	}

	if len(r.Errors) == 1 {
		return r.Errors[0].Error()
	}

	return fmt.Sprintf("%d errors occurred, first: %s", len(r.Errors), r.Errors[0].Error())
}