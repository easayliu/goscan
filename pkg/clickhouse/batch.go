package clickhouse

import (
	"context"
	"fmt"
	"goscan/pkg/logger"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// BatchInsertOptions batch insert options
type BatchInsertOptions struct {
	BatchSize   int           // Size per batch
	MaxRetries  int           // Maximum retry attempts
	RetryDelay  time.Duration // Retry delay
	EnableAsync bool          // Whether to enable async insert
	Timeout     time.Duration // Timeout duration
}

// DefaultBatchInsertOptions returns default batch insert options
func DefaultBatchInsertOptions() *BatchInsertOptions {
	return &BatchInsertOptions{
		BatchSize:   500,
		MaxRetries:  3,
		RetryDelay:  2 * time.Second,
		EnableAsync: false,
		Timeout:     30 * time.Second,
	}
}

// BatchInsertResult batch insert result
type BatchInsertResult struct {
	TotalRecords     int           `json:"total_records"`
	ProcessedBatches int           `json:"processed_batches"`
	FailedBatches    int           `json:"failed_batches"`
	InsertedRecords  int           `json:"inserted_records"`
	FailedRecords    int           `json:"failed_records"`
	Duration         time.Duration `json:"duration"`
	AverageSpeed     float64       `json:"average_speed"` // Records per second
	Errors           []error       `json:"errors,omitempty"`
}

// batchManager batch operations manager
type batchManager struct {
	conn         driver.Conn
	nameResolver *TableNameResolver
}

// NewBatchManager creates batch operations manager
func NewBatchManager(conn driver.Conn, nameResolver *TableNameResolver) BatchManager {
	return &batchManager{
		conn:         conn,
		nameResolver: nameResolver,
	}
}

// PrepareBatch prepares batch operations
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

// InsertBatch inserts data in batches
func (bm *batchManager) InsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return ErrEmptyData
	}

	// Automatically resolve table name
	resolvedTableName := bm.nameResolver.ResolveInsertTarget(tableName)

	logger.Debug("Batch insert table name resolution",
		zap.String("input_table", tableName),
		zap.String("resolved_table", resolvedTableName),
		zap.Bool("cluster_enabled", bm.nameResolver.IsClusterEnabled()),
		zap.Int("record_count", len(data)))

	columns := ExtractColumnsFromData(data)
	if len(columns) == 0 {
		return WrapError("insert batch", tableName, fmt.Errorf("no columns found in data"))
	}

	query := BuildInsertQuery(resolvedTableName, columns)
	logger.Debug("Preparing batch insert query",
		zap.String("query", query),
		zap.Strings("columns", columns))

	batch, err := bm.PrepareBatch(ctx, query)
	if err != nil {
		return WrapError("prepare batch", resolvedTableName, err)
	}

	for i, row := range data {
		values := PrepareValues(row, columns)
		if err := batch.Append(values...); err != nil {
			logger.Error("Failed to append row to batch",
				zap.Int("row_index", i),
				zap.String("table", resolvedTableName),
				zap.Error(err))
			return WrapError("append to batch", resolvedTableName, err)
		}
	}

	if err := batch.Send(); err != nil {
		logger.Error("Failed to send batch",
			zap.String("table", resolvedTableName),
			zap.Int("record_count", len(data)),
			zap.Error(err))
		return WrapError("send batch", resolvedTableName, err)
	}

	logger.Debug("Batch insert successful",
		zap.String("table", resolvedTableName),
		zap.Int("record_count", len(data)))
	return nil
}

// AsyncInsertBatch inserts data asynchronously in batches
func (bm *batchManager) AsyncInsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return ErrEmptyData
	}

	// Enable async insert
	ctxWithSettings := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":                 1,
		"wait_for_async_insert":        1,
		"async_insert_max_data_size":   10485760, // 10MB
		"async_insert_busy_timeout_ms": 200,
	}))

	return bm.InsertBatch(ctxWithSettings, tableName, data)
}

// OptimizedBatchInsert optimized batch insert method supporting chunking, retry, and performance monitoring
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

	// Validate batch size
	if opts.BatchSize <= 0 {
		return result, WrapError("optimized batch insert", tableName, ErrInvalidBatchSize)
	}

	// Process data in chunks
	totalBatches := int(math.Ceil(float64(len(data)) / float64(opts.BatchSize)))

	for i := 0; i < len(data); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		batchNum := (i / opts.BatchSize) + 1

		logger.Debug("Processing batch",
			zap.Int("batch_num", batchNum),
			zap.Int("total_batches", totalBatches),
			zap.Int("record_count", len(batch)))

		// Retry mechanism
		var batchErr error
		for retry := 0; retry <= opts.MaxRetries; retry++ {
			// Create context with timeout
			batchCtx, cancel := context.WithTimeout(ctx, opts.Timeout)

			if opts.EnableAsync {
				batchErr = bm.AsyncInsertBatch(batchCtx, tableName, batch)
			} else {
				batchErr = bm.InsertBatch(batchCtx, tableName, batch)
			}

			cancel()

			if batchErr == nil {
				// Success
				result.ProcessedBatches++
				result.InsertedRecords += len(batch)
				break
			}

			// Failed, log error
			if retry == opts.MaxRetries {
				// Last retry also failed
				result.FailedBatches++
				result.FailedRecords += len(batch)
				wrappedErr := WrapBatchError(batchNum, batchErr)
				result.Errors = append(result.Errors, fmt.Errorf("batch failed after %d retries: %w", opts.MaxRetries, wrappedErr))
				logger.Error("Batch finally failed",
					zap.Int("batch_num", batchNum),
					zap.Error(batchErr))
			} else {
				// Still have retry chances, wait and retry
				logger.Warn("Batch failed, preparing to retry",
					zap.Int("batch_num", batchNum),
					zap.Duration("retry_delay", opts.RetryDelay),
					zap.Int("retry_attempt", retry+1),
					zap.Int("max_retries", opts.MaxRetries),
					zap.Error(batchErr))
				time.Sleep(opts.RetryDelay)
			}
		}
	}

	return result, nil
}

// InsertBatchToDistributed distributed table batch insert
func (bm *batchManager) InsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return bm.InsertBatch(ctx, distributedTableName, data)
}

// AsyncInsertBatchToDistributed asynchronous distributed table batch insert
func (bm *batchManager) AsyncInsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return bm.AsyncInsertBatch(ctx, distributedTableName, data)
}

// OptimizedBatchInsertToDistributed optimized distributed table batch insert
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

	// Validate batch size
	if opts.BatchSize <= 0 {
		return result, WrapError("optimized distributed batch insert", distributedTableName, ErrInvalidBatchSize)
	}

	// Process data in chunks
	totalBatches := int(math.Ceil(float64(len(data)) / float64(opts.BatchSize)))

	for i := 0; i < len(data); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		batchNum := (i / opts.BatchSize) + 1

		logger.Debug("Processing distributed table batch",
			zap.Int("batch_num", batchNum),
			zap.Int("total_batches", totalBatches),
			zap.Int("record_count", len(batch)))

		// Retry mechanism
		var batchErr error
		for retry := 0; retry <= opts.MaxRetries; retry++ {
			// Create context with timeout
			batchCtx, cancel := context.WithTimeout(ctx, opts.Timeout)

			if opts.EnableAsync {
				batchErr = bm.AsyncInsertBatchToDistributed(batchCtx, distributedTableName, batch)
			} else {
				batchErr = bm.InsertBatchToDistributed(batchCtx, distributedTableName, batch)
			}

			cancel()

			if batchErr == nil {
				// Success
				result.ProcessedBatches++
				result.InsertedRecords += len(batch)
				break
			}

			// Failed, log error
			if retry == opts.MaxRetries {
				// Last retry also failed
				result.FailedBatches++
				result.FailedRecords += len(batch)
				wrappedErr := WrapBatchError(batchNum, batchErr)
				result.Errors = append(result.Errors, fmt.Errorf("distributed table batch failed after %d retries: %w", opts.MaxRetries, wrappedErr))
				logger.Error("Distributed table batch finally failed",
					zap.Int("batch_num", batchNum),
					zap.Error(batchErr))
			} else {
				// Still have retry chances, wait and retry
				logger.Warn("Distributed table batch failed, preparing to retry",
					zap.Int("batch_num", batchNum),
					zap.Duration("retry_delay", opts.RetryDelay),
					zap.Int("retry_attempt", retry+1),
					zap.Int("max_retries", opts.MaxRetries),
					zap.Error(batchErr))
				time.Sleep(opts.RetryDelay)
			}
		}
	}

	return result, nil
}

// ValidateBatchData validates batch data
func ValidateBatchData(data []map[string]interface{}) error {
	if len(data) == 0 {
		return ErrEmptyData
	}

	// Check if all rows have the same columns
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

// String returns string representation of batch insert result
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

// IsSuccess checks if all succeeded
func (r *BatchInsertResult) IsSuccess() bool {
	return r.FailedRecords == 0 && r.FailedBatches == 0
}

// GetSuccessRate gets success rate
func (r *BatchInsertResult) GetSuccessRate() float64 {
	if r.TotalRecords == 0 {
		return 0
	}
	return float64(r.InsertedRecords) / float64(r.TotalRecords) * 100
}

// HasErrors checks if there are errors
func (r *BatchInsertResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// GetErrorSummary gets error summary
func (r *BatchInsertResult) GetErrorSummary() string {
	if !r.HasErrors() {
		return "No errors"
	}

	if len(r.Errors) == 1 {
		return r.Errors[0].Error()
	}

	return fmt.Sprintf("%d errors occurred, first: %s", len(r.Errors), r.Errors[0].Error())
}
