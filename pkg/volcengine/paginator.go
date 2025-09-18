package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/logger"
	"time"

	"go.uber.org/zap"
)

// PaginatorConfig paginator configuration
type PaginatorConfig struct {
	BatchSize      int           // Number of records processed per batch
	MaxRetries     int           // Maximum number of retries
	RetryDelay     time.Duration // Retry delay
	MaxConcurrency int           // Maximum concurrency
}

// DefaultPaginatorConfig default paginator configuration
func DefaultPaginatorConfig() *PaginatorConfig {
	return &PaginatorConfig{
		BatchSize:      50,              // Reduce default batch size to decrease API pressure
		MaxRetries:     5,               // Increase retry count
		RetryDelay:     3 * time.Second, // Increase retry delay
		MaxConcurrency: 1,               // Reduce concurrency to avoid rate limiting
	}
}

// ProgressCallback progress callback function
type ProgressCallback func(current, total int, duration time.Duration)

// BillPaginator bill paginator
type BillPaginator struct {
	client    *Client
	config    *PaginatorConfig
	processor DataProcessor
	progress  ProgressCallback
}

// NewBillPaginator creates bill paginator
func NewBillPaginator(client *Client, processor DataProcessor, config ...*PaginatorConfig) *BillPaginator {
	cfg := DefaultPaginatorConfig()
	if len(config) > 0 && config[0] != nil {
		cfg = config[0]
	}

	return &BillPaginator{
		client:    client,
		config:    cfg,
		processor: processor,
	}
}

// SetProgressCallback sets progress callback
func (p *BillPaginator) SetProgressCallback(callback ProgressCallback) {
	p.progress = callback
}

// PaginateResult pagination result
type PaginateResult struct {
	TotalRecords     int           `json:"total_records"`
	ProcessedRecords int           `json:"processed_records"`
	Duration         time.Duration `json:"duration"`
	Errors           []error       `json:"errors"`
}

// PaginateBillDetails processes bill detail data with pagination
func (p *BillPaginator) PaginateBillDetails(ctx context.Context, req *ListBillDetailRequest) (*PaginateResult, error) {
	startTime := time.Now()
	result := &PaginateResult{}

	logger.Info("Volcengine paginator starting processing",
		zap.String("provider", "volcengine"),
		zap.String("period", req.BillPeriod))

	// Get total record count and first batch data
	totalRecords, err := p.initializeAndProcessFirstBatch(ctx, req, result)
	if err != nil {
		return result, err
	}

	result.TotalRecords = totalRecords
	logger.Info("Volcengine paginator statistics",
		zap.String("provider", "volcengine"),
		zap.Int("total_records", totalRecords),
		zap.Int("batch_size", p.config.BatchSize))

	// Return directly if only one batch of data
	if result.ProcessedRecords < p.config.BatchSize {
		p.finalizeResult(result, startTime)
		return result, nil
	}

	// Continue processing remaining batches
	err = p.processRemainingBatches(ctx, req, result, startTime)
	p.finalizeResult(result, startTime)

	return result, err
}

// initializeAndProcessFirstBatch initializes and processes first batch of data
func (p *BillPaginator) initializeAndProcessFirstBatch(ctx context.Context, req *ListBillDetailRequest, result *PaginateResult) (int, error) {
	// Build first request
	firstReq := p.buildPageRequest(req, 0, true)

	firstResp, err := p.fetchBillDetailWithRetry(ctx, firstReq)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch first page data: %w", err)
	}

	totalRecords := int(firstResp.Result.Total)
	if totalRecords == 0 {
		totalRecords = 999999 // Set a large number if API doesn't return total count
	}

	// Process first batch data
	if len(firstResp.Result.List) > 0 {
		p.processBatch(ctx, firstResp.Result.List, 1, result, time.Now())
	}

	return totalRecords, nil
}

// processRemainingBatches processes remaining batches
func (p *BillPaginator) processRemainingBatches(ctx context.Context, req *ListBillDetailRequest, result *PaginateResult, startTime time.Time) error {
	offset := p.config.BatchSize
	batchNum := 2

	for {
		if err := p.checkContext(ctx); err != nil {
			return err
		}

		// Build pagination request
		pageReq := p.buildPageRequest(req, offset, false)
		logger.Debug("Fetching paginated data",
			zap.String("provider", "volcengine"),
			zap.Int("batch_num", batchNum),
			zap.Int("offset", offset))

		// Fetch data
		resp, err := p.fetchBillDetailWithRetry(ctx, pageReq)
		if err != nil {
			result.Errors = append(result.Errors, err)
			logger.Error("Failed to fetch paginated data",
				zap.String("provider", "volcengine"),
				zap.Int("batch_num", batchNum),
				zap.Error(err))
			break
		}

		// Check if there is data
		if len(resp.Result.List) == 0 {
			logger.Info("Pagination has no data, stopping pagination",
				zap.String("provider", "volcengine"),
				zap.Int("batch_num", batchNum))
			break
		}

		// Process current batch data
		p.processBatch(ctx, resp.Result.List, batchNum, result, startTime)

		// Check if this is the last batch
		if len(resp.Result.List) < p.config.BatchSize {
			logger.Info("Reached last batch of data",
				zap.String("provider", "volcengine"),
				zap.Int("batch_num", batchNum),
				zap.Int("record_count", len(resp.Result.List)),
				zap.Int("batch_size", p.config.BatchSize))
			break
		}

		offset += p.config.BatchSize
		batchNum++
	}

	return nil
}

// buildPageRequest builds pagination request
func (p *BillPaginator) buildPageRequest(baseReq *ListBillDetailRequest, offset int, needRecordNum bool) *ListBillDetailRequest {
	req := *baseReq
	req.Limit = int32(p.config.BatchSize)
	req.Offset = int32(offset)

	if needRecordNum {
		req.NeedRecordNum = 1
	} else {
		req.NeedRecordNum = 0
	}

	return &req
}

// processBatch processes single batch of data
func (p *BillPaginator) processBatch(ctx context.Context, bills []BillDetail, batchNum int, result *PaginateResult, startTime time.Time) {
	if err := p.processor.Process(ctx, bills); err != nil {
		result.Errors = append(result.Errors, err)
		logger.Error("Failed to process paginated data",
			zap.String("provider", "volcengine"),
			zap.Int("batch_num", batchNum),
			zap.Error(err))
	} else {
		result.ProcessedRecords += len(bills)
		logger.Info("Successfully processed paginated data",
			zap.String("provider", "volcengine"),
			zap.Int("batch_num", batchNum),
			zap.Int("record_count", len(bills)))
	}

	// Progress callback
	if p.progress != nil {
		p.progress(result.ProcessedRecords, result.TotalRecords, time.Since(startTime))
	}
}

// checkContext checks if context is cancelled
func (p *BillPaginator) checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// finalizeResult finalizes result statistics
func (p *BillPaginator) finalizeResult(result *PaginateResult, startTime time.Time) {
	result.Duration = time.Since(startTime)
	logger.Info("Data processing completed",
		zap.String("provider", "volcengine"),
		zap.Int("processed_records", result.ProcessedRecords),
		zap.Duration("duration", result.Duration))
}

// fetchBillDetailWithRetry fetches bill detail with retry
func (p *BillPaginator) fetchBillDetailWithRetry(ctx context.Context, req *ListBillDetailRequest) (*ListBillDetailResponse, error) {
	var lastErr error

	for attempt := 0; attempt < p.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Wait for retry delay
			select {
			case <-time.After(p.config.RetryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			logger.Warn("Retrying data fetch",
				zap.String("provider", "volcengine"),
				zap.Int("retry_attempt", attempt+1))
		}

		resp, err := p.client.ListBillDetail(ctx, req)
		if err == nil {
			return resp, nil
		}

		lastErr = err

		// Check if should retry
		if !IsRetryableError(err) {
			logger.Error("Non-retryable error",
				zap.String("provider", "volcengine"),
				zap.Error(err))
			break
		}

		logger.Warn("Attempt failed, will retry",
			zap.String("provider", "volcengine"),
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}

	return nil, fmt.Errorf("still failed after %d retries: %w", p.config.MaxRetries, lastErr)
}

// WithBatchSize sets batch size (chain call)
func (p *BillPaginator) WithBatchSize(size int) *BillPaginator {
	if size > 0 {
		p.config.BatchSize = size
	}
	return p
}

// WithMaxRetries sets maximum retry count (chain call)
func (p *BillPaginator) WithMaxRetries(retries int) *BillPaginator {
	if retries > 0 {
		p.config.MaxRetries = retries
	}
	return p
}

// WithRetryDelay sets retry delay (chain call)
func (p *BillPaginator) WithRetryDelay(delay time.Duration) *BillPaginator {
	if delay > 0 {
		p.config.RetryDelay = delay
	}
	return p
}

// GetConfig gets paginator configuration
func (p *BillPaginator) GetConfig() *PaginatorConfig {
	return p.config
}

// String implements Stringer interface
func (pr *PaginateResult) String() string {
	successRate := float64(pr.ProcessedRecords) / float64(pr.TotalRecords) * 100
	if pr.TotalRecords == 0 {
		successRate = 0
	}

	return fmt.Sprintf("total=%d, processed=%d, success_rate=%.1f%%, errors=%d, duration=%v",
		pr.TotalRecords, pr.ProcessedRecords, successRate, len(pr.Errors), pr.Duration)
}

// IsSuccess determines if pagination processing was successful
func (pr *PaginateResult) IsSuccess() bool {
	return len(pr.Errors) == 0
}
