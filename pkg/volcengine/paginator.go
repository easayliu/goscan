package volcengine

import (
	"context"
	"fmt"
	"log"
	"time"
)

// PaginatorConfig 分页器配置
type PaginatorConfig struct {
	BatchSize      int           // 每批处理的记录数
	MaxRetries     int           // 最大重试次数
	RetryDelay     time.Duration // 重试延迟
	MaxConcurrency int           // 最大并发数
}

// DefaultPaginatorConfig 默认分页器配置
func DefaultPaginatorConfig() *PaginatorConfig {
	return &PaginatorConfig{
		BatchSize:      50,              // 降低默认批次大小，减少API压力
		MaxRetries:     5,               // 增加重试次数
		RetryDelay:     3 * time.Second, // 增加重试延迟
		MaxConcurrency: 1,               // 降低并发数，避免限流
	}
}

// ProgressCallback 进度回调函数
type ProgressCallback func(current, total int, duration time.Duration)

// BillPaginator 账单分页器
type BillPaginator struct {
	client    *Client
	config    *PaginatorConfig
	processor DataProcessor
	progress  ProgressCallback
}

// NewBillPaginator 创建账单分页器
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

// SetProgressCallback 设置进度回调
func (p *BillPaginator) SetProgressCallback(callback ProgressCallback) {
	p.progress = callback
}

// PaginateResult 分页结果
type PaginateResult struct {
	TotalRecords     int           `json:"total_records"`
	ProcessedRecords int           `json:"processed_records"`
	Duration         time.Duration `json:"duration"`
	Errors           []error       `json:"errors"`
}

// PaginateBillDetails 分页处理账单详情数据
func (p *BillPaginator) PaginateBillDetails(ctx context.Context, req *ListBillDetailRequest) (*PaginateResult, error) {
	startTime := time.Now()
	result := &PaginateResult{}

	log.Printf("[分页器] 开始处理账单数据，账期: %s", req.BillPeriod)

	// 获取总记录数和第一批数据
	totalRecords, err := p.initializeAndProcessFirstBatch(ctx, req, result)
	if err != nil {
		return result, err
	}

	result.TotalRecords = totalRecords
	log.Printf("[分页器] 总记录数: %d，批次大小: %d", totalRecords, p.config.BatchSize)

	// 如果只有一批数据，直接返回
	if result.ProcessedRecords < p.config.BatchSize {
		p.finalizeResult(result, startTime)
		return result, nil
	}

	// 继续处理剩余批次
	err = p.processRemainingBatches(ctx, req, result, startTime)
	p.finalizeResult(result, startTime)

	return result, err
}

// initializeAndProcessFirstBatch 初始化并处理第一批数据
func (p *BillPaginator) initializeAndProcessFirstBatch(ctx context.Context, req *ListBillDetailRequest, result *PaginateResult) (int, error) {
	// 构建第一次请求
	firstReq := p.buildPageRequest(req, 0, true)

	firstResp, err := p.fetchBillDetailWithRetry(ctx, firstReq)
	if err != nil {
		return 0, fmt.Errorf("获取首页数据失败: %w", err)
	}

	totalRecords := int(firstResp.Result.Total)
	if totalRecords == 0 {
		totalRecords = 999999 // 如果API不返回总数，设置一个大数
	}

	// 处理第一批数据
	if len(firstResp.Result.List) > 0 {
		p.processBatch(ctx, firstResp.Result.List, 1, result, time.Now())
	}

	return totalRecords, nil
}

// processRemainingBatches 处理剩余的批次
func (p *BillPaginator) processRemainingBatches(ctx context.Context, req *ListBillDetailRequest, result *PaginateResult, startTime time.Time) error {
	offset := p.config.BatchSize
	batchNum := 2

	for {
		if err := p.checkContext(ctx); err != nil {
			return err
		}

		// 构建分页请求
		pageReq := p.buildPageRequest(req, offset, false)
		log.Printf("[分页器] 获取第%d批数据，offset=%d", batchNum, offset)

		// 获取数据
		resp, err := p.fetchBillDetailWithRetry(ctx, pageReq)
		if err != nil {
			result.Errors = append(result.Errors, err)
			log.Printf("[分页器] 获取第%d批数据失败: %v", batchNum, err)
			break
		}

		// 检查是否有数据
		if len(resp.Result.List) == 0 {
			log.Printf("[分页器] 第%d批无数据，停止分页", batchNum)
			break
		}

		// 处理当前批次数据
		p.processBatch(ctx, resp.Result.List, batchNum, result, startTime)

		// 检查是否为最后一批
		if len(resp.Result.List) < p.config.BatchSize {
			log.Printf("[分页器] 第%d批是最后一批（%d条 < %d），停止分页",
				batchNum, len(resp.Result.List), p.config.BatchSize)
			break
		}

		offset += p.config.BatchSize
		batchNum++
	}

	return nil
}

// buildPageRequest 构建分页请求
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

// processBatch 处理单个批次的数据
func (p *BillPaginator) processBatch(ctx context.Context, bills []BillDetail, batchNum int, result *PaginateResult, startTime time.Time) {
	if err := p.processor.Process(ctx, bills); err != nil {
		result.Errors = append(result.Errors, err)
		log.Printf("[分页器] 处理第%d批数据失败: %v", batchNum, err)
	} else {
		result.ProcessedRecords += len(bills)
		log.Printf("[分页器] 处理第%d批数据成功: %d条", batchNum, len(bills))
	}

	// 进度回调
	if p.progress != nil {
		p.progress(result.ProcessedRecords, result.TotalRecords, time.Since(startTime))
	}
}

// checkContext 检查上下文是否已取消
func (p *BillPaginator) checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// finalizeResult 完成结果统计
func (p *BillPaginator) finalizeResult(result *PaginateResult, startTime time.Time) {
	result.Duration = time.Since(startTime)
	log.Printf("[分页器] 数据处理完成，共处理: %d条，耗时: %v", result.ProcessedRecords, result.Duration)
}

// fetchBillDetailWithRetry 带重试的获取账单详情
func (p *BillPaginator) fetchBillDetailWithRetry(ctx context.Context, req *ListBillDetailRequest) (*ListBillDetailResponse, error) {
	var lastErr error

	for attempt := 0; attempt < p.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// 等待重试延迟
			select {
			case <-time.After(p.config.RetryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			log.Printf("[分页器] 第%d次重试", attempt+1)
		}

		resp, err := p.client.ListBillDetail(ctx, req)
		if err == nil {
			return resp, nil
		}

		lastErr = err

		// 检查是否应该重试
		if !IsRetryableError(err) {
			log.Printf("[分页器] 不可重试的错误: %v", err)
			break
		}

		log.Printf("[分页器] 第%d次尝试失败: %v", attempt+1, err)
	}

	return nil, fmt.Errorf("重试%d次后仍然失败: %w", p.config.MaxRetries, lastErr)
}

// WithBatchSize 设置批次大小 (链式调用)
func (p *BillPaginator) WithBatchSize(size int) *BillPaginator {
	if size > 0 {
		p.config.BatchSize = size
	}
	return p
}

// WithMaxRetries 设置最大重试次数 (链式调用)
func (p *BillPaginator) WithMaxRetries(retries int) *BillPaginator {
	if retries > 0 {
		p.config.MaxRetries = retries
	}
	return p
}

// WithRetryDelay 设置重试延迟 (链式调用)
func (p *BillPaginator) WithRetryDelay(delay time.Duration) *BillPaginator {
	if delay > 0 {
		p.config.RetryDelay = delay
	}
	return p
}

// GetConfig 获取分页器配置
func (p *BillPaginator) GetConfig() *PaginatorConfig {
	return p.config
}

// String 实现 Stringer 接口
func (pr *PaginateResult) String() string {
	successRate := float64(pr.ProcessedRecords) / float64(pr.TotalRecords) * 100
	if pr.TotalRecords == 0 {
		successRate = 0
	}

	return fmt.Sprintf("总数=%d, 已处理=%d, 成功率=%.1f%%, 错误=%d, 耗时=%v",
		pr.TotalRecords, pr.ProcessedRecords, successRate, len(pr.Errors), pr.Duration)
}

// IsSuccess 判断分页处理是否成功
func (pr *PaginateResult) IsSuccess() bool {
	return len(pr.Errors) == 0
}