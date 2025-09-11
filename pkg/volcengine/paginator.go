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

// DataProcessor 数据处理器接口
type DataProcessor interface {
	Process(ctx context.Context, data []BillDetail) error
}

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

	offset := 0
	totalRecords := 0
	processedRecords := 0

	// 第一次请求获取总记录数
	firstReq := *req
	firstReq.Limit = int32(p.config.BatchSize)
	firstReq.Offset = 0
	firstReq.NeedRecordNum = 1

	firstResp, err := p.fetchBillDetailWithRetry(ctx, &firstReq)
	if err != nil {
		return result, fmt.Errorf("获取首页数据失败: %w", err)
	}

	totalRecords = int(firstResp.Result.Total)
	if totalRecords == 0 {
		totalRecords = 999999 // 如果API不返回总数，设置一个大数
	}

	result.TotalRecords = totalRecords
	log.Printf("[分页器] 总记录数: %d，批次大小: %d", totalRecords, p.config.BatchSize)

	// 处理第一批数据
	if len(firstResp.Result.List) > 0 {
		if err := p.processor.Process(ctx, firstResp.Result.List); err != nil {
			result.Errors = append(result.Errors, err)
			log.Printf("[分页器] 处理第1批数据失败: %v", err)
		} else {
			processedRecords += len(firstResp.Result.List)
			log.Printf("[分页器] 处理第1批数据成功: %d条", len(firstResp.Result.List))
		}

		if p.progress != nil {
			p.progress(processedRecords, totalRecords, time.Since(startTime))
		}

		// 如果第一批数据少于批次大小，说明没有更多数据
		if len(firstResp.Result.List) < p.config.BatchSize {
			result.ProcessedRecords = processedRecords
			result.Duration = time.Since(startTime)
			log.Printf("[分页器] 数据处理完成，共处理: %d条，耗时: %v", processedRecords, result.Duration)
			return result, nil
		}
	}

	offset = p.config.BatchSize
	batchNum := 2

	// 继续分页处理
	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		pageReq := *req
		pageReq.Limit = int32(p.config.BatchSize)
		pageReq.Offset = int32(offset)
		pageReq.NeedRecordNum = 0

		log.Printf("[分页器] 获取第%d批数据，offset=%d", batchNum, offset)

		resp, err := p.fetchBillDetailWithRetry(ctx, &pageReq)
		if err != nil {
			result.Errors = append(result.Errors, err)
			log.Printf("[分页器] 获取第%d批数据失败: %v", batchNum, err)
			break
		}

		// 没有更多数据
		if len(resp.Result.List) == 0 {
			log.Printf("[分页器] 第%d批无数据，停止分页", batchNum)
			break
		}

		// 处理数据
		if err := p.processor.Process(ctx, resp.Result.List); err != nil {
			result.Errors = append(result.Errors, err)
			log.Printf("[分页器] 处理第%d批数据失败: %v", batchNum, err)
		} else {
			processedRecords += len(resp.Result.List)
			log.Printf("[分页器] 处理第%d批数据成功: %d条", batchNum, len(resp.Result.List))
		}

		// 进度回调
		if p.progress != nil {
			p.progress(processedRecords, totalRecords, time.Since(startTime))
		}

		// 如果当前批次数据少于批次大小，说明这是最后一批
		if len(resp.Result.List) < p.config.BatchSize {
			log.Printf("[分页器] 第%d批是最后一批（%d条 < %d），停止分页",
				batchNum, len(resp.Result.List), p.config.BatchSize)
			break
		}

		offset += p.config.BatchSize
		batchNum++
	}

	result.ProcessedRecords = processedRecords
	result.Duration = time.Since(startTime)

	log.Printf("[分页器] 数据处理完成，总记录: %d，已处理: %d，错误: %d，耗时: %v",
		totalRecords, processedRecords, len(result.Errors), result.Duration)

	return result, nil
}

// fetchBillDetailWithRetry 带重试的数据获取
func (p *BillPaginator) fetchBillDetailWithRetry(ctx context.Context, req *ListBillDetailRequest) (*ListBillDetailResponse, error) {
	var lastErr error

	for attempt := 0; attempt <= p.config.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("[分页器] 重试第%d次，延迟%v", attempt, p.config.RetryDelay)
			select {
			case <-time.After(p.config.RetryDelay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		resp, err := p.client.ListBillDetail(ctx, req)
		if err == nil && resp.ResponseMetadata.Error == nil {
			return resp, nil
		}

		if err != nil {
			lastErr = err
		} else if resp.ResponseMetadata.Error != nil {
			lastErr = fmt.Errorf("API错误: %s - %s",
				resp.ResponseMetadata.Error.Code, resp.ResponseMetadata.Error.Message)
		}

		log.Printf("[分页器] 第%d次尝试失败: %v", attempt+1, lastErr)
	}

	return nil, fmt.Errorf("重试%d次后仍失败: %w", p.config.MaxRetries, lastErr)
}
