package alicloud

import (
	"context"
	"fmt"
	"goscan/pkg/logger"
	"time"

	"go.uber.org/zap"
)

// Paginator Alibaba Cloud API paginator
// Handles NextToken pagination mechanism
type Paginator struct {
	client           *Client
	baseRequest      *DescribeInstanceBillRequest
	nextToken        string
	hasMore          bool
	currentPage      int
	totalFetched     int
	startTime        time.Time
	progressCallback func(current, total int32)
}

// NewPaginator creates a new paginator
func NewPaginator(client *Client, baseRequest *DescribeInstanceBillRequest) *Paginator {
	// Copy base request to avoid modifying the original request
	requestCopy := *baseRequest

	return &Paginator{
		client:       client,
		baseRequest:  &requestCopy,
		nextToken:    "",
		hasMore:      true,
		currentPage:  0,
		totalFetched: 0,
		startTime:    time.Now(),
	}
}

// Next fetches the next page of data
func (p *Paginator) Next(ctx context.Context) (*DescribeInstanceBillResponse, error) {
	if !p.hasMore {
		return nil, fmt.Errorf("no more pages available")
	}

	// Prepare request
	request := *p.baseRequest
	request.NextToken = p.nextToken

	p.currentPage++
	logger.Debug("Alibaba Cloud paginator fetching data",
		zap.String("provider", "alicloud"),
		zap.Int("page", p.currentPage),
		zap.String("next_token", p.truncateToken(p.nextToken)))

	// Call API
	response, err := p.client.DescribeInstanceBill(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch page %d: %w", p.currentPage, err)
	}

	// Update pagination state
	p.nextToken = response.Data.NextToken
	p.hasMore = p.nextToken != ""
	p.totalFetched += len(response.Data.Items)

	// Record pagination information
	pageInfo := fmt.Sprintf("Page %d: %d records", p.currentPage, len(response.Data.Items))
	if p.hasMore {
		pageInfo += fmt.Sprintf(", more data available (NextToken: %s)", p.truncateToken(p.nextToken))
	} else {
		pageInfo += ", last page reached"
	}
	logger.Debug("Alibaba Cloud paginator page information",
		zap.String("provider", "alicloud"),
		zap.String("page_info", pageInfo))

	// Calculate statistics
	elapsed := time.Since(p.startTime)
	if elapsed > 0 {
		avgSpeed := float64(p.totalFetched) / elapsed.Seconds()
		logger.Debug("Alibaba Cloud paginator statistics",
			zap.String("provider", "alicloud"),
			zap.Int("total_fetched", p.totalFetched),
			zap.Float64("avg_speed", avgSpeed))
	}

	return response, nil
}

// HasMore checks if there are more pages
func (p *Paginator) HasMore() bool {
	return p.hasMore
}

// HasNext checks if there is a next page (implements PaginatorInterface)
func (p *Paginator) HasNext() bool {
	return p.hasMore
}

// GetCurrentPage 获取当前页码
func (p *Paginator) GetCurrentPage() int {
	return p.currentPage
}

// GetTotalFetched 获取累计获取的记录数
func (p *Paginator) GetTotalFetched() int {
	return p.totalFetched
}

// GetNextToken 获取下一页的Token
func (p *Paginator) GetNextToken() string {
	return p.nextToken
}

// Reset 重置分页器
func (p *Paginator) Reset() {
	p.nextToken = ""
	p.hasMore = true
	p.currentPage = 0
	p.totalFetched = 0
	p.startTime = time.Now()
}

// SetProgressCallback 设置进度回调（实现PaginatorInterface接口）
func (p *Paginator) SetProgressCallback(callback func(current, total int32)) {
	p.progressCallback = callback
}

// FetchAll 获取所有页的数据
func (p *Paginator) FetchAll(ctx context.Context) ([]BillDetail, error) {
	logger.Info("Alibaba Cloud paginator starting to fetch all data",
		zap.String("provider", "alicloud"))

	var allBills []BillDetail

	for p.HasMore() {
		response, err := p.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch all data at page %d: %w", p.currentPage, err)
		}

		allBills = append(allBills, response.Data.Items...)

		// 速率控制已由RateLimiter处理，这里只需要极短延迟避免CPU占用
		if p.HasMore() {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return allBills, ctx.Err()
			}
		}
	}

	logger.Info("Alibaba Cloud paginator all data fetching completed",
		zap.String("provider", "alicloud"),
		zap.Int("total_pages", p.currentPage),
		zap.Int("total_records", len(allBills)))

	return allBills, nil
}

// FetchAllWithCallback 获取所有数据并提供进度回调
func (p *Paginator) FetchAllWithCallback(ctx context.Context,
	callback func(page int, pageRecords int, totalRecords int)) ([]BillDetail, error) {

	logger.Info("Alibaba Cloud paginator starting to fetch data with progress callback",
		zap.String("provider", "alicloud"))

	var allBills []BillDetail

	for p.HasMore() {
		response, err := p.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch all data at page %d: %w", p.currentPage, err)
		}

		allBills = append(allBills, response.Data.Items...)

		// 调用进度回调
		if callback != nil {
			callback(p.currentPage, len(response.Data.Items), len(allBills))
		}

		// 速率控制已由RateLimiter处理，这里只需要极短延迟避免CPU占用
		if p.HasMore() {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return allBills, ctx.Err()
			}
		}
	}

	logger.Info("Alibaba Cloud paginator data fetching completed with callback",
		zap.String("provider", "alicloud"),
		zap.Int("total_pages", p.currentPage),
		zap.Int("total_records", len(allBills)))

	return allBills, nil
}

// FetchBatches 分批获取数据（返回批次数组）
func (p *Paginator) FetchBatches(ctx context.Context, batchSize int) ([][]BillDetail, error) {
	if batchSize <= 0 {
		batchSize = 1000 // 默认批次大小
	}

	logger.Info("Alibaba Cloud paginator starting batch data fetching",
		zap.String("provider", "alicloud"),
		zap.Int("batch_size", batchSize))

	var batches [][]BillDetail
	var currentBatch []BillDetail

	for p.HasMore() {
		response, err := p.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch batch data at page %d: %w", p.currentPage, err)
		}

		// 将页数据添加到当前批次
		for _, bill := range response.Data.Items {
			currentBatch = append(currentBatch, bill)

			// 如果当前批次达到指定大小，添加到批次数组
			if len(currentBatch) >= batchSize {
				batches = append(batches, make([]BillDetail, len(currentBatch)))
				copy(batches[len(batches)-1], currentBatch)
				currentBatch = currentBatch[:0] // 清空当前批次，但保留容量
			}
		}

		// 速率控制已由RateLimiter处理，这里只需要极短延迟避免CPU占用
		if p.HasMore() {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
				return batches, ctx.Err()
			}
		}
	}

	// 添加最后一个不满批次（如果有的话）
	if len(currentBatch) > 0 {
		batches = append(batches, make([]BillDetail, len(currentBatch)))
		copy(batches[len(batches)-1], currentBatch)
	}

	logger.Info("Alibaba Cloud paginator batch fetching completed",
		zap.String("provider", "alicloud"),
		zap.Int("total_pages", p.currentPage),
		zap.Int("batch_count", len(batches)),
		zap.Int("total_records", p.totalFetched))

	return batches, nil
}

// EstimateTotal 估算总记录数（基于第一页的TotalCount）
func (p *Paginator) EstimateTotal(ctx context.Context) (int32, error) {
	if p.currentPage > 0 {
		return 0, fmt.Errorf("cannot estimate total after pagination has started")
	}

	// 获取第一页数据以获取总数
	response, err := p.Next(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch first page for estimation: %w", err)
	}

	// 重置分页器状态，但保留已知的总数信息
	totalCount := response.Data.TotalCount
	p.Reset()

	logger.Info("Alibaba Cloud paginator estimated total record count",
		zap.String("provider", "alicloud"),
		zap.Int32("total_count", totalCount))
	return totalCount, nil
}

// GetPaginationInfo 获取分页信息
func (p *Paginator) GetPaginationInfo() map[string]interface{} {
	return map[string]interface{}{
		"current_page":  p.currentPage,
		"has_more":      p.hasMore,
		"total_fetched": p.totalFetched,
		"next_token":    p.truncateToken(p.nextToken),
		"elapsed_time":  time.Since(p.startTime),
		"granularity":   p.baseRequest.Granularity,
		"billing_cycle": p.baseRequest.BillingCycle,
		"billing_date":  p.baseRequest.BillingDate,
	}
}

// truncateToken shortens the token for safe logging
func (p *Paginator) truncateToken(token string) string {
	if len(token) <= 20 {
		return token
	}
	return token[:10] + "..." + token[len(token)-7:]
}

// UpdateRequest 更新基础请求参数
func (p *Paginator) UpdateRequest(updater func(*DescribeInstanceBillRequest)) {
	if updater != nil {
		updater(p.baseRequest)
	}
}

// Clone 克隆分页器（用于并发场景）
func (p *Paginator) Clone() *Paginator {
	// 复制基础请求
	requestCopy := *p.baseRequest

	return &Paginator{
		client:       p.client,
		baseRequest:  &requestCopy,
		nextToken:    "",
		hasMore:      true,
		currentPage:  0,
		totalFetched: 0,
		startTime:    time.Now(),
	}
}

// GetStats 获取分页统计信息
func (p *Paginator) GetStats() *PaginationStats {
	elapsed := time.Since(p.startTime)
	avgSpeed := float64(0)
	if elapsed > 0 && p.totalFetched > 0 {
		avgSpeed = float64(p.totalFetched) / elapsed.Seconds()
	}

	return &PaginationStats{
		StartTime:    p.startTime,
		CurrentPage:  p.currentPage,
		TotalFetched: p.totalFetched,
		HasMore:      p.hasMore,
		ElapsedTime:  elapsed,
		AverageSpeed: avgSpeed,
		Granularity:  p.baseRequest.Granularity,
		BillingCycle: p.baseRequest.BillingCycle,
		BillingDate:  p.baseRequest.BillingDate,
	}
}

// PaginationStats 分页统计信息
type PaginationStats struct {
	StartTime    time.Time     `json:"start_time"`
	CurrentPage  int           `json:"current_page"`
	TotalFetched int           `json:"total_fetched"`
	HasMore      bool          `json:"has_more"`
	ElapsedTime  time.Duration `json:"elapsed_time"`
	AverageSpeed float64       `json:"average_speed"` // records per second
	Granularity  string        `json:"granularity"`
	BillingCycle string        `json:"billing_cycle"`
	BillingDate  string        `json:"billing_date"`
}

// String 返回统计信息的字符串表示
func (ps *PaginationStats) String() string {
	granularityInfo := ps.Granularity
	if ps.BillingDate != "" {
		granularityInfo += fmt.Sprintf("(%s)", ps.BillingDate)
	}

	return fmt.Sprintf("PaginationStats{Page: %d, Fetched: %d, Speed: %.1f records/s, Elapsed: %v, %s}",
		ps.CurrentPage, ps.TotalFetched, ps.AverageSpeed, ps.ElapsedTime, granularityInfo)
}

// MultiplePaginator 多粒度分页器
// 用于同时处理按月和按天的分页
type MultiplePaginator struct {
	monthlyPaginator *Paginator
	dailyPaginators  map[string]*Paginator // key: billing_date
	client           *Client
	billingCycle     string
}

// NewMultiplePaginator 创建多粒度分页器
func NewMultiplePaginator(client *Client, billingCycle string, maxResults int32) *MultiplePaginator {
	return &MultiplePaginator{
		monthlyPaginator: NewPaginator(client, &DescribeInstanceBillRequest{
			BillingCycle: billingCycle,
			Granularity:  "MONTHLY",
			MaxResults:   maxResults,
		}),
		dailyPaginators: make(map[string]*Paginator),
		client:          client,
		billingCycle:    billingCycle,
	}
}

// InitializeDailyPaginators 初始化按天分页器
func (mp *MultiplePaginator) InitializeDailyPaginators(maxResults int32) error {
	// 生成该月份的所有日期
	dates, err := GenerateDatesInMonth(mp.billingCycle)
	if err != nil {
		return fmt.Errorf("failed to generate dates for cycle %s: %w", mp.billingCycle, err)
	}

	// 为每个日期创建分页器
	for _, date := range dates {
		mp.dailyPaginators[date] = NewPaginator(mp.client, &DescribeInstanceBillRequest{
			BillingCycle: mp.billingCycle,
			Granularity:  "DAILY",
			BillingDate:  date,
			MaxResults:   maxResults,
		})
	}

	logger.Info("Alibaba Cloud multi-granularity paginator initialization completed",
		zap.String("provider", "alicloud"),
		zap.Int("monthly_paginators", 1),
		zap.Int("daily_paginators", len(dates)))
	return nil
}

// FetchMonthlyData 获取按月数据
func (mp *MultiplePaginator) FetchMonthlyData(ctx context.Context) ([]BillDetail, error) {
	return mp.monthlyPaginator.FetchAll(ctx)
}

// FetchDailyData 获取按天数据
func (mp *MultiplePaginator) FetchDailyData(ctx context.Context) (map[string][]BillDetail, error) {
	result := make(map[string][]BillDetail)

	for date, paginator := range mp.dailyPaginators {
		bills, err := paginator.FetchAll(ctx)
		if err != nil {
			logger.Error("Alibaba Cloud multi-granularity paginator date fetching failed",
				zap.String("provider", "alicloud"),
				zap.String("date", date),
				zap.Error(err))
			continue // 跳过失败的日期
		}

		if len(bills) > 0 {
			result[date] = bills
			logger.Debug("Alibaba Cloud multi-granularity paginator date data",
				zap.String("provider", "alicloud"),
				zap.String("date", date),
				zap.Int("record_count", len(bills)))
		}

		// 速率控制已由RateLimiter处理，这里只需要极短延迟避免CPU占用
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return result, ctx.Err()
		}
	}

	return result, nil
}

// GetOverallStats 获取整体统计信息
func (mp *MultiplePaginator) GetOverallStats() map[string]interface{} {
	monthlyStats := mp.monthlyPaginator.GetStats()

	dailyStats := make(map[string]*PaginationStats)
	totalDailyRecords := 0
	for date, paginator := range mp.dailyPaginators {
		stats := paginator.GetStats()
		dailyStats[date] = stats
		totalDailyRecords += stats.TotalFetched
	}

	return map[string]interface{}{
		"billing_cycle":         mp.billingCycle,
		"monthly_stats":         monthlyStats,
		"daily_stats":           dailyStats,
		"total_daily_records":   totalDailyRecords,
		"daily_paginator_count": len(mp.dailyPaginators),
	}
}
