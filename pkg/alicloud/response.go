package alicloud

import (
	"fmt"
	"time"
)

// DescribeInstanceBillResponse 查询实例账单响应
type DescribeInstanceBillResponse struct {
	RequestId string             `json:"RequestId"` // 请求ID
	Success   bool               `json:"Success"`   // 是否成功
	Code      string             `json:"Code"`      // 响应代码
	Message   string             `json:"Message"`   // 响应消息
	Data      BillInstanceResult `json:"Data"`      // 账单数据
}

// BillInstanceResult 账单实例结果
type BillInstanceResult struct {
	BillingCycle string       `json:"BillingCycle"` // 账期
	AccountID    string       `json:"AccountID"`    // 账号ID
	AccountName  string       `json:"AccountName"`  // 账号名称
	TotalCount   int32        `json:"TotalCount"`   // 总记录数
	NextToken    string       `json:"NextToken"`    // 下一页标记
	MaxResults   int32        `json:"MaxResults"`   // 页大小
	Items        []BillDetail `json:"Items"`        // 账单明细列表
}

// IsSuccess 判断响应是否成功
func (resp *DescribeInstanceBillResponse) IsSuccess() bool {
	return resp.Success && resp.Code == "Success"
}

// HasError 判断响应是否有错误
func (resp *DescribeInstanceBillResponse) HasError() bool {
	return !resp.IsSuccess()
}

// GetError 获取错误信息
func (resp *DescribeInstanceBillResponse) GetError() error {
	if resp.IsSuccess() {
		return nil
	}

	return NewAPIError(resp.Code, resp.Message, "", 0)
}

// HasMorePages 判断是否还有更多页
func (resp *DescribeInstanceBillResponse) HasMorePages() bool {
	return resp.Data.NextToken != ""
}

// GetNextToken 获取下一页标记
func (resp *DescribeInstanceBillResponse) GetNextToken() string {
	return resp.Data.NextToken
}

// GetPageSize 获取当前页大小
func (resp *DescribeInstanceBillResponse) GetPageSize() int {
	return len(resp.Data.Items)
}

// GetTotalCount 获取总记录数
func (resp *DescribeInstanceBillResponse) GetTotalCount() int32 {
	return resp.Data.TotalCount
}

// String 返回响应的字符串表示
func (resp *DescribeInstanceBillResponse) String() string {
	status := "Success"
	if resp.HasError() {
		status = fmt.Sprintf("Error(%s)", resp.Code)
	}

	return fmt.Sprintf("DescribeInstanceBillResponse{Status: %s, Items: %d, TotalCount: %d, HasMore: %v}",
		status, resp.GetPageSize(), resp.GetTotalCount(), resp.HasMorePages())
}

// DataComparisonResult 数据比较结果
type DataComparisonResult struct {
	APICount      int32  // API返回的数据总数
	DatabaseCount int64  // 数据库中的记录总数
	NeedSync      bool   // 是否需要同步
	NeedCleanup   bool   // 是否需要先清理数据
	Reason        string // 决策原因
	Period        string // 时间段
	Granularity   string // 粒度（daily/monthly）
}

// String 返回比较结果的字符串表示
func (dcr *DataComparisonResult) String() string {
	return fmt.Sprintf("DataComparison{%s %s: API=%d, DB=%d, Sync=%v, Cleanup=%v, Reason=%s}",
		dcr.Granularity, dcr.Period, dcr.APICount, dcr.DatabaseCount,
		dcr.NeedSync, dcr.NeedCleanup, dcr.Reason)
}

// PreSyncCheckResult 同步前检查结果
type PreSyncCheckResult struct {
	ShouldSkip bool                    // 是否跳过同步
	Results    []*DataComparisonResult // 比较结果列表（支持多个时间段）
	Summary    string                  // 检查摘要
}

// String 返回检查结果的字符串表示
func (pscr *PreSyncCheckResult) String() string {
	return fmt.Sprintf("PreSyncCheck{ShouldSkip=%v, Results=%d, Summary=%s}",
		pscr.ShouldSkip, len(pscr.Results), pscr.Summary)
}

// GetTotalAPICount 获取所有结果的API数据总数
func (pscr *PreSyncCheckResult) GetTotalAPICount() int32 {
	var total int32
	for _, result := range pscr.Results {
		total += result.APICount
	}
	return total
}

// GetTotalDatabaseCount 获取所有结果的数据库记录总数
func (pscr *PreSyncCheckResult) GetTotalDatabaseCount() int64 {
	var total int64
	for _, result := range pscr.Results {
		total += result.DatabaseCount
	}
	return total
}

// GetSyncCount 获取需要同步的结果数量
func (pscr *PreSyncCheckResult) GetSyncCount() int {
	count := 0
	for _, result := range pscr.Results {
		if result.NeedSync {
			count++
		}
	}
	return count
}

// GetCleanupCount 获取需要清理的结果数量
func (pscr *PreSyncCheckResult) GetCleanupCount() int {
	count := 0
	for _, result := range pscr.Results {
		if result.NeedCleanup {
			count++
		}
	}
	return count
}

// ProcessingStats 处理统计信息
type ProcessingStats struct {
	StartTime        time.Time     `json:"start_time"`
	LastUpdateTime   time.Time     `json:"last_update_time"`
	TotalRecords     int           `json:"total_records"`
	ProcessedRecords int           `json:"processed_records"`
	CurrentBatch     int           `json:"current_batch"`
	TotalBatches     int           `json:"total_batches"`
	AverageSpeed     float64       `json:"average_speed"`  // records per second
	EstimatedTime    time.Duration `json:"estimated_time"` // remaining time
	Granularity      string        `json:"granularity"`    // MONTHLY 或 DAILY
}

// Update 更新统计信息
func (ps *ProcessingStats) Update(processedRecords int) {
	now := time.Now()
	ps.LastUpdateTime = now
	ps.ProcessedRecords = processedRecords

	// 计算平均速度
	elapsed := now.Sub(ps.StartTime)
	if elapsed > 0 {
		ps.AverageSpeed = float64(processedRecords) / elapsed.Seconds()
	}

	// 估算剩余时间
	if ps.AverageSpeed > 0 && ps.TotalRecords > 0 {
		remaining := ps.TotalRecords - processedRecords
		if remaining > 0 {
			ps.EstimatedTime = time.Duration(float64(remaining)/ps.AverageSpeed) * time.Second
		}
	}
}

// GetProgress 获取进度百分比
func (ps *ProcessingStats) GetProgress() float64 {
	if ps.TotalRecords == 0 {
		return 0
	}
	return float64(ps.ProcessedRecords) / float64(ps.TotalRecords) * 100
}

// String 返回统计信息的字符串表示
func (ps *ProcessingStats) String() string {
	progress := ps.GetProgress()
	granularityInfo := ""
	if ps.Granularity != "" {
		granularityInfo = fmt.Sprintf(" [%s]", ps.Granularity)
	}
	return fmt.Sprintf("Progress: %.1f%% (%d/%d)%s, Speed: %.1f records/s, ETA: %v",
		progress, ps.ProcessedRecords, ps.TotalRecords, granularityInfo, ps.AverageSpeed, ps.EstimatedTime)
}

// IsCompleted 判断是否已完成
func (ps *ProcessingStats) IsCompleted() bool {
	return ps.ProcessedRecords >= ps.TotalRecords && ps.TotalRecords > 0
}

// GetElapsedTime 获取已用时间
func (ps *ProcessingStats) GetElapsedTime() time.Duration {
	if ps.LastUpdateTime.IsZero() {
		return time.Since(ps.StartTime)
	}
	return ps.LastUpdateTime.Sub(ps.StartTime)
}

// BatchTransformationResult 批量转换结果
type BatchTransformationResult struct {
	TransformedRecords []*BillDetailForDB `json:"transformed_records"`
	Stats              *ProcessingStats   `json:"stats"`
	Errors             []error            `json:"errors,omitempty"`
	Granularity        string             `json:"granularity"`
}

// IsSuccess 检查批量转换是否成功
func (btr *BatchTransformationResult) IsSuccess() bool {
	return len(btr.Errors) == 0
}

// GetSuccessRate 获取成功率
func (btr *BatchTransformationResult) GetSuccessRate() float64 {
	if btr.Stats.TotalRecords == 0 {
		return 0
	}
	return float64(btr.Stats.ProcessedRecords) / float64(btr.Stats.TotalRecords) * 100
}

// GetErrorCount 获取错误数量
func (btr *BatchTransformationResult) GetErrorCount() int {
	return len(btr.Errors)
}

// String 返回转换结果的字符串表示
func (btr *BatchTransformationResult) String() string {
	return fmt.Sprintf("BatchTransformationResult{Success=%v, Records=%d, Errors=%d, SuccessRate=%.1f%%, Granularity=%s}",
		btr.IsSuccess(), len(btr.TransformedRecords), btr.GetErrorCount(), btr.GetSuccessRate(), btr.Granularity)
}

// ResponseCache 响应缓存
type ResponseCache struct {
	responses  map[string]*DescribeInstanceBillResponse
	ttl        time.Duration
	timestamps map[string]time.Time
}

// NewResponseCache 创建响应缓存
func NewResponseCache(ttl time.Duration) *ResponseCache {
	return &ResponseCache{
		responses:  make(map[string]*DescribeInstanceBillResponse),
		ttl:        ttl,
		timestamps: make(map[string]time.Time),
	}
}

// Get 获取缓存的响应
func (rc *ResponseCache) Get(key string) (*DescribeInstanceBillResponse, bool) {
	timestamp, exists := rc.timestamps[key]
	if !exists {
		return nil, false
	}

	// 检查是否过期
	if time.Since(timestamp) > rc.ttl {
		delete(rc.responses, key)
		delete(rc.timestamps, key)
		return nil, false
	}

	response, exists := rc.responses[key]
	return response, exists
}

// Put 放入响应到缓存
func (rc *ResponseCache) Put(key string, response *DescribeInstanceBillResponse) {
	rc.responses[key] = response
	rc.timestamps[key] = time.Now()
}

// Clear 清空缓存
func (rc *ResponseCache) Clear() {
	rc.responses = make(map[string]*DescribeInstanceBillResponse)
	rc.timestamps = make(map[string]time.Time)
}

// Size 获取缓存大小
func (rc *ResponseCache) Size() int {
	return len(rc.responses)
}

// ResponseMetrics 响应指标
type ResponseMetrics struct {
	TotalRequests    int64         `json:"total_requests"`
	SuccessRequests  int64         `json:"success_requests"`
	ErrorRequests    int64         `json:"error_requests"`
	TotalRecords     int64         `json:"total_records"`
	AverageLatency   time.Duration `json:"average_latency"`
	MaxLatency       time.Duration `json:"max_latency"`
	MinLatency       time.Duration `json:"min_latency"`
	LastRequestTime  time.Time     `json:"last_request_time"`
	FirstRequestTime time.Time     `json:"first_request_time"`
}

// RecordRequest 记录请求
func (rm *ResponseMetrics) RecordRequest(latency time.Duration, success bool, recordCount int) {
	now := time.Now()

	rm.TotalRequests++
	rm.TotalRecords += int64(recordCount)
	rm.LastRequestTime = now

	if rm.FirstRequestTime.IsZero() {
		rm.FirstRequestTime = now
	}

	if success {
		rm.SuccessRequests++
	} else {
		rm.ErrorRequests++
	}

	// 更新延迟统计
	if rm.MaxLatency < latency {
		rm.MaxLatency = latency
	}

	if rm.MinLatency == 0 || rm.MinLatency > latency {
		rm.MinLatency = latency
	}

	// 计算平均延迟（简单移动平均）
	if rm.TotalRequests == 1 {
		rm.AverageLatency = latency
	} else {
		rm.AverageLatency = (rm.AverageLatency*time.Duration(rm.TotalRequests-1) + latency) / time.Duration(rm.TotalRequests)
	}
}

// GetSuccessRate 获取成功率
func (rm *ResponseMetrics) GetSuccessRate() float64 {
	if rm.TotalRequests == 0 {
		return 0
	}
	return float64(rm.SuccessRequests) / float64(rm.TotalRequests) * 100
}

// GetErrorRate 获取错误率
func (rm *ResponseMetrics) GetErrorRate() float64 {
	if rm.TotalRequests == 0 {
		return 0
	}
	return float64(rm.ErrorRequests) / float64(rm.TotalRequests) * 100
}

// String 返回指标的字符串表示
func (rm *ResponseMetrics) String() string {
	return fmt.Sprintf("ResponseMetrics{Requests=%d, Success=%.1f%%, Records=%d, AvgLatency=%v}",
		rm.TotalRequests, rm.GetSuccessRate(), rm.TotalRecords, rm.AverageLatency)
}
