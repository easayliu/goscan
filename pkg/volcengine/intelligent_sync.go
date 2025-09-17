package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/logger"
	"time"

	"go.uber.org/zap"
)

// IntelligentSyncWithPagination 智能分页同步（支持处理所有数据）
func (s *billServiceImpl) IntelligentSyncWithPagination(ctx context.Context, billPeriod string, tableName string, isDistributed bool) (*SyncResult, error) {
	logger.Info("Volcengine intelligent pagination sync started",
		zap.String("provider", "volcengine"),
		zap.String("bill_period", billPeriod))

	result := &SyncResult{
		StartTime: time.Now(),
	}

	// 1. 创建数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(500) // 设置批次大小
	processor.EnableProgressLogging()

	// 2. 创建分页器
	paginator := NewBillPaginator(s.volcClient, processor)

	// 设置进度回调
	paginator.SetProgressCallback(func(current, total int, duration time.Duration) {
		progress := float64(current) / float64(total) * 100
		logger.Debug("Volcengine sync progress",
			zap.String("provider", "volcengine"),
			zap.Float64("progress_percent", progress),
			zap.Int("current", current),
			zap.Int("total", total),
			zap.Duration("duration", duration))
	})

	// 3. 准备请求参数
	req := &ListBillDetailRequest{
		BillPeriod:    billPeriod,
		Limit:         100, // 每页100条
		Offset:        0,
		NeedRecordNum: 1, // 需要返回总记录数
		GroupTerm:     1, // 分组条件
		IgnoreZero:    1, // 忽略零元账单
	}

	// 4. 执行分页同步
	paginateResult, err := paginator.PaginateBillDetails(ctx, req)
	if err != nil {
		result.Error = err
		return result, fmt.Errorf("pagination sync failed: %w", err)
	}

	// 5. 更新结果
	result.TotalRecords = paginateResult.TotalRecords
	result.InsertedRecords = paginateResult.ProcessedRecords
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	// 6. 打印最终统计
	logger.Info("Volcengine intelligent pagination sync completed",
		zap.String("provider", "volcengine"),
		zap.Int("total_records", result.TotalRecords),
		zap.Int("inserted_records", result.InsertedRecords),
		zap.Duration("duration", result.Duration),
		zap.Float64("avg_speed_records_per_sec", float64(result.InsertedRecords)/result.Duration.Seconds()))

	if len(paginateResult.Errors) > 0 {
		logger.Warn("Volcengine sync warning, errors occurred during process",
			zap.String("provider", "volcengine"),
			zap.Int("error_count", len(paginateResult.Errors)))
		for i, err := range paginateResult.Errors {
			if i < 5 { // 只显示前5个错误
				logger.Warn("Volcengine sync error details",
					zap.String("provider", "volcengine"),
					zap.Int("error_index", i+1),
					zap.Error(err))
			}
		}
	}

	return result, nil
}

// SmartSyncAllData 智能同步所有数据（边获取边写入）
func (s *billServiceImpl) SmartSyncAllData(ctx context.Context, billPeriod string, tableName string, isDistributed bool) (*SyncResult, error) {
	logger.Info("Volcengine intelligent sync started",
		zap.String("provider", "volcengine"),
		zap.String("bill_period", billPeriod))

	result := &SyncResult{
		StartTime: time.Now(),
	}

	// 创建请求参数
	// 使用配置中的 batch_size
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 100 // 默认值
	}

	req := &ListBillDetailRequest{
		BillPeriod:    billPeriod,
		Limit:         int32(batchSize),
		Offset:        0,
		NeedRecordNum: 1, // 第一次请求需要总记录数
		GroupTerm:     1, // 分组条件
		IgnoreZero:    1, // 忽略零元账单
	}

	// 第一次请求，获取总记录数
	logger.Info("Volcengine intelligent sync getting total data count",
		zap.String("provider", "volcengine"))
	firstResp, err := s.volcClient.ListBillDetail(ctx, req)
	if err != nil {
		return result, fmt.Errorf("failed to fetch first page data: %w", err)
	}

	totalRecords := firstResp.Result.Total
	logger.Info("Volcengine intelligent sync total records",
		zap.String("provider", "volcengine"),
		zap.Int32("total_records", totalRecords))

	if totalRecords == 0 {
		logger.Info("Volcengine intelligent sync no data to sync",
			zap.String("provider", "volcengine"))
		result.TotalRecords = 0
		result.InsertedRecords = 0
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result, nil
	}

	// 创建数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(500)
	processor.EnableProgressLogging()

	totalProcessed := 0
	pageNum := 0
	offset := int32(0)

	// 循环获取所有页数据
	for offset < totalRecords {
		pageNum++

		// 准备当前页请求
		pageReq := &ListBillDetailRequest{
			BillPeriod:    billPeriod,
			Limit:         int32(batchSize),
			Offset:        offset,
			NeedRecordNum: 0, // 后续请求不需要总记录数
			GroupTerm:     1, // 分组条件
			IgnoreZero:    1, // 忽略零元账单
		}

		logger.Info("Volcengine intelligent sync fetching page data",
			zap.String("provider", "volcengine"),
			zap.Int("page_num", pageNum),
			zap.Int32("offset", offset),
			zap.Int32("limit", pageReq.Limit))

		// 获取当前页数据
		resp, err := s.volcClient.ListBillDetail(ctx, pageReq)
		if err != nil {
			logger.Warn("Volcengine intelligent sync failed to fetch page data",
				zap.String("provider", "volcengine"),
				zap.Int("page_num", pageNum),
				zap.Error(err))
			// 继续处理下一页
			offset += pageReq.Limit
			continue
		}

		if len(resp.Result.List) == 0 {
			logger.Info("Volcengine intelligent sync page has no data, ending fetch",
				zap.String("provider", "volcengine"),
				zap.Int("page_num", pageNum))
			break
		}

		// 立即处理这一页的数据
		processResult, err := processor.ProcessWithResult(ctx, resp.Result.List)
		if err != nil {
			logger.Warn("Volcengine intelligent sync page processing failed",
				zap.String("provider", "volcengine"),
				zap.Int("page_num", pageNum),
				zap.Error(err))
			// 继续处理下一页
		} else {
			totalProcessed += processResult.InsertedRecords
			logger.Info("Volcengine intelligent sync page processing completed",
				zap.String("provider", "volcengine"),
				zap.Int("page_num", pageNum),
				zap.Int("inserted_records", processResult.InsertedRecords))
		}

		// 显示总体进度
		progress := float64(offset+int32(len(resp.Result.List))) / float64(totalRecords) * 100
		logger.Debug("Volcengine overall progress",
			zap.String("provider", "volcengine"),
			zap.Float64("progress_percent", progress),
			zap.Int32("processed", offset+int32(len(resp.Result.List))),
			zap.Int32("total", totalRecords))

		// 更新offset
		offset += pageReq.Limit

		// 如果当前页数据少于限制，说明已经是最后一页
		if int32(len(resp.Result.List)) < pageReq.Limit {
			logger.Info("Volcengine intelligent sync current page data insufficient, reached last page",
				zap.String("provider", "volcengine"))
			break
		}

		// 添加短暂延迟，避免API限流
		time.Sleep(200 * time.Millisecond)
	}

	// 更新最终结果
	result.TotalRecords = int(totalRecords)
	result.InsertedRecords = totalProcessed
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	// 打印最终统计
	logger.Info("Volcengine intelligent sync completed",
		zap.String("provider", "volcengine"),
		zap.Int("total_pages", pageNum),
		zap.Int("total_records", result.TotalRecords),
		zap.Int("inserted_records", result.InsertedRecords),
		zap.Duration("duration", result.Duration),
		zap.Float64("avg_speed_records_per_sec",
			func() float64 {
				if result.Duration.Seconds() > 0 {
					return float64(result.InsertedRecords) / result.Duration.Seconds()
				}
				return 0
			}()))

	return result, nil
}

// SyncAllDataWithRetry 带重试的智能同步
func (s *billServiceImpl) SyncAllDataWithRetry(ctx context.Context, billPeriod string, tableName string, isDistributed bool, maxRetries int) (*SyncResult, error) {
	var lastErr error
	var bestResult *SyncResult

	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Info("Volcengine intelligent sync retry attempt",
			zap.String("provider", "volcengine"),
			zap.Int("attempt", attempt),
			zap.String("bill_period", billPeriod))

		result, err := s.SmartSyncAllData(ctx, billPeriod, tableName, isDistributed)
		if err == nil && result.InsertedRecords > 0 {
			logger.Info("Volcengine intelligent sync attempt successful",
				zap.String("provider", "volcengine"),
				zap.Int("attempt", attempt))
			return result, nil
		}

		// 保存最好的结果
		if bestResult == nil || (result != nil && result.InsertedRecords > bestResult.InsertedRecords) {
			bestResult = result
		}

		lastErr = err
		if attempt < maxRetries {
			delay := time.Duration(attempt) * 5 * time.Second
			logger.Warn("Volcengine intelligent sync attempt failed, will retry",
				zap.String("provider", "volcengine"),
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay),
				zap.Error(err))
			time.Sleep(delay)
		}
	}

	if bestResult != nil && bestResult.InsertedRecords > 0 {
		logger.Warn("Volcengine intelligent sync partially successful",
			zap.String("provider", "volcengine"),
			zap.Int("inserted_records", bestResult.InsertedRecords),
			zap.Int("total_records", bestResult.TotalRecords))
		return bestResult, nil
	}

	return nil, fmt.Errorf("intelligent sync failed after %d retries: %w", maxRetries, lastErr)
}
