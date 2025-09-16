package volcengine

import (
	"context"
	"fmt"
	"log"
	"time"
)

// IntelligentSyncWithPagination 智能分页同步（支持处理所有数据）
func (s *billServiceImpl) IntelligentSyncWithPagination(ctx context.Context, billPeriod string, tableName string, isDistributed bool) (*SyncResult, error) {
	log.Printf("🚀 [智能分页同步] 开始同步账期 %s 的所有数据", billPeriod)
	
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
		log.Printf("📊 [同步进度] %.1f%% (%d/%d) - 已用时: %v", 
			progress, current, total, duration)
	})
	
	// 3. 准备请求参数
	req := &ListBillDetailRequest{
		BillPeriod: billPeriod,
		Limit:      100, // 每页100条
		Offset:     0,
		NeedRecordNum: 1, // 需要返回总记录数
	}
	
	// 4. 执行分页同步
	paginateResult, err := paginator.PaginateBillDetails(ctx, req)
	if err != nil {
		result.Error = err
		return result, fmt.Errorf("分页同步失败: %w", err)
	}
	
	// 5. 更新结果
	result.TotalRecords = paginateResult.TotalRecords
	result.InsertedRecords = paginateResult.ProcessedRecords
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	
	// 6. 打印最终统计
	log.Printf("✅ [智能分页同步完成]")
	log.Printf("   总记录数: %d", result.TotalRecords)
	log.Printf("   成功插入: %d", result.InsertedRecords)
	log.Printf("   总耗时: %v", result.Duration)
	log.Printf("   平均速度: %.1f records/s", float64(result.InsertedRecords)/result.Duration.Seconds())
	
	if len(paginateResult.Errors) > 0 {
		log.Printf("⚠️ [同步警告] 过程中有 %d 个错误:", len(paginateResult.Errors))
		for i, err := range paginateResult.Errors {
			if i < 5 { // 只显示前5个错误
				log.Printf("   错误 %d: %v", i+1, err)
			}
		}
	}
	
	return result, nil
}

// SmartSyncAllData 智能同步所有数据（边获取边写入）
func (s *billServiceImpl) SmartSyncAllData(ctx context.Context, billPeriod string, tableName string, isDistributed bool) (*SyncResult, error) {
	log.Printf("🚀 [智能同步] 开始智能同步账期 %s 的所有数据", billPeriod)
	
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
		NeedRecordNum: 1,    // 第一次请求需要总记录数
	}
	
	// 第一次请求，获取总记录数
	log.Printf("📊 [智能同步] 获取数据总量...")
	firstResp, err := s.volcClient.ListBillDetail(ctx, req)
	if err != nil {
		return result, fmt.Errorf("获取首页数据失败: %w", err)
	}
	
	totalRecords := firstResp.Result.Total
	log.Printf("📊 [智能同步] 总记录数: %d", totalRecords)
	
	if totalRecords == 0 {
		log.Printf("ℹ️ [智能同步] 没有数据需要同步")
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
		}
		
		log.Printf("📄 [智能同步] 获取第 %d 页数据 (offset: %d, limit: %d)", 
			pageNum, offset, pageReq.Limit)
		
		// 获取当前页数据
		resp, err := s.volcClient.ListBillDetail(ctx, pageReq)
		if err != nil {
			log.Printf("❌ [智能同步] 获取第 %d 页数据失败: %v", pageNum, err)
			// 继续处理下一页
			offset += pageReq.Limit
			continue
		}
		
		if len(resp.Result.List) == 0 {
			log.Printf("ℹ️ [智能同步] 第 %d 页没有数据，结束获取", pageNum)
			break
		}
		
		
		// 立即处理这一页的数据
		processResult, err := processor.ProcessWithResult(ctx, resp.Result.List)
		if err != nil {
			log.Printf("⚠️ [智能同步] 第 %d 页处理失败: %v", pageNum, err)
			// 继续处理下一页
		} else {
			totalProcessed += processResult.InsertedRecords
			log.Printf("✅ [智能同步] 第 %d 页处理完成，成功插入 %d 条", 
				pageNum, processResult.InsertedRecords)
		}
		
		// 显示总体进度
		progress := float64(offset + int32(len(resp.Result.List))) / float64(totalRecords) * 100
		log.Printf("📊 [总体进度] %.1f%% (%d/%d)", 
			progress, offset + int32(len(resp.Result.List)), totalRecords)
		
		// 更新offset
		offset += pageReq.Limit
		
		// 如果当前页数据少于限制，说明已经是最后一页
		if int32(len(resp.Result.List)) < pageReq.Limit {
			log.Printf("ℹ️ [智能同步] 当前页数据不足，已到达最后一页")
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
	log.Printf("🎉 [智能同步完成]")
	log.Printf("   总页数: %d", pageNum)
	log.Printf("   总记录数: %d", result.TotalRecords)
	log.Printf("   成功插入: %d", result.InsertedRecords)
	log.Printf("   总耗时: %v", result.Duration)
	if result.Duration.Seconds() > 0 {
		log.Printf("   平均速度: %.1f records/s", 
			float64(result.InsertedRecords)/result.Duration.Seconds())
	}
	
	return result, nil
}

// SyncAllDataWithRetry 带重试的智能同步
func (s *billServiceImpl) SyncAllDataWithRetry(ctx context.Context, billPeriod string, tableName string, isDistributed bool, maxRetries int) (*SyncResult, error) {
	var lastErr error
	var bestResult *SyncResult
	
	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("🔄 [智能同步] 第 %d 次尝试同步账期 %s", attempt, billPeriod)
		
		result, err := s.SmartSyncAllData(ctx, billPeriod, tableName, isDistributed)
		if err == nil && result.InsertedRecords > 0 {
			log.Printf("✅ [智能同步] 第 %d 次尝试成功", attempt)
			return result, nil
		}
		
		// 保存最好的结果
		if bestResult == nil || (result != nil && result.InsertedRecords > bestResult.InsertedRecords) {
			bestResult = result
		}
		
		lastErr = err
		if attempt < maxRetries {
			delay := time.Duration(attempt) * 5 * time.Second
			log.Printf("⏳ [智能同步] 第 %d 次尝试失败，%v 后重试: %v", 
				attempt, delay, err)
			time.Sleep(delay)
		}
	}
	
	if bestResult != nil && bestResult.InsertedRecords > 0 {
		log.Printf("⚠️ [智能同步] 部分成功，插入了 %d/%d 条记录", 
			bestResult.InsertedRecords, bestResult.TotalRecords)
		return bestResult, nil
	}
	
	return nil, fmt.Errorf("智能同步失败，已重试 %d 次: %w", maxRetries, lastErr)
}