package alicloud

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

// SyncManager 同步管理器实现
type syncManager struct {
	service *BillService
}

// NewSyncManager 创建同步管理器
func NewSyncManager(service *BillService) SyncManager {
	return &syncManager{service: service}
}

// SyncData 同步数据的通用方法
func (sm *syncManager) SyncData(ctx context.Context, req *SyncRequest) (*SyncResult, error) {
	start := time.Now()
	result := &SyncResult{
		Success: false,
		Duration: 0,
	}

	// 验证请求
	if err := sm.validateSyncRequest(req); err != nil {
		result.Error = err.Error()
		return result, err
	}

	// 根据粒度执行相应的同步
	var err error
	switch strings.ToUpper(req.Granularity) {
	case "MONTHLY":
		err = sm.syncMonthlyBillDataImpl(ctx, req.Period, req.Options)
	case "DAILY":
		err = sm.syncSpecificDayBillDataImpl(ctx, req.Period, req.Options)
	case "BOTH":
		err = sm.syncBothGranularityDataImpl(ctx, req.Period, req.Options)
	default:
		err = fmt.Errorf("unsupported granularity: %s", req.Granularity)
	}

	result.Duration = time.Since(start)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	result.Success = true
	return result, nil
}

// IntelligentSync 智能同步
func (sm *syncManager) IntelligentSync(ctx context.Context, params *IntelligentSyncParams) (*SyncResult, error) {
	start := time.Now()
	result := &SyncResult{
		Success: false,
		Duration: 0,
	}

	// 执行预检查
	if params.EnablePreCheck {
		checkResult, err := sm.PreSyncCheck(ctx, params.Granularity, params.Period)
		if err != nil {
			result.Error = fmt.Sprintf("pre-check failed: %v", err)
			result.Duration = time.Since(start)
			return result, err
		}

		if checkResult.ShouldSkip {
			result.Success = true
			result.Duration = time.Since(start)
			result.Details = checkResult
			log.Printf("[阿里云智能同步] %s", checkResult.Summary)
			return result, nil
		}

		// 执行智能清理和同步
		for _, compResult := range checkResult.Results {
			if err := sm.executeIntelligentCleanupAndSyncImpl(ctx, compResult, params.SyncOptions); err != nil {
				result.Error = err.Error()
				result.Duration = time.Since(start)
				return result, err
			}
		}
	} else {
		// 直接同步，不执行预检查
		req := &SyncRequest{
			Granularity: params.Granularity,
			Period:      params.Period,
			Options:     params.SyncOptions,
		}
		
		return sm.SyncData(ctx, req)
	}

	result.Success = true
	result.Duration = time.Since(start)
	return result, nil
}

// PreSyncCheck 同步前检查
func (sm *syncManager) PreSyncCheck(ctx context.Context, granularity, period string) (*PreSyncCheckResult, error) {
	return sm.service.PerformPreSyncCheck(ctx, granularity, period)
}

// CleanupBeforeSync 同步前清理
func (sm *syncManager) CleanupBeforeSync(ctx context.Context, granularity, period string) error {
	return sm.service.CleanSpecificPeriodData(ctx, granularity, period)
}

// validateSyncRequest 验证同步请求
func (sm *syncManager) validateSyncRequest(req *SyncRequest) error {
	if req == nil {
		return NewValidationError("request", nil, "sync request cannot be nil")
	}

	if req.Granularity == "" {
		return NewValidationError("granularity", req.Granularity, "granularity cannot be empty")
	}

	if req.Period == "" {
		return NewValidationError("period", req.Period, "period cannot be empty")
	}

	validator := NewValidator()
	if err := validator.ValidateGranularity(req.Granularity); err != nil {
		return err
	}

	// 根据粒度验证周期格式
	switch strings.ToUpper(req.Granularity) {
	case "MONTHLY":
		return validator.ValidateBillingCycle(req.Period)
	case "DAILY":
		return validator.ValidateBillingDate(req.Period)
	case "BOTH":
		// BOTH 粒度的特殊格式处理
		if !strings.Contains(req.Period, ",") {
			return NewValidationError("period", req.Period, 
				"BOTH granularity requires period format: 'yesterday:YYYY-MM-DD,last_month:YYYY-MM'")
		}
	}

	return nil
}

// syncMonthlyBillDataImpl 同步按月账单数据的实现
func (sm *syncManager) syncMonthlyBillDataImpl(ctx context.Context, billingCycle string, options *SyncOptions) error {
	log.Printf("[阿里云按月同步] 开始同步账期: %s", billingCycle)

	// 验证账期
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return fmt.Errorf("invalid billing cycle: %w", err)
	}

	// 创建分页器
	paginator := NewPaginator(sm.service.aliClient, &DescribeInstanceBillRequest{
		BillingCycle: billingCycle,
		Granularity:  "MONTHLY",
		MaxResults:   int32(sm.service.config.BatchSize),
	})

	// 创建数据处理器
	processor := NewProcessor(sm.service.chClient, options)

	// 获取目标表名
	tableName := sm.service.monthlyTableName
	if options != nil && options.UseDistributed && options.DistributedTableName != "" {
		tableName = options.DistributedTableName
	}

	return sm.executePaginatedSyncImpl(ctx, paginator, processor, tableName, 
		fmt.Sprintf("[阿里云按月同步] 账期 %s", billingCycle))
}

// syncDailyBillDataImpl 同步按天账单数据的实现
func (sm *syncManager) syncDailyBillDataImpl(ctx context.Context, billingCycle string, options *SyncOptions) error {
	log.Printf("[阿里云按天同步] 开始同步账期: %s", billingCycle)

	// 验证账期
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return fmt.Errorf("invalid billing cycle: %w", err)
	}

	// 生成该月份的所有日期
	dates, err := GenerateDatesInMonth(billingCycle)
	if err != nil {
		return fmt.Errorf("failed to generate dates for cycle %s: %w", billingCycle, err)
	}

	log.Printf("[阿里云按天同步] 账期 %s 包含 %d 天", billingCycle, len(dates))

	// 创建数据处理器
	processor := NewProcessor(sm.service.chClient, options)

	// 获取目标表名
	tableName := sm.service.dailyTableName
	if options != nil && options.UseDistributed && options.DistributedTableName != "" {
		tableName = options.DistributedTableName
	}

	totalRecords := 0

	// 按天循环获取数据
	for i, date := range dates {
		log.Printf("[阿里云按天同步] 同步日期 %s (%d/%d)", date, i+1, len(dates))

		// 创建分页器（每天的数据）
		paginator := NewPaginator(sm.service.aliClient, &DescribeInstanceBillRequest{
			BillingCycle: billingCycle,
			Granularity:  "DAILY",
			BillingDate:  date,
			MaxResults:   int32(sm.service.config.BatchSize),
		})

		dayRecords, err := sm.syncDayDataImpl(ctx, paginator, processor, tableName, date)
		if err != nil {
			log.Printf("[阿里云按天同步] 日期 %s 同步失败: %v", date, err)
			continue // 跳过这一天，继续下一天
		}

		totalRecords += dayRecords
		if dayRecords > 0 {
			log.Printf("[阿里云按天同步] 日期 %s 同步完成，%d 条记录", date, dayRecords)
		}

		// 简单的延迟，避免API调用过于频繁
		if i < len(dates)-1 {
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	log.Printf("[阿里云按天同步] 账期 %s 同步完成，共同步 %d 条记录", billingCycle, totalRecords)
	return nil
}

// syncSpecificDayBillDataImpl 同步指定日期的天表数据的实现
func (sm *syncManager) syncSpecificDayBillDataImpl(ctx context.Context, billingDate string, options *SyncOptions) error {
	log.Printf("[阿里云指定日期同步] 开始同步日期: %s", billingDate)

	// 验证日期格式
	date, err := time.Parse("2006-01-02", billingDate)
	if err != nil {
		return fmt.Errorf("invalid billing date format (expected YYYY-MM-DD): %w", err)
	}

	// 获取账期（YYYY-MM格式）
	billingCycle := date.Format("2006-01")

	// 验证账期
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return fmt.Errorf("invalid billing cycle: %w", err)
	}

	// 创建分页器（指定日期的数据）
	paginator := NewPaginator(sm.service.aliClient, &DescribeInstanceBillRequest{
		BillingCycle: billingCycle,
		Granularity:  "DAILY",
		BillingDate:  billingDate,
		MaxResults:   int32(sm.service.config.BatchSize),
	})

	// 创建数据处理器
	processor := NewProcessor(sm.service.chClient, options)

	// 获取目标表名
	tableName := sm.service.dailyTableName
	if options != nil && options.UseDistributed && options.DistributedTableName != "" {
		tableName = options.DistributedTableName
	}

	totalRecords, err := sm.syncDayDataImpl(ctx, paginator, processor, tableName, billingDate)
	if err != nil {
		return fmt.Errorf("failed to sync data for date %s: %w", billingDate, err)
	}

	log.Printf("[阿里云指定日期同步] 日期 %s 同步完成，共同步 %d 条记录", billingDate, totalRecords)
	return nil
}

// syncBothGranularityDataImpl 同步两种粒度的数据的实现
func (sm *syncManager) syncBothGranularityDataImpl(ctx context.Context, billingCycle string, options *SyncOptions) error {
	log.Printf("[阿里云双粒度同步] 开始同步账期: %s", billingCycle)

	// 先同步按月数据
	monthlyOptions := *options
	if options.UseDistributed {
		monthlyOptions.DistributedTableName = strings.Replace(options.DistributedTableName, "daily", "monthly", 1)
	}

	if err := sm.syncMonthlyBillDataImpl(ctx, billingCycle, &monthlyOptions); err != nil {
		return fmt.Errorf("failed to sync monthly data: %w", err)
	}

	// 再同步按天数据
	dailyOptions := *options
	if options.UseDistributed {
		dailyOptions.DistributedTableName = strings.Replace(options.DistributedTableName, "monthly", "daily", 1)
	}

	if err := sm.syncDailyBillDataImpl(ctx, billingCycle, &dailyOptions); err != nil {
		return fmt.Errorf("failed to sync daily data: %w", err)
	}

	log.Printf("[阿里云双粒度同步] 账期 %s 双粒度同步完成", billingCycle)
	return nil
}

// syncDayData 同步单天数据的辅助方法
func (sm *syncManager) syncDayDataImpl(ctx context.Context, paginator PaginatorInterface, processor DataProcessor, tableName, date string) (int, error) {
	totalRecords := 0

	// 分页获取指定日期的所有数据
	for {
		response, err := paginator.Next(ctx)
		if err != nil {
			return totalRecords, fmt.Errorf("failed to fetch data: %w", err)
		}

		if len(response.Data.Items) == 0 {
			break // 这个日期没有数据
		}

		// 批量处理数据
		if err := processor.ProcessBatchWithBillingCycle(ctx, tableName, response.Data.Items, response.Data.BillingCycle); err != nil {
			return totalRecords, fmt.Errorf("failed to process batch: %w", err)
		}

		totalRecords += len(response.Data.Items)

		// 检查是否还有更多数据
		if !paginator.HasNext() {
			break
		}
	}

	return totalRecords, nil
}

// executePaginatedSync 执行分页同步的通用方法
func (sm *syncManager) executePaginatedSyncImpl(ctx context.Context, paginator PaginatorInterface, processor DataProcessor, tableName, logPrefix string) error {
	totalRecords := 0

	for {
		// 获取下一批数据
		response, err := paginator.Next(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch data: %w", err)
		}

		if len(response.Data.Items) == 0 {
			break // 没有更多数据
		}

		// 批量处理数据
		if err := processor.ProcessBatchWithBillingCycle(ctx, tableName, response.Data.Items, response.Data.BillingCycle); err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}

		totalRecords += len(response.Data.Items)
		log.Printf("%s 已同步 %d 条记录", logPrefix, totalRecords)

		// 检查是否还有更多数据
		if !paginator.HasNext() {
			break
		}
	}

	log.Printf("%s 同步完成，共同步 %d 条记录", logPrefix, totalRecords)
	return nil
}

// ExecuteIntelligentCleanupAndSync 执行智能清理和同步
func (sm *syncManager) executeIntelligentCleanupAndSyncImpl(ctx context.Context, result *DataComparisonResult, syncOptions *SyncOptions) error {
	if !result.NeedSync {
		// 不需要同步，直接返回
		return nil
	}

	if result.NeedCleanup {
		// 需要先清理数据
		log.Printf("[阿里云智能同步] 检测到数据不一致，先清理 %s %s 的数据",
			result.Granularity, result.Period)

		if err := sm.service.CleanSpecificPeriodData(ctx, result.Granularity, result.Period); err != nil {
			return fmt.Errorf("failed to clean data before sync: %w", err)
		}

		log.Printf("[阿里云智能同步] 数据清理完成，开始同步新数据")
	}

	// 执行同步
	switch strings.ToUpper(result.Granularity) {
	case "DAILY":
		return sm.syncSpecificDayBillDataImpl(ctx, result.Period, syncOptions)
	case "MONTHLY":
		return sm.syncMonthlyBillDataImpl(ctx, result.Period, syncOptions)
	default:
		return fmt.Errorf("unsupported granularity for sync: %s", result.Granularity)
	}
}

// SyncOptions 同步选项（保持向后兼容）
type SyncOptions struct {
	BatchSize            int                        // 批次大小
	UseDistributed       bool                       // 是否使用分布式表
	DistributedTableName string                     // 分布式表名
	SkipZeroAmount       bool                       // 是否跳过零金额记录
	EnableValidation     bool                       // 是否启用数据验证
	MaxWorkers           int                        // 最大工作协程数
	ProgressCallback     func(processed, total int) // 进度回调
}

// DefaultSyncOptions 默认同步选项（保持向后兼容）
func DefaultSyncOptions() *SyncOptions {
	return &SyncOptions{
		BatchSize:        1000,
		UseDistributed:   false,
		SkipZeroAmount:   false,
		EnableValidation: true,
		MaxWorkers:       4,
	}
}