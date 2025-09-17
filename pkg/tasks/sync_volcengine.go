package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"goscan/pkg/volcengine"
	"time"

	"go.uber.org/zap"
)

// VolcEngineSyncExecutor 火山引擎同步执行器
type VolcEngineSyncExecutor struct {
	*BaseSyncExecutor
	billService volcengine.BillService
}

// NewVolcEngineSyncExecutor 创建火山引擎同步执行器
func NewVolcEngineSyncExecutor(cfg *config.Config, chClient *clickhouse.Client) *VolcEngineSyncExecutor {
	return &VolcEngineSyncExecutor{
		BaseSyncExecutor: NewBaseSyncExecutor(cfg, chClient),
	}
}

// ValidateConfig 验证配置
func (ve *VolcEngineSyncExecutor) ValidateConfig(ctx context.Context, config *SyncConfig) error {
	// 验证基础配置
	if err := ve.BaseSyncExecutor.ValidateConfig(ctx, config); err != nil {
		return err
	}

	// 验证火山引擎特定配置
	volcConfig := ve.config.GetVolcEngineConfig()
	if volcConfig.AccessKey == "" || volcConfig.SecretKey == "" {
		return fmt.Errorf("%w: volcengine access key or secret key", ErrCredentialsNotConfigured)
	}

	return nil
}

// CreateTables 创建数据表
func (ve *VolcEngineSyncExecutor) CreateTables(ctx context.Context, config *TableConfig) error {
	if err := ve.ValidateTableConfig(ctx, config); err != nil {
		return err
	}

	// 初始化bill service
	if err := ve.initBillService(); err != nil {
		return fmt.Errorf("%w: %v", ErrTableCreationFailed, err)
	}

	// 创建表（只使用基础方法）
	if err := ve.billService.CreateBillTable(ctx); err != nil {
		return fmt.Errorf("failed to create bill table: %w", err)
	}

	logger.Info("VolcEngine bill table creation completed")
	return nil
}

// ExecuteSync 执行数据同步
func (ve *VolcEngineSyncExecutor) ExecuteSync(ctx context.Context, config *SyncConfig) (*SyncResult, error) {
	startTime := time.Now()

	// 验证配置
	if err := ve.ValidateConfig(ctx, config); err != nil {
		return nil, err
	}

	// 初始化bill service
	if err := ve.initBillService(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSyncFailed, err)
	}

	ve.syncHelper.LogSyncStart(ctx, "volcengine", config.SyncMode, config)

	// 判断是否使用分布式表（检查是否配置了集群）
	isDistributed := ve.chClient.GetClusterName() != ""

	// Initialize variables for sync-optimal mode
	var periodsToSync []string
	var periodGranularity map[string]string
	
	// Perform data count validation for sync-optimal mode BEFORE sync
	if config.SyncMode == "sync-optimal" {
		logger.Info("performing pre-sync data count validation for sync-optimal mode",
			zap.String("provider", "volcengine"))
			
		// Get time periods first
		now := time.Now()
		lastMonth := now.AddDate(0, -1, 0).Format("2006-01")
		currentMonth := now.Format("2006-01")
		twoMonthsAgo := now.AddDate(0, -2, 0).Format("2006-01")
		
		// Check data consistency and handle inconsistencies
		periodsToSync = []string{}
		periodGranularity = map[string]string{}
		
		monthsToCheck := []string{twoMonthsAgo, lastMonth, currentMonth}
		
		for _, month := range monthsToCheck {
			consistent, err := ve.checkDataConsistency(ctx, month)
			if err != nil {
				logger.Warn("failed to check monthly data consistency, will sync",
					zap.String("provider", "volcengine"),
					zap.String("period", month),
					zap.Error(err))
				periodsToSync = append(periodsToSync, month)
				periodGranularity[month] = "monthly"
			} else if !consistent {
				logger.Info("monthly data inconsistency detected, will clean and resync",
					zap.String("provider", "volcengine"),
					zap.String("period", month))
				
				// Clean existing monthly data for this period
				baseTableName := "volcengine_bill_details"
				if isDistributed {
					baseTableName = "volcengine_bill_details_local"
				}
				err := ve.dataCleaner.CleanPeriodData(ctx, baseTableName, month, "BillPeriod", "volcengine")
				if err != nil {
					logger.Error("failed to clean monthly data",
						zap.String("provider", "volcengine"),
						zap.String("period", month),
						zap.Error(err))
				} else {
					logger.Info("successfully cleaned monthly data",
						zap.String("provider", "volcengine"),
						zap.String("period", month))
				}
				
				periodsToSync = append(periodsToSync, month)
				periodGranularity[month] = "monthly"
			} else {
				logger.Info("monthly data is consistent, skipping",
					zap.String("provider", "volcengine"),
					zap.String("period", month))
			}
		}
		
		// If no periods need sync, return success
		if len(periodsToSync) == 0 {
			logger.Info("all data is consistent, skipping sync",
				zap.String("provider", "volcengine"),
				zap.Strings("checked_periods", monthsToCheck))
			
			result := &SyncResult{
				Success:          true,
				RecordsProcessed: 0,
				RecordsFetched:   0,
				Duration:         time.Since(startTime),
				StartedAt:        startTime,
				CompletedAt:      time.Now(),
				Message:          "All volcengine periods are consistent, sync skipped",
			}
			ve.syncHelper.LogSyncComplete(ctx, result)
			return result, nil
		}
	} else {
		// 标准模式：只同步指定的单一账期
		periodsToSync = []string{config.BillPeriod}
	}

	// Handle force_update mode for standard sync mode
	if config.ForceUpdate && config.SyncMode != "sync-optimal" {
		// For standard mode, apply force_update logic similar to original implementation
		tableName := "volcengine_bill_details"
		queryTableName := tableName
		cleanupTableName := tableName
		
		if isDistributed {
			queryTableName = "volcengine_bill_details_distributed"
			cleanupTableName = "volcengine_bill_details_local"
		}
		
		logger.Info("force_update enabled for standard mode",
			zap.String("provider", "volcengine"),
			zap.Strings("periods", periodsToSync))

		var periodsNeedSync []string
		for _, period := range periodsToSync {
			apiCount, err := ve.billService.GetAPIDataCount(ctx, period)
			if err != nil {
				logger.Error("failed to get API data count",
					zap.String("provider", "volcengine"),
					zap.String("period", period),
					zap.Error(err))
				periodsNeedSync = append(periodsNeedSync, period)
				continue
			}

			exists, dbCount, err := ve.billService.CheckMonthlyDataExists(ctx, queryTableName, period)
			if err != nil {
				logger.Error("failed to check database data count",
					zap.String("provider", "volcengine"),
					zap.String("period", period),
					zap.Error(err))
				periodsNeedSync = append(periodsNeedSync, period)
				continue
			}

			if exists && int64(apiCount) == dbCount {
				logger.Info("data is consistent, skipping sync",
					zap.String("provider", "volcengine"),
					zap.String("period", period))
				continue
			}

			if exists && dbCount > 0 {
				err := ve.dataCleaner.CleanPeriodData(ctx, cleanupTableName, period, "BillPeriod", "volcengine")
				if err != nil {
					logger.Error("failed to clean period data",
						zap.String("provider", "volcengine"),
						zap.String("period", period),
						zap.Error(err))
				}
			}

			periodsNeedSync = append(periodsNeedSync, period)
		}

		periodsToSync = periodsNeedSync
		if len(periodsToSync) == 0 {
			result := &SyncResult{
				Success:          true,
				RecordsProcessed: 0,
				RecordsFetched:   0,
				Duration:         time.Since(startTime),
				StartedAt:        startTime,
				CompletedAt:      time.Now(),
				Message:          "All volcengine periods are consistent, sync skipped",
			}
			ve.syncHelper.LogSyncComplete(ctx, result)
			return result, nil
		}
	}

	// 执行多账期同步
	var totalRecords, totalInserted int
	var allErrors []error
	for i, period := range periodsToSync {
		logger.Info("Syncing billing data",
			zap.String("provider", "volcengine"),
			zap.String("period", period),
			zap.Int("current", i+1),
			zap.Int("total", len(periodsToSync)))

		syncResult, err := ve.billService.SmartSyncAllData(ctx, period, "volcengine_bill_details", isDistributed)
		if err != nil {
			allErrors = append(allErrors, fmt.Errorf("period %s: %w", period, err))
			logger.Error("Period sync failed",
				zap.String("provider", "volcengine"),
				zap.String("period", period),
				zap.Error(err))
			continue
		}

		totalRecords += syncResult.TotalRecords
		totalInserted += syncResult.InsertedRecords
		logger.Info("Period sync completed",
			zap.String("provider", "volcengine"),
			zap.String("period", period),
			zap.Int("records", syncResult.InsertedRecords))
	}

	// 如果所有同步都失败了
	if len(allErrors) == len(periodsToSync) {
		ve.syncHelper.LogSyncError(ctx, allErrors[0], "volcengine")
		return nil, fmt.Errorf("%w: all periods failed, first error: %v", ErrSyncFailed, allErrors[0])
	}

	// 创建综合结果
	combinedResult := &volcengine.SyncResult{
		TotalRecords:    totalRecords,
		FetchedRecords:  totalInserted,
		InsertedRecords: totalInserted,
		FailedRecords:   totalRecords - totalInserted,
		StartTime:       startTime,
		EndTime:         time.Now(),
	}
	combinedResult.Duration = combinedResult.EndTime.Sub(combinedResult.StartTime)

	// 转换结果
	result := ve.convertVolcEngineResult(combinedResult)
	result.Duration = time.Since(startTime)
	result.StartedAt = startTime
	result.CompletedAt = time.Now()
	
	// Update result message based on sync mode
	if config.SyncMode == "sync-optimal" {
		result.Message = fmt.Sprintf("VolcEngine sync-optimal completed: %d periods synced, %d records processed", 
			len(periodsToSync), totalInserted)
	} else {
		result.Message = fmt.Sprintf("VolcEngine sync completed: %d records processed", totalInserted)
	}

	ve.syncHelper.LogSyncComplete(ctx, result)
	return result, nil
}

// PerformDataCheck 执行数据校验
func (ve *VolcEngineSyncExecutor) PerformDataCheck(ctx context.Context, period string) (*DataCheckResult, error) {
	return &DataCheckResult{
		Success:      true,
		TotalRecords: 0,
		ChecksPassed: 0,
		ChecksFailed: 0,
		CheckTime:    time.Now(),
		Details:      map[string]interface{}{"message": "data check not implemented for volcengine"},
	}, nil
}

// GetProviderName 获取提供商名称
func (ve *VolcEngineSyncExecutor) GetProviderName() string {
	return "volcengine"
}

// GetSupportedSyncModes 获取支持的同步模式
func (ve *VolcEngineSyncExecutor) GetSupportedSyncModes() []string {
	return []string{"standard", "sync-optimal"}
}

// initBillService 初始化账单服务
func (ve *VolcEngineSyncExecutor) initBillService() error {
	if ve.billService != nil {
		return nil
	}

	volcConfig := ve.config.GetVolcEngineConfig()
	billService, err := volcengine.NewBillService(volcConfig, ve.chClient)
	if err != nil {
		return fmt.Errorf("failed to create VolcEngine bill service: %w", err)
	}

	ve.billService = billService
	return nil
}


// convertVolcEngineResult 转换火山引擎结果为通用结果
func (ve *VolcEngineSyncExecutor) convertVolcEngineResult(volcResult *volcengine.SyncResult) *SyncResult {
	return &SyncResult{
		Success:          volcResult.Error == nil,
		RecordsProcessed: volcResult.InsertedRecords,
		RecordsFetched:   volcResult.FetchedRecords,
		Duration:         volcResult.Duration,
		Message:          fmt.Sprintf("VolcEngine sync completed: %d records processed", volcResult.InsertedRecords),
		Metadata: map[string]interface{}{
			"total_records":    volcResult.TotalRecords,
			"inserted_records": volcResult.InsertedRecords,
			"fetched_records":  volcResult.FetchedRecords,
			"failed_records":   volcResult.FailedRecords,
		},
	}
}

// checkDataConsistency checks if data count in API matches database for volcengine
func (ve *VolcEngineSyncExecutor) checkDataConsistency(ctx context.Context, period string) (bool, error) {
	// Initialize bill service if needed
	if err := ve.initBillService(); err != nil {
		return false, fmt.Errorf("failed to initialize bill service: %w", err)
	}
	
	// Get API data count
	apiCount, err := ve.billService.GetAPIDataCount(ctx, period)
	if err != nil {
		return false, fmt.Errorf("failed to get API data count for period %s: %w", period, err)
	}
	
	// Determine table name based on cluster configuration
	tableName := "volcengine_bill_details"
	if ve.chClient.GetClusterName() != "" {
		// Use distributed table for query in cluster mode
		tableName = "volcengine_bill_details_distributed"
	}
	
	// Get database data count 
	exists, dbCount, err := ve.billService.CheckMonthlyDataExists(ctx, tableName, period)
	if err != nil {
		return false, fmt.Errorf("failed to check database data count for period %s: %w", period, err)
	}
	
	// If no data exists in database, consider it inconsistent (need to sync)
	if !exists {
		logger.Info("no data exists in database, needs sync",
			zap.String("provider", "volcengine"),
			zap.String("period", period),
			zap.Int32("api_count", apiCount))
		return false, nil
	}
	
	logger.Info("data count comparison",
		zap.String("provider", "volcengine"),
		zap.String("period", period),
		zap.Int32("api_count", apiCount),
		zap.Int64("db_count", dbCount))
	
	// Compare counts
	consistent := int64(apiCount) == dbCount
	if !consistent {
		logger.Warn("data count inconsistency detected",
			zap.String("provider", "volcengine"),
			zap.String("period", period),
			zap.Int32("api_count", apiCount),
			zap.Int64("db_count", dbCount))
	} else {
		logger.Info("data count is consistent",
			zap.String("provider", "volcengine"),
			zap.String("period", period),
			zap.Int64("count", dbCount))
	}
	
	return consistent, nil
}
