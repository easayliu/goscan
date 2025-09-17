package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/alicloud"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"time"

	"go.uber.org/zap"
)

// AliCloudSyncExecutor 阿里云同步执行器
type AliCloudSyncExecutor struct {
	*BaseSyncExecutor
	billService *alicloud.BillService
}

// NewAliCloudSyncExecutor 创建阿里云同步执行器
func NewAliCloudSyncExecutor(cfg *config.Config, chClient *clickhouse.Client) *AliCloudSyncExecutor {
	return &AliCloudSyncExecutor{
		BaseSyncExecutor: NewBaseSyncExecutor(cfg, chClient),
	}
}

// ValidateConfig 验证配置
func (ac *AliCloudSyncExecutor) ValidateConfig(ctx context.Context, config *SyncConfig) error {
	// 验证基础配置
	if err := ac.BaseSyncExecutor.ValidateConfig(ctx, config); err != nil {
		return err
	}

	// 验证阿里云特定配置
	aliConfig := ac.config.GetAliCloudConfig()
	if aliConfig.AccessKeyID == "" || aliConfig.AccessKeySecret == "" {
		return fmt.Errorf("%w: alicloud access key id or secret", ErrCredentialsNotConfigured)
	}

	return nil
}

// CreateTables 创建数据表
func (ac *AliCloudSyncExecutor) CreateTables(ctx context.Context, config *TableConfig) error {
	if err := ac.ValidateTableConfig(ctx, config); err != nil {
		return err
	}

	// 初始化bill service
	if err := ac.initBillService(); err != nil {
		return fmt.Errorf("%w: %v", ErrTableCreationFailed, err)
	}
	defer ac.billService.Close()

	// 创建表（使用基础方法）
	if err := ac.billService.CreateMonthlyBillTable(ctx); err != nil {
		return fmt.Errorf("failed to create monthly table: %w", err)
	}

	if err := ac.billService.CreateDailyBillTable(ctx); err != nil {
		return fmt.Errorf("failed to create daily table: %w", err)
	}

	logger.Info("Alibaba Cloud bill table creation completed")
	return nil
}

// ExecuteSync 执行数据同步
func (ac *AliCloudSyncExecutor) ExecuteSync(ctx context.Context, config *SyncConfig) (*SyncResult, error) {
	startTime := time.Now()

	// 验证配置
	if err := ac.ValidateConfig(ctx, config); err != nil {
		return nil, err
	}

	// 初始化bill service
	if err := ac.initBillService(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSyncFailed, err)
	}
	defer ac.billService.Close()

	ac.syncHelper.LogSyncStart(ctx, "alicloud", config.SyncMode, config)

	// Initialize variables for sync-optimal mode
	var periodsToSync []string
	var periodGranularity map[string]string

	// Perform data count validation for sync-optimal mode BEFORE sync
	if config.SyncMode == "sync-optimal" {
		logger.Info("performing pre-sync data count validation for sync-optimal mode",
			zap.String("provider", "alicloud"))
			
		// Get time periods first
		now := time.Now()
		lastMonth := now.AddDate(0, -1, 0).Format("2006-01")
		yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
		
		// Check data consistency and handle inconsistencies
		periodsToSync = []string{}
		periodGranularity = map[string]string{}
		
		// Check monthly data consistency
		monthlyConsistent, err := ac.checkDataConsistency(ctx, lastMonth, "monthly")
		if err != nil {
			logger.Warn("failed to check monthly data consistency, will sync",
				zap.String("provider", "alicloud"),
				zap.String("period", lastMonth),
				zap.Error(err))
			periodsToSync = append(periodsToSync, lastMonth)
			periodGranularity[lastMonth] = "monthly"
		} else if !monthlyConsistent {
			logger.Info("monthly data inconsistency detected, will clean and resync",
				zap.String("provider", "alicloud"),
				zap.String("period", lastMonth))
			
			// Clean existing monthly data for this period
			baseTableName := ac.billService.GetMonthlyTableName()
			err := ac.dataCleaner.CleanPeriodData(ctx, baseTableName, lastMonth, "billing_cycle", "alicloud")
			if err != nil {
				logger.Error("failed to clean monthly data",
					zap.String("provider", "alicloud"),
					zap.String("period", lastMonth),
					zap.Error(err))
			} else {
				logger.Info("successfully cleaned monthly data",
					zap.String("provider", "alicloud"),
					zap.String("period", lastMonth))
			}
			
			periodsToSync = append(periodsToSync, lastMonth)
			periodGranularity[lastMonth] = "monthly"
		} else {
			logger.Info("monthly data is consistent, skipping",
				zap.String("provider", "alicloud"),
				zap.String("period", lastMonth))
		}
		
		// Check daily data consistency  
		dailyConsistent, err := ac.checkDataConsistency(ctx, yesterday, "daily")
		if err != nil {
			logger.Warn("failed to check daily data consistency, will sync",
				zap.String("provider", "alicloud"),
				zap.String("period", yesterday),
				zap.Error(err))
			periodsToSync = append(periodsToSync, yesterday)
			periodGranularity[yesterday] = "daily"
		} else if !dailyConsistent {
			logger.Info("daily data inconsistency detected, will clean and resync",
				zap.String("provider", "alicloud"),
				zap.String("period", yesterday))
			
			// Clean existing daily data for this period
			baseTableName := ac.billService.GetDailyTableName()
			err := ac.dataCleaner.CleanPeriodData(ctx, baseTableName, yesterday, "billing_date", "alicloud")
			if err != nil {
				logger.Error("failed to clean daily data",
					zap.String("provider", "alicloud"),
					zap.String("period", yesterday),
					zap.Error(err))
			} else {
				logger.Info("successfully cleaned daily data",
					zap.String("provider", "alicloud"),
					zap.String("period", yesterday))
			}
			
			periodsToSync = append(periodsToSync, yesterday)
			periodGranularity[yesterday] = "daily"
		} else {
			logger.Info("daily data is consistent, skipping",
				zap.String("provider", "alicloud"),
				zap.String("period", yesterday))
		}
		
		// If no periods need sync, return success
		if len(periodsToSync) == 0 {
			logger.Info("all data is consistent, skipping sync",
				zap.String("provider", "alicloud"),
				zap.String("monthly_period", lastMonth),
				zap.String("daily_period", yesterday))
			
			result := &SyncResult{
				Success:          true,
				RecordsProcessed: 0,
				RecordsFetched:   0,
				Duration:         time.Since(startTime),
				StartedAt:        startTime,
				CompletedAt:      time.Now(),
				Message:          "AliCloud data is consistent, sync skipped",
			}
			ac.syncHelper.LogSyncComplete(ctx, result)
			return result, nil
		}
		
		logger.Info("data inconsistency detected, proceeding with sync",
			zap.String("provider", "alicloud"),
			zap.Strings("periods_to_sync", periodsToSync))
	}

	// 构建同步选项
	syncOptions := &alicloud.SyncOptions{
		BatchSize:        config.Limit,
		UseDistributed:   config.UseDistributed || ac.config.ClickHouse.Cluster != "",
		EnableValidation: true,
		MaxWorkers:       4,
	}

	// 确定同步参数
	granularity := config.Granularity
	if granularity == "" {
		granularity = "monthly"
	}
	
	// 根据同步模式确定需要同步的数据
	var monthlyPeriods []string
	var dailyPeriods []string
	
	if config.SyncMode == "sync-optimal" {
		// sync-optimal mode: only sync periods that are inconsistent
		if len(periodsToSync) > 0 {
			for _, period := range periodsToSync {
				if periodGranularity[period] == "monthly" {
					monthlyPeriods = append(monthlyPeriods, period)
				} else if periodGranularity[period] == "daily" {
					dailyPeriods = append(dailyPeriods, period)
				}
			}
		}
		
		logger.Info("sync-optimal mode: syncing inconsistent periods only",
			zap.String("provider", "alicloud"),
			zap.Strings("monthly_periods", monthlyPeriods),
			zap.Strings("daily_periods", dailyPeriods))
	} else {
		// Standard mode: sync only specified single period
		period := config.BillPeriod
		if period == "" {
			period = time.Now().Format("2006-01")
		}
		
		// For standard mode, use the specified granularity
		if granularity == "monthly" || granularity == "both" {
			monthlyPeriods = []string{period}
		}
		if granularity == "daily" || granularity == "both" {
			dailyPeriods = []string{period}
		}
	}

	// 执行同步
	var err error
	var totalRecords int
	var syncCount int
	
	// Sync monthly periods
	for _, period := range monthlyPeriods {
		logger.Info("syncing monthly period",
			zap.String("provider", "alicloud"),
			zap.String("period", period))
			
		err = ac.billService.SyncMonthlyBillData(ctx, period, syncOptions)
		if err != nil {
			ac.syncHelper.LogSyncError(ctx, err, "alicloud")
			return nil, fmt.Errorf("%w: failed to sync monthly period %s: %v", ErrSyncFailed, period, err)
		}
		
		logger.Info("monthly period sync completed",
			zap.String("provider", "alicloud"),
			zap.String("period", period))
		syncCount++
	}
	
	// Sync daily periods
	for _, period := range dailyPeriods {
		logger.Info("syncing daily period",
			zap.String("provider", "alicloud"),
			zap.String("period", period))
			
		// For specific day format (YYYY-MM-DD), use SyncSpecificDayBillData
		if _, parseErr := time.Parse("2006-01-02", period); parseErr == nil {
			err = ac.billService.SyncSpecificDayBillData(ctx, period, syncOptions)
		} else {
			err = ac.billService.SyncDailyBillData(ctx, period, syncOptions)
		}
		
		if err != nil {
			ac.syncHelper.LogSyncError(ctx, err, "alicloud")
			return nil, fmt.Errorf("%w: failed to sync daily period %s: %v", ErrSyncFailed, period, err)
		}
		syncCount++
	}

	// 创建结果
	var resultMessage string
	if config.SyncMode == "sync-optimal" {
		resultMessage = fmt.Sprintf("AliCloud sync-optimal completed: %d monthly + %d daily periods", 
			len(monthlyPeriods), len(dailyPeriods))
	} else {
		totalPeriods := len(monthlyPeriods) + len(dailyPeriods)
		resultMessage = fmt.Sprintf("AliCloud sync completed for %s granularity, %d periods", granularity, totalPeriods)
	}
	
	result := &SyncResult{
		Success:          true,
		RecordsProcessed: totalRecords,
		RecordsFetched:   totalRecords,
		Duration:         time.Since(startTime),
		StartedAt:        startTime,
		CompletedAt:      time.Now(),
		Message:          resultMessage,
	}

	ac.syncHelper.LogSyncComplete(ctx, result)
	return result, nil
}

// PerformDataCheck 执行数据校验
func (ac *AliCloudSyncExecutor) PerformDataCheck(ctx context.Context, period string) (*DataCheckResult, error) {
	// Parse period to determine if it's monthly or daily
	var granularity string
	if _, err := time.Parse("2006-01-02", period); err == nil {
		granularity = "daily"
	} else if _, err := time.Parse("2006-01", period); err == nil {
		granularity = "monthly"
	} else {
		return nil, fmt.Errorf("invalid period format: %s", period)
	}
	
	// Initialize bill service
	if err := ac.initBillService(); err != nil {
		return nil, fmt.Errorf("failed to initialize bill service for validation: %w", err)
	}
	defer ac.billService.Close()
	
	// Check data consistency
	consistent, err := ac.checkDataConsistency(ctx, period, granularity)
	if err != nil {
		return &DataCheckResult{
			Success:      false,
			TotalRecords: 0,
			ChecksPassed: 0,
			ChecksFailed: 1,
			Issues:       []string{fmt.Sprintf("failed to check data consistency: %v", err)},
			CheckTime:    time.Now(),
			Details:      map[string]interface{}{"error": err.Error()},
		}, err
	}
	
	if consistent {
		return &DataCheckResult{
			Success:      true,
			TotalRecords: 1,
			ChecksPassed: 1,
			ChecksFailed: 0,
			Issues:       []string{},
			CheckTime:    time.Now(),
			Details:      map[string]interface{}{"message": "data count is consistent"},
		}, nil
	} else {
		return &DataCheckResult{
			Success:      false,
			TotalRecords: 1,
			ChecksPassed: 0,
			ChecksFailed: 1,
			Issues:       []string{fmt.Sprintf("data count inconsistency for period %s", period)},
			CheckTime:    time.Now(),
			Details:      map[string]interface{}{"message": "data count is inconsistent"},
		}, nil
	}
}

// GetProviderName 获取提供商名称
func (ac *AliCloudSyncExecutor) GetProviderName() string {
	return "alicloud"
}

// GetSupportedSyncModes 获取支持的同步模式
func (ac *AliCloudSyncExecutor) GetSupportedSyncModes() []string {
	return []string{"standard", "sync-optimal"}
}

// initBillService 初始化账单服务
func (ac *AliCloudSyncExecutor) initBillService() error {
	if ac.billService != nil {
		return nil
	}

	aliConfig := ac.config.GetAliCloudConfig()
	billService, err := alicloud.NewBillService(aliConfig, ac.chClient)
	if err != nil {
		return fmt.Errorf("failed to create AliCloud bill service: %w", err)
	}

	ac.billService = billService
	return nil
}

// checkDataConsistency checks if API data count matches database data count
func (ac *AliCloudSyncExecutor) checkDataConsistency(ctx context.Context, period, granularity string) (bool, error) {
	// Get API data count
	var apiCount int64
	var err error
	
	if granularity == "monthly" {
		// Get monthly API data count
		apiCount32, err := ac.billService.GetMonthlyAPIDataCount(ctx, period)
		if err == nil {
			apiCount = int64(apiCount32)
		}
	} else {
		// Get daily API data count  
		apiCount32, err := ac.billService.GetDailyAPIDataCount(ctx, period)
		if err == nil {
			apiCount = int64(apiCount32)
		}
	}
	
	if err != nil {
		return false, fmt.Errorf("failed to get API data count: %w", err)
	}
	
	// Get database data count
	var dbCount int64
	resolver := ac.chClient.GetTableNameResolver()
	
	if granularity == "monthly" {
		baseTableName := ac.billService.GetMonthlyTableName()
		actualTableName := resolver.ResolveQueryTarget(baseTableName)
		
		// Query monthly data: billing_cycle = period AND billing_date IS NULL
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE billing_cycle = ? AND billing_date IS NULL", actualTableName)
		rows, err := ac.chClient.Query(ctx, query, period)
		if err != nil {
			return false, fmt.Errorf("failed to query monthly database count: %w", err)
		}
		defer rows.Close()
		
		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				return false, fmt.Errorf("failed to scan monthly database count: %w", err)
			}
			dbCount = int64(count)
		}
	} else {
		baseTableName := ac.billService.GetDailyTableName()
		actualTableName := resolver.ResolveQueryTarget(baseTableName)
		
		// Query daily data: billing_date = period
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE billing_date = ?", actualTableName)
		rows, err := ac.chClient.Query(ctx, query, period)
		if err != nil {
			return false, fmt.Errorf("failed to query daily database count: %w", err)
		}
		defer rows.Close()
		
		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				return false, fmt.Errorf("failed to scan daily database count: %w", err)
			}
			dbCount = int64(count)
		}
	}
	
	logger.Info("data count comparison",
		zap.String("provider", "alicloud"),
		zap.String("period", period),
		zap.String("granularity", granularity),
		zap.Int64("api_count", apiCount),
		zap.Int64("db_count", dbCount))
	
	// Compare counts
	consistent := apiCount == dbCount
	if !consistent {
		logger.Warn("data count inconsistency detected",
			zap.String("provider", "alicloud"),
			zap.String("period", period),
			zap.String("granularity", granularity),
			zap.Int64("api_count", apiCount),
			zap.Int64("db_count", dbCount))
	} else {
		logger.Info("data count is consistent",
			zap.String("provider", "alicloud"),
			zap.String("period", period),
			zap.String("granularity", granularity),
			zap.Int64("count", apiCount))
	}
	
	return consistent, nil
}


