package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/alicloud"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"log/slog"
	"time"
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

	slog.InfoContext(ctx, "创建阿里云账单表完成")
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

	// 构建同步选项
	syncOptions := &alicloud.SyncOptions{
		BatchSize:        config.Limit,
		UseDistributed:   config.UseDistributed || ac.config.ClickHouse.Cluster != "",
		EnableValidation: true,
		MaxWorkers:       4,
	}

	// 确定同步参数
	period := config.BillPeriod
	granularity := config.Granularity
	if granularity == "" {
		granularity = "monthly"
	}
	if period == "" {
		period = time.Now().Format("2006-01")
	}

	// 执行同步
	var err error
	switch granularity {
	case "monthly":
		err = ac.billService.SyncMonthlyBillData(ctx, period, syncOptions)
	case "daily":
		// 判断period格式
		if _, parseErr := time.Parse("2006-01-02", period); parseErr == nil {
			err = ac.billService.SyncSpecificDayBillData(ctx, period, syncOptions)
		} else {
			err = ac.billService.SyncDailyBillData(ctx, period, syncOptions)
		}
	case "both":
		err = ac.billService.SyncBothGranularityData(ctx, period, syncOptions)
	default:
		return nil, fmt.Errorf("unsupported granularity: %s", granularity)
	}

	if err != nil {
		ac.syncHelper.LogSyncError(ctx, err, "alicloud")
		return nil, fmt.Errorf("%w: %v", ErrSyncFailed, err)
	}

	// 创建结果
	result := &SyncResult{
		Success:          true,
		RecordsProcessed: 0, // 阿里云同步不返回具体数字
		RecordsFetched:   0,
		Duration:         time.Since(startTime),
		StartedAt:        startTime,
		CompletedAt:      time.Now(),
		Message:          fmt.Sprintf("AliCloud sync completed for %s %s", granularity, period),
	}

	ac.syncHelper.LogSyncComplete(ctx, result)
	return result, nil
}

// PerformDataCheck 执行数据校验
func (ac *AliCloudSyncExecutor) PerformDataCheck(ctx context.Context, period string) (*DataCheckResult, error) {
	return &DataCheckResult{
		Success:      true,
		TotalRecords: 0,
		ChecksPassed: 0,
		ChecksFailed: 0,
		CheckTime:    time.Now(),
		Details:      map[string]interface{}{"message": "data check not implemented for alicloud"},
	}, nil
}

// GetProviderName 获取提供商名称
func (ac *AliCloudSyncExecutor) GetProviderName() string {
	return "alicloud"
}

// GetSupportedSyncModes 获取支持的同步模式
func (ac *AliCloudSyncExecutor) GetSupportedSyncModes() []string {
	return []string{"standard"}
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