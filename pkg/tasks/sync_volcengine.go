package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/volcengine"
	"log/slog"
	"time"
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

	slog.InfoContext(ctx, "创建火山引擎账单表完成")
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

	// 构建同步请求
	req := &volcengine.ListBillDetailRequest{
		BillPeriod: config.BillPeriod,
		Limit:      int32(config.Limit),
		Offset:     0,
	}

	// 执行同步
	syncResult, err := ve.billService.SyncBillData(ctx, req)
	if err != nil {
		ve.syncHelper.LogSyncError(ctx, err, "volcengine")
		return nil, fmt.Errorf("%w: %v", ErrSyncFailed, err)
	}

	// 转换结果
	result := ve.convertVolcEngineResult(syncResult)
	result.Duration = time.Since(startTime)
	result.StartedAt = startTime
	result.CompletedAt = time.Now()

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
	return []string{"standard"}
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