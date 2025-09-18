package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/alicloud"
	"goscan/pkg/clickhouse"
	"goscan/pkg/cloudsync"
	"goscan/pkg/config"
	"goscan/pkg/utils/configutils"
)

// AliCloudSyncExecutor implements synchronization logic for AliCloud using cloudsync framework
type AliCloudSyncExecutor struct {
	*cloudsync.BaseCloudSyncExecutor
	provider *alicloud.AliCloudProvider
	config   *config.Config
	chClient *clickhouse.Client
}

// NewAliCloudSyncExecutor creates a new AliCloud sync executor using cloudsync framework
func NewAliCloudSyncExecutor(cfg *config.Config, chClient *clickhouse.Client) *AliCloudSyncExecutor {
	// Create the AliCloud provider
	aliConfig := cfg.GetAliCloudConfig()
	provider := alicloud.NewAliCloudProvider(aliConfig, chClient)

	// Create data cleaner
	dataCleaner := NewCommonDataCleaner(chClient)

	// Create base executor using cloudsync framework
	baseExecutor := cloudsync.NewBaseCloudSyncExecutor(
		provider,
		dataCleaner,
		cfg,
		chClient,
	)

	return &AliCloudSyncExecutor{
		BaseCloudSyncExecutor: baseExecutor,
		provider:              provider,
		config:                cfg,
		chClient:              chClient,
	}
}

// convertToCloudSyncConfig converts tasks.SyncConfig to cloudsync.SyncConfig
func (e *AliCloudSyncExecutor) convertToCloudSyncConfig(config *SyncConfig) *cloudsync.SyncConfig {
	input := &configutils.SyncConfigInput{
		SyncMode:       config.SyncMode,
		BillPeriod:     config.BillPeriod,
		Granularity:    config.Granularity,
		ForceUpdate:    config.ForceUpdate,
		UseDistributed: config.UseDistributed,
		Limit:          config.Limit,
	}
	return configutils.ConvertSyncConfigWithDefaults(input, "alicloud")
}

// convertToTasksSyncResult converts cloudsync.SyncResult to tasks.SyncResult
func (e *AliCloudSyncExecutor) convertToTasksSyncResult(result *cloudsync.SyncResult) *SyncResult {
	output := configutils.ConvertSyncResult(result)
	if output == nil {
		return nil
	}

	return &SyncResult{
		Success:          output.Success,
		RecordsProcessed: output.RecordsProcessed,
		RecordsFetched:   output.RecordsFetched,
		Duration:         output.Duration,
		Message:          output.Message,
		Error:            output.Error,
		StartedAt:        output.StartedAt,
		CompletedAt:      output.CompletedAt,
		Metadata:         output.Metadata,
	}
}

// convertToCloudSyncTableConfig converts tasks.TableConfig to cloudsync.TableConfig
func (e *AliCloudSyncExecutor) convertToCloudSyncTableConfig(config *TableConfig) *cloudsync.TableConfig {
	if config == nil {
		return configutils.ConvertTableConfigWithProvider(nil, "alicloud")
	}

	input := &configutils.TableConfigInput{
		LocalTableName:       config.LocalTableName,
		DistributedTableName: config.DistributedTableName,
	}
	return configutils.ConvertTableConfigWithProvider(input, "alicloud")
}

// ValidateConfig validates the synchronization configuration
func (e *AliCloudSyncExecutor) ValidateConfig(ctx context.Context, config *SyncConfig) error {
	// Convert to cloudsync config and delegate to base executor
	cloudSyncConfig := e.convertToCloudSyncConfig(config)
	return e.BaseCloudSyncExecutor.ValidateConfig(ctx, cloudSyncConfig)
}

// ExecuteSync executes the synchronization
func (e *AliCloudSyncExecutor) ExecuteSync(ctx context.Context, config *SyncConfig) (*SyncResult, error) {
	// Convert to cloudsync config and delegate to base executor
	cloudSyncConfig := e.convertToCloudSyncConfig(config)
	cloudSyncResult, err := e.BaseCloudSyncExecutor.ExecuteSync(ctx, cloudSyncConfig)
	if err != nil {
		return nil, err
	}

	// Convert result back to tasks result
	return e.convertToTasksSyncResult(cloudSyncResult), nil
}

// CreateTables creates the necessary tables
func (e *AliCloudSyncExecutor) CreateTables(ctx context.Context, config *TableConfig) error {
	// Convert to cloudsync config and delegate to base executor
	cloudSyncConfig := e.convertToCloudSyncTableConfig(config)
	return e.BaseCloudSyncExecutor.CreateTables(ctx, cloudSyncConfig)
}

// PerformDataCheck performs data validation
func (e *AliCloudSyncExecutor) PerformDataCheck(ctx context.Context, period string) (*DataCheckResult, error) {
	// Delegate to base executor and convert result
	cloudSyncResult, err := e.BaseCloudSyncExecutor.PerformDataCheck(ctx, period)
	if err != nil {
		return nil, err
	}

	// Convert cloudsync.DataCheckResult to tasks.DataCheckResult
	output := configutils.ConvertDataCheckResult(cloudSyncResult)
	if output == nil {
		return nil, fmt.Errorf("failed to convert data check result")
	}

	return &DataCheckResult{
		Success:      output.Success,
		TotalRecords: output.TotalRecords,
		ChecksPassed: output.ChecksPassed,
		ChecksFailed: output.ChecksFailed,
		Issues:       output.Issues,
		CheckTime:    output.CheckTime,
		Details:      output.Details,
	}, nil
}
