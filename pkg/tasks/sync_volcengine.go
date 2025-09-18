package tasks

import (
	"context"
	"goscan/pkg/clickhouse"
	"goscan/pkg/cloudsync"
	"goscan/pkg/config"
	"goscan/pkg/utils/configutils"
	"goscan/pkg/volcengine"
)

// VolcEngineSyncExecutor uses the common cloud sync framework
type VolcEngineSyncExecutor struct {
	*cloudsync.BaseCloudSyncExecutor
	provider *volcengine.VolcEngineProvider
}

// NewVolcEngineSyncExecutor creates a new VolcEngine sync executor using the common framework
func NewVolcEngineSyncExecutor(cfg *config.Config, chClient *clickhouse.Client) *VolcEngineSyncExecutor {
	// Create the VolcEngine provider
	volcConfig := cfg.GetVolcEngineConfig()
	provider := volcengine.NewVolcEngineProvider(volcConfig, chClient)

	// Create data cleaner from base sync executor
	baseSyncExecutor := NewBaseSyncExecutor(cfg, chClient)
	dataCleaner := baseSyncExecutor.dataCleaner

	// Create base executor
	baseExecutor := cloudsync.NewBaseCloudSyncExecutor(
		provider,
		dataCleaner,
		cfg,
		chClient,
	)

	return &VolcEngineSyncExecutor{
		BaseCloudSyncExecutor: baseExecutor,
		provider:              provider,
	}
}

// ValidateConfig validates the synchronization configuration
func (e *VolcEngineSyncExecutor) ValidateConfig(ctx context.Context, config *SyncConfig) error {
	// Convert to cloudsync.SyncConfig
	cloudSyncConfig := e.convertToCloudSyncConfig(config)
	return e.BaseCloudSyncExecutor.ValidateConfig(ctx, cloudSyncConfig)
}

// ExecuteSync executes the synchronization
func (e *VolcEngineSyncExecutor) ExecuteSync(ctx context.Context, config *SyncConfig) (*SyncResult, error) {
	// Convert to cloudsync.SyncConfig
	cloudSyncConfig := e.convertToCloudSyncConfig(config)

	// Execute sync using base executor
	result, err := e.BaseCloudSyncExecutor.ExecuteSync(ctx, cloudSyncConfig)
	if err != nil {
		return nil, err
	}

	// Convert result back to tasks.SyncResult
	return e.convertToTasksSyncResult(result), nil
}

// CreateTables creates the necessary tables
func (e *VolcEngineSyncExecutor) CreateTables(ctx context.Context, config *TableConfig) error {
	// Convert to cloudsync.TableConfig
	cloudTableConfig := e.convertToCloudSyncTableConfig(config)
	return e.BaseCloudSyncExecutor.CreateTables(ctx, cloudTableConfig)
}

// PerformDataCheck performs data validation
func (e *VolcEngineSyncExecutor) PerformDataCheck(ctx context.Context, period string) (*DataCheckResult, error) {
	// Use base executor's data check
	result, err := e.BaseCloudSyncExecutor.PerformDataCheck(ctx, period)
	if err != nil {
		return nil, err
	}

	// Convert to tasks.DataCheckResult
	return &DataCheckResult{
		Success:      result.Success,
		TotalRecords: result.TotalRecords,
		ChecksPassed: result.ChecksPassed,
		ChecksFailed: result.ChecksFailed,
		Issues:       result.Issues,
		CheckTime:    result.CheckTime,
		Details:      result.Details,
	}, nil
}

// GetProviderName returns the provider name
func (e *VolcEngineSyncExecutor) GetProviderName() string {
	return "volcengine"
}

// GetSupportedSyncModes returns supported sync modes
func (e *VolcEngineSyncExecutor) GetSupportedSyncModes() []string {
	return []string{"standard", "sync-optimal"}
}

// Helper methods for type conversion

// convertToCloudSyncConfig converts tasks.SyncConfig to cloudsync.SyncConfig
func (e *VolcEngineSyncExecutor) convertToCloudSyncConfig(config *SyncConfig) *cloudsync.SyncConfig {
	input := &configutils.SyncConfigInput{
		SyncMode:       config.SyncMode,
		BillPeriod:     config.BillPeriod,
		Granularity:    config.Granularity,
		ForceUpdate:    config.ForceUpdate,
		UseDistributed: config.UseDistributed,
		Limit:          config.Limit,
	}
	return configutils.ConvertSyncConfigWithDefaults(input, "volcengine")
}

// convertToCloudSyncTableConfig converts tasks.TableConfig to cloudsync.TableConfig
func (e *VolcEngineSyncExecutor) convertToCloudSyncTableConfig(config *TableConfig) *cloudsync.TableConfig {
	if config == nil {
		return configutils.ConvertTableConfigWithProvider(nil, "volcengine")
	}

	input := &configutils.TableConfigInput{
		LocalTableName:       config.LocalTableName,
		DistributedTableName: config.DistributedTableName,
	}
	return configutils.ConvertTableConfigWithProvider(input, "volcengine")
}

// convertToTasksSyncResult converts cloudsync.SyncResult to tasks.SyncResult
func (e *VolcEngineSyncExecutor) convertToTasksSyncResult(result *cloudsync.SyncResult) *SyncResult {
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
