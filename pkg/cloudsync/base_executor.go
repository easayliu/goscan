package cloudsync

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"goscan/pkg/utils/dateutils"
	"time"

	"go.uber.org/zap"
)

// BaseCloudSyncExecutor provides common synchronization functionality for all cloud providers
type BaseCloudSyncExecutor struct {
	provider           CloudProvider
	consistencyChecker ConsistencyChecker
	dataCleaner        DataCleaner
	config             *config.Config
	chClient           *clickhouse.Client
}

// NewBaseCloudSyncExecutor creates a new base cloud sync executor
func NewBaseCloudSyncExecutor(
	provider CloudProvider,
	dataCleaner DataCleaner,
	config *config.Config,
	chClient *clickhouse.Client,
) *BaseCloudSyncExecutor {
	executor := &BaseCloudSyncExecutor{
		provider:    provider,
		dataCleaner: dataCleaner,
		config:      config,
		chClient:    chClient,
	}

	// Initialize consistency checker
	executor.consistencyChecker = NewDefaultConsistencyChecker(provider, chClient, dataCleaner)

	return executor
}

// ValidateConfig validates the synchronization configuration
func (e *BaseCloudSyncExecutor) ValidateConfig(ctx context.Context, config *SyncConfig) error {
	// Validate provider credentials
	if err := e.provider.ValidateCredentials(ctx); err != nil {
		return fmt.Errorf("provider credential validation failed: %w", err)
	}

	// Validate sync mode
	if config.SyncMode == "" {
		config.SyncMode = "standard"
	}

	supportedModes := []string{"standard", "sync-optimal"}
	validMode := false
	for _, mode := range supportedModes {
		if config.SyncMode == mode {
			validMode = true
			break
		}
	}
	if !validMode {
		return fmt.Errorf("unsupported sync mode: %s", config.SyncMode)
	}

	// Validate granularity
	if config.Granularity == "" {
		config.Granularity = "monthly"
	}

	validGranularities := []string{"monthly", "daily", "both"}
	validGranularity := false
	for _, granularity := range validGranularities {
		if config.Granularity == granularity {
			validGranularity = true
			break
		}
	}
	if !validGranularity {
		return fmt.Errorf("unsupported granularity: %s", config.Granularity)
	}

	// Set default values
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 4
	}

	return nil
}

// ExecuteSync executes the synchronization based on the provided configuration
func (e *BaseCloudSyncExecutor) ExecuteSync(ctx context.Context, config *SyncConfig) (*SyncResult, error) {
	startTime := time.Now()

	// Validate configuration
	if err := e.ValidateConfig(ctx, config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	e.logSyncStart(ctx, config)

	// Execute based on sync mode
	var result *SyncResult
	var err error

	if config.SyncMode == "sync-optimal" {
		result, err = e.executeOptimalSync(ctx, config)
	} else {
		result, err = e.executeStandardSync(ctx, config)
	}

	if err != nil {
		e.logSyncError(ctx, err)
		return nil, err
	}

	// Update result timing
	result.Duration = time.Since(startTime)
	result.StartedAt = startTime
	result.CompletedAt = time.Now()

	e.logSyncComplete(ctx, result)
	return result, nil
}

// executeOptimalSync executes sync-optimal mode
func (e *BaseCloudSyncExecutor) executeOptimalSync(ctx context.Context, config *SyncConfig) (*SyncResult, error) {
	logger.Info("performing pre-sync data count validation for sync-optimal mode",
		zap.String("provider", e.provider.GetProviderName()))

	// Find inconsistent periods
	inconsistentPeriods, err := e.consistencyChecker.FindInconsistentPeriods(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to find inconsistent periods: %w", err)
	}

	// If no periods need sync, return success
	if len(inconsistentPeriods) == 0 {
		logger.Info("all data is consistent, skipping sync",
			zap.String("provider", e.provider.GetProviderName()))

		return &SyncResult{
			Success:          true,
			RecordsProcessed: 0,
			RecordsFetched:   0,
			Message:          fmt.Sprintf("%s data is consistent, sync skipped", e.provider.GetProviderName()),
		}, nil
	}

	logger.Info("data inconsistency detected, proceeding with sync",
		zap.String("provider", e.provider.GetProviderName()),
		zap.Int("periods_to_sync", len(inconsistentPeriods)))

	// Clean inconsistent data if auto-clean is enabled
	if config.AutoClean {
		if err := e.consistencyChecker.CleanInconsistentData(ctx, inconsistentPeriods); err != nil {
			return nil, fmt.Errorf("failed to clean inconsistent data: %w", err)
		}
	}

	// Sync inconsistent periods
	return e.syncPeriods(ctx, inconsistentPeriods, config)
}

// executeStandardSync executes standard sync mode
func (e *BaseCloudSyncExecutor) executeStandardSync(ctx context.Context, config *SyncConfig) (*SyncResult, error) {
	// Determine periods to sync
	periods := e.determinePeriods(config)

	// Handle force update mode
	if config.ForceUpdate {
		periodsToSync, err := e.handleForceUpdate(ctx, periods, config)
		if err != nil {
			return nil, fmt.Errorf("failed to handle force update: %w", err)
		}
		periods = periodsToSync
	}

	// If no periods to sync after force update check
	if len(periods) == 0 {
		return &SyncResult{
			Success:          true,
			RecordsProcessed: 0,
			RecordsFetched:   0,
			Message:          fmt.Sprintf("%s: All periods are consistent, sync skipped", e.provider.GetProviderName()),
		}, nil
	}

	// Sync periods
	return e.syncPeriods(ctx, periods, config)
}

// syncPeriods synchronizes the provided periods
func (e *BaseCloudSyncExecutor) syncPeriods(ctx context.Context, periods []*PeriodInfo, config *SyncConfig) (*SyncResult, error) {
	var totalRecords, totalInserted int
	var allErrors []error

	for i, period := range periods {
		logger.Info("syncing period",
			zap.String("provider", e.provider.GetProviderName()),
			zap.String("period", period.Period),
			zap.String("granularity", period.Granularity),
			zap.Int("current", i+1),
			zap.Int("total", len(periods)))

		// Create sync options
		syncOptions := &SyncOptions{
			BatchSize:        config.BatchSize,
			UseDistributed:   config.UseDistributed,
			EnableValidation: true,
			MaxWorkers:       config.MaxWorkers,
		}

		// Sync this period
		err := e.provider.SyncPeriodData(ctx, period.Period, syncOptions)
		if err != nil {
			allErrors = append(allErrors, fmt.Errorf("period %s: %w", period.Period, err))
			logger.Error("period sync failed",
				zap.String("provider", e.provider.GetProviderName()),
				zap.String("period", period.Period),
				zap.String("granularity", period.Granularity),
				zap.Error(err))
			continue
		}

		logger.Info("period sync completed",
			zap.String("provider", e.provider.GetProviderName()),
			zap.String("period", period.Period),
			zap.String("granularity", period.Granularity))
	}

	// If all periods failed
	if len(allErrors) == len(periods) {
		return nil, fmt.Errorf("all periods failed, first error: %w", allErrors[0])
	}

	// Create result
	successfulPeriods := len(periods) - len(allErrors)
	message := fmt.Sprintf("%s sync completed: %d/%d periods successful",
		e.provider.GetProviderName(), successfulPeriods, len(periods))

	if config.SyncMode == "sync-optimal" {
		message = fmt.Sprintf("%s sync-optimal completed: %d periods synced",
			e.provider.GetProviderName(), successfulPeriods)
	}

	return &SyncResult{
		Success:          len(allErrors) == 0,
		RecordsProcessed: totalInserted,
		RecordsFetched:   totalRecords,
		Message:          message,
	}, nil
}

// CreateTables creates the necessary tables for the provider
func (e *BaseCloudSyncExecutor) CreateTables(ctx context.Context, config *TableConfig) error {
	return e.provider.CreateTables(ctx, config)
}

// PerformDataCheck performs data validation for a specific period
func (e *BaseCloudSyncExecutor) PerformDataCheck(ctx context.Context, period string) (*DataCheckResult, error) {
	// Determine granularity from period format using dateutils
	granularity, err := dateutils.DetermineGranularityFromPeriod(period)
	if err != nil {
		return nil, fmt.Errorf("invalid period format: %s", period)
	}

	periodInfo := &PeriodInfo{
		Period:      period,
		Granularity: granularity,
	}

	consistent, err := e.consistencyChecker.CheckPeriodConsistency(ctx, periodInfo)
	if err != nil {
		return &DataCheckResult{
			Success:      false,
			TotalRecords: 0,
			ChecksPassed: 0,
			ChecksFailed: 1,
			Issues:       []string{fmt.Sprintf("failed to check data consistency: %v", err)},
			CheckTime:    time.Now(),
			Details:      map[string]any{"error": err.Error()},
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
			Details:      map[string]any{"message": "data count is consistent"},
		}, nil
	}

	return &DataCheckResult{
		Success:      false,
		TotalRecords: 1,
		ChecksPassed: 0,
		ChecksFailed: 1,
		Issues:       []string{fmt.Sprintf("data count inconsistency for period %s", period)},
		CheckTime:    time.Now(),
		Details:      map[string]any{"message": "data count is inconsistent"},
	}, nil
}

// GetProviderName returns the provider name
func (e *BaseCloudSyncExecutor) GetProviderName() string {
	return e.provider.GetProviderName()
}

// GetSupportedSyncModes returns supported sync modes
func (e *BaseCloudSyncExecutor) GetSupportedSyncModes() []string {
	return []string{"standard", "sync-optimal"}
}

// determinePeriods determines which periods to sync for standard mode
func (e *BaseCloudSyncExecutor) determinePeriods(config *SyncConfig) []*PeriodInfo {
	var periods []*PeriodInfo

	// Use specified period or default to current month
	period := config.BillPeriod
	if period == "" {
		period = time.Now().Format("2006-01")
	}

	// Determine granularities
	granularities := []string{config.Granularity}
	if config.Granularity == "both" {
		granularities = []string{"monthly", "daily"}
	}

	for _, granularity := range granularities {
		periods = append(periods, &PeriodInfo{
			Period:      period,
			Granularity: granularity,
		})
	}

	return periods
}

// handleForceUpdate handles force update logic for standard mode
func (e *BaseCloudSyncExecutor) handleForceUpdate(ctx context.Context, periods []*PeriodInfo, config *SyncConfig) ([]*PeriodInfo, error) {
	var periodsNeedSync []*PeriodInfo

	for _, period := range periods {
		// Check consistency
		consistent, err := e.consistencyChecker.CheckPeriodConsistency(ctx, period)
		if err != nil {
			logger.Error("failed to check period consistency",
				zap.String("provider", e.provider.GetProviderName()),
				zap.String("period", period.Period),
				zap.String("granularity", period.Granularity),
				zap.Error(err))
			periodsNeedSync = append(periodsNeedSync, period)
			continue
		}

		if consistent {
			logger.Info("data is consistent, skipping sync",
				zap.String("provider", e.provider.GetProviderName()),
				zap.String("period", period.Period),
				zap.String("granularity", period.Granularity))
			continue
		}

		// Clean existing data if needed
		if period.DBCount > 0 && config.AutoClean {
			if err := e.consistencyChecker.CleanInconsistentData(ctx, []*PeriodInfo{period}); err != nil {
				logger.Error("failed to clean period data",
					zap.String("provider", e.provider.GetProviderName()),
					zap.String("period", period.Period),
					zap.String("granularity", period.Granularity),
					zap.Error(err))
			}
		}

		periodsNeedSync = append(periodsNeedSync, period)
	}

	return periodsNeedSync, nil
}

// logSyncStart logs the start of synchronization
func (e *BaseCloudSyncExecutor) logSyncStart(ctx context.Context, config *SyncConfig) {
	logger.Info("starting synchronization",
		zap.String("provider", e.provider.GetProviderName()),
		zap.String("sync_mode", config.SyncMode),
		zap.String("granularity", config.Granularity),
		zap.String("bill_period", config.BillPeriod),
		zap.Bool("force_update", config.ForceUpdate),
		zap.Bool("auto_clean", config.AutoClean))
}

// logSyncComplete logs the completion of synchronization
func (e *BaseCloudSyncExecutor) logSyncComplete(ctx context.Context, result *SyncResult) {
	logger.Info("synchronization completed",
		zap.String("provider", e.provider.GetProviderName()),
		zap.Bool("success", result.Success),
		zap.Int("records_processed", result.RecordsProcessed),
		zap.Duration("duration", result.Duration),
		zap.String("message", result.Message))
}

// logSyncError logs synchronization errors
func (e *BaseCloudSyncExecutor) logSyncError(ctx context.Context, err error) {
	logger.Error("synchronization failed",
		zap.String("provider", e.provider.GetProviderName()),
		zap.Error(err))
}
