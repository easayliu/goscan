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

	// sync-optimal means "pull only what is missing", which is the very thing
	// force_update switches off. Obeying both at once obeys neither, so an
	// explicit force wins and the run pulls every period it was given —
	// otherwise the flag is silently a no-op in that mode.
	if config.SyncMode == "sync-optimal" && !config.ForceUpdate {
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

	// Sync inconsistent periods. Each is cleared right before it is pulled
	// (see syncPeriods), not all of them up front.
	return e.syncPeriods(ctx, inconsistentPeriods, config)
}

// executeStandardSync executes standard sync mode
func (e *BaseCloudSyncExecutor) executeStandardSync(ctx context.Context, config *SyncConfig) (*SyncResult, error) {
	// Determine periods to sync
	periods, err := e.determinePeriods(config)
	if err != nil {
		return nil, err
	}

	// force_update means "pull these periods again even though the rows are
	// already there", so it is the absence of the flag that allows a period to
	// be skipped, never its presence. Reading it the other way round is what
	// made the checkbox do the opposite of its label: ticking "已有数据也重新拉取"
	// used to skip precisely the periods it was meant to refetch, and leaving
	// it unticked pulled every period whether or not the data was already in.
	if !config.ForceUpdate {
		periods, err = e.skipConsistentPeriods(ctx, periods, config)
		if err != nil {
			return nil, fmt.Errorf("failed to work out which periods need syncing: %w", err)
		}
	} else {
		// Forced: every period is pulled again, and cleared first. Why a
		// re-pull has to start from an empty period is in clearBeforePull.
		for _, period := range periods {
			period.NeedCleanup = true
		}
	}

	// Everything asked for is already in the database
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

	report := func(done int, period *PeriodInfo, records, recordsTotal int64) {
		if config.Progress == nil {
			return
		}
		p := SyncProgress{Done: done, Total: len(periods), Records: records, RecordsTotal: recordsTotal}
		if period != nil {
			p.Period, p.Granularity = period.Period, period.Granularity
		}
		config.Progress(p)
	}

	for i, period := range periods {
		// A stop request is honoured between passes only. Stopping inside one
		// would leave its period cleared and half written — and nothing comes
		// back to repair an old period: sync-optimal only looks at the latest.
		if stopRequested(config) {
			return e.cancelledResult(periods, i, totalInserted, allErrors, report), nil
		}

		logger.Info("syncing period",
			zap.String("provider", e.provider.GetProviderName()),
			zap.String("period", period.Period),
			zap.String("granularity", period.Granularity),
			zap.Int("current", i+1),
			zap.Int("total", len(periods)))
		// Report before the period starts, so the caller sees which one is in
		// flight rather than only which ones are already done.
		report(i, period, 0, 0)

		if config.AutoClean && period.NeedCleanup {
			e.clearBeforePull(ctx, period)
		}

		// Create sync options
		var written int64
		syncOptions := &SyncOptions{
			BatchSize:        config.BatchSize,
			UseDistributed:   config.UseDistributed,
			EnableValidation: true,
			MaxWorkers:       config.MaxWorkers,
			// The provider calls this as each batch of the period lands.
			ProgressCallback: func(processed, total int64, _ string) {
				written = processed
				report(i, period, processed, total)
			},
		}

		// Sync this period. Rows written count even when the pass fails later
		// on: they are in the table.
		err := e.provider.SyncPeriodData(ctx, period.Period, period.Granularity, syncOptions)
		totalInserted += int(written)
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

	report(len(periods), nil, 0, 0)

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

// determinePeriods works out which (period, granularity) pairs a run has to
// sync, and is the only place that answer is computed — sync-optimal mode reads
// the same list when it is told which periods to look at.
//
// Every period is crossed with every granularity, because for AliCloud the
// granularity picks the table: "both" over four months is eight pulls, four
// into the monthly table and four into the daily one. Skipping the cross
// product is how a run asked for month + day used to write the monthly table
// twice and leave the daily one empty.
func (e *BaseCloudSyncExecutor) determinePeriods(config *SyncConfig) ([]*PeriodInfo, error) {
	return periodsToSync(e.provider, config)
}

// periodsToSync is determinePeriods without the executor, so the consistency
// checker can answer "which periods did the caller ask for" the same way.
func periodsToSync(provider CloudProvider, config *SyncConfig) ([]*PeriodInfo, error) {
	periods, err := resolvePeriods(config)
	if err != nil {
		return nil, err
	}

	var out []*PeriodInfo
	for _, period := range periods {
		for _, granularity := range granularitiesFor(provider, period, config.Granularity) {
			out = append(out, &PeriodInfo{Period: period, Granularity: granularity})
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no period to sync: %s supports none of the requested granularities",
			provider.GetProviderName())
	}

	return out, nil
}

// hasExplicitPeriods reports whether the caller named the periods rather than
// leaving the choice to the defaults.
func hasExplicitPeriods(config *SyncConfig) bool {
	return len(config.Periods) > 0 || config.StartPeriod != "" ||
		config.EndPeriod != "" || config.BillPeriod != ""
}

// resolvePeriods picks the period list out of the config: an explicit list
// first, then a start..end range, then a single period, and failing all of
// those the current month.
func resolvePeriods(config *SyncConfig) ([]string, error) {
	if len(config.Periods) > 0 {
		return config.Periods, nil
	}

	periods, err := dateutils.ExpandPeriodRange(config.StartPeriod, config.EndPeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid period range: %w", err)
	}
	if len(periods) > 0 {
		return periods, nil
	}

	if config.BillPeriod != "" {
		return []string{config.BillPeriod}, nil
	}
	return []string{time.Now().Format(dateutils.LayoutYearMonth)}, nil
}

// granularitiesFor says which tables one period goes into.
//
// A period written as YYYY-MM-DD is a single day, so it can only be daily —
// there is no monthly bill for one day, and honouring "both" there would fail
// half the pulls. Granularities the provider has no table for are dropped:
// VolcEngine keeps one table and answers nil for "daily", so asking it for
// "both" syncs its one table once instead of twice.
func granularitiesFor(provider CloudProvider, period, granularity string) []string {
	var wanted []string
	switch {
	case dateutils.IsValidBillingDate(period):
		wanted = []string{dateutils.GranularityDaily}
	case granularity == dateutils.GranularityBoth:
		wanted = []string{dateutils.GranularityMonthly, dateutils.GranularityDaily}
	case granularity == "":
		wanted = []string{dateutils.GranularityMonthly}
	default:
		wanted = []string{granularity}
	}

	supported := make([]string, 0, len(wanted))
	for _, g := range wanted {
		if provider.GetTableConfig(g) != nil {
			supported = append(supported, g)
		}
	}
	return supported
}

// skipConsistentPeriods drops the periods whose row count already matches what
// the API reports, and marks the ones that do not match (and hold rows) to be
// cleared before they are pulled, so the pull replaces them instead of merging
// into them.
//
// A period that cannot be checked is kept: re-pulling costs API quota, missing
// a month costs the numbers on the page.
func (e *BaseCloudSyncExecutor) skipConsistentPeriods(ctx context.Context, periods []*PeriodInfo, config *SyncConfig) ([]*PeriodInfo, error) {
	var periodsNeedSync []*PeriodInfo

	for i, period := range periods {
		// Checking only reads, so a stop can land here at once. The periods
		// not yet looked at go through unchecked: syncPeriods sees the same
		// stop before starting any of them and reports them as not run.
		if stopRequested(config) {
			return append(periodsNeedSync, periods[i:]...), nil
		}

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

		// Rows that are there but do not match get cleared — right before the
		// period is pulled, in syncPeriods, not here.
		period.NeedCleanup = period.DBCount > 0
		periodsNeedSync = append(periodsNeedSync, period)
	}

	return periodsNeedSync, nil
}

// clearBeforePull empties a period right before it is pulled again.
//
// Re-pulling on top of the old rows leans on ReplacingMergeTree to replace them,
// and it can only replace a line whose key comes back. A line the provider has
// since dropped — a refund that got netted out, an Alibaba Cloud group that is
// one line shorter and so no longer reaches its old last line_seq — never comes
// back, and would stay in the table counted twice with nothing to replace it.
//
// It happens per pass and not for the whole run up front: a run that stops
// early, by request or by failure, then leaves the passes it never reached
// exactly as they were, rather than cleared and empty.
//
// A period that fails to clear is still pulled: the pull overwrites every line
// that still exists, which leaves the table no worse than before and the next
// consistency check to catch the rest.
func (e *BaseCloudSyncExecutor) clearBeforePull(ctx context.Context, period *PeriodInfo) {
	if err := e.consistencyChecker.CleanInconsistentData(ctx, []*PeriodInfo{period}); err != nil {
		logger.Error("failed to clear period before re-pull",
			zap.String("provider", e.provider.GetProviderName()),
			zap.String("period", period.Period),
			zap.String("granularity", period.Granularity),
			zap.Error(err))
	}
}

// stopRequested reports whether the caller has asked the run to stop.
func stopRequested(config *SyncConfig) bool {
	if config.Stop == nil {
		return false
	}
	select {
	case <-config.Stop:
		return true
	default:
		return false
	}
}

// cancelledResult describes a run that stopped on request with done of its
// passes finished.
func (e *BaseCloudSyncExecutor) cancelledResult(periods []*PeriodInfo, done, written int, errs []error, report func(int, *PeriodInfo, int64, int64)) *SyncResult {
	notRun := make([]string, 0, len(periods)-done)
	for _, period := range periods[done:] {
		notRun = append(notRun, period.Period+" "+period.Granularity)
	}
	report(done, nil, 0, 0)

	logger.Info("sync stopped on request",
		zap.String("provider", e.provider.GetProviderName()),
		zap.Int("passes_done", done),
		zap.Int("passes_total", len(periods)),
		zap.Strings("not_run", notRun))

	message := fmt.Sprintf("%s sync cancelled after %d/%d passes", e.provider.GetProviderName(), done, len(periods))
	if len(errs) > 0 {
		message += fmt.Sprintf(", %d of them failed", len(errs))
	}
	return &SyncResult{
		Success:          len(errs) == 0,
		RecordsProcessed: written,
		Message:          message,
		Cancelled:        true,
		NotRun:           notRun,
	}
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
