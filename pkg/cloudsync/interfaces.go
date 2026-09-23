package cloudsync

import (
	"context"
	"time"
)

// CloudProvider defines the interface that all cloud providers must implement
type CloudProvider interface {
	// Provider identification
	GetProviderName() string
	ValidateCredentials(ctx context.Context) error
	Close() error

	// Data retrieval
	GetAPIDataCount(ctx context.Context, period, granularity string) (int64, error)
	FetchBillData(ctx context.Context, req *FetchRequest) (*FetchResult, error)

	// Table configuration
	GetTableConfig(granularity string) *TableConfig
	GetPeriodField() string

	// Business logic specific methods
	CreateTables(ctx context.Context, config *TableConfig) error
	// SyncPeriodData pulls one period into the table the granularity selects.
	// Granularity is "monthly" or "daily"; empty means "read it off the period
	// format", which is what a caller that only knows a period string wants.
	SyncPeriodData(ctx context.Context, period, granularity string, options *SyncOptions) error
}

// DataProcessor processes bill data for database insertion
type DataProcessor interface {
	ProcessBatch(ctx context.Context, data interface{}, tableName string) error
	SetBatchSize(size int)
	GetProcessedCount() int64
}

// ConsistencyChecker checks data consistency between API and database
type ConsistencyChecker interface {
	CheckPeriodConsistency(ctx context.Context, period *PeriodInfo) (bool, error)
	FindInconsistentPeriods(ctx context.Context, config *SyncConfig) ([]*PeriodInfo, error)
	CleanInconsistentData(ctx context.Context, periods []*PeriodInfo) error
}

// SyncExecutor executes synchronization tasks
type SyncExecutor interface {
	ValidateConfig(ctx context.Context, config *SyncConfig) error
	ExecuteSync(ctx context.Context, config *SyncConfig) (*SyncResult, error)
	CreateTables(ctx context.Context, config *TableConfig) error
	PerformDataCheck(ctx context.Context, period string) (*DataCheckResult, error)
	GetProviderName() string
	GetSupportedSyncModes() []string
}

// DataCleaner handles data cleanup operations
type DataCleaner interface {
	CleanPeriodData(ctx context.Context, tableName, period, periodField, provider string) error
	CleanTableData(ctx context.Context, tableName, condition string) error
}

// RateLimiter controls API request rate
type RateLimiter interface {
	Wait(ctx context.Context) error
	OnSuccess()
	OnRateLimit()
	OnError(err error)
	GetCurrentDelay() time.Duration
}

// ProgressCallback for tracking sync progress
type ProgressCallback func(processed, total int64, message string)

// SyncProgress is what a running sync reports about itself while it runs.
//
// The unit is the billing period, because that is the unit the sync actually
// works in: it pulls one period at a time, page by page, and a manual pull is
// usually a handful of them. Records within a period are not in here — the
// providers do count pages internally, but only to log them, and threading that
// out is a change in both SDK wrappers rather than in this loop.
type SyncProgress struct {
	// Period being pulled right now, e.g. "2026-08".
	Period string `json:"period"`
	// Which table that period is going into: "monthly" or "daily". A run over
	// both granularities visits every period twice, so the period alone does
	// not say what is happening.
	Granularity string `json:"granularity,omitempty"`
	// Periods already finished, and how many there are in total.
	Done  int `json:"done"`
	Total int `json:"total"`
	// Rows written so far for the period in flight, and how many the API said
	// it holds (0 when the provider cannot tell up front, as for a whole cycle
	// at daily granularity, which is fetched one day at a time). Periods are
	// the unit of Done/Total, but one period can take minutes; these are what
	// keep a progress bar moving in the meantime.
	Records      int64 `json:"records"`
	RecordsTotal int64 `json:"records_total,omitempty"`
}

// ProgressReporter receives SyncProgress as the sync moves from one period to
// the next, and as rows of the period in flight get written. It is called from
// the sync goroutine, so whoever installs it has to be ready for that (the
// task manager updates the task under its own lock).
type ProgressReporter func(SyncProgress)

// ErrorHandler handles and categorizes errors
type ErrorHandler interface {
	HandleError(ctx context.Context, err error) error
	ShouldRetry(err error) bool
	GetRetryDelay(attempt int) time.Duration
	IsRetryableError(err error) bool
}
