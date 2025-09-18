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
	SyncPeriodData(ctx context.Context, period string, options *SyncOptions) error
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

// ErrorHandler handles and categorizes errors
type ErrorHandler interface {
	HandleError(ctx context.Context, err error) error
	ShouldRetry(err error) bool
	GetRetryDelay(attempt int) time.Duration
	IsRetryableError(err error) bool
}
