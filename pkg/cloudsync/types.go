package cloudsync

import (
	"time"
)

// SyncConfig represents configuration for synchronization
type SyncConfig struct {
	SyncMode       string   `json:"sync_mode"`       // "standard" | "sync-optimal"
	BillPeriod     string   `json:"bill_period"`     // billing period (YYYY-MM or YYYY-MM-DD)
	Periods        []string `json:"periods"`         // multiple periods for batch sync
	Granularity    string   `json:"granularity"`     // "monthly" | "daily" | "both"
	ForceUpdate    bool     `json:"force_update"`    // force update existing data
	AutoClean      bool     `json:"auto_clean"`      // automatically clean inconsistent data
	BatchSize      int      `json:"batch_size"`      // batch size for processing
	Limit          int      `json:"limit"`           // API request limit
	UseDistributed bool     `json:"use_distributed"` // use distributed tables
	MaxWorkers     int      `json:"max_workers"`     // maximum worker goroutines
}

// TableConfig represents table configuration for a provider
type TableConfig struct {
	TableName        string `json:"table_name"`        // table name
	DistributedTable string `json:"distributed_table"` // distributed table name
	PeriodField      string `json:"period_field"`      // field name for period (e.g., "billing_cycle", "BillPeriod")
	DateField        string `json:"date_field"`        // field name for date (e.g., "billing_date", "ExpenseDate")
	ProviderField    string `json:"provider_field"`    // field name for provider
	Granularity      string `json:"granularity"`       // "monthly" | "daily"
	Schema           string `json:"schema"`            // table schema SQL
}

// SyncResult represents the result of a synchronization operation
type SyncResult struct {
	Success          bool                   `json:"success"`
	RecordsProcessed int                    `json:"records_processed"`
	RecordsFetched   int                    `json:"records_fetched"`
	RecordsInserted  int                    `json:"records_inserted"`
	RecordsFailed    int                    `json:"records_failed"`
	Duration         time.Duration          `json:"duration"`
	StartedAt        time.Time              `json:"started_at"`
	CompletedAt      time.Time              `json:"completed_at"`
	Message          string                 `json:"message"`
	Error            error                  `json:"error,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// PeriodInfo contains information about a billing period
type PeriodInfo struct {
	Period      string `json:"period"`       // billing period
	Granularity string `json:"granularity"`  // "monthly" | "daily"
	APICount    int64  `json:"api_count"`    // count from API
	DBCount     int64  `json:"db_count"`     // count from database
	NeedSync    bool   `json:"need_sync"`    // whether sync is needed
	NeedCleanup bool   `json:"need_cleanup"` // whether cleanup is needed
	Reason      string `json:"reason"`       // reason for sync/cleanup
}

// DataCheckResult represents the result of data validation
type DataCheckResult struct {
	Success      bool                   `json:"success"`
	TotalRecords int                    `json:"total_records"`
	ChecksPassed int                    `json:"checks_passed"`
	ChecksFailed int                    `json:"checks_failed"`
	Issues       []string               `json:"issues"`
	CheckTime    time.Time              `json:"check_time"`
	Details      map[string]interface{} `json:"details"`
}

// FetchRequest represents a request to fetch bill data
type FetchRequest struct {
	Period      string                 `json:"period"`
	Granularity string                 `json:"granularity"`
	Offset      int32                  `json:"offset"`
	Limit       int32                  `json:"limit"`
	Options     map[string]interface{} `json:"options"`
}

// FetchResult represents the result of fetching bill data
type FetchResult struct {
	Data       interface{} `json:"data"`        // actual bill data
	Total      int32       `json:"total"`       // total count
	Fetched    int32       `json:"fetched"`     // fetched count
	HasMore    bool        `json:"has_more"`    // whether there's more data
	NextOffset int32       `json:"next_offset"` // next offset for pagination
}

// SyncOptions represents options for synchronization
type SyncOptions struct {
	BatchSize        int                    `json:"batch_size"`
	UseDistributed   bool                   `json:"use_distributed"`
	EnableValidation bool                   `json:"enable_validation"`
	MaxWorkers       int                    `json:"max_workers"`
	RetryAttempts    int                    `json:"retry_attempts"`
	RetryDelay       time.Duration          `json:"retry_delay"`
	ProgressCallback ProgressCallback       `json:"-"`
	Metadata         map[string]interface{} `json:"metadata"`
}

// SyncStats tracks synchronization statistics
type SyncStats struct {
	StartTime       time.Time     `json:"start_time"`
	EndTime         time.Time     `json:"end_time"`
	Duration        time.Duration `json:"duration"`
	TotalAPICalls   int           `json:"total_api_calls"`
	SuccessfulCalls int           `json:"successful_calls"`
	FailedCalls     int           `json:"failed_calls"`
	TotalRecords    int           `json:"total_records"`
	ProcessedCount  int           `json:"processed_count"`
	ErrorCount      int           `json:"error_count"`
	AvgCallDuration time.Duration `json:"avg_call_duration"`
}

// ConsistencyCheckConfig represents configuration for consistency checking
type ConsistencyCheckConfig struct {
	CheckLastMonths   int      `json:"check_last_months"`   // how many months to check
	CheckCurrentMonth bool     `json:"check_current_month"` // whether to check current month
	CheckYesterday    bool     `json:"check_yesterday"`     // whether to check yesterday
	Granularities     []string `json:"granularities"`       // which granularities to check
	AutoClean         bool     `json:"auto_clean"`          // automatically clean inconsistent data
}

// ProviderConfig represents configuration for a cloud provider
type ProviderConfig struct {
	ProviderName    string                 `json:"provider_name"`
	Credentials     map[string]string      `json:"credentials"`
	Endpoints       map[string]string      `json:"endpoints"`
	TableMappings   map[string]TableConfig `json:"table_mappings"`
	DefaultLimits   map[string]int         `json:"default_limits"`
	RetryConfig     RetryConfig            `json:"retry_config"`
	RateLimitConfig RateLimitConfig        `json:"rate_limit_config"`
}

// RetryConfig represents retry configuration
type RetryConfig struct {
	MaxAttempts        int           `json:"max_attempts"`
	InitialDelay       time.Duration `json:"initial_delay"`
	MaxDelay           time.Duration `json:"max_delay"`
	Multiplier         float64       `json:"multiplier"`
	RetryableHTTPCodes []int         `json:"retryable_http_codes"`
}

// RateLimitConfig represents rate limiting configuration
type RateLimitConfig struct {
	RequestsPerSecond float64       `json:"requests_per_second"`
	BurstSize         int           `json:"burst_size"`
	BackoffDelay      time.Duration `json:"backoff_delay"`
	MaxBackoffDelay   time.Duration `json:"max_backoff_delay"`
}

// DefaultSyncConfig returns default sync configuration
func DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		SyncMode:       "standard",
		Granularity:    "monthly",
		ForceUpdate:    false,
		AutoClean:      true,
		BatchSize:      100,
		Limit:          1000,
		UseDistributed: false,
		MaxWorkers:     4,
	}
}

// DefaultSyncOptions returns default sync options
func DefaultSyncOptions() *SyncOptions {
	return &SyncOptions{
		BatchSize:        100,
		UseDistributed:   false,
		EnableValidation: true,
		MaxWorkers:       4,
		RetryAttempts:    3,
		RetryDelay:       time.Second,
		Metadata:         make(map[string]interface{}),
	}
}

// DefaultRetryConfig returns default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:        3,
		InitialDelay:       time.Second,
		MaxDelay:           30 * time.Second,
		Multiplier:         2.0,
		RetryableHTTPCodes: []int{429, 500, 502, 503, 504},
	}
}

// DefaultRateLimitConfig returns default rate limit configuration
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		RequestsPerSecond: 10.0,
		BurstSize:         20,
		BackoffDelay:      time.Second,
		MaxBackoffDelay:   30 * time.Second,
	}
}
