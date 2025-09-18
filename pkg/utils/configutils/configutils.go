package configutils

import (
	"fmt"
	"goscan/pkg/cloudsync"
	"time"
)

// SyncConfigInput represents input for sync configuration conversion
type SyncConfigInput struct {
	SyncMode       string
	BillPeriod     string
	Granularity    string
	ForceUpdate    bool
	UseDistributed bool
	Limit          int
}

// TableConfigInput represents input for table configuration conversion
type TableConfigInput struct {
	LocalTableName       string
	DistributedTableName string
}

// SyncResultOutput represents output for sync result conversion
type SyncResultOutput struct {
	Success          bool
	RecordsProcessed int
	RecordsFetched   int
	Duration         time.Duration
	Message          string
	Error            string
	StartedAt        time.Time
	CompletedAt      time.Time
	Metadata         map[string]interface{}
}

// DataCheckResultOutput represents output for data check result conversion
type DataCheckResultOutput struct {
	Success      bool
	TotalRecords int
	ChecksPassed int
	ChecksFailed int
	Issues       []string
	CheckTime    time.Time
	Details      map[string]interface{}
}

// ConvertSyncConfig converts SyncConfigInput to cloudsync.SyncConfig
// This provides a standardized way to convert between different sync config formats
func ConvertSyncConfig(from *SyncConfigInput) *cloudsync.SyncConfig {
	if from == nil {
		return cloudsync.DefaultSyncConfig()
	}

	// Apply defaults for empty values
	syncMode := from.SyncMode
	if syncMode == "" {
		syncMode = "standard"
	}

	granularity := from.Granularity
	if granularity == "" {
		granularity = "monthly"
	}

	batchSize := from.Limit
	if batchSize <= 0 {
		batchSize = 100
	}

	maxWorkers := 4 // Default worker count

	return &cloudsync.SyncConfig{
		SyncMode:       syncMode,
		BillPeriod:     from.BillPeriod,
		Granularity:    granularity,
		ForceUpdate:    from.ForceUpdate,
		AutoClean:      true, // Always enable auto-clean for consistency
		BatchSize:      batchSize,
		Limit:          from.Limit,
		UseDistributed: from.UseDistributed,
		MaxWorkers:     maxWorkers,
	}
}

// ConvertSyncConfigWithDefaults converts SyncConfigInput to cloudsync.SyncConfig with provider-specific defaults
func ConvertSyncConfigWithDefaults(from *SyncConfigInput, provider string) *cloudsync.SyncConfig {
	config := ConvertSyncConfig(from)

	// Apply provider-specific defaults
	switch provider {
	case "alicloud":
		// AliCloud supports both monthly and daily granularity
		if config.Granularity == "" {
			config.Granularity = "monthly"
		}
		config.MaxWorkers = 4

	case "volcengine":
		// VolcEngine only supports monthly granularity
		config.Granularity = "monthly"
		config.MaxWorkers = 4

	default:
		// Default settings
		if config.Granularity == "" {
			config.Granularity = "monthly"
		}
		config.MaxWorkers = 4
	}

	return config
}

// ConvertTableConfig converts TableConfigInput to cloudsync.TableConfig
func ConvertTableConfig(from *TableConfigInput) *cloudsync.TableConfig {
	if from == nil {
		return nil
	}

	return &cloudsync.TableConfig{
		TableName:        from.LocalTableName,
		DistributedTable: from.DistributedTableName,
		PeriodField:      "BillPeriod",  // Default field name
		DateField:        "ExpenseDate", // Default field name
		ProviderField:    "Provider",    // Default field name
		Granularity:      "monthly",     // Default granularity
	}
}

// ConvertTableConfigWithProvider converts TableConfigInput to cloudsync.TableConfig with provider-specific settings
func ConvertTableConfigWithProvider(from *TableConfigInput, provider string) *cloudsync.TableConfig {
	config := ConvertTableConfig(from)
	if config == nil {
		config = &cloudsync.TableConfig{}
	}

	// Apply provider-specific table configurations
	switch provider {
	case "alicloud":
		if config.TableName == "" {
			config.TableName = "alicloud_bill_details"
		}
		config.PeriodField = "BillingCycle"
		config.DateField = "BillingDate"
		config.ProviderField = "Provider"

	case "volcengine":
		if config.TableName == "" {
			config.TableName = "volcengine_bill_details"
		}
		config.PeriodField = "BillPeriod"
		config.DateField = "ExpenseDate"
		config.ProviderField = "Provider"
		config.Granularity = "monthly"

	default:
		if config.TableName == "" {
			config.TableName = fmt.Sprintf("%s_bill_details", provider)
		}
	}

	return config
}

// ConvertSyncResult converts cloudsync.SyncResult to SyncResultOutput
func ConvertSyncResult(from *cloudsync.SyncResult) *SyncResultOutput {
	if from == nil {
		return nil
	}

	result := &SyncResultOutput{
		Success:          from.Success,
		RecordsProcessed: from.RecordsProcessed,
		RecordsFetched:   from.RecordsFetched,
		Duration:         from.Duration,
		Message:          from.Message,
		StartedAt:        from.StartedAt,
		CompletedAt:      from.CompletedAt,
		Metadata:         from.Metadata,
	}

	if from.Error != nil {
		result.Error = from.Error.Error()
	}

	return result
}

// ConvertDataCheckResult converts cloudsync.DataCheckResult to DataCheckResultOutput
func ConvertDataCheckResult(from *cloudsync.DataCheckResult) *DataCheckResultOutput {
	if from == nil {
		return nil
	}

	return &DataCheckResultOutput{
		Success:      from.Success,
		TotalRecords: from.TotalRecords,
		ChecksPassed: from.ChecksPassed,
		ChecksFailed: from.ChecksFailed,
		Issues:       from.Issues,
		CheckTime:    from.CheckTime,
		Details:      from.Details,
	}
}

// CreateSyncOptions creates cloudsync.SyncOptions from configuration parameters
func CreateSyncOptions(batchSize int, useDistributed bool, maxWorkers int) *cloudsync.SyncOptions {
	if batchSize <= 0 {
		batchSize = 100
	}
	if maxWorkers <= 0 {
		maxWorkers = 4
	}

	return &cloudsync.SyncOptions{
		BatchSize:        batchSize,
		UseDistributed:   useDistributed,
		EnableValidation: true,
		MaxWorkers:       maxWorkers,
		RetryAttempts:    3,
		RetryDelay:       time.Second,
		Metadata:         make(map[string]interface{}),
	}
}

// MergeSyncConfigs merges multiple sync configurations with the latter taking precedence
func MergeSyncConfigs(configs ...*cloudsync.SyncConfig) *cloudsync.SyncConfig {
	if len(configs) == 0 {
		return cloudsync.DefaultSyncConfig()
	}

	// Start with default config
	result := cloudsync.DefaultSyncConfig()

	// Apply each config in order
	for _, config := range configs {
		if config == nil {
			continue
		}

		if config.SyncMode != "" {
			result.SyncMode = config.SyncMode
		}
		if config.BillPeriod != "" {
			result.BillPeriod = config.BillPeriod
		}
		if config.Granularity != "" {
			result.Granularity = config.Granularity
		}
		if config.BatchSize > 0 {
			result.BatchSize = config.BatchSize
		}
		if config.Limit > 0 {
			result.Limit = config.Limit
		}
		if config.MaxWorkers > 0 {
			result.MaxWorkers = config.MaxWorkers
		}

		// Boolean fields - apply directly
		result.ForceUpdate = config.ForceUpdate
		result.AutoClean = config.AutoClean
		result.UseDistributed = config.UseDistributed

		// Merge periods
		if len(config.Periods) > 0 {
			result.Periods = append(result.Periods, config.Periods...)
		}
	}

	return result
}

// ValidateSyncConfig validates a sync configuration for common issues
func ValidateSyncConfig(config *cloudsync.SyncConfig) error {
	if config == nil {
		return fmt.Errorf("sync config cannot be nil")
	}

	// Validate sync mode
	validSyncModes := []string{"standard", "sync-optimal"}
	valid := false
	for _, mode := range validSyncModes {
		if config.SyncMode == mode {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("invalid sync mode: %s, supported modes: %v", config.SyncMode, validSyncModes)
	}

	// Validate granularity
	validGranularities := []string{"monthly", "daily", "both"}
	valid = false
	for _, granularity := range validGranularities {
		if config.Granularity == granularity {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("invalid granularity: %s, supported granularities: %v", config.Granularity, validGranularities)
	}

	// Validate numeric fields
	if config.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got: %d", config.BatchSize)
	}
	if config.MaxWorkers <= 0 {
		return fmt.Errorf("max workers must be positive, got: %d", config.MaxWorkers)
	}
	if config.Limit < 0 {
		return fmt.Errorf("limit cannot be negative, got: %d", config.Limit)
	}

	return nil
}

// ValidateTableConfig validates a table configuration
func ValidateTableConfig(config *cloudsync.TableConfig) error {
	if config == nil {
		return fmt.Errorf("table config cannot be nil")
	}

	if config.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Validate table name format (basic validation)
	if len(config.TableName) > 100 {
		return fmt.Errorf("table name too long: %s (max 100 characters)", config.TableName)
	}

	return nil
}

// CreateProviderSyncConfig creates a sync config with provider-specific optimizations
func CreateProviderSyncConfig(provider, syncMode, granularity, billPeriod string, forceUpdate, useDistributed bool) *cloudsync.SyncConfig {
	config := &cloudsync.SyncConfig{
		SyncMode:       syncMode,
		BillPeriod:     billPeriod,
		Granularity:    granularity,
		ForceUpdate:    forceUpdate,
		AutoClean:      true,
		UseDistributed: useDistributed,
	}

	// Apply provider-specific optimizations
	switch provider {
	case "alicloud":
		config.BatchSize = 100
		config.Limit = 300 // AliCloud API limit
		config.MaxWorkers = 4

	case "volcengine":
		config.BatchSize = 100
		config.Limit = 1000 // VolcEngine API limit
		config.MaxWorkers = 4
		config.Granularity = "monthly" // Force monthly for VolcEngine

	default:
		config.BatchSize = 100
		config.Limit = 1000
		config.MaxWorkers = 4
	}

	// Apply defaults for empty values
	if config.SyncMode == "" {
		config.SyncMode = "standard"
	}
	if config.Granularity == "" {
		config.Granularity = "monthly"
	}

	return config
}

// CloneSyncConfig creates a deep copy of a sync configuration
func CloneSyncConfig(config *cloudsync.SyncConfig) *cloudsync.SyncConfig {
	if config == nil {
		return nil
	}

	clone := &cloudsync.SyncConfig{
		SyncMode:       config.SyncMode,
		BillPeriod:     config.BillPeriod,
		Granularity:    config.Granularity,
		ForceUpdate:    config.ForceUpdate,
		AutoClean:      config.AutoClean,
		BatchSize:      config.BatchSize,
		Limit:          config.Limit,
		UseDistributed: config.UseDistributed,
		MaxWorkers:     config.MaxWorkers,
	}

	// Deep copy periods slice
	if len(config.Periods) > 0 {
		clone.Periods = make([]string, len(config.Periods))
		copy(clone.Periods, config.Periods)
	}

	return clone
}

// CloneTableConfig creates a deep copy of a table configuration
func CloneTableConfig(config *cloudsync.TableConfig) *cloudsync.TableConfig {
	if config == nil {
		return nil
	}

	return &cloudsync.TableConfig{
		TableName:        config.TableName,
		DistributedTable: config.DistributedTable,
		PeriodField:      config.PeriodField,
		DateField:        config.DateField,
		ProviderField:    config.ProviderField,
		Granularity:      config.Granularity,
		Schema:           config.Schema,
	}
}

// ConfigSummary creates a human-readable summary of a sync configuration
func ConfigSummary(config *cloudsync.SyncConfig) string {
	if config == nil {
		return "No configuration"
	}

	return fmt.Sprintf("SyncConfig{mode=%s, period=%s, granularity=%s, batch=%d, workers=%d, distributed=%v, force=%v}",
		config.SyncMode, config.BillPeriod, config.Granularity,
		config.BatchSize, config.MaxWorkers, config.UseDistributed, config.ForceUpdate)
}

// TableConfigSummary creates a human-readable summary of a table configuration
func TableConfigSummary(config *cloudsync.TableConfig) string {
	if config == nil {
		return "No table configuration"
	}

	return fmt.Sprintf("TableConfig{table=%s, distributed=%s, granularity=%s}",
		config.TableName, config.DistributedTable, config.Granularity)
}
