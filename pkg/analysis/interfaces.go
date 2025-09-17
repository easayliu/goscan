package analysis

import (
	"context"
	"time"
)

// CostCalculator cost calculator interface
type CostCalculator interface {
	// CalculateCostChanges calculates cost changes
	CalculateCostChanges(rawData []*RawCostData, yesterday, today time.Time) (*CostAnalysisResult, error)

	// AnalyzeProviderCosts analyzes costs for a single provider
	AnalyzeProviderCosts(provider string, data []*RawCostData, yesterday, today time.Time) (*ProviderCostMetric, error)
}

// DataAggregator data aggregator interface
type DataAggregator interface {
	// GroupDataByProvider groups data by provider
	GroupDataByProvider(rawData []*RawCostData) map[string][]*RawCostData

	// GetCostDataForDates gets cost data for specified dates
	GetCostDataForDates(ctx context.Context, dates []time.Time, providers []string) ([]*RawCostData, error)

	// BatchQueryCostData batch queries cost data
	BatchQueryCostData(ctx context.Context, tables []DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error)
}

// AlertGenerator alert generator interface
type AlertGenerator interface {
	// GenerateAlerts generates alert messages
	GenerateAlerts(result *CostAnalysisResult, threshold float64) []string
}

// ResultFormatter result formatter interface
type ResultFormatter interface {
	// ConvertToWeChatFormat converts to WeChat message format
	ConvertToWeChatFormat(result *CostAnalysisResult) interface{}
}

// QueryExecutor query executor interface
type QueryExecutor interface {
	// ExecuteQuery executes query
	ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]*RawCostData, error)

	// QueryCostDataFromTable queries cost data from single table
	QueryCostDataFromTable(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error)
}

// CacheManager cache manager interface
type CacheManager interface {
	// CacheQuery caches query statement information
	CacheQuery(query string) string

	// ClearQueryCache clears query cache
	ClearQueryCache()

	// GetCacheStats gets cache statistics
	GetCacheStats() map[string]interface{}
}

// MetricsCollector metrics collector interface
type MetricsCollector interface {
	// RecordQueryMetrics records query metrics
	RecordQueryMetrics(duration time.Duration, err error)

	// GetQueryMetrics gets query performance metrics
	GetQueryMetrics() QueryMetrics
}

// Analyzer main analyzer interface
type Analyzer interface {
	// AnalyzeDailyCosts analyzes daily cost comparison
	AnalyzeDailyCosts(ctx context.Context, req *CostAnalysisRequest) (*CostAnalysisResult, error)

	// SetAlertThreshold sets alert threshold
	SetAlertThreshold(threshold float64)

	// SetQueryTimeout sets query timeout
	SetQueryTimeout(timeout time.Duration)

	// Close closes analyzer and cleans up resources
	Close() error

	// GetConnectionInfo gets connection information
	GetConnectionInfo() map[string]interface{}
}
