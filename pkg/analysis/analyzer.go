package analysis

import (
	"context"
	"crypto/sha256"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"strings"
	"sync"
	"time"

	chSDK "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// CostAnalyzer represents a cost analyzer
type CostAnalyzer struct {
	// connection related
	chClient         *clickhouse.Client
	directConn       driver.Conn
	config           *config.ClickHouseConfig
	nameResolver     *clickhouse.TableNameResolver
	connectionPooled bool

	// components
	calculator *costCalculator
	aggregator *dataAggregator
	formatter  *resultFormatter

	// caching and metrics
	queryCache map[string]*QueryCacheEntry
	cacheMutex sync.RWMutex
	metrics    *QueryMetrics

	// configuration
	alertThreshold float64
	queryTimeout   time.Duration
}

// QueryCacheEntry represents a query cache entry
type QueryCacheEntry struct {
	Query    string
	LastUsed time.Time
	UseCount int64
	CachedAt time.Time
	TTL      time.Duration
}

// QueryMetrics represents query performance metrics
type QueryMetrics struct {
	TotalQueries    int64
	TotalDuration   time.Duration
	AverageDuration time.Duration
	SlowQueries     int64 // queries exceeding 1 second
	ErrorCount      int64
	CacheHits       int64
	CacheMisses     int64
	mu              sync.Mutex
}

// NewCostAnalyzer creates a cost analyzer
func NewCostAnalyzer(chClient *clickhouse.Client) *CostAnalyzer {
	ca := &CostAnalyzer{
		chClient:       chClient,
		alertThreshold: 20.0, // default 20% change threshold
		queryCache:     make(map[string]*QueryCacheEntry),
		queryTimeout:   30 * time.Second, // default 30 second timeout
		metrics:        &QueryMetrics{},
	}

	// if chClient is provided, try to get configuration and direct connection
	if chClient != nil {
		ca.nameResolver = chClient.GetTableNameResolver()
	}

	// initialize components (needs to be after setting nameResolver)
	ca.initComponents()

	return ca
}

// NewCostAnalyzerWithConfig creates a cost analyzer with configuration (supports direct connection)
func NewCostAnalyzerWithConfig(cfg *config.ClickHouseConfig) (*CostAnalyzer, error) {
	if cfg == nil {
		return nil, wrapError(ErrInvalidConfig, "configuration is empty")
	}

	ca := &CostAnalyzer{
		config:         cfg,
		alertThreshold: 20.0,
		queryCache:     make(map[string]*QueryCacheEntry),
		queryTimeout:   30 * time.Second,
		metrics:        &QueryMetrics{},
	}

	// create name resolver
	ca.nameResolver = clickhouse.NewTableNameResolver(cfg)

	// create direct connection first
	if err := ca.initDirectConnection(); err != nil {
		return nil, wrapError(err, "failed to initialize direct connection")
	}

	// initialize components (needs to be after creating nameResolver and directConn)
	ca.initComponents()

	return ca, nil
}

// initComponents initializes components
func (ca *CostAnalyzer) initComponents() {
	ca.calculator = newCostCalculator(ca.alertThreshold)
	ca.aggregator = newDataAggregator(ca.chClient, ca.directConn, ca.nameResolver)
	ca.formatter = newResultFormatter()
}

// AnalyzeDailyCosts analyzes daily cost comparison
func (ca *CostAnalyzer) AnalyzeDailyCosts(ctx context.Context, req *CostAnalysisRequest) (*CostAnalysisResult, error) {
	// validate request parameters
	if err := ca.validateAnalysisRequest(req); err != nil {
		return nil, wrapError(err, "request parameter validation failed")
	}

	// set default values
	ca.setDefaultRequestValues(req)

	// calculate previous day's date
	yesterday := req.Date.AddDate(0, 0, -1)

	logger.Info("Starting cost data analysis",
		zap.String("today", req.Date.Format("2006-01-02")),
		zap.String("yesterday", yesterday.Format("2006-01-02")),
		zap.Strings("providers", req.Providers),
		zap.Float64("alert_threshold", req.AlertThreshold))

	// get cost data for specified cloud providers
	rawData, err := ca.aggregator.GetCostDataForDates(ctx, []time.Time{yesterday, req.Date}, req.Providers)
	if err != nil {
		return nil, wrapError(err, "failed to get cost data")
	}

	// calculate cost changes
	result, err := ca.calculator.CalculateCostChanges(rawData, yesterday, req.Date)
	if err != nil {
		return nil, wrapError(err, "failed to calculate cost changes")
	}

	// generate alerts
	result.Alerts = ca.calculator.GenerateAlerts(result, req.AlertThreshold)

	logger.Info("Cost analysis completed",
		zap.Int("providers_count", len(result.Providers)),
		zap.Float64("total_yesterday", result.TotalCost.YesterdayCost),
		zap.Float64("total_today", result.TotalCost.TodayCost),
		zap.Float64("total_change_percent", result.TotalCost.ChangePercent),
		zap.Int("alerts_count", len(result.Alerts)))

	return result, nil
}

// SetAlertThreshold sets alert threshold
func (ca *CostAnalyzer) SetAlertThreshold(threshold float64) {
	ca.alertThreshold = threshold
	if ca.calculator != nil {
		ca.calculator.alertThreshold = threshold
	}
}

// SetQueryTimeout sets query timeout
func (ca *CostAnalyzer) SetQueryTimeout(timeout time.Duration) {
	ca.queryTimeout = timeout
	if ca.aggregator != nil {
		ca.aggregator.setQueryTimeout(timeout)
	}
}

// ConvertToWeChatFormat converts to WeChat Work message format
func (ca *CostAnalyzer) ConvertToWeChatFormat(result *CostAnalysisResult) interface{} {
	return ca.formatter.ConvertToWeChatFormat(result)
}

// GenerateReportImage generates cost analysis report as PNG image
func (ca *CostAnalyzer) GenerateReportImage(result *CostAnalysisResult) ([]byte, error) {
	// Use modern Apple-style generator with enhanced design system
	generator := NewModernAppleReportGenerator()
	
	// Generate PNG image
	return generator.GenerateReport(result)
}

// BatchQueryCostData batch queries cost data with concurrent querying of multiple tables
func (ca *CostAnalyzer) BatchQueryCostData(ctx context.Context, tables []DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	return ca.aggregator.BatchQueryCostData(ctx, tables, dates)
}

// Close closes the analyzer and cleans up resources
func (ca *CostAnalyzer) Close() error {
	ca.cacheMutex.Lock()
	defer ca.cacheMutex.Unlock()

	// clean up query cache
	ca.queryCache = make(map[string]*QueryCacheEntry)

	// close direct connection
	if ca.directConn != nil {
		if err := ca.directConn.Close(); err != nil {
			logger.Warn("Failed to close direct connection", zap.Error(err))
			return wrapError(ErrResourceCleanupFailed, "failed to close direct connection: %v", err)
		}
		ca.directConn = nil
	}

	logger.Info("CostAnalyzer resources cleaned up")
	return nil
}

// GetQueryMetrics gets query performance metrics
func (ca *CostAnalyzer) GetQueryMetrics() QueryMetrics {
	ca.metrics.mu.Lock()
	defer ca.metrics.mu.Unlock()
	return QueryMetrics{
		TotalQueries:    ca.metrics.TotalQueries,
		TotalDuration:   ca.metrics.TotalDuration,
		AverageDuration: ca.metrics.AverageDuration,
		SlowQueries:     ca.metrics.SlowQueries,
		ErrorCount:      ca.metrics.ErrorCount,
		CacheHits:       ca.metrics.CacheHits,
		CacheMisses:     ca.metrics.CacheMisses,
	}
}

// GetConnectionInfo gets connection information
func (ca *CostAnalyzer) GetConnectionInfo() map[string]interface{} {
	info := make(map[string]interface{})

	info["direct_connection"] = ca.directConn != nil
	info["legacy_client"] = ca.chClient != nil
	info["connection_pooled"] = ca.connectionPooled
	info["query_timeout"] = ca.queryTimeout

	if ca.config != nil {
		info["addresses"] = ca.config.GetAddresses()
		info["database"] = ca.config.Database
		info["protocol"] = ca.config.GetProtocol()
	}

	// get query metrics
	metrics := ca.GetQueryMetrics()
	info["query_metrics"] = map[string]interface{}{
		"total_queries":    metrics.TotalQueries,
		"total_duration":   metrics.TotalDuration.String(),
		"average_duration": metrics.AverageDuration.String(),
		"slow_queries":     metrics.SlowQueries,
		"error_count":      metrics.ErrorCount,
	}

	// query cache information
	ca.cacheMutex.RLock()
	info["query_cache_size"] = len(ca.queryCache)
	ca.cacheMutex.RUnlock()

	return info
}

// ClearQueryCache clears query cache
func (ca *CostAnalyzer) ClearQueryCache() {
	ca.cacheMutex.Lock()
	defer ca.cacheMutex.Unlock()

	oldSize := len(ca.queryCache)
	ca.queryCache = make(map[string]*QueryCacheEntry)
	logger.Info("Query cache cleared", zap.Int("old_size", oldSize))
}

// GetCacheStats gets cache statistics
func (ca *CostAnalyzer) GetCacheStats() map[string]interface{} {
	ca.cacheMutex.RLock()
	defer ca.cacheMutex.RUnlock()

	stats := make(map[string]interface{})
	stats["cache_size"] = len(ca.queryCache)
	stats["cache_hits"] = ca.metrics.CacheHits
	stats["cache_misses"] = ca.metrics.CacheMisses

	hitRate := float64(0)
	total := ca.metrics.CacheHits + ca.metrics.CacheMisses
	if total > 0 {
		hitRate = float64(ca.metrics.CacheHits) / float64(total) * 100
	}
	stats["hit_rate"] = fmt.Sprintf("%.2f%%", hitRate)

	return stats
}

// EnableDirectConnection enables direct connection mode
func (ca *CostAnalyzer) EnableDirectConnection(cfg *config.ClickHouseConfig) error {
	if ca.directConn != nil {
		logger.Warn("Direct connection already exists, closing old connection",
			zap.String("action", "replacing_connection"))
		ca.directConn.Close()
	}

	ca.config = cfg
	ca.nameResolver = clickhouse.NewTableNameResolver(cfg)

	// initialize direct connection first
	if err := ca.initDirectConnection(); err != nil {
		return err
	}

	// then reinitialize aggregator (directConn is already set at this point)
	ca.aggregator = newDataAggregator(ca.chClient, ca.directConn, ca.nameResolver)

	return nil
}

// IsDirectConnectionEnabled checks if direct connection is enabled
func (ca *CostAnalyzer) IsDirectConnectionEnabled() bool {
	return ca.directConn != nil
}

// validateAnalysisRequest validates analysis request
func (ca *CostAnalyzer) validateAnalysisRequest(req *CostAnalysisRequest) error {
	if req == nil {
		return NewValidationError("request", req, "request parameter cannot be empty")
	}

	if req.AlertThreshold < 0 || req.AlertThreshold > 100 {
		return NewValidationError("alert_threshold", req.AlertThreshold, "alert threshold must be between 0-100")
	}

	return nil
}

// setDefaultRequestValues sets default request values
func (ca *CostAnalyzer) setDefaultRequestValues(req *CostAnalysisRequest) {
	// if no date specified, default to comparing yesterday and the day before
	if req.Date.IsZero() {
		req.Date = time.Now().AddDate(0, 0, -1) // yesterday
	}

	if req.AlertThreshold == 0 {
		req.AlertThreshold = ca.alertThreshold
	}
}

// initDirectConnection initializes direct ClickHouse connection
func (ca *CostAnalyzer) initDirectConnection() error {
	if ca.config == nil {
		return wrapError(ErrInvalidConfig, "configuration is empty, cannot create direct connection")
	}

	opts := ca.buildConnectionOptions()

	conn, err := chSDK.Open(opts)
	if err != nil {
		return NewConnectionError("direct", "", "failed to create direct connection", err)
	}

	// test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return NewConnectionError("direct", "", "direct connection test failed", err)
	}

	ca.directConn = conn
	ca.connectionPooled = true

	// reinitialize aggregator to use new connection
	ca.aggregator = newDataAggregator(ca.chClient, ca.directConn, ca.nameResolver)

	logger.Info("Direct ClickHouse connection initialized successfully",
		zap.Strings("addresses", ca.config.GetAddresses()),
		zap.String("database", ca.config.Database),
		zap.String("protocol", ca.config.GetProtocol().String()))

	return nil
}

// buildConnectionOptions builds connection options
func (ca *CostAnalyzer) buildConnectionOptions() *chSDK.Options {
	opts := &chSDK.Options{
		Addr: ca.config.GetAddresses(),
		Auth: chSDK.Auth{
			Database: ca.config.Database,
			Username: ca.config.Username,
			Password: ca.config.Password,
		},
		Debug:    ca.config.Debug,
		Protocol: ca.config.GetProtocol(),
		Settings: chSDK.Settings{
			"max_execution_time":               60,
			"max_memory_usage":                 "4000000000", // 4GB
			"max_result_rows":                  1000000,
			"max_result_bytes":                 "100000000", // 100MB
			"result_overflow_mode":             "break",
			"readonly":                         1,
			"enable_http_compression":          1,
			"log_queries":                      1,
			"log_query_threads":                1,
			"connect_timeout_with_failover_ms": 5000,
			"receive_timeout":                  30000,
			"send_timeout":                     30000,
		},
		DialTimeout:          10 * time.Second,
		MaxOpenConns:         20, // increase connection pool size
		MaxIdleConns:         10, // increase idle connection count
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     chSDK.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	}

	// only set compression when using Native protocol
	if ca.config.GetProtocol() == chSDK.Native {
		opts.Compression = &chSDK.Compression{
			Method: chSDK.CompressionLZ4,
		}
	}

	return opts
}

// recordQueryMetrics records query metrics
func (ca *CostAnalyzer) recordQueryMetrics(duration time.Duration, err error) {
	ca.metrics.mu.Lock()
	defer ca.metrics.mu.Unlock()

	ca.metrics.TotalQueries++
	ca.metrics.TotalDuration += duration

	if ca.metrics.TotalQueries > 0 {
		ca.metrics.AverageDuration = ca.metrics.TotalDuration / time.Duration(ca.metrics.TotalQueries)
	}

	if duration > time.Second {
		ca.metrics.SlowQueries++
	}

	if err != nil {
		ca.metrics.ErrorCount++
	}
}

// cacheQuery caches query statement information
func (ca *CostAnalyzer) cacheQuery(query string) string {
	queryKey := fmt.Sprintf("cost_query_%s", sha256Hash(query))

	ca.cacheMutex.Lock()
	defer ca.cacheMutex.Unlock()

	now := time.Now()
	if entry, exists := ca.queryCache[queryKey]; exists {
		entry.LastUsed = now
		entry.UseCount++
		ca.metrics.CacheHits++
		logger.Debug("Query cache hit", zap.String("key", queryKey), zap.Int64("use_count", entry.UseCount))
	} else {
		ca.queryCache[queryKey] = &QueryCacheEntry{
			Query:    query,
			LastUsed: now,
			UseCount: 1,
			CachedAt: now,
			TTL:      time.Hour, // default cache for 1 hour
		}
		ca.metrics.CacheMisses++
		logger.Debug("Query cache entry added", zap.String("key", queryKey), zap.Int("cache_size", len(ca.queryCache)))
	}

	// clean up expired cache entries
	ca.cleanExpiredCache()

	return queryKey
}

// cleanExpiredCache cleans up expired cache entries (called while holding lock)
func (ca *CostAnalyzer) cleanExpiredCache() {
	now := time.Now()
	for key, entry := range ca.queryCache {
		if now.Sub(entry.CachedAt) > entry.TTL {
			delete(ca.queryCache, key)
			logger.Debug("Cleaning expired cache", zap.String("key", key), zap.Duration("age", now.Sub(entry.CachedAt)))
		}
	}
}

// sha256Hash calculates SHA256 hash value of a string
func sha256Hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}

// utility functions

// extractBaseProvider extracts base cloud provider identifier (removes suffixes like _monthly, _daily)
func extractBaseProvider(tableKey string) string {
	if strings.HasSuffix(tableKey, "_monthly") {
		return strings.TrimSuffix(tableKey, "_monthly")
	}
	if strings.HasSuffix(tableKey, "_daily") {
		return strings.TrimSuffix(tableKey, "_daily")
	}
	return tableKey
}

// contains checks if string slice contains specified value
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
