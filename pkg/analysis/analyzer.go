package analysis

import (
	"context"
	"crypto/sha256"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"log/slog"
	"strings"
	"sync"
	"time"

	chSDK "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CostAnalyzer 费用分析器
type CostAnalyzer struct {
	// 连接相关
	chClient         *clickhouse.Client
	directConn       driver.Conn
	config           *config.ClickHouseConfig
	nameResolver     *clickhouse.TableNameResolver
	connectionPooled bool

	// 组件
	calculator *costCalculator
	aggregator *dataAggregator
	formatter  *resultFormatter

	// 缓存和指标
	queryCache     map[string]*QueryCacheEntry
	cacheMutex     sync.RWMutex
	metrics        *QueryMetrics

	// 配置
	alertThreshold float64
	queryTimeout   time.Duration
}

// QueryCacheEntry 查询缓存条目
type QueryCacheEntry struct {
	Query    string
	LastUsed time.Time
	UseCount int64
	CachedAt time.Time
	TTL      time.Duration
}

// QueryMetrics 查询性能指标
type QueryMetrics struct {
	TotalQueries    int64
	TotalDuration   time.Duration
	AverageDuration time.Duration
	SlowQueries     int64 // 超过1秒的查询
	ErrorCount      int64
	CacheHits       int64
	CacheMisses     int64
	mu              sync.Mutex
}

// NewCostAnalyzer 创建费用分析器
func NewCostAnalyzer(chClient *clickhouse.Client) *CostAnalyzer {
	ca := &CostAnalyzer{
		chClient:       chClient,
		alertThreshold: 20.0, // 默认20%变化阈值
		queryCache:     make(map[string]*QueryCacheEntry),
		queryTimeout:   30 * time.Second, // 默认30秒超时
		metrics:        &QueryMetrics{},
	}

	// 如果提供了 chClient，尝试获取配置和直接连接
	if chClient != nil {
		ca.nameResolver = chClient.GetTableNameResolver()
	}
	
	// 初始化组件（需要在设置 nameResolver 之后）
	ca.initComponents()

	return ca
}

// NewCostAnalyzerWithConfig 使用配置创建费用分析器（支持直接连接）
func NewCostAnalyzerWithConfig(cfg *config.ClickHouseConfig) (*CostAnalyzer, error) {
	if cfg == nil {
		return nil, wrapError(ErrInvalidConfig, "配置为空")
	}

	ca := &CostAnalyzer{
		config:         cfg,
		alertThreshold: 20.0,
		queryCache:     make(map[string]*QueryCacheEntry),
		queryTimeout:   30 * time.Second,
		metrics:        &QueryMetrics{},
	}

	// 创建名称解析器
	ca.nameResolver = clickhouse.NewTableNameResolver(cfg)
	
	// 先创建直接连接
	if err := ca.initDirectConnection(); err != nil {
		return nil, wrapError(err, "初始化直接连接失败")
	}
	
	// 初始化组件（需要在创建nameResolver和directConn之后）
	ca.initComponents()

	return ca, nil
}

// initComponents 初始化组件
func (ca *CostAnalyzer) initComponents() {
	ca.calculator = newCostCalculator(ca.alertThreshold)
	ca.aggregator = newDataAggregator(ca.chClient, ca.directConn, ca.nameResolver)
	ca.formatter = newResultFormatter()
}

// AnalyzeDailyCosts 分析每日费用对比
func (ca *CostAnalyzer) AnalyzeDailyCosts(ctx context.Context, req *CostAnalysisRequest) (*CostAnalysisResult, error) {
	// 验证请求参数
	if err := ca.validateAnalysisRequest(req); err != nil {
		return nil, wrapError(err, "请求参数验证失败")
	}

	// 设置默认值
	ca.setDefaultRequestValues(req)

	// 计算前一天日期
	yesterday := req.Date.AddDate(0, 0, -1)

	slog.Info("开始分析费用数据",
		"today", req.Date.Format("2006-01-02"),
		"yesterday", yesterday.Format("2006-01-02"),
		"providers", req.Providers)

	// 获取指定云服务商的费用数据
	rawData, err := ca.aggregator.GetCostDataForDates(ctx, []time.Time{yesterday, req.Date}, req.Providers)
	if err != nil {
		return nil, wrapError(err, "获取费用数据失败")
	}

	// 计算成本变化
	result, err := ca.calculator.CalculateCostChanges(rawData, yesterday, req.Date)
	if err != nil {
		return nil, wrapError(err, "计算成本变化失败")
	}

	// 生成告警
	result.Alerts = ca.calculator.GenerateAlerts(result, req.AlertThreshold)

	slog.Info("费用分析完成",
		"providers", len(result.Providers),
		"total_yesterday", result.TotalCost.YesterdayCost,
		"total_today", result.TotalCost.TodayCost,
		"alerts", len(result.Alerts))

	return result, nil
}

// SetAlertThreshold 设置告警阈值
func (ca *CostAnalyzer) SetAlertThreshold(threshold float64) {
	ca.alertThreshold = threshold
	if ca.calculator != nil {
		ca.calculator.alertThreshold = threshold
	}
}

// SetQueryTimeout 设置查询超时时间
func (ca *CostAnalyzer) SetQueryTimeout(timeout time.Duration) {
	ca.queryTimeout = timeout
	if ca.aggregator != nil {
		ca.aggregator.setQueryTimeout(timeout)
	}
}

// ConvertToWeChatFormat 转换为企业微信消息格式
func (ca *CostAnalyzer) ConvertToWeChatFormat(result *CostAnalysisResult) interface{} {
	return ca.formatter.ConvertToWeChatFormat(result)
}

// BatchQueryCostData 批量查询费用数据，支持并发查询多个表
func (ca *CostAnalyzer) BatchQueryCostData(ctx context.Context, tables []DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	return ca.aggregator.BatchQueryCostData(ctx, tables, dates)
}

// Close 关闭分析器并清理资源
func (ca *CostAnalyzer) Close() error {
	ca.cacheMutex.Lock()
	defer ca.cacheMutex.Unlock()

	// 清理查询缓存
	ca.queryCache = make(map[string]*QueryCacheEntry)

	// 关闭直接连接
	if ca.directConn != nil {
		if err := ca.directConn.Close(); err != nil {
			slog.Warn("关闭直接连接失败", "error", err)
			return wrapError(ErrResourceCleanupFailed, "关闭直接连接失败: %v", err)
		}
		ca.directConn = nil
	}

	slog.Info("CostAnalyzer 资源已清理")
	return nil
}

// GetQueryMetrics 获取查询性能指标
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

// GetConnectionInfo 获取连接信息
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

	// 获取查询指标
	metrics := ca.GetQueryMetrics()
	info["query_metrics"] = map[string]interface{}{
		"total_queries":    metrics.TotalQueries,
		"total_duration":   metrics.TotalDuration.String(),
		"average_duration": metrics.AverageDuration.String(),
		"slow_queries":     metrics.SlowQueries,
		"error_count":      metrics.ErrorCount,
	}

	// 查询缓存信息
	ca.cacheMutex.RLock()
	info["query_cache_size"] = len(ca.queryCache)
	ca.cacheMutex.RUnlock()

	return info
}

// ClearQueryCache 清理查询缓存
func (ca *CostAnalyzer) ClearQueryCache() {
	ca.cacheMutex.Lock()
	defer ca.cacheMutex.Unlock()

	oldSize := len(ca.queryCache)
	ca.queryCache = make(map[string]*QueryCacheEntry)
	slog.Info("查询缓存已清理", "old_size", oldSize)
}

// GetCacheStats 获取缓存统计信息
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

// EnableDirectConnection 启用直接连接模式
func (ca *CostAnalyzer) EnableDirectConnection(cfg *config.ClickHouseConfig) error {
	if ca.directConn != nil {
		slog.Warn("直接连接已存在，将关闭旧连接")
		ca.directConn.Close()
	}

	ca.config = cfg
	ca.nameResolver = clickhouse.NewTableNameResolver(cfg)

	// 先初始化直接连接
	if err := ca.initDirectConnection(); err != nil {
		return err
	}
	
	// 然后重新初始化聚合器（此时directConn已经设置好了）
	ca.aggregator = newDataAggregator(ca.chClient, ca.directConn, ca.nameResolver)
	
	return nil
}

// IsDirectConnectionEnabled 检查是否启用了直接连接
func (ca *CostAnalyzer) IsDirectConnectionEnabled() bool {
	return ca.directConn != nil
}

// validateAnalysisRequest 验证分析请求
func (ca *CostAnalyzer) validateAnalysisRequest(req *CostAnalysisRequest) error {
	if req == nil {
		return NewValidationError("request", req, "请求参数不能为空")
	}

	if req.AlertThreshold < 0 || req.AlertThreshold > 100 {
		return NewValidationError("alert_threshold", req.AlertThreshold, "告警阈值必须在0-100之间")
	}

	return nil
}

// setDefaultRequestValues 设置请求默认值
func (ca *CostAnalyzer) setDefaultRequestValues(req *CostAnalysisRequest) {
	// 如果没有指定日期，默认对比昨天和前天
	if req.Date.IsZero() {
		req.Date = time.Now().AddDate(0, 0, -1) // 昨天
	}

	if req.AlertThreshold == 0 {
		req.AlertThreshold = ca.alertThreshold
	}
}

// initDirectConnection 初始化直接 ClickHouse 连接
func (ca *CostAnalyzer) initDirectConnection() error {
	if ca.config == nil {
		return wrapError(ErrInvalidConfig, "配置为空，无法创建直接连接")
	}

	opts := ca.buildConnectionOptions()

	conn, err := chSDK.Open(opts)
	if err != nil {
		return NewConnectionError("direct", "", "创建直接连接失败", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return NewConnectionError("direct", "", "直接连接测试失败", err)
	}

	ca.directConn = conn
	ca.connectionPooled = true

	// 重新初始化聚合器以使用新连接
	ca.aggregator = newDataAggregator(ca.chClient, ca.directConn, ca.nameResolver)

	slog.Info("直接 ClickHouse 连接初始化成功",
		"addresses", ca.config.GetAddresses(),
		"database", ca.config.Database,
		"protocol", ca.config.GetProtocol())

	return nil
}

// buildConnectionOptions 构建连接选项
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
		MaxOpenConns:         20, // 增加连接池大小
		MaxIdleConns:         10, // 增加空闲连接数
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     chSDK.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	}

	// 只有在 Native 协议时才设置压缩
	if ca.config.GetProtocol() == chSDK.Native {
		opts.Compression = &chSDK.Compression{
			Method: chSDK.CompressionLZ4,
		}
	}

	return opts
}

// recordQueryMetrics 记录查询指标
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

// cacheQuery 缓存查询语句信息
func (ca *CostAnalyzer) cacheQuery(query string) string {
	queryKey := fmt.Sprintf("cost_query_%s", sha256Hash(query))

	ca.cacheMutex.Lock()
	defer ca.cacheMutex.Unlock()

	now := time.Now()
	if entry, exists := ca.queryCache[queryKey]; exists {
		entry.LastUsed = now
		entry.UseCount++
		ca.metrics.CacheHits++
		slog.Debug("查询缓存命中", "key", queryKey, "use_count", entry.UseCount)
	} else {
		ca.queryCache[queryKey] = &QueryCacheEntry{
			Query:    query,
			LastUsed: now,
			UseCount: 1,
			CachedAt: now,
			TTL:      time.Hour, // 默认缓存1小时
		}
		ca.metrics.CacheMisses++
		slog.Debug("查询缓存新增", "key", queryKey, "cache_size", len(ca.queryCache))
	}

	// 清理过期的缓存条目
	ca.cleanExpiredCache()

	return queryKey
}

// cleanExpiredCache 清理过期的缓存条目（在持有锁的情况下调用）
func (ca *CostAnalyzer) cleanExpiredCache() {
	now := time.Now()
	for key, entry := range ca.queryCache {
		if now.Sub(entry.CachedAt) > entry.TTL {
			delete(ca.queryCache, key)
			slog.Debug("清理过期缓存", "key", key, "age", now.Sub(entry.CachedAt))
		}
	}
}

// sha256Hash 计算字符串的 SHA256 哈希值
func sha256Hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}

// 工具函数

// extractBaseProvider 提取基础的云服务商标识（去掉_monthly、_daily等后缀）
func extractBaseProvider(tableKey string) string {
	if strings.HasSuffix(tableKey, "_monthly") {
		return strings.TrimSuffix(tableKey, "_monthly")
	}
	if strings.HasSuffix(tableKey, "_daily") {
		return strings.TrimSuffix(tableKey, "_daily")
	}
	return tableKey
}

// contains 检查字符串切片是否包含指定值
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// min 返回两个整数中的最小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}