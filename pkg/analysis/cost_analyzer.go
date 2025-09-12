package analysis

import (
	"context"
	"crypto/sha256"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/wechat"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	chSDK "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// CostAnalyzer 费用分析器
type CostAnalyzer struct {
	chClient         *clickhouse.Client
	directConn       driver.Conn // 直接的 ClickHouse 连接
	config           *config.ClickHouseConfig
	nameResolver     *clickhouse.TableNameResolver
	queryCache       map[string]*QueryCacheEntry // 查询缓存
	cacheMutex       sync.RWMutex                // 缓存互斥锁
	alertThreshold   float64                     // 默认告警阈值（百分比）
	queryTimeout     time.Duration               // 查询超时时间
	connectionPooled bool                        // 是否使用连接池
	metrics          *QueryMetrics               // 查询性能指标
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
		// 这里可以获取配置并创建直接连接
		// ca.initDirectConnection()
	}

	return ca
}

// NewCostAnalyzerWithConfig 使用配置创建费用分析器（支持直接连接）
func NewCostAnalyzerWithConfig(cfg *config.ClickHouseConfig) (*CostAnalyzer, error) {
	ca := &CostAnalyzer{
		config:         cfg,
		alertThreshold: 20.0,
		queryCache:     make(map[string]*QueryCacheEntry),
		queryTimeout:   30 * time.Second,
		metrics:        &QueryMetrics{},
	}

	// 创建名称解析器
	ca.nameResolver = clickhouse.NewTableNameResolver(cfg)

	// 创建直接连接
	if err := ca.initDirectConnection(); err != nil {
		return nil, fmt.Errorf("初始化直接连接失败: %w", err)
	}

	return ca, nil
}

// initDirectConnection 初始化直接 ClickHouse 连接
func (ca *CostAnalyzer) initDirectConnection() error {
	if ca.config == nil {
		return fmt.Errorf("配置为空，无法创建直接连接")
	}

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

	conn, err := chSDK.Open(opts)
	if err != nil {
		return fmt.Errorf("创建直接连接失败: %w", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return fmt.Errorf("直接连接测试失败: %w", err)
	}

	ca.directConn = conn
	ca.connectionPooled = true

	slog.Info("直接 ClickHouse 连接初始化成功",
		"addresses", ca.config.GetAddresses(),
		"database", ca.config.Database,
		"protocol", ca.config.GetProtocol())

	return nil
}

// SetAlertThreshold 设置告警阈值
func (ca *CostAnalyzer) SetAlertThreshold(threshold float64) {
	ca.alertThreshold = threshold
}

// AnalyzeDailyCosts 分析每日费用对比
func (ca *CostAnalyzer) AnalyzeDailyCosts(ctx context.Context, req *CostAnalysisRequest) (*CostAnalysisResult, error) {
	// 如果没有指定日期，默认对比昨天和前天
	if req.Date.IsZero() {
		req.Date = time.Now().AddDate(0, 0, -1) // 昨天
	}

	if req.AlertThreshold == 0 {
		req.AlertThreshold = ca.alertThreshold
	}

	// 计算前一天日期（如果Date是昨天，那么yesterday就是前天）
	yesterday := req.Date.AddDate(0, 0, -1)

	slog.Info("开始分析费用数据",
		"today", req.Date.Format("2006-01-02"),
		"yesterday", yesterday.Format("2006-01-02"),
		"providers", req.Providers)

	// 获取指定云服务商的费用数据，支持平台过滤
	rawData, err := ca.getCostDataForDates(ctx, []time.Time{yesterday, req.Date}, req.Providers)
	if err != nil {
		return nil, fmt.Errorf("获取费用数据失败: %w", err)
	}

	// 分析数据
	result := &CostAnalysisResult{
		Date:          req.Date,
		YesterdayDate: yesterday,
		Providers:     make([]*ProviderCostMetric, 0),
		Alerts:        make([]string, 0),
		GeneratedAt:   time.Now(),
	}

	// 按服务商分组数据
	providerData := ca.groupDataByProvider(rawData)

	// 计算总费用
	totalMetric := &CostMetric{
		Name:     "总费用",
		Currency: "CNY", // 默认人民币
	}

	// 分析各服务商数据
	for provider, data := range providerData {
		providerMetric := ca.analyzeProviderCosts(provider, data, yesterday, req.Date)
		if providerMetric != nil && len(providerMetric.Products) > 0 {
			result.Providers = append(result.Providers, providerMetric)

			// 累加到总费用
			totalMetric.YesterdayCost += providerMetric.TotalCost.YesterdayCost
			totalMetric.TodayCost += providerMetric.TotalCost.TodayCost
		}
	}

	// 计算总费用变化
	totalMetric.CalculateChange()
	totalMetric.SetSignificant(req.AlertThreshold)
	result.TotalCost = totalMetric

	// 生成告警
	result.Alerts = ca.generateAlerts(result, req.AlertThreshold)

	// 按服务商名称排序
	sort.Slice(result.Providers, func(i, j int) bool {
		return result.Providers[i].Provider < result.Providers[j].Provider
	})

	slog.Info("费用分析完成",
		"providers", len(result.Providers),
		"total_yesterday", totalMetric.YesterdayCost,
		"total_today", totalMetric.TodayCost,
		"alerts", len(result.Alerts))

	return result, nil
}

// getCostDataForDates 获取指定日期的费用数据，支持云服务商过滤
func (ca *CostAnalyzer) getCostDataForDates(ctx context.Context, dates []time.Time, providers []string) ([]*RawCostData, error) {
	var allData []*RawCostData
	allTableInfo := GetProviderTableInfo()

	// 如果没有指定provider，使用所有的
	if len(providers) == 0 {
		providers = make([]string, 0, len(allTableInfo))
		for provider := range allTableInfo {
			// 提取基础的云服务商标识（去掉_monthly、_daily等后缀）
			baseProvider := extractBaseProvider(provider)
			if !contains(providers, baseProvider) {
				providers = append(providers, baseProvider)
			}
		}
	}

	// 根据指定的云服务商过滤表信息
	for tableKey, info := range allTableInfo {
		baseProvider := extractBaseProvider(tableKey)

		// 检查是否在指定的云服务商列表中
		if !contains(providers, baseProvider) {
			slog.Debug("跳过非指定云服务商的表", "table", info.TableName, "provider", baseProvider, "requested_providers", providers)
			continue
		}

		data, err := ca.queryCostDataFromTable(ctx, info, dates)
		if err != nil {
			// 获取解析后的表名用于日志
			resolvedTableName := info.TableName
			if ca.chClient != nil && ca.chClient.GetTableNameResolver() != nil {
				resolvedTableName = ca.chClient.GetTableNameResolver().ResolveQueryTarget(info.TableName)
			}
			slog.Warn("查询表数据失败", "original_table", info.TableName, "resolved_table", resolvedTableName, "provider", baseProvider, "error", err)
			continue // 继续查询其他表
		}
		slog.Info("成功查询表数据", "table", info.TableName, "provider", baseProvider, "records", len(data))
		allData = append(allData, data...)
	}

	return allData, nil
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
			return err
		}
		ca.directConn = nil
	}

	slog.Info("CostAnalyzer 资源已清理")
	return nil
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
		// 注意：不复制mutex字段
	}
}

// SetQueryTimeout 设置查询超时时间
func (ca *CostAnalyzer) SetQueryTimeout(timeout time.Duration) {
	ca.queryTimeout = timeout
}

// directQueryCostData 使用直接连接查询费用数据
func (ca *CostAnalyzer) directQueryCostData(ctx context.Context, query string, args ...interface{}) ([]*RawCostData, error) {
	if ca.directConn == nil {
		return nil, fmt.Errorf("直接连接未初始化")
	}

	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		ca.recordQueryMetrics(duration, nil)
		slog.Debug("直接查询执行时间", "duration", duration, "query_preview", query[:min(100, len(query))])
	}()

	// 创建带超时的上下文
	queryCtx, cancel := context.WithTimeout(ctx, ca.queryTimeout)
	defer cancel()

	// 执行查询
	rows, err := ca.directConn.Query(queryCtx, query, args...)
	if err != nil {
		ca.recordQueryMetrics(time.Since(startTime), err)
		return nil, fmt.Errorf("直接查询执行失败: %w", err)
	}
	defer rows.Close()

	var data []*RawCostData
	rowCount := 0

	for rows.Next() {
		var item RawCostData
		if err := rows.Scan(
			&item.Provider,
			&item.Product,
			&item.ExpenseDate,
			&item.TotalAmount,
			&item.Currency,
			&item.RecordCount,
		); err != nil {
			slog.Warn("扫描行数据失败", "error", err, "row", rowCount)
			continue
		}
		data = append(data, &item)
		rowCount++

		// 防止内存溢出，限制最大行数
		if rowCount > 100000 {
			slog.Warn("查询结果超过最大行数限制", "limit", 100000, "query", query[:min(100, len(query))])
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历查询结果失败: %w", err)
	}

	slog.Info("直接查询完成", "rows", len(data), "duration", time.Since(startTime))
	return data, nil
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

// queryCostDataFromTable 从单个表查询费用数据（原方法，保持兼容性）
func (ca *CostAnalyzer) queryCostDataFromTable(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	// 优先使用直接连接
	if ca.directConn != nil {
		return ca.queryCostDataFromTableDirect(ctx, info, dates)
	}

	// 回退到原来的封装方法
	return ca.queryCostDataFromTableLegacy(ctx, info, dates)
}

// queryCostDataFromTableDirect 使用直接连接查询费用数据
func (ca *CostAnalyzer) queryCostDataFromTableDirect(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	startTime := time.Now()

	// 使用表名解析器解析目标查询表名
	resolvedTableName := info.TableName
	if ca.nameResolver != nil {
		resolvedTableName = ca.nameResolver.ResolveQueryTarget(info.TableName)
	}

	slog.Debug("直接查询表名解析", "original", info.TableName, "resolved", resolvedTableName)

	// 构建参数化查询，避免SQL注入
	placeholders := make([]string, len(dates))
	args := make([]interface{}, len(dates))
	for i, date := range dates {
		placeholders[i] = "?"
		args[i] = date.Format("2006-01-02")
	}
	dateCondition := "(" + strings.Join(placeholders, ", ") + ")"

	// 构建优化的查询语句
	query := fmt.Sprintf(`
		SELECT 
			'%s' as provider,
			%s as product,
			%s as expense_date,
			SUM(%s) as total_amount,
			%s as currency,
			COUNT(*) as record_count
		FROM %s
		WHERE %s IN %s
			AND %s > 0
			AND %s IS NOT NULL
			AND %s != ''
		GROUP BY provider, product, expense_date, currency
		ORDER BY provider, product, expense_date`,
		info.Provider,
		info.ProductColumn,
		info.DateColumn,
		info.AmountColumn,
		info.CurrencyColumn,
		resolvedTableName,
		info.DateColumn,
		dateCondition,
		info.AmountColumn,
		info.ProductColumn,
		info.ProductColumn)

	// 缓存查询信息
	queryKey := ca.cacheQuery(query)

	slog.Info("执行直接费用查询",
		"query_key", queryKey,
		"provider", info.Provider,
		"table", resolvedTableName,
		"dates", len(dates))

	// 执行直接查询
	data, err := ca.directQueryCostData(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("直接查询表 %s 失败: %w", resolvedTableName, err)
	}

	duration := time.Since(startTime)
	slog.Info("直接查询表完成",
		"table", resolvedTableName,
		"provider", info.Provider,
		"records", len(data),
		"duration", duration,
		"query_key", queryKey)

	return data, nil
}

// queryCostDataFromTableLegacy 使用封装客户端查询费用数据（原实现）
func (ca *CostAnalyzer) queryCostDataFromTableLegacy(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	// 构建日期条件
	dateStrs := make([]string, len(dates))
	for i, date := range dates {
		dateStrs[i] = "'" + date.Format("2006-01-02") + "'"
	}
	dateCondition := "(" + fmt.Sprintf("%s", dateStrs[0])
	for i := 1; i < len(dateStrs); i++ {
		dateCondition += ", " + dateStrs[i]
	}
	dateCondition += ")"

	// 使用表名解析器解析目标查询表名
	resolvedTableName := info.TableName
	if ca.chClient != nil && ca.chClient.GetTableNameResolver() != nil {
		resolvedTableName = ca.chClient.GetTableNameResolver().ResolveQueryTarget(info.TableName)
	}
	slog.Debug("Legacy表名解析", "original", info.TableName, "resolved", resolvedTableName)

	query := fmt.Sprintf(`
		SELECT 
			'%s' as provider,
			%s as product,
			%s as expense_date,
			SUM(%s) as total_amount,
			%s as currency,
			COUNT(*) as record_count
		FROM %s
		WHERE %s IN %s
			AND %s > 0
		GROUP BY provider, product, expense_date, currency
		ORDER BY provider, product, expense_date`,
		info.Provider,
		info.ProductColumn,
		info.DateColumn,
		info.AmountColumn,
		info.CurrencyColumn,
		resolvedTableName, // 使用解析后的表名
		info.DateColumn,
		dateCondition,
		info.AmountColumn)

	slog.Info("执行Legacy费用查询SQL", "query", query, "provider", info.Provider, "table", resolvedTableName)

	if ca.chClient == nil {
		return nil, fmt.Errorf("ClickHouse客户端未初始化")
	}

	rows, err := ca.chClient.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("Legacy查询失败: %w", err)
	}
	defer rows.Close()

	var data []*RawCostData
	for rows.Next() {
		var item RawCostData
		if err := rows.Scan(
			&item.Provider,
			&item.Product,
			&item.ExpenseDate,
			&item.TotalAmount,
			&item.Currency,
			&item.RecordCount,
		); err != nil {
			slog.Warn("扫描行数据失败", "error", err)
			continue
		}
		data = append(data, &item)
	}

	return data, nil
}

// groupDataByProvider 按服务商分组数据
func (ca *CostAnalyzer) groupDataByProvider(rawData []*RawCostData) map[string][]*RawCostData {
	grouped := make(map[string][]*RawCostData)

	for _, item := range rawData {
		if _, exists := grouped[item.Provider]; !exists {
			grouped[item.Provider] = make([]*RawCostData, 0)
		}
		grouped[item.Provider] = append(grouped[item.Provider], item)
	}

	return grouped
}

// analyzeProviderCosts 分析单个服务商的费用
func (ca *CostAnalyzer) analyzeProviderCosts(provider string, data []*RawCostData, yesterday, today time.Time) *ProviderCostMetric {
	if len(data) == 0 {
		return nil
	}

	// 按产品分组
	productData := make(map[string]map[string]*RawCostData) // product -> date -> data

	for _, item := range data {
		if _, exists := productData[item.Product]; !exists {
			productData[item.Product] = make(map[string]*RawCostData)
		}
		// 使用日期字符串作为key
		dateKey := item.ExpenseDate.Format("2006-01-02")
		productData[item.Product][dateKey] = item
	}

	// 分析各产品费用
	products := make([]*CostMetric, 0)
	providerTotal := &CostMetric{
		Name:     GetProviderDisplayName(provider),
		Currency: "CNY",
	}

	for product, dates := range productData {
		metric := &CostMetric{
			Name:     product,
			Currency: "CNY", // 默认货币
		}

		// 获取昨天和今天的数据
		yesterdayStr := yesterday.Format("2006-01-02")
		todayStr := today.Format("2006-01-02")

		if yesterdayData, exists := dates[yesterdayStr]; exists {
			metric.YesterdayCost = yesterdayData.TotalAmount
			metric.Currency = yesterdayData.Currency
		}

		if todayData, exists := dates[todayStr]; exists {
			metric.TodayCost = todayData.TotalAmount
			metric.Currency = todayData.Currency
			metric.RecordCount = todayData.RecordCount
		}

		// 计算变化
		metric.CalculateChange()

		// 累加到服务商总计
		providerTotal.YesterdayCost += metric.YesterdayCost
		providerTotal.TodayCost += metric.TodayCost

		products = append(products, metric)
	}

	// 计算服务商总计变化
	providerTotal.CalculateChange()

	// 按产品名称排序
	sort.Slice(products, func(i, j int) bool {
		return products[i].Name < products[j].Name
	})

	return &ProviderCostMetric{
		Provider:    provider,
		DisplayName: GetProviderDisplayName(provider),
		TotalCost:   providerTotal,
		Products:    products,
	}
}

// generateAlerts 生成告警信息
func (ca *CostAnalyzer) generateAlerts(result *CostAnalysisResult, threshold float64) []string {
	var alerts []string

	// 总费用异常
	if result.TotalCost != nil && result.TotalCost.IsSignificant {
		if result.TotalCost.IsIncrease() {
			alerts = append(alerts,
				fmt.Sprintf("总费用增长 %.1f%%，超过告警阈值 %.1f%%",
					result.TotalCost.ChangePercent, threshold))
		} else if result.TotalCost.IsDecrease() {
			alerts = append(alerts,
				fmt.Sprintf("总费用下降 %.1f%%，超过告警阈值 %.1f%%",
					-result.TotalCost.ChangePercent, threshold))
		}
	}

	// 各产品异常
	for _, provider := range result.Providers {
		for _, product := range provider.Products {
			if product.ChangePercent >= threshold {
				alerts = append(alerts,
					fmt.Sprintf("%s %s 费用增长 %.1f%%",
						provider.DisplayName, product.Name, product.ChangePercent))
			} else if product.ChangePercent <= -threshold {
				alerts = append(alerts,
					fmt.Sprintf("%s %s 费用下降 %.1f%%",
						provider.DisplayName, product.Name, -product.ChangePercent))
			}
		}
	}

	return alerts
}

// ConvertToWeChatFormat 转换为企业微信消息格式
func (ca *CostAnalyzer) ConvertToWeChatFormat(result *CostAnalysisResult) *wechat.CostComparisonData {
	wechatData := &wechat.CostComparisonData{
		Date:        time.Now().Format("2006-01-02"), // 报告日期应该是今天
		Alerts:      result.Alerts,
		GeneratedAt: result.GeneratedAt,
	}

	// 转换总费用
	if result.TotalCost != nil {
		wechatData.TotalCost = &wechat.CostChange{
			Name:          result.TotalCost.Name,
			YesterdayCost: result.TotalCost.YesterdayCost,
			TodayCost:     result.TotalCost.TodayCost,
			ChangeAmount:  result.TotalCost.ChangeAmount,
			ChangePercent: result.TotalCost.ChangePercent,
			Currency:      result.TotalCost.Currency,
		}
	}

	// 转换各服务商数据
	wechatData.Providers = make([]*wechat.ProviderCostData, len(result.Providers))
	for i, provider := range result.Providers {
		wechatProvider := &wechat.ProviderCostData{
			Provider:    provider.Provider,
			DisplayName: provider.DisplayName,
			Products:    make([]*wechat.CostChange, len(provider.Products)),
		}

		// 转换总费用
		if provider.TotalCost != nil {
			wechatProvider.TotalCost = &wechat.CostChange{
				Name:          provider.TotalCost.Name,
				YesterdayCost: provider.TotalCost.YesterdayCost,
				TodayCost:     provider.TotalCost.TodayCost,
				ChangeAmount:  provider.TotalCost.ChangeAmount,
				ChangePercent: provider.TotalCost.ChangePercent,
				Currency:      provider.TotalCost.Currency,
			}
		}

		// 转换各产品数据
		for j, product := range provider.Products {
			wechatProvider.Products[j] = &wechat.CostChange{
				Name:          product.Name,
				YesterdayCost: product.YesterdayCost,
				TodayCost:     product.TodayCost,
				ChangeAmount:  product.ChangeAmount,
				ChangePercent: product.ChangePercent,
				Currency:      product.Currency,
			}
		}

		wechatData.Providers[i] = wechatProvider
	}

	return wechatData
}

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

// sha256Hash 计算字符串的 SHA256 哈希值
func sha256Hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h)
}

// BatchQueryCostData 批量查询费用数据，支持并发查询多个表
func (ca *CostAnalyzer) BatchQueryCostData(ctx context.Context, tables []DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	// 使用工作池模式进行并发查询
	const maxConcurrency = 5 // 限制并发数避免过载
	semaphore := make(chan struct{}, maxConcurrency)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var allData []*RawCostData
	var errors []error

	for _, table := range tables {
		wg.Add(1)
		go func(tableInfo DatabaseTableInfo) {
			defer wg.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			data, err := ca.queryCostDataFromTable(ctx, tableInfo, dates)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errors = append(errors, fmt.Errorf("查询表 %s 失败: %w", tableInfo.TableName, err))
				slog.Warn("批量查询中表查询失败", "table", tableInfo.TableName, "error", err)
			} else {
				allData = append(allData, data...)
				slog.Debug("批量查询表完成", "table", tableInfo.TableName, "records", len(data))
			}
		}(table)
	}

	wg.Wait()

	// 如果有错误但也有数据，记录警告但继续
	if len(errors) > 0 {
		slog.Warn("批量查询完成，但有部分失败", "total_tables", len(tables), "errors", len(errors), "success_records", len(allData))
		for _, err := range errors {
			slog.Warn("批量查询错误详情", "error", err)
		}
	}

	slog.Info("批量查询完成", "tables", len(tables), "total_records", len(allData), "errors", len(errors))
	return allData, nil
}

// EnableDirectConnection 启用直接连接模式
func (ca *CostAnalyzer) EnableDirectConnection(cfg *config.ClickHouseConfig) error {
	if ca.directConn != nil {
		slog.Warn("直接连接已存在，将关闭旧连接")
		ca.directConn.Close()
	}

	ca.config = cfg
	ca.nameResolver = clickhouse.NewTableNameResolver(cfg)

	return ca.initDirectConnection()
}

// IsDirectConnectionEnabled 检查是否启用了直接连接
func (ca *CostAnalyzer) IsDirectConnectionEnabled() bool {
	return ca.directConn != nil
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
