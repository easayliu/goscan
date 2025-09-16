package analysis

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// dataAggregator 数据聚合器实现
type dataAggregator struct {
	chClient    *clickhouse.Client
	directConn  driver.Conn
	nameResolver *clickhouse.TableNameResolver
	queryTimeout time.Duration
}

// newDataAggregator 创建数据聚合器
func newDataAggregator(chClient *clickhouse.Client, directConn driver.Conn, nameResolver *clickhouse.TableNameResolver) *dataAggregator {
	return &dataAggregator{
		chClient:     chClient,
		directConn:   directConn,
		nameResolver: nameResolver,
		queryTimeout: 30 * time.Second,
	}
}

// GroupDataByProvider 按服务商分组数据
func (a *dataAggregator) GroupDataByProvider(rawData []*RawCostData) map[string][]*RawCostData {
	grouped := make(map[string][]*RawCostData)

	for _, item := range rawData {
		if item == nil || item.Provider == "" {
			continue
		}

		if _, exists := grouped[item.Provider]; !exists {
			grouped[item.Provider] = make([]*RawCostData, 0)
		}
		grouped[item.Provider] = append(grouped[item.Provider], item)
	}

	return grouped
}

// GetCostDataForDates 获取指定日期的费用数据，支持云服务商过滤
func (a *dataAggregator) GetCostDataForDates(ctx context.Context, dates []time.Time, providers []string) ([]*RawCostData, error) {
	if len(dates) == 0 {
		return nil, NewValidationError("dates", dates, "日期列表不能为空")
	}

	var allData []*RawCostData
	allTableInfo := GetProviderTableInfo()

	// 如果没有指定provider，使用所有的
	if len(providers) == 0 {
		providers = a.getAllProviders(allTableInfo)
	}

	// 根据指定的云服务商过滤表信息
	for tableKey, info := range allTableInfo {
		baseProvider := extractBaseProvider(tableKey)

		// 检查是否在指定的云服务商列表中
		if !contains(providers, baseProvider) {
			slog.Debug("跳过非指定云服务商的表", "table", info.TableName, "provider", baseProvider, "requested_providers", providers)
			continue
		}

		data, err := a.queryCostDataFromTable(ctx, info, dates)
		if err != nil {
			// 获取解析后的表名用于日志
			resolvedTableName := a.resolveTableName(info.TableName)
			slog.Warn("查询表数据失败", "original_table", info.TableName, "resolved_table", resolvedTableName, "provider", baseProvider, "error", err)
			continue // 继续查询其他表
		}
		slog.Info("成功查询表数据", "table", info.TableName, "provider", baseProvider, "records", len(data))
		allData = append(allData, data...)
	}

	return allData, nil
}

// BatchQueryCostData 批量查询费用数据，支持并发查询多个表
func (a *dataAggregator) BatchQueryCostData(ctx context.Context, tables []DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	if len(dates) == 0 {
		return nil, NewValidationError("dates", dates, "日期列表不能为空")
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

			data, err := a.queryCostDataFromTable(ctx, tableInfo, dates)

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

// queryCostDataFromTable 从单个表查询费用数据
func (a *dataAggregator) queryCostDataFromTable(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	// 验证表信息
	if err := a.validateTableInfo(info); err != nil {
		return nil, wrapError(err, "表信息验证失败")
	}

	// 优先使用直接连接
	if a.directConn != nil {
		return a.queryCostDataFromTableDirect(ctx, info, dates)
	}

	// 回退到原来的封装方法
	return a.queryCostDataFromTableLegacy(ctx, info, dates)
}

// queryCostDataFromTableDirect 使用直接连接查询费用数据
func (a *dataAggregator) queryCostDataFromTableDirect(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	startTime := time.Now()

	// 使用表名解析器解析目标查询表名
	resolvedTableName := a.resolveTableName(info.TableName)

	slog.Debug("直接查询表名解析", "original", info.TableName, "resolved", resolvedTableName)

	// 构建查询语句
	query, args := a.buildQuery(info, resolvedTableName, dates)

	// 创建带超时的上下文
	queryCtx, cancel := context.WithTimeout(ctx, a.queryTimeout)
	defer cancel()

	// 执行查询
	rows, err := a.directConn.Query(queryCtx, query, args...)
	if err != nil {
		return nil, NewQueryError(resolvedTableName, query, "直接查询执行失败", err)
	}
	defer rows.Close()

	data, err := a.scanQueryResults(rows)
	if err != nil {
		return nil, wrapError(err, "扫描查询结果失败")
	}

	duration := time.Since(startTime)
	slog.Info("直接查询表完成",
		"table", resolvedTableName,
		"provider", info.Provider,
		"records", len(data),
		"duration", duration)

	return data, nil
}

// queryCostDataFromTableLegacy 使用封装客户端查询费用数据（原实现）
func (a *dataAggregator) queryCostDataFromTableLegacy(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	if a.chClient == nil {
		return nil, wrapError(ErrConnectionNotInitialized, "ClickHouse客户端未初始化")
	}

	// 使用表名解析器解析目标查询表名
	resolvedTableName := a.resolveTableName(info.TableName)
	slog.Info("Legacy表名解析", 
		"original_table", info.TableName, 
		"resolved_table", resolvedTableName,
		"provider", info.Provider,
		"has_resolver", a.nameResolver != nil)

	// 构建查询语句（Legacy方式）
	query := a.buildLegacyQuery(info, resolvedTableName, dates)

	slog.Info("执行Legacy费用查询SQL", "query", query, "provider", info.Provider, "table", resolvedTableName)

	rows, err := a.chClient.Query(ctx, query)
	if err != nil {
		return nil, NewQueryError(resolvedTableName, query, "Legacy查询失败", err)
	}
	defer rows.Close()

	data, err := a.scanQueryResults(rows)
	if err != nil {
		return nil, wrapError(err, "扫描查询结果失败")
	}

	return data, nil
}

// buildQuery 构建参数化查询语句
func (a *dataAggregator) buildQuery(info DatabaseTableInfo, resolvedTableName string, dates []time.Time) (string, []interface{}) {
	// 构建参数化查询，避免SQL注入
	placeholders := make([]string, len(dates))
	args := make([]interface{}, len(dates))
	for i, date := range dates {
		placeholders[i] = "?"
		args[i] = date.Format("2006-01-02")
	}
	dateCondition := "(" + strings.Join(placeholders, ", ") + ")"

	// 对于火山引擎，需要特殊处理字段类型转换
	dateSelectExpr := info.DateColumn
	amountSumExpr := fmt.Sprintf("SUM(%s)", info.AmountColumn)
	amountWhereExpr := info.AmountColumn

	if info.Provider == "volcengine" {
		// 日期字段：字符串转Date类型
		dateSelectExpr = fmt.Sprintf("toDate(%s)", info.DateColumn)
		// 金额字段：字符串转Float64类型
		amountSumExpr = fmt.Sprintf("SUM(toFloat64OrZero(%s))", info.AmountColumn)
		amountWhereExpr = fmt.Sprintf("toFloat64OrZero(%s)", info.AmountColumn)
	}

	// 构建后付费过滤条件
	postpaidCondition := a.buildPostpaidCondition(info.Provider)
	
	// 构建优化的查询语句
	query := fmt.Sprintf(`
		SELECT 
			'%s' as provider,
			%s as product,
			%s as expense_date,
			%s as total_amount,
			%s as currency,
			COUNT(*) as record_count
		FROM %s
		WHERE %s IN %s
			AND %s > 0
			AND %s IS NOT NULL
			AND %s != ''
			%s
		GROUP BY provider, product, expense_date, currency
		ORDER BY provider, product, expense_date`,
		info.Provider,
		info.ProductColumn,
		dateSelectExpr,
		amountSumExpr,
		info.CurrencyColumn,
		resolvedTableName,
		info.DateColumn, // WHERE子句中保持原始字段
		dateCondition,
		amountWhereExpr,
		info.ProductColumn,
		info.ProductColumn,
		postpaidCondition)

	return query, args
}

// buildLegacyQuery 构建Legacy查询语句
func (a *dataAggregator) buildLegacyQuery(info DatabaseTableInfo, resolvedTableName string, dates []time.Time) string {
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

	// 对于火山引擎，需要特殊处理字段类型转换
	dateSelectExpr := info.DateColumn
	amountSumExpr := fmt.Sprintf("SUM(%s)", info.AmountColumn)
	amountWhereExpr := info.AmountColumn

	if info.Provider == "volcengine" {
		// 日期字段：字符串转Date类型
		dateSelectExpr = fmt.Sprintf("toDate(%s)", info.DateColumn)
		// 金额字段：字符串转Float64类型
		amountSumExpr = fmt.Sprintf("SUM(toFloat64OrZero(%s))", info.AmountColumn)
		amountWhereExpr = fmt.Sprintf("toFloat64OrZero(%s)", info.AmountColumn)
	}

	// 构建后付费过滤条件
	postpaidCondition := a.buildPostpaidCondition(info.Provider)
	
	query := fmt.Sprintf(`
		SELECT 
			'%s' as provider,
			%s as product,
			%s as expense_date,
			%s as total_amount,
			%s as currency,
			COUNT(*) as record_count
		FROM %s
		WHERE %s IN %s
			AND %s > 0
			%s
		GROUP BY provider, product, expense_date, currency
		ORDER BY provider, product, expense_date`,
		info.Provider,
		info.ProductColumn,
		dateSelectExpr,
		amountSumExpr,
		info.CurrencyColumn,
		resolvedTableName, // 使用解析后的表名
		info.DateColumn,   // WHERE子句中保持原始字段
		dateCondition,
		amountWhereExpr,
		postpaidCondition)

	return query
}

// buildPostpaidCondition 构建后付费过滤条件
func (a *dataAggregator) buildPostpaidCondition(provider string) string {
	switch strings.ToLower(provider) {
	case "volcengine":
		// 火山引擎：BillingMode = '按量计费'
		return "AND BillingMode = '按量计费'"
	case "alicloud":
		// 阿里云：subscription_type = 'PayAsYouGo' (注意数据库中字段名是下划线格式)
		return "AND subscription_type = 'PayAsYouGo'"
	default:
		// 其他云服务商暂时不过滤
		return ""
	}
}

// scanQueryResults 扫描查询结果
func (a *dataAggregator) scanQueryResults(rows driver.Rows) ([]*RawCostData, error) {
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
			slog.Warn("查询结果超过最大行数限制", "limit", 100000)
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, wrapError(err, "遍历查询结果失败")
	}

	return data, nil
}

// getAllProviders 获取所有云服务商列表
func (a *dataAggregator) getAllProviders(allTableInfo map[string]DatabaseTableInfo) []string {
	providers := make([]string, 0, len(allTableInfo))
	for provider := range allTableInfo {
		// 提取基础的云服务商标识（去掉_monthly、_daily等后缀）
		baseProvider := extractBaseProvider(provider)
		if !contains(providers, baseProvider) {
			providers = append(providers, baseProvider)
		}
	}
	return providers
}

// resolveTableName 解析表名
func (a *dataAggregator) resolveTableName(tableName string) string {
	if a.nameResolver != nil {
		return a.nameResolver.ResolveQueryTarget(tableName)
	}
	return tableName
}

// validateTableInfo 验证表信息
func (a *dataAggregator) validateTableInfo(info DatabaseTableInfo) error {
	if info.Provider == "" {
		return NewValidationError("provider", info.Provider, "服务商名称不能为空")
	}
	if info.TableName == "" {
		return NewValidationError("table_name", info.TableName, "表名不能为空")
	}
	if info.DateColumn == "" {
		return NewValidationError("date_column", info.DateColumn, "日期字段不能为空")
	}
	if info.AmountColumn == "" {
		return NewValidationError("amount_column", info.AmountColumn, "金额字段不能为空")
	}
	if info.ProductColumn == "" {
		return NewValidationError("product_column", info.ProductColumn, "产品字段不能为空")
	}
	if info.CurrencyColumn == "" {
		return NewValidationError("currency_column", info.CurrencyColumn, "货币字段不能为空")
	}
	return nil
}

// setQueryTimeout 设置查询超时时间
func (a *dataAggregator) setQueryTimeout(timeout time.Duration) {
	a.queryTimeout = timeout
}