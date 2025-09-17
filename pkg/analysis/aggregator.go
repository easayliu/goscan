package analysis

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/logger"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// dataAggregator is the data aggregator implementation
type dataAggregator struct {
	chClient     *clickhouse.Client
	directConn   driver.Conn
	nameResolver *clickhouse.TableNameResolver
	queryTimeout time.Duration
}

// newDataAggregator creates a data aggregator
func newDataAggregator(chClient *clickhouse.Client, directConn driver.Conn, nameResolver *clickhouse.TableNameResolver) *dataAggregator {
	return &dataAggregator{
		chClient:     chClient,
		directConn:   directConn,
		nameResolver: nameResolver,
		queryTimeout: 30 * time.Second,
	}
}

// GroupDataByProvider groups data by service provider
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

// GetCostDataForDates retrieves cost data for specified dates with cloud provider filtering support
func (a *dataAggregator) GetCostDataForDates(ctx context.Context, dates []time.Time, providers []string) ([]*RawCostData, error) {
	if len(dates) == 0 {
		return nil, NewValidationError("dates", dates, "date list cannot be empty")
	}

	var allData []*RawCostData
	allTableInfo := GetProviderTableInfo()

	// if no provider specified, use all providers
	if len(providers) == 0 {
		providers = a.getAllProviders(allTableInfo)
	}

	// filter table information based on specified cloud providers
	for tableKey, info := range allTableInfo {
		baseProvider := extractBaseProvider(tableKey)

		// check if in the specified cloud provider list
		if !contains(providers, baseProvider) {
			logger.Debug("Skipping table for non-specified cloud provider",
				zap.String("table", info.TableName),
				zap.String("provider", baseProvider),
				zap.Strings("requested_providers", providers))
			continue
		}

		data, err := a.queryCostDataFromTable(ctx, info, dates)
		if err != nil {
			// Resolve normalized table name for logging
			resolvedTableName := a.resolveTableName(info.TableName)
			logger.Error("Failed to query table data - this will cause missing provider data",
				zap.String("original_table", info.TableName),
				zap.String("resolved_table", resolvedTableName),
				zap.String("provider", baseProvider),
				zap.Strings("requested_dates", func() []string {
					var dateStrs []string
					for _, d := range dates {
						dateStrs = append(dateStrs, d.Format("2006-01-02"))
					}
					return dateStrs
				}()),
				zap.Error(err))
			continue // continue querying other tables
		}
		logger.Debug("Successfully queried table data",
			zap.String("table", info.TableName),
			zap.String("provider", baseProvider),
			zap.Int("records", len(data)),
			zap.Int("dates_count", len(dates)))
		allData = append(allData, data...)
	}

	return allData, nil
}

// BatchQueryCostData batch queries cost data with concurrent querying of multiple tables
func (a *dataAggregator) BatchQueryCostData(ctx context.Context, tables []DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	if len(tables) == 0 {
		return nil, nil
	}

	if len(dates) == 0 {
		return nil, NewValidationError("dates", dates, "date list cannot be empty")
	}

	// use worker pool pattern for concurrent querying
	const maxConcurrency = 5 // limit concurrency to avoid overload
	semaphore := make(chan struct{}, maxConcurrency)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var allData []*RawCostData
	var errors []error

	for _, table := range tables {
		wg.Add(1)
		go func(tableInfo DatabaseTableInfo) {
			defer wg.Done()

			// acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			data, err := a.queryCostDataFromTable(ctx, tableInfo, dates)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errors = append(errors, fmt.Errorf("Failed to query table %s: %w", tableInfo.TableName, err))
				logger.Warn("Table query failed in batch query", zap.String("table", tableInfo.TableName), zap.Error(err))
			} else {
				allData = append(allData, data...)
				logger.Debug("Batch query table completed", zap.String("table", tableInfo.TableName), zap.Int("records", len(data)))
			}
		}(table)
	}

	wg.Wait()

	// if there are errors but also data, log warning but continue
	if len(errors) > 0 {
		logger.Warn("Batch query completed with some failures",
			zap.Int("total_tables", len(tables)),
			zap.Int("errors", len(errors)),
			zap.Int("success_records", len(allData)))
		for _, err := range errors {
			logger.Warn("Batch query error details", zap.Error(err))
		}
	}

	logger.Info("Batch query completed",
		zap.Int("tables", len(tables)),
		zap.Int("total_records", len(allData)),
		zap.Int("errors", len(errors)))
	return allData, nil
}

// queryCostDataFromTable queries cost data from a single table
func (a *dataAggregator) queryCostDataFromTable(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	// validate table information
	if err := a.validateTableInfo(info); err != nil {
		return nil, wrapError(err, "table information validation failed")
	}

	// prefer using direct connection
	if a.directConn != nil {
		return a.queryCostDataFromTableDirect(ctx, info, dates)
	}

	// fall back to the original wrapped method
	return a.queryCostDataFromTableLegacy(ctx, info, dates)
}

// queryCostDataFromTableDirect queries cost data using direct connection
func (a *dataAggregator) queryCostDataFromTableDirect(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	startTime := time.Now()

	// use table name resolver to resolve target query table name
	resolvedTableName := a.resolveTableName(info.TableName)

	logger.Debug("Direct query table name resolution",
		zap.String("original_table", info.TableName),
		zap.String("resolved_table", resolvedTableName),
		zap.String("provider", info.Provider))

	// build query statement
	query, args := a.buildQuery(info, resolvedTableName, dates)


	// create context with timeout
	queryCtx, cancel := context.WithTimeout(ctx, a.queryTimeout)
	defer cancel()

	// execute query
	rows, err := a.directConn.Query(queryCtx, query, args...)
	if err != nil {
		return nil, NewQueryError(resolvedTableName, query, "direct query execution failed", err)
	}
	defer rows.Close()

	data, err := a.scanQueryResults(rows)
	if err != nil {
		return nil, wrapError(err, "scan query results failed")
	}

	duration := time.Since(startTime)
	logger.Debug("Direct table query completed",
		zap.String("table", resolvedTableName),
		zap.String("provider", info.Provider),
		zap.Int("records", len(data)),
		zap.Duration("duration", duration),
		zap.Int("dates_count", len(dates)))


	return data, nil
}

// queryCostDataFromTableLegacy queries cost data using wrapped client (original implementation)
func (a *dataAggregator) queryCostDataFromTableLegacy(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error) {
	if a.chClient == nil {
		return nil, wrapError(ErrConnectionNotInitialized, "ClickHouse client not initialized")
	}

	// use table name resolver to resolve target query table name
	resolvedTableName := a.resolveTableName(info.TableName)
	logger.Debug("Legacy table name resolution",
		zap.String("original_table", info.TableName),
		zap.String("resolved_table", resolvedTableName),
		zap.String("provider", info.Provider),
		zap.Bool("has_resolver", a.nameResolver != nil))

	// build query statement (Legacy method)
	query := a.buildLegacyQuery(info, resolvedTableName, dates)

	logger.Debug("Executing legacy cost query SQL",
		zap.String("provider", info.Provider),
		zap.String("table", resolvedTableName),
		zap.Int("query_length", len(query)))

	rows, err := a.chClient.Query(ctx, query)
	if err != nil {
		return nil, NewQueryError(resolvedTableName, query, "Legacy query failed", err)
	}
	defer rows.Close()

	data, err := a.scanQueryResults(rows)
	if err != nil {
		return nil, wrapError(err, "scan query results failed")
	}

	return data, nil
}

// buildQuery builds parameterized query statement
func (a *dataAggregator) buildQuery(info DatabaseTableInfo, resolvedTableName string, dates []time.Time) (string, []interface{}) {
	// build parameterized query to prevent SQL injection
	placeholders := make([]string, len(dates))
	args := make([]interface{}, len(dates))
	for i, date := range dates {
		placeholders[i] = "?"
		args[i] = date.Format("2006-01-02")
	}
	dateCondition := "(" + strings.Join(placeholders, ", ") + ")"

	// for VolcEngine, special handling of field type conversion is needed
	dateSelectExpr := info.DateColumn
	amountSumExpr := fmt.Sprintf("SUM(%s)", info.AmountColumn)
	amountWhereExpr := info.AmountColumn
	dateWhereExpr := info.DateColumn

	if info.Provider == "volcengine" {
		// date field: string to Date type (for both SELECT and WHERE)
		dateSelectExpr = fmt.Sprintf("toDate(%s)", info.DateColumn)
		dateWhereExpr = fmt.Sprintf("toDate(%s)", info.DateColumn)
		// amount field: string to Float64 type
		amountSumExpr = fmt.Sprintf("SUM(toFloat64OrZero(%s))", info.AmountColumn)
		amountWhereExpr = fmt.Sprintf("toFloat64OrZero(%s)", info.AmountColumn)
	}

	// build postpaid filter condition
	postpaidCondition := a.buildPostpaidCondition(info.Provider)

	// build optimized query statement
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
		dateWhereExpr,   // use processed date field in WHERE clause
		dateCondition,
		amountWhereExpr,
		info.ProductColumn,
		info.ProductColumn,
		postpaidCondition)

	return query, args
}

// buildLegacyQuery builds Legacy query statement
func (a *dataAggregator) buildLegacyQuery(info DatabaseTableInfo, resolvedTableName string, dates []time.Time) string {
	// build date condition
	dateStrs := make([]string, len(dates))
	for i, date := range dates {
		dateStrs[i] = "'" + date.Format("2006-01-02") + "'"
	}
	dateCondition := "(" + fmt.Sprintf("%s", dateStrs[0])
	for i := 1; i < len(dateStrs); i++ {
		dateCondition += ", " + dateStrs[i]
	}
	dateCondition += ")"

	// for VolcEngine, special handling of field type conversion is needed
	dateSelectExpr := info.DateColumn
	amountSumExpr := fmt.Sprintf("SUM(%s)", info.AmountColumn)
	amountWhereExpr := info.AmountColumn
	dateWhereExpr := info.DateColumn

	if info.Provider == "volcengine" {
		// date field: string to Date type (for both SELECT and WHERE)
		dateSelectExpr = fmt.Sprintf("toDate(%s)", info.DateColumn)
		dateWhereExpr = fmt.Sprintf("toDate(%s)", info.DateColumn)
		// amount field: string to Float64 type
		amountSumExpr = fmt.Sprintf("SUM(toFloat64OrZero(%s))", info.AmountColumn)
		amountWhereExpr = fmt.Sprintf("toFloat64OrZero(%s)", info.AmountColumn)
	}

	// build postpaid filter condition
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
		resolvedTableName, // use resolved table name
		dateWhereExpr,     // use processed date field in WHERE clause
		dateCondition,
		amountWhereExpr,
		postpaidCondition)

	return query
}

// buildPostpaidCondition builds postpaid filter condition
func (a *dataAggregator) buildPostpaidCondition(provider string) string {
	switch strings.ToLower(provider) {
	case "volcengine":
		// Volcengine: BillingMode = '按量计费'
		return "AND BillingMode = '按量计费'"
	case "alicloud":
		// Alibaba Cloud: subscription_type = 'PayAsYouGo' (note database field name uses underscore format)
		return "AND subscription_type = 'PayAsYouGo'"
	default:
		// other cloud providers not filtered for now
		return ""
	}
}

// scanQueryResults scans query results
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
			logger.Warn("Failed to scan row data", zap.Error(err), zap.Int("row", rowCount))
			continue
		}
		data = append(data, &item)
		rowCount++

		// prevent memory overflow, limit maximum row count
		if rowCount > 100000 {
			logger.Warn("Query result exceeds maximum row limit", zap.Int("limit", 100000))
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, wrapError(err, "failed to iterate query results")
	}

	return data, nil
}

// getAllProviders gets list of all cloud providers
func (a *dataAggregator) getAllProviders(allTableInfo map[string]DatabaseTableInfo) []string {
	providers := make([]string, 0, len(allTableInfo))
	for provider := range allTableInfo {
		// extract base cloud provider identifier (remove suffixes like _monthly, _daily)
		baseProvider := extractBaseProvider(provider)
		if !contains(providers, baseProvider) {
			providers = append(providers, baseProvider)
		}
	}
	return providers
}

// resolveTableName resolves table name
func (a *dataAggregator) resolveTableName(tableName string) string {
	if a.nameResolver != nil {
		return a.nameResolver.ResolveQueryTarget(tableName)
	}
	return tableName
}

// validateTableInfo validates table information
func (a *dataAggregator) validateTableInfo(info DatabaseTableInfo) error {
	if info.Provider == "" {
		return NewValidationError("provider", info.Provider, "provider name cannot be empty")
	}
	if info.TableName == "" {
		return NewValidationError("table_name", info.TableName, "table name cannot be empty")
	}
	if info.DateColumn == "" {
		return NewValidationError("date_column", info.DateColumn, "date column cannot be empty")
	}
	if info.AmountColumn == "" {
		return NewValidationError("amount_column", info.AmountColumn, "amount column cannot be empty")
	}
	if info.ProductColumn == "" {
		return NewValidationError("product_column", info.ProductColumn, "product column cannot be empty")
	}
	if info.CurrencyColumn == "" {
		return NewValidationError("currency_column", info.CurrencyColumn, "currency column cannot be empty")
	}
	return nil
}

// setQueryTimeout sets query timeout
func (a *dataAggregator) setQueryTimeout(timeout time.Duration) {
	a.queryTimeout = timeout
}
