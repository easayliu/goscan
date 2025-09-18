package clickhouse

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// Client ClickHouse client that implements all interfaces
type Client struct {
	// Composite of various managers
	connManager      ConnectionManager
	queryExecutor    QueryExecutor
	batchManager     BatchManager
	tableManager     TableManager
	metricsCollector MetricsCollector

	// Legacy fields for backward compatibility
	conn         driver.Conn
	config       *config.ClickHouseConfig
	nameResolver *TableNameResolver
}

// NewClient creates a ClickHouse client
func NewClient(cfg *config.ClickHouseConfig) (*Client, error) {
	// Create connection manager
	connManager, err := NewConnectionManager(cfg, nil)
	if err != nil {
		return nil, WrapConnectionError(err)
	}

	// Get underlying connection
	conn := connManager.GetConnection()

	// Create table name resolver
	nameResolver := NewTableNameResolver(cfg)

	// Create various managers
	queryExecutor := NewQueryExecutor(conn)
	batchManager := NewBatchManager(conn, nameResolver)
	tableManager := NewTableManager(queryExecutor, nameResolver, cfg)
	metricsCollector := NewMetricsCollector(queryExecutor, cfg)

	return &Client{
		connManager:      connManager,
		queryExecutor:    queryExecutor,
		batchManager:     batchManager,
		tableManager:     tableManager,
		metricsCollector: metricsCollector,
		// Maintain backward compatibility
		conn:         conn,
		config:       cfg,
		nameResolver: nameResolver,
	}, nil
}

// ============================================================================
// ConnectionManager interface implementation
// ============================================================================

// Close closes the connection
func (c *Client) Close() error {
	return c.connManager.Close()
}

// Ping checks connection status
func (c *Client) Ping(ctx context.Context) error {
	return c.connManager.Ping(ctx)
}

// GetConnection returns the underlying connection
func (c *Client) GetConnection() driver.Conn {
	return c.connManager.GetConnection()
}

// ============================================================================
// QueryExecutor interface implementation
// ============================================================================

// Exec executes SQL statements
func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) error {
	return c.queryExecutor.Exec(ctx, query, args...)
}

// Query executes query and returns result set
func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.queryExecutor.Query(ctx, query, args...)
}

// QueryRow executes query and returns single row result
func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return c.queryExecutor.QueryRow(ctx, query, args...)
}

// ============================================================================
// BatchManager interface implementation
// ============================================================================

// PrepareBatch prepares batch operations
func (c *Client) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return c.batchManager.PrepareBatch(ctx, query)
}

// InsertBatch inserts data in batches
func (c *Client) InsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	// Pre-validate table before batch insert
	if err := c.ValidateTableForInsert(ctx, tableName); err != nil {
		logger.Error("Table validation failed before batch insert",
			zap.String("table", tableName),
			zap.Error(err))
		// Don't return error immediately, let ClickHouse provide detailed error
		// return err
	}
	return c.batchManager.InsertBatch(ctx, tableName, data)
}

// AsyncInsertBatch inserts data asynchronously in batches
func (c *Client) AsyncInsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	return c.batchManager.AsyncInsertBatch(ctx, tableName, data)
}

// OptimizedBatchInsert optimized batch insert method
func (c *Client) OptimizedBatchInsert(ctx context.Context, tableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error) {
	return c.batchManager.OptimizedBatchInsert(ctx, tableName, data, opts)
}

// ============================================================================
// TableManager interface implementation
// ============================================================================

// CreateTable creates a table
func (c *Client) CreateTable(ctx context.Context, tableName string, schema string) error {
	return c.tableManager.CreateTable(ctx, tableName, schema)
}

// DropTable drops a table
func (c *Client) DropTable(ctx context.Context, tableName string) error {
	return c.tableManager.DropTable(ctx, tableName)
}

// TruncateTable truncates a table
func (c *Client) TruncateTable(ctx context.Context, tableName string) error {
	return c.tableManager.TruncateTable(ctx, tableName)
}

// TableExists checks if table exists
func (c *Client) TableExists(ctx context.Context, tableName string) (bool, error) {
	return c.tableManager.TableExists(ctx, tableName)
}

// GetTableInfo retrieves table information
func (c *Client) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	return c.tableManager.GetTableInfo(ctx, tableName)
}

// ListTables lists all tables
func (c *Client) ListTables(ctx context.Context) ([]string, error) {
	return c.tableManager.ListTables(ctx)
}

// ============================================================================
// MetricsCollector interface implementation
// ============================================================================

// GetTableSize retrieves table size information
func (c *Client) GetTableSize(ctx context.Context, tableName string) (map[string]interface{}, error) {
	return c.metricsCollector.GetTableSize(ctx, tableName)
}

// CheckTableHealth checks table health status
func (c *Client) CheckTableHealth(ctx context.Context, tableName string) (map[string]interface{}, error) {
	return c.metricsCollector.CheckTableHealth(ctx, tableName)
}

// GetTablesInfo retrieves basic information for all tables
func (c *Client) GetTablesInfo(ctx context.Context) ([]map[string]interface{}, error) {
	return c.metricsCollector.GetTablesInfo(ctx)
}

// ============================================================================
// Single row insert and other core methods
// ============================================================================

// Insert inserts single row data
func (c *Client) Insert(ctx context.Context, tableName string, data map[string]interface{}) error {
	// Automatically resolve table name
	resolvedTableName := c.nameResolver.ResolveInsertTarget(tableName)

	columns := ExtractColumnsFromData([]map[string]interface{}{data})
	values := PrepareValues(data, columns)

	query := BuildInsertQuery(resolvedTableName, columns)
	return c.queryExecutor.Exec(ctx, query, values...)
}

// ValidateTableForInsert validates that table exists and is ready for insert
func (c *Client) ValidateTableForInsert(ctx context.Context, tableName string) error {
	// Resolve the actual target table name
	targetTable := c.nameResolver.ResolveInsertTarget(tableName)

	// Check if table exists
	exists, err := c.TableExists(ctx, targetTable)
	if err != nil {
		return WrapTableError(targetTable, fmt.Errorf("failed to check table existence: %w", err))
	}

	if !exists {
		return WrapTableError(targetTable, fmt.Errorf("target table does not exist"))
	}

	logger.Debug("Table validation successful",
		zap.String("input_table", tableName),
		zap.String("target_table", targetTable))

	return nil
}

// ============================================================================
// Distributed table related methods
// ============================================================================

// CreateDistributedTable creates distributed table
func (c *Client) CreateDistributedTable(ctx context.Context, localTableName, distributedTableName string, schema string) error {
	if c.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	// Step 1: Use ON CLUSTER to create local table on all nodes
	logger.Info("Starting to create local table for distributed table",
		zap.String("cluster", c.config.Cluster),
		zap.String("local_table", localTableName),
		zap.String("distributed_table", distributedTableName))
	createLocalQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		localTableName, c.config.Cluster, schema)

	logger.Debug("Creating local table SQL", zap.String("sql", createLocalQuery))
	if err := c.Exec(ctx, createLocalQuery); err != nil {
		return WrapTableError(localTableName, fmt.Errorf("failed to create local table on cluster: %w", err))
	}

	// Step 2: Wait for cluster synchronization
	time.Sleep(3 * time.Second)

	// Step 3: Create distributed table with explicit column structure
	// Important: Use explicit column definition instead of "AS localTable" to avoid field mismatch
	distributedSchema := fmt.Sprintf("%s ENGINE = Distributed(%s, %s, %s, rand())",
		schema, c.config.Cluster, c.config.Database, localTableName)

	createDistributedQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		distributedTableName, c.config.Cluster, distributedSchema)

	logger.Debug("Creating distributed table SQL", zap.String("sql", createDistributedQuery))
	if err := c.Exec(ctx, createDistributedQuery); err != nil {
		return WrapTableError(distributedTableName, fmt.Errorf("failed to create distributed table: %w", err))
	}

	logger.Info("Successfully created distributed table structure",
		zap.String("local_table", localTableName),
		zap.String("distributed_table", distributedTableName),
		zap.String("cluster", c.config.Cluster))
	return nil
}

// DropDistributedTable drops distributed table
func (c *Client) DropDistributedTable(ctx context.Context, localTableName, distributedTableName string) error {
	if c.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	// First drop the distributed table
	if distributedTableName != "" {
		if err := c.DropTable(ctx, distributedTableName); err != nil {
			logger.Warn("Failed to drop distributed table",
				zap.String("table", distributedTableName),
				zap.Error(err))
		}
	}

	// Then drop the local table on all nodes
	if localTableName != "" {
		if err := c.DropTableOnCluster(ctx, localTableName); err != nil {
			return WrapTableError(localTableName, fmt.Errorf("failed to drop local table on cluster: %w", err))
		}
	}

	return nil
}

// DropTableOnCluster drops table on cluster
func (c *Client) DropTableOnCluster(ctx context.Context, tableName string) error {
	if c.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s ON CLUSTER %s", tableName, c.config.Cluster)
	return c.Exec(ctx, dropQuery)
}

// IsDistributedTable checks if table is distributed
func (c *Client) IsDistributedTable(ctx context.Context, tableName string) (bool, error) {
	query := "SELECT engine FROM system.tables WHERE database = ? AND name = ?"
	row := c.QueryRow(ctx, query, c.config.Database, tableName)

	var engine string
	err := row.Scan(&engine)
	if err != nil {
		return false, WrapTableError(tableName, err)
	}

	return engine == "Distributed", nil
}

// GetClusterInfo retrieves cluster information
func (c *Client) GetClusterInfo(ctx context.Context) ([]map[string]interface{}, error) {
	if c.config.Cluster == "" {
		return nil, ErrClusterNotConfigured
	}

	query := "SELECT host_name, port, is_local FROM system.clusters WHERE cluster = ?"
	rows, err := c.Query(ctx, query, c.config.Cluster)
	if err != nil {
		return nil, WrapError("get cluster info", "", err)
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var hostName string
		var port uint16
		var isLocal uint8

		if err := rows.Scan(&hostName, &port, &isLocal); err != nil {
			return nil, WrapError("scan cluster info", "", err)
		}

		result = append(result, map[string]interface{}{
			"host_name": hostName,
			"port":      port,
			"is_local":  isLocal == 1,
		})
	}

	return result, rows.Err()
}

// ============================================================================
// Data cleanup methods
// ============================================================================

// DeleteByCondition deletes data by condition
func (c *Client) DeleteByCondition(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	if tm, ok := c.tableManager.(*tableManager); ok {
		return tm.DeleteByCondition(ctx, tableName, condition, args...)
	}
	return fmt.Errorf("table manager not available")
}

// CleanTableData cleans table data
func (c *Client) CleanTableData(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	if tm, ok := c.tableManager.(*tableManager); ok {
		return tm.CleanTableData(ctx, tableName, condition, args...)
	}
	return fmt.Errorf("table manager not available")
}

// EnhancedCleanTableData enhanced data cleanup
func (c *Client) EnhancedCleanTableData(ctx context.Context, tableName string, opts *CleanupOptions) (*CleanupResult, error) {
	return c.enhancedCleanTableDataImpl(ctx, tableName, opts)
}

// enhancedCleanTableDataImpl internal implementation of enhanced data cleanup
func (c *Client) enhancedCleanTableDataImpl(ctx context.Context, tableName string, opts *CleanupOptions) (*CleanupResult, error) {
	if opts == nil {
		opts = &CleanupOptions{}
	}

	result := &CleanupResult{
		AffectedTables: []string{tableName},
		DryRun:         opts.DryRun,
	}

	startTime := time.Now()
	defer func() {
		result.Duration = time.Since(startTime)
	}()

	// Check if it's a distributed table
	isDistributed, err := c.IsDistributedTable(ctx, tableName)
	if err != nil {
		logger.Warn("Unable to check table type, treating as regular table", zap.Error(err))
		isDistributed = false
	}

	// If no condition, use TRUNCATE
	if opts.Condition == "" {
		if opts.DryRun {
			// Preview mode: count total rows in table
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
			row := c.QueryRow(ctx, countQuery)
			var count uint64
			if err := row.Scan(&count); err != nil {
				result.Error = WrapError("count rows", tableName, err)
				return result, result.Error
			}
			result.PreviewRows = int64(count)
			return result, nil
		}

		// Actually execute TRUNCATE
		if err := c.TruncateTable(ctx, tableName); err != nil {
			result.Error = err
			return result, err
		}
		return result, nil
	}

	// Conditional deletion - only use DROP PARTITION approach
	if opts.DryRun {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, opts.Condition)
		row := c.QueryRow(ctx, countQuery, opts.Args...)
		var count uint64
		if err := row.Scan(&count); err != nil {
			result.Error = WrapError("count conditional rows", tableName, err)
			return result, result.Error
		}
		result.PreviewRows = int64(count)
		return result, nil
	}

	// Extract partition information
	partition, canUsePartition := c.extractPartitionFromCondition(opts.Condition)
	if !canUsePartition {
		result.Error = ErrUnsupportedPartitionCondition
		return result, result.Error
	}

	// Check if partition exists
	partitionExists, checkErr := c.checkPartitionExists(ctx, tableName, partition)
	if checkErr != nil {
		logger.Warn("Unable to check partition existence, attempting direct deletion",
			zap.String("partition", partition),
			zap.Error(checkErr))
		partitionExists = true
	}

	if !partitionExists {
		logger.Debug("Partition does not exist, no cleanup needed", zap.String("partition", partition))
		result.DeletedRows = 0
		return result, nil
	}

	// Execute DROP PARTITION
	var dropQuery string
	if c.config.Cluster != "" && isDistributed {
		// Distributed table, get local table name
		localTableName := c.getLocalTableName(tableName)
		dropQuery = fmt.Sprintf("ALTER TABLE %s ON CLUSTER %s DROP PARTITION '%s'", localTableName, c.config.Cluster, partition)
	} else if c.config.Cluster != "" {
		dropQuery = fmt.Sprintf("ALTER TABLE %s ON CLUSTER %s DROP PARTITION '%s'", tableName, c.config.Cluster, partition)
	} else {
		dropQuery = fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tableName, partition)
	}

	if err := c.Exec(ctx, dropQuery); err != nil {
		result.Error = WrapPartitionError(partition, err)
		return result, result.Error
	}

	logger.Debug("Partition deleted", zap.String("partition", partition))
	return result, nil
}

// CleanTableByDateRange cleans data by date range
func (c *Client) CleanTableByDateRange(ctx context.Context, tableName string, dateColumn string, startDate, endDate time.Time, dryRun bool) (*CleanupResult, error) {
	condition := fmt.Sprintf("%s >= ? AND %s <= ?", dateColumn, dateColumn)
	args := []interface{}{startDate, endDate}

	opts := &CleanupOptions{
		Condition:   condition,
		Args:        args,
		DryRun:      dryRun,
		ProgressLog: true,
	}

	return c.EnhancedCleanTableData(ctx, tableName, opts)
}

// CleanOldData cleans old data
func (c *Client) CleanOldData(ctx context.Context, tableName string, dateColumn string, daysToKeep int, dryRun bool) (*CleanupResult, error) {
	cutoffDate := time.Now().AddDate(0, 0, -daysToKeep)
	condition := fmt.Sprintf("%s < ?", dateColumn)
	args := []interface{}{cutoffDate}

	opts := &CleanupOptions{
		Condition:   condition,
		Args:        args,
		DryRun:      dryRun,
		ProgressLog: true,
		BatchSize:   1000,
	}

	return c.EnhancedCleanTableData(ctx, tableName, opts)
}

// ============================================================================
// Table maintenance methods
// ============================================================================

// OptimizeTable optimizes table
func (c *Client) OptimizeTable(ctx context.Context, tableName string, final bool) error {
	var query string
	if final {
		query = fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
	} else {
		query = fmt.Sprintf("OPTIMIZE TABLE %s", tableName)
	}

	if err := c.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, fmt.Errorf("table optimization failed: %w", err))
	}

	logger.Debug("Table optimization completed", zap.String("table", tableName))
	return nil
}

// AnalyzeTable analyzes table
func (c *Client) AnalyzeTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("ANALYZE TABLE %s", tableName)
	if err := c.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, fmt.Errorf("table analysis failed: %w", err))
	}

	logger.Debug("Table analysis completed", zap.String("table", tableName))
	return nil
}

// BackupTable backs up table
func (c *Client) BackupTable(ctx context.Context, sourceTable, backupTable string) error {
	// Get source table structure
	query := "SHOW CREATE TABLE " + sourceTable
	row := c.QueryRow(ctx, query)

	var createStatement string
	err := row.Scan(&createStatement)
	if err != nil {
		return WrapTableError(sourceTable, fmt.Errorf("failed to get source table structure: %w", err))
	}

	// Modify table name to create backup table
	backupSchema := strings.Replace(createStatement, sourceTable, backupTable, 1)
	if err := c.Exec(ctx, backupSchema); err != nil {
		return WrapTableError(backupTable, fmt.Errorf("failed to create backup table: %w", err))
	}

	// Copy data
	copyQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", backupTable, sourceTable)
	if err := c.Exec(ctx, copyQuery); err != nil {
		// If copy fails, delete backup table
		c.DropTable(ctx, backupTable)
		return WrapTableError(sourceTable, fmt.Errorf("failed to copy data to backup table: %w", err))
	}

	logger.Debug("Table backup completed",
		zap.String("source_table", sourceTable),
		zap.String("backup_table", backupTable))
	return nil
}

// RenameTable renames table
func (c *Client) RenameTable(ctx context.Context, oldName, newName string) error {
	query := fmt.Sprintf("RENAME TABLE %s TO %s", oldName, newName)
	if err := c.Exec(ctx, query); err != nil {
		return WrapTableError(oldName, fmt.Errorf("table rename failed: %w", err))
	}

	logger.Debug("Table rename completed",
		zap.String("old_name", oldName),
		zap.String("new_name", newName))
	return nil
}

// ============================================================================
// Convenience methods and utility functions
// ============================================================================

// GetClusterName returns cluster name
func (c *Client) GetClusterName() string {
	return c.config.Cluster
}

// GetTableNameResolver returns table name resolver
func (c *Client) GetTableNameResolver() *TableNameResolver {
	return c.nameResolver
}

// extractPartitionFromCondition extracts partition information from cleanup condition
func (c *Client) extractPartitionFromCondition(condition string) (partition string, canUse bool) {
	logger.Debug("Partition detection: analyzing cleanup condition", zap.String("condition", condition))

	// Handle daily billing date conditions
	if matches := regexp.MustCompile(`billing_date\s*=\s*'(\d{4}-\d{2}-\d{2})'`).FindStringSubmatch(condition); len(matches) > 1 {
		date := matches[1]
		yearMonthDay := strings.ReplaceAll(date, "-", "")
		logger.Debug("Partition detection: extracted daily table partition", zap.String("partition", yearMonthDay))
		return yearMonthDay, true
	}

	// Handle monthly billing cycle conditions
	if matches := regexp.MustCompile(`billing_cycle\s*=\s*'(\d{4}-\d{2})'`).FindStringSubmatch(condition); len(matches) > 1 {
		cycle := matches[1]
		yearMonth := strings.Replace(cycle, "-", "", 1)
		logger.Debug("Partition detection: extracted partition", zap.String("partition", yearMonth))
		return yearMonth, true
	}

	// Handle Volcengine related conditions
	if matches := regexp.MustCompile(`ExpenseDate\s+LIKE\s+'(\d{4}-\d{2})%'`).FindStringSubmatch(condition); len(matches) > 1 {
		cycle := matches[1]
		yearMonth := strings.Replace(cycle, "-", "", 1)
		return yearMonth, true
	}

	// Handle Volcengine BillPeriod conditions (format: YYYY-MM)
	if matches := regexp.MustCompile(`BillPeriod\s*=\s*'(\d{4}-\d{2})'`).FindStringSubmatch(condition); len(matches) > 1 {
		cycle := matches[1]
		yearMonth := strings.Replace(cycle, "-", "", 1)
		logger.Debug("Partition detection: extracted BillPeriod partition", zap.String("partition", yearMonth))
		return yearMonth, true
	}

	return "", false
}

// checkPartitionExists checks if partition exists
func (c *Client) checkPartitionExists(ctx context.Context, tableName, partition string) (bool, error) {
	query := `
		SELECT COUNT(*) 
		FROM system.parts 
		WHERE database = ? AND table = ? AND partition = ? AND active = 1
	`

	row := c.QueryRow(ctx, query, c.config.Database, tableName, partition)
	var count uint64
	err := row.Scan(&count)
	if err != nil {
		return false, WrapPartitionError(partition, err)
	}

	return count > 0, nil
}

// getLocalTableName gets local table name corresponding to distributed table
func (c *Client) getLocalTableName(distributedTableName string) string {
	if strings.Contains(distributedTableName, "_distributed") {
		return strings.Replace(distributedTableName, "_distributed", "_local", 1)
	}
	return distributedTableName + "_local"
}

// ============================================================================
// Compatibility methods (maintain backward compatibility)
// ============================================================================

// InsertToDistributed distributed table insert (compatibility method)
func (c *Client) InsertToDistributed(ctx context.Context, distributedTableName string, data map[string]interface{}) error {
	return c.Insert(ctx, distributedTableName, data)
}

// InsertBatchToDistributed distributed table batch insert (compatibility method)
func (c *Client) InsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return c.InsertBatch(ctx, distributedTableName, data)
}

// AsyncInsertBatchToDistributed asynchronous distributed table batch insert (compatibility method)
func (c *Client) AsyncInsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return c.AsyncInsertBatch(ctx, distributedTableName, data)
}

// OptimizedBatchInsertToDistributed optimized distributed table batch insert (compatibility method)
func (c *Client) OptimizedBatchInsertToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error) {
	return c.OptimizedBatchInsert(ctx, distributedTableName, data, opts)
}

// CreateDistributedTableWithResolver creates complete distributed table structure (compatibility method)
func (c *Client) CreateDistributedTableWithResolver(ctx context.Context, baseTableName string, localSchema string) error {
	if c.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	distributedTableName, localTableName := c.nameResolver.GetTablePair(baseTableName)

	logger.Info("Creating distributed table structure with resolver",
		zap.String("base_table", baseTableName),
		zap.String("local_table", localTableName),
		zap.String("distributed_table", distributedTableName),
		zap.String("cluster", c.config.Cluster))

	// First create local table
	if err := c.CreateLocalTable(ctx, baseTableName, localSchema); err != nil {
		return WrapTableError(localTableName, fmt.Errorf("failed to create local table: %w", err))
	}

	// Wait for cluster synchronization
	time.Sleep(2 * time.Second)

	// Create distributed table with explicit schema to avoid field mismatch
	// Important: Extract only the column definitions from local schema
	// Remove ENGINE and everything after it, then add Distributed ENGINE
	engineIndex := strings.Index(localSchema, "ENGINE")
	if engineIndex == -1 {
		return WrapTableError(distributedTableName, fmt.Errorf("invalid schema: no ENGINE clause found"))
	}

	// Extract only the column definitions part (before ENGINE)
	columnDefinitions := strings.TrimSpace(localSchema[:engineIndex])
	distributedSchema := fmt.Sprintf("%s ENGINE = Distributed(%s, %s, %s, rand())",
		columnDefinitions, c.config.Cluster, c.config.Database, localTableName)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		distributedTableName, c.config.Cluster, distributedSchema)

	logger.Debug("Creating distributed table SQL", zap.String("sql", query))
	if err := c.Exec(ctx, query); err != nil {
		return WrapTableError(distributedTableName, fmt.Errorf("failed to create distributed table: %w", err))
	}

	logger.Info("Automatic table name resolution: successfully created distributed table structure",
		zap.String("local_table", localTableName),
		zap.String("distributed_table", distributedTableName))

	return nil
}

// CreateLocalTable explicitly creates local table (compatibility method)
func (c *Client) CreateLocalTable(ctx context.Context, baseTableName string, schema string) error {
	localTableName := c.nameResolver.ResolveLocalTableName(baseTableName)

	if c.nameResolver.IsClusterEnabled() {
		// Create local table on cluster
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
			localTableName, c.config.Cluster, schema)
		return c.Exec(ctx, query)
	} else {
		// Single node mode
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", localTableName, schema)
		return c.Exec(ctx, query)
	}
}
