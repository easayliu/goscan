package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"strings"
	"time"

	"go.uber.org/zap"
)

// TableInfo table information
type TableInfo struct {
	Name         string            `json:"name"`
	Database     string            `json:"database"`
	Engine       string            `json:"engine"`
	TotalRows    int64             `json:"total_rows"`
	TotalBytes   int64             `json:"total_bytes"`
	PartitionKey string            `json:"partition_key,omitempty"`
	OrderBy      string            `json:"order_by,omitempty"`
	PrimaryKey   string            `json:"primary_key,omitempty"`
	SamplingKey  string            `json:"sampling_key,omitempty"`
	CreateTime   time.Time         `json:"create_time"`
	LastModified time.Time         `json:"last_modified"`
	Columns      []ColumnInfo      `json:"columns"`
	Partitions   []PartitionInfo   `json:"partitions,omitempty"`
	Properties   map[string]string `json:"properties,omitempty"`
}

// ColumnInfo column information
type ColumnInfo struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	DefaultKind  string `json:"default_kind,omitempty"`
	DefaultValue string `json:"default_value,omitempty"`
	Comment      string `json:"comment,omitempty"`
	IsNullable   bool   `json:"is_nullable"`
}

// PartitionInfo partition information
type PartitionInfo struct {
	Partition string    `json:"partition"`
	Name      string    `json:"name"`
	Active    bool      `json:"active"`
	MinDate   time.Time `json:"min_date"`
	MaxDate   time.Time `json:"max_date"`
	Rows      int64     `json:"rows"`
	Size      int64     `json:"size"`
	Path      string    `json:"path"`
}

// CleanupOptions data cleanup options
type CleanupOptions struct {
	Condition     string        // Cleanup condition
	Args          []interface{} // Condition parameters
	DryRun        bool          // Whether to only preview without actual deletion
	BatchSize     int           // Size for batch deletion (for large datasets)
	MaxRows       int           // Maximum deletion row limit
	ConfirmDelete bool          // Whether confirmation is required for deletion
	ProgressLog   bool          // Whether to show deletion progress
}

// CleanupResult cleanup result
type CleanupResult struct {
	DeletedRows    int64         `json:"deleted_rows"`
	AffectedTables []string      `json:"affected_tables"`
	Duration       time.Duration `json:"duration"`
	DryRun         bool          `json:"dry_run"`
	PreviewRows    int64         `json:"preview_rows,omitempty"` // Only valid in DryRun mode
	Error          error         `json:"error,omitempty"`
}

// tableManager table manager
type tableManager struct {
	queryExecutor QueryExecutor
	nameResolver  *TableNameResolver
	config        *config.ClickHouseConfig
}

// NewTableManager creates table manager
func NewTableManager(queryExecutor QueryExecutor, nameResolver *TableNameResolver, config *config.ClickHouseConfig) TableManager {
	return &tableManager{
		queryExecutor: queryExecutor,
		nameResolver:  nameResolver,
		config:        config,
	}
}

// CreateTable creates a table
func (tm *tableManager) CreateTable(ctx context.Context, tableName string, schema string) error {
	// For CreateTable, we need to explicitly specify whether to create distributed or local table
	// By default, if cluster is configured, create distributed table
	resolvedTableName := tm.nameResolver.ResolveCreateTableTarget(tableName, true)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", resolvedTableName, schema)
	if err := tm.queryExecutor.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// DropTable drops a table
func (tm *tableManager) DropTable(ctx context.Context, tableName string) error {
	// Automatically resolve table name for delete operation
	resolvedTableName := tm.nameResolver.ResolveQueryTarget(tableName)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", resolvedTableName)
	if err := tm.queryExecutor.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// TruncateTable truncates a table
func (tm *tableManager) TruncateTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	if err := tm.queryExecutor.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// TableExists checks if table exists
func (tm *tableManager) TableExists(ctx context.Context, tableName string) (bool, error) {
	// Automatically resolve table name
	resolvedTableName := tm.nameResolver.ResolveQueryTarget(tableName)

	// If cluster is configured, use cluster query
	if tm.config.Cluster != "" {
		return tm.tableExistsInCluster(ctx, resolvedTableName)
	}

	// Single node query
	query := "SELECT 1 FROM system.tables WHERE database = ? AND name = ?"
	row := tm.queryExecutor.QueryRow(ctx, query, tm.config.Database, resolvedTableName)

	var exists uint8
	err := row.Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, WrapTableError(tableName, err)
	}

	return exists == 1, nil
}

// tableExistsInCluster checks if table exists in cluster
func (tm *tableManager) tableExistsInCluster(ctx context.Context, tableName string) (bool, error) {
	// Query table status on all nodes in cluster
	query := fmt.Sprintf(`
		SELECT 
			hostName() as host,
			COUNT(*) as table_count
		FROM clusterAllReplicas(%s, system.tables) 
		WHERE database = '%s' AND name = '%s'
		GROUP BY hostName()
		ORDER BY hostName()`,
		tm.config.Cluster, tm.config.Database, tableName)

	rows, err := tm.queryExecutor.Query(ctx, query)
	if err != nil {
		// If cluster query fails, fall back to simple query
		logger.Warn("Cluster table check: cluster query failed, falling back to simple query", zap.Error(err))
		return tm.simpleTableExists(ctx, tableName)
	}
	defer rows.Close()

	var nodeCount int
	var nodesWithTable int

	for rows.Next() {
		var host string
		var tableCount uint64

		if err := rows.Scan(&host, &tableCount); err != nil {
			return false, WrapTableError(tableName, fmt.Errorf("failed to scan cluster table result: %w", err))
		}

		nodeCount++
		if tableCount > 0 {
			nodesWithTable++
		}
	}

	if nodeCount == 0 {
		// No nodes responded, try simple query
		return tm.simpleTableExists(ctx, tableName)
	}

	// If all nodes have the table, consider the table exists
	// If partial nodes have the table, log warning but consider table exists (might be during creation)
	if nodesWithTable == nodeCount {
		return true, nil
	} else if nodesWithTable > 0 {
		logger.Warn("Cluster table check: table exists on partial nodes",
			zap.String("table", tableName),
			zap.Int("nodes_with_table", nodesWithTable),
			zap.Int("total_nodes", nodeCount))
		return true, nil
	}

	return false, nil
}

// simpleTableExists simple table existence check (fallback method)
func (tm *tableManager) simpleTableExists(ctx context.Context, tableName string) (bool, error) {
	query := "SELECT 1 FROM system.tables WHERE database = ? AND name = ?"
	row := tm.queryExecutor.QueryRow(ctx, query, tm.config.Database, tableName)

	var exists uint8
	err := row.Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, WrapTableError(tableName, err)
	}
	return exists == 1, nil
}

// GetTableInfo retrieves detailed table information
func (tm *tableManager) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	info := &TableInfo{
		Name:       tableName,
		Database:   tm.config.Database,
		Columns:    make([]ColumnInfo, 0),
		Properties: make(map[string]string),
	}

	// Get basic table information
	basicQuery := `
		SELECT 
			engine, 
			total_rows, 
			total_bytes,
			partition_key,
			sorting_key,
			primary_key,
			sampling_key,
			metadata_modification_time
		FROM system.tables 
		WHERE database = ? AND name = ?
	`

	row := tm.queryExecutor.QueryRow(ctx, basicQuery, tm.config.Database, tableName)
	var metadataTime string
	err := row.Scan(
		&info.Engine,
		&info.TotalRows,
		&info.TotalBytes,
		&info.PartitionKey,
		&info.OrderBy,
		&info.PrimaryKey,
		&info.SamplingKey,
		&metadataTime,
	)
	if err != nil {
		return nil, WrapTableError(tableName, fmt.Errorf("failed to get basic table info: %w", err))
	}

	// Parse time
	if t, err := time.Parse("2006-01-02 15:04:05", metadataTime); err == nil {
		info.LastModified = t
	}

	// Get column information
	if err := tm.loadTableColumns(ctx, info); err != nil {
		return nil, err
	}

	// Get partition information (if it's a partitioned table)
	if info.PartitionKey != "" {
		if err := tm.loadTablePartitions(ctx, info); err != nil {
			// Partition information retrieval failure doesn't affect overall result, only log
			logger.Error("Failed to get table partition info",
				zap.String("table", tableName),
				zap.Error(err))
		}
	}

	return info, nil
}

// loadTableColumns loads table column information
func (tm *tableManager) loadTableColumns(ctx context.Context, info *TableInfo) error {
	columnsQuery := `
		SELECT 
			name, 
			type, 
			default_kind, 
			default_expression,
			comment,
			is_in_primary_key
		FROM system.columns 
		WHERE database = ? AND table = ?
		ORDER BY position
	`

	rows, err := tm.queryExecutor.Query(ctx, columnsQuery, tm.config.Database, info.Name)
	if err != nil {
		return WrapTableError(info.Name, fmt.Errorf("failed to get column info: %w", err))
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnInfo
		var defaultExpr, comment string
		var isInPrimaryKey uint8

		err := rows.Scan(
			&col.Name,
			&col.Type,
			&col.DefaultKind,
			&defaultExpr,
			&comment,
			&isInPrimaryKey,
		)
		if err != nil {
			return WrapTableError(info.Name, fmt.Errorf("failed to scan column info: %w", err))
		}

		col.DefaultValue = defaultExpr
		col.Comment = comment
		col.IsNullable = !strings.Contains(col.Type, "Nullable")

		info.Columns = append(info.Columns, col)
	}

	return rows.Err()
}

// loadTablePartitions loads table partition information
func (tm *tableManager) loadTablePartitions(ctx context.Context, info *TableInfo) error {
	partitionsQuery := `
		SELECT 
			partition,
			name,
			active,
			min_date,
			max_date,
			rows,
			bytes_on_disk,
			path
		FROM system.parts 
		WHERE database = ? AND table = ?
		ORDER BY partition, name
	`

	partRows, err := tm.queryExecutor.Query(ctx, partitionsQuery, tm.config.Database, info.Name)
	if err != nil {
		return err
	}
	defer partRows.Close()

	for partRows.Next() {
		var part PartitionInfo
		var minDateStr, maxDateStr string
		var active uint8

		err := partRows.Scan(
			&part.Partition,
			&part.Name,
			&active,
			&minDateStr,
			&maxDateStr,
			&part.Rows,
			&part.Size,
			&part.Path,
		)
		if err != nil {
			return err
		}

		part.Active = active == 1
		if t, err := time.Parse("2006-01-02", minDateStr); err == nil {
			part.MinDate = t
		}
		if t, err := time.Parse("2006-01-02", maxDateStr); err == nil {
			part.MaxDate = t
		}
		info.Partitions = append(info.Partitions, part)
	}

	return partRows.Err()
}

// ListTables lists all tables in the database
func (tm *tableManager) ListTables(ctx context.Context) ([]string, error) {
	query := "SELECT name FROM system.tables WHERE database = ? ORDER BY name"
	rows, err := tm.queryExecutor.Query(ctx, query, tm.config.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

// DropTableOnCluster drops table on cluster
func (tm *tableManager) DropTableOnCluster(ctx context.Context, tableName string) error {
	if tm.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s ON CLUSTER %s", tableName, tm.config.Cluster)
	if err := tm.queryExecutor.Exec(ctx, dropQuery); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// DropTableSafely safely drops table (with confirmation)
func (tm *tableManager) DropTableSafely(ctx context.Context, tableName string, confirmation string) error {
	if confirmation != "YES" {
		return ErrInvalidConfirmation
	}

	// Check if table exists
	exists, err := tm.TableExists(ctx, tableName)
	if err != nil {
		return WrapTableError(tableName, fmt.Errorf("error checking if table exists: %w", err))
	}

	if !exists {
		logger.Debug("Table does not exist, skipping deletion", zap.String("table", tableName))
		return nil
	}

	// Get table information for logging
	info, err := tm.GetTableInfo(ctx, tableName)
	if err == nil {
		logger.Debug("About to delete table",
			zap.String("table", tableName),
			zap.Int64("total_rows", info.TotalRows),
			zap.String("size", FormatBytesHumanReadable(uint64(info.TotalBytes))))
	}

	return tm.DropTable(ctx, tableName)
}

// DeleteByCondition deletes data by condition
func (tm *tableManager) DeleteByCondition(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, condition)
	if err := tm.queryExecutor.Exec(ctx, query, args...); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// CleanTableData cleans table data
func (tm *tableManager) CleanTableData(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	if condition == "" {
		return tm.TruncateTable(ctx, tableName)
	}
	return tm.DeleteByCondition(ctx, tableName, condition, args...)
}

// String returns string representation of table information
func (ti *TableInfo) String() string {
	return fmt.Sprintf("Table{Name: %s, Engine: %s, Rows: %d, Size: %d bytes, Columns: %d}",
		ti.Name, ti.Engine, ti.TotalRows, ti.TotalBytes, len(ti.Columns))
}

// String returns string representation of cleanup result
func (r *CleanupResult) String() string {
	if r.DryRun {
		return fmt.Sprintf("CleanupResult{DryRun: true, PreviewRows: %d, Duration: %v, Tables: %v}",
			r.PreviewRows, r.Duration, r.AffectedTables)
	}

	status := "SUCCESS"
	if r.Error != nil {
		status = "FAILED"
	}

	return fmt.Sprintf("CleanupResult{Status: %s, DeletedRows: %d, Duration: %v, Tables: %v}",
		status, r.DeletedRows, r.Duration, r.AffectedTables)
}
