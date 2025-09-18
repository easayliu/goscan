package clickhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ConnectionManager defines connection management interface
type ConnectionManager interface {
	// Close closes the connection
	Close() error

	// Ping checks connection status
	Ping(ctx context.Context) error

	// GetConnection gets the underlying connection
	GetConnection() driver.Conn
}

// QueryExecutor defines query execution interface
type QueryExecutor interface {
	// Exec executes SQL statements
	Exec(ctx context.Context, query string, args ...interface{}) error

	// Query executes query and returns result set
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)

	// QueryRow executes query and returns single row result
	QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row
}

// BatchManager defines batch operation interface
type BatchManager interface {
	// PrepareBatch prepares batch operations
	PrepareBatch(ctx context.Context, query string) (driver.Batch, error)

	// InsertBatch inserts data in batches
	InsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error

	// AsyncInsertBatch inserts data asynchronously in batches
	AsyncInsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error

	// OptimizedBatchInsert optimized batch insert method
	OptimizedBatchInsert(ctx context.Context, tableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error)
}

// TableManager defines table management interface
type TableManager interface {
	// CreateTable creates a table
	CreateTable(ctx context.Context, tableName string, schema string) error

	// DropTable drops a table
	DropTable(ctx context.Context, tableName string) error

	// TruncateTable truncates a table
	TruncateTable(ctx context.Context, tableName string) error

	// TableExists checks if table exists
	TableExists(ctx context.Context, tableName string) (bool, error)

	// GetTableInfo retrieves table information
	GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)

	// ListTables lists all tables
	ListTables(ctx context.Context) ([]string, error)
}

// DistributedTableManager defines distributed table management interface
type DistributedTableManager interface {
	TableManager

	// CreateDistributedTable creates distributed table
	CreateDistributedTable(ctx context.Context, localTableName, distributedTableName string, schema string) error

	// DropDistributedTable drops distributed table
	DropDistributedTable(ctx context.Context, localTableName, distributedTableName string) error

	// IsDistributedTable checks if table is distributed
	IsDistributedTable(ctx context.Context, tableName string) (bool, error)

	// GetClusterInfo retrieves cluster information
	GetClusterInfo(ctx context.Context) ([]map[string]interface{}, error)
}

// DataCleaner defines data cleanup interface
type DataCleaner interface {
	// CleanTableData cleans table data
	CleanTableData(ctx context.Context, tableName string, condition string, args ...interface{}) error

	// EnhancedCleanTableData enhanced data cleanup
	EnhancedCleanTableData(ctx context.Context, tableName string, opts *CleanupOptions) (*CleanupResult, error)

	// CleanTableByDateRange cleans data by date range
	CleanTableByDateRange(ctx context.Context, tableName string, dateColumn string, startDate, endDate time.Time, dryRun bool) (*CleanupResult, error)

	// CleanOldData cleans old data
	CleanOldData(ctx context.Context, tableName string, dateColumn string, daysToKeep int, dryRun bool) (*CleanupResult, error)
}

// MetricsCollector defines metrics collection interface
type MetricsCollector interface {
	// GetTableSize retrieves table size information
	GetTableSize(ctx context.Context, tableName string) (map[string]interface{}, error)

	// CheckTableHealth checks table health status
	CheckTableHealth(ctx context.Context, tableName string) (map[string]interface{}, error)

	// GetTablesInfo retrieves basic information for all tables
	GetTablesInfo(ctx context.Context) ([]map[string]interface{}, error)
}

// TableMaintenanceManager defines table maintenance interface
type TableMaintenanceManager interface {
	// OptimizeTable optimizes table
	OptimizeTable(ctx context.Context, tableName string, final bool) error

	// AnalyzeTable analyzes table
	AnalyzeTable(ctx context.Context, tableName string) error

	// BackupTable backs up table
	BackupTable(ctx context.Context, sourceTable, backupTable string) error

	// RenameTable renames table
	RenameTable(ctx context.Context, oldName, newName string) error
}

// ClickHouseClient defines complete ClickHouse client interface
type ClickHouseClient interface {
	ConnectionManager
	QueryExecutor
	BatchManager
	TableManager
	DistributedTableManager
	DataCleaner
	MetricsCollector
	TableMaintenanceManager

	// Insert inserts single row data
	Insert(ctx context.Context, tableName string, data map[string]interface{}) error

	// GetClusterName returns cluster name
	GetClusterName() string

	// GetTableNameResolver returns table name resolver
	GetTableNameResolver() *TableNameResolver
}
