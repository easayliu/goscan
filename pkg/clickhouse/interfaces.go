package clickhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ConnectionManager 定义连接管理接口
type ConnectionManager interface {
	// Close 关闭连接
	Close() error
	
	// Ping 检查连接状态
	Ping(ctx context.Context) error
	
	// GetConnection 获取底层连接
	GetConnection() driver.Conn
}

// QueryExecutor 定义查询执行接口
type QueryExecutor interface {
	// Exec 执行SQL语句
	Exec(ctx context.Context, query string, args ...interface{}) error
	
	// Query 执行查询并返回结果集
	Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
	
	// QueryRow 执行查询并返回单行结果
	QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row
}

// BatchManager 定义批量操作接口
type BatchManager interface {
	// PrepareBatch 准备批量操作
	PrepareBatch(ctx context.Context, query string) (driver.Batch, error)
	
	// InsertBatch 批量插入数据
	InsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error
	
	// AsyncInsertBatch 异步批量插入数据
	AsyncInsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error
	
	// OptimizedBatchInsert 优化的批量插入
	OptimizedBatchInsert(ctx context.Context, tableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error)
}

// TableManager 定义表管理接口
type TableManager interface {
	// CreateTable 创建表
	CreateTable(ctx context.Context, tableName string, schema string) error
	
	// DropTable 删除表
	DropTable(ctx context.Context, tableName string) error
	
	// TruncateTable 清空表
	TruncateTable(ctx context.Context, tableName string) error
	
	// TableExists 检查表是否存在
	TableExists(ctx context.Context, tableName string) (bool, error)
	
	// GetTableInfo 获取表信息
	GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)
	
	// ListTables 列出所有表
	ListTables(ctx context.Context) ([]string, error)
}

// DistributedTableManager 定义分布式表管理接口
type DistributedTableManager interface {
	TableManager
	
	// CreateDistributedTable 创建分布式表
	CreateDistributedTable(ctx context.Context, localTableName, distributedTableName string, schema string) error
	
	// DropDistributedTable 删除分布式表
	DropDistributedTable(ctx context.Context, localTableName, distributedTableName string) error
	
	// IsDistributedTable 检查是否为分布式表
	IsDistributedTable(ctx context.Context, tableName string) (bool, error)
	
	// GetClusterInfo 获取集群信息
	GetClusterInfo(ctx context.Context) ([]map[string]interface{}, error)
}

// DataCleaner 定义数据清理接口
type DataCleaner interface {
	// CleanTableData 清理表数据
	CleanTableData(ctx context.Context, tableName string, condition string, args ...interface{}) error
	
	// EnhancedCleanTableData 增强的数据清理
	EnhancedCleanTableData(ctx context.Context, tableName string, opts *CleanupOptions) (*CleanupResult, error)
	
	// CleanTableByDateRange 按日期范围清理数据
	CleanTableByDateRange(ctx context.Context, tableName string, dateColumn string, startDate, endDate time.Time, dryRun bool) (*CleanupResult, error)
	
	// CleanOldData 清理旧数据
	CleanOldData(ctx context.Context, tableName string, dateColumn string, daysToKeep int, dryRun bool) (*CleanupResult, error)
}

// MetricsCollector 定义指标收集接口
type MetricsCollector interface {
	// GetTableSize 获取表大小信息
	GetTableSize(ctx context.Context, tableName string) (map[string]interface{}, error)
	
	// CheckTableHealth 检查表健康状态
	CheckTableHealth(ctx context.Context, tableName string) (map[string]interface{}, error)
	
	// GetTablesInfo 获取所有表的基本信息
	GetTablesInfo(ctx context.Context) ([]map[string]interface{}, error)
}

// TableMaintenanceManager 定义表维护接口
type TableMaintenanceManager interface {
	// OptimizeTable 优化表
	OptimizeTable(ctx context.Context, tableName string, final bool) error
	
	// AnalyzeTable 分析表
	AnalyzeTable(ctx context.Context, tableName string) error
	
	// BackupTable 备份表
	BackupTable(ctx context.Context, sourceTable, backupTable string) error
	
	// RenameTable 重命名表
	RenameTable(ctx context.Context, oldName, newName string) error
}

// ClickHouseClient 定义完整的ClickHouse客户端接口
type ClickHouseClient interface {
	ConnectionManager
	QueryExecutor
	BatchManager
	TableManager
	DistributedTableManager
	DataCleaner
	MetricsCollector
	TableMaintenanceManager
	
	// Insert 插入单行数据
	Insert(ctx context.Context, tableName string, data map[string]interface{}) error
	
	// GetClusterName 获取集群名称
	GetClusterName() string
	
	// GetTableNameResolver 获取表名解析器
	GetTableNameResolver() *TableNameResolver
}