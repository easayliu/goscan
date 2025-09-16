package clickhouse

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client ClickHouse客户端，实现所有接口
type Client struct {
	// 组合各种管理器
	connManager      ConnectionManager
	queryExecutor    QueryExecutor
	batchManager     BatchManager
	tableManager     TableManager
	metricsCollector MetricsCollector
	
	// 原有字段保持向后兼容
	conn         driver.Conn
	config       *config.ClickHouseConfig
	nameResolver *TableNameResolver
}

// NewClient 创建ClickHouse客户端
func NewClient(cfg *config.ClickHouseConfig) (*Client, error) {
	// 创建连接管理器
	connManager, err := NewConnectionManager(cfg, nil)
	if err != nil {
		return nil, WrapConnectionError(err)
	}

	// 获取底层连接
	conn := connManager.GetConnection()
	
	// 创建表名解析器
	nameResolver := NewTableNameResolver(cfg)
	
	// 创建各种管理器
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
		// 保持向后兼容
		conn:         conn,
		config:       cfg,
		nameResolver: nameResolver,
	}, nil
}

// ============================================================================
// ConnectionManager 接口实现
// ============================================================================

// Close 关闭连接
func (c *Client) Close() error {
	return c.connManager.Close()
}

// Ping 检查连接状态
func (c *Client) Ping(ctx context.Context) error {
	return c.connManager.Ping(ctx)
}

// GetConnection 获取底层连接
func (c *Client) GetConnection() driver.Conn {
	return c.connManager.GetConnection()
}

// ============================================================================
// QueryExecutor 接口实现
// ============================================================================

// Exec 执行SQL语句
func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) error {
	return c.queryExecutor.Exec(ctx, query, args...)
}

// Query 执行查询并返回结果集
func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.queryExecutor.Query(ctx, query, args...)
}

// QueryRow 执行查询并返回单行结果
func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return c.queryExecutor.QueryRow(ctx, query, args...)
}

// ============================================================================
// BatchManager 接口实现
// ============================================================================

// PrepareBatch 准备批量操作
func (c *Client) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return c.batchManager.PrepareBatch(ctx, query)
}

// InsertBatch 批量插入数据
func (c *Client) InsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	return c.batchManager.InsertBatch(ctx, tableName, data)
}

// AsyncInsertBatch 异步批量插入数据
func (c *Client) AsyncInsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	return c.batchManager.AsyncInsertBatch(ctx, tableName, data)
}

// OptimizedBatchInsert 优化的批量插入方法
func (c *Client) OptimizedBatchInsert(ctx context.Context, tableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error) {
	return c.batchManager.OptimizedBatchInsert(ctx, tableName, data, opts)
}

// ============================================================================
// TableManager 接口实现
// ============================================================================

// CreateTable 创建表
func (c *Client) CreateTable(ctx context.Context, tableName string, schema string) error {
	return c.tableManager.CreateTable(ctx, tableName, schema)
}

// DropTable 删除表
func (c *Client) DropTable(ctx context.Context, tableName string) error {
	return c.tableManager.DropTable(ctx, tableName)
}

// TruncateTable 清空表
func (c *Client) TruncateTable(ctx context.Context, tableName string) error {
	return c.tableManager.TruncateTable(ctx, tableName)
}

// TableExists 检查表是否存在
func (c *Client) TableExists(ctx context.Context, tableName string) (bool, error) {
	return c.tableManager.TableExists(ctx, tableName)
}

// GetTableInfo 获取表信息
func (c *Client) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	return c.tableManager.GetTableInfo(ctx, tableName)
}

// ListTables 列出所有表
func (c *Client) ListTables(ctx context.Context) ([]string, error) {
	return c.tableManager.ListTables(ctx)
}

// ============================================================================
// MetricsCollector 接口实现
// ============================================================================

// GetTableSize 获取表大小信息
func (c *Client) GetTableSize(ctx context.Context, tableName string) (map[string]interface{}, error) {
	return c.metricsCollector.GetTableSize(ctx, tableName)
}

// CheckTableHealth 检查表健康状态
func (c *Client) CheckTableHealth(ctx context.Context, tableName string) (map[string]interface{}, error) {
	return c.metricsCollector.CheckTableHealth(ctx, tableName)
}

// GetTablesInfo 获取所有表的基本信息
func (c *Client) GetTablesInfo(ctx context.Context) ([]map[string]interface{}, error) {
	return c.metricsCollector.GetTablesInfo(ctx)
}

// ============================================================================
// 单行插入和其他核心方法
// ============================================================================

// Insert 插入单行数据
func (c *Client) Insert(ctx context.Context, tableName string, data map[string]interface{}) error {
	// 自动解析表名
	resolvedTableName := c.nameResolver.ResolveInsertTarget(tableName)

	columns := ExtractColumnsFromData([]map[string]interface{}{data})
	values := PrepareValues(data, columns)

	query := BuildInsertQuery(resolvedTableName, columns)
	return c.queryExecutor.Exec(ctx, query, values...)
}

// ============================================================================
// 分布式表相关方法
// ============================================================================

// CreateDistributedTable 创建分布式表
func (c *Client) CreateDistributedTable(ctx context.Context, localTableName, distributedTableName string, schema string) error {
	if c.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	// 步骤1: 使用ON CLUSTER在所有节点上创建本地表
	log.Printf("[分布式表] 开始在集群 %s 上创建本地表 %s", c.config.Cluster, localTableName)
	createLocalQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		localTableName, c.config.Cluster, schema)

	if err := c.Exec(ctx, createLocalQuery); err != nil {
		return WrapTableError(localTableName, fmt.Errorf("failed to create local table on cluster: %w", err))
	}

	// 步骤2: 等待集群同步
	time.Sleep(3 * time.Second)

	// 步骤3: 创建分布式表
	distributedSchema := fmt.Sprintf(`AS %s ENGINE = Distributed(%s, %s, %s, rand())`,
		localTableName, c.config.Cluster, c.config.Database, localTableName)

	createDistributedQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		distributedTableName, c.config.Cluster, distributedSchema)

	if err := c.Exec(ctx, createDistributedQuery); err != nil {
		return WrapTableError(distributedTableName, fmt.Errorf("failed to create distributed table: %w", err))
	}

	return nil
}

// DropDistributedTable 删除分布式表
func (c *Client) DropDistributedTable(ctx context.Context, localTableName, distributedTableName string) error {
	if c.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	// 先删除分布式表
	if distributedTableName != "" {
		if err := c.DropTable(ctx, distributedTableName); err != nil {
			log.Printf("警告: 删除分布式表 %s 失败: %v", distributedTableName, err)
		}
	}

	// 然后删除所有节点上的本地表
	if localTableName != "" {
		if err := c.DropTableOnCluster(ctx, localTableName); err != nil {
			return WrapTableError(localTableName, fmt.Errorf("failed to drop local table on cluster: %w", err))
		}
	}

	return nil
}

// DropTableOnCluster 在集群上删除表
func (c *Client) DropTableOnCluster(ctx context.Context, tableName string) error {
	if c.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s ON CLUSTER %s", tableName, c.config.Cluster)
	return c.Exec(ctx, dropQuery)
}

// IsDistributedTable 检查表是否为分布式表
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

// GetClusterInfo 获取集群信息
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
// 数据清理方法
// ============================================================================

// DeleteByCondition 按条件删除数据
func (c *Client) DeleteByCondition(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	if tm, ok := c.tableManager.(*tableManager); ok {
		return tm.DeleteByCondition(ctx, tableName, condition, args...)
	}
	return fmt.Errorf("table manager not available")
}

// CleanTableData 清理表数据
func (c *Client) CleanTableData(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	if tm, ok := c.tableManager.(*tableManager); ok {
		return tm.CleanTableData(ctx, tableName, condition, args...)
	}
	return fmt.Errorf("table manager not available")
}

// EnhancedCleanTableData 增强的数据清理
func (c *Client) EnhancedCleanTableData(ctx context.Context, tableName string, opts *CleanupOptions) (*CleanupResult, error) {
	return c.enhancedCleanTableDataImpl(ctx, tableName, opts)
}

// enhancedCleanTableDataImpl 增强数据清理的内部实现
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

	// 检查是否为分布式表
	isDistributed, err := c.IsDistributedTable(ctx, tableName)
	if err != nil {
		log.Printf("无法检查表类型，按普通表处理: %v", err)
		isDistributed = false
	}

	// 如果没有条件，使用TRUNCATE
	if opts.Condition == "" {
		if opts.DryRun {
			// 预览模式：统计表中的总行数
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

		// 实际执行TRUNCATE
		if err := c.TruncateTable(ctx, tableName); err != nil {
			result.Error = err
			return result, err
		}
		return result, nil
	}

	// 有条件的删除 - 只使用DROP PARTITION方式
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

	// 提取分区信息
	partition, canUsePartition := c.extractPartitionFromCondition(opts.Condition)
	if !canUsePartition {
		result.Error = ErrUnsupportedPartitionCondition
		return result, result.Error
	}

	// 检查分区是否存在
	partitionExists, checkErr := c.checkPartitionExists(ctx, tableName, partition)
	if checkErr != nil {
		log.Printf("[警告] 无法检查分区存在性: %v，尝试直接删除", checkErr)
		partitionExists = true
	}

	if !partitionExists {
		log.Printf("分区 %s 不存在，无需清理", partition)
		result.DeletedRows = 0
		return result, nil
	}

	// 执行DROP PARTITION
	var dropQuery string
	if c.config.Cluster != "" && isDistributed {
		// 分布式表，获取本地表名
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

	log.Printf("分区 %s 已删除", partition)
	return result, nil
}

// CleanTableByDateRange 按日期范围清理数据
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

// CleanOldData 清理旧数据
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
// 表维护方法
// ============================================================================

// OptimizeTable 优化表
func (c *Client) OptimizeTable(ctx context.Context, tableName string, final bool) error {
	var query string
	if final {
		query = fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
	} else {
		query = fmt.Sprintf("OPTIMIZE TABLE %s", tableName)
	}

	if err := c.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, fmt.Errorf("优化表失败: %w", err))
	}

	log.Printf("表 %s 优化完成", tableName)
	return nil
}

// AnalyzeTable 分析表
func (c *Client) AnalyzeTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("ANALYZE TABLE %s", tableName)
	if err := c.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, fmt.Errorf("分析表失败: %w", err))
	}

	log.Printf("表 %s 分析完成", tableName)
	return nil
}

// BackupTable 备份表
func (c *Client) BackupTable(ctx context.Context, sourceTable, backupTable string) error {
	// 获取源表结构
	query := "SHOW CREATE TABLE " + sourceTable
	row := c.QueryRow(ctx, query)

	var createStatement string
	err := row.Scan(&createStatement)
	if err != nil {
		return WrapTableError(sourceTable, fmt.Errorf("获取源表结构失败: %w", err))
	}

	// 修改表名创建备份表
	backupSchema := strings.Replace(createStatement, sourceTable, backupTable, 1)
	if err := c.Exec(ctx, backupSchema); err != nil {
		return WrapTableError(backupTable, fmt.Errorf("创建备份表失败: %w", err))
	}

	// 复制数据
	copyQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", backupTable, sourceTable)
	if err := c.Exec(ctx, copyQuery); err != nil {
		// 如果复制失败，删除备份表
		c.DropTable(ctx, backupTable)
		return WrapTableError(sourceTable, fmt.Errorf("复制数据到备份表失败: %w", err))
	}

	log.Printf("表 %s 已备份到 %s", sourceTable, backupTable)
	return nil
}

// RenameTable 重命名表
func (c *Client) RenameTable(ctx context.Context, oldName, newName string) error {
	query := fmt.Sprintf("RENAME TABLE %s TO %s", oldName, newName)
	if err := c.Exec(ctx, query); err != nil {
		return WrapTableError(oldName, fmt.Errorf("重命名表失败: %w", err))
	}

	log.Printf("表 %s 已重命名为 %s", oldName, newName)
	return nil
}

// ============================================================================
// 便利方法和工具函数
// ============================================================================

// GetClusterName 获取集群名称
func (c *Client) GetClusterName() string {
	return c.config.Cluster
}

// GetTableNameResolver 获取表名解析器
func (c *Client) GetTableNameResolver() *TableNameResolver {
	return c.nameResolver
}

// extractPartitionFromCondition 从清理条件中提取分区信息
func (c *Client) extractPartitionFromCondition(condition string) (partition string, canUse bool) {
	log.Printf("[分区检测] 分析清理条件: %s", condition)

	// 处理按天账单的日期条件
	if matches := regexp.MustCompile(`billing_date\s*=\s*'(\d{4}-\d{2}-\d{2})'`).FindStringSubmatch(condition); len(matches) > 1 {
		date := matches[1]
		yearMonthDay := strings.ReplaceAll(date, "-", "")
		log.Printf("[分区检测] 提取天表分区: %s", yearMonthDay)
		return yearMonthDay, true
	}

	// 处理按月账单的账期条件
	if matches := regexp.MustCompile(`billing_cycle\s*=\s*'(\d{4}-\d{2})'`).FindStringSubmatch(condition); len(matches) > 1 {
		cycle := matches[1]
		yearMonth := strings.Replace(cycle, "-", "", 1)
		log.Printf("[分区检测] 提取分区: %s", yearMonth)
		return yearMonth, true
	}

	// 处理火山引擎相关条件
	if matches := regexp.MustCompile(`ExpenseDate\s+LIKE\s+'(\d{4}-\d{2})%'`).FindStringSubmatch(condition); len(matches) > 1 {
		cycle := matches[1]
		yearMonth := strings.Replace(cycle, "-", "", 1)
		return yearMonth, true
	}

	return "", false
}

// checkPartitionExists 检查分区是否存在
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

// getLocalTableName 获取分布式表对应的本地表名
func (c *Client) getLocalTableName(distributedTableName string) string {
	if strings.Contains(distributedTableName, "_distributed") {
		return strings.Replace(distributedTableName, "_distributed", "_local", 1)
	}
	return distributedTableName + "_local"
}

// ============================================================================
// 兼容性方法（保持向后兼容）
// ============================================================================

// InsertToDistributed 分布式表插入（兼容性方法）
func (c *Client) InsertToDistributed(ctx context.Context, distributedTableName string, data map[string]interface{}) error {
	return c.Insert(ctx, distributedTableName, data)
}

// InsertBatchToDistributed 分布式表批量插入（兼容性方法）
func (c *Client) InsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return c.InsertBatch(ctx, distributedTableName, data)
}

// AsyncInsertBatchToDistributed 异步分布式表批量插入（兼容性方法）
func (c *Client) AsyncInsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return c.AsyncInsertBatch(ctx, distributedTableName, data)
}

// OptimizedBatchInsertToDistributed 优化的分布式表批量插入（兼容性方法）
func (c *Client) OptimizedBatchInsertToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error) {
	return c.OptimizedBatchInsert(ctx, distributedTableName, data, opts)
}

// CreateDistributedTableWithResolver 创建完整的分布式表结构（兼容性方法）
func (c *Client) CreateDistributedTableWithResolver(ctx context.Context, baseTableName string, localSchema string) error {
	if c.config.Cluster == "" {
		return ErrClusterNotConfigured
	}

	distributedTableName, localTableName := c.nameResolver.GetTablePair(baseTableName)

	// 先创建本地表
	if err := c.CreateLocalTable(ctx, baseTableName, localSchema); err != nil {
		return WrapTableError(localTableName, fmt.Errorf("failed to create local table: %w", err))
	}

	// 等待集群同步
	time.Sleep(2 * time.Second)

	// 创建分布式表
	distributedSchema := fmt.Sprintf(`AS %s ENGINE = Distributed(%s, %s, %s, rand())`,
		localTableName, c.config.Cluster, c.config.Database, localTableName)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		distributedTableName, c.config.Cluster, distributedSchema)

	if err := c.Exec(ctx, query); err != nil {
		return WrapTableError(distributedTableName, fmt.Errorf("failed to create distributed table: %w", err))
	}

	log.Printf("[自动表名解析] 成功创建分布式表结构：本地表=%s，分布式表=%s",
		localTableName, distributedTableName)

	return nil
}

// CreateLocalTable 明确创建本地表（兼容性方法）
func (c *Client) CreateLocalTable(ctx context.Context, baseTableName string, schema string) error {
	localTableName := c.nameResolver.ResolveLocalTableName(baseTableName)

	if c.nameResolver.IsClusterEnabled() {
		// 在集群上创建本地表
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
			localTableName, c.config.Cluster, schema)
		return c.Exec(ctx, query)
	} else {
		// 单机模式
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", localTableName, schema)
		return c.Exec(ctx, query)
	}
}