package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"goscan/pkg/config"
	"log"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Client struct {
	conn         driver.Conn
	config       *config.ClickHouseConfig
	nameResolver *TableNameResolver
}

// BatchInsertOptions 批量插入选项
type BatchInsertOptions struct {
	BatchSize   int           // 每批次大小
	MaxRetries  int           // 最大重试次数
	RetryDelay  time.Duration // 重试延迟
	EnableAsync bool          // 是否启用异步插入
	Timeout     time.Duration // 超时时间
}

// DefaultBatchInsertOptions 返回默认的批量插入选项
func DefaultBatchInsertOptions() *BatchInsertOptions {
	return &BatchInsertOptions{
		BatchSize:   500,
		MaxRetries:  3,
		RetryDelay:  2 * time.Second,
		EnableAsync: false,
		Timeout:     30 * time.Second,
	}
}

// BatchInsertResult 批量插入结果
type BatchInsertResult struct {
	TotalRecords     int           `json:"total_records"`
	ProcessedBatches int           `json:"processed_batches"`
	FailedBatches    int           `json:"failed_batches"`
	InsertedRecords  int           `json:"inserted_records"`
	FailedRecords    int           `json:"failed_records"`
	Duration         time.Duration `json:"duration"`
	AverageSpeed     float64       `json:"average_speed"` // records per second
	Errors           []error       `json:"errors,omitempty"`
}

func NewClient(cfg *config.ClickHouseConfig) (*Client, error) {
	opts := &clickhouse.Options{
		Addr: cfg.GetAddresses(),
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Debug:    cfg.Debug,
		Protocol: cfg.GetProtocol(),
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:          30 * time.Second,
		MaxOpenConns:         10,
		MaxIdleConns:         5,
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	}

	// 只有在 Native 协议时才设置压缩，HTTP 协议不支持 LZ4 压缩
	if cfg.GetProtocol() == clickhouse.Native {
		opts.Compression = &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &Client{
		conn:         conn,
		config:       cfg,
		nameResolver: NewTableNameResolver(cfg),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

func (c *Client) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return c.conn.PrepareBatch(ctx, query)
}

func (c *Client) InsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to insert")
	}

	// 自动解析表名
	resolvedTableName := c.nameResolver.ResolveInsertTarget(tableName)

	columns := make([]string, 0, len(data[0]))
	for column := range data[0] {
		columns = append(columns, column)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s)",
		resolvedTableName,
		joinStrings(columns, ", "))

	batch, err := c.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, column := range columns {
			values[i] = row[column]
		}

		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	return batch.Send()
}

func (c *Client) Insert(ctx context.Context, tableName string, data map[string]interface{}) error {
	// 自动解析表名
	resolvedTableName := c.nameResolver.ResolveInsertTarget(tableName)

	columns := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	for column, value := range data {
		columns = append(columns, column)
		values = append(values, value)
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		resolvedTableName,
		joinStrings(columns, ", "),
		joinStrings(placeholders, ", "))

	return c.conn.Exec(ctx, query, values...)
}

func (c *Client) AsyncInsertBatch(ctx context.Context, tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data to insert")
	}

	// 启用异步插入
	ctxWithSettings := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":                 1,
		"wait_for_async_insert":        1,
		"async_insert_max_data_size":   10485760, // 10MB
		"async_insert_busy_timeout_ms": 200,
	}))

	return c.InsertBatch(ctxWithSettings, tableName, data)
}

func (c *Client) CreateTable(ctx context.Context, tableName string, schema string) error {
	// 对于CreateTable，我们需要明确是创建分布式表还是本地表
	// 默认情况下，如果配置了集群，创建分布式表
	resolvedTableName := c.nameResolver.ResolveCreateTableTarget(tableName, true)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", resolvedTableName, schema)
	return c.Exec(ctx, query)
}

func (c *Client) DropTable(ctx context.Context, tableName string) error {
	// 自动解析表名用于删除操作
	resolvedTableName := c.nameResolver.ResolveQueryTarget(tableName)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", resolvedTableName)
	return c.Exec(ctx, query)
}

// DropDistributedTable 删除分布式表（包括本地表和分布式表）
func (c *Client) DropDistributedTable(ctx context.Context, localTableName, distributedTableName string) error {
	if c.config.Cluster == "" {
		return fmt.Errorf("cluster name is required for distributed table operations")
	}

	// 先删除分布式表
	if distributedTableName != "" {
		dropDistributedQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", distributedTableName)
		if err := c.Exec(ctx, dropDistributedQuery); err != nil {
			log.Printf("警告: 删除分布式表 %s 失败: %v", distributedTableName, err)
		}
	}

	// 然后删除所有节点上的本地表
	if localTableName != "" {
		if err := c.DropTableOnCluster(ctx, localTableName); err != nil {
			return fmt.Errorf("failed to drop local table %s on cluster: %w", localTableName, err)
		}
	}

	return nil
}

// DropTableOnCluster 在集群上删除表
func (c *Client) DropTableOnCluster(ctx context.Context, tableName string) error {
	if c.config.Cluster == "" {
		return fmt.Errorf("cluster name is required for cluster operations")
	}

	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s ON CLUSTER %s", tableName, c.config.Cluster)
	return c.Exec(ctx, dropQuery)
}

// DropTableSafely 安全删除表（带确认）
func (c *Client) DropTableSafely(ctx context.Context, tableName string, confirmation string) error {
	if confirmation != "YES" {
		return fmt.Errorf("删除表操作已取消，如需删除请提供确认字符串 'YES'")
	}

	// 检查表是否存在
	exists, err := c.TableExists(ctx, tableName)
	if err != nil {
		return fmt.Errorf("检查表是否存在时出错: %w", err)
	}

	if !exists {
		log.Printf("表 %s 不存在，跳过删除", tableName)
		return nil
	}

	// 获取表信息用于日志记录
	info, err := c.GetTableInfo(ctx, tableName)
	if err == nil {
		log.Printf("即将删除表: %s, 记录数: %d, 大小: %s",
			tableName, info.TotalRows, formatBytes(uint64(info.TotalBytes)))
	}

	return c.DropTable(ctx, tableName)
}

// DropDistributedTableSafely 安全删除分布式表（带确认）
func (c *Client) DropDistributedTableSafely(ctx context.Context, localTableName, distributedTableName string, confirmation string) error {
	if confirmation != "YES" {
		return fmt.Errorf("删除分布式表操作已取消，如需删除请提供确认字符串 'YES'")
	}

	if c.config.Cluster == "" {
		return fmt.Errorf("cluster name is required for distributed table operations")
	}

	// 记录删除信息
	log.Printf("即将删除分布式表结构:")
	log.Printf("  - 分布式表: %s", distributedTableName)
	log.Printf("  - 本地表: %s (集群: %s)", localTableName, c.config.Cluster)

	// 获取表信息
	if distributedTableName != "" {
		if exists, err := c.TableExists(ctx, distributedTableName); err == nil && exists {
			if info, err := c.GetTableInfo(ctx, distributedTableName); err == nil {
				log.Printf("  - 分布式表记录数: %d", info.TotalRows)
			}
		}
	}

	return c.DropDistributedTable(ctx, localTableName, distributedTableName)
}

// formatBytes 格式化字节数为可读字符串
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(bytes)/float64(div), "KMGTPE"[exp])
}

func (c *Client) CreateDistributedTable(ctx context.Context, localTableName, distributedTableName string, schema string) error {
	if c.config.Cluster == "" {
		return fmt.Errorf("cluster name is required for distributed table")
	}

	// 步骤1: 使用ON CLUSTER在所有节点上创建本地表
	log.Printf("[分布式表] 开始在集群 %s 上创建本地表 %s", c.config.Cluster, localTableName)
	createLocalQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		localTableName, c.config.Cluster, schema)

	log.Printf("[分布式表] 执行建表SQL: %s", createLocalQuery)

	if err := c.conn.Exec(ctx, createLocalQuery); err != nil {
		return fmt.Errorf("failed to create local table on cluster: %w", err)
	}
	log.Printf("[分布式表] 本地表 %s 在集群所有节点创建成功", localTableName)

	// 步骤2: 等待集群同步，然后创建分布式表
	log.Printf("[分布式表] 等待集群同步...")
	time.Sleep(3 * time.Second)

	// 步骤3: 使用AS语法创建分布式表，引用本地表（ClickHouse最佳实践）
	log.Printf("[分布式表] 开始创建分布式表 %s，引用本地表 %s", distributedTableName, localTableName)
	distributedSchema := fmt.Sprintf(`AS %s ENGINE = Distributed(%s, %s, %s, rand())`,
		localTableName, c.config.Cluster, c.config.Database, localTableName)

	createDistributedQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		distributedTableName, c.config.Cluster, distributedSchema)

	if err := c.conn.Exec(ctx, createDistributedQuery); err != nil {
		return fmt.Errorf("failed to create distributed table: %w", err)
	}
	log.Printf("[分布式表] 分布式表 %s 创建成功，已链接到本地表 %s", distributedTableName, localTableName)

	return nil
}

func (c *Client) InsertToDistributed(ctx context.Context, distributedTableName string, data map[string]interface{}) error {
	return c.Insert(ctx, distributedTableName, data)
}

func (c *Client) InsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return c.InsertBatch(ctx, distributedTableName, data)
}

func (c *Client) AsyncInsertBatchToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}) error {
	return c.AsyncInsertBatch(ctx, distributedTableName, data)
}

func (c *Client) GetClusterInfo(ctx context.Context) ([]map[string]interface{}, error) {
	if c.config.Cluster == "" {
		return nil, fmt.Errorf("cluster name is not configured")
	}

	query := "SELECT host_name, port, is_local FROM system.clusters WHERE cluster = ?"
	rows, err := c.Query(ctx, query, c.config.Cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to query cluster info: %w", err)
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var hostName string
		var port uint16
		var isLocal uint8

		if err := rows.Scan(&hostName, &port, &isLocal); err != nil {
			return nil, fmt.Errorf("failed to scan cluster info: %w", err)
		}

		result = append(result, map[string]interface{}{
			"host_name": hostName,
			"port":      port,
			"is_local":  isLocal == 1,
		})
	}

	return result, rows.Err()
}

func (c *Client) TableExists(ctx context.Context, tableName string) (bool, error) {
	// 自动解析表名
	resolvedTableName := c.nameResolver.ResolveQueryTarget(tableName)

	// 如果配置了集群，使用集群查询
	if c.config.Cluster != "" {
		return c.tableExistsInCluster(ctx, resolvedTableName)
	}

	// 单机查询
	query := "SELECT 1 FROM system.tables WHERE database = ? AND name = ?"
	row := c.QueryRow(ctx, query, c.config.Database, resolvedTableName)

	var exists uint8
	err := row.Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	return exists == 1, nil
}

// tableExistsInCluster 检查表在集群中是否存在
func (c *Client) tableExistsInCluster(ctx context.Context, tableName string) (bool, error) {
	// 查询集群中所有节点的表状态
	query := fmt.Sprintf(`
		SELECT 
			hostName() as host,
			COUNT(*) as table_count
		FROM clusterAllReplicas(%s, system.tables) 
		WHERE database = '%s' AND name = '%s'
		GROUP BY hostName()
		ORDER BY hostName()`,
		c.config.Cluster, c.config.Database, tableName)

	rows, err := c.Query(ctx, query)
	if err != nil {
		// 如果集群查询失败，回退到简单查询
		log.Printf("[集群表检查] 集群查询失败，回退到简单查询: %v", err)
		return c.simpleTableExists(ctx, tableName)
	}
	defer rows.Close()

	var nodeCount int
	var nodesWithTable int

	for rows.Next() {
		var host string
		var tableCount uint64

		if err := rows.Scan(&host, &tableCount); err != nil {
			return false, fmt.Errorf("failed to scan cluster table result: %w", err)
		}

		nodeCount++
		if tableCount > 0 {
			nodesWithTable++
		}
	}

	if nodeCount == 0 {
		// 没有节点响应，尝试简单查询
		return c.simpleTableExists(ctx, tableName)
	}

	// 如果所有节点都有表，则认为表存在
	// 如果部分节点有表，记录警告但认为表存在（可能是创建过程中）
	if nodesWithTable == nodeCount {
		return true, nil
	} else if nodesWithTable > 0 {
		log.Printf("[集群表检查] 警告：表 %s 在 %d/%d 个节点上存在", tableName, nodesWithTable, nodeCount)
		return true, nil
	}

	return false, nil
}

// simpleTableExists 简单的表存在性检查（回退方法）
func (c *Client) simpleTableExists(ctx context.Context, tableName string) (bool, error) {
	query := "SELECT 1 FROM system.tables WHERE database = ? AND name = ?"
	row := c.QueryRow(ctx, query, c.config.Database, tableName)

	var exists uint8
	err := row.Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return exists == 1, nil
}

func (c *Client) TruncateTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	return c.Exec(ctx, query)
}

func (c *Client) DeleteByCondition(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, condition)
	return c.Exec(ctx, query, args...)
}

func (c *Client) CleanTableData(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	if condition == "" {
		return c.TruncateTable(ctx, tableName)
	}
	return c.DeleteByCondition(ctx, tableName, condition, args...)
}

// GetClusterName 获取集群名称
func (c *Client) GetClusterName() string {
	return c.config.Cluster
}

// GetTableNameResolver 获取表名解析器
func (c *Client) GetTableNameResolver() *TableNameResolver {
	return c.nameResolver
}

// CreateTableWithResolver 使用解析器创建表，允许明确指定是否创建分布式表
func (c *Client) CreateTableWithResolver(ctx context.Context, baseTableName string, schema string, useDistributed bool) error {
	resolvedTableName := c.nameResolver.ResolveCreateTableTarget(baseTableName, useDistributed)
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", resolvedTableName, schema)
	return c.Exec(ctx, query)
}

// CreateLocalTable 明确创建本地表
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

// CreateDistributedTableWithResolver 创建完整的分布式表结构（本地表+分布式表）
func (c *Client) CreateDistributedTableWithResolver(ctx context.Context, baseTableName string, localSchema string) error {
	if !c.nameResolver.IsClusterEnabled() {
		return fmt.Errorf("cluster not configured, cannot create distributed table")
	}

	distributedTableName, localTableName := c.nameResolver.GetTablePair(baseTableName)

	// 先创建本地表
	if err := c.CreateLocalTable(ctx, baseTableName, localSchema); err != nil {
		return fmt.Errorf("failed to create local table %s: %w", localTableName, err)
	}

	// 等待集群同步
	time.Sleep(2 * time.Second)

	// 创建分布式表
	distributedSchema := fmt.Sprintf(`AS %s ENGINE = Distributed(%s, %s, %s, rand())`,
		localTableName, c.config.Cluster, c.config.Database, localTableName)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s %s",
		distributedTableName, c.config.Cluster, distributedSchema)

	if err := c.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create distributed table %s: %w", distributedTableName, err)
	}

	log.Printf("[自动表名解析] 成功创建分布式表结构：本地表=%s，分布式表=%s",
		localTableName, distributedTableName)

	return nil
}

// CleanDistributedTableData 清理分布式表数据（通过清理对应的本地表）
func (c *Client) CleanDistributedTableData(ctx context.Context, distributedTableName, localTableName string, condition string, args ...interface{}) error {
	if c.config.Cluster == "" {
		return fmt.Errorf("cluster name is required for distributed table operations")
	}

	// 获取清理前的记录数用于验证
	var beforeCount uint64
	if condition != "" {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", distributedTableName, condition)
		row := c.QueryRow(ctx, countQuery, args...)
		if err := row.Scan(&beforeCount); err != nil {
			log.Printf("[警告] 无法获取清理前记录数: %v", err)
		} else {
			log.Printf("[分布式清理] 清理前符合条件的记录数: %d", beforeCount)
		}
	}

	// 执行清理操作
	var err error
	if condition == "" {
		// TRUNCATE 操作可以直接在分布式表上执行，会影响所有本地表
		query := fmt.Sprintf("TRUNCATE TABLE %s ON CLUSTER %s", localTableName, c.config.Cluster)
		err = c.Exec(ctx, query)
	} else {
		// 尝试使用DROP PARTITION进行高效清理
		partitionDropped := false
		partition, canUsePartition := c.extractPartitionFromCondition(condition)
		if canUsePartition {
			// 首先检查分区是否存在
			partitionExists, checkErr := c.checkPartitionExists(ctx, localTableName, partition)
			if checkErr != nil {
				log.Printf("[警告] 无法检查分区存在性: %v，尝试直接执行DROP PARTITION", checkErr)
				partitionExists = true // 假设存在，让DROP PARTITION自己处理
			}

			if partitionExists {
				log.Printf("[分布式清理] 分区 %s 存在，使用DROP PARTITION清理", partition)
				query := fmt.Sprintf("ALTER TABLE %s ON CLUSTER %s DROP PARTITION '%s'", localTableName, c.config.Cluster, partition)
				log.Printf("[分布式清理] 执行DROP PARTITION语句: %s", query)
				if err = c.Exec(ctx, query); err != nil {
					log.Printf("[警告] DROP PARTITION失败: %v，回退到DELETE", err)
				} else {
					partitionDropped = true
					log.Printf("[分布式清理] DROP PARTITION成功，分区 %s 已删除", partition)
				}
			} else {
				log.Printf("[分布式清理] 分区 %s 不存在，使用DELETE处理", partition)
			}
		}

		// 如果不能使用DROP PARTITION或失败，则使用DELETE
		if !partitionDropped {
			log.Printf("[分布式清理] 使用DELETE清理条件: %s", condition)
			query := fmt.Sprintf("DELETE FROM %s ON CLUSTER %s WHERE %s", localTableName, c.config.Cluster, condition)
			log.Printf("[分布式清理] 执行DELETE语句: %s", query)
			err = c.Exec(ctx, query, args...)
			if err != nil {
				log.Printf("[错误] DELETE执行失败: %v", err)
			} else {
				log.Printf("[分布式清理] DELETE执行成功")
			}
		}
	}

	if err != nil {
		return fmt.Errorf("分布式表清理操作失败: %w", err)
	}

	// 验证清理效果（等待一定时间让数据同步）
	if condition != "" && beforeCount > 0 {
		time.Sleep(2 * time.Second) // 等待分布式表数据同步

		var afterCount uint64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", distributedTableName, condition)
		row := c.QueryRow(ctx, countQuery, args...)
		if err := row.Scan(&afterCount); err != nil {
			log.Printf("[警告] 无法验证清理效果: %v", err)
		} else {
			log.Printf("[分布式清理] 清理后符合条件的记录数: %d", afterCount)
			if afterCount > 0 {
				log.Printf("[警告] 分布式表清理不完全，仍有 %d 条记录未清理", afterCount)
				// 可以选择重试清理操作
				return c.retryDistributedClean(ctx, distributedTableName, localTableName, condition, args...)
			}
		}
	}

	log.Printf("[分布式清理] 清理操作完成")
	return nil
}

// extractPartitionFromCondition 从清理条件中提取分区信息
// 支持阿里云账单表的分区模式：天表按日分区 toYYYYMMDD(billing_date)，月表按月分区 toYYYYMM(parseDateTimeBestEffort(billing_cycle || '-01'))
func (c *Client) extractPartitionFromCondition(condition string) (partition string, canUse bool) {
	log.Printf("[分区检测] 分析清理条件: %s", condition)

	// 处理按天账单的日期条件：billing_date = '2024-09-09' -> 分区 20240909 (toYYYYMMDD格式)
	if matches := regexp.MustCompile(`billing_date\s*=\s*'(\d{4}-\d{2}-\d{2})'`).FindStringSubmatch(condition); len(matches) > 1 {
		date := matches[1]
		log.Printf("[分区检测] 匹配到按天条件，日期: %s", date)
		if len(date) == 10 { // YYYY-MM-DD格式
			yearMonthDay := strings.ReplaceAll(date, "-", "") // 去掉所有-，得到YYYYMMDD格式
			log.Printf("[分区检测] 提取天表分区: %s", yearMonthDay)
			return yearMonthDay, true
		}
	}

	// 处理按月账单的账期条件：billing_cycle = '2024-09' -> 分区 202409
	if matches := regexp.MustCompile(`billing_cycle\s*=\s*'(\d{4}-\d{2})'`).FindStringSubmatch(condition); len(matches) > 1 {
		cycle := matches[1]
		log.Printf("[分区检测] 匹配到按月条件，账期: %s", cycle)
		if len(cycle) == 7 { // YYYY-MM格式
			yearMonth := strings.Replace(cycle, "-", "", 1) // 去掉-
			log.Printf("[分区检测] 提取分区: %s", yearMonth)
			return yearMonth, true
		}
	}

	// 处理包含月份范围的条件：toYYYYMM(billing_date) = 202409
	if matches := regexp.MustCompile(`toYYYYMM\([^)]+\)\s*=\s*(\d{6})`).FindStringSubmatch(condition); len(matches) > 1 {
		partition := matches[1]
		log.Printf("[分区检测] 匹配到函数条件，分区: %s", partition)
		if len(partition) == 6 { // YYYYMM格式
			return partition, true
		}
	}

	// 处理复杂的月表清理条件：toYYYYMM(parseDateTimeBestEffort(billing_cycle || '-01')) = toYYYYMM(now())
	if regexp.MustCompile(`toYYYYMM\(parseDateTimeBestEffort\(billing_cycle\s*\|\|\s*'-01'\)\)\s*=\s*toYYYYMM\(now\(\)\)`).MatchString(condition) {
		currentMonth := time.Now().Format("200601") // YYYYMM格式
		log.Printf("[分区检测] 匹配到复杂月表条件，当前月分区: %s", currentMonth)
		return currentMonth, true
	}

	log.Printf("[分区检测] 无法提取分区信息，将使用DELETE")
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
		return false, fmt.Errorf("failed to check partition existence: %w", err)
	}

	return count > 0, nil
}

// retryDistributedClean 重试分布式表清理
func (c *Client) retryDistributedClean(ctx context.Context, distributedTableName, localTableName string, condition string, args ...interface{}) error {
	log.Printf("[分布式清理] 开始重试清理操作")

	// 重试最多3次
	for i := 0; i < 3; i++ {
		time.Sleep(time.Duration(i+1) * time.Second) // 递增延迟

		// 再次执行DELETE
		query := fmt.Sprintf("DELETE FROM %s ON CLUSTER %s WHERE %s", localTableName, c.config.Cluster, condition)
		if err := c.Exec(ctx, query, args...); err != nil {
			log.Printf("[分布式清理] 重试第%d次失败: %v", i+1, err)
			continue
		}

		// 验证是否清理完成
		time.Sleep(2 * time.Second)
		var count uint64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", distributedTableName, condition)
		row := c.QueryRow(ctx, countQuery, args...)
		if err := row.Scan(&count); err != nil {
			log.Printf("[分布式清理] 重试第%d次验证失败: %v", i+1, err)
			continue
		}

		if count == 0 {
			log.Printf("[分布式清理] 重试第%d次成功，数据已完全清理", i+1)
			return nil
		} else {
			log.Printf("[分布式清理] 重试第%d次后仍有 %d 条记录", i+1, count)
		}
	}

	log.Printf("[分布式清理] 重试3次后仍未完全清理数据，建议检查集群状态")
	return fmt.Errorf("分布式表清理重试失败，可能存在集群节点问题")
}

// IsDistributedTable 检查表是否为分布式表
func (c *Client) IsDistributedTable(ctx context.Context, tableName string) (bool, error) {
	query := "SELECT engine FROM system.tables WHERE database = ? AND name = ?"
	row := c.QueryRow(ctx, query, c.config.Database, tableName)

	var engine string
	err := row.Scan(&engine)
	if err != nil {
		return false, err
	}

	return engine == "Distributed", nil
}

// GetLocalTableNameForDistributed 获取分布式表对应的本地表名
func (c *Client) GetLocalTableNameForDistributed(ctx context.Context, distributedTableName string) (string, error) {
	// 查询分布式表的配置信息
	query := `
		SELECT engine_full 
		FROM system.tables 
		WHERE database = ? AND name = ? AND engine = 'Distributed'
	`
	row := c.QueryRow(ctx, query, c.config.Database, distributedTableName)

	var engineFull string
	err := row.Scan(&engineFull)
	if err != nil {
		return "", fmt.Errorf("failed to get distributed table config: %w", err)
	}

	// 解析引擎配置，提取本地表名
	// 格式类似: Distributed(cluster, database, table)
	// 简化处理，假设本地表名遵循命名约定
	if strings.Contains(distributedTableName, "_distributed") {
		return strings.Replace(distributedTableName, "_distributed", "_local", 1), nil
	}

	// 默认在表名后添加 _local
	return distributedTableName + "_local", nil
}

// CleanupOptions 数据清理选项
type CleanupOptions struct {
	Condition     string        // 清理条件
	Args          []interface{} // 条件参数
	DryRun        bool          // 是否只是预览而不实际删除
	BatchSize     int           // 分批删除的大小（用于大数据量）
	MaxRows       int           // 最大删除行数限制
	ConfirmDelete bool          // 是否需要确认删除
	ProgressLog   bool          // 是否显示删除进度
}

// CleanupResult 清理结果
type CleanupResult struct {
	DeletedRows    int64         `json:"deleted_rows"`
	AffectedTables []string      `json:"affected_tables"`
	Duration       time.Duration `json:"duration"`
	DryRun         bool          `json:"dry_run"`
	PreviewRows    int64         `json:"preview_rows,omitempty"` // 仅在DryRun模式下有效
	Error          error         `json:"error,omitempty"`
}

// EnhancedCleanTableData 增强的数据清理方法
func (c *Client) EnhancedCleanTableData(ctx context.Context, tableName string, opts *CleanupOptions) (*CleanupResult, error) {
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

	var localTableName string
	if isDistributed {
		localTableName, err = c.GetLocalTableNameForDistributed(ctx, tableName)
		if err != nil {
			result.Error = fmt.Errorf("无法获取本地表名: %w", err)
			return result, result.Error
		}
		log.Printf("检测到分布式表 %s，将操作本地表 %s", tableName, localTableName)
	}

	// 如果没有条件，使用TRUNCATE
	if opts.Condition == "" {
		if opts.DryRun {
			// 预览模式：统计表中的总行数
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
			row := c.QueryRow(ctx, countQuery)
			var count uint64
			if err := row.Scan(&count); err != nil {
				result.Error = fmt.Errorf("统计表行数失败: %w", err)
				return result, err
			}
			result.PreviewRows = int64(count)
			if isDistributed {
				log.Printf("[预览模式] 分布式表 %s 将被清空（通过本地表 %s），影响 %d 行", tableName, localTableName, count)
			} else {
				log.Printf("[预览模式] 表 %s 将被清空，影响 %d 行", tableName, count)
			}
			return result, nil
		}

		// 实际执行TRUNCATE
		if isDistributed {
			if err := c.CleanDistributedTableData(ctx, tableName, localTableName, "", nil); err != nil {
				result.Error = err
				return result, err
			}
			log.Printf("分布式表 %s 已清空（通过本地表 %s）", tableName, localTableName)
		} else {
			if err := c.TruncateTable(ctx, tableName); err != nil {
				result.Error = err
				return result, err
			}
			log.Printf("表 %s 已清空", tableName)
		}
		return result, nil
	}

	// 有条件的删除
	if opts.DryRun {
		// 预览模式：统计符合条件的行数
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, opts.Condition)
		row := c.QueryRow(ctx, countQuery, opts.Args...)
		var count uint64
		if err := row.Scan(&count); err != nil {
			result.Error = fmt.Errorf("统计符合条件的行数失败: %w", err)
			return result, err
		}
		result.PreviewRows = int64(count)
		if isDistributed {
			log.Printf("[预览模式] 分布式表 %s 中符合条件 '%s' 的记录有 %d 行（将通过本地表 %s 删除）", tableName, opts.Condition, count, localTableName)
		} else {
			log.Printf("[预览模式] 表 %s 中符合条件 '%s' 的记录有 %d 行", tableName, opts.Condition, count)
		}
		return result, nil
	}

	// 检查是否有最大行数限制
	if opts.MaxRows > 0 {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, opts.Condition)
		row := c.QueryRow(ctx, countQuery, opts.Args...)
		var count uint64
		if err := row.Scan(&count); err != nil {
			result.Error = fmt.Errorf("统计行数失败: %w", err)
			return result, err
		}

		if count > uint64(opts.MaxRows) {
			result.Error = fmt.Errorf("要删除的行数 (%d) 超过最大限制 (%d)", count, opts.MaxRows)
			return result, result.Error
		}
	}

	// 在删除前先统计行数（提供准确的删除反馈）
	var beforeCount uint64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, opts.Condition)
	row := c.QueryRow(ctx, countQuery, opts.Args...)
	if err := row.Scan(&beforeCount); err != nil {
		log.Printf("[警告] 无法统计删除前行数: %v", err)
		beforeCount = 0
	}

	// 实际删除数据
	if opts.BatchSize > 0 {
		// 分批删除
		if isDistributed {
			result.DeletedRows = c.batchDeleteDistributed(ctx, tableName, localTableName, opts)
		} else {
			result.DeletedRows = c.batchDelete(ctx, tableName, opts)
		}
	} else {
		// 一次性删除
		if isDistributed {
			log.Printf("检测到分布式表，调用分布式清理方法: 表=%s, 本地表=%s, 条件=%s", tableName, localTableName, opts.Condition)
			if err := c.CleanDistributedTableData(ctx, tableName, localTableName, opts.Condition, opts.Args); err != nil {
				result.Error = err
				return result, err
			}
		} else {
			// 尝试对本地表也使用分区删除优化
			partitionDropped := false
			partition, canUsePartition := c.extractPartitionFromCondition(opts.Condition)
			if canUsePartition {
				// 检查分区是否存在
				partitionExists, checkErr := c.checkPartitionExists(ctx, tableName, partition)
				if checkErr != nil {
					log.Printf("[警告] 无法检查分区存在性: %v，使用DELETE", checkErr)
				} else if partitionExists {
					log.Printf("[本地表清理] 分区 %s 存在，使用DROP PARTITION清理", partition)
					var dropQuery string
					if c.config.Cluster != "" {
						// 集群环境，使用 ON CLUSTER 语法
						dropQuery = fmt.Sprintf("ALTER TABLE %s ON CLUSTER %s DROP PARTITION '%s'", tableName, c.config.Cluster, partition)
					} else {
						// 单机环境
						dropQuery = fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tableName, partition)
					}
					log.Printf("[本地表清理] 执行DROP PARTITION语句: %s", dropQuery)
					if err := c.Exec(ctx, dropQuery); err != nil {
						log.Printf("[警告] DROP PARTITION失败: %v，回退到DELETE", err)
					} else {
						partitionDropped = true
						log.Printf("[本地表清理] DROP PARTITION成功，分区 %s 已删除", partition)
					}
				} else {
					log.Printf("[本地表清理] 分区 %s 不存在，使用DELETE处理", partition)
				}
			}

			// 如果不能使用DROP PARTITION或失败，则使用DELETE
			if !partitionDropped {
				log.Printf("[本地表清理] 使用DELETE清理条件: %s", opts.Condition)
				deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, opts.Condition)
				log.Printf("[本地表清理] 执行DELETE语句: %s", deleteQuery)
				if err := c.Exec(ctx, deleteQuery, opts.Args...); err != nil {
					result.Error = err
					return result, err
				}
			}
		}

		// 设置删除行数（基于删除前统计）
		result.DeletedRows = int64(beforeCount)

		// 验证删除效果（可选，对于性能敏感的场景可以关闭）
		if beforeCount > 0 {
			time.Sleep(100 * time.Millisecond) // 短暂等待确保数据一致性
			var afterCount uint64
			afterRow := c.QueryRow(ctx, countQuery, opts.Args...)
			if err := afterRow.Scan(&afterCount); err == nil {
				if afterCount > 0 {
					log.Printf("[警告] 清理不完全: 删除前 %d 行，删除后仍有 %d 行", beforeCount, afterCount)
					result.DeletedRows = int64(beforeCount - afterCount)
				} else {
					log.Printf("[清理成功] 已删除 %d 行记录", beforeCount)
				}
			}
		}
	}

	if opts.ProgressLog {
		log.Printf("表 %s 数据清理完成，删除条件: %s", tableName, opts.Condition)
	}

	return result, nil
}

// batchDelete 分批删除数据
func (c *Client) batchDelete(ctx context.Context, tableName string, opts *CleanupOptions) int64 {
	var totalDeleted int64
	batchNum := 1

	for {
		// 构建分批删除查询
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT %d", tableName, opts.Condition, opts.BatchSize)

		if opts.ProgressLog {
			log.Printf("执行第 %d 批删除，批次大小: %d", batchNum, opts.BatchSize)
		}

		if err := c.Exec(ctx, deleteQuery, opts.Args...); err != nil {
			log.Printf("第 %d 批删除失败: %v", batchNum, err)
			break
		}

		// 检查是否还有符合条件的数据
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tableName, opts.Condition)
		row := c.QueryRow(ctx, countQuery, opts.Args...)
		var remaining uint64
		if err := row.Scan(&remaining); err != nil {
			log.Printf("统计剩余行数失败: %v", err)
			break
		}

		if remaining == 0 {
			if opts.ProgressLog {
				log.Printf("第 %d 批删除完成，无剩余数据", batchNum)
			}
			break
		}

		totalDeleted += int64(opts.BatchSize)
		batchNum++

		// 如果达到最大行数限制，停止删除
		if opts.MaxRows > 0 && totalDeleted >= int64(opts.MaxRows) {
			break
		}

		// 短暂休息，避免对数据库造成过大压力
		time.Sleep(100 * time.Millisecond)
	}

	return totalDeleted
}

// batchDeleteDistributed 分布式表分批删除数据
func (c *Client) batchDeleteDistributed(ctx context.Context, distributedTableName, localTableName string, opts *CleanupOptions) int64 {
	var totalDeleted int64
	batchNum := 1

	for {
		// 构建分批删除查询，使用ON CLUSTER在所有本地表上执行
		deleteQuery := fmt.Sprintf("DELETE FROM %s ON CLUSTER %s WHERE %s LIMIT %d",
			localTableName, c.config.Cluster, opts.Condition, opts.BatchSize)

		if opts.ProgressLog {
			log.Printf("执行分布式表第 %d 批删除，批次大小: %d", batchNum, opts.BatchSize)
		}

		if err := c.Exec(ctx, deleteQuery, opts.Args...); err != nil {
			log.Printf("分布式表第 %d 批删除失败: %v", batchNum, err)
			break
		}

		// 检查分布式表是否还有符合条件的数据
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", distributedTableName, opts.Condition)
		row := c.QueryRow(ctx, countQuery, opts.Args...)
		var remaining uint64
		if err := row.Scan(&remaining); err != nil {
			log.Printf("统计分布式表剩余行数失败: %v", err)
			break
		}

		if remaining == 0 {
			if opts.ProgressLog {
				log.Printf("分布式表第 %d 批删除完成，无剩余数据", batchNum)
			}
			break
		}

		totalDeleted += int64(opts.BatchSize)
		batchNum++

		// 如果达到最大行数限制，停止删除
		if opts.MaxRows > 0 && totalDeleted >= int64(opts.MaxRows) {
			break
		}

		// 短暂休息，避免对数据库造成过大压力
		time.Sleep(100 * time.Millisecond)
	}

	return totalDeleted
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

// CleanTableByBillPeriod 按账期清理账单数据
func (c *Client) CleanTableByBillPeriod(ctx context.Context, tableName string, billPeriod string, dryRun bool) (*CleanupResult, error) {
	condition := "bill_period = ?"
	args := []interface{}{billPeriod}

	opts := &CleanupOptions{
		Condition:   condition,
		Args:        args,
		DryRun:      dryRun,
		ProgressLog: true,
	}

	return c.EnhancedCleanTableData(ctx, tableName, opts)
}

// CleanOldData 清理指定天数之前的旧数据
func (c *Client) CleanOldData(ctx context.Context, tableName string, dateColumn string, daysToKeep int, dryRun bool) (*CleanupResult, error) {
	cutoffDate := time.Now().AddDate(0, 0, -daysToKeep)
	condition := fmt.Sprintf("%s < ?", dateColumn)
	args := []interface{}{cutoffDate}

	opts := &CleanupOptions{
		Condition:   condition,
		Args:        args,
		DryRun:      dryRun,
		ProgressLog: true,
		BatchSize:   1000, // 分批删除，避免长时间锁表
	}

	return c.EnhancedCleanTableData(ctx, tableName, opts)
}

// String 返回清理结果的字符串表示
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

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	totalLen := len(sep) * (len(strs) - 1)
	for _, s := range strs {
		totalLen += len(s)
	}

	result := make([]byte, 0, totalLen)
	result = append(result, strs[0]...)

	for i := 1; i < len(strs); i++ {
		result = append(result, sep...)
		result = append(result, strs[i]...)
	}

	return string(result)
}

// TableManagementOptions 表管理选项
type TableManagementOptions struct {
	CheckExists     bool   // 是否检查表是否存在
	BackupTable     string // 备份表名
	CreateIfMissing bool   // 如果表不存在是否创建
	DropIfExists    bool   // 如果表存在是否删除
}

// TableInfo 表信息
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

// ColumnInfo 列信息
type ColumnInfo struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	DefaultKind  string `json:"default_kind,omitempty"`
	DefaultValue string `json:"default_value,omitempty"`
	Comment      string `json:"comment,omitempty"`
	IsNullable   bool   `json:"is_nullable"`
}

// PartitionInfo 分区信息
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

// GetTableInfo 获取表的详细信息
func (c *Client) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	info := &TableInfo{
		Name:       tableName,
		Database:   c.config.Database,
		Columns:    make([]ColumnInfo, 0),
		Properties: make(map[string]string),
	}

	// 获取表基本信息
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

	row := c.QueryRow(ctx, basicQuery, c.config.Database, tableName)
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
		return nil, fmt.Errorf("获取表基本信息失败: %w", err)
	}

	// 解析时间
	if t, err := time.Parse("2006-01-02 15:04:05", metadataTime); err == nil {
		info.LastModified = t
	}

	// 获取列信息
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

	rows, err := c.Query(ctx, columnsQuery, c.config.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %w", err)
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
			return nil, fmt.Errorf("扫描列信息失败: %w", err)
		}

		col.DefaultValue = defaultExpr
		col.Comment = comment
		col.IsNullable = !strings.Contains(col.Type, "Nullable")

		info.Columns = append(info.Columns, col)
	}

	// 获取分区信息（如果是分区表）
	if info.PartitionKey != "" {
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

		partRows, err := c.Query(ctx, partitionsQuery, c.config.Database, tableName)
		if err == nil {
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
				if err == nil {
					part.Active = active == 1
					if t, err := time.Parse("2006-01-02", minDateStr); err == nil {
						part.MinDate = t
					}
					if t, err := time.Parse("2006-01-02", maxDateStr); err == nil {
						part.MaxDate = t
					}
					info.Partitions = append(info.Partitions, part)
				}
			}
		}
	}

	return info, nil
}

// GetTableSchema 获取表的DDL创建语句
func (c *Client) GetTableSchema(ctx context.Context, tableName string) (string, error) {
	query := "SHOW CREATE TABLE " + tableName
	row := c.QueryRow(ctx, query)

	var createStatement string
	err := row.Scan(&createStatement)
	if err != nil {
		return "", fmt.Errorf("获取表结构失败: %w", err)
	}

	return createStatement, nil
}

// BackupTable 备份表数据到新表
func (c *Client) BackupTable(ctx context.Context, sourceTable, backupTable string) error {
	// 获取源表结构
	schema, err := c.GetTableSchema(ctx, sourceTable)
	if err != nil {
		return fmt.Errorf("获取源表结构失败: %w", err)
	}

	// 修改表名创建备份表
	backupSchema := strings.Replace(schema, sourceTable, backupTable, 1)
	if err := c.Exec(ctx, backupSchema); err != nil {
		return fmt.Errorf("创建备份表失败: %w", err)
	}

	// 复制数据
	copyQuery := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", backupTable, sourceTable)
	if err := c.Exec(ctx, copyQuery); err != nil {
		// 如果复制失败，删除备份表
		c.DropTable(ctx, backupTable)
		return fmt.Errorf("复制数据到备份表失败: %w", err)
	}

	log.Printf("表 %s 已备份到 %s", sourceTable, backupTable)
	return nil
}

// OptimizeTable 优化表
func (c *Client) OptimizeTable(ctx context.Context, tableName string, final bool) error {
	var query string
	if final {
		query = fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
	} else {
		query = fmt.Sprintf("OPTIMIZE TABLE %s", tableName)
	}

	if err := c.Exec(ctx, query); err != nil {
		return fmt.Errorf("优化表失败: %w", err)
	}

	log.Printf("表 %s 优化完成", tableName)
	return nil
}

// AnalyzeTable 分析表（更新统计信息）
func (c *Client) AnalyzeTable(ctx context.Context, tableName string) error {
	// ClickHouse 中的 ANALYZE TABLE 主要用于生成列的统计信息
	query := fmt.Sprintf("ANALYZE TABLE %s", tableName)

	if err := c.Exec(ctx, query); err != nil {
		return fmt.Errorf("分析表失败: %w", err)
	}

	log.Printf("表 %s 分析完成", tableName)
	return nil
}

// RenameTable 重命名表
func (c *Client) RenameTable(ctx context.Context, oldName, newName string) error {
	query := fmt.Sprintf("RENAME TABLE %s TO %s", oldName, newName)

	if err := c.Exec(ctx, query); err != nil {
		return fmt.Errorf("重命名表失败: %w", err)
	}

	log.Printf("表 %s 已重命名为 %s", oldName, newName)
	return nil
}

// GetTableSize 获取表的存储大小信息
func (c *Client) GetTableSize(ctx context.Context, tableName string) (map[string]interface{}, error) {
	query := `
		SELECT 
			sum(rows) as total_rows,
			sum(bytes_on_disk) as total_size_bytes,
			sum(data_compressed_bytes) as compressed_size_bytes,
			sum(data_uncompressed_bytes) as uncompressed_size_bytes,
			count() as parts_count
		FROM system.parts 
		WHERE database = ? AND table = ? AND active = 1
	`

	row := c.QueryRow(ctx, query, c.config.Database, tableName)

	var totalRows, totalSize, compressedSize, uncompressedSize, partsCount int64
	err := row.Scan(&totalRows, &totalSize, &compressedSize, &uncompressedSize, &partsCount)
	if err != nil {
		return nil, fmt.Errorf("获取表大小信息失败: %w", err)
	}

	compressionRatio := float64(0)
	if uncompressedSize > 0 {
		compressionRatio = float64(compressedSize) / float64(uncompressedSize)
	}

	return map[string]interface{}{
		"total_rows":              totalRows,
		"total_size_bytes":        totalSize,
		"total_size_mb":           float64(totalSize) / 1024 / 1024,
		"compressed_size_bytes":   compressedSize,
		"compressed_size_mb":      float64(compressedSize) / 1024 / 1024,
		"uncompressed_size_bytes": uncompressedSize,
		"uncompressed_size_mb":    float64(uncompressedSize) / 1024 / 1024,
		"compression_ratio":       compressionRatio,
		"parts_count":             partsCount,
	}, nil
}

// ListTables 列出数据库中的所有表
func (c *Client) ListTables(ctx context.Context) ([]string, error) {
	query := "SELECT name FROM system.tables WHERE database = ? ORDER BY name"
	rows, err := c.Query(ctx, query, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("列出表失败: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("扫描表名失败: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

// GetTablesInfo 获取所有表的基本信息
func (c *Client) GetTablesInfo(ctx context.Context) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			name,
			engine,
			total_rows,
			total_bytes,
			metadata_modification_time
		FROM system.tables 
		WHERE database = ?
		ORDER BY name
	`

	rows, err := c.Query(ctx, query, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("获取表信息失败: %w", err)
	}
	defer rows.Close()

	var tables []map[string]interface{}
	for rows.Next() {
		var name, engine, modTime string
		var totalRows, totalBytes int64

		if err := rows.Scan(&name, &engine, &totalRows, &totalBytes, &modTime); err != nil {
			return nil, fmt.Errorf("扫描表信息失败: %w", err)
		}

		tableInfo := map[string]interface{}{
			"name":          name,
			"engine":        engine,
			"total_rows":    totalRows,
			"total_bytes":   totalBytes,
			"total_mb":      float64(totalBytes) / 1024 / 1024,
			"modified_time": modTime,
		}

		tables = append(tables, tableInfo)
	}

	return tables, rows.Err()
}

// CheckTableHealth 检查表的健康状态
func (c *Client) CheckTableHealth(ctx context.Context, tableName string) (map[string]interface{}, error) {
	healthInfo := make(map[string]interface{})

	// 检查表是否存在
	exists, err := c.TableExists(ctx, tableName)
	if err != nil {
		return nil, err
	}
	healthInfo["exists"] = exists

	if !exists {
		healthInfo["status"] = "NOT_EXISTS"
		return healthInfo, nil
	}

	// 获取表基本信息
	info, err := c.GetTableInfo(ctx, tableName)
	if err != nil {
		healthInfo["status"] = "ERROR"
		healthInfo["error"] = err.Error()
		return healthInfo, nil
	}

	healthInfo["total_rows"] = info.TotalRows
	healthInfo["total_bytes"] = info.TotalBytes
	healthInfo["engine"] = info.Engine

	// 检查是否有损坏的部分
	corruptedQuery := `
		SELECT count() as corrupted_parts
		FROM system.parts 
		WHERE database = ? AND table = ? AND (
			bytes_on_disk = 0 OR 
			rows = 0 OR 
			modification_time < toDate('1970-01-02')
		)
	`

	row := c.QueryRow(ctx, corruptedQuery, c.config.Database, tableName)
	var corruptedParts int64
	if err := row.Scan(&corruptedParts); err == nil {
		healthInfo["corrupted_parts"] = corruptedParts
	}

	// 确定健康状态
	if corruptedParts > 0 {
		healthInfo["status"] = "CORRUPTED"
	} else if info.TotalRows > 0 {
		healthInfo["status"] = "HEALTHY"
	} else {
		healthInfo["status"] = "EMPTY"
	}

	return healthInfo, nil
}

// String 返回表信息的字符串表示
func (ti *TableInfo) String() string {
	return fmt.Sprintf("Table{Name: %s, Engine: %s, Rows: %d, Size: %d bytes, Columns: %d}",
		ti.Name, ti.Engine, ti.TotalRows, ti.TotalBytes, len(ti.Columns))
}

// OptimizedBatchInsert 优化的批量插入方法，支持分块、重试、性能监控
func (c *Client) OptimizedBatchInsert(ctx context.Context, tableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error) {
	if opts == nil {
		opts = DefaultBatchInsertOptions()
	}

	result := &BatchInsertResult{
		TotalRecords: len(data),
		Errors:       make([]error, 0),
	}

	if len(data) == 0 {
		return result, nil
	}

	startTime := time.Now()
	defer func() {
		result.Duration = time.Since(startTime)
		if result.Duration > 0 {
			result.AverageSpeed = float64(result.InsertedRecords) / result.Duration.Seconds()
		}
	}()

	// 分块处理数据
	totalBatches := int(math.Ceil(float64(len(data)) / float64(opts.BatchSize)))

	for i := 0; i < len(data); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		batchNum := (i / opts.BatchSize) + 1

		log.Printf("处理批次 %d/%d, 记录数: %d", batchNum, totalBatches, len(batch))

		// 重试机制
		var batchErr error
		for retry := 0; retry <= opts.MaxRetries; retry++ {
			// 创建带超时的上下文
			batchCtx, cancel := context.WithTimeout(ctx, opts.Timeout)

			if opts.EnableAsync {
				batchErr = c.AsyncInsertBatch(batchCtx, tableName, batch)
			} else {
				batchErr = c.InsertBatch(batchCtx, tableName, batch)
			}

			cancel()

			if batchErr == nil {
				// 成功
				result.ProcessedBatches++
				result.InsertedRecords += len(batch)
				break
			}

			// 失败，记录错误
			if retry == opts.MaxRetries {
				// 最后一次重试也失败了
				result.FailedBatches++
				result.FailedRecords += len(batch)
				result.Errors = append(result.Errors, fmt.Errorf("批次 %d 失败（重试 %d 次后）: %w", batchNum, opts.MaxRetries, batchErr))
				log.Printf("批次 %d 最终失败: %v", batchNum, batchErr)
			} else {
				// 还有重试机会，等待后重试
				log.Printf("批次 %d 失败，%v 后重试（第 %d/%d 次）: %v", batchNum, opts.RetryDelay, retry+1, opts.MaxRetries, batchErr)
				time.Sleep(opts.RetryDelay)
			}
		}
	}

	return result, nil
}

// OptimizedBatchInsertToDistributed 优化的分布式表批量插入
func (c *Client) OptimizedBatchInsertToDistributed(ctx context.Context, distributedTableName string, data []map[string]interface{}, opts *BatchInsertOptions) (*BatchInsertResult, error) {
	if opts == nil {
		opts = DefaultBatchInsertOptions()
	}

	result := &BatchInsertResult{
		TotalRecords: len(data),
		Errors:       make([]error, 0),
	}

	if len(data) == 0 {
		return result, nil
	}

	startTime := time.Now()
	defer func() {
		result.Duration = time.Since(startTime)
		if result.Duration > 0 {
			result.AverageSpeed = float64(result.InsertedRecords) / result.Duration.Seconds()
		}
	}()

	// 分块处理数据
	totalBatches := int(math.Ceil(float64(len(data)) / float64(opts.BatchSize)))

	for i := 0; i < len(data); i += opts.BatchSize {
		end := i + opts.BatchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		batchNum := (i / opts.BatchSize) + 1

		log.Printf("处理分布式表批次 %d/%d, 记录数: %d", batchNum, totalBatches, len(batch))

		// 重试机制
		var batchErr error
		for retry := 0; retry <= opts.MaxRetries; retry++ {
			// 创建带超时的上下文
			batchCtx, cancel := context.WithTimeout(ctx, opts.Timeout)

			if opts.EnableAsync {
				batchErr = c.AsyncInsertBatchToDistributed(batchCtx, distributedTableName, batch)
			} else {
				batchErr = c.InsertBatchToDistributed(batchCtx, distributedTableName, batch)
			}

			cancel()

			if batchErr == nil {
				// 成功
				result.ProcessedBatches++
				result.InsertedRecords += len(batch)
				break
			}

			// 失败，记录错误
			if retry == opts.MaxRetries {
				// 最后一次重试也失败了
				result.FailedBatches++
				result.FailedRecords += len(batch)
				result.Errors = append(result.Errors, fmt.Errorf("分布式表批次 %d 失败（重试 %d 次后）: %w", batchNum, opts.MaxRetries, batchErr))
				log.Printf("分布式表批次 %d 最终失败: %v", batchNum, batchErr)
			} else {
				// 还有重试机会，等待后重试
				log.Printf("分布式表批次 %d 失败，%v 后重试（第 %d/%d 次）: %v", batchNum, opts.RetryDelay, retry+1, opts.MaxRetries, batchErr)
				time.Sleep(opts.RetryDelay)
			}
		}
	}

	return result, nil
}

// String 返回批量插入结果的字符串表示
func (r *BatchInsertResult) String() string {
	status := "SUCCESS"
	if r.FailedBatches > 0 || r.FailedRecords > 0 {
		if r.InsertedRecords > 0 {
			status = "PARTIAL"
		} else {
			status = "FAILED"
		}
	}

	return fmt.Sprintf("BatchInsertResult{Status: %s, Duration: %v, Total: %d, Inserted: %d, Failed: %d, Batches: %d/%d, Speed: %.1f records/s}",
		status, r.Duration, r.TotalRecords, r.InsertedRecords, r.FailedRecords,
		r.ProcessedBatches, r.ProcessedBatches+r.FailedBatches, r.AverageSpeed)
}

// IsSuccess 检查是否全部成功
func (r *BatchInsertResult) IsSuccess() bool {
	return r.FailedRecords == 0 && r.FailedBatches == 0
}

// GetSuccessRate 获取成功率
func (r *BatchInsertResult) GetSuccessRate() float64 {
	if r.TotalRecords == 0 {
		return 0
	}
	return float64(r.InsertedRecords) / float64(r.TotalRecords) * 100
}
