package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"goscan/pkg/config"
	"log"
	"regexp"
	"strings"
	"time"
)

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

// tableManager 表管理器
type tableManager struct {
	queryExecutor QueryExecutor
	nameResolver  *TableNameResolver
	config        *config.ClickHouseConfig
}

// NewTableManager 创建表管理器
func NewTableManager(queryExecutor QueryExecutor, nameResolver *TableNameResolver, config *config.ClickHouseConfig) TableManager {
	return &tableManager{
		queryExecutor: queryExecutor,
		nameResolver:  nameResolver,
		config:        config,
	}
}

// CreateTable 创建表
func (tm *tableManager) CreateTable(ctx context.Context, tableName string, schema string) error {
	// 对于CreateTable，我们需要明确是创建分布式表还是本地表
	// 默认情况下，如果配置了集群，创建分布式表
	resolvedTableName := tm.nameResolver.ResolveCreateTableTarget(tableName, true)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", resolvedTableName, schema)
	if err := tm.queryExecutor.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// DropTable 删除表
func (tm *tableManager) DropTable(ctx context.Context, tableName string) error {
	// 自动解析表名用于删除操作
	resolvedTableName := tm.nameResolver.ResolveQueryTarget(tableName)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", resolvedTableName)
	if err := tm.queryExecutor.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// TruncateTable 清空表
func (tm *tableManager) TruncateTable(ctx context.Context, tableName string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	if err := tm.queryExecutor.Exec(ctx, query); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// TableExists 检查表是否存在
func (tm *tableManager) TableExists(ctx context.Context, tableName string) (bool, error) {
	// 自动解析表名
	resolvedTableName := tm.nameResolver.ResolveQueryTarget(tableName)

	// 如果配置了集群，使用集群查询
	if tm.config.Cluster != "" {
		return tm.tableExistsInCluster(ctx, resolvedTableName)
	}

	// 单机查询
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

// tableExistsInCluster 检查表在集群中是否存在
func (tm *tableManager) tableExistsInCluster(ctx context.Context, tableName string) (bool, error) {
	// 查询集群中所有节点的表状态
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
		// 如果集群查询失败，回退到简单查询
		log.Printf("[集群表检查] 集群查询失败，回退到简单查询: %v", err)
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
		// 没有节点响应，尝试简单查询
		return tm.simpleTableExists(ctx, tableName)
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

// GetTableInfo 获取表的详细信息
func (tm *tableManager) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	info := &TableInfo{
		Name:       tableName,
		Database:   tm.config.Database,
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
		return nil, WrapTableError(tableName, fmt.Errorf("获取表基本信息失败: %w", err))
	}

	// 解析时间
	if t, err := time.Parse("2006-01-02 15:04:05", metadataTime); err == nil {
		info.LastModified = t
	}

	// 获取列信息
	if err := tm.loadTableColumns(ctx, info); err != nil {
		return nil, err
	}

	// 获取分区信息（如果是分区表）
	if info.PartitionKey != "" {
		if err := tm.loadTablePartitions(ctx, info); err != nil {
			// 分区信息获取失败不影响整体结果，只记录日志
			log.Printf("获取表 %s 分区信息失败: %v", tableName, err)
		}
	}

	return info, nil
}

// loadTableColumns 加载表的列信息
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
		return WrapTableError(info.Name, fmt.Errorf("获取列信息失败: %w", err))
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
			return WrapTableError(info.Name, fmt.Errorf("扫描列信息失败: %w", err))
		}

		col.DefaultValue = defaultExpr
		col.Comment = comment
		col.IsNullable = !strings.Contains(col.Type, "Nullable")

		info.Columns = append(info.Columns, col)
	}

	return rows.Err()
}

// loadTablePartitions 加载表的分区信息
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

// ListTables 列出数据库中的所有表
func (tm *tableManager) ListTables(ctx context.Context) ([]string, error) {
	query := "SELECT name FROM system.tables WHERE database = ? ORDER BY name"
	rows, err := tm.queryExecutor.Query(ctx, query, tm.config.Database)
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

// DropTableOnCluster 在集群上删除表
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

// DropTableSafely 安全删除表（带确认）
func (tm *tableManager) DropTableSafely(ctx context.Context, tableName string, confirmation string) error {
	if confirmation != "YES" {
		return ErrInvalidConfirmation
	}

	// 检查表是否存在
	exists, err := tm.TableExists(ctx, tableName)
	if err != nil {
		return WrapTableError(tableName, fmt.Errorf("检查表是否存在时出错: %w", err))
	}

	if !exists {
		log.Printf("表 %s 不存在，跳过删除", tableName)
		return nil
	}

	// 获取表信息用于日志记录
	info, err := tm.GetTableInfo(ctx, tableName)
	if err == nil {
		log.Printf("即将删除表: %s, 记录数: %d, 大小: %s",
			tableName, info.TotalRows, FormatBytesHumanReadable(uint64(info.TotalBytes)))
	}

	return tm.DropTable(ctx, tableName)
}

// DeleteByCondition 按条件删除数据
func (tm *tableManager) DeleteByCondition(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, condition)
	if err := tm.queryExecutor.Exec(ctx, query, args...); err != nil {
		return WrapTableError(tableName, err)
	}
	return nil
}

// CleanTableData 清理表数据
func (tm *tableManager) CleanTableData(ctx context.Context, tableName string, condition string, args ...interface{}) error {
	if condition == "" {
		return tm.TruncateTable(ctx, tableName)
	}
	return tm.DeleteByCondition(ctx, tableName, condition, args...)
}

// extractPartitionFromCondition 从清理条件中提取分区信息
func (tm *tableManager) extractPartitionFromCondition(condition string) (partition string, canUse bool) {
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
		log.Printf("[分区检测] 匹配到按月条件（阿里云），账期: %s", cycle)
		if len(cycle) == 7 { // YYYY-MM格式
			yearMonth := strings.Replace(cycle, "-", "", 1) // 去掉-
			log.Printf("[分区检测] 提取分区: %s", yearMonth)
			return yearMonth, true
		}
	}

	// 处理火山引擎的ExpenseDate条件：ExpenseDate分区
	if matches := regexp.MustCompile(`ExpenseDate\s+LIKE\s+'(\d{4}-\d{2})%'`).FindStringSubmatch(condition); len(matches) > 1 {
		cycle := matches[1]
		log.Printf("[分区检测] 匹配到ExpenseDate条件（火山引擎），月份: %s", cycle)
		if len(cycle) == 7 { // YYYY-MM格式
			yearMonth := strings.Replace(cycle, "-", "", 1) // 去掉-
			log.Printf("[分区检测] 提取分区: %s", yearMonth)
			return yearMonth, true
		}
	}

	// 处理火山引擎的toYYYYMM(toDate(ExpenseDate))条件
	if matches := regexp.MustCompile(`toYYYYMM\(toDate\(ExpenseDate\)\)\s*=\s*(\d{6})`).FindStringSubmatch(condition); len(matches) > 1 {
		partition := matches[1]
		log.Printf("[分区检测] 匹配到ExpenseDate函数条件（火山引擎），分区: %s", partition)
		if len(partition) == 6 { // YYYYMM格式
			return partition, true
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
func (tm *tableManager) checkPartitionExists(ctx context.Context, tableName, partition string) (bool, error) {
	query := `
		SELECT COUNT(*) 
		FROM system.parts 
		WHERE database = ? AND table = ? AND partition = ? AND active = 1
	`

	row := tm.queryExecutor.QueryRow(ctx, query, tm.config.Database, tableName, partition)
	var count uint64
	err := row.Scan(&count)
	if err != nil {
		return false, WrapPartitionError(partition, fmt.Errorf("failed to check partition existence: %w", err))
	}

	return count > 0, nil
}

// String 返回表信息的字符串表示
func (ti *TableInfo) String() string {
	return fmt.Sprintf("Table{Name: %s, Engine: %s, Rows: %d, Size: %d bytes, Columns: %d}",
		ti.Name, ti.Engine, ti.TotalRows, ti.TotalBytes, len(ti.Columns))
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