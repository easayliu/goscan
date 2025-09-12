package clickhouse

import (
	"fmt"
	"goscan/pkg/config"
	"strings"
)

// TableNameResolver 表名解析器，负责根据集群配置自动解析表名
type TableNameResolver struct {
	config *config.ClickHouseConfig
}

// NewTableNameResolver 创建表名解析器
func NewTableNameResolver(config *config.ClickHouseConfig) *TableNameResolver {
	return &TableNameResolver{
		config: config,
	}
}

// ResolveTableName 解析表名
// 规则：
// 1. 如果配置了 cluster，且表名不以 _distributed 结尾，则自动添加 _distributed 后缀
// 2. 如果未配置 cluster，则返回原始表名
// 3. 如果表名已经包含 _distributed 或 _local 后缀，则不进行转换
func (r *TableNameResolver) ResolveTableName(baseTableName string) string {
	// 如果未配置集群，返回原始表名
	if r.config.Cluster == "" {
		return baseTableName
	}

	// 如果表名已经包含分布式表或本地表标识，不进行转换
	if strings.HasSuffix(baseTableName, "_distributed") ||
		strings.HasSuffix(baseTableName, "_local") {
		return baseTableName
	}

	// 配置了集群且表名是基础表名，自动添加 _distributed 后缀
	return baseTableName + "_distributed"
}

// ResolveLocalTableName 解析本地表名（用于分布式表场景）
// 规则：
// 1. 如果配置了 cluster，且表名不以 _local 结尾，则自动添加 _local 后缀
// 2. 如果表名以 _distributed 结尾，则替换为 _local
// 3. 如果未配置 cluster，返回原始表名
func (r *TableNameResolver) ResolveLocalTableName(baseTableName string) string {
	// 如果未配置集群，返回原始表名
	if r.config.Cluster == "" {
		return baseTableName
	}

	// 如果表名以 _distributed 结尾，替换为 _local
	if strings.HasSuffix(baseTableName, "_distributed") {
		return strings.TrimSuffix(baseTableName, "_distributed") + "_local"
	}

	// 如果表名已经以 _local 结尾，保持不变
	if strings.HasSuffix(baseTableName, "_local") {
		return baseTableName
	}

	// 基础表名，添加 _local 后缀
	return baseTableName + "_local"
}

// GetTablePair 获取表名对（分布式表名，本地表名）
// 返回值：distributedTableName, localTableName
func (r *TableNameResolver) GetTablePair(baseTableName string) (string, string) {
	distributedTable := r.ResolveTableName(baseTableName)
	localTable := r.ResolveLocalTableName(baseTableName)
	return distributedTable, localTable
}

// IsClusterEnabled 检查是否启用了集群模式
func (r *TableNameResolver) IsClusterEnabled() bool {
	return r.config.Cluster != ""
}

// GetClusterName 获取集群名称
func (r *TableNameResolver) GetClusterName() string {
	return r.config.Cluster
}

// ResolveCreateTableTarget 解析建表目标表名
// 在集群模式下：
// - 如果是分布式表操作，返回分布式表名
// - 如果是本地表操作，返回本地表名
func (r *TableNameResolver) ResolveCreateTableTarget(baseTableName string, useDistributed bool) string {
	if !r.IsClusterEnabled() {
		return baseTableName
	}

	if useDistributed {
		return r.ResolveTableName(baseTableName)
	}

	return r.ResolveLocalTableName(baseTableName)
}

// ResolveInsertTarget 解析插入目标表名
// 规则：在集群模式下总是返回分布式表名，这样数据会自动路由到各个节点
func (r *TableNameResolver) ResolveInsertTarget(baseTableName string) string {
	return r.ResolveTableName(baseTableName)
}

// ResolveQueryTarget 解析查询目标表名
// 规则：在集群模式下总是返回分布式表名，这样能查询所有节点的数据
func (r *TableNameResolver) ResolveQueryTarget(baseTableName string) string {
	return r.ResolveTableName(baseTableName)
}

// ValidateTableName 验证表名是否符合命名规范
func (r *TableNameResolver) ValidateTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// 检查表名长度
	if len(tableName) > 64 {
		return fmt.Errorf("table name too long (max 64 characters): %s", tableName)
	}

	// 检查是否包含非法字符
	for _, char := range tableName {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_') {
			return fmt.Errorf("table name contains invalid character '%c': %s", char, tableName)
		}
	}

	// 检查首字符不能是数字
	if tableName[0] >= '0' && tableName[0] <= '9' {
		return fmt.Errorf("table name cannot start with a number: %s", tableName)
	}

	return nil
}

// GetTableInfo 获取表名信息（用于调试和日志）
func (r *TableNameResolver) GetTableInfo(baseTableName string) map[string]string {
	distributedTable, localTable := r.GetTablePair(baseTableName)

	info := map[string]string{
		"base_table":        baseTableName,
		"distributed_table": distributedTable,
		"local_table":       localTable,
		"cluster_enabled":   fmt.Sprintf("%t", r.IsClusterEnabled()),
		"cluster_name":      r.GetClusterName(),
		"insert_target":     r.ResolveInsertTarget(baseTableName),
		"query_target":      r.ResolveQueryTarget(baseTableName),
	}

	return info
}
