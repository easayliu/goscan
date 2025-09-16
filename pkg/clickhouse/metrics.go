package clickhouse

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"log"
	"strings"
	"time"
)

// metricsCollector 指标收集器
type metricsCollector struct {
	queryExecutor QueryExecutor
	config        *config.ClickHouseConfig
}

// NewMetricsCollector 创建指标收集器
func NewMetricsCollector(queryExecutor QueryExecutor, config *config.ClickHouseConfig) MetricsCollector {
	return &metricsCollector{
		queryExecutor: queryExecutor,
		config:        config,
	}
}

// GetTableSize 获取表的存储大小信息
func (mc *metricsCollector) GetTableSize(ctx context.Context, tableName string) (map[string]interface{}, error) {
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

	row := mc.queryExecutor.QueryRow(ctx, query, mc.config.Database, tableName)

	var totalRows, totalSize, compressedSize, uncompressedSize, partsCount int64
	err := row.Scan(&totalRows, &totalSize, &compressedSize, &uncompressedSize, &partsCount)
	if err != nil {
		return nil, WrapTableError(tableName, fmt.Errorf("获取表大小信息失败: %w", err))
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

// CheckTableHealth 检查表的健康状态
func (mc *metricsCollector) CheckTableHealth(ctx context.Context, tableName string) (map[string]interface{}, error) {
	healthInfo := make(map[string]interface{})

	// 检查表是否存在
	exists, err := mc.tableExists(ctx, tableName)
	if err != nil {
		return nil, WrapTableError(tableName, err)
	}
	healthInfo["exists"] = exists

	if !exists {
		healthInfo["status"] = "NOT_EXISTS"
		return healthInfo, nil
	}

	// 获取表基本信息
	info, err := mc.getBasicTableInfo(ctx, tableName)
	if err != nil {
		healthInfo["status"] = "ERROR"
		healthInfo["error"] = err.Error()
		return healthInfo, nil
	}

	healthInfo["total_rows"] = info["total_rows"]
	healthInfo["total_bytes"] = info["total_bytes"]
	healthInfo["engine"] = info["engine"]

	// 检查是否有损坏的部分
	corruptedParts, err := mc.getCorruptedPartsCount(ctx, tableName)
	if err == nil {
		healthInfo["corrupted_parts"] = corruptedParts
	}

	// 确定健康状态
	if corruptedParts > 0 {
		healthInfo["status"] = "CORRUPTED"
	} else if info["total_rows"].(int64) > 0 {
		healthInfo["status"] = "HEALTHY"
	} else {
		healthInfo["status"] = "EMPTY"
	}

	return healthInfo, nil
}

// GetTablesInfo 获取所有表的基本信息
func (mc *metricsCollector) GetTablesInfo(ctx context.Context) ([]map[string]interface{}, error) {
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

	rows, err := mc.queryExecutor.Query(ctx, query, mc.config.Database)
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

// tableExists 检查表是否存在
func (mc *metricsCollector) tableExists(ctx context.Context, tableName string) (bool, error) {
	query := "SELECT 1 FROM system.tables WHERE database = ? AND name = ?"
	row := mc.queryExecutor.QueryRow(ctx, query, mc.config.Database, tableName)

	var exists uint8
	err := row.Scan(&exists)
	if err != nil {
		// sql.ErrNoRows 表示表不存在
		if strings.Contains(err.Error(), "no rows") {
			return false, nil
		}
		return false, err
	}

	return exists == 1, nil
}

// getBasicTableInfo 获取表的基本信息
func (mc *metricsCollector) getBasicTableInfo(ctx context.Context, tableName string) (map[string]interface{}, error) {
	query := `
		SELECT 
			engine,
			total_rows,
			total_bytes
		FROM system.tables 
		WHERE database = ? AND name = ?
	`

	row := mc.queryExecutor.QueryRow(ctx, query, mc.config.Database, tableName)

	var engine string
	var totalRows, totalBytes int64
	err := row.Scan(&engine, &totalRows, &totalBytes)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"engine":      engine,
		"total_rows":  totalRows,
		"total_bytes": totalBytes,
	}, nil
}

// getCorruptedPartsCount 获取损坏的分区数量
func (mc *metricsCollector) getCorruptedPartsCount(ctx context.Context, tableName string) (int64, error) {
	corruptedQuery := `
		SELECT count() as corrupted_parts
		FROM system.parts 
		WHERE database = ? AND table = ? AND (
			bytes_on_disk = 0 OR 
			rows = 0 OR 
			modification_time < toDate('1970-01-02')
		)
	`

	row := mc.queryExecutor.QueryRow(ctx, corruptedQuery, mc.config.Database, tableName)
	var corruptedParts int64
	err := row.Scan(&corruptedParts)
	return corruptedParts, err
}

// GetClusterMetrics 获取集群指标信息
func (mc *metricsCollector) GetClusterMetrics(ctx context.Context) (map[string]interface{}, error) {
	if mc.config.Cluster == "" {
		return nil, ErrClusterNotConfigured
	}

	metrics := make(map[string]interface{})

	// 获取集群节点信息
	nodesInfo, err := mc.getClusterNodesInfo(ctx)
	if err != nil {
		log.Printf("获取集群节点信息失败: %v", err)
		metrics["nodes_error"] = err.Error()
	} else {
		metrics["nodes"] = nodesInfo
		metrics["nodes_count"] = len(nodesInfo)
	}

	// 获取集群表统计
	tablesStats, err := mc.getClusterTablesStats(ctx)
	if err != nil {
		log.Printf("获取集群表统计失败: %v", err)
		metrics["tables_error"] = err.Error()
	} else {
		metrics["tables_stats"] = tablesStats
	}

	return metrics, nil
}

// getClusterNodesInfo 获取集群节点信息
func (mc *metricsCollector) getClusterNodesInfo(ctx context.Context) ([]map[string]interface{}, error) {
	query := "SELECT host_name, port, is_local FROM system.clusters WHERE cluster = ?"
	rows, err := mc.queryExecutor.Query(ctx, query, mc.config.Cluster)
	if err != nil {
		return nil, fmt.Errorf("查询集群节点信息失败: %w", err)
	}
	defer rows.Close()

	var nodesInfo []map[string]interface{}
	for rows.Next() {
		var hostName string
		var port uint16
		var isLocal uint8

		if err := rows.Scan(&hostName, &port, &isLocal); err != nil {
			return nil, fmt.Errorf("扫描集群节点信息失败: %w", err)
		}

		nodeInfo := map[string]interface{}{
			"host_name": hostName,
			"port":      port,
			"is_local":  isLocal == 1,
		}

		nodesInfo = append(nodesInfo, nodeInfo)
	}

	return nodesInfo, rows.Err()
}

// getClusterTablesStats 获取集群表统计信息
func (mc *metricsCollector) getClusterTablesStats(ctx context.Context) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			COUNT(*) as total_tables,
			SUM(total_rows) as total_rows,
			SUM(total_bytes) as total_bytes
		FROM clusterAllReplicas(%s, system.tables)
		WHERE database = '%s'`,
		mc.config.Cluster, mc.config.Database)

	row := mc.queryExecutor.QueryRow(ctx, query)

	var totalTables, totalRows, totalBytes int64
	err := row.Scan(&totalTables, &totalRows, &totalBytes)
	if err != nil {
		return nil, fmt.Errorf("查询集群表统计失败: %w", err)
	}

	return map[string]interface{}{
		"total_tables": totalTables,
		"total_rows":   totalRows,
		"total_bytes":  totalBytes,
		"total_mb":     float64(totalBytes) / 1024 / 1024,
	}, nil
}

// GetQueryMetrics 获取查询相关指标
func (mc *metricsCollector) GetQueryMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	// 获取当前正在执行的查询数量
	runningQueries, err := mc.getRunningQueriesCount(ctx)
	if err != nil {
		log.Printf("获取运行中查询数量失败: %v", err)
	} else {
		metrics["running_queries"] = runningQueries
	}

	// 获取查询统计
	queryStats, err := mc.getQueryStats(ctx)
	if err != nil {
		log.Printf("获取查询统计失败: %v", err)
	} else {
		metrics["query_stats"] = queryStats
	}

	return metrics, nil
}

// getRunningQueriesCount 获取正在运行的查询数量
func (mc *metricsCollector) getRunningQueriesCount(ctx context.Context) (int64, error) {
	query := "SELECT COUNT(*) FROM system.processes WHERE query != ''"
	row := mc.queryExecutor.QueryRow(ctx, query)

	var count int64
	err := row.Scan(&count)
	return count, err
}

// getQueryStats 获取查询统计信息
func (mc *metricsCollector) getQueryStats(ctx context.Context) (map[string]interface{}, error) {
	// 这里可以获取更详细的查询统计，比如最近一段时间的查询数量、平均执行时间等
	// 由于system.query_log可能需要特殊配置，这里提供基础实现
	return map[string]interface{}{
		"status": "basic_stats_only",
		"note":   "详细查询统计需要启用query_log",
	}, nil
}

// GetStorageMetrics 获取存储相关指标
func (mc *metricsCollector) GetStorageMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	// 获取磁盘使用情况
	diskUsage, err := mc.getDiskUsage(ctx)
	if err != nil {
		log.Printf("获取磁盘使用情况失败: %v", err)
	} else {
		metrics["disk_usage"] = diskUsage
	}

	// 获取数据库大小
	dbSize, err := mc.getDatabaseSize(ctx)
	if err != nil {
		log.Printf("获取数据库大小失败: %v", err)
	} else {
		metrics["database_size"] = dbSize
	}

	return metrics, nil
}

// getDiskUsage 获取磁盘使用情况
func (mc *metricsCollector) getDiskUsage(ctx context.Context) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			name,
			path,
			free_space,
			total_space,
			used_space
		FROM system.disks
	`

	rows, err := mc.queryExecutor.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询磁盘信息失败: %w", err)
	}
	defer rows.Close()

	var diskInfo []map[string]interface{}
	for rows.Next() {
		var name, path string
		var freeSpace, totalSpace, usedSpace int64

		if err := rows.Scan(&name, &path, &freeSpace, &totalSpace, &usedSpace); err != nil {
			return nil, fmt.Errorf("扫描磁盘信息失败: %w", err)
		}

		usagePercent := float64(0)
		if totalSpace > 0 {
			usagePercent = float64(usedSpace) / float64(totalSpace) * 100
		}

		disk := map[string]interface{}{
			"name":           name,
			"path":           path,
			"free_space":     freeSpace,
			"total_space":    totalSpace,
			"used_space":     usedSpace,
			"usage_percent":  usagePercent,
			"free_space_gb":  float64(freeSpace) / 1024 / 1024 / 1024,
			"total_space_gb": float64(totalSpace) / 1024 / 1024 / 1024,
			"used_space_gb":  float64(usedSpace) / 1024 / 1024 / 1024,
		}

		diskInfo = append(diskInfo, disk)
	}

	return diskInfo, rows.Err()
}

// getDatabaseSize 获取数据库大小
func (mc *metricsCollector) getDatabaseSize(ctx context.Context) (map[string]interface{}, error) {
	query := `
		SELECT 
			SUM(total_bytes) as total_bytes,
			SUM(total_rows) as total_rows,
			COUNT(*) as table_count
		FROM system.tables 
		WHERE database = ?
	`

	row := mc.queryExecutor.QueryRow(ctx, query, mc.config.Database)

	var totalBytes, totalRows, tableCount int64
	err := row.Scan(&totalBytes, &totalRows, &tableCount)
	if err != nil {
		return nil, fmt.Errorf("查询数据库大小失败: %w", err)
	}

	return map[string]interface{}{
		"total_bytes":  totalBytes,
		"total_rows":   totalRows,
		"table_count":  tableCount,
		"total_gb":     float64(totalBytes) / 1024 / 1024 / 1024,
		"avg_table_gb": float64(totalBytes) / float64(tableCount) / 1024 / 1024 / 1024,
	}, nil
}

// GetPerformanceMetrics 获取性能指标
func (mc *metricsCollector) GetPerformanceMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})

	// 获取系统指标
	systemMetrics, err := mc.getSystemMetrics(ctx)
	if err != nil {
		log.Printf("获取系统指标失败: %v", err)
	} else {
		metrics["system"] = systemMetrics
	}

	// 添加时间戳
	metrics["timestamp"] = time.Now().Unix()

	return metrics, nil
}

// getSystemMetrics 获取系统级指标
func (mc *metricsCollector) getSystemMetrics(ctx context.Context) (map[string]interface{}, error) {
	// 这里可以扩展获取更多系统指标
	// 当前提供基础的连接和版本信息
	return map[string]interface{}{
		"database":    mc.config.Database,
		"cluster":     mc.config.Cluster,
		"connections": "active", // 简化表示连接状态
	}, nil
}