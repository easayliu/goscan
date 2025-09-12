package clickhouse

import (
	"goscan/pkg/config"
	"testing"
)

func TestTableNameResolver(t *testing.T) {
	tests := []struct {
		name           string
		cluster        string
		baseTableName  string
		expectedQuery  string
		expectedInsert string
		expectedLocal  string
	}{
		{
			name:           "单机模式",
			cluster:        "",
			baseTableName:  "test_table",
			expectedQuery:  "test_table",
			expectedInsert: "test_table",
			expectedLocal:  "test_table",
		},
		{
			name:           "集群模式_基础表名",
			cluster:        "test_cluster",
			baseTableName:  "test_table",
			expectedQuery:  "test_table_distributed",
			expectedInsert: "test_table_distributed",
			expectedLocal:  "test_table_local",
		},
		{
			name:           "集群模式_已有distributed后缀",
			cluster:        "test_cluster",
			baseTableName:  "test_table_distributed",
			expectedQuery:  "test_table_distributed",
			expectedInsert: "test_table_distributed",
			expectedLocal:  "test_table_local",
		},
		{
			name:           "集群模式_已有local后缀",
			cluster:        "test_cluster",
			baseTableName:  "test_table_local",
			expectedQuery:  "test_table_local",
			expectedInsert: "test_table_local",
			expectedLocal:  "test_table_local",
		},
		{
			name:           "阿里云月表",
			cluster:        "log",
			baseTableName:  "alicloud_bill_monthly",
			expectedQuery:  "alicloud_bill_monthly_distributed",
			expectedInsert: "alicloud_bill_monthly_distributed",
			expectedLocal:  "alicloud_bill_monthly_local",
		},
		{
			name:           "火山云账单表",
			cluster:        "log",
			baseTableName:  "volcengine_bill_details",
			expectedQuery:  "volcengine_bill_details_distributed",
			expectedInsert: "volcengine_bill_details_distributed",
			expectedLocal:  "volcengine_bill_details_local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.ClickHouseConfig{
				Cluster: tt.cluster,
			}

			resolver := NewTableNameResolver(cfg)

			// 测试查询目标表名
			queryTarget := resolver.ResolveQueryTarget(tt.baseTableName)
			if queryTarget != tt.expectedQuery {
				t.Errorf("ResolveQueryTarget() = %v, want %v", queryTarget, tt.expectedQuery)
			}

			// 测试插入目标表名
			insertTarget := resolver.ResolveInsertTarget(tt.baseTableName)
			if insertTarget != tt.expectedInsert {
				t.Errorf("ResolveInsertTarget() = %v, want %v", insertTarget, tt.expectedInsert)
			}

			// 测试本地表名
			localTable := resolver.ResolveLocalTableName(tt.baseTableName)
			if localTable != tt.expectedLocal {
				t.Errorf("ResolveLocalTableName() = %v, want %v", localTable, tt.expectedLocal)
			}

			// 测试集群状态
			isClusterEnabled := resolver.IsClusterEnabled()
			expectedClusterEnabled := tt.cluster != ""
			if isClusterEnabled != expectedClusterEnabled {
				t.Errorf("IsClusterEnabled() = %v, want %v", isClusterEnabled, expectedClusterEnabled)
			}
		})
	}
}

func TestTableNameResolver_GetTablePair(t *testing.T) {
	cfg := &config.ClickHouseConfig{
		Cluster: "test_cluster",
	}

	resolver := NewTableNameResolver(cfg)

	distributedTable, localTable := resolver.GetTablePair("test_table")

	if distributedTable != "test_table_distributed" {
		t.Errorf("GetTablePair() distributedTable = %v, want %v", distributedTable, "test_table_distributed")
	}

	if localTable != "test_table_local" {
		t.Errorf("GetTablePair() localTable = %v, want %v", localTable, "test_table_local")
	}
}

func TestTableNameResolver_ValidateTableName(t *testing.T) {
	cfg := &config.ClickHouseConfig{
		Cluster: "test_cluster",
	}

	resolver := NewTableNameResolver(cfg)

	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{"有效表名", "valid_table_name", false},
		{"有效表名_数字", "table_123", false},
		{"空表名", "", true},
		{"数字开头", "123_table", true},
		{"包含特殊字符", "table-name", true},
		{"表名过长", "very_long_table_name_that_exceeds_the_maximum_length_limit_of_64_characters_definitely", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := resolver.ValidateTableName(tt.tableName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTableName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTableNameResolver_GetTableInfo(t *testing.T) {
	cfg := &config.ClickHouseConfig{
		Cluster:  "test_cluster",
		Database: "test_db",
	}

	resolver := NewTableNameResolver(cfg)
	info := resolver.GetTableInfo("test_table")

	expected := map[string]string{
		"base_table":        "test_table",
		"distributed_table": "test_table_distributed",
		"local_table":       "test_table_local",
		"cluster_enabled":   "true",
		"cluster_name":      "test_cluster",
		"insert_target":     "test_table_distributed",
		"query_target":      "test_table_distributed",
	}

	for key, expectedValue := range expected {
		if info[key] != expectedValue {
			t.Errorf("GetTableInfo()[%s] = %v, want %v", key, info[key], expectedValue)
		}
	}
}

func TestTableNameResolver_ResolveCreateTableTarget(t *testing.T) {
	cfg := &config.ClickHouseConfig{
		Cluster: "test_cluster",
	}

	resolver := NewTableNameResolver(cfg)

	// 测试创建分布式表
	distributedTarget := resolver.ResolveCreateTableTarget("test_table", true)
	if distributedTarget != "test_table_distributed" {
		t.Errorf("ResolveCreateTableTarget(distributed=true) = %v, want %v", distributedTarget, "test_table_distributed")
	}

	// 测试创建本地表
	localTarget := resolver.ResolveCreateTableTarget("test_table", false)
	if localTarget != "test_table_local" {
		t.Errorf("ResolveCreateTableTarget(distributed=false) = %v, want %v", localTarget, "test_table_local")
	}

	// 测试单机模式
	singleNodeCfg := &config.ClickHouseConfig{
		Cluster: "",
	}
	singleNodeResolver := NewTableNameResolver(singleNodeCfg)

	singleNodeTarget := singleNodeResolver.ResolveCreateTableTarget("test_table", true)
	if singleNodeTarget != "test_table" {
		t.Errorf("ResolveCreateTableTarget(single_node) = %v, want %v", singleNodeTarget, "test_table")
	}
}
