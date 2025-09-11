package clickhouse

import (
	"context"
	"goscan/pkg/config"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	cfg := &config.ClickHouseConfig{
		Hosts:    []string{"localhost"},
		Port:     9000,
		Database: "default",
		Username: "default",
		Password: "",
		Debug:    false,
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Failed to ping ClickHouse: %v", err)
	}
}

func TestTableOperations(t *testing.T) {
	cfg := &config.ClickHouseConfig{
		Hosts:    []string{"localhost"},
		Port:     9000,
		Database: "default",
		Username: "default",
		Password: "",
		Debug:    false,
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	tableName := "test_table"

	// 清理
	defer client.DropTable(ctx, tableName)

	// 创建表
	schema := `(
		id UInt64,
		name String,
		created_at DateTime
	) ENGINE = MergeTree()
	ORDER BY id`

	if err := client.CreateTable(ctx, tableName, schema); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 检查表是否存在
	exists, err := client.TableExists(ctx, tableName)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}
	if !exists {
		t.Fatal("Table should exist after creation")
	}

	// 插入数据
	data := map[string]interface{}{
		"id":         1,
		"name":       "test",
		"created_at": time.Now(),
	}

	if err := client.Insert(ctx, tableName, data); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// 批量插入
	batchData := []map[string]interface{}{
		{
			"id":         2,
			"name":       "test2",
			"created_at": time.Now(),
		},
		{
			"id":         3,
			"name":       "test3",
			"created_at": time.Now(),
		},
	}

	if err := client.InsertBatch(ctx, tableName, batchData); err != nil {
		t.Fatalf("Failed to insert batch: %v", err)
	}

	// 查询数据
	rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+tableName)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	var count uint64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan count: %v", err)
		}
	}

	expectedCount := uint64(3) // 1 single + 2 batch
	if count != expectedCount {
		t.Fatalf("Expected %d records, got %d", expectedCount, count)
	}
}

func TestClientCleanupFunctions(t *testing.T) {
	// 创建测试配置
	cfg := &config.ClickHouseConfig{
		Hosts:    []string{"localhost"},
		Port:     9000,
		Database: "default",
		Username: "default",
		Password: "",
		Debug:    false,
		Protocol: "native",
	}

	// 创建客户端
	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("跳过测试：无法连接到 ClickHouse: %v", err)
		return
	}
	defer client.Close()

	ctx := context.Background()

	// 测试表名
	testTable := "test_cleanup_table"

	// 清理测试环境
	defer func() {
		client.DropTable(ctx, testTable)
	}()

	// 创建测试表
	schema := `(
		id UInt64,
		name String,
		value Float64,
		created_at DateTime64(3)
	) ENGINE = MergeTree()
	ORDER BY id`

	err = client.CreateTable(ctx, testTable, schema)
	if err != nil {
		t.Fatalf("创建测试表失败: %v", err)
	}

	// 插入测试数据
	testData := []map[string]interface{}{
		{"id": 1, "name": "test1", "value": 100.0, "created_at": time.Now()},
		{"id": 2, "name": "test2", "value": 200.0, "created_at": time.Now()},
		{"id": 3, "name": "test3", "value": 300.0, "created_at": time.Now()},
	}

	err = client.InsertBatch(ctx, testTable, testData)
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	// 测试 CleanTableData 方法 - 按条件删除
	t.Run("TestDeleteByCondition", func(t *testing.T) {
		err := client.CleanTableData(ctx, testTable, "id = ?", 1)
		if err != nil {
			t.Fatalf("按条件清理数据失败: %v", err)
		}

		// 验证数据是否被删除
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable+" WHERE id = 1")
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("扫描失败: %v", err)
			}
			if count != 0 {
				t.Errorf("预期删除后数量为 0，实际为 %d", count)
			}
		}
	})

	// 测试 CleanTableData 方法 - 清空表
	t.Run("TestTruncateTable", func(t *testing.T) {
		// 重新插入数据
		err := client.InsertBatch(ctx, testTable, testData)
		if err != nil {
			t.Fatalf("重新插入测试数据失败: %v", err)
		}

		// 清空表
		err = client.CleanTableData(ctx, testTable, "")
		if err != nil {
			t.Fatalf("清空表失败: %v", err)
		}

		// 验证表是否为空
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable)
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("扫描失败: %v", err)
			}
			if count != 0 {
				t.Errorf("预期清空后数量为 0，实际为 %d", count)
			}
		}
	})

	// 测试 TruncateTable 方法
	t.Run("TestTruncateTableDirect", func(t *testing.T) {
		// 重新插入数据
		err := client.InsertBatch(ctx, testTable, testData)
		if err != nil {
			t.Fatalf("重新插入测试数据失败: %v", err)
		}

		// 直接调用 TruncateTable
		err = client.TruncateTable(ctx, testTable)
		if err != nil {
			t.Fatalf("TRUNCATE 表失败: %v", err)
		}

		// 验证表是否为空
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable)
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("扫描失败: %v", err)
			}
			if count != 0 {
				t.Errorf("预期 TRUNCATE 后数量为 0，实际为 %d", count)
			}
		}
	})

	// 测试 DeleteByCondition 方法
	t.Run("TestDeleteByConditionDirect", func(t *testing.T) {
		// 重新插入数据
		err := client.InsertBatch(ctx, testTable, testData)
		if err != nil {
			t.Fatalf("重新插入测试数据失败: %v", err)
		}

		// 直接调用 DeleteByCondition
		err = client.DeleteByCondition(ctx, testTable, "value > ?", 150.0)
		if err != nil {
			t.Fatalf("按条件删除失败: %v", err)
		}

		// 验证只剩下 value <= 150 的数据
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable+" WHERE value > 150")
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("扫描失败: %v", err)
			}
			if count != 0 {
				t.Errorf("预期删除后数量为 0，实际为 %d", count)
			}
		}

		// 验证剩余数据数量
		rows2, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable)
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		defer rows2.Close()

		if rows2.Next() {
			var count uint64
			err = rows2.Scan(&count)
			if err != nil {
				t.Fatalf("扫描失败: %v", err)
			}
			if count != 1 {
				t.Errorf("预期剩余数量为 1，实际为 %d", count)
			}
		}
	})
}
