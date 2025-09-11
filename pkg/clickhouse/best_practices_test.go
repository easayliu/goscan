package clickhouse

import (
	"context"
	"goscan/pkg/config"
	"testing"
	"time"
)

func TestClientWithBestPractices(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Failed to ping ClickHouse: %v", err)
	}
}

func TestAsyncInsert(t *testing.T) {
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
	tableName := "test_async_table"

	defer client.DropTable(ctx, tableName)

	schema := `(
		id UInt64,
		name String,
		created_at DateTime
	) ENGINE = MergeTree()
	ORDER BY id`

	if err := client.CreateTable(ctx, tableName, schema); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	batchData := []map[string]interface{}{
		{
			"id":         1,
			"name":       "async_test1",
			"created_at": time.Now(),
		},
		{
			"id":         2,
			"name":       "async_test2",
			"created_at": time.Now(),
		},
	}

	if err := client.AsyncInsertBatch(ctx, tableName, batchData); err != nil {
		t.Fatalf("Failed to async insert batch: %v", err)
	}

	// 等待异步插入完成
	time.Sleep(1 * time.Second)

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

	if count != 2 {
		t.Fatalf("Expected 2 records, got %d", count)
	}
}

func TestClusterOperations(t *testing.T) {
	cfg := &config.ClickHouseConfig{
		Hosts:    []string{"localhost"},
		Port:     9000,
		Database: "default",
		Username: "default",
		Password: "",
		Debug:    false,
		Cluster:  "test_cluster_two_shards",
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// 测试获取集群信息（如果集群存在）
	clusterInfo, err := client.GetClusterInfo(ctx)
	if err != nil {
		t.Skipf("Cluster not available: %v", err)
	}

	if len(clusterInfo) == 0 {
		t.Skip("No cluster nodes found")
	}

	t.Logf("Cluster info: %+v", clusterInfo)
}
