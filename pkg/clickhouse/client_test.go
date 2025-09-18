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

	// Cleanup
	defer client.DropTable(ctx, tableName)

	// Create table
	schema := `(
		id UInt64,
		name String,
		created_at DateTime
	) ENGINE = MergeTree()
	ORDER BY id`

	if err := client.CreateTable(ctx, tableName, schema); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Check if table exists
	exists, err := client.TableExists(ctx, tableName)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}
	if !exists {
		t.Fatal("Table should exist after creation")
	}

	// Insert data
	data := map[string]interface{}{
		"id":         1,
		"name":       "test",
		"created_at": time.Now(),
	}

	if err := client.Insert(ctx, tableName, data); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Batch insert
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

	// Query data
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
	// Create test configuration
	cfg := &config.ClickHouseConfig{
		Hosts:    []string{"localhost"},
		Port:     9000,
		Database: "default",
		Username: "default",
		Password: "",
		Debug:    false,
		Protocol: "native",
	}

	// Create client
	client, err := NewClient(cfg)
	if err != nil {
		t.Skipf("Skip test: unable to connect to ClickHouse: %v", err)
		return
	}
	defer client.Close()

	ctx := context.Background()

	// Test table name
	testTable := "test_cleanup_table"

	// Cleanup test environment
	defer func() {
		client.DropTable(ctx, testTable)
	}()

	// Create test table
	schema := `(
		id UInt64,
		name String,
		value Float64,
		created_at DateTime64(3)
	) ENGINE = MergeTree()
	ORDER BY id`

	err = client.CreateTable(ctx, testTable, schema)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	// Insert test data
	testData := []map[string]interface{}{
		{"id": 1, "name": "test1", "value": 100.0, "created_at": time.Now()},
		{"id": 2, "name": "test2", "value": 200.0, "created_at": time.Now()},
		{"id": 3, "name": "test3", "value": 300.0, "created_at": time.Now()},
	}

	err = client.InsertBatch(ctx, testTable, testData)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	// Test CleanTableData method - delete by condition
	t.Run("TestDeleteByCondition", func(t *testing.T) {
		err := client.CleanTableData(ctx, testTable, "id = ?", 1)
		if err != nil {
			t.Fatalf("failed to clean data by condition: %v", err)
		}

		// Verify data was deleted
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable+" WHERE id = 1")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if count != 0 {
				t.Errorf("expected count after deletion to be 0, got %d", count)
			}
		}
	})

	// Test CleanTableData method - truncate table
	t.Run("TestTruncateTable", func(t *testing.T) {
		// Re-insert data
		err := client.InsertBatch(ctx, testTable, testData)
		if err != nil {
			t.Fatalf("failed to re-insert test data: %v", err)
		}

		// Truncate table
		err = client.CleanTableData(ctx, testTable, "")
		if err != nil {
			t.Fatalf("failed to truncate table: %v", err)
		}

		// Verify table is empty
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if count != 0 {
				t.Errorf("expected count after truncate to be 0, got %d", count)
			}
		}
	})

	// Test TruncateTable method
	t.Run("TestTruncateTableDirect", func(t *testing.T) {
		// Re-insert data
		err := client.InsertBatch(ctx, testTable, testData)
		if err != nil {
			t.Fatalf("failed to re-insert test data: %v", err)
		}

		// Call TruncateTable directly
		err = client.TruncateTable(ctx, testTable)
		if err != nil {
			t.Fatalf("failed to TRUNCATE table: %v", err)
		}

		// Verify table is empty
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if count != 0 {
				t.Errorf("expected count after TRUNCATE to be 0, got %d", count)
			}
		}
	})

	// Test DeleteByCondition method
	t.Run("TestDeleteByConditionDirect", func(t *testing.T) {
		// Re-insert data
		err := client.InsertBatch(ctx, testTable, testData)
		if err != nil {
			t.Fatalf("failed to re-insert test data: %v", err)
		}

		// Call DeleteByCondition directly
		err = client.DeleteByCondition(ctx, testTable, "value > ?", 150.0)
		if err != nil {
			t.Fatalf("failed to delete by condition: %v", err)
		}

		// Verify only data with value <= 150 remains
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable+" WHERE value > 150")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows.Close()

		if rows.Next() {
			var count uint64
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if count != 0 {
				t.Errorf("expected count after deletion to be 0, got %d", count)
			}
		}

		// Verify remaining data count
		rows2, err := client.Query(ctx, "SELECT COUNT(*) FROM "+testTable)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		defer rows2.Close()

		if rows2.Next() {
			var count uint64
			err = rows2.Scan(&count)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if count != 1 {
				t.Errorf("expected remaining count to be 1, got %d", count)
			}
		}
	})
}
