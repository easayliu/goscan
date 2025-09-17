package analysis

import (
	"context"
	"testing"
	"time"

	"goscan/pkg/config"
)

func TestNewCostAnalyzer(t *testing.T) {
	// Test basic constructor
	analyzer := NewCostAnalyzer(nil)
	if analyzer == nil {
		t.Fatal("NewCostAnalyzer returned nil")
	}

	if analyzer.alertThreshold != 20.0 {
		t.Errorf("expected alert threshold to be 20.0, got %.1f", analyzer.alertThreshold)
	}

	if analyzer.queryTimeout != 30*time.Second {
		t.Errorf("expected query timeout to be 30s, got %v", analyzer.queryTimeout)
	}

	if analyzer.queryCache == nil {
		t.Error("query cache not initialized")
	}

	if analyzer.metrics == nil {
		t.Error("query metrics not initialized")
	}

	// Test resource cleanup
	if err := analyzer.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}
}

func TestNewCostAnalyzerWithConfig_InvalidConfig(t *testing.T) {
	// Test invalid configuration
	_, err := NewCostAnalyzerWithConfig(nil)
	if err == nil {
		t.Error("expected error when config is nil")
	}
}

func TestNewCostAnalyzerWithConfig_ValidConfig(t *testing.T) {
	// Skip tests that require real ClickHouse connection
	if testing.Short() {
		t.Skip("skipping test that requires ClickHouse connection")
	}

	cfg := &config.ClickHouseConfig{
		Hosts:    []string{"localhost"},
		Port:     9000,
		Database: "default",
		Username: "default",
		Password: "",
		Protocol: "native",
		Debug:    false,
	}

	analyzer, err := NewCostAnalyzerWithConfig(cfg)
	if err != nil {
		// Connection failure is expected (no ClickHouse server)
		t.Skipf("skipping test, unable to connect to ClickHouse: %v", err)
	}

	defer analyzer.Close()

	if !analyzer.IsDirectConnectionEnabled() {
		t.Error("expected direct connection to be enabled")
	}

	// Test connection info
	connInfo := analyzer.GetConnectionInfo()
	if connInfo == nil {
		t.Error("connection info is nil")
	}

	if !connInfo["direct_connection"].(bool) {
		t.Error("expected direct connection status to be true")
	}
}

func TestQueryMetrics(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	// Test initial metrics
	metrics := analyzer.GetQueryMetrics()
	if metrics.TotalQueries != 0 {
		t.Errorf("expected initial query count to be 0, got %d", metrics.TotalQueries)
	}

	// Test recording metrics
	analyzer.recordQueryMetrics(100*time.Millisecond, nil)
	metrics = analyzer.GetQueryMetrics()
	if metrics.TotalQueries != 1 {
		t.Errorf("expected query count to be 1, got %d", metrics.TotalQueries)
	}

	if metrics.TotalDuration != 100*time.Millisecond {
		t.Errorf("expected total duration to be 100ms, got %v", metrics.TotalDuration)
	}

	// Test slow query statistics
	analyzer.recordQueryMetrics(2*time.Second, nil)
	metrics = analyzer.GetQueryMetrics()
	if metrics.SlowQueries != 1 {
		t.Errorf("expected slow query count to be 1, got %d", metrics.SlowQueries)
	}

	// Test error statistics
	analyzer.recordQueryMetrics(100*time.Millisecond, &testError{"test error"})
	metrics = analyzer.GetQueryMetrics()
	if metrics.ErrorCount != 1 {
		t.Errorf("expected error count to be 1, got %d", metrics.ErrorCount)
	}
}

func TestQueryCache(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	// Test cache query
	query := "SELECT * FROM test_table"
	key1 := analyzer.cacheQuery(query)
	key2 := analyzer.cacheQuery(query)

	if key1 != key2 {
		t.Error("same query should produce same cache key")
	}

	// Test cache statistics
	cacheStats := analyzer.GetCacheStats()
	if cacheStats["cache_hits"].(int64) != 1 {
		t.Errorf("expected cache hits to be 1, got %v", cacheStats["cache_hits"])
	}

	if cacheStats["cache_misses"].(int64) != 1 {
		t.Errorf("expected cache misses to be 1, got %v", cacheStats["cache_misses"])
	}

	// Test cache cleanup
	analyzer.ClearQueryCache()
	cacheStats = analyzer.GetCacheStats()
	if cacheStats["cache_size"].(int) != 0 {
		t.Errorf("expected cache size to be 0, got %v", cacheStats["cache_size"])
	}
}

func TestSetQueryTimeout(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	// Test setting timeout
	newTimeout := 60 * time.Second
	analyzer.SetQueryTimeout(newTimeout)

	if analyzer.queryTimeout != newTimeout {
		t.Errorf("expected query timeout to be %v, got %v", newTimeout, analyzer.queryTimeout)
	}
}

func TestSetAlertThreshold(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	// Test setting alert threshold
	newThreshold := 25.0
	analyzer.SetAlertThreshold(newThreshold)

	if analyzer.alertThreshold != newThreshold {
		t.Errorf("expected alert threshold to be %.1f, got %.1f", newThreshold, analyzer.alertThreshold)
	}
}

func TestBatchQueryCostData_EmptyTables(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	ctx := context.Background()
	dates := []time.Time{time.Now()}

	// Test empty table list
	data, err := analyzer.BatchQueryCostData(ctx, []DatabaseTableInfo{}, dates)
	if err != nil {
		t.Errorf("empty table list should not return error: %v", err)
	}
	if data != nil {
		t.Error("empty table list should return nil data")
	}
}

// testError is an error type for testing
type testError struct {
	message string
}

func (e *testError) Error() string {
	return e.message
}

func TestSha256Hash(t *testing.T) {
	// Test hash function
	input := "test string"
	hash1 := sha256Hash(input)
	hash2 := sha256Hash(input)

	if hash1 != hash2 {
		t.Error("same input should produce same hash")
	}

	if len(hash1) != 64 { // SHA256 hash length
		t.Errorf("expected hash length to be 64, got %d", len(hash1))
	}

	// Test different inputs produce different hashes
	hash3 := sha256Hash("different string")
	if hash1 == hash3 {
		t.Error("different inputs should produce different hashes")
	}
}

func TestMin(t *testing.T) {
	// Test min function
	tests := []struct {
		a, b     int
		expected int
	}{
		{1, 2, 1},
		{5, 3, 3},
		{10, 10, 10},
		{-1, 0, -1},
	}

	for _, test := range tests {
		result := min(test.a, test.b)
		if result != test.expected {
			t.Errorf("min(%d, %d) = %d, expected %d", test.a, test.b, result, test.expected)
		}
	}
}

func TestContains(t *testing.T) {
	// Test contains function
	slice := []string{"apple", "banana", "cherry"}

	if !contains(slice, "banana") {
		t.Error("expected to contain 'banana'")
	}

	if contains(slice, "orange") {
		t.Error("should not contain 'orange'")
	}

	// Test empty slice
	if contains([]string{}, "test") {
		t.Error("empty slice should not contain any elements")
	}
}

// BenchmarkCacheQuery cache query performance test
func BenchmarkCacheQuery(b *testing.B) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	query := "SELECT * FROM test_table WHERE date = '2024-01-01'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		analyzer.cacheQuery(query)
	}
}

// BenchmarkSha256Hash hash function performance test
func BenchmarkSha256Hash(b *testing.B) {
	input := "SELECT * FROM large_table WHERE complex_condition = 'value'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sha256Hash(input)
	}
}
