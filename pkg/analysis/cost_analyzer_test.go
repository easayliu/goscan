package analysis

import (
	"context"
	"testing"
	"time"

	"goscan/pkg/config"
)

func TestNewCostAnalyzer(t *testing.T) {
	// 测试基础构造函数
	analyzer := NewCostAnalyzer(nil)
	if analyzer == nil {
		t.Fatal("NewCostAnalyzer 返回了 nil")
	}

	if analyzer.alertThreshold != 20.0 {
		t.Errorf("期望告警阈值为 20.0，实际为 %.1f", analyzer.alertThreshold)
	}

	if analyzer.queryTimeout != 30*time.Second {
		t.Errorf("期望查询超时为 30s，实际为 %v", analyzer.queryTimeout)
	}

	if analyzer.queryCache == nil {
		t.Error("查询缓存未初始化")
	}

	if analyzer.metrics == nil {
		t.Error("查询指标未初始化")
	}

	// 测试资源清理
	if err := analyzer.Close(); err != nil {
		t.Errorf("Close() 失败: %v", err)
	}
}

func TestNewCostAnalyzerWithConfig_InvalidConfig(t *testing.T) {
	// 测试无效配置
	_, err := NewCostAnalyzerWithConfig(nil)
	if err == nil {
		t.Error("期望配置为空时返回错误")
	}
}

func TestNewCostAnalyzerWithConfig_ValidConfig(t *testing.T) {
	// 跳过需要真实 ClickHouse 连接的测试
	if testing.Short() {
		t.Skip("跳过需要 ClickHouse 连接的测试")
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
		// 如果连接失败是正常的（没有 ClickHouse 服务器）
		t.Skipf("跳过测试，无法连接到 ClickHouse: %v", err)
	}

	defer analyzer.Close()

	if !analyzer.IsDirectConnectionEnabled() {
		t.Error("期望启用直接连接")
	}

	// 测试连接信息
	connInfo := analyzer.GetConnectionInfo()
	if connInfo == nil {
		t.Error("连接信息为空")
	}

	if !connInfo["direct_connection"].(bool) {
		t.Error("期望直接连接状态为 true")
	}
}

func TestQueryMetrics(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	// 测试初始指标
	metrics := analyzer.GetQueryMetrics()
	if metrics.TotalQueries != 0 {
		t.Errorf("期望初始查询数为 0，实际为 %d", metrics.TotalQueries)
	}

	// 测试记录指标
	analyzer.recordQueryMetrics(100*time.Millisecond, nil)
	metrics = analyzer.GetQueryMetrics()
	if metrics.TotalQueries != 1 {
		t.Errorf("期望查询数为 1，实际为 %d", metrics.TotalQueries)
	}

	if metrics.TotalDuration != 100*time.Millisecond {
		t.Errorf("期望总耗时为 100ms，实际为 %v", metrics.TotalDuration)
	}

	// 测试慢查询统计
	analyzer.recordQueryMetrics(2*time.Second, nil)
	metrics = analyzer.GetQueryMetrics()
	if metrics.SlowQueries != 1 {
		t.Errorf("期望慢查询数为 1，实际为 %d", metrics.SlowQueries)
	}

	// 测试错误统计
	analyzer.recordQueryMetrics(100*time.Millisecond, &testError{"测试错误"})
	metrics = analyzer.GetQueryMetrics()
	if metrics.ErrorCount != 1 {
		t.Errorf("期望错误数为 1，实际为 %d", metrics.ErrorCount)
	}
}

func TestQueryCache(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	// 测试缓存查询
	query := "SELECT * FROM test_table"
	key1 := analyzer.cacheQuery(query)
	key2 := analyzer.cacheQuery(query)

	if key1 != key2 {
		t.Error("相同查询应该产生相同的缓存键")
	}

	// 测试缓存统计
	cacheStats := analyzer.GetCacheStats()
	if cacheStats["cache_hits"].(int64) != 1 {
		t.Errorf("期望缓存命中数为 1，实际为 %v", cacheStats["cache_hits"])
	}

	if cacheStats["cache_misses"].(int64) != 1 {
		t.Errorf("期望缓存未命中数为 1，实际为 %v", cacheStats["cache_misses"])
	}

	// 测试清理缓存
	analyzer.ClearQueryCache()
	cacheStats = analyzer.GetCacheStats()
	if cacheStats["cache_size"].(int) != 0 {
		t.Errorf("期望缓存大小为 0，实际为 %v", cacheStats["cache_size"])
	}
}

func TestSetQueryTimeout(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	// 测试设置超时
	newTimeout := 60 * time.Second
	analyzer.SetQueryTimeout(newTimeout)

	if analyzer.queryTimeout != newTimeout {
		t.Errorf("期望查询超时为 %v，实际为 %v", newTimeout, analyzer.queryTimeout)
	}
}

func TestSetAlertThreshold(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	// 测试设置告警阈值
	newThreshold := 25.0
	analyzer.SetAlertThreshold(newThreshold)

	if analyzer.alertThreshold != newThreshold {
		t.Errorf("期望告警阈值为 %.1f，实际为 %.1f", newThreshold, analyzer.alertThreshold)
	}
}

func TestBatchQueryCostData_EmptyTables(t *testing.T) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	ctx := context.Background()
	dates := []time.Time{time.Now()}

	// 测试空表列表
	data, err := analyzer.BatchQueryCostData(ctx, []DatabaseTableInfo{}, dates)
	if err != nil {
		t.Errorf("空表列表不应返回错误: %v", err)
	}
	if data != nil {
		t.Error("空表列表应返回 nil 数据")
	}
}

// testError 用于测试的错误类型
type testError struct {
	message string
}

func (e *testError) Error() string {
	return e.message
}

func TestSha256Hash(t *testing.T) {
	// 测试哈希函数
	input := "test string"
	hash1 := sha256Hash(input)
	hash2 := sha256Hash(input)

	if hash1 != hash2 {
		t.Error("相同输入应产生相同哈希")
	}

	if len(hash1) != 64 { // SHA256 哈希长度
		t.Errorf("期望哈希长度为 64，实际为 %d", len(hash1))
	}

	// 测试不同输入产生不同哈希
	hash3 := sha256Hash("different string")
	if hash1 == hash3 {
		t.Error("不同输入应产生不同哈希")
	}
}

func TestMin(t *testing.T) {
	// 测试 min 函数
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
			t.Errorf("min(%d, %d) = %d，期望 %d", test.a, test.b, result, test.expected)
		}
	}
}

func TestContains(t *testing.T) {
	// 测试 contains 函数
	slice := []string{"apple", "banana", "cherry"}

	if !contains(slice, "banana") {
		t.Error("期望包含 'banana'")
	}

	if contains(slice, "orange") {
		t.Error("不期望包含 'orange'")
	}

	// 测试空切片
	if contains([]string{}, "test") {
		t.Error("空切片不应包含任何元素")
	}
}

// BenchmarkCacheQuery 缓存查询性能测试
func BenchmarkCacheQuery(b *testing.B) {
	analyzer := NewCostAnalyzer(nil)
	defer analyzer.Close()

	query := "SELECT * FROM test_table WHERE date = '2024-01-01'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		analyzer.cacheQuery(query)
	}
}

// BenchmarkSha256Hash 哈希函数性能测试
func BenchmarkSha256Hash(b *testing.B) {
	input := "SELECT * FROM large_table WHERE complex_condition = 'value'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sha256Hash(input)
	}
}
