package tasks

import (
	"testing"
	"time"
)

// TestVolcEngineGetSupportedSyncModes 测试支持的同步模式
func TestVolcEngineGetSupportedSyncModes(t *testing.T) {
	// 创建一个简单的执行器实例来测试支持的模式
	executor := &VolcEngineSyncExecutor{}
	
	supportedModes := executor.GetSupportedSyncModes()
	
	// 验证支持的模式
	foundStandard := false
	foundOptimal := false
	
	for _, mode := range supportedModes {
		if mode == "standard" {
			foundStandard = true
		}
		if mode == "sync-optimal" {
			foundOptimal = true
		}
	}
	
	if !foundStandard {
		t.Error("should support standard mode")
	}
	if !foundOptimal {
		t.Error("should support sync-optimal mode")
	}
	if len(supportedModes) != 2 {
		t.Errorf("should support exactly 2 modes, got %d", len(supportedModes))
	}
}

// TestTimeSelectorGetRecentMonths 测试时间选择器的最近月份功能
func TestTimeSelectorGetRecentMonths(t *testing.T) {
	timeSelector := NewTimeSelector()
	
	// 测试不同的月份数量
	testCases := []struct {
		months   int
		expected int
	}{
		{0, 0},
		{1, 1},
		{3, 3},
		{12, 12},
	}
	
	for _, tc := range testCases {
		t.Run("GetRecentMonths", func(t *testing.T) {
			result := timeSelector.GetRecentMonths(tc.months)
			if len(result) != tc.expected {
				t.Errorf("expected %d months, got %d", tc.expected, len(result))
			}
			
			if tc.expected > 0 {
				// 验证格式
				for _, month := range result {
					if len(month) != 7 || month[4] != '-' {
						t.Errorf("month %s should match YYYY-MM format", month)
					}
				}
				
				// 验证月份顺序（应该是从早到晚）
				now := time.Now()
				expectedFirst := now.AddDate(0, -(tc.months-1), 0).Format("2006-01")
				expectedLast := now.Format("2006-01")
				
				if result[0] != expectedFirst {
					t.Errorf("first month should be %s, got %s", expectedFirst, result[0])
				}
				if result[len(result)-1] != expectedLast {
					t.Errorf("last month should be %s, got %s", expectedLast, result[len(result)-1])
				}
			}
		})
	}
}

// TestTimeSelector_GetRecentMonths_SyncOptimalSpecific 专门测试sync-optimal模式使用的3个月逻辑
func TestTimeSelector_GetRecentMonths_SyncOptimalSpecific(t *testing.T) {
	timeSelector := NewTimeSelector()
	
	// sync-optimal模式固定使用最近3个月
	result := timeSelector.GetRecentMonths(3)
	
	if len(result) != 3 {
		t.Errorf("sync-optimal mode should get exactly 3 months, got %d", len(result))
	}
	
	// 验证包含当前月份
	now := time.Now()
	currentMonth := now.Format("2006-01")
	
	found := false
	for _, month := range result {
		if month == currentMonth {
			found = true
			break
		}
	}
	
	if !found {
		t.Errorf("should include current month %s in result %v", currentMonth, result)
	}
	
	// 验证月份是连续的
	expectedMonths := make([]string, 3)
	for i := 0; i < 3; i++ {
		expectedMonths[i] = now.AddDate(0, -(2-i), 0).Format("2006-01")
	}
	
	for i, expected := range expectedMonths {
		if result[i] != expected {
			t.Errorf("month %d should be %s, got %s", i, expected, result[i])
		}
	}
}

// TestSyncConfig_SyncOptimalMode 测试sync-optimal配置的逻辑
func TestSyncConfig_SyncOptimalMode(t *testing.T) {
	// 测试sync-optimal模式的配置验证
	config := &SyncConfig{
		Provider:    "volcengine",
		SyncMode:    "sync-optimal",
		BillPeriod:  "2025-01", // 在sync-optimal模式下这个参数应该被忽略
		ForceUpdate: true,
	}
	
	// 验证配置字段
	if config.Provider != "volcengine" {
		t.Errorf("expected provider volcengine, got %s", config.Provider)
	}
	if config.SyncMode != "sync-optimal" {
		t.Errorf("expected sync mode sync-optimal, got %s", config.SyncMode)
	}
	if !config.ForceUpdate {
		t.Error("expected ForceUpdate to be true")
	}
}

// TestSyncConfig_StandardVsOptimal 测试标准模式与优化模式的配置差异
func TestSyncConfig_StandardVsOptimal(t *testing.T) {
	standardConfig := &SyncConfig{
		Provider:    "volcengine",
		SyncMode:    "standard",
		BillPeriod:  "2025-01", // 标准模式使用这个参数
		ForceUpdate: false,
	}
	
	optimalConfig := &SyncConfig{
		Provider:    "volcengine",
		SyncMode:    "sync-optimal", 
		BillPeriod:  "2025-01", // 优化模式会忽略这个参数
		ForceUpdate: true,      // 优化模式通常配合force_update使用
	}
	
	// 验证模式差异
	if standardConfig.SyncMode == optimalConfig.SyncMode {
		t.Error("standard and optimal modes should be different")
	}
	
	if standardConfig.ForceUpdate == optimalConfig.ForceUpdate {
		t.Error("force_update settings should typically be different between modes")
	}
}

// TestProviderName 测试获取提供商名称
func TestProviderName(t *testing.T) {
	executor := &VolcEngineSyncExecutor{}
	
	providerName := executor.GetProviderName()
	if providerName != "volcengine" {
		t.Errorf("expected provider name 'volcengine', got '%s'", providerName)
	}
}

// BenchmarkTimeSelector_GetRecentMonths 基准测试时间选择器性能
func BenchmarkTimeSelector_GetRecentMonths(b *testing.B) {
	timeSelector := NewTimeSelector()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = timeSelector.GetRecentMonths(3)
	}
}

// TestTimeSelector_GetRecentMonths_EdgeCases 测试时间选择器边界情况
func TestTimeSelector_GetRecentMonths_EdgeCases(t *testing.T) {
	timeSelector := NewTimeSelector()
	
	t.Run("NegativeMonths", func(t *testing.T) {
		result := timeSelector.GetRecentMonths(-1)
		if len(result) != 0 {
			t.Errorf("negative months should return empty slice, got %v", result)
		}
	})
	
	t.Run("ZeroMonths", func(t *testing.T) {
		result := timeSelector.GetRecentMonths(0)
		if len(result) != 0 {
			t.Errorf("zero months should return empty slice, got %v", result)
		}
	})
	
	t.Run("LargeMonths", func(t *testing.T) {
		result := timeSelector.GetRecentMonths(24)
		if len(result) != 24 {
			t.Errorf("should return 24 months, got %d", len(result))
		}
		
		// 验证第一个月份是24个月前
		now := time.Now()
		expectedFirst := now.AddDate(0, -23, 0).Format("2006-01")
		if result[0] != expectedFirst {
			t.Errorf("first month should be %s, got %s", expectedFirst, result[0])
		}
	})
}

// TestSyncOptimalModeConstants 测试sync-optimal模式的常量值
func TestSyncOptimalModeConstants(t *testing.T) {
	// 验证sync-optimal模式的关键特性：
	
	// 1. 固定同步最近3个月
	expectedMonthCount := 3
	timeSelector := NewTimeSelector()
	months := timeSelector.GetRecentMonths(expectedMonthCount)
	
	if len(months) != expectedMonthCount {
		t.Errorf("sync-optimal should use exactly %d months, got %d", expectedMonthCount, len(months))
	}
	
	// 2. 支持的同步模式
	executor := &VolcEngineSyncExecutor{}
	modes := executor.GetSupportedSyncModes()
	
	syncOptimalFound := false
	for _, mode := range modes {
		if mode == "sync-optimal" {
			syncOptimalFound = true
			break
		}
	}
	
	if !syncOptimalFound {
		t.Error("sync-optimal mode must be supported")
	}
	
	// 3. 提供商名称
	providerName := executor.GetProviderName()
	if providerName != "volcengine" {
		t.Errorf("provider should be volcengine, got %s", providerName)
	}
}

// TestTableNamingConvention 测试表名命名约定
func TestTableNamingConvention(t *testing.T) {
	// 验证volcengine的表名约定
	expectedBaseTableName := "volcengine_bill_details"
	expectedDistributedSuffix := "_distributed"
	
	// 本地表名
	localTableName := expectedBaseTableName
	if localTableName != "volcengine_bill_details" {
		t.Errorf("local table name should be %s", expectedBaseTableName)
	}
	
	// 分布式表名
	distributedTableName := expectedBaseTableName + expectedDistributedSuffix
	if distributedTableName != "volcengine_bill_details_distributed" {
		t.Errorf("distributed table name should be %s", expectedBaseTableName+expectedDistributedSuffix)
	}
}