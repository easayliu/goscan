package tasks

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// TimeSelectorImpl 时间选择器的具体实现
type TimeSelectorImpl struct{}

// NewTimeSelector 创建新的时间选择器
func NewTimeSelector() *TimeSelectorImpl {
	return &TimeSelectorImpl{}
}

// GetSmartTimeSelection 实现智能时间选择，用于sync-optimal模式
// 根据粒度返回昨天和上月的时间段
func (ts *TimeSelectorImpl) GetSmartTimeSelection(granularity string) *SmartTimeSelection {
	now := time.Now()

	// 计算昨天的日期（用于日数据）
	yesterday := now.AddDate(0, 0, -1)
	yesterdayPeriod := yesterday.Format("2006-01-02")

	// 计算上月的月份（用于月数据）
	lastMonth := now.AddDate(0, -1, 0)
	lastMonthPeriod := lastMonth.Format("2006-01")

	return &SmartTimeSelection{
		YesterdayPeriod: yesterdayPeriod,
		LastMonthPeriod: lastMonthPeriod,
	}
}

// GetRecentMonths 获取最近N个月的月份列表（包括当月）
// 例如：当前是2025-01，n=3 返回 ["2024-11", "2024-12", "2025-01"]
func (ts *TimeSelectorImpl) GetRecentMonths(n int) []string {
	if n <= 0 {
		return []string{}
	}
	
	now := time.Now()
	months := make([]string, 0, n)
	
	// 从最早的月份开始（n-1个月前）
	for i := n - 1; i >= 0; i-- {
		t := now.AddDate(0, -i, 0)
		months = append(months, t.Format("2006-01"))
	}
	
	return months
}

// ValidatePeriod 验证账期格式
func (ts *TimeSelectorImpl) ValidatePeriod(period string) error {
	if period == "" {
		return fmt.Errorf("period cannot be empty")
	}
	
	// 尝试解析不同的时间格式
	layouts := []string{
		"2006-01",    // 月度格式
		"2006-01-02", // 日度格式
	}
	
	for _, layout := range layouts {
		if _, err := time.Parse(layout, period); err == nil {
			return nil
		}
	}
	
	return fmt.Errorf("invalid period format: %s, expected YYYY-MM or YYYY-MM-DD", period)
}

// 包级别的便利函数
var defaultTimeSelector = NewTimeSelector()

// GetSmartTimeSelection 获取智能时间选择（包级别函数）
func GetSmartTimeSelection(granularity string) *SmartTimeSelection {
	return defaultTimeSelector.GetSmartTimeSelection(granularity)
}

// GetRecentMonths 获取最近N个月（包级别函数）
func GetRecentMonths(n int) []string {
	return defaultTimeSelector.GetRecentMonths(n)
}

// ValidatePeriod 验证账期格式（包级别函数）
func ValidatePeriod(period string) error {
	return defaultTimeSelector.ValidatePeriod(period)
}

// CommonSyncHelper 提供通用的同步辅助功能
type CommonSyncHelper struct {
	timeSelector TimeSelector
}

// NewCommonSyncHelper 创建新的同步辅助器
func NewCommonSyncHelper() *CommonSyncHelper {
	return &CommonSyncHelper{
		timeSelector: NewTimeSelector(),
	}
}

// ValidateSyncConfig 验证同步配置
func (h *CommonSyncHelper) ValidateSyncConfig(config *SyncConfig) error {
	if config == nil {
		return fmt.Errorf("sync config cannot be nil")
	}
	
	if config.Provider == "" {
		return fmt.Errorf("provider cannot be empty")
	}
	
	if config.SyncMode == "" {
		return fmt.Errorf("sync mode cannot be empty")
	}
	
	// 验证账期格式
	if config.BillPeriod != "" {
		if err := h.timeSelector.ValidatePeriod(config.BillPeriod); err != nil {
			return fmt.Errorf("invalid bill period: %w", err)
		}
	}
	
	if config.StartPeriod != "" {
		if err := h.timeSelector.ValidatePeriod(config.StartPeriod); err != nil {
			return fmt.Errorf("invalid start period: %w", err)
		}
	}
	
	if config.EndPeriod != "" {
		if err := h.timeSelector.ValidatePeriod(config.EndPeriod); err != nil {
			return fmt.Errorf("invalid end period: %w", err)
		}
	}
	
	return nil
}

// ValidateTableConfig 验证表配置
func (h *CommonSyncHelper) ValidateTableConfig(config *TableConfig) error {
	if config == nil {
		return fmt.Errorf("table config cannot be nil")
	}
	
	if config.UseDistributed {
		if config.LocalTableName == "" {
			return fmt.Errorf("local table name is required for distributed tables")
		}
		if config.DistributedTableName == "" {
			return fmt.Errorf("distributed table name is required for distributed tables")
		}
		if config.ClusterName == "" {
			return fmt.Errorf("cluster name is required for distributed tables")
		}
	}
	
	return nil
}

// LogSyncStart 记录同步开始日志
func (h *CommonSyncHelper) LogSyncStart(ctx context.Context, provider, syncMode string, config *SyncConfig) {
	slog.InfoContext(ctx, "开始数据同步",
		"provider", provider,
		"sync_mode", syncMode,
		"use_distributed", config.UseDistributed,
		"create_table", config.CreateTable,
		"force_update", config.ForceUpdate,
		"bill_period", config.BillPeriod,
		"start_period", config.StartPeriod,
		"end_period", config.EndPeriod,
	)
}

// LogSyncComplete 记录同步完成日志
func (h *CommonSyncHelper) LogSyncComplete(ctx context.Context, result *SyncResult) {
	slog.InfoContext(ctx, "数据同步完成",
		"success", result.Success,
		"records_processed", result.RecordsProcessed,
		"records_fetched", result.RecordsFetched,
		"duration", result.Duration,
		"message", result.Message,
	)
}

// LogSyncError 记录同步错误日志
func (h *CommonSyncHelper) LogSyncError(ctx context.Context, err error, provider string) {
	slog.ErrorContext(ctx, "数据同步失败",
		"provider", provider,
		"error", err.Error(),
	)
}

// CreateSyncResult 创建同步结果
func (h *CommonSyncHelper) CreateSyncResult(success bool, recordsProcessed, recordsFetched int, duration time.Duration, message string, err error) *SyncResult {
	result := &SyncResult{
		Success:          success,
		RecordsProcessed: recordsProcessed,
		RecordsFetched:   recordsFetched,
		Duration:         duration,
		Message:          message,
		StartedAt:        time.Now().Add(-duration),
		CompletedAt:      time.Now(),
		Metadata:         make(map[string]interface{}),
	}
	
	if err != nil {
		result.Error = err.Error()
	}
	
	return result
}

// SanitizeTableName 清理表名，确保符合数据库命名规范
func (h *CommonSyncHelper) SanitizeTableName(tableName string) string {
	// 移除特殊字符，只保留字母、数字和下划线
	result := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, tableName)
	
	// 确保以字母开头
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "t_" + result
	}
	
	return result
}

// CalculateSyncProgress 计算同步进度
func (h *CommonSyncHelper) CalculateSyncProgress(current, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(current) / float64(total) * 100
}

// 包级别的便利实例
var defaultSyncHelper = NewCommonSyncHelper()

// ValidateSyncConfig 验证同步配置（包级别函数）
func ValidateSyncConfig(config *SyncConfig) error {
	return defaultSyncHelper.ValidateSyncConfig(config)
}

// ValidateTableConfig 验证表配置（包级别函数）
func ValidateTableConfig(config *TableConfig) error {
	return defaultSyncHelper.ValidateTableConfig(config)
}