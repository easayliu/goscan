package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/logger"
	"strings"
	"time"

	"go.uber.org/zap"
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

// LogSyncStart logs the beginning of a sync
func (h *CommonSyncHelper) LogSyncStart(ctx context.Context, provider, syncMode string, config *SyncConfig) {
	logger.Info("Starting data synchronization",
		zap.String("provider", provider),
		zap.String("sync_mode", syncMode),
		zap.Bool("use_distributed", config.UseDistributed),
		zap.Bool("create_table", config.CreateTable),
		zap.Bool("force_update", config.ForceUpdate),
		zap.String("bill_period", config.BillPeriod),
		zap.String("start_period", config.StartPeriod),
		zap.String("end_period", config.EndPeriod),
	)
}

// LogSyncComplete logs that the sync finished
func (h *CommonSyncHelper) LogSyncComplete(ctx context.Context, result *SyncResult) {
	logger.Info("Data synchronization completed",
		zap.Bool("success", result.Success),
		zap.Int("records_processed", result.RecordsProcessed),
		zap.Int("records_fetched", result.RecordsFetched),
		zap.Duration("duration", result.Duration),
		zap.String("message", result.Message),
	)
}

// LogSyncError logs sync failures
func (h *CommonSyncHelper) LogSyncError(ctx context.Context, err error, provider string) {
	logger.Error("Data synchronization failed",
		zap.String("provider", provider),
		zap.Error(err),
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

// DataValidationRule represents a single data validation rule
type DataValidationRule struct {
	Name        string // Name of the validation rule
	Description string // Human-readable description
	Query       string // SQL query to execute for validation
	Expected    int    // Expected result count (0 for issues, >0 for existence checks)
	Severity    string // "error", "warning", "info"
}

// DataValidationProvider interface for provider-specific validation rules
type DataValidationProvider interface {
	// GetValidationRules returns validation rules for given periods
	GetValidationRules(monthlyPeriods, dailyPeriods []string) []DataValidationRule
}

// CommonDataValidator provides common data validation functionality
type CommonDataValidator struct {
	chClient *clickhouse.Client
}

// NewCommonDataValidator creates a new data validator
func NewCommonDataValidator(chClient *clickhouse.Client) *CommonDataValidator {
	return &CommonDataValidator{
		chClient: chClient,
	}
}

// PerformValidation executes data validation using provided rules
func (v *CommonDataValidator) PerformValidation(ctx context.Context, provider DataValidationProvider, monthlyPeriods, dailyPeriods []string) (*DataCheckResult, error) {
	startTime := time.Now()
	
	// Get validation rules from provider
	rules := provider.GetValidationRules(monthlyPeriods, dailyPeriods)
	if len(rules) == 0 {
		return &DataCheckResult{
			Success:      true,
			TotalRecords: 0,
			ChecksPassed: 0,
			ChecksFailed: 0,
			CheckTime:    startTime,
			Details:      map[string]interface{}{"message": "no validation rules defined"},
		}, nil
	}

	var issues []string
	checksPassed := 0
	checksFailed := 0
	details := make(map[string]interface{})

	// Execute each validation rule
	for _, rule := range rules {
		logger.Info("executing validation rule",
			zap.String("name", rule.Name),
			zap.String("description", rule.Description),
			zap.String("severity", rule.Severity))

		result, err := v.executeValidationQuery(ctx, rule.Query)
		if err != nil {
			issue := fmt.Sprintf("Rule '%s' failed to execute: %v", rule.Name, err)
			issues = append(issues, issue)
			checksFailed++
			
			logger.Error("validation rule execution failed",
				zap.String("rule", rule.Name),
				zap.Error(err))
			continue
		}

		// Check if result matches expected value
		if result == rule.Expected {
			checksPassed++
			logger.Info("validation rule passed",
				zap.String("rule", rule.Name),
				zap.Int("result", result),
				zap.Int("expected", rule.Expected))
		} else {
			issue := fmt.Sprintf("%s (%s): expected %d, got %d", 
				rule.Name, rule.Severity, rule.Expected, result)
			issues = append(issues, issue)
			
			if rule.Severity == "error" {
				checksFailed++
			} else {
				checksPassed++ // Warnings don't count as failures
			}
			
			logger.Warn("validation rule failed",
				zap.String("rule", rule.Name),
				zap.String("severity", rule.Severity),
				zap.Int("result", result),
				zap.Int("expected", rule.Expected))
		}

		// Store detailed results
		details[rule.Name] = map[string]interface{}{
			"description": rule.Description,
			"result":      result,
			"expected":    rule.Expected,
			"severity":    rule.Severity,
			"passed":      result == rule.Expected,
		}
	}

	// Determine overall success
	success := checksFailed == 0

	return &DataCheckResult{
		Success:      success,
		TotalRecords: len(rules),
		ChecksPassed: checksPassed,
		ChecksFailed: checksFailed,
		Issues:       issues,
		CheckTime:    startTime,
		Details:      details,
	}, nil
}

// executeValidationQuery executes a validation query and returns the count result
func (v *CommonDataValidator) executeValidationQuery(ctx context.Context, query string) (int, error) {
	// Wrap the entire query to cast result to UInt64 for consistent type handling
	wrappedQuery := fmt.Sprintf("SELECT CAST((%s) AS UInt64)", strings.TrimSpace(query))
	
	rows, err := v.chClient.Query(ctx, wrappedQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to execute validation query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, fmt.Errorf("validation query returned no results")
	}

	var result uint64
	if err := rows.Scan(&result); err != nil {
		return 0, fmt.Errorf("failed to scan validation result: %w", err)
	}

	return int(result), nil
}

// CommonDataCleaner provides common data cleanup functionality
type CommonDataCleaner struct {
	chClient *clickhouse.Client
}

// NewCommonDataCleaner creates a new data cleaner
func NewCommonDataCleaner(chClient *clickhouse.Client) *CommonDataCleaner {
	return &CommonDataCleaner{
		chClient: chClient,
	}
}

// CleanPeriodData cleans data for specified period using partition drop or DELETE
func (c *CommonDataCleaner) CleanPeriodData(ctx context.Context, tableName, period, conditionField, provider string) error {
	resolver := c.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(tableName)
	
	// Try partition drop first (more efficient)
	partitionValue, canUsePartition := c.calculatePartitionValue(period, conditionField, provider)
	if canUsePartition {
		return c.dropPartitionByValue(ctx, actualTableName, partitionValue, provider, period)
	}
	
	// Fallback to condition-based cleanup
	condition := fmt.Sprintf("%s = '%s'", conditionField, period)
	return c.conditionBasedCleanup(ctx, actualTableName, condition, provider, period)
}

// calculatePartitionValue calculates partition value for DROP PARTITION operation
func (c *CommonDataCleaner) calculatePartitionValue(period, conditionField, provider string) (string, bool) {
	switch provider {
	case "alicloud":
		if conditionField == "billing_cycle" {
			// Monthly table: PARTITION BY toYYYYMM(parseDateTimeBestEffort(billing_cycle || '-01'))
			// period = "2025-08" -> partition = "202508"
			return strings.ReplaceAll(period, "-", ""), true
		} else if conditionField == "billing_date" {
			// Daily table: PARTITION BY toYYYYMMDD(billing_date)
			// period = "2025-09-16" -> partition = "20250916"
			return strings.ReplaceAll(period, "-", ""), true
		}
	case "volcengine":
		if conditionField == "BillPeriod" {
			// VolcEngine table: PARTITION BY toYYYYMM(toDate(ExpenseDate))
			// period = "2025-08" -> partition = "202508"
			return strings.ReplaceAll(period, "-", ""), true
		}
	}
	return "", false
}

// dropPartitionByValue drops partition by specific partition value
func (c *CommonDataCleaner) dropPartitionByValue(ctx context.Context, tableName, partitionValue, provider, period string) error {
	clusterName := c.chClient.GetClusterName()
	
	// Build DROP PARTITION SQL
	var dropSQL string
	if clusterName != "" {
		// Get local table name for distributed setup
		localTableName := tableName
		if strings.Contains(tableName, "_distributed") {
			localTableName = strings.Replace(tableName, "_distributed", "_local", 1)
		}
		dropSQL = fmt.Sprintf("ALTER TABLE %s ON CLUSTER %s DROP PARTITION '%s'", localTableName, clusterName, partitionValue)
	} else {
		dropSQL = fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", tableName, partitionValue)
	}
	
	logger.Info("dropping partition for period cleanup",
		zap.String("provider", provider),
		zap.String("period", period),
		zap.String("partition", partitionValue),
		zap.String("sql", dropSQL))
	
	err := c.chClient.Exec(ctx, dropSQL)
	if err != nil {
		logger.Error("partition drop failed",
			zap.String("provider", provider),
			zap.String("period", period),
			zap.Error(err))
		return fmt.Errorf("failed to drop partition %s: %w", partitionValue, err)
	}
	
	logger.Info("partition drop completed",
		zap.String("provider", provider),
		zap.String("period", period),
		zap.String("partition", partitionValue))
	
	return nil
}

// conditionBasedCleanup uses condition-based DELETE as fallback
func (c *CommonDataCleaner) conditionBasedCleanup(ctx context.Context, tableName, condition, provider, period string) error {
	clusterName := c.chClient.GetClusterName()
	
	// Build DELETE SQL
	var deleteSQL string
	if clusterName != "" {
		deleteSQL = fmt.Sprintf("ALTER TABLE %s ON CLUSTER %s DELETE WHERE %s", tableName, clusterName, condition)
	} else {
		deleteSQL = fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s", tableName, condition)
	}
	
	logger.Info("using DELETE for period cleanup",
		zap.String("provider", provider),
		zap.String("period", period),
		zap.String("sql", deleteSQL))
	
	err := c.chClient.Exec(ctx, deleteSQL)
	if err != nil {
		return fmt.Errorf("condition-based cleanup failed: %w", err)
	}
	
	logger.Info("DELETE cleanup completed",
		zap.String("provider", provider),
		zap.String("period", period))
	
	return nil
}

