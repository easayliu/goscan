package alicloud

import (
	"context"
	"time"
)

// BillProvider 账单数据提供者接口
type BillProvider interface {
	// DescribeInstanceBill 获取实例账单
	DescribeInstanceBill(ctx context.Context, req *DescribeInstanceBillRequest) (*DescribeInstanceBillResponse, error)

	// GetAvailableBillingCycles 获取可用的账期列表
	GetAvailableBillingCycles(ctx context.Context) ([]string, error)

	// TestConnection 测试连接
	TestConnection(ctx context.Context) error

	// Close 关闭连接
	Close() error
}

// DataProcessor 数据处理器接口
type DataProcessor interface {
	// ProcessBatch 处理一批账单数据
	ProcessBatch(ctx context.Context, tableName string, bills []BillDetail) error

	// ProcessBatchWithBillingCycle 处理一批账单数据，传入账期信息
	ProcessBatchWithBillingCycle(ctx context.Context, tableName string, bills []BillDetail, billingCycle string) error

	// SetBatchSize 设置批处理大小
	SetBatchSize(size int)

	// GetProcessedCount 获取已处理记录数
	GetProcessedCount() int64

	// GetTotalCount 获取总记录数
	GetTotalCount() int64
}

// BillServiceInterface 账单服务接口
type BillServiceInterface interface {
	// 表管理
	CreateMonthlyBillTable(ctx context.Context) error
	CreateDailyBillTable(ctx context.Context) error
	CreateDistributedMonthlyBillTable(ctx context.Context, localTableName, distributedTableName string) error
	CreateDistributedDailyBillTable(ctx context.Context, localTableName, distributedTableName string) error
	DropTable(ctx context.Context, granularity string) error

	// 数据同步
	SyncMonthlyBillData(ctx context.Context, billingCycle string, options *SyncOptions) error
	SyncDailyBillData(ctx context.Context, billingCycle string, options *SyncOptions) error
	SyncSpecificDayBillData(ctx context.Context, billingDate string, options *SyncOptions) error
	SyncBothGranularityData(ctx context.Context, billingCycle string, options *SyncOptions) error

	// 数据清理
	CleanBillData(ctx context.Context, granularity string, condition string, dryRun bool) error
	CleanSpecificTableData(ctx context.Context, tableName, condition string, dryRun bool) error
	CleanSpecificPeriodData(ctx context.Context, granularity, period string) error

	// 数据检查
	CheckDailyDataExists(ctx context.Context, tableName, billingDate string) (bool, int64, error)
	CheckMonthlyDataExists(ctx context.Context, tableName, billingCycle string) (bool, int64, error)
	CheckDailyDataExistsWithOptimize(ctx context.Context, tableName, billingDate string) (bool, int64, error)
	CheckMonthlyDataExistsWithOptimize(ctx context.Context, tableName, billingCycle string) (bool, int64, error)

	// 数据量获取
	GetDailyAPIDataCount(ctx context.Context, billingDate string) (int32, error)
	GetMonthlyAPIDataCount(ctx context.Context, billingCycle string) (int32, error)
	GetBillDataCount(ctx context.Context, granularity, period string) (int32, error)
	GetDatabaseRecordCount(ctx context.Context, granularity, period string) (int64, error)

	// 智能同步
	PerformDataComparison(ctx context.Context, granularity, period string) (*DataComparisonResult, error)
	PerformPreSyncCheck(ctx context.Context, granularity, period string) (*PreSyncCheckResult, error)
	ExecuteIntelligentCleanupAndSync(ctx context.Context, result *DataComparisonResult, syncOptions *SyncOptions) error

	// 工具方法
	GetTableName(granularity string) string
	GetMonthlyTableName() string
	GetDailyTableName() string
	SetTableNames(monthlyTable, dailyTable string)
	SetDistributedTableNames(monthlyDistributedTable, dailyDistributedTable string)
	GetAvailableBillingCycles(ctx context.Context) ([]string, error)
	TestConnection(ctx context.Context) error
	Close() error
}

// PaginatorInterface 分页器接口
type PaginatorInterface interface {
	// Next 获取下一页数据
	Next(ctx context.Context) (*DescribeInstanceBillResponse, error)

	// HasNext 是否有下一页
	HasNext() bool

	// Reset 重置分页器
	Reset()

	// GetCurrentPage 获取当前页码
	GetCurrentPage() int

	// EstimateTotal 估算总记录数
	EstimateTotal(ctx context.Context) (int32, error)

	// SetProgressCallback 设置进度回调
	SetProgressCallback(callback func(current, total int32))
}

// BatchTransformerInterface 批量转换器接口
type BatchTransformerInterface interface {
	// TransformBatch 转换一批数据
	TransformBatch(bills []BillDetail, granularity string) ([]BillDetail, error)

	// ValidateBatch 验证一批数据
	ValidateBatch(bills []BillDetail) ([]BillDetail, []error)

	// FilterBatch 过滤一批数据
	FilterBatch(bills []BillDetail, filters ...func(*BillDetail) bool) []BillDetail

	// DeduplicateBatch 去重一批数据
	DeduplicateBatch(bills []BillDetail) []BillDetail
}

// RateLimiterInterface 限流器接口
type RateLimiterInterface interface {
	// Wait 等待合适的时间间隔
	Wait(ctx context.Context) error

	// OnSuccess 记录成功的请求
	OnSuccess()

	// OnRateLimit 记录被限流的请求
	OnRateLimit()

	// OnError 记录失败的请求
	OnError(err error)

	// GetCurrentDelay 获取当前延迟
	GetCurrentDelay() time.Duration
}

// Validator 验证器接口
type Validator interface {
	// ValidateBillingCycle 验证账期格式
	ValidateBillingCycle(billingCycle string) error

	// ValidateBillingDate 验证账单日期格式
	ValidateBillingDate(billingDate string) error

	// ValidateGranularity 验证粒度参数
	ValidateGranularity(granularity string) error

	// ValidateRequest 验证请求参数
	ValidateRequest(req *DescribeInstanceBillRequest) error

	// ValidateBillDetail 验证账单明细
	ValidateBillDetail(bill *BillDetail) error

	// ValidateTableName 验证表名格式
	ValidateTableName(tableName string) error
}

// TableManager 表管理器接口
type TableManager interface {
	// CreateTable 创建表
	CreateTable(ctx context.Context, tableName, granularity string) error

	// DropTable 删除表
	DropTable(ctx context.Context, tableName string) error

	// TableExists 检查表是否存在
	TableExists(ctx context.Context, tableName string) (bool, error)

	// GetTableSchema 获取表结构
	GetTableSchema(granularity string) string

	// IsDistributedTable 是否为分布式表
	IsDistributedTable(tableName string) bool
}

// SyncManager 同步管理器接口
type SyncManager interface {
	// SyncData 同步数据
	SyncData(ctx context.Context, req *SyncRequest) (*SyncResult, error)

	// IntelligentSync 智能同步
	IntelligentSync(ctx context.Context, params *IntelligentSyncParams) (*SyncResult, error)

	// PreSyncCheck 同步前检查
	PreSyncCheck(ctx context.Context, granularity, period string) (*PreSyncCheckResult, error)

	// CleanupBeforeSync 同步前清理
	CleanupBeforeSync(ctx context.Context, granularity, period string) error
}

// ProgressCallback 进度回调函数类型
type ProgressCallback func(processed, total int64)

// ErrorHandler 错误处理器接口
type ErrorHandler interface {
	// HandleError 处理错误
	HandleError(ctx context.Context, err error) error

	// ShouldRetry 判断是否应该重试
	ShouldRetry(err error) bool

	// GetRetryDelay 获取重试延迟
	GetRetryDelay(attempt int) time.Duration

	// OnRetry 重试时的回调
	OnRetry(attempt int, err error)
}

// MetricsCollector 指标收集器接口
type MetricsCollector interface {
	// RecordAPICall 记录API调用
	RecordAPICall(method string, duration time.Duration, err error)

	// RecordDataProcessed 记录处理的数据量
	RecordDataProcessed(count int64)

	// RecordError 记录错误
	RecordError(errorType string, err error)

	// GetMetrics 获取指标
	GetMetrics() map[string]interface{}
}

// 便利类型定义
type (
	// SyncRequest 同步请求
	SyncRequest struct {
		Granularity  string       `json:"granularity"`
		Period       string       `json:"period"`
		TableName    string       `json:"table_name,omitempty"`
		Options      *SyncOptions `json:"options,omitempty"`
		ForceCleanup bool         `json:"force_cleanup,omitempty"`
	}

	// SyncResult 同步结果
	SyncResult struct {
		Success        bool          `json:"success"`
		RecordsSync    int64         `json:"records_sync"`
		RecordsCleaned int64         `json:"records_cleaned"`
		Duration       time.Duration `json:"duration"`
		Error          string        `json:"error,omitempty"`
		Details        interface{}   `json:"details,omitempty"`
	}

	// IntelligentSyncParams 智能同步参数
	IntelligentSyncParams struct {
		Granularity       string        `json:"granularity"`
		Period            string        `json:"period"`
		EnablePreCheck    bool          `json:"enable_pre_check"`
		EnableAutoCleanup bool          `json:"enable_auto_cleanup"`
		SyncOptions       *SyncOptions  `json:"sync_options,omitempty"`
		MaxRetries        int           `json:"max_retries,omitempty"`
		RetryDelay        time.Duration `json:"retry_delay,omitempty"`
	}
)
