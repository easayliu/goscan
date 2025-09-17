package tasks

import (
	"context"
)

// SyncExecutor 定义数据同步执行器的核心接口
// 每个云服务提供商需要实现这个接口
type SyncExecutor interface {
	// ValidateConfig 验证配置是否正确
	ValidateConfig(ctx context.Context, config *SyncConfig) error

	// CreateTables 创建数据表（支持本地表和分布式表）
	CreateTables(ctx context.Context, config *TableConfig) error

	// ExecuteSync 执行数据同步
	ExecuteSync(ctx context.Context, config *SyncConfig) (*SyncResult, error)

	// PerformDataCheck 执行数据校验
	PerformDataCheck(ctx context.Context, period string) (*DataCheckResult, error)

	// GetProviderName 获取提供商名称
	GetProviderName() string

	// GetSupportedSyncModes 获取支持的同步模式
	GetSupportedSyncModes() []string
}

// SyncExecutorFactory 定义同步执行器工厂接口
type SyncExecutorFactory interface {
	// CreateExecutor 根据提供商名称创建对应的执行器
	CreateExecutor(ctx context.Context, provider string) (SyncExecutor, error)

	// GetSupportedProviders 获取支持的提供商列表
	GetSupportedProviders() []string

	// RegisterExecutor 注册新的执行器
	RegisterExecutor(provider string, creator ExecutorCreator) error
}

// ExecutorCreator 定义执行器创建函数类型
type ExecutorCreator func(ctx context.Context) (SyncExecutor, error)

// TaskExecutor 定义任务执行器接口
type TaskExecutor interface {
	// Execute 执行任务
	Execute(ctx context.Context, task *Task) (*TaskResult, error)

	// Cancel 取消任务
	Cancel(ctx context.Context, taskID string) error

	// GetStatus 获取任务状态
	GetStatus(ctx context.Context, taskID string) (*Task, error)
}

// NotificationExecutor 定义通知执行器接口
type NotificationExecutor interface {
	// SendNotification 发送通知
	SendNotification(ctx context.Context, config *TaskConfig) (*TaskResult, error)

	// ValidateNotificationConfig 验证通知配置
	ValidateNotificationConfig(ctx context.Context, config *TaskConfig) error
}

// DataValidator 定义数据验证器接口
type DataValidator interface {
	// ValidateData 验证数据完整性
	ValidateData(ctx context.Context, period string) (*DataComparisonResult, error)

	// PerformPreSyncCheck 执行同步前检查
	PerformPreSyncCheck(ctx context.Context, config *SyncConfig) (*PreSyncCheckResult, error)

	// CompareData 比较数据
	CompareData(ctx context.Context, sourceData, targetData interface{}) (*DataComparisonResult, error)
}

// TaskManager 定义任务管理器接口
type TaskManager interface {
	// ExecuteTask 执行任务
	ExecuteTask(ctx context.Context, req *TaskRequest) (*TaskResult, error)

	// GetTask 获取任务信息
	GetTask(taskID string) (*Task, error)

	// GetTasks 获取所有任务
	GetTasks() []*Task

	// GetTaskHistory 获取任务历史
	GetTaskHistory() []*Task

	// CancelTask 取消任务
	CancelTask(taskID string) error

	// GetRunningTaskCount 获取运行中的任务数量
	GetRunningTaskCount() int

	// GetTotalTaskCount 获取总任务数量
	GetTotalTaskCount() int
}

// TimeSelector 定义时间选择器接口
type TimeSelector interface {
	// GetSmartTimeSelection 获取智能时间选择
	GetSmartTimeSelection(granularity string) *SmartTimeSelection

	// GetRecentMonths 获取最近N个月
	GetRecentMonths(n int) []string

	// ValidatePeriod 验证账期格式
	ValidatePeriod(period string) error
}
