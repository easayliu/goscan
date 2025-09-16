package tasks

import "errors"

// 包级错误变量，用于统一错误处理
var (
	// ErrCredentialsNotConfigured 表示云服务凭证未配置
	ErrCredentialsNotConfigured = errors.New("cloud provider credentials not configured")
	
	// ErrTableCreationFailed 表示数据表创建失败
	ErrTableCreationFailed = errors.New("failed to create table")
	
	// ErrSyncFailed 表示数据同步失败
	ErrSyncFailed = errors.New("data synchronization failed")
	
	// ErrTaskNotFound 表示任务未找到
	ErrTaskNotFound = errors.New("task not found")
	
	// ErrTooManyTasks 表示运行的任务过多
	ErrTooManyTasks = errors.New("too many running tasks")
	
	// ErrInvalidTaskConfig 表示任务配置无效
	ErrInvalidTaskConfig = errors.New("invalid task configuration")
	
	// ErrTaskAlreadyRunning 表示任务已在运行
	ErrTaskAlreadyRunning = errors.New("task already running")
	
	// ErrTaskCancelled 表示任务已被取消
	ErrTaskCancelled = errors.New("task was cancelled")
	
	// ErrInvalidPeriod 表示账期参数无效
	ErrInvalidPeriod = errors.New("invalid billing period")
	
	// ErrDataValidationFailed 表示数据校验失败
	ErrDataValidationFailed = errors.New("data validation failed")
	
	// ErrProviderNotSupported 表示不支持的云服务提供商
	ErrProviderNotSupported = errors.New("provider not supported")
	
	// ErrContextCancelled 表示上下文已取消
	ErrContextCancelled = errors.New("context cancelled")
	
	// ErrDatabaseConnection 表示数据库连接失败
	ErrDatabaseConnection = errors.New("database connection failed")
	
	// ErrNotificationFailed 表示通知发送失败
	ErrNotificationFailed = errors.New("notification delivery failed")
)