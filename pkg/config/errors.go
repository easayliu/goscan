package config

import "errors"

// 配置相关的错误定义，使用 sentinel errors 模式
var (
	// 通用错误
	ErrConfigNotFound = errors.New("配置文件未找到")
	ErrInvalidFormat  = errors.New("配置文件格式无效")
	
	// 配置验证错误
	ErrMissingRequired = errors.New("缺少必需的配置项")
	ErrInvalidValue    = errors.New("配置值无效")
	
	// 云服务提供商配置错误
	ErrVolcEngineConfig = errors.New("火山引擎配置错误")
	ErrAliCloudConfig   = errors.New("阿里云配置错误")
	ErrAWSConfig        = errors.New("AWS配置错误")
	ErrAzureConfig      = errors.New("Azure配置错误")
	ErrGCPConfig        = errors.New("GCP配置错误")
	
	// 数据库配置错误
	ErrClickHouseConfig = errors.New("ClickHouse配置错误")
	
	// 通知配置错误
	ErrWeChatConfig = errors.New("微信通知配置错误")
	
	// 调度器配置错误
	ErrSchedulerConfig = errors.New("调度器配置错误")
	ErrInvalidCron     = errors.New("无效的Cron表达式")
)