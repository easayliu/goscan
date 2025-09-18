package config

import "errors"

// Configuration-related error definitions using sentinel errors pattern
var (
	// Generic errors
	ErrConfigNotFound = errors.New("configuration file not found")
	ErrInvalidFormat  = errors.New("invalid configuration file format")

	// Configuration validation errors
	ErrMissingRequired = errors.New("missing required configuration item")
	ErrInvalidValue    = errors.New("invalid configuration value")

	// Cloud service provider configuration errors
	ErrVolcEngineConfig = errors.New("VolcEngine configuration error")
	ErrAliCloudConfig   = errors.New("AliCloud configuration error")
	ErrAWSConfig        = errors.New("AWS configuration error")
	ErrAzureConfig      = errors.New("azure configuration error")
	ErrGCPConfig        = errors.New("GCP configuration error")

	// Database configuration errors
	ErrClickHouseConfig = errors.New("ClickHouse configuration error")

	// Notification configuration errors
	ErrWeChatConfig = errors.New("WeChat notification configuration error")

	// Scheduler configuration errors
	ErrSchedulerConfig = errors.New("scheduler configuration error")
	ErrInvalidCron     = errors.New("invalid Cron expression")
)
