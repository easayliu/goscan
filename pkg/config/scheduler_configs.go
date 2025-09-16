package config

import "fmt"

// SchedulerConfig 调度器配置
type SchedulerConfig struct {
	Enabled bool           `json:"enabled" yaml:"enabled"`
	Jobs    []ScheduledJob `json:"jobs" yaml:"jobs"`
}

// ScheduledJob 调度任务配置
type ScheduledJob struct {
	Name     string    `json:"name" yaml:"name"`
	Provider string    `json:"provider" yaml:"provider"`
	Cron     string    `json:"cron" yaml:"cron"`
	Config   JobConfig `json:"config" yaml:"config"`
}

// JobConfig 任务配置
type JobConfig struct {
	SyncMode       string `json:"sync_mode" yaml:"sync_mode"`
	UseDistributed bool   `json:"use_distributed" yaml:"use_distributed"`
	CreateTable    bool   `json:"create_table" yaml:"create_table"`
	ForceUpdate    bool   `json:"force_update" yaml:"force_update"`
	Granularity    string `json:"granularity,omitempty" yaml:"granularity,omitempty"`
}

// RuntimeConfig 运行时配置
type RuntimeConfig struct {
	MaxConcurrentTasks      int `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`
	TaskTimeout             int `json:"task_timeout" yaml:"task_timeout"`                           // seconds
	GracefulShutdownTimeout int `json:"graceful_shutdown_timeout" yaml:"graceful_shutdown_timeout"` // seconds
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port    int    `json:"port" yaml:"port"`
	Address string `json:"address" yaml:"address"`
}

// AppConfig 应用配置
type AppConfig struct {
	LogLevel string `json:"log_level" yaml:"log_level"`
	LogFile  string `json:"log_file" yaml:"log_file"`
}

// NewSchedulerConfig 创建调度器配置，使用环境变量填充默认值
func NewSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		Enabled: getEnvBool("SCHEDULER_ENABLED", true),
		Jobs:    []ScheduledJob{},
	}
}

// NewRuntimeConfig 创建运行时配置，使用环境变量填充默认值
func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		MaxConcurrentTasks:      getEnvInt("RUNTIME_MAX_CONCURRENT_TASKS", 3),
		TaskTimeout:             getEnvInt("RUNTIME_TASK_TIMEOUT", 3600),
		GracefulShutdownTimeout: getEnvInt("RUNTIME_GRACEFUL_SHUTDOWN_TIMEOUT", 30),
	}
}

// NewServerConfig 创建服务器配置，使用环境变量填充默认值
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Port:    getEnvInt("SERVER_PORT", 8080),
		Address: getEnv("SERVER_ADDRESS", "0.0.0.0"),
	}
}

// NewAppConfig 创建应用配置，使用环境变量填充默认值
func NewAppConfig() *AppConfig {
	return &AppConfig{
		LogLevel: getEnv("LOG_LEVEL", "info"),
		LogFile:  getEnv("LOG_FILE", ""),
	}
}

// Validate 验证调度器配置
func (sc *SchedulerConfig) Validate() error {
	if !sc.Enabled {
		return nil // 如果未启用，跳过验证
	}

	for i, job := range sc.Jobs {
		if err := job.Validate(); err != nil {
			return fmt.Errorf("job[%d]: %w", i, err)
		}
	}

	return nil
}

// Validate 验证单个调度任务
func (sj *ScheduledJob) Validate() error {
	if sj.Name == "" {
		return ErrMissingRequired
	}

	if sj.Provider == "" {
		return ErrMissingRequired
	}

	if sj.Cron == "" {
		return ErrMissingRequired
	}

	// 简单的Cron表达式验证
	if !isValidCronExpression(sj.Cron) {
		return ErrInvalidCron
	}

	return sj.Config.Validate()
}

// Validate 验证任务配置
func (jc *JobConfig) Validate() error {
	// 验证同步模式
	if jc.SyncMode != "" {
		validSyncModes := []string{"all_periods", "current_period", "range"}
		if !isValidValue(jc.SyncMode, validSyncModes) {
			return ErrInvalidValue
		}
	}

	// 验证粒度
	if jc.Granularity != "" {
		validGranularities := []string{"monthly", "daily", "both"}
		if !isValidValue(jc.Granularity, validGranularities) {
			return ErrInvalidValue
		}
	}

	return nil
}

// Validate 验证运行时配置
func (rc *RuntimeConfig) Validate() error {
	if rc.MaxConcurrentTasks <= 0 {
		rc.MaxConcurrentTasks = 3
	}

	if rc.TaskTimeout <= 0 {
		rc.TaskTimeout = 3600
	}

	if rc.GracefulShutdownTimeout <= 0 {
		rc.GracefulShutdownTimeout = 30
	}

	return nil
}

// Validate 验证服务器配置
func (sc *ServerConfig) Validate() error {
	if sc.Port <= 0 || sc.Port > 65535 {
		return ErrInvalidValue
	}

	if sc.Address == "" {
		sc.Address = "0.0.0.0"
	}

	return nil
}

// Validate 验证应用配置
func (ac *AppConfig) Validate() error {
	if ac.LogLevel != "" {
		validLevels := []string{"debug", "info", "warn", "error", "fatal"}
		if !isValidValue(ac.LogLevel, validLevels) {
			return ErrInvalidValue
		}
	}

	return nil
}