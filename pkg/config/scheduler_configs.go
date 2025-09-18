package config

import "fmt"

// SchedulerConfig represents the scheduler configuration
type SchedulerConfig struct {
	Enabled bool           `json:"enabled" yaml:"enabled"`
	Jobs    []ScheduledJob `json:"jobs" yaml:"jobs"`
}

// ScheduledJob represents a scheduled job configuration
type ScheduledJob struct {
	Name     string    `json:"name" yaml:"name"`
	Provider string    `json:"provider" yaml:"provider"`
	Cron     string    `json:"cron" yaml:"cron"`
	Config   JobConfig `json:"config" yaml:"config"`
}

// JobConfig represents job-specific configuration
type JobConfig struct {
	SyncMode       string `json:"sync_mode" yaml:"sync_mode"`
	UseDistributed bool   `json:"use_distributed" yaml:"use_distributed"`
	CreateTable    bool   `json:"create_table" yaml:"create_table"`
	ForceUpdate    bool   `json:"force_update" yaml:"force_update"`
	Granularity    string `json:"granularity,omitempty" yaml:"granularity,omitempty"`
}

// RuntimeConfig represents runtime configuration settings
type RuntimeConfig struct {
	MaxConcurrentTasks      int `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`
	TaskTimeout             int `json:"task_timeout" yaml:"task_timeout"`                           // seconds
	GracefulShutdownTimeout int `json:"graceful_shutdown_timeout" yaml:"graceful_shutdown_timeout"` // seconds
}

// ServerConfig represents server configuration settings
type ServerConfig struct {
	Port    int    `json:"port" yaml:"port"`
	Address string `json:"address" yaml:"address"`
}

// AppConfig represents application configuration settings
type AppConfig struct {
	LogLevel    string `json:"log_level" yaml:"log_level"`
	LogFile     string `json:"log_file" yaml:"log_file"`
	Environment string `json:"environment" yaml:"environment"`
	LogPath     string `json:"log_path" yaml:"log_path"`
}

// NewSchedulerConfig creates a scheduler configuration with default values populated from environment variables
func NewSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		Enabled: getEnvBool("SCHEDULER_ENABLED", true),
		Jobs:    []ScheduledJob{},
	}
}

// NewRuntimeConfig creates a runtime configuration with default values populated from environment variables
func NewRuntimeConfig() *RuntimeConfig {
	return &RuntimeConfig{
		MaxConcurrentTasks:      getEnvInt("RUNTIME_MAX_CONCURRENT_TASKS", 3),
		TaskTimeout:             getEnvInt("RUNTIME_TASK_TIMEOUT", 3600),
		GracefulShutdownTimeout: getEnvInt("RUNTIME_GRACEFUL_SHUTDOWN_TIMEOUT", 30),
	}
}

// NewServerConfig creates a server configuration with default values populated from environment variables
func NewServerConfig() *ServerConfig {
	return &ServerConfig{
		Port:    getEnvInt("SERVER_PORT", 8080),
		Address: getEnv("SERVER_ADDRESS", "0.0.0.0"),
	}
}

// NewAppConfig creates an application configuration with default values populated from environment variables
func NewAppConfig() *AppConfig {
	return &AppConfig{
		LogLevel: getEnv("LOG_LEVEL", "info"),
		LogFile:  getEnv("LOG_FILE", ""),
	}
}

// Validate validates the scheduler configuration
func (sc *SchedulerConfig) Validate() error {
	if !sc.Enabled {
		return nil // skip validation if not enabled
	}

	for i, job := range sc.Jobs {
		if err := job.Validate(); err != nil {
			return fmt.Errorf("job[%d]: %w", i, err)
		}
	}

	return nil
}

// Validate validates a single scheduled job
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

	// simple cron expression validation
	if !isValidCronExpression(sj.Cron) {
		return ErrInvalidCron
	}

	return sj.Config.Validate()
}

// Validate validates job configuration
func (jc *JobConfig) Validate() error {
	// validate sync mode
	if jc.SyncMode != "" {
		validSyncModes := []string{"all_periods", "current_period", "range"}
		if !isValidValue(jc.SyncMode, validSyncModes) {
			return ErrInvalidValue
		}
	}

	// validate granularity
	if jc.Granularity != "" {
		validGranularities := []string{"monthly", "daily", "both"}
		if !isValidValue(jc.Granularity, validGranularities) {
			return ErrInvalidValue
		}
	}

	return nil
}

// Validate validates runtime configuration
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

// Validate validates server configuration
func (sc *ServerConfig) Validate() error {
	if sc.Port <= 0 || sc.Port > 65535 {
		return ErrInvalidValue
	}

	if sc.Address == "" {
		sc.Address = "0.0.0.0"
	}

	return nil
}

// Validate validates application configuration
func (ac *AppConfig) Validate() error {
	if ac.LogLevel != "" {
		validLevels := []string{"debug", "info", "warn", "error", "fatal"}
		if !isValidValue(ac.LogLevel, validLevels) {
			return ErrInvalidValue
		}
	}

	return nil
}
