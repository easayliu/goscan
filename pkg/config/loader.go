package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// LoadConfig 从指定路径加载配置文件
func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = getDefaultConfigPath()
	}

	// 如果配置文件不存在，返回默认配置
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return getDefaultConfig(), nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConfigNotFound, err)
	}

	config := &Config{}
	ext := filepath.Ext(configPath)

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("%w: JSON parsing failed: %v", ErrInvalidFormat, err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("%w: YAML parsing failed: %v", ErrInvalidFormat, err)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported config file format: %s", ErrInvalidFormat, ext)
	}

	mergeEnvVars(config)
	return config, nil
}

// SaveConfig 保存配置到指定路径
func SaveConfig(config *Config, configPath string) error {
	if configPath == "" {
		configPath = getDefaultConfigPath()
	}

	// 确保目录存在
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	ext := filepath.Ext(configPath)
	var data []byte
	var err error

	switch ext {
	case ".json":
		data, err = json.MarshalIndent(config, "", "  ")
	case ".yaml", ".yml":
		data, err = yaml.Marshal(config)
	default:
		return fmt.Errorf("%w: unsupported config file format: %s", ErrInvalidFormat, ext)
	}

	if err != nil {
		return fmt.Errorf("config serialization failed: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// getDefaultConfigPath 获取默认配置文件路径
func getDefaultConfigPath() string {
	// 优先级：当前目录 > 用户配置目录 > 系统配置目录
	paths := []string{
		"./config.yaml",
		"./config.json",
	}

	// 检查用户配置目录
	if homeDir, err := os.UserHomeDir(); err == nil {
		paths = append(paths,
			filepath.Join(homeDir, ".goscan", "config.yaml"),
			filepath.Join(homeDir, ".goscan", "config.json"),
		)
	}

	// 检查系统配置目录
	paths = append(paths,
		"/etc/goscan/config.yaml",
		"/etc/goscan/config.json",
	)

	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// 默认返回当前目录的 yaml 配置
	return "./config.yaml"
}

// mergeEnvVars 将环境变量合并到配置中
func mergeEnvVars(config *Config) {
	mergeClickHouseEnvVars(config)
	mergeCloudProviderEnvVars(config)
	mergeServerEnvVars(config)
	mergeSchedulerEnvVars(config)
	mergeRuntimeEnvVars(config)
	mergeWeChatEnvVars(config)
	mergeAppEnvVars(config)
}

// mergeClickHouseEnvVars 合并ClickHouse环境变量
func mergeClickHouseEnvVars(config *Config) {
	if config.ClickHouse == nil {
		config.ClickHouse = NewClickHouseConfig()
		return
	}

	// 优先使用环境变量
	if hostsEnv := os.Getenv("CLICKHOUSE_HOSTS"); hostsEnv != "" {
		config.ClickHouse.Hosts = parseHosts(hostsEnv)
	} else if host := os.Getenv("CLICKHOUSE_HOST"); host != "" {
		config.ClickHouse.Hosts = []string{host}
	}

	envMappings := map[string]interface{}{
		"CLICKHOUSE_PORT":     &config.ClickHouse.Port,
		"CLICKHOUSE_DATABASE": &config.ClickHouse.Database,
		"CLICKHOUSE_USERNAME": &config.ClickHouse.Username,
		"CLICKHOUSE_PASSWORD": &config.ClickHouse.Password,
		"CLICKHOUSE_CLUSTER":  &config.ClickHouse.Cluster,
		"CLICKHOUSE_PROTOCOL": &config.ClickHouse.Protocol,
	}

	for envKey, fieldPtr := range envMappings {
		if value := os.Getenv(envKey); value != "" {
			switch ptr := fieldPtr.(type) {
			case *int:
				if intVal := parseIntOrDefault(value, 0); intVal != 0 {
					*ptr = intVal
				}
			case *string:
				*ptr = value
			}
		}
	}

	if debug := os.Getenv("CLICKHOUSE_DEBUG"); debug != "" {
		config.ClickHouse.Debug = debug == "true" || debug == "1"
	}
}

// mergeServerEnvVars 合并Server环境变量
func mergeServerEnvVars(config *Config) {
	if config.Server == nil {
		config.Server = &ServerConfig{}
	}

	if port := getEnvInt("SERVER_PORT", 0); port != 0 {
		config.Server.Port = port
	}
	if address := os.Getenv("SERVER_ADDRESS"); address != "" {
		config.Server.Address = address
	}
}

// mergeSchedulerEnvVars 合并Scheduler环境变量
func mergeSchedulerEnvVars(config *Config) {
	if config.Scheduler == nil {
		config.Scheduler = &SchedulerConfig{}
	}

	if enabled := os.Getenv("SCHEDULER_ENABLED"); enabled != "" {
		config.Scheduler.Enabled = enabled == "true" || enabled == "1"
	}
}

// mergeRuntimeEnvVars 合并Runtime环境变量
func mergeRuntimeEnvVars(config *Config) {
	if config.Runtime == nil {
		config.Runtime = &RuntimeConfig{}
	}

	if maxTasks := getEnvInt("RUNTIME_MAX_CONCURRENT_TASKS", 0); maxTasks != 0 {
		config.Runtime.MaxConcurrentTasks = maxTasks
	}
	if timeout := getEnvInt("RUNTIME_TASK_TIMEOUT", 0); timeout != 0 {
		config.Runtime.TaskTimeout = timeout
	}
	if shutdownTimeout := getEnvInt("RUNTIME_GRACEFUL_SHUTDOWN_TIMEOUT", 0); shutdownTimeout != 0 {
		config.Runtime.GracefulShutdownTimeout = shutdownTimeout
	}
}

// mergeAppEnvVars 合并App环境变量
func mergeAppEnvVars(config *Config) {
	if config.App == nil {
		config.App = &AppConfig{}
	}

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		config.App.LogLevel = logLevel
	}
	if logFile := os.Getenv("LOG_FILE"); logFile != "" {
		config.App.LogFile = logFile
	}
}

// mergeWeChatEnvVars 合并微信环境变量
func mergeWeChatEnvVars(config *Config) {
	if config.WeChat == nil {
		config.WeChat = NewWeChatConfig()
		return
	}

	wc := config.WeChat

	envMappings := map[string]interface{}{
		"WECHAT_WEBHOOK_URL":         &wc.WebhookURL,
		"WECHAT_MENTION_USERS":       nil, // 特殊处理
		"WECHAT_ALERT_THRESHOLD":     &wc.AlertThreshold,
		"WECHAT_MAX_RETRIES":         &wc.MaxRetries,
		"WECHAT_RETRY_DELAY":         &wc.RetryDelay,
		"WECHAT_NOTIFICATION_FORMAT": &wc.NotificationFormat,
	}

	for envKey, fieldPtr := range envMappings {
		if value := os.Getenv(envKey); value != "" {
			switch envKey {
			case "WECHAT_MENTION_USERS":
				wc.MentionUsers = parseStringList(value)
			default:
				switch ptr := fieldPtr.(type) {
				case *int:
					if intVal := parseIntOrDefault(value, 0); intVal != 0 {
						*ptr = intVal
					}
				case *string:
					*ptr = value
				case *float64:
					if floatVal := parseFloatOrDefault(value, 0); floatVal != 0 {
						*ptr = floatVal
					}
				}
			}
		}
	}

	if enabled := os.Getenv("WECHAT_ENABLED"); enabled != "" {
		wc.Enabled = enabled == "true" || enabled == "1"
	}
}
