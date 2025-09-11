package config

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

type Config struct {
	ClickHouse     *ClickHouseConfig     `json:"clickhouse" yaml:"clickhouse"`
	CloudProviders *CloudProvidersConfig `json:"cloud_providers" yaml:"cloud_providers"`
	Server         *ServerConfig         `json:"server" yaml:"server"`
	Scheduler      *SchedulerConfig      `json:"scheduler" yaml:"scheduler"`
	Runtime        *RuntimeConfig        `json:"runtime" yaml:"runtime"`
	WeChat         *WeChatConfig         `json:"wechat" yaml:"wechat"`
	App            *AppConfig            `json:"app" yaml:"app"`
}

type CloudProvidersConfig struct {
	VolcEngine *VolcEngineConfig `json:"volcengine" yaml:"volcengine"`
	AliCloud   *AliCloudConfig   `json:"alicloud" yaml:"alicloud"`
	AWS        *AWSConfig        `json:"aws" yaml:"aws"`
	Azure      *AzureConfig      `json:"azure" yaml:"azure"`
	GCP        *GCPConfig        `json:"gcp" yaml:"gcp"`
}

type ServerConfig struct {
	Port    int    `json:"port" yaml:"port"`
	Address string `json:"address" yaml:"address"`
}

type SchedulerConfig struct {
	Enabled bool           `json:"enabled" yaml:"enabled"`
	Jobs    []ScheduledJob `json:"jobs" yaml:"jobs"`
}

type ScheduledJob struct {
	Name     string    `json:"name" yaml:"name"`
	Provider string    `json:"provider" yaml:"provider"`
	Cron     string    `json:"cron" yaml:"cron"`
	Config   JobConfig `json:"config" yaml:"config"`
}

type JobConfig struct {
	SyncMode       string `json:"sync_mode" yaml:"sync_mode"`
	UseDistributed bool   `json:"use_distributed" yaml:"use_distributed"`
	CreateTable    bool   `json:"create_table" yaml:"create_table"`
	ForceUpdate    bool   `json:"force_update" yaml:"force_update"`
	Granularity    string `json:"granularity,omitempty" yaml:"granularity,omitempty"`
}

type RuntimeConfig struct {
	MaxConcurrentTasks      int `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`
	TaskTimeout             int `json:"task_timeout" yaml:"task_timeout"`                           // seconds
	GracefulShutdownTimeout int `json:"graceful_shutdown_timeout" yaml:"graceful_shutdown_timeout"` // seconds
}

type AppConfig struct {
	LogLevel string `json:"log_level" yaml:"log_level"`
	LogFile  string `json:"log_file" yaml:"log_file"`
}

type VolcEngineConfig struct {
	AccessKey  string `json:"access_key" yaml:"access_key"`
	SecretKey  string `json:"secret_key" yaml:"secret_key"`
	Region     string `json:"region" yaml:"region"`
	Host       string `json:"host" yaml:"host"`
	Timeout    int    `json:"timeout" yaml:"timeout"`         // seconds
	BatchSize  int    `json:"batch_size" yaml:"batch_size"`   // API批量获取大小
	MaxRetries int    `json:"max_retries" yaml:"max_retries"` // 最大重试次数
	RetryDelay int    `json:"retry_delay" yaml:"retry_delay"` // 重试延迟(秒)

	// === 历史数据同步配置 ===
	DefaultSyncMode     string `json:"default_sync_mode" yaml:"default_sync_mode"`         // all_periods, current_period, range
	MaxHistoricalMonths int    `json:"max_historical_months" yaml:"max_historical_months"` // 最多同步几个月的历史数据，0表示无限制
	AutoDetectPeriods   bool   `json:"auto_detect_periods" yaml:"auto_detect_periods"`     // 是否自动检测可用账期
	DefaultStartPeriod  string `json:"default_start_period" yaml:"default_start_period"`   // 默认开始账期 YYYY-MM
	DefaultEndPeriod    string `json:"default_end_period" yaml:"default_end_period"`       // 默认结束账期 YYYY-MM
	SkipEmptyPeriods    bool   `json:"skip_empty_periods" yaml:"skip_empty_periods"`       // 是否跳过空账期
}

type AWSConfig struct {
	AccessKey string `json:"access_key" yaml:"access_key"`
	SecretKey string `json:"secret_key" yaml:"secret_key"`
	Region    string `json:"region" yaml:"region"`
	Timeout   int    `json:"timeout" yaml:"timeout"` // seconds
}

type AzureConfig struct {
	ClientID       string `json:"client_id" yaml:"client_id"`
	ClientSecret   string `json:"client_secret" yaml:"client_secret"`
	TenantID       string `json:"tenant_id" yaml:"tenant_id"`
	SubscriptionID string `json:"subscription_id" yaml:"subscription_id"`
	Timeout        int    `json:"timeout" yaml:"timeout"` // seconds
}

type AliCloudConfig struct {
	AccessKeyID     string `json:"access_key_id" yaml:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret" yaml:"access_key_secret"`
	Region          string `json:"region" yaml:"region"`
	Endpoint        string `json:"endpoint" yaml:"endpoint"`
	Timeout         int    `json:"timeout" yaml:"timeout"`         // seconds
	BatchSize       int    `json:"batch_size" yaml:"batch_size"`   // API批量获取大小
	MaxRetries      int    `json:"max_retries" yaml:"max_retries"` // 最大重试次数
	RetryDelay      int    `json:"retry_delay" yaml:"retry_delay"` // 重试延迟(秒)

	// === 粒度配置 ===
	DefaultGranularity string `json:"default_granularity" yaml:"default_granularity"` // monthly, daily, both
	EnableDailySync    bool   `json:"enable_daily_sync" yaml:"enable_daily_sync"`     // 是否启用按天同步
	DailySyncDays      int    `json:"daily_sync_days" yaml:"daily_sync_days"`         // 按天同步最近N天

	// === 表名配置 ===
	MonthlyTable string `json:"monthly_table" yaml:"monthly_table"`
	DailyTable   string `json:"daily_table" yaml:"daily_table"`

	// === 历史数据同步配置 ===
	DefaultSyncMode     string `json:"default_sync_mode" yaml:"default_sync_mode"`         // all_periods, current_period, range
	MaxHistoricalMonths int    `json:"max_historical_months" yaml:"max_historical_months"` // 最多同步几个月的历史数据，阿里云支持18个月
	AutoDetectPeriods   bool   `json:"auto_detect_periods" yaml:"auto_detect_periods"`     // 是否自动检测可用账期
	DefaultStartPeriod  string `json:"default_start_period" yaml:"default_start_period"`   // 默认开始账期 YYYY-MM
	DefaultEndPeriod    string `json:"default_end_period" yaml:"default_end_period"`       // 默认结束账期 YYYY-MM
	SkipEmptyPeriods    bool   `json:"skip_empty_periods" yaml:"skip_empty_periods"`       // 是否跳过空账期
}

type GCPConfig struct {
	ProjectID         string `json:"project_id" yaml:"project_id"`
	ServiceAccountKey string `json:"service_account_key" yaml:"service_account_key"`
	Timeout           int    `json:"timeout" yaml:"timeout"` // seconds
}

type WeChatConfig struct {
	WebhookURL         string   `json:"webhook_url" yaml:"webhook_url"`                 // 企业微信机器人webhook地址
	Enabled            bool     `json:"enabled" yaml:"enabled"`                         // 是否启用微信通知
	MentionUsers       []string `json:"mention_users" yaml:"mention_users"`             // @用户列表
	AlertThreshold     float64  `json:"alert_threshold" yaml:"alert_threshold"`         // 告警阈值（百分比）
	SendTime           string   `json:"send_time" yaml:"send_time"`                     // 发送时间（cron格式）
	MaxRetries         int      `json:"max_retries" yaml:"max_retries"`                 // 最大重试次数
	RetryDelay         int      `json:"retry_delay" yaml:"retry_delay"`                 // 重试延迟（秒）
	NotificationFormat string   `json:"notification_format" yaml:"notification_format"` // 通知格式：markdown/template_card/auto
}

func NewVolcEngineConfig() *VolcEngineConfig {
	return &VolcEngineConfig{
		AccessKey:  getEnv("VOLCENGINE_ACCESS_KEY", ""),
		SecretKey:  getEnv("VOLCENGINE_SECRET_KEY", ""),
		Region:     getEnv("VOLCENGINE_REGION", "cn-north-1"),
		Host:       getEnv("VOLCENGINE_HOST", "billing.volcengineapi.com"),
		Timeout:    getEnvInt("VOLCENGINE_TIMEOUT", 30),
		BatchSize:  getEnvInt("VOLCENGINE_BATCH_SIZE", 100),
		MaxRetries: getEnvInt("VOLCENGINE_MAX_RETRIES", 3),
		RetryDelay: getEnvInt("VOLCENGINE_RETRY_DELAY", 2),

		// 历史数据同步配置默认值
		DefaultSyncMode:     getEnv("VOLCENGINE_DEFAULT_SYNC_MODE", "all_periods"),
		MaxHistoricalMonths: getEnvInt("VOLCENGINE_MAX_HISTORICAL_MONTHS", 12),
		AutoDetectPeriods:   getEnvBool("VOLCENGINE_AUTO_DETECT_PERIODS", true),
		DefaultStartPeriod:  getEnv("VOLCENGINE_DEFAULT_START_PERIOD", ""),
		DefaultEndPeriod:    getEnv("VOLCENGINE_DEFAULT_END_PERIOD", ""),
		SkipEmptyPeriods:    getEnvBool("VOLCENGINE_SKIP_EMPTY_PERIODS", true),
	}
}

func NewAliCloudConfig() *AliCloudConfig {
	return &AliCloudConfig{
		AccessKeyID:     getEnv("ALICLOUD_ACCESS_KEY_ID", ""),
		AccessKeySecret: getEnv("ALICLOUD_ACCESS_KEY_SECRET", ""),
		Region:          getEnv("ALICLOUD_REGION", "cn-hangzhou"),
		Endpoint:        getEnv("ALICLOUD_ENDPOINT", ""),
		Timeout:         getEnvInt("ALICLOUD_TIMEOUT", 30),
		BatchSize:       getEnvInt("ALICLOUD_BATCH_SIZE", 100),
		MaxRetries:      getEnvInt("ALICLOUD_MAX_RETRIES", 5),
		RetryDelay:      getEnvInt("ALICLOUD_RETRY_DELAY", 2),

		// 粒度配置默认值
		DefaultGranularity: getEnv("ALICLOUD_DEFAULT_GRANULARITY", "monthly"),
		EnableDailySync:    getEnvBool("ALICLOUD_ENABLE_DAILY_SYNC", false),
		DailySyncDays:      getEnvInt("ALICLOUD_DAILY_SYNC_DAYS", 30),

		// 表名配置默认值
		MonthlyTable: getEnv("ALICLOUD_MONTHLY_TABLE", "alicloud_bill_monthly"),
		DailyTable:   getEnv("ALICLOUD_DAILY_TABLE", "alicloud_bill_daily"),

		// 历史数据同步配置默认值
		DefaultSyncMode:     getEnv("ALICLOUD_DEFAULT_SYNC_MODE", "all_periods"),
		MaxHistoricalMonths: getEnvInt("ALICLOUD_MAX_HISTORICAL_MONTHS", 18), // 阿里云支持18个月
		AutoDetectPeriods:   getEnvBool("ALICLOUD_AUTO_DETECT_PERIODS", true),
		DefaultStartPeriod:  getEnv("ALICLOUD_DEFAULT_START_PERIOD", ""),
		DefaultEndPeriod:    getEnv("ALICLOUD_DEFAULT_END_PERIOD", ""),
		SkipEmptyPeriods:    getEnvBool("ALICLOUD_SKIP_EMPTY_PERIODS", true),
	}
}

func NewWeChatConfig() *WeChatConfig {
	return &WeChatConfig{
		WebhookURL:     getEnv("WECHAT_WEBHOOK_URL", ""),
		Enabled:        getEnvBool("WECHAT_ENABLED", false),
		MentionUsers:   parseStringList(getEnv("WECHAT_MENTION_USERS", "")),
		AlertThreshold: getEnvFloat("WECHAT_ALERT_THRESHOLD", 20.0),
		SendTime:       getEnv("WECHAT_SEND_TIME", "0 9 * * *"), // 默认每天上午9点
		MaxRetries:     getEnvInt("WECHAT_MAX_RETRIES", 3),
		RetryDelay:     getEnvInt("WECHAT_RETRY_DELAY", 2),
	}
}

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
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := &Config{}
	ext := filepath.Ext(configPath)

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", ext)
	}

	mergeEnvVars(config)

	return config, nil
}

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
		return fmt.Errorf("unsupported config file format: %s", ext)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func getDefaultConfig() *Config {
	return &Config{
		ClickHouse: NewClickHouseConfig(),
		CloudProviders: &CloudProvidersConfig{
			VolcEngine: NewVolcEngineConfig(),
			AliCloud:   NewAliCloudConfig(),
			AWS: &AWSConfig{
				AccessKey: getEnv("AWS_ACCESS_KEY", ""),
				SecretKey: getEnv("AWS_SECRET_KEY", ""),
				Region:    getEnv("AWS_REGION", "us-east-1"),
				Timeout:   getEnvInt("AWS_TIMEOUT", 30),
			},
			Azure: &AzureConfig{
				ClientID:       getEnv("AZURE_CLIENT_ID", ""),
				ClientSecret:   getEnv("AZURE_CLIENT_SECRET", ""),
				TenantID:       getEnv("AZURE_TENANT_ID", ""),
				SubscriptionID: getEnv("AZURE_SUBSCRIPTION_ID", ""),
				Timeout:        getEnvInt("AZURE_TIMEOUT", 30),
			},
			GCP: &GCPConfig{
				ProjectID:         getEnv("GCP_PROJECT_ID", ""),
				ServiceAccountKey: getEnv("GCP_SERVICE_ACCOUNT_KEY", ""),
				Timeout:           getEnvInt("GCP_TIMEOUT", 30),
			},
		},
		Server: &ServerConfig{
			Port:    getEnvInt("SERVER_PORT", 8080),
			Address: getEnv("SERVER_ADDRESS", "0.0.0.0"),
		},
		Scheduler: &SchedulerConfig{
			Enabled: getEnvBool("SCHEDULER_ENABLED", true),
			Jobs:    []ScheduledJob{},
		},
		Runtime: &RuntimeConfig{
			MaxConcurrentTasks:      getEnvInt("RUNTIME_MAX_CONCURRENT_TASKS", 3),
			TaskTimeout:             getEnvInt("RUNTIME_TASK_TIMEOUT", 3600),
			GracefulShutdownTimeout: getEnvInt("RUNTIME_GRACEFUL_SHUTDOWN_TIMEOUT", 30),
		},
		WeChat: NewWeChatConfig(),
		App: &AppConfig{
			LogLevel: getEnv("LOG_LEVEL", "info"),
			LogFile:  getEnv("LOG_FILE", ""),
		},
	}
}

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

func mergeEnvVars(config *Config) {
	if config.ClickHouse == nil {
		config.ClickHouse = NewClickHouseConfig()
	} else {
		// 优先使用环境变量
		if hostsEnv := os.Getenv("CLICKHOUSE_HOSTS"); hostsEnv != "" {
			config.ClickHouse.Hosts = parseHosts(hostsEnv)
		} else if host := os.Getenv("CLICKHOUSE_HOST"); host != "" {
			config.ClickHouse.Hosts = []string{host}
		}
		if port := getEnvInt("CLICKHOUSE_PORT", 0); port != 0 {
			config.ClickHouse.Port = port
		}
		if database := os.Getenv("CLICKHOUSE_DATABASE"); database != "" {
			config.ClickHouse.Database = database
		}
		if username := os.Getenv("CLICKHOUSE_USERNAME"); username != "" {
			config.ClickHouse.Username = username
		}
		if password := os.Getenv("CLICKHOUSE_PASSWORD"); password != "" {
			config.ClickHouse.Password = password
		}
		if debug := os.Getenv("CLICKHOUSE_DEBUG"); debug != "" {
			config.ClickHouse.Debug = debug == "true" || debug == "1"
		}
		if cluster := os.Getenv("CLICKHOUSE_CLUSTER"); cluster != "" {
			config.ClickHouse.Cluster = cluster
		}
		if protocol := os.Getenv("CLICKHOUSE_PROTOCOL"); protocol != "" {
			config.ClickHouse.Protocol = protocol
		}
	}

	// 处理云服务提供商配置
	if config.CloudProviders == nil {
		config.CloudProviders = &CloudProvidersConfig{}
	}

	// 火山云配置
	if config.CloudProviders.VolcEngine == nil {
		config.CloudProviders.VolcEngine = NewVolcEngineConfig()
	} else {
		if accessKey := os.Getenv("VOLCENGINE_ACCESS_KEY"); accessKey != "" {
			config.CloudProviders.VolcEngine.AccessKey = accessKey
		}
		if secretKey := os.Getenv("VOLCENGINE_SECRET_KEY"); secretKey != "" {
			config.CloudProviders.VolcEngine.SecretKey = secretKey
		}
		if region := os.Getenv("VOLCENGINE_REGION"); region != "" {
			config.CloudProviders.VolcEngine.Region = region
		}
		if host := os.Getenv("VOLCENGINE_HOST"); host != "" {
			config.CloudProviders.VolcEngine.Host = host
		}
		if timeout := getEnvInt("VOLCENGINE_TIMEOUT", 0); timeout != 0 {
			config.CloudProviders.VolcEngine.Timeout = timeout
		}
		if batchSize := getEnvInt("VOLCENGINE_BATCH_SIZE", 0); batchSize != 0 {
			config.CloudProviders.VolcEngine.BatchSize = batchSize
		}
		if maxRetries := getEnvInt("VOLCENGINE_MAX_RETRIES", 0); maxRetries != 0 {
			config.CloudProviders.VolcEngine.MaxRetries = maxRetries
		}
		if retryDelay := getEnvInt("VOLCENGINE_RETRY_DELAY", 0); retryDelay != 0 {
			config.CloudProviders.VolcEngine.RetryDelay = retryDelay
		}

		// 历史数据同步配置环境变量覆盖
		if syncMode := os.Getenv("VOLCENGINE_DEFAULT_SYNC_MODE"); syncMode != "" {
			config.CloudProviders.VolcEngine.DefaultSyncMode = syncMode
		}
		if maxMonths := getEnvInt("VOLCENGINE_MAX_HISTORICAL_MONTHS", 0); maxMonths != 0 {
			config.CloudProviders.VolcEngine.MaxHistoricalMonths = maxMonths
		}
		if autoDetect := os.Getenv("VOLCENGINE_AUTO_DETECT_PERIODS"); autoDetect != "" {
			config.CloudProviders.VolcEngine.AutoDetectPeriods = autoDetect == "true" || autoDetect == "1"
		}
		if startPeriod := os.Getenv("VOLCENGINE_DEFAULT_START_PERIOD"); startPeriod != "" {
			config.CloudProviders.VolcEngine.DefaultStartPeriod = startPeriod
		}
		if endPeriod := os.Getenv("VOLCENGINE_DEFAULT_END_PERIOD"); endPeriod != "" {
			config.CloudProviders.VolcEngine.DefaultEndPeriod = endPeriod
		}
		if skipEmpty := os.Getenv("VOLCENGINE_SKIP_EMPTY_PERIODS"); skipEmpty != "" {
			config.CloudProviders.VolcEngine.SkipEmptyPeriods = skipEmpty == "true" || skipEmpty == "1"
		}
	}

	// 阿里云配置
	if config.CloudProviders.AliCloud == nil {
		config.CloudProviders.AliCloud = NewAliCloudConfig()
	} else {
		if accessKeyID := os.Getenv("ALICLOUD_ACCESS_KEY_ID"); accessKeyID != "" {
			config.CloudProviders.AliCloud.AccessKeyID = accessKeyID
		}
		if accessKeySecret := os.Getenv("ALICLOUD_ACCESS_KEY_SECRET"); accessKeySecret != "" {
			config.CloudProviders.AliCloud.AccessKeySecret = accessKeySecret
		}
		if region := os.Getenv("ALICLOUD_REGION"); region != "" {
			config.CloudProviders.AliCloud.Region = region
		}
		if endpoint := os.Getenv("ALICLOUD_ENDPOINT"); endpoint != "" {
			config.CloudProviders.AliCloud.Endpoint = endpoint
		}
		if timeout := getEnvInt("ALICLOUD_TIMEOUT", 0); timeout != 0 {
			config.CloudProviders.AliCloud.Timeout = timeout
		}
		if batchSize := getEnvInt("ALICLOUD_BATCH_SIZE", 0); batchSize != 0 {
			config.CloudProviders.AliCloud.BatchSize = batchSize
		}
		if maxRetries := getEnvInt("ALICLOUD_MAX_RETRIES", 0); maxRetries != 0 {
			config.CloudProviders.AliCloud.MaxRetries = maxRetries
		}
		if retryDelay := getEnvInt("ALICLOUD_RETRY_DELAY", 0); retryDelay != 0 {
			config.CloudProviders.AliCloud.RetryDelay = retryDelay
		}

		// 粒度配置环境变量覆盖
		if granularity := os.Getenv("ALICLOUD_DEFAULT_GRANULARITY"); granularity != "" {
			config.CloudProviders.AliCloud.DefaultGranularity = granularity
		}
		if enableDaily := os.Getenv("ALICLOUD_ENABLE_DAILY_SYNC"); enableDaily != "" {
			config.CloudProviders.AliCloud.EnableDailySync = enableDaily == "true" || enableDaily == "1"
		}
		if dailyDays := getEnvInt("ALICLOUD_DAILY_SYNC_DAYS", 0); dailyDays != 0 {
			config.CloudProviders.AliCloud.DailySyncDays = dailyDays
		}

		// 表名配置环境变量覆盖
		if monthlyTable := os.Getenv("ALICLOUD_MONTHLY_TABLE"); monthlyTable != "" {
			config.CloudProviders.AliCloud.MonthlyTable = monthlyTable
		}
		if dailyTable := os.Getenv("ALICLOUD_DAILY_TABLE"); dailyTable != "" {
			config.CloudProviders.AliCloud.DailyTable = dailyTable
		}

		// 历史数据同步配置环境变量覆盖
		if syncMode := os.Getenv("ALICLOUD_DEFAULT_SYNC_MODE"); syncMode != "" {
			config.CloudProviders.AliCloud.DefaultSyncMode = syncMode
		}
		if maxMonths := getEnvInt("ALICLOUD_MAX_HISTORICAL_MONTHS", 0); maxMonths != 0 {
			config.CloudProviders.AliCloud.MaxHistoricalMonths = maxMonths
		}
		if autoDetect := os.Getenv("ALICLOUD_AUTO_DETECT_PERIODS"); autoDetect != "" {
			config.CloudProviders.AliCloud.AutoDetectPeriods = autoDetect == "true" || autoDetect == "1"
		}
		if startPeriod := os.Getenv("ALICLOUD_DEFAULT_START_PERIOD"); startPeriod != "" {
			config.CloudProviders.AliCloud.DefaultStartPeriod = startPeriod
		}
		if endPeriod := os.Getenv("ALICLOUD_DEFAULT_END_PERIOD"); endPeriod != "" {
			config.CloudProviders.AliCloud.DefaultEndPeriod = endPeriod
		}
		if skipEmpty := os.Getenv("ALICLOUD_SKIP_EMPTY_PERIODS"); skipEmpty != "" {
			config.CloudProviders.AliCloud.SkipEmptyPeriods = skipEmpty == "true" || skipEmpty == "1"
		}
	}

	// AWS配置
	if config.CloudProviders.AWS == nil {
		config.CloudProviders.AWS = &AWSConfig{}
	}
	if accessKey := os.Getenv("AWS_ACCESS_KEY"); accessKey != "" {
		config.CloudProviders.AWS.AccessKey = accessKey
	}
	if secretKey := os.Getenv("AWS_SECRET_KEY"); secretKey != "" {
		config.CloudProviders.AWS.SecretKey = secretKey
	}
	if region := os.Getenv("AWS_REGION"); region != "" {
		config.CloudProviders.AWS.Region = region
	}
	if timeout := getEnvInt("AWS_TIMEOUT", 0); timeout != 0 {
		config.CloudProviders.AWS.Timeout = timeout
	}

	// Azure配置
	if config.CloudProviders.Azure == nil {
		config.CloudProviders.Azure = &AzureConfig{}
	}
	if clientID := os.Getenv("AZURE_CLIENT_ID"); clientID != "" {
		config.CloudProviders.Azure.ClientID = clientID
	}
	if clientSecret := os.Getenv("AZURE_CLIENT_SECRET"); clientSecret != "" {
		config.CloudProviders.Azure.ClientSecret = clientSecret
	}
	if tenantID := os.Getenv("AZURE_TENANT_ID"); tenantID != "" {
		config.CloudProviders.Azure.TenantID = tenantID
	}
	if subscriptionID := os.Getenv("AZURE_SUBSCRIPTION_ID"); subscriptionID != "" {
		config.CloudProviders.Azure.SubscriptionID = subscriptionID
	}
	if timeout := getEnvInt("AZURE_TIMEOUT", 0); timeout != 0 {
		config.CloudProviders.Azure.Timeout = timeout
	}

	// GCP配置
	if config.CloudProviders.GCP == nil {
		config.CloudProviders.GCP = &GCPConfig{}
	}
	if projectID := os.Getenv("GCP_PROJECT_ID"); projectID != "" {
		config.CloudProviders.GCP.ProjectID = projectID
	}
	if serviceAccountKey := os.Getenv("GCP_SERVICE_ACCOUNT_KEY"); serviceAccountKey != "" {
		config.CloudProviders.GCP.ServiceAccountKey = serviceAccountKey
	}
	if timeout := getEnvInt("GCP_TIMEOUT", 0); timeout != 0 {
		config.CloudProviders.GCP.Timeout = timeout
	}

	// Server configuration
	if config.Server == nil {
		config.Server = &ServerConfig{}
	}
	if port := getEnvInt("SERVER_PORT", 0); port != 0 {
		config.Server.Port = port
	}
	if address := os.Getenv("SERVER_ADDRESS"); address != "" {
		config.Server.Address = address
	}

	// Scheduler configuration
	if config.Scheduler == nil {
		config.Scheduler = &SchedulerConfig{}
	}
	if enabled := os.Getenv("SCHEDULER_ENABLED"); enabled != "" {
		config.Scheduler.Enabled = enabled == "true" || enabled == "1"
	}

	// Runtime configuration
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

	// 微信配置
	if config.WeChat == nil {
		config.WeChat = NewWeChatConfig()
	} else {
		if webhookURL := os.Getenv("WECHAT_WEBHOOK_URL"); webhookURL != "" {
			config.WeChat.WebhookURL = webhookURL
		}
		if enabled := os.Getenv("WECHAT_ENABLED"); enabled != "" {
			config.WeChat.Enabled = enabled == "true" || enabled == "1"
		}
		if mentionUsers := os.Getenv("WECHAT_MENTION_USERS"); mentionUsers != "" {
			config.WeChat.MentionUsers = parseStringList(mentionUsers)
		}
		if alertThreshold := getEnvFloat("WECHAT_ALERT_THRESHOLD", 0); alertThreshold != 0 {
			config.WeChat.AlertThreshold = alertThreshold
		}
		if sendTime := os.Getenv("WECHAT_SEND_TIME"); sendTime != "" {
			config.WeChat.SendTime = sendTime
		}
		if maxRetries := getEnvInt("WECHAT_MAX_RETRIES", 0); maxRetries != 0 {
			config.WeChat.MaxRetries = maxRetries
		}
		if retryDelay := getEnvInt("WECHAT_RETRY_DELAY", 0); retryDelay != 0 {
			config.WeChat.RetryDelay = retryDelay
		}
	}

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

// GetVolcEngineConfig 获取火山云配置
func (c *Config) GetVolcEngineConfig() *VolcEngineConfig {
	if c.CloudProviders != nil && c.CloudProviders.VolcEngine != nil {
		return c.CloudProviders.VolcEngine
	}
	return NewVolcEngineConfig()
}

// GetAWSConfig 获取AWS配置
func (c *Config) GetAWSConfig() *AWSConfig {
	if c.CloudProviders != nil && c.CloudProviders.AWS != nil {
		return c.CloudProviders.AWS
	}
	return &AWSConfig{
		AccessKey: getEnv("AWS_ACCESS_KEY", ""),
		SecretKey: getEnv("AWS_SECRET_KEY", ""),
		Region:    getEnv("AWS_REGION", "us-east-1"),
		Timeout:   getEnvInt("AWS_TIMEOUT", 30),
	}
}

// GetAzureConfig 获取Azure配置
func (c *Config) GetAzureConfig() *AzureConfig {
	if c.CloudProviders != nil && c.CloudProviders.Azure != nil {
		return c.CloudProviders.Azure
	}
	return &AzureConfig{
		ClientID:       getEnv("AZURE_CLIENT_ID", ""),
		ClientSecret:   getEnv("AZURE_CLIENT_SECRET", ""),
		TenantID:       getEnv("AZURE_TENANT_ID", ""),
		SubscriptionID: getEnv("AZURE_SUBSCRIPTION_ID", ""),
		Timeout:        getEnvInt("AZURE_TIMEOUT", 30),
	}
}

// GetAliCloudConfig 获取阿里云配置
func (c *Config) GetAliCloudConfig() *AliCloudConfig {
	if c.CloudProviders != nil && c.CloudProviders.AliCloud != nil {
		return c.CloudProviders.AliCloud
	}
	return NewAliCloudConfig()
}

// GetGCPConfig 获取GCP配置
func (c *Config) GetGCPConfig() *GCPConfig {
	if c.CloudProviders != nil && c.CloudProviders.GCP != nil {
		return c.CloudProviders.GCP
	}
	return &GCPConfig{
		ProjectID:         getEnv("GCP_PROJECT_ID", ""),
		ServiceAccountKey: getEnv("GCP_SERVICE_ACCOUNT_KEY", ""),
		Timeout:           getEnvInt("GCP_TIMEOUT", 30),
	}
}

// GetWeChatConfig 获取微信配置
func (c *Config) GetWeChatConfig() *WeChatConfig {
	if c.WeChat != nil {
		return c.WeChat
	}
	return NewWeChatConfig()
}
