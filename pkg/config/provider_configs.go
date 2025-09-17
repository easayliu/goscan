package config

import "os"

// CloudProvidersConfig 云服务提供商配置集合
type CloudProvidersConfig struct {
	VolcEngine *VolcEngineConfig `json:"volcengine" yaml:"volcengine"`
	AliCloud   *AliCloudConfig   `json:"alicloud" yaml:"alicloud"`
	AWS        *AWSConfig        `json:"aws" yaml:"aws"`
	Azure      *AzureConfig      `json:"azure" yaml:"azure"`
	GCP        *GCPConfig        `json:"gcp" yaml:"gcp"`
}

// VolcEngineConfig 火山引擎配置
type VolcEngineConfig struct {
	AccessKey   string `json:"access_key" yaml:"access_key"`
	SecretKey   string `json:"secret_key" yaml:"secret_key"`
	Region      string `json:"region" yaml:"region"`
	Host        string `json:"host" yaml:"host"`
	Timeout     int    `json:"timeout" yaml:"timeout"`           // seconds
	BatchSize   int    `json:"batch_size" yaml:"batch_size"`     // API批量获取大小
	MaxRetries  int    `json:"max_retries" yaml:"max_retries"`   // 最大重试次数
	RetryDelay  int    `json:"retry_delay" yaml:"retry_delay"`   // 重试延迟(秒)
	RateLimit   int    `json:"rate_limit" yaml:"rate_limit"`     // QPS 限制
	EnableDebug bool   `json:"enable_debug" yaml:"enable_debug"` // 启用调试模式

	// === 历史数据同步配置 ===
	DefaultSyncMode     string `json:"default_sync_mode" yaml:"default_sync_mode"`         // all_periods, current_period, range
	MaxHistoricalMonths int    `json:"max_historical_months" yaml:"max_historical_months"` // 最多同步几个月的历史数据，0表示无限制
	AutoDetectPeriods   bool   `json:"auto_detect_periods" yaml:"auto_detect_periods"`     // 是否自动检测可用账期
	DefaultStartPeriod  string `json:"default_start_period" yaml:"default_start_period"`   // 默认开始账期 YYYY-MM
	DefaultEndPeriod    string `json:"default_end_period" yaml:"default_end_period"`       // 默认结束账期 YYYY-MM
	SkipEmptyPeriods    bool   `json:"skip_empty_periods" yaml:"skip_empty_periods"`       // 是否跳过空账期
}

// AliCloudConfig 阿里云配置
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

// AWSConfig AWS配置
type AWSConfig struct {
	AccessKey string `json:"access_key" yaml:"access_key"`
	SecretKey string `json:"secret_key" yaml:"secret_key"`
	Region    string `json:"region" yaml:"region"`
	Timeout   int    `json:"timeout" yaml:"timeout"` // seconds
}

// AzureConfig Azure配置
type AzureConfig struct {
	ClientID       string `json:"client_id" yaml:"client_id"`
	ClientSecret   string `json:"client_secret" yaml:"client_secret"`
	TenantID       string `json:"tenant_id" yaml:"tenant_id"`
	SubscriptionID string `json:"subscription_id" yaml:"subscription_id"`
	Timeout        int    `json:"timeout" yaml:"timeout"` // seconds
}

// GCPConfig GCP配置
type GCPConfig struct {
	ProjectID         string `json:"project_id" yaml:"project_id"`
	ServiceAccountKey string `json:"service_account_key" yaml:"service_account_key"`
	Timeout           int    `json:"timeout" yaml:"timeout"` // seconds
}

// NewVolcEngineConfig 创建火山引擎配置，使用环境变量填充默认值
func NewVolcEngineConfig() *VolcEngineConfig {
	return &VolcEngineConfig{
		AccessKey:   getEnv("VOLCENGINE_ACCESS_KEY", ""),
		SecretKey:   getEnv("VOLCENGINE_SECRET_KEY", ""),
		Region:      getEnv("VOLCENGINE_REGION", "cn-north-1"),
		Host:        getEnv("VOLCENGINE_HOST", "billing.volcengineapi.com"),
		Timeout:     getEnvInt("VOLCENGINE_TIMEOUT", 30),
		BatchSize:   getEnvInt("VOLCENGINE_BATCH_SIZE", 50),
		MaxRetries:  getEnvInt("VOLCENGINE_MAX_RETRIES", 5),
		RetryDelay:  getEnvInt("VOLCENGINE_RETRY_DELAY", 1),
		RateLimit:   getEnvInt("VOLCENGINE_RATE_LIMIT", 10),
		EnableDebug: getEnvBool("VOLCENGINE_DEBUG", false),

		// 历史数据同步配置默认值
		DefaultSyncMode:     getEnv("VOLCENGINE_DEFAULT_SYNC_MODE", "all_periods"),
		MaxHistoricalMonths: getEnvInt("VOLCENGINE_MAX_HISTORICAL_MONTHS", 12),
		AutoDetectPeriods:   getEnvBool("VOLCENGINE_AUTO_DETECT_PERIODS", true),
		DefaultStartPeriod:  getEnv("VOLCENGINE_DEFAULT_START_PERIOD", ""),
		DefaultEndPeriod:    getEnv("VOLCENGINE_DEFAULT_END_PERIOD", ""),
		SkipEmptyPeriods:    getEnvBool("VOLCENGINE_SKIP_EMPTY_PERIODS", true),
	}
}

// NewAliCloudConfig 创建阿里云配置，使用环境变量填充默认值
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

// NewAWSConfig 创建AWS配置，使用环境变量填充默认值
func NewAWSConfig() *AWSConfig {
	return &AWSConfig{
		AccessKey: getEnv("AWS_ACCESS_KEY", ""),
		SecretKey: getEnv("AWS_SECRET_KEY", ""),
		Region:    getEnv("AWS_REGION", "us-east-1"),
		Timeout:   getEnvInt("AWS_TIMEOUT", 30),
	}
}

// NewAzureConfig 创建Azure配置，使用环境变量填充默认值
func NewAzureConfig() *AzureConfig {
	return &AzureConfig{
		ClientID:       getEnv("AZURE_CLIENT_ID", ""),
		ClientSecret:   getEnv("AZURE_CLIENT_SECRET", ""),
		TenantID:       getEnv("AZURE_TENANT_ID", ""),
		SubscriptionID: getEnv("AZURE_SUBSCRIPTION_ID", ""),
		Timeout:        getEnvInt("AZURE_TIMEOUT", 30),
	}
}

// NewGCPConfig 创建GCP配置，使用环境变量填充默认值
func NewGCPConfig() *GCPConfig {
	return &GCPConfig{
		ProjectID:         getEnv("GCP_PROJECT_ID", ""),
		ServiceAccountKey: getEnv("GCP_SERVICE_ACCOUNT_KEY", ""),
		Timeout:           getEnvInt("GCP_TIMEOUT", 30),
	}
}

// Validate 验证火山引擎配置
func (c *VolcEngineConfig) Validate() error {
	if c.AccessKey == "" {
		return ErrMissingRequired
	}

	if c.SecretKey == "" {
		return ErrMissingRequired
	}

	if c.Region == "" {
		return ErrMissingRequired
	}

	if c.Timeout <= 0 {
		c.Timeout = 30
	}

	if c.MaxRetries < 0 {
		c.MaxRetries = 5
	}

	if c.RetryDelay <= 0 {
		c.RetryDelay = 1
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 50
	}

	if c.RateLimit <= 0 {
		c.RateLimit = 10
	}

	return nil
}

// Validate 验证阿里云配置
func (c *AliCloudConfig) Validate() error {
	if c.AccessKeyID == "" {
		return ErrMissingRequired
	}

	if c.AccessKeySecret == "" {
		return ErrMissingRequired
	}

	if c.Region == "" {
		return ErrMissingRequired
	}

	if c.Timeout <= 0 {
		c.Timeout = 30
	}

	if c.MaxRetries < 0 {
		c.MaxRetries = 5
	}

	if c.RetryDelay <= 0 {
		c.RetryDelay = 2
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}

	return nil
}

// mergeCloudProviderEnvVars 合并云服务提供商环境变量
func mergeCloudProviderEnvVars(config *Config) {
	if config.CloudProviders == nil {
		config.CloudProviders = &CloudProvidersConfig{}
	}

	mergeVolcEngineEnvVars(config)
	mergeAliCloudEnvVars(config)
	mergeAWSEnvVars(config)
	mergeAzureEnvVars(config)
	mergeGCPEnvVars(config)
}

// mergeVolcEngineEnvVars 合并火山引擎环境变量
func mergeVolcEngineEnvVars(config *Config) {
	if config.CloudProviders.VolcEngine == nil {
		config.CloudProviders.VolcEngine = NewVolcEngineConfig()
		return
	}

	ve := config.CloudProviders.VolcEngine

	envMappings := map[string]interface{}{
		"VOLCENGINE_ACCESS_KEY":            &ve.AccessKey,
		"VOLCENGINE_SECRET_KEY":            &ve.SecretKey,
		"VOLCENGINE_REGION":                &ve.Region,
		"VOLCENGINE_HOST":                  &ve.Host,
		"VOLCENGINE_TIMEOUT":               &ve.Timeout,
		"VOLCENGINE_BATCH_SIZE":            &ve.BatchSize,
		"VOLCENGINE_MAX_RETRIES":           &ve.MaxRetries,
		"VOLCENGINE_RETRY_DELAY":           &ve.RetryDelay,
		"VOLCENGINE_RATE_LIMIT":            &ve.RateLimit,
		"VOLCENGINE_DEFAULT_SYNC_MODE":     &ve.DefaultSyncMode,
		"VOLCENGINE_MAX_HISTORICAL_MONTHS": &ve.MaxHistoricalMonths,
		"VOLCENGINE_DEFAULT_START_PERIOD":  &ve.DefaultStartPeriod,
		"VOLCENGINE_DEFAULT_END_PERIOD":    &ve.DefaultEndPeriod,
	}

	applyEnvMappings(envMappings)

	// 布尔值单独处理
	if debug := os.Getenv("VOLCENGINE_DEBUG"); debug != "" {
		ve.EnableDebug = debug == "true" || debug == "1"
	}
	if autoDetect := os.Getenv("VOLCENGINE_AUTO_DETECT_PERIODS"); autoDetect != "" {
		ve.AutoDetectPeriods = autoDetect == "true" || autoDetect == "1"
	}
	if skipEmpty := os.Getenv("VOLCENGINE_SKIP_EMPTY_PERIODS"); skipEmpty != "" {
		ve.SkipEmptyPeriods = skipEmpty == "true" || skipEmpty == "1"
	}
}

// mergeAliCloudEnvVars 合并阿里云环境变量
func mergeAliCloudEnvVars(config *Config) {
	if config.CloudProviders.AliCloud == nil {
		config.CloudProviders.AliCloud = NewAliCloudConfig()
		return
	}

	ac := config.CloudProviders.AliCloud

	envMappings := map[string]interface{}{
		"ALICLOUD_ACCESS_KEY_ID":         &ac.AccessKeyID,
		"ALICLOUD_ACCESS_KEY_SECRET":     &ac.AccessKeySecret,
		"ALICLOUD_REGION":                &ac.Region,
		"ALICLOUD_ENDPOINT":              &ac.Endpoint,
		"ALICLOUD_TIMEOUT":               &ac.Timeout,
		"ALICLOUD_BATCH_SIZE":            &ac.BatchSize,
		"ALICLOUD_MAX_RETRIES":           &ac.MaxRetries,
		"ALICLOUD_RETRY_DELAY":           &ac.RetryDelay,
		"ALICLOUD_DEFAULT_GRANULARITY":   &ac.DefaultGranularity,
		"ALICLOUD_DAILY_SYNC_DAYS":       &ac.DailySyncDays,
		"ALICLOUD_MONTHLY_TABLE":         &ac.MonthlyTable,
		"ALICLOUD_DAILY_TABLE":           &ac.DailyTable,
		"ALICLOUD_DEFAULT_SYNC_MODE":     &ac.DefaultSyncMode,
		"ALICLOUD_MAX_HISTORICAL_MONTHS": &ac.MaxHistoricalMonths,
		"ALICLOUD_DEFAULT_START_PERIOD":  &ac.DefaultStartPeriod,
		"ALICLOUD_DEFAULT_END_PERIOD":    &ac.DefaultEndPeriod,
	}

	applyEnvMappings(envMappings)

	// 布尔值单独处理
	if enableDaily := os.Getenv("ALICLOUD_ENABLE_DAILY_SYNC"); enableDaily != "" {
		ac.EnableDailySync = enableDaily == "true" || enableDaily == "1"
	}
	if autoDetect := os.Getenv("ALICLOUD_AUTO_DETECT_PERIODS"); autoDetect != "" {
		ac.AutoDetectPeriods = autoDetect == "true" || autoDetect == "1"
	}
	if skipEmpty := os.Getenv("ALICLOUD_SKIP_EMPTY_PERIODS"); skipEmpty != "" {
		ac.SkipEmptyPeriods = skipEmpty == "true" || skipEmpty == "1"
	}
}

// mergeAWSEnvVars 合并AWS环境变量
func mergeAWSEnvVars(config *Config) {
	if config.CloudProviders.AWS == nil {
		config.CloudProviders.AWS = NewAWSConfig()
		return
	}

	aws := config.CloudProviders.AWS

	envMappings := map[string]interface{}{
		"AWS_ACCESS_KEY": &aws.AccessKey,
		"AWS_SECRET_KEY": &aws.SecretKey,
		"AWS_REGION":     &aws.Region,
		"AWS_TIMEOUT":    &aws.Timeout,
	}

	applyEnvMappings(envMappings)
}

// mergeAzureEnvVars 合并Azure环境变量
func mergeAzureEnvVars(config *Config) {
	if config.CloudProviders.Azure == nil {
		config.CloudProviders.Azure = NewAzureConfig()
		return
	}

	azure := config.CloudProviders.Azure

	envMappings := map[string]interface{}{
		"AZURE_CLIENT_ID":       &azure.ClientID,
		"AZURE_CLIENT_SECRET":   &azure.ClientSecret,
		"AZURE_TENANT_ID":       &azure.TenantID,
		"AZURE_SUBSCRIPTION_ID": &azure.SubscriptionID,
		"AZURE_TIMEOUT":         &azure.Timeout,
	}

	applyEnvMappings(envMappings)
}

// mergeGCPEnvVars 合并GCP环境变量
func mergeGCPEnvVars(config *Config) {
	if config.CloudProviders.GCP == nil {
		config.CloudProviders.GCP = NewGCPConfig()
		return
	}

	gcp := config.CloudProviders.GCP

	envMappings := map[string]interface{}{
		"GCP_PROJECT_ID":          &gcp.ProjectID,
		"GCP_SERVICE_ACCOUNT_KEY": &gcp.ServiceAccountKey,
		"GCP_TIMEOUT":             &gcp.Timeout,
	}

	applyEnvMappings(envMappings)
}

// applyEnvMappings 应用环境变量映射的工具函数
func applyEnvMappings(envMappings map[string]interface{}) {
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
}
