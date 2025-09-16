package config

// Config 主配置结构体
type Config struct {
	ClickHouse     *ClickHouseConfig     `json:"clickhouse" yaml:"clickhouse"`
	CloudProviders *CloudProvidersConfig `json:"cloud_providers" yaml:"cloud_providers"`
	Server         *ServerConfig         `json:"server" yaml:"server"`
	Scheduler      *SchedulerConfig      `json:"scheduler" yaml:"scheduler"`
	Runtime        *RuntimeConfig        `json:"runtime" yaml:"runtime"`
	WeChat         *WeChatConfig         `json:"wechat" yaml:"wechat"`
	App            *AppConfig            `json:"app" yaml:"app"`
}

// getDefaultConfig 获取默认配置，所有配置项都使用各自的默认值
func getDefaultConfig() *Config {
	return &Config{
		ClickHouse:     NewClickHouseConfig(),
		CloudProviders: getDefaultCloudProvidersConfig(),
		Server:         NewServerConfig(),
		Scheduler:      NewSchedulerConfig(),
		Runtime:        NewRuntimeConfig(),
		WeChat:         NewWeChatConfig(),
		App:            NewAppConfig(),
	}
}

// getDefaultCloudProvidersConfig 获取默认的云服务提供商配置
func getDefaultCloudProvidersConfig() *CloudProvidersConfig {
	return &CloudProvidersConfig{
		VolcEngine: NewVolcEngineConfig(),
		AliCloud:   NewAliCloudConfig(),
		AWS:        NewAWSConfig(),
		Azure:      NewAzureConfig(),
		GCP:        NewGCPConfig(),
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
	return NewAWSConfig()
}

// GetAzureConfig 获取Azure配置
func (c *Config) GetAzureConfig() *AzureConfig {
	if c.CloudProviders != nil && c.CloudProviders.Azure != nil {
		return c.CloudProviders.Azure
	}
	return NewAzureConfig()
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
	return NewGCPConfig()
}

// GetWeChatConfig 获取微信配置
func (c *Config) GetWeChatConfig() *WeChatConfig {
	if c.WeChat != nil {
		return c.WeChat
	}
	return NewWeChatConfig()
}



