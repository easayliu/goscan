package config

import (
	"fmt"
	"strings"
)

// ValidateConfig 验证完整的配置
func (c *Config) ValidateConfig() error {
	if err := c.validateClickHouseConfig(); err != nil {
		return fmt.Errorf("%w: %v", ErrClickHouseConfig, err)
	}

	if err := c.validateCloudProvidersConfig(); err != nil {
		return err
	}

	if err := c.validateSchedulerConfig(); err != nil {
		return fmt.Errorf("%w: %v", ErrSchedulerConfig, err)
	}

	if err := c.validateWeChatConfig(); err != nil {
		return fmt.Errorf("%w: %v", ErrWeChatConfig, err)
	}

	return nil
}

// validateClickHouseConfig 验证ClickHouse配置
func (c *Config) validateClickHouseConfig() error {
	if c.ClickHouse == nil {
		return fmt.Errorf("%w: ClickHouse配置不能为空", ErrMissingRequired)
	}

	ch := c.ClickHouse

	if len(ch.Hosts) == 0 {
		return fmt.Errorf("%w: hosts", ErrMissingRequired)
	}

	if ch.Port <= 0 || ch.Port > 65535 {
		return fmt.Errorf("%w: port必须在1-65535范围内", ErrInvalidValue)
	}

	if ch.Database == "" {
		return fmt.Errorf("%w: database", ErrMissingRequired)
	}

	if ch.Protocol != "" && ch.Protocol != "native" && ch.Protocol != "http" {
		return fmt.Errorf("%w: protocol必须是'native'或'http'", ErrInvalidValue)
	}

	return nil
}

// validateCloudProvidersConfig 验证云服务提供商配置
func (c *Config) validateCloudProvidersConfig() error {
	if c.CloudProviders == nil {
		return nil // 云服务提供商配置是可选的
	}

	cp := c.CloudProviders

	if cp.VolcEngine != nil {
		if err := validateVolcEngineConfig(cp.VolcEngine); err != nil {
			return fmt.Errorf("%w: %v", ErrVolcEngineConfig, err)
		}
	}

	if cp.AliCloud != nil {
		if err := validateAliCloudConfig(cp.AliCloud); err != nil {
			return fmt.Errorf("%w: %v", ErrAliCloudConfig, err)
		}
	}

	if cp.AWS != nil {
		if err := validateAWSConfig(cp.AWS); err != nil {
			return fmt.Errorf("%w: %v", ErrAWSConfig, err)
		}
	}

	if cp.Azure != nil {
		if err := validateAzureConfig(cp.Azure); err != nil {
			return fmt.Errorf("%w: %v", ErrAzureConfig, err)
		}
	}

	if cp.GCP != nil {
		if err := validateGCPConfig(cp.GCP); err != nil {
			return fmt.Errorf("%w: %v", ErrGCPConfig, err)
		}
	}

	return nil
}

// validateVolcEngineConfig 验证火山引擎配置
func validateVolcEngineConfig(config *VolcEngineConfig) error {
	if config.AccessKey == "" {
		return fmt.Errorf("%w: access_key", ErrMissingRequired)
	}

	if config.SecretKey == "" {
		return fmt.Errorf("%w: secret_key", ErrMissingRequired)
	}

	if config.Region == "" {
		return fmt.Errorf("%w: region", ErrMissingRequired)
	}

	if config.Timeout <= 0 {
		config.Timeout = 30
	}

	if config.MaxRetries < 0 {
		config.MaxRetries = 5
	}

	if config.RetryDelay <= 0 {
		config.RetryDelay = 1
	}

	if config.BatchSize <= 0 {
		config.BatchSize = 50
	}

	if config.RateLimit <= 0 {
		config.RateLimit = 10
	}

	return validateSyncModeConfig(config.DefaultSyncMode, config.MaxHistoricalMonths)
}

// validateAliCloudConfig 验证阿里云配置
func validateAliCloudConfig(config *AliCloudConfig) error {
	if config.AccessKeyID == "" {
		return fmt.Errorf("%w: access_key_id", ErrMissingRequired)
	}

	if config.AccessKeySecret == "" {
		return fmt.Errorf("%w: access_key_secret", ErrMissingRequired)
	}

	if config.Region == "" {
		return fmt.Errorf("%w: region", ErrMissingRequired)
	}

	if config.Timeout <= 0 {
		config.Timeout = 30
	}

	if config.MaxRetries < 0 {
		config.MaxRetries = 5
	}

	if config.RetryDelay <= 0 {
		config.RetryDelay = 2
	}

	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}

	// 验证粒度配置
	if config.DefaultGranularity != "" {
		validGranularities := []string{"monthly", "daily", "both"}
		if !isValidValue(config.DefaultGranularity, validGranularities) {
			return fmt.Errorf("%w: default_granularity必须是%v之一", ErrInvalidValue, validGranularities)
		}
	}

	if config.DailySyncDays < 0 {
		config.DailySyncDays = 30
	}

	return validateSyncModeConfig(config.DefaultSyncMode, config.MaxHistoricalMonths)
}

// validateAWSConfig 验证AWS配置
func validateAWSConfig(config *AWSConfig) error {
	if config.AccessKey == "" {
		return fmt.Errorf("%w: access_key", ErrMissingRequired)
	}

	if config.SecretKey == "" {
		return fmt.Errorf("%w: secret_key", ErrMissingRequired)
	}

	if config.Region == "" {
		return fmt.Errorf("%w: region", ErrMissingRequired)
	}

	if config.Timeout <= 0 {
		config.Timeout = 30
	}

	return nil
}

// validateAzureConfig 验证Azure配置
func validateAzureConfig(config *AzureConfig) error {
	if config.ClientID == "" {
		return fmt.Errorf("%w: client_id", ErrMissingRequired)
	}

	if config.ClientSecret == "" {
		return fmt.Errorf("%w: client_secret", ErrMissingRequired)
	}

	if config.TenantID == "" {
		return fmt.Errorf("%w: tenant_id", ErrMissingRequired)
	}

	if config.SubscriptionID == "" {
		return fmt.Errorf("%w: subscription_id", ErrMissingRequired)
	}

	if config.Timeout <= 0 {
		config.Timeout = 30
	}

	return nil
}

// validateGCPConfig 验证GCP配置
func validateGCPConfig(config *GCPConfig) error {
	if config.ProjectID == "" {
		return fmt.Errorf("%w: project_id", ErrMissingRequired)
	}

	if config.ServiceAccountKey == "" {
		return fmt.Errorf("%w: service_account_key", ErrMissingRequired)
	}

	if config.Timeout <= 0 {
		config.Timeout = 30
	}

	return nil
}

// validateSchedulerConfig 验证调度器配置
func (c *Config) validateSchedulerConfig() error {
	if c.Scheduler == nil {
		return nil // 调度器配置是可选的
	}

	for i, job := range c.Scheduler.Jobs {
		if err := validateScheduledJob(&job); err != nil {
			return fmt.Errorf("job[%d]: %w", i, err)
		}
	}

	return nil
}

// validateScheduledJob 验证单个调度任务
func validateScheduledJob(job *ScheduledJob) error {
	if job.Name == "" {
		return fmt.Errorf("%w: name", ErrMissingRequired)
	}

	if job.Provider == "" {
		return fmt.Errorf("%w: provider", ErrMissingRequired)
	}

	if job.Cron == "" {
		return fmt.Errorf("%w: cron", ErrMissingRequired)
	}

	// 简单的Cron表达式验证
	if !isValidCronExpression(job.Cron) {
		return fmt.Errorf("%w: %s", ErrInvalidCron, job.Cron)
	}

	// 验证同步模式
	if job.Config.SyncMode != "" {
		validSyncModes := []string{"all_periods", "current_period", "range"}
		if !isValidValue(job.Config.SyncMode, validSyncModes) {
			return fmt.Errorf("%w: sync_mode必须是%v之一", ErrInvalidValue, validSyncModes)
		}
	}

	return nil
}

// validateWeChatConfig 验证微信配置
func (c *Config) validateWeChatConfig() error {
	if c.WeChat == nil || !c.WeChat.Enabled {
		return nil // 微信配置是可选的
	}

	wc := c.WeChat

	if wc.WebhookURL == "" {
		return fmt.Errorf("%w: webhook_url", ErrMissingRequired)
	}

	if wc.AlertThreshold < 0 || wc.AlertThreshold > 100 {
		return fmt.Errorf("%w: alert_threshold必须在0-100范围内", ErrInvalidValue)
	}

	if wc.MaxRetries < 0 {
		wc.MaxRetries = 3
	}

	if wc.RetryDelay <= 0 {
		wc.RetryDelay = 2
	}

	// 验证通知格式
	if wc.NotificationFormat != "" {
		validFormats := []string{"markdown", "template_card", "auto"}
		if !isValidValue(wc.NotificationFormat, validFormats) {
			return fmt.Errorf("%w: notification_format必须是%v之一", ErrInvalidValue, validFormats)
		}
	}

	return nil
}

// 工具函数：验证同步模式配置
func validateSyncModeConfig(syncMode string, maxMonths int) error {
	if syncMode != "" {
		validSyncModes := []string{"all_periods", "current_period", "range"}
		if !isValidValue(syncMode, validSyncModes) {
			return fmt.Errorf("%w: default_sync_mode必须是%v之一", ErrInvalidValue, validSyncModes)
		}
	}

	if maxMonths < 0 {
		return fmt.Errorf("%w: max_historical_months不能为负数", ErrInvalidValue)
	}

	return nil
}

// 工具函数：检查值是否在有效列表中
func isValidValue(value string, validValues []string) bool {
	for _, valid := range validValues {
		if value == valid {
			return true
		}
	}
	return false
}

// 工具函数：简单的Cron表达式验证
func isValidCronExpression(cron string) bool {
	// 简单验证：检查是否有5或6个字段（分 时 日 月 周 [年]）
	fields := strings.Fields(cron)
	return len(fields) == 5 || len(fields) == 6
}
