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

	if err := c.validateJobCredentials(); err != nil {
		return err
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

	// 一个凭据全空的 provider 块就是「这朵云没接」，跳过不校验：示例配置和
	// 部署用的 ConfigMap 都会把五朵云的块都列出来，只填其中一两朵。
	// 填了一半的（比如只有 access_key 没有 secret_key）照样报错。

	if credentialsPresent(cp.VolcEngine != nil, func() bool {
		return cp.VolcEngine.AccessKey != "" || cp.VolcEngine.SecretKey != ""
	}) {
		if err := validateVolcEngineConfig(cp.VolcEngine); err != nil {
			return fmt.Errorf("%w: %v", ErrVolcEngineConfig, err)
		}
	}

	if credentialsPresent(cp.AliCloud != nil, func() bool {
		return cp.AliCloud.AccessKeyID != "" || cp.AliCloud.AccessKeySecret != ""
	}) {
		if err := validateAliCloudConfig(cp.AliCloud); err != nil {
			return fmt.Errorf("%w: %v", ErrAliCloudConfig, err)
		}
	}

	if credentialsPresent(cp.AWS != nil, func() bool {
		return cp.AWS.AccessKey != "" || cp.AWS.SecretKey != ""
	}) {
		if err := validateAWSConfig(cp.AWS); err != nil {
			return fmt.Errorf("%w: %v", ErrAWSConfig, err)
		}
	}

	if credentialsPresent(cp.Azure != nil, func() bool {
		return cp.Azure.ClientID != "" || cp.Azure.ClientSecret != "" ||
			cp.Azure.TenantID != "" || cp.Azure.SubscriptionID != ""
	}) {
		if err := validateAzureConfig(cp.Azure); err != nil {
			return fmt.Errorf("%w: %v", ErrAzureConfig, err)
		}
	}

	if credentialsPresent(cp.GCP != nil, func() bool {
		return cp.GCP.ProjectID != "" || cp.GCP.ServiceAccountKey != ""
	}) {
		if err := validateGCPConfig(cp.GCP); err != nil {
			return fmt.Errorf("%w: %v", ErrGCPConfig, err)
		}
	}

	return nil
}

// credentialsPresent 判断一个 provider 块是否需要校验：块存在，且至少填了一项凭据。
func credentialsPresent(present bool, anyCredential func() bool) bool {
	return present && anyCredential()
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

// validateJobCredentials 交叉检查：调度里配了哪朵云的任务，那朵云的凭据就必须齐全。
//
// 只看 cloud_providers 是判断不出来的 —— 凭据全空的块会被当成「这朵云没接」跳过，
// 而 K8s 上 Secret 忘了填正好就是这个样子：--check 通过、Pod 正常起来，凌晨两点
// 才发现一条账单都没同步。有定时任务指着它，就说明这朵云是要用的。
func (c *Config) validateJobCredentials() error {
	if c.Scheduler == nil || !c.Scheduler.Enabled {
		return nil
	}

	for _, job := range c.Scheduler.Jobs {
		switch job.Provider {
		case "volcengine":
			ve := c.GetVolcEngineConfig()
			if ve.AccessKey == "" || ve.SecretKey == "" {
				return fmt.Errorf("%w: 任务 %q 要同步火山引擎，但 access_key / secret_key 没配"+
					"（容器里检查 VOLCENGINE_ACCESS_KEY、VOLCENGINE_SECRET_KEY 这两个环境变量挂上没有）",
					ErrVolcEngineConfig, job.Name)
			}
		case "alicloud":
			ac := c.GetAliCloudConfig()
			if ac.AccessKeyID == "" || ac.AccessKeySecret == "" {
				return fmt.Errorf("%w: 任务 %q 要同步阿里云，但 access_key_id / access_key_secret 没配"+
					"（容器里检查 ALICLOUD_ACCESS_KEY_ID、ALICLOUD_ACCESS_KEY_SECRET 这两个环境变量挂上没有）",
					ErrAliCloudConfig, job.Name)
			}
		}
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

	// 验证同步模式。执行器认的是 standard / sync-optimal，
	// cost_report 不是同步模式，是通知任务借这个字段传的标记。
	//
	// LegacySyncModes 里那几个是这个字段以前接受的取值，执行器其实从来没认过
	// （配了的任务每次都在运行时失败）。仍然放行是为了别让老配置在升级后卡在
	// initContainer 的 --check 上起不来；调度器加载时会把它们折算成 standard 并告警。
	if job.Config.SyncMode != "" {
		valid := append([]string{"standard", "sync-optimal", "cost_report"}, LegacySyncModes...)
		if !isValidValue(job.Config.SyncMode, valid) {
			return fmt.Errorf("%w: sync_mode必须是%v之一", ErrInvalidValue, valid[:3])
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
