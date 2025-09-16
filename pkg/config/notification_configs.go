package config

// WeChatConfig 微信通知配置
type WeChatConfig struct {
	WebhookURL         string   `json:"webhook_url" yaml:"webhook_url"`                 // 企业微信机器人webhook地址
	Enabled            bool     `json:"enabled" yaml:"enabled"`                         // 是否启用微信通知
	MentionUsers       []string `json:"mention_users" yaml:"mention_users"`             // @用户列表
	AlertThreshold     float64  `json:"alert_threshold" yaml:"alert_threshold"`         // 告警阈值（百分比）
	MaxRetries         int      `json:"max_retries" yaml:"max_retries"`                 // 最大重试次数
	RetryDelay         int      `json:"retry_delay" yaml:"retry_delay"`                 // 重试延迟（秒）
	NotificationFormat string   `json:"notification_format" yaml:"notification_format"` // 通知格式：markdown/template_card/auto
}

// NewWeChatConfig 创建微信配置，使用环境变量填充默认值
func NewWeChatConfig() *WeChatConfig {
	return &WeChatConfig{
		WebhookURL:         getEnv("WECHAT_WEBHOOK_URL", ""),
		Enabled:            getEnvBool("WECHAT_ENABLED", false),
		MentionUsers:       parseStringList(getEnv("WECHAT_MENTION_USERS", "")),
		AlertThreshold:     getEnvFloat("WECHAT_ALERT_THRESHOLD", 20.0),
		MaxRetries:         getEnvInt("WECHAT_MAX_RETRIES", 3),
		RetryDelay:         getEnvInt("WECHAT_RETRY_DELAY", 2),
		NotificationFormat: getEnv("WECHAT_NOTIFICATION_FORMAT", "auto"),
	}
}

// Validate 验证微信配置
func (wc *WeChatConfig) Validate() error {
	if !wc.Enabled {
		return nil // 如果未启用，跳过验证
	}

	if wc.WebhookURL == "" {
		return ErrMissingRequired
	}

	if wc.AlertThreshold < 0 || wc.AlertThreshold > 100 {
		return ErrInvalidValue
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
			return ErrInvalidValue
		}
	}

	return nil
}