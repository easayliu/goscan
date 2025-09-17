package config

// WeChatConfig represents WeChat notification configuration
type WeChatConfig struct {
	WebhookURL         string   `json:"webhook_url" yaml:"webhook_url"`                 // WeChat Work bot webhook URL
	Enabled            bool     `json:"enabled" yaml:"enabled"`                         // whether WeChat notification is enabled
	MentionUsers       []string `json:"mention_users" yaml:"mention_users"`             // list of users to mention
	AlertThreshold     float64  `json:"alert_threshold" yaml:"alert_threshold"`         // alert threshold (percentage)
	MaxRetries         int      `json:"max_retries" yaml:"max_retries"`                 // maximum retry attempts
	RetryDelay         int      `json:"retry_delay" yaml:"retry_delay"`                 // retry delay (seconds)
	NotificationFormat string   `json:"notification_format" yaml:"notification_format"` // notification format: markdown/template_card/auto
}

// NewWeChatConfig creates WeChat configuration with default values populated from environment variables
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

// Validate validates WeChat configuration
func (wc *WeChatConfig) Validate() error {
	if !wc.Enabled {
		return nil // skip validation if not enabled
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

	// validate notification format
	if wc.NotificationFormat != "" {
		validFormats := []string{"markdown", "template_card", "auto"}
		if !isValidValue(wc.NotificationFormat, validFormats) {
			return ErrInvalidValue
		}
	}

	return nil
}
