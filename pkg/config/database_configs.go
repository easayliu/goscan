package config

// database_configs.go 包含数据库相关的配置
// ClickHouseConfig 在 config.go 中定义，这里只包含数据库特定的验证和扩展方法

// Validate 验证ClickHouse配置
func (c *ClickHouseConfig) Validate() error {
	if len(c.Hosts) == 0 {
		return ErrMissingRequired
	}

	if c.Port <= 0 || c.Port > 65535 {
		return ErrInvalidValue
	}

	if c.Database == "" {
		return ErrMissingRequired
	}

	if c.Protocol != "" && c.Protocol != "native" && c.Protocol != "http" {
		return ErrInvalidValue
	}

	return nil
}