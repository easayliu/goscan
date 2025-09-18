package config

// database_configs.go contains database-related configurations
// ClickHouseConfig is defined in config.go, this file only contains database-specific validation and extension methods

// Validate validates ClickHouse configuration
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
