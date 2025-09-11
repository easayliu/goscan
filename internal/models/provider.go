package models

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Provider represents cloud service provider configuration
type Provider struct {
	ID          uint           `gorm:"primaryKey" json:"id"`
	Name        string         `gorm:"uniqueIndex;not null" json:"name"`
	Type        string         `gorm:"not null" json:"type"` // volcengine, alicloud, aws, azure, gcp
	DisplayName string         `json:"display_name"`
	AccessKey   string         `json:"access_key"` // Will be encrypted
	SecretKey   string         `json:"secret_key"` // Will be encrypted
	Region      string         `json:"region"`
	Config      datatypes.JSON `json:"config"` // Additional provider-specific config
	Enabled     bool           `gorm:"default:true" json:"enabled"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

// TableName returns the table name for Provider model
func (Provider) TableName() string {
	return "providers"
}

// ProviderConfig represents additional configuration for each provider
type ProviderConfig struct {
	// VolcEngine specific
	Host       string `json:"host,omitempty"`
	BatchSize  int    `json:"batch_size,omitempty"`
	MaxRetries int    `json:"max_retries,omitempty"`

	// AliCloud specific
	Endpoint    string `json:"endpoint,omitempty"`
	Granularity string `json:"granularity,omitempty"`

	// Common
	Timeout int `json:"timeout,omitempty"`
}
