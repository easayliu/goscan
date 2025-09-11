package models

import (
	"time"

	"gorm.io/gorm"
)

// Setting represents system configuration settings
type Setting struct {
	ID        uint           `gorm:"primaryKey" json:"id"`
	Key       string         `gorm:"uniqueIndex;not null" json:"key"`
	Value     string         `gorm:"type:text" json:"value"`
	Category  string         `gorm:"default:system" json:"category"`
	Label     string         `json:"label"`
	Type      string         `gorm:"default:string" json:"type"` // string, int, bool, json
	Required  bool           `gorm:"default:false" json:"required"`
	Encrypted bool           `gorm:"default:false" json:"encrypted"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

// TableName returns the table name for Setting model
func (Setting) TableName() string {
	return "settings"
}

// Common setting keys
const (
	// ClickHouse settings
	SettingClickHouseHosts    = "clickhouse.hosts"
	SettingClickHousePort     = "clickhouse.port"
	SettingClickHouseDatabase = "clickhouse.database"
	SettingClickHouseUsername = "clickhouse.username"
	SettingClickHousePassword = "clickhouse.password"
	SettingClickHouseCluster  = "clickhouse.cluster"
	SettingClickHouseProtocol = "clickhouse.protocol"

	// Application settings
	SettingAppLogLevel                = "app.log_level"
	SettingAppMaxConcurrentTasks      = "app.max_concurrent_tasks"
	SettingAppTaskTimeout             = "app.task_timeout"
	SettingAppGracefulShutdownTimeout = "app.graceful_shutdown_timeout"
)
