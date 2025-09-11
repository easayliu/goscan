package models

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// ScheduledJob represents a scheduled synchronization job
type ScheduledJob struct {
	ID          uint           `gorm:"primaryKey" json:"id"`
	Name        string         `gorm:"uniqueIndex;not null" json:"name"`
	Description string         `json:"description"`
	Provider    string         `gorm:"not null" json:"provider"`
	CronExpr    string         `gorm:"not null" json:"cron_expr"`
	SyncMode    string         `gorm:"default:sync-optimal" json:"sync_mode"`
	Config      datatypes.JSON `json:"config"` // Job-specific configuration
	Enabled     bool           `gorm:"default:true" json:"enabled"`
	LastRunAt   *time.Time     `json:"last_run_at"`
	NextRunAt   *time.Time     `json:"next_run_at"`
	RunCount    int            `gorm:"default:0" json:"run_count"`
	FailCount   int            `gorm:"default:0" json:"fail_count"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

// TableName returns the table name for ScheduledJob model
func (ScheduledJob) TableName() string {
	return "scheduled_jobs"
}

// JobConfig represents job-specific configuration
type JobConfig struct {
	UseDistributed bool   `json:"use_distributed"`
	CreateTable    bool   `json:"create_table"`
	ForceUpdate    bool   `json:"force_update"`
	Granularity    string `json:"granularity,omitempty"`  // For AliCloud
	SyncDays       int    `json:"sync_days,omitempty"`    // For AliCloud daily sync
	BillPeriod     string `json:"bill_period,omitempty"`  // Specific period YYYY-MM
	StartPeriod    string `json:"start_period,omitempty"` // Range start
	EndPeriod      string `json:"end_period,omitempty"`   // Range end
}
