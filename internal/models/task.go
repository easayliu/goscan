package models

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// TaskStatus represents the status of a sync task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// SyncTask represents a synchronization task execution record
type SyncTask struct {
	ID          uint           `gorm:"primaryKey" json:"id"`
	TaskID      string         `gorm:"uniqueIndex;not null" json:"task_id"` // UUID
	JobID       *uint          `json:"job_id"`                              // Reference to ScheduledJob if triggered by scheduler
	Provider    string         `gorm:"not null" json:"provider"`
	SyncMode    string         `json:"sync_mode"`
	Status      TaskStatus     `gorm:"default:pending" json:"status"`
	Progress    int            `gorm:"default:0" json:"progress"` // 0-100
	Message     string         `json:"message"`
	Config      datatypes.JSON `json:"config"` // Task configuration
	Result      datatypes.JSON `json:"result"` // Task execution result
	StartedAt   *time.Time     `json:"started_at"`
	CompletedAt *time.Time     `json:"completed_at"`
	Duration    int64          `json:"duration"` // Duration in milliseconds
	ErrorMsg    string         `json:"error_msg"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index" json:"-"`
}

// TableName returns the table name for SyncTask model
func (SyncTask) TableName() string {
	return "sync_tasks"
}

// TaskResult represents the result of a sync task
type TaskResult struct {
	TotalRecords     int64  `json:"total_records"`
	ProcessedRecords int64  `json:"processed_records"`
	InsertedRecords  int64  `json:"inserted_records"`
	UpdatedRecords   int64  `json:"updated_records"`
	ErrorRecords     int64  `json:"error_records"`
	TableName        string `json:"table_name"`
	BillPeriod       string `json:"bill_period,omitempty"`
}
