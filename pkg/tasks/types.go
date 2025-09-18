package tasks

import (
	"context"
	"time"
)

// TaskType represents the type of task
type TaskType string

const (
	TaskTypeSync         TaskType = "sync"
	TaskTypeNotification TaskType = "notification"
)

// TaskStatus represents the status of a task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TaskRequest represents a request to execute a task
type TaskRequest struct {
	ID       string     `json:"id"`
	Type     TaskType   `json:"type"`
	Provider string     `json:"provider"`
	Config   TaskConfig `json:"config"`
}

// TaskConfig holds task-specific configuration
type TaskConfig struct {
	SyncMode       string `json:"sync_mode"`
	UseDistributed bool   `json:"use_distributed"`
	CreateTable    bool   `json:"create_table"`
	ForceUpdate    bool   `json:"force_update"`
	Granularity    string `json:"granularity,omitempty"` // For AliCloud
	BillPeriod     string `json:"bill_period,omitempty"`
	StartPeriod    string `json:"start_period,omitempty"`
	EndPeriod      string `json:"end_period,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

// Task represents a running or completed task
type Task struct {
	ID        string             `json:"id"`
	Type      TaskType           `json:"type"`
	Provider  string             `json:"provider"`
	Status    TaskStatus         `json:"status"`
	StartTime time.Time          `json:"start_time"`
	EndTime   time.Time          `json:"end_time"`
	Duration  time.Duration      `json:"duration"`
	Config    TaskConfig         `json:"config"`
	Result    *TaskResult        `json:"result,omitempty"`
	Error     string             `json:"error,omitempty"`
	Cancel    context.CancelFunc `json:"-"`
}

// TaskResult holds the result of a completed task
type TaskResult struct {
	ID               string        `json:"id"`
	Type             string        `json:"type"`
	Status           string        `json:"status"`
	RecordsProcessed int           `json:"records_processed"`
	RecordsFetched   int           `json:"records_fetched"`
	Duration         time.Duration `json:"duration"`
	Success          bool          `json:"success"`
	Message          string        `json:"message"`
	Error            string        `json:"error,omitempty"`
	StartedAt        time.Time     `json:"started_at"`
	CompletedAt      time.Time     `json:"completed_at"`
}

// SmartTimeSelection represents smart time selection for sync-optimal mode
type SmartTimeSelection struct {
	YesterdayPeriod string `json:"yesterday_period"`  // Format: 2006-01-02 for daily
	LastMonthPeriod string `json:"last_month_period"` // Format: 2006-01 for monthly
}

// DataComparisonResult holds data comparison results
type DataComparisonResult struct {
	TotalRecords     int       `json:"total_records"`
	NewRecords       int       `json:"new_records"`
	UpdatedRecords   int       `json:"updated_records"`
	UnchangedRecords int       `json:"unchanged_records"`
	ErrorRecords     int       `json:"error_records"`
	SuccessRate      float64   `json:"success_rate"`
	ComparisonTime   time.Time `json:"comparison_time"`
	Details          string    `json:"details"`
}

// PreSyncCheckResult holds pre-sync check results
type PreSyncCheckResult struct {
	CanProceed      bool      `json:"can_proceed"`
	ChecksPassed    int       `json:"checks_passed"`
	ChecksFailed    int       `json:"checks_failed"`
	TotalChecks     int       `json:"total_checks"`
	CheckTime       time.Time `json:"check_time"`
	FailureReasons  []string  `json:"failure_reasons,omitempty"`
	Recommendations []string  `json:"recommendations,omitempty"`
}

// SyncConfig holds synchronization configuration
type SyncConfig struct {
	Provider       string
	SyncMode       string
	UseDistributed bool
	CreateTable    bool
	ForceUpdate    bool
	Granularity    string
	BillPeriod     string
	StartPeriod    string
	EndPeriod      string
	Limit          int
}

// TableConfig holds table creation configuration
type TableConfig struct {
	UseDistributed       bool
	LocalTableName       string
	DistributedTableName string
	ClusterName          string
}

// SyncResult holds synchronization results
type SyncResult struct {
	Success          bool                   `json:"success"`
	RecordsProcessed int                    `json:"records_processed"`
	RecordsFetched   int                    `json:"records_fetched"`
	Duration         time.Duration          `json:"duration"`
	Message          string                 `json:"message"`
	Error            string                 `json:"error,omitempty"`
	StartedAt        time.Time              `json:"started_at"`
	CompletedAt      time.Time              `json:"completed_at"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// DataCheckResult holds data check results
type DataCheckResult struct {
	Success      bool                   `json:"success"`
	TotalRecords int                    `json:"total_records"`
	ChecksPassed int                    `json:"checks_passed"`
	ChecksFailed int                    `json:"checks_failed"`
	Issues       []string               `json:"issues,omitempty"`
	CheckTime    time.Time              `json:"check_time"`
	Details      map[string]interface{} `json:"details,omitempty"`
}
