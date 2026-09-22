package models

import "time"

// SystemStatus represents the system status response
type SystemStatus struct {
	Service   string           `json:"service" example:"goscan"`
	Version   string           `json:"version" example:"1.0.0"`
	Status    string           `json:"status" example:"running"`
	Timestamp time.Time        `json:"timestamp" example:"2025-09-11T08:13:24Z"`
	Uptime    int64            `json:"uptime" example:"3600"`
	Tasks     TasksStatus      `json:"tasks"`
	Scheduler *SchedulerStatus `json:"scheduler,omitempty"`
}

// TasksStatus represents the tasks status
type TasksStatus struct {
	Running int `json:"running" example:"2"`
	Total   int `json:"total" example:"10"`
}

// SchedulerStatus represents the scheduler status
type SchedulerStatus struct {
	Running   bool      `json:"running" example:"true"`
	JobCount  int       `json:"job_count" example:"3"`
	Entries   int       `json:"entries" example:"3"`
	Timestamp time.Time `json:"timestamp" example:"2025-09-11T08:13:24Z"`
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status    string    `json:"status" example:"healthy"`
	Timestamp time.Time `json:"timestamp" example:"2025-09-11T08:13:24Z"`
	Service   string    `json:"service" example:"goscan"`
	Version   string    `json:"version" example:"1.0.0"`
}

// HealthCheckResponse represents the health check response with detailed checks
type HealthCheckResponse struct {
	Status    string                 `json:"status" example:"healthy"`
	Timestamp time.Time              `json:"timestamp" example:"2025-09-11T08:13:24Z"`
	Checks    map[string]interface{} `json:"checks"`
}

// TaskRequest represents a task creation request
type TaskRequest struct {
	Name        string                 `json:"name" example:"volcengine_sync_task" validate:"required"`
	Type        string                 `json:"type" example:"sync" validate:"required"`
	Provider    string                 `json:"provider" example:"volcengine" validate:"required"`
	Parameters  map[string]interface{} `json:"parameters"`
	Schedule    string                 `json:"schedule,omitempty" example:"0 2 * * *"`
	Description string                 `json:"description,omitempty" example:"Sync VolcEngine billing data"`
}

// TaskResponse represents a task response
type TaskResponse struct {
	ID          string                 `json:"id" example:"task_123456"`
	Name        string                 `json:"name" example:"volcengine_sync_task"`
	Type        string                 `json:"type" example:"sync"`
	Provider    string                 `json:"provider" example:"volcengine"`
	Status      string                 `json:"status" example:"running"`
	Parameters  map[string]interface{} `json:"parameters"`
	Schedule    string                 `json:"schedule,omitempty" example:"0 2 * * *"`
	Description string                 `json:"description,omitempty" example:"Sync VolcEngine billing data"`
	CreatedAt   time.Time              `json:"created_at" example:"2025-09-11T08:13:24Z"`
	UpdatedAt   time.Time              `json:"updated_at" example:"2025-09-11T08:13:24Z"`
	StartedAt   *time.Time             `json:"started_at,omitempty" example:"2025-09-11T08:13:24Z"`
	CompletedAt *time.Time             `json:"completed_at,omitempty" example:"2025-09-11T08:13:30Z"`
}

// TaskListResponse represents a list of tasks response
type TaskListResponse struct {
	Tasks []TaskResponse `json:"tasks"`
	Count int            `json:"count" example:"5"`
}

// SyncTriggerRequest is the body POST /sync binds. The handler decodes into this
// type rather than an anonymous struct so the Swagger schema cannot drift away
// from what the endpoint actually reads.
type SyncTriggerRequest struct {
	// Which cloud to sync. Required.
	Provider string `json:"provider" example:"alicloud" enums:"volcengine,alicloud" validate:"required"`
	// standard fetches every period in full, sync-optimal compares record counts
	// and only fills the gap. Empty uses the provider default from the config.
	SyncMode string `json:"sync_mode,omitempty" example:"sync-optimal" enums:"standard,sync-optimal"`
	// AliCloud only, selects the target table: monthly writes alicloud_bill_monthly,
	// daily writes alicloud_bill_daily, both writes each in one run. Ignored for
	// VolcEngine, whose bills land in the single volcengine_bill table.
	Granularity string `json:"granularity,omitempty" example:"both" enums:"monthly,daily,both"`
	// A single billing period. The format also picks the granularity when the
	// field above is empty: YYYY-MM is monthly, YYYY-MM-DD is one day.
	BillPeriod string `json:"bill_period,omitempty" example:"2026-09"`
	// Start of an inclusive period range, used instead of bill_period to backfill
	// several periods in one task.
	StartPeriod string `json:"start_period,omitempty" example:"2026-01"`
	// End of the inclusive period range.
	EndPeriod string `json:"end_period,omitempty" example:"2026-06"`
	// Re-fetch periods that already have data instead of skipping them.
	ForceUpdate bool `json:"force_update,omitempty" example:"false"`
	// Cap on records to sync; 0 means no cap.
	Limit int `json:"limit,omitempty" example:"0"`
	// Write through the Distributed table instead of the local one.
	UseDistributed bool `json:"use_distributed,omitempty" example:"false"`
	// Create the target tables first if they are missing.
	CreateTable bool `json:"create_table,omitempty" example:"false"`
}

// SyncTriggerResponse is what POST /sync answers with: the task is registered,
// not finished, so the caller polls GET /tasks/{task_id} with the id returned here.
type SyncTriggerResponse struct {
	TaskID    string `json:"task_id" example:"6f1c8f0e-6c2a-4a1f-9a4d-1f0b7a2c3d4e"`
	Status    string `json:"status" example:"started"`
	Message   string `json:"message" example:"Sync triggered for provider alicloud"`
	Provider  string `json:"provider" example:"alicloud"`
	Timestamp string `json:"timestamp" example:"2026-09-11T08:13:24Z"`
}

// SyncStatusResponse represents sync status response
type SyncStatusResponse struct {
	Provider    string     `json:"provider" example:"volcengine"`
	Status      string     `json:"status" example:"running"`
	Progress    float64    `json:"progress" example:"75.5"`
	Message     string     `json:"message,omitempty" example:"Syncing in progress..."`
	StartedAt   *time.Time `json:"started_at,omitempty" example:"2025-09-11T08:13:24Z"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	RecordCount int        `json:"record_count" example:"1500"`
	ErrorCount  int        `json:"error_count" example:"0"`
}

// SyncHistoryResponse represents sync history response
type SyncHistoryResponse struct {
	History []SyncHistoryEntry `json:"history"`
	Count   int                `json:"count" example:"10"`
}

// SyncHistoryEntry represents a sync history entry
type SyncHistoryEntry struct {
	ID          string     `json:"id" example:"sync_123456"`
	Provider    string     `json:"provider" example:"volcengine"`
	BillPeriod  string     `json:"bill_period" example:"2025-09"`
	Status      string     `json:"status" example:"completed"`
	RecordCount int        `json:"record_count" example:"1500"`
	ErrorCount  int        `json:"error_count" example:"0"`
	Duration    int64      `json:"duration" example:"125"`
	StartedAt   time.Time  `json:"started_at" example:"2025-09-11T08:13:24Z"`
	CompletedAt *time.Time `json:"completed_at,omitempty" example:"2025-09-11T08:15:29Z"`
	Message     string     `json:"message,omitempty" example:"Sync completed"`
}

// JobRequest represents a scheduled job creation request
type JobRequest struct {
	Name        string                 `json:"name" example:"daily_volcengine_sync" validate:"required"`
	CronExpr    string                 `json:"cron_expr" example:"0 2 * * *" validate:"required"`
	Provider    string                 `json:"provider" example:"volcengine" validate:"required"`
	TaskType    string                 `json:"task_type" example:"sync" validate:"required"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Description string                 `json:"description,omitempty" example:"Daily VolcEngine billing sync at 2 AM"`
	Enabled     bool                   `json:"enabled" example:"true"`
}

// JobResponse represents a scheduled job response
type JobResponse struct {
	ID          string                 `json:"id" example:"job_123456"`
	Name        string                 `json:"name" example:"daily_volcengine_sync"`
	CronExpr    string                 `json:"cron_expr" example:"0 2 * * *"`
	Provider    string                 `json:"provider" example:"volcengine"`
	TaskType    string                 `json:"task_type" example:"sync"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Description string                 `json:"description,omitempty" example:"Daily VolcEngine billing sync at 2 AM"`
	Enabled     bool                   `json:"enabled" example:"true"`
	NextRun     *time.Time             `json:"next_run,omitempty" example:"2025-09-12T02:00:00Z"`
	LastRun     *time.Time             `json:"last_run,omitempty" example:"2025-09-11T02:00:00Z"`
	CreatedAt   time.Time              `json:"created_at" example:"2025-09-11T08:13:24Z"`
	UpdatedAt   time.Time              `json:"updated_at" example:"2025-09-11T08:13:24Z"`
}

// JobListResponse represents a list of scheduled jobs response
type JobListResponse struct {
	Jobs  []JobResponse `json:"jobs"`
	Count int           `json:"count" example:"3"`
}

// SchedulerMetricsResponse represents scheduler metrics response
type SchedulerMetricsResponse struct {
	SchedulerStatus *SchedulerStatus `json:"scheduler_status"`
	TotalJobs       int              `json:"total_jobs" example:"10"`
	ActiveJobs      int              `json:"active_jobs" example:"8"`
	InactiveJobs    int              `json:"inactive_jobs" example:"2"`
	Timestamp       time.Time        `json:"timestamp" example:"2025-09-11T08:13:24Z"`
	Uptime          string           `json:"uptime" example:"3h45m"`
}

// ConfigResponse represents configuration response (sensitive data masked)
type ConfigResponse struct {
	ClickHouse    ClickHouseConfig    `json:"clickhouse"`
	CloudProvider CloudProviderConfig `json:"cloud_providers"`
	App           AppConfig           `json:"app"`
}

// ClickHouseConfig represents ClickHouse configuration
type ClickHouseConfig struct {
	Hosts    []string `json:"hosts" example:"localhost"`
	Port     int      `json:"port" example:"9000"`
	Database string   `json:"database" example:"default"`
	Username string   `json:"username" example:"default"`
	Protocol string   `json:"protocol" example:"native"`
	Cluster  string   `json:"cluster,omitempty" example:"my_cluster"`
}

// CloudProviderConfig represents cloud provider configuration
type CloudProviderConfig struct {
	VolcEngine *ProviderConfig `json:"volcengine,omitempty"`
	AliCloud   *ProviderConfig `json:"alicloud,omitempty"`
	AWS        *ProviderConfig `json:"aws,omitempty"`
	Azure      *ProviderConfig `json:"azure,omitempty"`
	GCP        *ProviderConfig `json:"gcp,omitempty"`
}

// ProviderConfig represents a single cloud provider configuration
type ProviderConfig struct {
	Region   string `json:"region" example:"cn-north-1"`
	Host     string `json:"host,omitempty" example:"billing.volcengineapi.com"`
	Endpoint string `json:"endpoint,omitempty" example:"ecs.cn-hangzhou.aliyuncs.com"`
	Timeout  int    `json:"timeout" example:"30"`
	Enabled  bool   `json:"enabled" example:"true"`
	// Note: AccessKey and SecretKey are masked for security
	HasCredentials bool `json:"has_credentials" example:"true"`
}

// AppConfig represents application configuration
type AppConfig struct {
	LogLevel string `json:"log_level" example:"info"`
	LogFile  string `json:"log_file,omitempty"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   bool   `json:"error" example:"true"`
	Message string `json:"message" example:"Parameter validation failed"`
	Code    int    `json:"code" example:"400"`
	Details string `json:"details,omitempty" example:"Provider field cannot be empty"`
}

// MessageResponse represents a simple message response
type MessageResponse struct {
	Message string `json:"message" example:"Operation successful"`
	Success bool   `json:"success" example:"true"`
}

// WeChatNotificationRequest represents WeChat Work notification trigger request parameters
// @Description WeChat Work cost report notification trigger request, supports specifying analysis date, cloud providers and alert threshold
type WeChatNotificationRequest struct {
	Date           string   `json:"date,omitempty" example:"2025-09-12" validate:"omitempty,len=10"`                                                     // Analysis date, format: YYYY-MM-DD, defaults to current date. Example: 2025-09-12
	Providers      []string `json:"providers,omitempty" example:"volcengine,alicloud" validate:"omitempty,dive,oneof=volcengine alicloud aws azure gcp"` // List of cloud providers to analyze, supports: volcengine (VolcEngine), alicloud (Alibaba Cloud), aws, azure, gcp, defaults to ["volcengine", "alicloud"]
	AlertThreshold float64  `json:"alert_threshold,omitempty" example:"10.0" validate:"omitempty,min=0,max=100"`                                         // Cost change alert threshold (percentage), triggers alert when cost change exceeds this value, defaults to system configuration value, range: 0-100
	ForceNotify    bool     `json:"force_notify,omitempty" example:"false"`                                                                              // Force send notification, when set to true will send notification even if alert threshold is not reached, defaults to false
	TestMode       bool     `json:"test_mode,omitempty" example:"false"`                                                                                 // Test mode, when set to true only tests WeChat Work connection without sending actual cost report, defaults to false
}

// WeChatTestRequest represents WeChat Work Webhook connection test request parameters
// @Description WeChat Work Webhook connection test request, used to verify Webhook URL availability
type WeChatTestRequest struct {
	CustomMessage string `json:"custom_message,omitempty" example:"This is a test message from cost monitoring system" validate:"omitempty,max=500"` // Custom test message content, maximum length 500 characters, defaults to system preset test message
	Timeout       int    `json:"timeout,omitempty" example:"10" validate:"omitempty,min=1,max=60"`                                                   // Connection timeout (seconds), range 1-60 seconds, defaults to 10 seconds
}

// WeChatNotificationStatusQueryParams represents WeChat Work notification status query parameters
// @Description Optional parameters for WeChat Work notification status query, used to filter and control returned data
type WeChatNotificationStatusQueryParams struct {
	HistoryLimit  int  `json:"history_limit,omitempty" example:"10" validate:"omitempty,min=1,max=50"` // Limit on number of history records returned, range 1-50, defaults to 10 entries
	IncludeConfig bool `json:"include_config,omitempty" example:"true"`                                // Whether to include detailed configuration information, defaults to true
}

// WeChatNotificationStatusResponse represents WeChat notification status response
type WeChatNotificationStatusResponse struct {
	Enabled           bool                             `json:"enabled" example:"true"`                        // Whether WeChat notification feature is enabled
	WebhookConfigured bool                             `json:"webhook_configured" example:"true"`             // Whether Webhook is configured
	WebhookURL        string                           `json:"webhook_url" example:"https://qyapi***webhook"` // Masked Webhook URL
	AlertThreshold    float64                          `json:"alert_threshold" example:"10.0"`                // Alert threshold
	MentionUsers      []string                         `json:"mention_users" example:"user1,user2"`           // List of users to mention
	MaxRetries        int                              `json:"max_retries" example:"3"`                       // Maximum retry attempts
	RetryDelay        int                              `json:"retry_delay" example:"5"`                       // Retry delay time (seconds)
	ExecutorStatus    string                           `json:"executor_status" example:"ready"`               // Executor status
	ExecutorEnabled   bool                             `json:"executor_enabled" example:"true"`               // Whether executor is enabled
	RecentHistory     []WeChatNotificationHistoryEntry `json:"recent_history"`                                // Recent notification history
	HistoryCount      int                              `json:"history_count" example:"5"`                     // Number of history records
	Timestamp         time.Time                        `json:"timestamp" example:"2025-09-12T08:13:24Z"`      // Status query timestamp
}

// WeChatNotificationHistoryEntry represents a WeChat notification history entry
type WeChatNotificationHistoryEntry struct {
	ID               string    `json:"id" example:"task_123456"`                                   // Task ID
	Status           string    `json:"status" example:"completed"`                                 // Execution status
	StartTime        time.Time `json:"start_time" example:"2025-09-12T08:13:24Z"`                  // Start time
	EndTime          time.Time `json:"end_time" example:"2025-09-12T08:15:30Z"`                    // End time
	Duration         string    `json:"duration" example:"2m6s"`                                    // Execution duration
	Message          string    `json:"message,omitempty" example:"Notification sent successfully"` // Execution message
	RecordsProcessed int       `json:"records_processed,omitempty" example:"1500"`                 // Number of records processed
	Error            string    `json:"error,omitempty" example:"Connection timeout"`               // Error message
}
