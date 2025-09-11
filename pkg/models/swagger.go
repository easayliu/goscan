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

// TaskRequest represents a task creation request
type TaskRequest struct {
	Name        string                 `json:"name" example:"volcengine_sync_task" validate:"required"`
	Type        string                 `json:"type" example:"sync" validate:"required"`
	Provider    string                 `json:"provider" example:"volcengine" validate:"required"`
	Parameters  map[string]interface{} `json:"parameters"`
	Schedule    string                 `json:"schedule,omitempty" example:"0 2 * * *"`
	Description string                 `json:"description,omitempty" example:"同步火山云账单数据"`
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
	Description string                 `json:"description,omitempty" example:"同步火山云账单数据"`
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

// SyncTriggerRequest represents a sync trigger request
type SyncTriggerRequest struct {
	Provider   string                 `json:"provider" example:"volcengine" validate:"required"`
	BillPeriod string                 `json:"bill_period,omitempty" example:"2025-09"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
	Force      bool                   `json:"force,omitempty" example:"false"`
}

// SyncStatusResponse represents sync status response
type SyncStatusResponse struct {
	Provider    string     `json:"provider" example:"volcengine"`
	Status      string     `json:"status" example:"running"`
	Progress    float64    `json:"progress" example:"75.5"`
	Message     string     `json:"message,omitempty" example:"同步进行中..."`
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
	Message     string     `json:"message,omitempty" example:"同步完成"`
}

// JobRequest represents a scheduled job creation request
type JobRequest struct {
	Name        string                 `json:"name" example:"daily_volcengine_sync" validate:"required"`
	CronExpr    string                 `json:"cron_expr" example:"0 2 * * *" validate:"required"`
	Provider    string                 `json:"provider" example:"volcengine" validate:"required"`
	TaskType    string                 `json:"task_type" example:"sync" validate:"required"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Description string                 `json:"description,omitempty" example:"每日凌晨2点同步火山云账单"`
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
	Description string                 `json:"description,omitempty" example:"每日凌晨2点同步火山云账单"`
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
	Message string `json:"message" example:"参数验证失败"`
	Code    int    `json:"code" example:"400"`
	Details string `json:"details,omitempty" example:"provider字段不能为空"`
}

// MessageResponse represents a simple message response
type MessageResponse struct {
	Message string `json:"message" example:"操作成功"`
	Success bool   `json:"success" example:"true"`
}

// WeChatNotificationRequest 企业微信通知触发请求参数
// @Description 企业微信费用报告通知触发请求，支持指定分析日期、云服务商和告警阈值
type WeChatNotificationRequest struct {
	Date           string   `json:"date,omitempty" example:"2025-09-12" validate:"omitempty,len=10"`                     // 分析日期，格式：YYYY-MM-DD，默认为当前日期。示例：2025-09-12
	Providers      []string `json:"providers,omitempty" example:"volcengine,alicloud" validate:"omitempty,dive,oneof=volcengine alicloud aws azure gcp"` // 要分析的云服务商列表，支持：volcengine（火山云）、alicloud（阿里云）、aws、azure、gcp，默认为 ["volcengine", "alicloud"]
	AlertThreshold float64  `json:"alert_threshold,omitempty" example:"10.0" validate:"omitempty,min=0,max=100"`         // 费用变化告警阈值（百分比），当费用变化超过此值时触发告警，默认使用系统配置值，范围：0-100
	ForceNotify    bool     `json:"force_notify,omitempty" example:"false"`                                              // 强制发送通知，设为true时即使未达到告警阈值也会发送通知，默认为false
	TestMode       bool     `json:"test_mode,omitempty" example:"false"`                                                 // 测试模式，设为true时仅测试企业微信连接而不发送实际的费用报告，默认为false
}

// WeChatTestRequest 企业微信Webhook连接测试请求参数  
// @Description 企业微信Webhook连接测试请求，用于验证Webhook URL的可用性
type WeChatTestRequest struct {
	CustomMessage string `json:"custom_message,omitempty" example:"这是一条来自费用监控系统的测试消息" validate:"omitempty,max=500"` // 自定义测试消息内容，最大长度500字符，默认使用系统预设的测试消息
	Timeout       int    `json:"timeout,omitempty" example:"10" validate:"omitempty,min=1,max=60"`                         // 连接超时时间（秒），范围1-60秒，默认为10秒  
}

// WeChatNotificationStatusQueryParams 企业微信通知状态查询参数
// @Description 企业微信通知状态查询的可选参数，用于筛选和控制返回的数据
type WeChatNotificationStatusQueryParams struct {
	HistoryLimit int  `json:"history_limit,omitempty" example:"10" validate:"omitempty,min=1,max=50"` // 返回的历史记录数量限制，范围1-50，默认为10条
	IncludeConfig bool `json:"include_config,omitempty" example:"true"`                                // 是否包含详细的配置信息，默认为true
}

// WeChatNotificationStatusResponse represents WeChat notification status response
type WeChatNotificationStatusResponse struct {
	Enabled            bool                      `json:"enabled" example:"true"`                                        // 微信通知功能是否启用
	WebhookConfigured  bool                      `json:"webhook_configured" example:"true"`                            // Webhook是否已配置
	WebhookURL         string                    `json:"webhook_url" example:"https://qyapi***webhook"`                // 脱敏后的Webhook URL
	AlertThreshold     float64                   `json:"alert_threshold" example:"10.0"`                               // 告警阈值
	SendTime           string                    `json:"send_time" example:"09:00"`                                     // 发送时间
	MentionUsers       []string                  `json:"mention_users" example:"user1,user2"`                          // 提及用户列表
	MaxRetries         int                       `json:"max_retries" example:"3"`                                       // 最大重试次数
	RetryDelay         int                       `json:"retry_delay" example:"5"`                                       // 重试延迟时间（秒）
	ExecutorStatus     string                    `json:"executor_status" example:"ready"`                              // 执行器状态
	ExecutorEnabled    bool                      `json:"executor_enabled" example:"true"`                              // 执行器是否启用
	RecentHistory      []WeChatNotificationHistoryEntry `json:"recent_history"`                                        // 最近通知历史
	HistoryCount       int                       `json:"history_count" example:"5"`                                     // 历史记录数量
	Timestamp          time.Time                 `json:"timestamp" example:"2025-09-12T08:13:24Z"`                     // 状态查询时间戳
}

// WeChatNotificationHistoryEntry represents a WeChat notification history entry
type WeChatNotificationHistoryEntry struct {
	ID               string    `json:"id" example:"task_123456"`                      // 任务ID
	Status           string    `json:"status" example:"completed"`                    // 执行状态
	StartTime        time.Time `json:"start_time" example:"2025-09-12T08:13:24Z"`     // 开始时间
	EndTime          time.Time `json:"end_time" example:"2025-09-12T08:15:30Z"`       // 结束时间
	Duration         string    `json:"duration" example:"2m6s"`                       // 执行耗时
	Message          string    `json:"message,omitempty" example:"通知发送成功"`       // 执行消息
	RecordsProcessed int       `json:"records_processed,omitempty" example:"1500"`    // 处理记录数
	Error            string    `json:"error,omitempty" example:"连接超时"`              // 错误信息
}
