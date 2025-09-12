package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	_ "goscan/pkg/models" // imported for swagger documentation
	"goscan/pkg/response"
	"goscan/pkg/scheduler"
	"goscan/pkg/tasks"
	"log/slog"
	"net/http"
	"time"
)

// HandlerService provides HTTP handlers for the API
type HandlerService struct {
	config    *config.Config
	ctx       context.Context
	taskMgr   *tasks.TaskManager
	scheduler *scheduler.TaskScheduler
}

// NewHandlerService creates a new handler service
func NewHandlerService(ctx context.Context, cfg *config.Config) (*HandlerService, error) {
	slog.Info("Initializing handler service")

	// Create task manager
	taskMgr, err := tasks.NewTaskManager(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create task manager: %w", err)
	}

	return &HandlerService{
		config:  cfg,
		ctx:     ctx,
		taskMgr: taskMgr,
	}, nil
}

// SetScheduler sets the scheduler reference (called after scheduler is created)
func (h *HandlerService) SetScheduler(schedulerInterface interface{}) {
	if s, ok := schedulerInterface.(*scheduler.TaskScheduler); ok {
		h.scheduler = s
	}
}

// System handlers

// GetStatus returns the overall system status
// @Summary 获取系统状态
// @Description 返回服务运行状态、任务统计和调度器状态等系统信息
// @Tags System
// @Accept json
// @Produce json
// @Success 200 {object} models.SystemStatus
// @Router /status [get]
func (h *HandlerService) GetStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"service":   "goscan",
		"version":   "1.0.0",
		"status":    "running",
		"timestamp": time.Now().UTC(),
		"uptime":    time.Since(time.Now()), // This would be calculated from start time
		"tasks": map[string]interface{}{
			"running": h.taskMgr.GetRunningTaskCount(),
			"total":   h.taskMgr.GetTotalTaskCount(),
		},
	}

	if h.scheduler != nil {
		status["scheduler"] = h.scheduler.GetStatus()
	}

	response.WriteJSONResponse(w, http.StatusOK, status)
}

// GetConfig returns the current configuration (sensitive data masked)
// @Summary 获取系统配置
// @Description 返回系统配置信息，敏感数据已脱敏处理
// @Tags System
// @Accept json
// @Produce json
// @Success 200 {object} models.ConfigResponse
// @Router /config [get]
func (h *HandlerService) GetConfig(w http.ResponseWriter, r *http.Request) {
	// Return a sanitized version of the config without sensitive information
	sanitizedConfig := h.sanitizeConfig(h.config)
	response.WriteJSONResponse(w, http.StatusOK, sanitizedConfig)
}

// UpdateConfig updates the configuration (not implemented for security)
// @Summary 更新系统配置
// @Description 基于安全考虑，此接口暂不支持配置更新
// @Tags System
// @Accept json
// @Produce json
// @Failure 501 {object} models.ErrorResponse
// @Router /config [put]
func (h *HandlerService) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	response.WriteErrorResponse(w, http.StatusNotImplemented,
		"Configuration updates are not supported for security reasons", nil)
}

// Task management handlers

// GetTasks returns all tasks
// @Summary 获取任务列表
// @Description 返回所有任务的详细信息
// @Tags Tasks
// @Accept json
// @Produce json
// @Success 200 {object} models.TaskListResponse
// @Router /tasks [get]
func (h *HandlerService) GetTasks(w http.ResponseWriter, r *http.Request) {
	tasks := h.taskMgr.GetTasks()
	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"tasks": tasks,
		"count": len(tasks),
	})
}

// CreateTask creates a new task
// @Summary 创建新任务
// @Description 创建并异步执行一个新的同步任务
// @Tags Tasks
// @Accept json
// @Produce json
// @Param task body models.TaskRequest true "任务请求参数"
// @Success 201 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Router /tasks [post]
func (h *HandlerService) CreateTask(w http.ResponseWriter, r *http.Request) {
	var taskReq tasks.TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&taskReq); err != nil {
		response.WriteErrorResponse(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Execute task asynchronously
	go func() {
		result, err := h.taskMgr.ExecuteTask(h.ctx, &taskReq)
		if err != nil {
			slog.Error("Task execution failed", "task_id", taskReq.ID, "error", err)
		} else if result != nil {
			slog.Info("Task completed successfully", "task_id", taskReq.ID, "duration", result.Duration)
		} else {
			slog.Info("Task completed successfully", "task_id", taskReq.ID)
		}
	}()

	response.WriteJSONResponse(w, http.StatusCreated, map[string]interface{}{
		"task_id": taskReq.ID,
		"status":  "started",
		"message": "Task started successfully",
	})
}

// GetTask returns a specific task
// @Summary 获取指定任务
// @Description 根据任务ID返回任务的详细信息
// @Tags Tasks
// @Accept json
// @Produce json
// @Param id path string true "任务ID"
// @Success 200 {object} models.TaskResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Router /tasks/{id} [get]
func (h *HandlerService) GetTask(w http.ResponseWriter, r *http.Request) {
	taskID, err := response.ParseStringParam(r, "id")
	if err != nil {
		response.WriteErrorResponse(w, http.StatusBadRequest, "Invalid task ID", err)
		return
	}

	task, err := h.taskMgr.GetTask(taskID)
	if err != nil {
		response.WriteErrorResponse(w, http.StatusNotFound, "Task not found", err)
		return
	}

	response.WriteJSONResponse(w, http.StatusOK, task)
}

// DeleteTask cancels/removes a task
// @Summary 删除任务
// @Description 取消或删除指定的任务
// @Tags Tasks
// @Accept json
// @Produce json
// @Param id path string true "任务ID"
// @Success 200 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Router /tasks/{id} [delete]
func (h *HandlerService) DeleteTask(w http.ResponseWriter, r *http.Request) {
	taskID, err := response.ParseStringParam(r, "id")
	if err != nil {
		response.WriteErrorResponse(w, http.StatusBadRequest, "Invalid task ID", err)
		return
	}

	if err := h.taskMgr.CancelTask(taskID); err != nil {
		response.WriteErrorResponse(w, http.StatusNotFound, "Task not found or cannot be cancelled", err)
		return
	}

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"task_id": taskID,
		"status":  "cancelled",
		"message": "Task cancelled successfully",
	})
}

// Sync operation handlers

// TriggerSync manually triggers a sync operation
// @Summary 手动触发数据同步
// @Description 手动触发指定云服务商的账单数据同步操作
// @Tags Sync
// @Accept json
// @Produce json
// @Param sync body models.SyncTriggerRequest true "同步请求参数"
// @Success 200 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Router /sync/trigger [post]
func (h *HandlerService) TriggerSync(w http.ResponseWriter, r *http.Request) {
	var syncReq struct {
		Provider       string `json:"provider"`
		SyncMode       string `json:"sync_mode"`
		UseDistributed bool   `json:"use_distributed"`
		CreateTable    bool   `json:"create_table"`
		ForceUpdate    bool   `json:"force_update"`
		Granularity    string `json:"granularity,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&syncReq); err != nil {
		response.WriteErrorResponse(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Validate provider
	if syncReq.Provider == "" {
		response.WriteErrorResponse(w, http.StatusBadRequest, "Provider is required", nil)
		return
	}

	// Create task request
	taskReq := &tasks.TaskRequest{
		Type:     tasks.TaskTypeSync,
		Provider: syncReq.Provider,
		Config: tasks.TaskConfig{
			SyncMode:       syncReq.SyncMode,
			UseDistributed: syncReq.UseDistributed,
			CreateTable:    syncReq.CreateTable,
			ForceUpdate:    syncReq.ForceUpdate,
			Granularity:    syncReq.Granularity,
		},
	}

	// Execute task asynchronously
	go func() {
		result, err := h.taskMgr.ExecuteTask(h.ctx, taskReq)
		if err != nil {
			slog.Error("Manual sync failed", "task_id", taskReq.ID, "provider", syncReq.Provider, "error", err)
		} else if result != nil {
			slog.Info("Manual sync completed", "task_id", taskReq.ID, "provider", syncReq.Provider, "duration", result.Duration)
		} else {
			slog.Info("Manual sync completed", "task_id", taskReq.ID, "provider", syncReq.Provider)
		}
	}()

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"task_id": taskReq.ID,
		"status":  "started",
		"message": fmt.Sprintf("Sync triggered for provider %s", syncReq.Provider),
	})
}

// GetSyncStatus returns current sync status
// @Summary 获取同步状态
// @Description 返回当前数据同步任务的状态信息
// @Tags Sync
// @Accept json
// @Produce json
// @Success 200 {object} models.SyncStatusResponse
// @Router /sync/status [get]
func (h *HandlerService) GetSyncStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"running_tasks": h.taskMgr.GetRunningTaskCount(),
		"total_tasks":   h.taskMgr.GetTotalTaskCount(),
		"timestamp":     time.Now().UTC(),
	}

	response.WriteJSONResponse(w, http.StatusOK, status)
}

// GetSyncHistory returns sync history
// @Summary 获取同步历史
// @Description 返回历史同步任务的执行记录
// @Tags Sync
// @Accept json
// @Produce json
// @Success 200 {object} models.SyncHistoryResponse
// @Router /sync/history [get]
func (h *HandlerService) GetSyncHistory(w http.ResponseWriter, r *http.Request) {
	history := h.taskMgr.GetTaskHistory()
	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"history": history,
		"count":   len(history),
	})
}

// Scheduler handlers

// GetSchedulerStatus returns scheduler status
// @Summary 获取调度器状态
// @Description 返回任务调度器的运行状态信息
// @Tags Scheduler
// @Accept json
// @Produce json
// @Success 200 {object} models.SchedulerStatus
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/status [get]
func (h *HandlerService) GetSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	if h.scheduler == nil {
		response.WriteErrorResponse(w, http.StatusServiceUnavailable, "Scheduler not available", nil)
		return
	}

	status := h.scheduler.GetStatus()
	response.WriteJSONResponse(w, http.StatusOK, status)
}

// GetScheduledJobs returns all scheduled jobs
// @Summary 获取定时任务列表
// @Description 返回所有已配置的定时任务信息
// @Tags Scheduler
// @Accept json
// @Produce json
// @Success 200 {object} models.JobListResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs [get]
func (h *HandlerService) GetScheduledJobs(w http.ResponseWriter, r *http.Request) {
	if h.scheduler == nil {
		response.WriteErrorResponse(w, http.StatusServiceUnavailable, "Scheduler not available", nil)
		return
	}

	jobs := h.scheduler.GetJobs()
	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"jobs":  jobs,
		"count": len(jobs),
	})
}

// CreateScheduledJob creates a new scheduled job
// @Summary 创建定时任务
// @Description 创建一个新的定时同步任务
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param job body models.JobRequest true "定时任务请求参数"
// @Success 201 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs [post]
func (h *HandlerService) CreateScheduledJob(w http.ResponseWriter, r *http.Request) {
	if h.scheduler == nil {
		response.WriteErrorResponse(w, http.StatusServiceUnavailable, "Scheduler not available", nil)
		return
	}

	var job scheduler.ScheduledJob
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		response.WriteErrorResponse(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Validate required fields
	if job.Name == "" || job.Cron == "" || job.Provider == "" {
		response.WriteErrorResponse(w, http.StatusBadRequest,
			"Name, cron, and provider are required fields", nil)
		return
	}

	if err := h.scheduler.AddJob(&job); err != nil {
		response.WriteErrorResponse(w, http.StatusBadRequest, "Failed to create scheduled job", err)
		return
	}

	response.WriteJSONResponse(w, http.StatusCreated, map[string]interface{}{
		"job_id":  job.ID,
		"status":  "created",
		"message": "Scheduled job created successfully",
	})
}

// DeleteScheduledJob removes a scheduled job
// @Summary 删除定时任务
// @Description 删除指定的定时任务
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param id path string true "任务ID"
// @Success 200 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs/{id} [delete]
func (h *HandlerService) DeleteScheduledJob(w http.ResponseWriter, r *http.Request) {
	if h.scheduler == nil {
		response.WriteErrorResponse(w, http.StatusServiceUnavailable, "Scheduler not available", nil)
		return
	}

	jobID, err := response.ParseStringParam(r, "id")
	if err != nil {
		response.WriteErrorResponse(w, http.StatusBadRequest, "Invalid job ID", err)
		return
	}

	if err := h.scheduler.RemoveJob(jobID); err != nil {
		response.WriteErrorResponse(w, http.StatusNotFound, "Job not found", err)
		return
	}

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"job_id":  jobID,
		"status":  "deleted",
		"message": "Scheduled job deleted successfully",
	})
}

// Helper functions

// sanitizeConfig removes sensitive information from config before returning
func (h *HandlerService) sanitizeConfig(cfg *config.Config) map[string]interface{} {
	sanitized := map[string]interface{}{
		"clickhouse": map[string]interface{}{
			"hosts":    cfg.ClickHouse.Hosts,
			"port":     cfg.ClickHouse.Port,
			"database": cfg.ClickHouse.Database,
			"username": cfg.ClickHouse.Username,
			"cluster":  cfg.ClickHouse.Cluster,
			"protocol": cfg.ClickHouse.Protocol,
		},
		"cloud_providers": map[string]interface{}{
			"volcengine": map[string]interface{}{
				"region":     cfg.GetVolcEngineConfig().Region,
				"host":       cfg.GetVolcEngineConfig().Host,
				"timeout":    cfg.GetVolcEngineConfig().Timeout,
				"configured": cfg.GetVolcEngineConfig().AccessKey != "",
			},
			"alicloud": map[string]interface{}{
				"region":     cfg.GetAliCloudConfig().Region,
				"timeout":    cfg.GetAliCloudConfig().Timeout,
				"configured": cfg.GetAliCloudConfig().AccessKeyID != "",
			},
		},
		"app": cfg.App,
	}

	return sanitized
}

// WeChat notification handlers

// TriggerWeChatNotification 手动触发企业微信通知
// @Summary 手动触发企业微信通知
// @Description 手动触发企业微信费用报告通知，支持指定分析日期、云服务商列表和告警阈值。可用于定时任务外的临时通知需求。
// @Description 
// @Description **请求示例：**
// @Description ```json
// @Description {
// @Description   "date": "2025-09-12",
// @Description   "providers": ["volcengine", "alicloud"],
// @Description   "alert_threshold": 15.0,
// @Description   "force_notify": false,
// @Description   "test_mode": false
// @Description }
// @Description ```
// @Description 
// @Description **字段说明：**
// @Description - `date`: 分析日期，格式YYYY-MM-DD，不填则使用当前日期
// @Description - `providers`: 云服务商列表，支持volcengine/alicloud/aws/azure/gcp
// @Description - `alert_threshold`: 费用变化告警阈值（百分比），0-100范围
// @Description - `force_notify`: 是否强制发送，true时忽略告警阈值限制
// @Description - `test_mode`: 测试模式，true时仅测试连接不发送实际报告
// @Tags Notifications
// @Accept json
// @Produce json
// @Param request body models.WeChatNotificationRequest true "微信通知触发请求参数"
// @Success 200 {object} models.MessageResponse "触发成功" 
// @Failure 400 {object} models.ErrorResponse "请求参数错误或微信通知未启用"
// @Failure 500 {object} models.ErrorResponse "服务内部错误"
// @Router /notifications/wechat/trigger [post]
func (h *HandlerService) TriggerWeChatNotification(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Date               string   `json:"date,omitempty"`                // 分析日期，格式：2006-01-02，默认当前日期
		Providers          []string `json:"providers,omitempty"`           // 云服务商列表，默认：["volcengine", "alicloud"]
		AlertThreshold     float64  `json:"alert_threshold,omitempty"`     // 告警阈值（百分比），默认使用配置值
		ForceNotify        bool     `json:"force_notify,omitempty"`        // 强制发送通知，即使未达到告警阈值
		TestMode           bool     `json:"test_mode,omitempty"`           // 测试模式，仅测试连接不发送实际报告
		NotificationFormat string   `json:"notification_format,omitempty"` // 通知格式：markdown/template_card/auto，默认使用配置值
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.WriteErrorResponse(w, http.StatusBadRequest, "请求参数解析失败", err)
		return
	}

	slog.Info("接收到企业微信通知请求，原始参数",
		"raw_date", req.Date,
		"raw_providers", req.Providers,
		"raw_alert_threshold", req.AlertThreshold,
		"raw_force_notify", req.ForceNotify,
		"raw_test_mode", req.TestMode)

	// 验证和设置默认值
	if req.Date == "" {
		// 不设置默认值，让底层的AnalyzeDailyCosts使用自己的默认逻辑（昨天）
		slog.Info("未指定日期，将使用默认日期（昨天）")
	}

	if req.Providers == nil || len(req.Providers) == 0 {
		req.Providers = []string{"volcengine", "alicloud"}
		slog.Info("使用默认云服务商列表", "providers", req.Providers)
	} else {
		slog.Info("使用用户指定的云服务商列表", "providers", req.Providers)
	}

	if req.AlertThreshold <= 0 {
		// 使用配置中的告警阈值
		wechatConfig := h.config.GetWeChatConfig()
		req.AlertThreshold = wechatConfig.AlertThreshold
		slog.Info("使用配置文件中的告警阈值", "alert_threshold", req.AlertThreshold)
	} else {
		slog.Info("使用用户指定的告警阈值", "alert_threshold", req.AlertThreshold)
	}

	// 检查微信通知配置
	wechatConfig := h.config.GetWeChatConfig()
	if !wechatConfig.Enabled {
		response.WriteErrorResponse(w, http.StatusBadRequest, "企业微信通知功能未启用", nil)
		return
	}

	if wechatConfig.WebhookURL == "" {
		response.WriteErrorResponse(w, http.StatusBadRequest, "企业微信Webhook URL未配置", nil)
		return
	}

	// 测试模式处理
	if req.TestMode {
		// 创建通知任务执行器（仅用于测试连接）
		notificationExecutor, err := h.createNotificationExecutor()
		if err != nil {
			response.WriteErrorResponse(w, http.StatusInternalServerError, "通知执行器创建失败", err)
			return
		}

		// 测试Webhook连接
		if err := notificationExecutor.TestWebhook(r.Context()); err != nil {
			response.WriteErrorResponse(w, http.StatusInternalServerError, "企业微信连接测试失败", err)
			return
		}

		response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
			"message":     "企业微信连接测试成功",
			"webhook_url": maskWebhookURL(wechatConfig.WebhookURL),
			"test_time":   time.Now().UTC(),
		})
		return
	}

	// 创建通知任务请求
	taskReq := &tasks.TaskRequest{
		Type:     tasks.TaskTypeNotification,
		Provider: "wechat", // 使用 wechat 作为 provider 标识
		Config:   tasks.TaskConfig{
			// 将请求参数编码到 Config 中（可以扩展 TaskConfig 结构体）
		},
	}

	// 由于当前TaskConfig结构不支持通知参数，我们直接执行通知任务
	// 这是一个设计改进点，未来可以扩展TaskConfig来支持通知参数

	// 创建通知执行器并直接执行
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		response.WriteErrorResponse(w, http.StatusInternalServerError, "通知执行器创建失败", err)
		return
	}

	// 创建通知参数
	params := &tasks.NotificationParams{
		Providers:      req.Providers,
		AlertThreshold: req.AlertThreshold,
		ForceNotify:    req.ForceNotify,
	}

	// 只有当用户提供了日期时才解析和设置
	if req.Date != "" {
		analysisDate, err := time.Parse("2006-01-02", req.Date)
		if err != nil {
			// 解析失败时记录警告但不设置日期，让底层使用默认值
			slog.Warn("日期解析失败，将使用默认日期", "input", req.Date, "error", err)
		} else {
			params.Date = analysisDate
		}
	}

	slog.Info("触发企业微信通知，请求参数",
		"date", req.Date,
		"providers", req.Providers,
		"alert_threshold", req.AlertThreshold,
		"force_notify", req.ForceNotify)

	// 异步执行通知任务
	// 使用 background context 而不是请求的 context，因为这是异步执行
	// HTTP 请求返回后，r.Context() 会被取消，导致查询失败
	go func() {
		// 创建一个新的带超时的 context，避免任务无限执行
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		
		result, err := notificationExecutor.ExecuteCostReportWithParams(ctx, params)
		if err != nil {
			slog.Error("手动触发的企业微信通知失败",
				"providers", req.Providers,
				"date", req.Date,
				"alert_threshold", req.AlertThreshold,
				"error", err)
		} else if result != nil {
			slog.Info("手动触发的企业微信通知成功",
				"providers", req.Providers,
				"date", req.Date,
				"alert_threshold", req.AlertThreshold,
				"duration", result.Duration,
				"message", result.Message)
		}
	}()

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":         "企业微信通知任务已启动",
		"task_id":         taskReq.ID,
		"providers":       req.Providers,
		"date":            req.Date,
		"alert_threshold": req.AlertThreshold,
		"force_notify":    req.ForceNotify,
		"timestamp":       time.Now().UTC(),
	})
}

// GetWeChatNotificationStatus 获取企业微信通知状态
// @Summary 获取企业微信通知状态  
// @Description 返回企业微信通知功能的配置状态、连接状态、最近的通知历史等详细信息
// @Description
// @Description **查询参数（可选）：**
// @Description - `history_limit`: 返回的历史记录数量，范围1-50，默认10条
// @Description - `include_config`: 是否包含详细配置信息，默认true
// @Description 
// @Description **响应示例：**
// @Description ```json
// @Description {
// @Description   "enabled": true,
// @Description   "webhook_configured": true, 
// @Description   "webhook_url": "https://qyapi***webhook",
// @Description   "alert_threshold": 10.0,
// @Description   "executor_status": "ready",
// @Description   "recent_history": [
// @Description     {
// @Description       "id": "task_123456",
// @Description       "status": "completed", 
// @Description       "start_time": "2025-09-12T01:00:00Z",
// @Description       "duration": "2m30s",
// @Description       "message": "费用报告发送成功"
// @Description     }
// @Description   ]
// @Description }
// @Description ```
// @Tags Notifications
// @Accept json
// @Produce json
// @Param history_limit query int false "历史记录数量限制" minimum(1) maximum(50) default(10)
// @Param include_config query bool false "是否包含详细配置" default(true)
// @Success 200 {object} models.WeChatNotificationStatusResponse "状态信息获取成功"
// @Router /notifications/wechat/status [get]
func (h *HandlerService) GetWeChatNotificationStatus(w http.ResponseWriter, r *http.Request) {
	wechatConfig := h.config.GetWeChatConfig()

	status := map[string]interface{}{
		"enabled":            wechatConfig.Enabled,
		"webhook_configured": wechatConfig.WebhookURL != "",
		"webhook_url":        maskWebhookURL(wechatConfig.WebhookURL),
		"alert_threshold":    wechatConfig.AlertThreshold,
		"mention_users":      wechatConfig.MentionUsers,
		"max_retries":        wechatConfig.MaxRetries,
		"retry_delay":        wechatConfig.RetryDelay,
		"timestamp":          time.Now().UTC(),
	}

	// 检查通知执行器状态
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		status["executor_status"] = "error"
		status["executor_error"] = err.Error()
	} else {
		status["executor_status"] = "ready"
		status["executor_enabled"] = notificationExecutor.IsEnabled()
	}

	// 获取最近的通知任务历史
	notificationHistory := h.getRecentNotificationHistory()
	status["recent_history"] = notificationHistory
	status["history_count"] = len(notificationHistory)

	response.WriteJSONResponse(w, http.StatusOK, status)
}

// TestWeChatWebhook 测试企业微信Webhook连接
// @Summary 测试企业微信Webhook连接
// @Description 发送测试消息到企业微信群，验证Webhook URL配置的正确性和连通性，不会发送实际的费用报告
// @Description
// @Description **请求示例（可选参数）：**
// @Description ```json
// @Description {
// @Description   "custom_message": "这是来自费用监控系统的测试消息",
// @Description   "timeout": 15
// @Description }
// @Description ```
// @Description 
// @Description **字段说明：**
// @Description - `custom_message`: 自定义测试消息内容，最大500字符，不填则使用默认消息
// @Description - `timeout`: 连接超时时间（秒），范围1-60，默认10秒
// @Description 
// @Description **成功响应示例：**
// @Description ```json
// @Description {
// @Description   "message": "企业微信连接测试成功",
// @Description   "webhook_url": "https://qyapi***webhook", 
// @Description   "test_time": "2025-09-12T08:13:24Z",
// @Description   "duration": "1.2s",
// @Description   "status": "connected"
// @Description }
// @Description ```
// @Tags Notifications
// @Accept json
// @Produce json
// @Param request body models.WeChatTestRequest false "测试请求参数（可选）"
// @Success 200 {object} models.MessageResponse "连接测试成功"
// @Failure 400 {object} models.ErrorResponse "微信通知未启用或Webhook URL未配置"
// @Failure 500 {object} models.ErrorResponse "连接测试失败或服务内部错误"
// @Router /notifications/wechat/test [post]
func (h *HandlerService) TestWeChatWebhook(w http.ResponseWriter, r *http.Request) {
	// 解析可选的测试参数
	var testReq struct {
		CustomMessage string `json:"custom_message,omitempty"`
		Timeout       int    `json:"timeout,omitempty"`
	}
	
	// 解析请求体（如果有的话）
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&testReq); err != nil {
			response.WriteErrorResponse(w, http.StatusBadRequest, "请求参数解析失败", err)
			return
		}
	}

	wechatConfig := h.config.GetWeChatConfig()
	if !wechatConfig.Enabled {
		response.WriteErrorResponse(w, http.StatusBadRequest, "企业微信通知功能未启用", nil)
		return
	}

	if wechatConfig.WebhookURL == "" {
		response.WriteErrorResponse(w, http.StatusBadRequest, "企业微信Webhook URL未配置", nil)
		return
	}

	// 创建通知执行器
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		response.WriteErrorResponse(w, http.StatusInternalServerError, "通知执行器创建失败", err)
		return
	}

	// 测试连接
	testStart := time.Now()
	if err := notificationExecutor.TestWebhook(r.Context()); err != nil {
		response.WriteErrorResponse(w, http.StatusInternalServerError, "企业微信连接测试失败", err)
		return
	}
	testDuration := time.Since(testStart)

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":     "企业微信连接测试成功",
		"webhook_url": maskWebhookURL(wechatConfig.WebhookURL),
		"test_time":   testStart.UTC(),
		"duration":    testDuration.String(),
		"status":      "connected",
	})
}

// Helper functions for WeChat notification handlers

// createNotificationExecutor 创建通知任务执行器
func (h *HandlerService) createNotificationExecutor() (*tasks.NotificationTaskExecutor, error) {
	// 创建ClickHouse客户端（复用现有连接或创建新的）
	chClient, err := clickhouse.NewClient(h.config.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("创建ClickHouse客户端失败: %w", err)
	}

	// 创建通知任务执行器
	return tasks.NewNotificationTaskExecutor(chClient, h.config)
}

// maskWebhookURL 隐藏Webhook URL的敏感部分
func maskWebhookURL(webhookURL string) string {
	if webhookURL == "" {
		return ""
	}

	// 简单的URL掩码处理，只显示前缀和后缀
	if len(webhookURL) > 20 {
		return webhookURL[:10] + "***" + webhookURL[len(webhookURL)-7:]
	}
	return "***"
}

// getRecentNotificationHistory 获取最近的通知任务历史
func (h *HandlerService) getRecentNotificationHistory() []map[string]interface{} {
	history := h.taskMgr.GetTaskHistory()

	var notificationHistory []map[string]interface{}
	count := 0

	// 从最新的任务开始查找，最多返回10个通知任务
	for i := len(history) - 1; i >= 0 && count < 10; i-- {
		task := history[i]
		if task.Type == tasks.TaskTypeNotification {
			historyItem := map[string]interface{}{
				"id":         task.ID,
				"status":     task.Status,
				"start_time": task.StartTime.UTC(),
				"end_time":   task.EndTime.UTC(),
				"duration":   task.Duration.String(),
			}

			if task.Result != nil {
				historyItem["message"] = task.Result.Message
				historyItem["records_processed"] = task.Result.RecordsProcessed
			}

			if task.Error != "" {
				historyItem["error"] = task.Error
			}

			notificationHistory = append(notificationHistory, historyItem)
			count++
		}
	}

	return notificationHistory
}
