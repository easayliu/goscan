package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"goscan/pkg/response"
	"goscan/pkg/tasks"
)

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
		Date               string   `json:"date,omitempty"`
		Providers          []string `json:"providers,omitempty"`
		AlertThreshold     float64  `json:"alert_threshold,omitempty"`
		ForceNotify        bool     `json:"force_notify,omitempty"`
		TestMode           bool     `json:"test_mode,omitempty"`
		NotificationFormat string   `json:"notification_format,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiError := NewBadRequestError("请求参数解析失败", err)
		HandleError(w, apiError)
		return
	}

	slog.Info("接收到企业微信通知请求，原始参数",
		"raw_date", req.Date,
		"raw_providers", req.Providers,
		"raw_alert_threshold", req.AlertThreshold,
		"raw_force_notify", req.ForceNotify,
		"raw_test_mode", req.TestMode)

	// 验证和设置默认值
	if err := h.validateWeChatRequest(&req); err != nil {
		apiError := NewBadRequestError("请求参数验证失败", err)
		HandleError(w, apiError)
		return
	}

	// 检查微信通知配置
	if err := h.validateWeChatConfig(); err != nil {
		apiError := NewBadRequestError("微信通知配置错误", err)
		HandleError(w, apiError)
		return
	}

	// 测试模式处理
	if req.TestMode {
		if err := h.handleTestMode(w, r); err != nil {
			HandleError(w, err)
			return
		}
		return
	}

	// 执行通知任务
	taskID, err := h.executeWeChatNotification(&req)
	if err != nil {
		apiError := NewInternalServerError("通知任务执行失败", err)
		HandleError(w, apiError)
		return
	}

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":         "企业微信通知任务已启动",
		"task_id":         taskID,
		"providers":       req.Providers,
		"date":            req.Date,
		"alert_threshold": req.AlertThreshold,
		"force_notify":    req.ForceNotify,
		"timestamp":       getCurrentTimestamp(),
	})
}

// validateWeChatRequest 验证微信通知请求参数
func (h *HandlerService) validateWeChatRequest(req *struct {
	Date               string   `json:"date,omitempty"`
	Providers          []string `json:"providers,omitempty"`
	AlertThreshold     float64  `json:"alert_threshold,omitempty"`
	ForceNotify        bool     `json:"force_notify,omitempty"`
	TestMode           bool     `json:"test_mode,omitempty"`
	NotificationFormat string   `json:"notification_format,omitempty"`
}) error {
	if req.Date == "" {
		slog.Info("未指定日期，将使用默认日期（昨天）")
	}

	if req.Providers == nil || len(req.Providers) == 0 {
		req.Providers = []string{"volcengine", "alicloud"}
		slog.Info("使用默认云服务商列表", "providers", req.Providers)
	} else {
		slog.Info("使用用户指定的云服务商列表", "providers", req.Providers)
	}

	if req.AlertThreshold <= 0 {
		wechatConfig := h.config.GetWeChatConfig()
		req.AlertThreshold = wechatConfig.AlertThreshold
		slog.Info("使用配置文件中的告警阈值", "alert_threshold", req.AlertThreshold)
	} else {
		slog.Info("使用用户指定的告警阈值", "alert_threshold", req.AlertThreshold)
	}

	return nil
}

// validateWeChatConfig 验证微信配置
func (h *HandlerService) validateWeChatConfig() error {
	wechatConfig := h.config.GetWeChatConfig()
	if !wechatConfig.Enabled {
		return NewBadRequestError("企业微信通知功能未启用", nil)
	}

	if wechatConfig.WebhookURL == "" {
		return NewBadRequestError("企业微信Webhook URL未配置", nil)
	}

	return nil
}

// handleTestMode 处理测试模式请求
func (h *HandlerService) handleTestMode(w http.ResponseWriter, r *http.Request) error {
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		return NewInternalServerError("通知执行器创建失败", err)
	}

	if err := notificationExecutor.TestWebhook(r.Context()); err != nil {
		return NewInternalServerError("企业微信连接测试失败", err)
	}

	wechatConfig := h.config.GetWeChatConfig()
	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":     "企业微信连接测试成功",
		"webhook_url": maskWebhookURL(wechatConfig.WebhookURL),
		"test_time":   getCurrentTimestamp(),
	})

	return nil
}

// executeWeChatNotification 执行微信通知任务
func (h *HandlerService) executeWeChatNotification(req *struct {
	Date               string   `json:"date,omitempty"`
	Providers          []string `json:"providers,omitempty"`
	AlertThreshold     float64  `json:"alert_threshold,omitempty"`
	ForceNotify        bool     `json:"force_notify,omitempty"`
	TestMode           bool     `json:"test_mode,omitempty"`
	NotificationFormat string   `json:"notification_format,omitempty"`
}) (string, error) {
	// 创建通知任务请求
	taskReq := &tasks.TaskRequest{
		Type:     tasks.TaskTypeNotification,
		Provider: "wechat",
		Config:   tasks.TaskConfig{},
	}

	// 创建通知执行器并直接执行
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		return "", WrapError(err, "通知执行器创建失败")
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
	go h.executeNotificationAsync(notificationExecutor, params, req)

	return taskReq.ID, nil
}

// executeNotificationAsync 异步执行通知任务
func (h *HandlerService) executeNotificationAsync(
	executor *tasks.NotificationTaskExecutor,
	params *tasks.NotificationParams,
	req *struct {
		Date               string   `json:"date,omitempty"`
		Providers          []string `json:"providers,omitempty"`
		AlertThreshold     float64  `json:"alert_threshold,omitempty"`
		ForceNotify        bool     `json:"force_notify,omitempty"`
		TestMode           bool     `json:"test_mode,omitempty"`
		NotificationFormat string   `json:"notification_format,omitempty"`
	},
) {
	// 创建一个新的带超时的 context，避免任务无限执行
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := executor.ExecuteCostReportWithParams(ctx, params)
	if err != nil {
		LogErrorWithContext(err, "手动触发的企业微信通知失败",
			"providers", req.Providers,
			"date", req.Date,
			"alert_threshold", req.AlertThreshold)
	} else if result != nil {
		slog.Info("手动触发的企业微信通知成功",
			"providers", req.Providers,
			"date", req.Date,
			"alert_threshold", req.AlertThreshold,
			"duration", result.Duration,
			"message", result.Message)
	}
}

// GetWeChatNotificationStatus 获取企业微信通知状态
// @Summary 获取企业微信通知状态
// @Description 返回企业微信通知功能的配置状态、连接状态、最近的通知历史等详细信息
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
		"timestamp":          getCurrentTimestamp(),
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
			apiError := NewBadRequestError("请求参数解析失败", err)
			HandleError(w, apiError)
			return
		}
	}

	// 验证微信配置
	if err := h.validateWeChatConfig(); err != nil {
		HandleError(w, err)
		return
	}

	// 创建通知执行器
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		apiError := NewInternalServerError("通知执行器创建失败", err)
		HandleError(w, apiError)
		return
	}

	// 测试连接
	testStart := time.Now()
	if err := notificationExecutor.TestWebhook(r.Context()); err != nil {
		apiError := NewInternalServerError("企业微信连接测试失败", err)
		HandleError(w, apiError)
		return
	}
	testDuration := time.Since(testStart)

	wechatConfig := h.config.GetWeChatConfig()
	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"message":     "企业微信连接测试成功",
		"webhook_url": maskWebhookURL(wechatConfig.WebhookURL),
		"test_time":   testStart.UTC(),
		"duration":    formatDuration(testDuration),
		"status":      "connected",
	})
}