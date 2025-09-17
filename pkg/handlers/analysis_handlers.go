package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"goscan/pkg/logger"
	_ "goscan/pkg/models"
	"goscan/pkg/tasks"
)

// TriggerWeChatNotification manually triggers WeChat Enterprise notification
// @Summary Manually trigger WeChat Enterprise notification
// @Description Manually trigger WeChat Enterprise cost report notification, supports specifying analysis date, cloud provider list and alert threshold. Can be used for temporary notification needs outside of scheduled tasks.
// @Description
// @Description **Request Example:**
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
// @Description **Field Descriptions:**
// @Description - `date`: Analysis date, format YYYY-MM-DD, uses current date if not provided
// @Description - `providers`: Cloud provider list, supports volcengine/alicloud/aws/azure/gcp
// @Description - `alert_threshold`: Cost change alert threshold (percentage), range 0-100
// @Description - `force_notify`: Whether to force send, ignores alert threshold when true
// @Description - `test_mode`: Test mode, only tests connection without sending actual report when true
// @Tags Notification Management
// @Accept json
// @Produce json
// @Param request body models.WeChatNotificationRequest true "WeChat notification trigger request parameters"
// @Success 200 {object} models.MessageResponse "Triggered successfully"
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters or WeChat notification not enabled"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /notifications/wechat [post]
func (h *HandlerService) TriggerWeChatNotification(c *gin.Context) {
	var req struct {
		Date               string   `json:"date,omitempty"`
		Providers          []string `json:"providers,omitempty"`
		AlertThreshold     float64  `json:"alert_threshold,omitempty"`
		ForceNotify        bool     `json:"force_notify,omitempty"`
		TestMode           bool     `json:"test_mode,omitempty"`
		NotificationFormat string   `json:"notification_format,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Request parameter parsing failed",
			"details": err.Error(),
		})
		return
	}

	logger.Info("Received WeChat notification request",
		zap.String("date", req.Date),
		zap.Strings("providers", req.Providers),
		zap.Float64("alert_threshold", req.AlertThreshold),
		zap.Bool("force_notify", req.ForceNotify),
		zap.Bool("test_mode", req.TestMode))

	// Validate and set default values
	if err := h.validateWeChatRequest(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Request parameter validation failed",
			"details": err.Error(),
		})
		return
	}

	// Check WeChat notification configuration
	if err := h.validateWeChatConfig(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "WeChat notification configuration error",
			"details": err.Error(),
		})
		return
	}

	// Handle test mode
	if req.TestMode {
		if err := h.handleTestModeGin(c); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   true,
				"message": "Test mode processing failed",
				"details": err.Error(),
			})
			return
		}
		return
	}

	// Execute notification task
	taskID, err := h.executeWeChatNotification(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   true,
			"message": "Notification task execution failed",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":         "WeChat notification task started",
		"task_id":         taskID,
		"providers":       req.Providers,
		"date":            req.Date,
		"alert_threshold": req.AlertThreshold,
		"force_notify":    req.ForceNotify,
		"timestamp":       getCurrentTimestamp(),
	})
}

// validateWeChatRequest validates WeChat notification request parameters
func (h *HandlerService) validateWeChatRequest(req *struct {
	Date               string   `json:"date,omitempty"`
	Providers          []string `json:"providers,omitempty"`
	AlertThreshold     float64  `json:"alert_threshold,omitempty"`
	ForceNotify        bool     `json:"force_notify,omitempty"`
	TestMode           bool     `json:"test_mode,omitempty"`
	NotificationFormat string   `json:"notification_format,omitempty"`
}) error {
	// Set default values
	if req.Providers == nil || len(req.Providers) == 0 {
		req.Providers = []string{"volcengine", "alicloud"}
	}

	if req.AlertThreshold <= 0 {
		wechatConfig := h.config.GetWeChatConfig()
		req.AlertThreshold = wechatConfig.AlertThreshold
	}

	return nil
}

// validateWeChatConfig validates WeChat configuration
func (h *HandlerService) validateWeChatConfig() error {
	wechatConfig := h.config.GetWeChatConfig()
	if !wechatConfig.Enabled {
		return NewBadRequestError("WeChat notification is not enabled", nil)
	}

	if wechatConfig.WebhookURL == "" {
		return NewBadRequestError("WeChat Webhook URL is not configured", nil)
	}

	return nil
}

// handleTestModeGin handles test mode request (Gin version)
func (h *HandlerService) handleTestModeGin(c *gin.Context) error {
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		return err
	}

	if err := notificationExecutor.TestWebhook(c.Request.Context()); err != nil {
		return err
	}

	wechatConfig := h.config.GetWeChatConfig()
	c.JSON(http.StatusOK, gin.H{
		"message":     "WeChat connection test successful",
		"webhook_url": maskWebhookURL(wechatConfig.WebhookURL),
		"test_time":   getCurrentTimestamp(),
	})

	return nil
}

// executeWeChatNotification executes WeChat notification task
func (h *HandlerService) executeWeChatNotification(req *struct {
	Date               string   `json:"date,omitempty"`
	Providers          []string `json:"providers,omitempty"`
	AlertThreshold     float64  `json:"alert_threshold,omitempty"`
	ForceNotify        bool     `json:"force_notify,omitempty"`
	TestMode           bool     `json:"test_mode,omitempty"`
	NotificationFormat string   `json:"notification_format,omitempty"`
}) (string, error) {
	// Create notification task request
	taskReq := &tasks.TaskRequest{
		Type:     tasks.TaskTypeNotification,
		Provider: "wechat",
		Config:   tasks.TaskConfig{},
	}

	// Create notification executor and execute directly
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		return "", WrapError(err, "Failed to create notification executor")
	}

	// Create notification parameters
	params := &tasks.NotificationParams{
		Providers:      req.Providers,
		AlertThreshold: req.AlertThreshold,
		ForceNotify:    req.ForceNotify,
	}

	// Only parse and set when user provides date
	if req.Date != "" {
		analysisDate, err := time.Parse("2006-01-02", req.Date)
		if err != nil {
			logger.Warn("Date parsing failed, using default date",
				zap.String("input", req.Date),
				zap.Error(err))
		} else {
			params.Date = analysisDate
		}
	}

	logger.Info("Triggering WeChat notification",
		zap.Strings("providers", req.Providers),
		zap.Bool("force_notify", req.ForceNotify))

	// Execute notification task asynchronously
	go h.executeNotificationAsync(notificationExecutor, params, req)

	return taskReq.ID, nil
}

// executeNotificationAsync executes notification task asynchronously
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
	// Create a new context with timeout to avoid infinite task execution
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := executor.ExecuteCostReportWithParams(ctx, params)
	if err != nil {
		LogErrorWithContext(err, "Manual WeChat Enterprise notification failed",
			"providers", req.Providers,
			"date", req.Date,
			"alert_threshold", req.AlertThreshold)
	} else if result != nil {
		logger.Info("Manual WeChat notification triggered successfully",
			zap.Strings("providers", req.Providers),
			zap.String("date", req.Date),
			zap.Float64("alert_threshold", req.AlertThreshold),
			zap.Duration("duration", result.Duration),
			zap.String("message", result.Message))
	}
}

// GetWeChatNotificationStatus gets WeChat Enterprise notification status
// @Summary Get WeChat Enterprise notification status
// @Description Returns detailed information about WeChat Enterprise notification feature configuration status, connection status, recent notification history, etc.
// @Tags Notification Management
// @Accept json
// @Produce json
// @Param history_limit query int false "History record count limit" minimum(1) maximum(50) default(10)
// @Param include_config query bool false "Whether to include detailed configuration" default(true)
// @Success 200 {object} models.WeChatNotificationStatusResponse "Status information retrieved successfully"
// @Router /notifications/wechat/status [get]
func (h *HandlerService) GetWeChatNotificationStatus(c *gin.Context) {
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

	// Check notification executor status
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		status["executor_status"] = "error"
		status["executor_error"] = err.Error()
	} else {
		status["executor_status"] = "ready"
		status["executor_enabled"] = notificationExecutor.IsEnabled()
	}

	// Get recent notification task history
	notificationHistory := h.getRecentNotificationHistory()
	status["recent_history"] = notificationHistory
	status["history_count"] = len(notificationHistory)

	c.JSON(http.StatusOK, status)
}

// TestWeChatWebhook tests WeChat Enterprise Webhook connection
// @Summary Test WeChat Enterprise Webhook connection
// @Description Send test message to WeChat Enterprise group to verify correctness and connectivity of Webhook URL configuration, will not send actual cost reports
// @Tags Notification Management
// @Accept json
// @Produce json
// @Param request body models.WeChatTestRequest false "Test request parameters (optional)"
// @Success 200 {object} models.MessageResponse "Connection test successful"
// @Failure 400 {object} models.ErrorResponse "WeChat notification not enabled or Webhook URL not configured"
// @Failure 500 {object} models.ErrorResponse "Connection test failed or internal server error"
// @Router /notifications/wechat/test [post]
func (h *HandlerService) TestWeChatWebhook(c *gin.Context) {
	// Parse optional test parameters
	var testReq struct {
		CustomMessage string `json:"custom_message,omitempty"`
		Timeout       int    `json:"timeout,omitempty"`
	}

	// Parse request body (if any)
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&testReq); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   true,
				"message": "Request parameter parsing failed",
				"details": err.Error(),
			})
			return
		}
	}

	// Validate WeChat configuration
	if err := h.validateWeChatConfig(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": err.Error(),
		})
		return
	}

	// Create notification executor
	notificationExecutor, err := h.createNotificationExecutor()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   true,
			"message": "Failed to create notification executor",
			"details": err.Error(),
		})
		return
	}

	// Test connection
	testStart := time.Now()
	if err := notificationExecutor.TestWebhook(c.Request.Context()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   true,
			"message": "WeChat connection test failed",
			"details": err.Error(),
		})
		return
	}
	testDuration := time.Since(testStart)

	wechatConfig := h.config.GetWeChatConfig()
	c.JSON(http.StatusOK, gin.H{
		"message":     "WeChat connection test successful",
		"webhook_url": maskWebhookURL(wechatConfig.WebhookURL),
		"test_time":   testStart.UTC(),
		"duration":    formatDuration(testDuration),
		"status":      "connected",
	})
}
