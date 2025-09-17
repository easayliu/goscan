package handlers

import (
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/tasks"
	"time"
)

// sanitizeConfig removes sensitive information from config before returning
// Cleans configuration information by removing sensitive data before returning
func (h *HandlerService) sanitizeConfig() map[string]interface{} {
	cfg := h.config
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

// convertFlatToTaskRequest converts flat format JSON to standard TaskRequest format
// This is a backward compatible helper function
func (h *HandlerService) convertFlatToTaskRequest(flatReq map[string]interface{}) tasks.TaskRequest {
	// Extract provider
	provider := ""
	if p, ok := flatReq["provider"].(string); ok {
		provider = p
	}

	// Set default type to sync
	taskType := tasks.TaskTypeSync
	if t, ok := flatReq["type"].(string); ok {
		taskType = tasks.TaskType(t)
	}

	// Create config, manually map known fields
	config := tasks.TaskConfig{}

	if billPeriod, ok := flatReq["bill_period"].(string); ok {
		config.BillPeriod = billPeriod
	}
	if createTable, ok := flatReq["create_table"].(bool); ok {
		config.CreateTable = createTable
	}
	if forceUpdate, ok := flatReq["force_update"].(bool); ok {
		config.ForceUpdate = forceUpdate
	}
	if syncMode, ok := flatReq["sync_mode"].(string); ok {
		config.SyncMode = syncMode
	}
	if useDistributed, ok := flatReq["use_distributed"].(bool); ok {
		config.UseDistributed = useDistributed
	}
	if granularity, ok := flatReq["granularity"].(string); ok {
		config.Granularity = granularity
	}
	if startPeriod, ok := flatReq["start_period"].(string); ok {
		config.StartPeriod = startPeriod
	}
	if endPeriod, ok := flatReq["end_period"].(string); ok {
		config.EndPeriod = endPeriod
	}
	if limit, ok := flatReq["limit"].(float64); ok { // JSON numbers are float64
		config.Limit = int(limit)
	}

	return tasks.TaskRequest{
		Type:     taskType,
		Provider: provider,
		Config:   config,
	}
}

// createNotificationExecutor creates notification task executor
// This is a helper function for creating WeChat Enterprise notification executor
func (h *HandlerService) createNotificationExecutor() (*tasks.NotificationTaskExecutor, error) {
	// Create ClickHouse client (reuse existing connection or create new one)
	chClient, err := clickhouse.NewClient(h.config.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	// Create notification task executor
	return tasks.NewNotificationTaskExecutor(chClient, h.config)
}

// maskWebhookURL masks sensitive parts of Webhook URL
// Used for safely displaying URL information in API responses
func maskWebhookURL(webhookURL string) string {
	if webhookURL == "" {
		return ""
	}

	// Simple URL masking, only show prefix and suffix
	if len(webhookURL) > 20 {
		return webhookURL[:10] + "***" + webhookURL[len(webhookURL)-7:]
	}
	return "***"
}

// getRecentNotificationHistory gets recent notification task history
// Used for querying recent notification task execution records
func (h *HandlerService) getRecentNotificationHistory() []map[string]interface{} {
	history := h.taskMgr.GetTaskHistory()

	var notificationHistory []map[string]interface{}
	count := 0

	// Search from latest tasks, return at most 10 notification tasks
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

// formatDuration formats duration to readable string
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%.0fms", float64(d.Nanoseconds())/1e6)
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

// getCurrentTimestamp gets current UTC timestamp
func getCurrentTimestamp() time.Time {
	return time.Now().UTC()
}

// buildTaskResponse builds standard task response
func buildTaskResponse(taskID, status, message string) map[string]interface{} {
	return map[string]interface{}{
		"task_id":   taskID,
		"status":    status,
		"message":   message,
		"timestamp": getCurrentTimestamp(),
	}
}

// buildSuccessResponse builds success response
func buildSuccessResponse(data interface{}) map[string]interface{} {
	response := map[string]interface{}{
		"success":   true,
		"timestamp": getCurrentTimestamp(),
	}

	if data != nil {
		response["data"] = data
	}

	return response
}
