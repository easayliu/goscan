package handlers

import (
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/tasks"
	"time"
)

// sanitizeConfig removes sensitive information from config before returning
// 清理配置信息，移除敏感数据后返回
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

// convertFlatToTaskRequest 将扁平格式的JSON转换为标准的TaskRequest格式
// 这是一个向后兼容的辅助函数
func (h *HandlerService) convertFlatToTaskRequest(flatReq map[string]interface{}) tasks.TaskRequest {
	// 提取provider
	provider := ""
	if p, ok := flatReq["provider"].(string); ok {
		provider = p
	}
	
	// 设置默认type为sync
	taskType := tasks.TaskTypeSync
	if t, ok := flatReq["type"].(string); ok {
		taskType = tasks.TaskType(t)
	}
	
	// 创建config，手动映射已知字段
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

// createNotificationExecutor 创建通知任务执行器
// 这是一个辅助函数，用于创建企业微信通知执行器
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
// 用于在API响应中安全地显示URL信息
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
// 用于查询最近的通知任务执行记录
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

// formatDuration 格式化时间段为可读字符串
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

// getCurrentTimestamp 获取当前UTC时间戳
func getCurrentTimestamp() time.Time {
	return time.Now().UTC()
}

// buildTaskResponse 构建标准的任务响应
func buildTaskResponse(taskID, status, message string) map[string]interface{} {
	return map[string]interface{}{
		"task_id":   taskID,
		"status":    status,
		"message":   message,
		"timestamp": getCurrentTimestamp(),
	}
}

// buildSuccessResponse 构建成功响应
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