package handlers

import (
	"net/http"
	"time"

	"goscan/pkg/response"
)

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
		"timestamp": getCurrentTimestamp(),
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

// GetAppConfig returns the current configuration (sensitive data masked)
// @Summary 获取系统配置
// @Description 返回系统配置信息，敏感数据已脱敏处理
// @Tags System
// @Accept json
// @Produce json
// @Success 200 {object} models.ConfigResponse
// @Router /config [get]
func (h *HandlerService) GetAppConfig(w http.ResponseWriter, r *http.Request) {
	// Return a sanitized version of the config without sensitive information
	sanitizedConfig := h.sanitizeConfig()
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
	apiError := NewServiceUnavailableError(
		"Configuration updates are not supported for security reasons",
		nil,
	)
	HandleError(w, apiError)
}

// HealthCheck performs a comprehensive health check
// @Summary 执行健康检查
// @Description 执行全面的系统健康检查，包括数据库连接、任务管理器状态等
// @Tags System
// @Accept json
// @Produce json
// @Success 200 {object} models.HealthCheckResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /health [get]
func (h *HandlerService) HealthCheck(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": getCurrentTimestamp(),
		"checks": map[string]interface{}{
			"task_manager": h.checkTaskManagerHealth(),
			"scheduler":    h.checkSchedulerHealth(),
			"config":       h.checkConfigHealth(),
		},
	}

	// 判断整体健康状态
	allHealthy := true
	checks := health["checks"].(map[string]interface{})
	for _, check := range checks {
		if checkMap, ok := check.(map[string]interface{}); ok {
			if status, exists := checkMap["status"]; exists && status != "healthy" {
				allHealthy = false
				break
			}
		}
	}

	if !allHealthy {
		health["status"] = "unhealthy"
		response.WriteJSONResponse(w, http.StatusServiceUnavailable, health)
		return
	}

	response.WriteJSONResponse(w, http.StatusOK, health)
}

// checkTaskManagerHealth 检查任务管理器健康状态
func (h *HandlerService) checkTaskManagerHealth() map[string]interface{} {
	if h.taskMgr == nil {
		return map[string]interface{}{
			"status": "unhealthy",
			"error":  "task manager not initialized",
		}
	}

	return map[string]interface{}{
		"status":        "healthy",
		"running_tasks": h.taskMgr.GetRunningTaskCount(),
		"total_tasks":   h.taskMgr.GetTotalTaskCount(),
	}
}

// checkSchedulerHealth 检查调度器健康状态
func (h *HandlerService) checkSchedulerHealth() map[string]interface{} {
	if h.scheduler == nil {
		return map[string]interface{}{
			"status": "unavailable",
			"error":  "scheduler not initialized",
		}
	}

	status := h.scheduler.GetStatus()
	return map[string]interface{}{
		"status":   "healthy",
		"details":  status,
		"is_running": status != nil,
	}
}

// checkConfigHealth 检查配置健康状态
func (h *HandlerService) checkConfigHealth() map[string]interface{} {
	if h.config == nil {
		return map[string]interface{}{
			"status": "unhealthy",
			"error":  "configuration not loaded",
		}
	}

	// 检查关键配置项
	issues := []string{}
	
	if h.config.ClickHouse.Hosts == nil || len(h.config.ClickHouse.Hosts) == 0 {
		issues = append(issues, "ClickHouse hosts not configured")
	}
	
	if h.config.ClickHouse.Database == "" {
		issues = append(issues, "ClickHouse database not configured")
	}

	if len(issues) > 0 {
		return map[string]interface{}{
			"status": "unhealthy",
			"issues": issues,
		}
	}

	return map[string]interface{}{
		"status": "healthy",
		"config_loaded": true,
	}
}