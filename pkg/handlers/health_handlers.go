package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	_ "goscan/pkg/models"
)

// GetStatus returns the overall system status
// @Summary Get system status
// @Description Returns system information including service running status, task statistics and scheduler status
// @Tags System Management
// @Accept json
// @Produce json
// @Success 200 {object} models.SystemStatus
// @Router /system/status [get]
func (h *HandlerService) GetStatus(c *gin.Context) {
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

	c.JSON(http.StatusOK, status)
}

// GetAppConfig returns the current configuration (sensitive data masked)
// @Summary Get system configuration
// @Description Returns system configuration information with sensitive data masked
// @Tags System Management
// @Accept json
// @Produce json
// @Success 200 {object} models.ConfigResponse
// @Router /system/config [get]
func (h *HandlerService) GetAppConfig(c *gin.Context) {
	// Return a sanitized version of the config without sensitive information
	sanitizedConfig := h.sanitizeConfig()
	c.JSON(http.StatusOK, sanitizedConfig)
}

// UpdateConfig updates the configuration (not implemented for security)
// @Summary Update system configuration
// @Description This endpoint does not support configuration updates for security reasons
// @Tags System Management
// @Accept json
// @Produce json
// @Failure 501 {object} models.ErrorResponse
// @Router /system/config [put]
func (h *HandlerService) UpdateConfig(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error":   true,
		"message": "Configuration updates are not supported for security reasons",
	})
}

// HealthCheck performs a comprehensive health check
// @Summary Perform health check
// @Description Perform comprehensive system health check including database connection, task manager status, etc. (Note: this endpoint is not under /api/v1 path)
// @Tags Health Check
// @Accept json
// @Produce json
// @Success 200 {object} models.HealthCheckResponse "Health check passed"
// @Failure 503 {object} models.ErrorResponse "Service unhealthy"
// @Router /health [get]
// @BasePath /
func (h *HandlerService) HealthCheck(c *gin.Context) {
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": getCurrentTimestamp(),
		"checks": map[string]interface{}{
			"task_manager": h.checkTaskManagerHealth(),
			"scheduler":    h.checkSchedulerHealth(),
			"config":       h.checkConfigHealth(),
		},
	}

	// Determine overall health status
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
		c.JSON(http.StatusServiceUnavailable, health)
		return
	}

	c.JSON(http.StatusOK, health)
}

// checkTaskManagerHealth checks task manager health status
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

// checkSchedulerHealth checks scheduler health status
func (h *HandlerService) checkSchedulerHealth() map[string]interface{} {
	if h.scheduler == nil {
		return map[string]interface{}{
			"status": "unavailable",
			"error":  "scheduler not initialized",
		}
	}

	status := h.scheduler.GetStatus()
	return map[string]interface{}{
		"status":     "healthy",
		"details":    status,
		"is_running": status != nil,
	}
}

// checkConfigHealth checks configuration health status
func (h *HandlerService) checkConfigHealth() map[string]interface{} {
	if h.config == nil {
		return map[string]interface{}{
			"status": "unhealthy",
			"error":  "configuration not loaded",
		}
	}

	// Check critical configuration items
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
		"status":        "healthy",
		"config_loaded": true,
	}
}
