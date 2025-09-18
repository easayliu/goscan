package handlers

import (
	"fmt"
	"net/http"

	"goscan/pkg/logger"
	_ "goscan/pkg/models"
	"goscan/pkg/tasks"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// GetTasks returns all tasks
// @Summary Get all task list
// @Description Returns detailed information of all tasks in the system, including running, completed and failed tasks
// @Tags Task Management
// @Accept json
// @Produce json
// @Success 200 {object} models.TaskListResponse "Task list retrieved successfully"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /tasks [get]
func (h *HandlerService) GetTasks(c *gin.Context) {
	tasks := h.taskMgr.GetTasks()
	c.JSON(http.StatusOK, gin.H{
		"tasks": tasks,
		"count": len(tasks),
	})
}

// CreateTask creates a new task
// @Summary Create new sync task
// @Description Create and asynchronously execute a new cloud provider billing data sync task. Supports multiple cloud providers like Volcengine, Alibaba Cloud, etc.
// @Tags Task Management
// @Accept json
// @Produce json
// @Param task body models.TaskRequest true "Task request parameters including task type, cloud provider, sync configuration, etc."
// @Success 201 {object} models.MessageResponse "Task created successfully"
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /tasks [post]
func (h *HandlerService) CreateTask(c *gin.Context) {
	var flatReq map[string]interface{}
	if err := c.ShouldBindJSON(&flatReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid JSON format",
			"details": err.Error(),
		})
		return
	}

	// Check if it's flat format (no config field but has provider field)
	var taskReq tasks.TaskRequest
	if _, hasConfig := flatReq["config"]; !hasConfig && flatReq["provider"] != nil {
		// Flat format, perform conversion
		taskReq = h.convertFlatToTaskRequest(flatReq)
	} else {
		// Standard format, parse directly
		if err := c.ShouldBindJSON(&taskReq); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   true,
				"message": "Invalid TaskRequest format",
				"details": err.Error(),
			})
			return
		}
	}

	logger.Info("Creating task",
		zap.String("task_id", taskReq.ID),
		zap.String("type", string(taskReq.Type)),
		zap.String("provider", taskReq.Provider))

	// Execute task asynchronously
	go h.executeTaskAsync(&taskReq)

	c.JSON(http.StatusCreated, buildTaskResponse(
		taskReq.ID,
		"started",
		"Task started successfully",
	))
}

// executeTaskAsync executes task asynchronously
func (h *HandlerService) executeTaskAsync(taskReq *tasks.TaskRequest) {
	result, err := h.taskMgr.ExecuteTask(h.ctx, taskReq)
	if err != nil {
		LogErrorWithContext(err, "Task execution failed",
			"task_id", taskReq.ID,
			"provider", taskReq.Provider,
			"type", taskReq.Type)
	} else if result != nil {
		logger.Info("Task completed successfully",
			zap.String("task_id", taskReq.ID),
			zap.String("provider", taskReq.Provider),
			zap.Duration("duration", result.Duration))
	} else {
		logger.Info("Task completed successfully",
			zap.String("task_id", taskReq.ID),
			zap.String("provider", taskReq.Provider))
	}
}

// GetTask returns a specific task
// @Summary Get specific task details
// @Description Get detailed information of a specific task by task ID, including task status, execution progress, error information, etc.
// @Tags Task Management
// @Accept json
// @Produce json
// @Param id path string true "Unique task identifier ID" example:"task_123456789"
// @Success 200 {object} models.TaskResponse "Task details retrieved successfully"
// @Failure 400 {object} models.ErrorResponse "Invalid task ID"
// @Failure 404 {object} models.ErrorResponse "Task not found"
// @Router /tasks/{id} [get]
func (h *HandlerService) GetTask(c *gin.Context) {
	taskID := c.Param("id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid task ID",
		})
		return
	}

	task, err := h.taskMgr.GetTask(taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   true,
			"message": "Task not found",
		})
		return
	}

	c.JSON(http.StatusOK, task)
}

// DeleteTask cancels/removes a task
// @Summary Cancel or delete task
// @Description Cancel running tasks or delete completed task records. Running tasks will be forcibly stopped.
// @Tags Task Management
// @Accept json
// @Produce json
// @Param id path string true "Unique task identifier ID" example:"task_123456789"
// @Success 200 {object} models.MessageResponse "Task cancelled/deleted successfully"
// @Failure 400 {object} models.ErrorResponse "Invalid task ID"
// @Failure 404 {object} models.ErrorResponse "Task not found or cannot be cancelled"
// @Router /tasks/{id} [delete]
func (h *HandlerService) DeleteTask(c *gin.Context) {
	taskID := c.Param("id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid task ID",
		})
		return
	}

	if err := h.taskMgr.CancelTask(taskID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   true,
			"message": "Task not found or cannot be cancelled",
		})
		return
	}

	c.JSON(http.StatusOK, buildTaskResponse(
		taskID,
		"cancelled",
		"Task cancelled successfully",
	))
}

// TriggerSync manually triggers a sync operation
// @Summary Manually trigger billing data sync
// @Description Immediately trigger billing data sync operation for specified cloud provider. Supports multiple cloud providers like Volcengine, Alibaba Cloud, etc.
// @Description Supports configuring sync mode, time range, granularity and other parameters.
// @Tags Data Sync
// @Accept json
// @Produce json
// @Param sync body models.SyncTriggerRequest true "Sync request parameters including cloud provider, sync mode, billing period range and other configurations"
// @Success 200 {object} models.MessageResponse "Sync task triggered successfully"
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters or missing cloud provider configuration"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Router /sync [post]
func (h *HandlerService) TriggerSync(c *gin.Context) {
	var syncReq struct {
		Provider       string `json:"provider"`
		SyncMode       string `json:"sync_mode"`
		UseDistributed bool   `json:"use_distributed"`
		CreateTable    bool   `json:"create_table"`
		ForceUpdate    bool   `json:"force_update"`
		Granularity    string `json:"granularity,omitempty"`
		BillPeriod     string `json:"bill_period,omitempty"`
		StartPeriod    string `json:"start_period,omitempty"`
		EndPeriod      string `json:"end_period,omitempty"`
		Limit          int    `json:"limit,omitempty"`
	}

	if err := c.ShouldBindJSON(&syncReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid request body",
			"details": err.Error(),
		})
		return
	}

	// Validate provider
	if syncReq.Provider == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Provider is required",
		})
		return
	}

	logger.Info("Triggering sync",
		zap.String("provider", syncReq.Provider),
		zap.String("sync_mode", syncReq.SyncMode))

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
			BillPeriod:     syncReq.BillPeriod,
			StartPeriod:    syncReq.StartPeriod,
			EndPeriod:      syncReq.EndPeriod,
			Limit:          syncReq.Limit,
		},
	}

	// Execute task asynchronously
	go h.executeSyncTaskAsync(taskReq, syncReq.Provider)

	c.JSON(http.StatusOK, gin.H{
		"task_id":   taskReq.ID,
		"status":    "started",
		"message":   fmt.Sprintf("Sync triggered for provider %s", syncReq.Provider),
		"provider":  syncReq.Provider,
		"timestamp": getCurrentTimestamp(),
	})
}

// executeSyncTaskAsync executes sync task asynchronously
func (h *HandlerService) executeSyncTaskAsync(taskReq *tasks.TaskRequest, provider string) {
	result, err := h.taskMgr.ExecuteTask(h.ctx, taskReq)
	if err != nil {
		LogErrorWithContext(err, "Manual sync failed",
			"task_id", taskReq.ID,
			"provider", provider)
	} else if result != nil {
		logger.Info("Manual sync completed",
			zap.String("task_id", taskReq.ID),
			zap.String("provider", provider),
			zap.Duration("duration", result.Duration))
	} else {
		logger.Info("Manual sync completed",
			zap.String("task_id", taskReq.ID),
			zap.String("provider", provider))
	}
}

// GetSyncStatus returns current sync status
// @Summary Get data sync status
// @Description Returns summary status information of all current data sync tasks, including running task count, total task count and other statistics
// @Tags Data Sync
// @Accept json
// @Produce json
// @Success 200 {object} models.SyncStatusResponse "Sync status retrieved successfully"
// @Router /sync [get]
func (h *HandlerService) GetSyncStatus(c *gin.Context) {
	status := gin.H{
		"running_tasks": h.taskMgr.GetRunningTaskCount(),
		"total_tasks":   h.taskMgr.GetTotalTaskCount(),
		"timestamp":     getCurrentTimestamp(),
		"service":       "goscan-sync",
		"status":        "active",
	}

	c.JSON(http.StatusOK, status)
}

// GetSyncHistory returns sync history
// @Summary Get sync history records
// @Description Returns execution records of historical sync tasks, including successful and failed tasks, as well as detailed information of each sync
// @Tags Data Sync
// @Accept json
// @Produce json
// @Param limit query int false "Limit of returned records" default(100) minimum(1) maximum(1000)
// @Param provider query string false "Filter records by specified cloud provider" Enums(volcengine,alicloud,aws,azure,gcp)
// @Success 200 {object} models.SyncHistoryResponse "History records retrieved successfully"
// @Router /sync/history [get]
func (h *HandlerService) GetSyncHistory(c *gin.Context) {
	history := h.taskMgr.GetTaskHistory()

	// Filter out sync-related task history
	var syncHistory []interface{}
	for _, task := range history {
		if task.Type == tasks.TaskTypeSync {
			syncHistory = append(syncHistory, task)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"history":   syncHistory,
		"count":     len(syncHistory),
		"timestamp": getCurrentTimestamp(),
	})
}
