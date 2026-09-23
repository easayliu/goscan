package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"goscan/pkg/logger"
	"goscan/pkg/models"
	"goscan/pkg/tasks"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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

	// Callers may supply their own id (to make a retry idempotent from their
	// side); otherwise mint one here so it can go back in the response.
	if taskReq.ID == "" {
		taskReq.ID = uuid.New().String()
	}

	logger.Info("Creating task",
		zap.String("task_id", taskReq.ID),
		zap.String("type", string(taskReq.Type)),
		zap.String("provider", taskReq.Provider))

	// Runs in the background on the application context; the response only says
	// the task was accepted, GET /tasks/{id} says how it went.
	if _, err := h.taskMgr.ExecuteTask(h.ctx, &taskReq); err != nil {
		respondTaskRejected(c, err, taskReq.Provider)
		return
	}

	c.JSON(http.StatusCreated, buildTaskResponse(
		taskReq.ID,
		"started",
		"Task started successfully",
	))
}

// GetTask returns a specific task
// @Summary Get specific task details
// @Description Get detailed information of a specific task by task ID, including task status, execution progress, error information, etc.
// @Tags Task Management
// @Accept json
// @Produce json
// @Param id path string true "Unique task identifier ID" example:"task_123456789"
// @Success 200 {object} tasks.Task "Task details retrieved successfully"
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

// DeleteTask asks a running sync to stop
// @Summary Stop a running sync
// @Description Asks the sync to stop after the pass (one period × one granularity) it is on. The answer comes at once, with 202: the task keeps running until that pass is written, meanwhile carrying cancel_requested=true, and then ends as cancelled — or as completed, if that pass was its last. Watch it on GET /tasks/{id}/events.
// @Description Passes that never started are listed in result.not_run and their data is untouched; no period is ever left half written. Asking again while it stops is fine.
// @Tags Task Management
// @Produce json
// @Param id path string true "Task ID returned by POST /sync"
// @Success 202 {object} models.MessageResponse "Stop requested"
// @Failure 404 {object} models.ErrorResponse "Task not found"
// @Failure 409 {object} models.ErrorResponse "Task has already finished, or is not a sync"
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
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, tasks.ErrTaskNotFound):
			status = http.StatusNotFound
		case errors.Is(err, tasks.ErrTaskNotCancellable):
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{
			"error":   true,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusAccepted, buildTaskResponse(
		taskID,
		"cancelling",
		"Stop requested: the task stops after the pass in flight is written",
	))
}

// TriggerSync manually triggers a sync operation
// @Summary Manually trigger billing data sync
// @Description Immediately trigger billing data sync operation for specified cloud provider. Supports multiple cloud providers like Volcengine, Alibaba Cloud, etc.
// @Description Supports configuring sync mode, time range, granularity and other parameters.
// @Description For AliCloud the granularity field picks the target table: monthly writes alicloud_bill_monthly, daily writes alicloud_bill_daily, both writes each. When it is empty the bill_period format decides instead (YYYY-MM monthly, YYYY-MM-DD daily). VolcEngine has a single table and ignores granularity.
// @Description The endpoint returns as soon as the task is registered; poll GET /tasks/{task_id} for the outcome.
// @Tags Data Sync
// @Accept json
// @Produce json
// @Param sync body models.SyncTriggerRequest true "Sync request parameters including cloud provider, sync mode, billing period range and other configurations"
// @Success 200 {object} models.SyncTriggerResponse "Sync task accepted, poll the returned task_id"
// @Failure 400 {object} models.ErrorResponse "Invalid request parameters or missing cloud provider configuration"
// @Failure 409 {object} models.ErrorResponse "A sync for this provider is already running"
// @Failure 429 {object} models.ErrorResponse "Too many tasks in flight, retry later"
// @Failure 500 {object} models.ErrorResponse "Internal server error"
// @Failure 503 {object} models.ErrorResponse "Shutting down, no new tasks are taken; retry later"
// @Router /sync [post]
func (h *HandlerService) TriggerSync(c *gin.Context) {
	var syncReq models.SyncTriggerRequest

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

	// Create task request. The id is minted here rather than inside the task
	// manager so it can go back in the response: whoever triggered the sync
	// (opdash, curl, a script) polls GET /tasks/{id} for the outcome.
	taskReq := &tasks.TaskRequest{
		ID:       uuid.New().String(),
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

	// ExecuteTask only registers the task and returns; the sync itself runs in
	// the background on the application context, not the request one — the
	// response is sent long before the bills are in.
	if _, err := h.taskMgr.ExecuteTask(h.ctx, taskReq); err != nil {
		respondTaskRejected(c, err, syncReq.Provider)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"task_id":   taskReq.ID,
		"status":    "started",
		"message":   fmt.Sprintf("Sync triggered for provider %s", syncReq.Provider),
		"provider":  syncReq.Provider,
		"timestamp": getCurrentTimestamp(),
	})
}

// respondTaskRejected turns a refusal from the task manager into a status code
// a caller can act on: 409 means "wait for the run in flight", 429 means "try
// again shortly", anything else is ours to fix.
func respondTaskRejected(c *gin.Context, err error, provider string) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, tasks.ErrTaskAlreadyRunning):
		status = http.StatusConflict
	case errors.Is(err, tasks.ErrTooManyTasks):
		status = http.StatusTooManyRequests
	case errors.Is(err, tasks.ErrShuttingDown):
		status = http.StatusServiceUnavailable
	}

	logger.Warn("Task was not accepted",
		zap.String("provider", provider),
		zap.Int("status", status),
		zap.Error(err))

	c.JSON(status, gin.H{
		"error":    true,
		"message":  err.Error(),
		"provider": provider,
	})
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
