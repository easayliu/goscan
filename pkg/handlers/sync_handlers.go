package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"

	"goscan/pkg/response"
	"goscan/pkg/tasks"
)

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
	// 读取完整的body内容
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		apiError := NewBadRequestError("Failed to read request body", err)
		HandleError(w, apiError)
		return
	}
	r.Body.Close()
	
	// 先尝试解析为扁平格式（用户当前使用的格式）
	var flatReq map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &flatReq); err != nil {
		apiError := NewBadRequestError("Invalid JSON format", err)
		HandleError(w, apiError)
		return
	}
	
	log.Printf("🔍 [Handler] 接收到的原始请求: %+v", flatReq)
	
	// 检查是否是扁平格式（没有config字段但有provider字段）
	var taskReq tasks.TaskRequest
	if _, hasConfig := flatReq["config"]; !hasConfig && flatReq["provider"] != nil {
		// 扁平格式，进行转换
		taskReq = h.convertFlatToTaskRequest(flatReq)
		log.Printf("🔄 [Handler] 已转换扁平格式到标准格式")
	} else {
		// 标准格式，直接解析
		if err := json.Unmarshal(bodyBytes, &taskReq); err != nil {
			apiError := NewBadRequestError("Invalid TaskRequest format", err)
			HandleError(w, apiError)
			return
		}
		log.Printf("🔍 [Handler] 使用标准格式解析")
	}
	
	// 添加调试信息
	log.Printf("🔍 [Handler] 最终TaskRequest - Type: '%s', Provider: '%s', Config.BillPeriod: '%s' (长度: %d)", 
		taskReq.Type, taskReq.Provider, taskReq.Config.BillPeriod, len(taskReq.Config.BillPeriod))

	// Execute task asynchronously
	go h.executeTaskAsync(&taskReq)

	response.WriteJSONResponse(w, http.StatusCreated, buildTaskResponse(
		taskReq.ID,
		"started",
		"Task started successfully",
	))
}

// executeTaskAsync 异步执行任务
func (h *HandlerService) executeTaskAsync(taskReq *tasks.TaskRequest) {
	result, err := h.taskMgr.ExecuteTask(h.ctx, taskReq)
	if err != nil {
		LogErrorWithContext(err, "Task execution failed",
			"task_id", taskReq.ID,
			"provider", taskReq.Provider,
			"type", taskReq.Type)
	} else if result != nil {
		slog.Info("Task completed successfully",
			"task_id", taskReq.ID,
			"provider", taskReq.Provider,
			"duration", result.Duration)
	} else {
		slog.Info("Task completed successfully",
			"task_id", taskReq.ID,
			"provider", taskReq.Provider)
	}
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
		apiError := NewBadRequestError("Invalid task ID", err)
		HandleError(w, apiError)
		return
	}

	task, err := h.taskMgr.GetTask(taskID)
	if err != nil {
		apiError := NewNotFoundError("Task not found", err)
		HandleError(w, apiError)
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
		apiError := NewBadRequestError("Invalid task ID", err)
		HandleError(w, apiError)
		return
	}

	if err := h.taskMgr.CancelTask(taskID); err != nil {
		apiError := NewNotFoundError("Task not found or cannot be cancelled", err)
		HandleError(w, apiError)
		return
	}

	response.WriteJSONResponse(w, http.StatusOK, buildTaskResponse(
		taskID,
		"cancelled",
		"Task cancelled successfully",
	))
}

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
		BillPeriod     string `json:"bill_period,omitempty"`
		StartPeriod    string `json:"start_period,omitempty"`
		EndPeriod      string `json:"end_period,omitempty"`
		Limit          int    `json:"limit,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&syncReq); err != nil {
		apiError := NewBadRequestError("Invalid request body", err)
		HandleError(w, apiError)
		return
	}

	// Validate provider
	if err := ValidateRequired(syncReq.Provider, "provider"); err != nil {
		apiError := NewBadRequestError("Provider validation failed", err)
		HandleError(w, apiError)
		return
	}

	// 添加调试信息
	log.Printf("🔍 [TriggerSync] 接收到的请求 - Provider: '%s', BillPeriod: '%s' (长度: %d)", 
		syncReq.Provider, syncReq.BillPeriod, len(syncReq.BillPeriod))

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

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"task_id":   taskReq.ID,
		"status":    "started",
		"message":   fmt.Sprintf("Sync triggered for provider %s", syncReq.Provider),
		"provider":  syncReq.Provider,
		"timestamp": getCurrentTimestamp(),
	})
}

// executeSyncTaskAsync 异步执行同步任务
func (h *HandlerService) executeSyncTaskAsync(taskReq *tasks.TaskRequest, provider string) {
	result, err := h.taskMgr.ExecuteTask(h.ctx, taskReq)
	if err != nil {
		LogErrorWithContext(err, "Manual sync failed",
			"task_id", taskReq.ID,
			"provider", provider)
	} else if result != nil {
		slog.Info("Manual sync completed",
			"task_id", taskReq.ID,
			"provider", provider,
			"duration", result.Duration)
	} else {
		slog.Info("Manual sync completed",
			"task_id", taskReq.ID,
			"provider", provider)
	}
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
		"timestamp":     getCurrentTimestamp(),
		"service":       "goscan-sync",
		"status":        "active",
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
	
	// 过滤出同步相关的任务历史
	var syncHistory []interface{}
	for _, task := range history {
		if task.Type == tasks.TaskTypeSync {
			syncHistory = append(syncHistory, task)
		}
	}
	
	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"history":   syncHistory,
		"count":     len(syncHistory),
		"timestamp": getCurrentTimestamp(),
	})
}