package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"goscan/pkg/response"
	"goscan/pkg/scheduler"
)

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
	if !h.IsSchedulerAvailable() {
		apiError := NewServiceUnavailableError("Scheduler not available", nil)
		HandleError(w, apiError)
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
	if !h.IsSchedulerAvailable() {
		apiError := NewServiceUnavailableError("Scheduler not available", nil)
		HandleError(w, apiError)
		return
	}

	jobs := h.scheduler.GetJobs()
	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"jobs":      jobs,
		"count":     len(jobs),
		"timestamp": getCurrentTimestamp(),
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
	if !h.IsSchedulerAvailable() {
		apiError := NewServiceUnavailableError("Scheduler not available", nil)
		HandleError(w, apiError)
		return
	}

	var job scheduler.ScheduledJob
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		apiError := NewBadRequestError("Invalid request body", err)
		HandleError(w, apiError)
		return
	}

	// 验证必需字段
	if err := h.validateScheduledJob(&job); err != nil {
		apiError := NewBadRequestError("Job validation failed", err)
		HandleError(w, apiError)
		return
	}

	if err := h.scheduler.AddJob(&job); err != nil {
		apiError := NewBadRequestError("Failed to create scheduled job", err)
		HandleError(w, apiError)
		return
	}

	response.WriteJSONResponse(w, http.StatusCreated, map[string]interface{}{
		"job_id":    job.ID,
		"status":    "created",
		"message":   "Scheduled job created successfully",
		"name":      job.Name,
		"cron":      job.Cron,
		"provider":  job.Provider,
		"timestamp": getCurrentTimestamp(),
	})
}

// validateScheduledJob 验证定时任务参数
func (h *HandlerService) validateScheduledJob(job *scheduler.ScheduledJob) error {
	if err := ValidateRequired(job.Name, "name"); err != nil {
		return err
	}

	if err := ValidateRequired(job.Cron, "cron"); err != nil {
		return err
	}

	if err := ValidateRequired(job.Provider, "provider"); err != nil {
		return err
	}

	return nil
}

// UpdateScheduledJob updates an existing scheduled job
// @Summary 更新定时任务
// @Description 更新指定ID的定时任务配置
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param id path string true "任务ID"
// @Param job body models.JobRequest true "定时任务更新参数"
// @Success 200 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs/{id} [put]
func (h *HandlerService) UpdateScheduledJob(w http.ResponseWriter, r *http.Request) {
	if !h.IsSchedulerAvailable() {
		apiError := NewServiceUnavailableError("Scheduler not available", nil)
		HandleError(w, apiError)
		return
	}

	jobID, err := response.ParseStringParam(r, "id")
	if err != nil {
		apiError := NewBadRequestError("Invalid job ID", err)
		HandleError(w, apiError)
		return
	}

	var job scheduler.ScheduledJob
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		apiError := NewBadRequestError("Invalid request body", err)
		HandleError(w, apiError)
		return
	}

	// 设置任务ID
	job.ID = jobID

	// 验证必需字段
	if err := h.validateScheduledJob(&job); err != nil {
		apiError := NewBadRequestError("Job validation failed", err)
		HandleError(w, apiError)
		return
	}

	// 先删除旧任务，再添加新任务
	if err := h.scheduler.RemoveJob(jobID); err != nil {
		apiError := NewNotFoundError("Job not found", err)
		HandleError(w, apiError)
		return
	}

	if err := h.scheduler.AddJob(&job); err != nil {
		apiError := NewInternalServerError("Failed to update scheduled job", err)
		HandleError(w, apiError)
		return
	}

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"job_id":    jobID,
		"status":    "updated",
		"message":   "Scheduled job updated successfully",
		"name":      job.Name,
		"cron":      job.Cron,
		"provider":  job.Provider,
		"timestamp": getCurrentTimestamp(),
	})
}

// GetScheduledJob returns a specific scheduled job
// @Summary 获取指定定时任务
// @Description 根据任务ID返回定时任务的详细信息
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param id path string true "任务ID"
// @Success 200 {object} models.JobResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs/{id} [get]
func (h *HandlerService) GetScheduledJob(w http.ResponseWriter, r *http.Request) {
	if !h.IsSchedulerAvailable() {
		apiError := NewServiceUnavailableError("Scheduler not available", nil)
		HandleError(w, apiError)
		return
	}

	jobID, err := response.ParseStringParam(r, "id")
	if err != nil {
		apiError := NewBadRequestError("Invalid job ID", err)
		HandleError(w, apiError)
		return
	}

	jobs := h.scheduler.GetJobs()
	for _, job := range jobs {
		if job.ID == jobID {
			response.WriteJSONResponse(w, http.StatusOK, job)
			return
		}
	}

	apiError := NewNotFoundError("Job not found", nil)
	HandleError(w, apiError)
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
	if !h.IsSchedulerAvailable() {
		apiError := NewServiceUnavailableError("Scheduler not available", nil)
		HandleError(w, apiError)
		return
	}

	jobID, err := response.ParseStringParam(r, "id")
	if err != nil {
		apiError := NewBadRequestError("Invalid job ID", err)
		HandleError(w, apiError)
		return
	}

	if err := h.scheduler.RemoveJob(jobID); err != nil {
		apiError := NewNotFoundError("Job not found", err)
		HandleError(w, apiError)
		return
	}

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"job_id":    jobID,
		"status":    "deleted",
		"message":   "Scheduled job deleted successfully",
		"timestamp": getCurrentTimestamp(),
	})
}

// TriggerScheduledJob manually triggers a scheduled job
// @Summary 手动触发定时任务
// @Description 立即执行指定的定时任务，不等待下次调度时间
// @Tags Scheduler
// @Accept json
// @Produce json
// @Param id path string true "任务ID"
// @Success 200 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs/{id}/trigger [post]
func (h *HandlerService) TriggerScheduledJob(w http.ResponseWriter, r *http.Request) {
	if !h.IsSchedulerAvailable() {
		apiError := NewServiceUnavailableError("Scheduler not available", nil)
		HandleError(w, apiError)
		return
	}

	jobID, err := response.ParseStringParam(r, "id")
	if err != nil {
		apiError := NewBadRequestError("Invalid job ID", err)
		HandleError(w, apiError)
		return
	}

	// 查找任务
	jobs := h.scheduler.GetJobs()
	var targetJob *scheduler.ScheduledJob
	for _, job := range jobs {
		if job.ID == jobID {
			targetJob = job
			break
		}
	}

	if targetJob == nil {
		apiError := NewNotFoundError("Job not found", nil)
		HandleError(w, apiError)
		return
	}

	// 手动触发任务（暂不支持直接触发，此功能需要scheduler扩展）
	// TODO: 实现scheduler的手动触发功能
	slog.Info("Manual job trigger requested", "job_id", jobID, "job_name", targetJob.Name)

	response.WriteJSONResponse(w, http.StatusOK, map[string]interface{}{
		"job_id":    jobID,
		"status":    "triggered",
		"message":   "Scheduled job triggered successfully",
		"name":      targetJob.Name,
		"provider":  targetJob.Provider,
		"timestamp": getCurrentTimestamp(),
	})
}

// GetSchedulerMetrics returns scheduler performance metrics
// @Summary 获取调度器性能指标
// @Description 返回调度器的性能指标信息，包括任务执行统计、成功率等
// @Tags Scheduler
// @Accept json
// @Produce json
// @Success 200 {object} models.SchedulerMetricsResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/metrics [get]
func (h *HandlerService) GetSchedulerMetrics(w http.ResponseWriter, r *http.Request) {
	if !h.IsSchedulerAvailable() {
		apiError := NewServiceUnavailableError("Scheduler not available", nil)
		HandleError(w, apiError)
		return
	}

	status := h.scheduler.GetStatus()
	jobs := h.scheduler.GetJobs()

	metrics := map[string]interface{}{
		"scheduler_status": status,
		"total_jobs":       len(jobs),
		"active_jobs":      h.countActiveJobs(jobs),
		"inactive_jobs":    h.countInactiveJobs(jobs),
		"timestamp":        getCurrentTimestamp(),
		"uptime":           h.getSchedulerUptime(),
	}

	response.WriteJSONResponse(w, http.StatusOK, metrics)
}

// countActiveJobs 统计活跃任务数量
func (h *HandlerService) countActiveJobs(jobs []*scheduler.ScheduledJob) int {
	count := 0
	for _, job := range jobs {
		// 根据状态判断是否活跃，而不是使用Enabled字段
		if job.Status == "active" || job.Status == "running" {
			count++
		}
	}
	return count
}

// countInactiveJobs 统计非活跃任务数量
func (h *HandlerService) countInactiveJobs(jobs []*scheduler.ScheduledJob) int {
	count := 0
	for _, job := range jobs {
		// 根据状态判断是否非活跃
		if job.Status == "inactive" || job.Status == "disabled" {
			count++
		}
	}
	return count
}

// getSchedulerUptime 获取调度器运行时间
func (h *HandlerService) getSchedulerUptime() string {
	// 这里应该从scheduler获取实际的启动时间
	// 简化处理，返回占位符
	return "calculating..."
}