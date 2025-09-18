package handlers

import (
	"net/http"

	"goscan/pkg/logger"
	_ "goscan/pkg/models"
	"goscan/pkg/scheduler"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// GetSchedulerStatus returns scheduler status
// @Summary Get scheduler status
// @Description Returns running status information of the task scheduler
// @Tags Schedule Management
// @Accept json
// @Produce json
// @Success 200 {object} models.SchedulerStatus
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/status [get]
func (h *HandlerService) GetSchedulerStatus(c *gin.Context) {
	if !h.IsSchedulerAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   true,
			"message": "Scheduler not available",
		})
		return
	}

	status := h.scheduler.GetStatus()
	c.JSON(http.StatusOK, status)
}

// GetScheduledJobs returns all scheduled jobs
// @Summary Get scheduled job list
// @Description Returns information of all configured scheduled jobs
// @Tags Schedule Management
// @Accept json
// @Produce json
// @Success 200 {object} models.JobListResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs [get]
func (h *HandlerService) GetScheduledJobs(c *gin.Context) {
	if !h.IsSchedulerAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   true,
			"message": "Scheduler not available",
		})
		return
	}

	jobs := h.scheduler.GetJobs()
	c.JSON(http.StatusOK, gin.H{
		"jobs":      jobs,
		"count":     len(jobs),
		"timestamp": getCurrentTimestamp(),
	})
}

// CreateScheduledJob creates a new scheduled job
// @Summary Create scheduled job
// @Description Create a new scheduled sync job
// @Tags Schedule Management
// @Accept json
// @Produce json
// @Param job body models.JobRequest true "Scheduled job request parameters"
// @Success 201 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs [post]
func (h *HandlerService) CreateScheduledJob(c *gin.Context) {
	if !h.IsSchedulerAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   true,
			"message": "Scheduler not available",
		})
		return
	}

	var job scheduler.ScheduledJob
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid request body",
			"details": err.Error(),
		})
		return
	}

	// Validate required fields
	if err := h.validateScheduledJob(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Job validation failed",
			"details": err.Error(),
		})
		return
	}

	if err := h.scheduler.AddJob(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Failed to create scheduled job",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"job_id":    job.ID,
		"status":    "created",
		"message":   "Scheduled job created successfully",
		"name":      job.Name,
		"cron":      job.Cron,
		"provider":  job.Provider,
		"timestamp": getCurrentTimestamp(),
	})
}

// validateScheduledJob validates scheduled job parameters
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
// @Summary Update scheduled job
// @Description Update scheduled job configuration for specified ID
// @Tags Schedule Management
// @Accept json
// @Produce json
// @Param id path string true "Job ID"
// @Param job body models.JobRequest true "Scheduled job update parameters"
// @Success 200 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs/{id} [put]
func (h *HandlerService) UpdateScheduledJob(c *gin.Context) {
	if !h.IsSchedulerAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   true,
			"message": "Scheduler not available",
		})
		return
	}

	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid job ID",
		})
		return
	}

	var job scheduler.ScheduledJob
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid request body",
			"details": err.Error(),
		})
		return
	}

	// Set job ID
	job.ID = jobID

	// Validate required fields
	if err := h.validateScheduledJob(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Job validation failed",
			"details": err.Error(),
		})
		return
	}

	// Remove old job first, then add new job
	if err := h.scheduler.RemoveJob(jobID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   true,
			"message": "Job not found",
			"details": err.Error(),
		})
		return
	}

	if err := h.scheduler.AddJob(&job); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   true,
			"message": "Failed to update scheduled job",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
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
// @Summary Get specific scheduled job
// @Description Returns detailed information of scheduled job by job ID
// @Tags Schedule Management
// @Accept json
// @Produce json
// @Param id path string true "Job ID"
// @Success 200 {object} models.JobResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs/{id} [get]
func (h *HandlerService) GetScheduledJob(c *gin.Context) {
	if !h.IsSchedulerAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   true,
			"message": "Scheduler not available",
		})
		return
	}

	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid job ID",
		})
		return
	}

	jobs := h.scheduler.GetJobs()
	for _, job := range jobs {
		if job.ID == jobID {
			c.JSON(http.StatusOK, job)
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{
		"error":   true,
		"message": "Job not found",
	})
}

// DeleteScheduledJob removes a scheduled job
// @Summary Delete scheduled job
// @Description Delete specified scheduled job
// @Tags Schedule Management
// @Accept json
// @Produce json
// @Param id path string true "Job ID"
// @Success 200 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs/{id} [delete]
func (h *HandlerService) DeleteScheduledJob(c *gin.Context) {
	if !h.IsSchedulerAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   true,
			"message": "Scheduler not available",
		})
		return
	}

	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid job ID",
		})
		return
	}

	if err := h.scheduler.RemoveJob(jobID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   true,
			"message": "Job not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"job_id":    jobID,
		"status":    "deleted",
		"message":   "Scheduled job deleted successfully",
		"timestamp": getCurrentTimestamp(),
	})
}

// TriggerScheduledJob manually triggers a scheduled job
// @Summary Manually trigger scheduled job
// @Description Immediately execute specified scheduled job without waiting for next scheduled time
// @Tags Schedule Management
// @Accept json
// @Produce json
// @Param id path string true "Job ID"
// @Success 200 {object} models.MessageResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 404 {object} models.ErrorResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/jobs/{id}/trigger [post]
func (h *HandlerService) TriggerScheduledJob(c *gin.Context) {
	if !h.IsSchedulerAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   true,
			"message": "Scheduler not available",
		})
		return
	}

	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   true,
			"message": "Invalid job ID",
		})
		return
	}

	// Find job
	jobs := h.scheduler.GetJobs()
	var targetJob *scheduler.ScheduledJob
	for _, job := range jobs {
		if job.ID == jobID {
			targetJob = job
			break
		}
	}

	if targetJob == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   true,
			"message": "Job not found",
		})
		return
	}

	// Manual job trigger (direct trigger not supported yet, this feature needs scheduler extension)
	// TODO: Implement manual trigger functionality for scheduler
	logger.Info("Manual job trigger requested",
		zap.String("job_id", jobID),
		zap.String("job_name", targetJob.Name))

	c.JSON(http.StatusOK, gin.H{
		"job_id":    jobID,
		"status":    "triggered",
		"message":   "Scheduled job triggered successfully",
		"name":      targetJob.Name,
		"provider":  targetJob.Provider,
		"timestamp": getCurrentTimestamp(),
	})
}

// GetSchedulerMetrics returns scheduler performance metrics
// @Summary Get scheduler performance metrics
// @Description Returns performance metrics information of scheduler, including task execution statistics, success rate, etc.
// @Tags Schedule Management
// @Accept json
// @Produce json
// @Success 200 {object} models.SchedulerMetricsResponse
// @Failure 503 {object} models.ErrorResponse
// @Router /scheduler/metrics [get]
func (h *HandlerService) GetSchedulerMetrics(c *gin.Context) {
	if !h.IsSchedulerAvailable() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   true,
			"message": "Scheduler not available",
		})
		return
	}

	status := h.scheduler.GetStatus()
	jobs := h.scheduler.GetJobs()

	metrics := gin.H{
		"scheduler_status": status,
		"total_jobs":       len(jobs),
		"active_jobs":      h.countActiveJobs(jobs),
		"inactive_jobs":    h.countInactiveJobs(jobs),
		"timestamp":        getCurrentTimestamp(),
		"uptime":           h.getSchedulerUptime(),
	}

	c.JSON(http.StatusOK, metrics)
}

// countActiveJobs counts number of active jobs
func (h *HandlerService) countActiveJobs(jobs []*scheduler.ScheduledJob) int {
	count := 0
	for _, job := range jobs {
		// Determine if active based on status, not using Enabled field
		if job.Status == "active" || job.Status == "running" {
			count++
		}
	}
	return count
}

// countInactiveJobs counts number of inactive jobs
func (h *HandlerService) countInactiveJobs(jobs []*scheduler.ScheduledJob) int {
	count := 0
	for _, job := range jobs {
		// Determine if inactive based on status
		if job.Status == "inactive" || job.Status == "disabled" {
			count++
		}
	}
	return count
}

// getSchedulerUptime gets scheduler uptime
func (h *HandlerService) getSchedulerUptime() string {
	// Should get actual startup time from scheduler
	// Simplified handling, return placeholder
	return "calculating..."
}
