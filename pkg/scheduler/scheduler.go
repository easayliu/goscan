package scheduler

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"goscan/pkg/tasks"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

// Job statuses
const (
	JobStatusScheduled = "scheduled"
	JobStatusRunning   = "running"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"
)

// Error variables
var (
	ErrJobNotFound = fmt.Errorf("job not found")
)

// Config holds scheduler configuration
type Config struct {
	Config *config.Config
}

// TaskScheduler manages scheduled tasks using cron
type TaskScheduler struct {
	cron      *cron.Cron
	config    *Config
	ctx       context.Context
	jobs      map[string]*ScheduledJob
	jobsMutex sync.RWMutex
	taskMgr   tasks.TaskManager
}

// ScheduledJob represents a scheduled job
type ScheduledJob struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Cron     string    `json:"cron"`
	Provider string    `json:"provider"`
	Config   JobConfig `json:"config"`
	NextRun  time.Time `json:"next_run"`
	LastRun  time.Time `json:"last_run"`
	Status   string    `json:"status"`
	EntryID  cron.EntryID
}

// JobConfig holds job-specific configuration
type JobConfig struct {
	SyncMode       string `json:"sync_mode"`
	UseDistributed bool   `json:"use_distributed"`
	CreateTable    bool   `json:"create_table"`
	ForceUpdate    bool   `json:"force_update"`
	Granularity    string `json:"granularity,omitempty"` // For AliCloud
}

// NewTaskScheduler creates a new task scheduler
func NewTaskScheduler(ctx context.Context, config *Config) (*TaskScheduler, error) {
	logger.Info("Initializing task scheduler")

	// Create cron scheduler with logger
	cronScheduler := cron.New(
		cron.WithChain(cron.Recover(cron.DefaultLogger)),
	)

	// Create task manager
	taskMgr, err := tasks.NewTaskManager(ctx, config.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create task manager: %w", err)
	}

	scheduler := &TaskScheduler{
		cron:    cronScheduler,
		config:  config,
		ctx:     ctx,
		jobs:    make(map[string]*ScheduledJob),
		taskMgr: taskMgr,
	}

	// Load predefined jobs from configuration
	if err := scheduler.loadConfiguredJobs(); err != nil {
		return nil, fmt.Errorf("failed to load configured jobs: %w", err)
	}

	logger.Info("Task scheduler initialized", zap.Int("job_count", len(scheduler.jobs)))
	return scheduler, nil
}

// Start starts the task scheduler
func (ts *TaskScheduler) Start() error {
	logger.Info("Starting task scheduler")

	ts.cron.Start()

	// Update next run times for all jobs after cron starts
	ts.jobsMutex.Lock()
	for _, job := range ts.jobs {
		if err := ts.updateJobNextRunTime(job); err != nil {
			logger.Warn("Failed to update next run time after start",
				zap.String("job_name", job.Name),
				zap.Error(err))
		}
	}
	ts.jobsMutex.Unlock()

	// Log scheduled jobs
	ts.logScheduledJobs()

	// Keep scheduler running until context is cancelled
	<-ts.ctx.Done()
	logger.Info("Task scheduler context cancelled")

	return nil
}

// Shutdown gracefully shuts down the task scheduler
func (ts *TaskScheduler) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down task scheduler")

	// Stop accepting new jobs
	cronCtx := ts.cron.Stop()

	// Wait for running jobs to complete or timeout
	select {
	case <-cronCtx.Done():
		logger.Info("All scheduled jobs completed")
	case <-ctx.Done():
		logger.Warn("Scheduler shutdown timeout, some jobs may still be running")
	}

	return nil
}

// AddJob adds a new scheduled job
func (ts *TaskScheduler) AddJob(job *ScheduledJob) error {
	ts.jobsMutex.Lock()
	defer ts.jobsMutex.Unlock()

	if job.ID == "" {
		job.ID = uuid.New().String()
	}

	// Create cron job function
	jobFunc := ts.createJobFunction(job)

	// Add to cron scheduler
	entryID, err := ts.cron.AddFunc(job.Cron, jobFunc)
	if err != nil {
		return fmt.Errorf("failed to add cron job: %w", err)
	}

	job.EntryID = entryID
	job.Status = JobStatusScheduled

	// Update next run time
	if err := ts.updateJobNextRunTime(job); err != nil {
		logger.Warn("Failed to update next run time", zap.String("job_name", job.Name), zap.Error(err))
	}

	ts.jobs[job.ID] = job

	logger.Info("Added scheduled job",
		zap.String("job_id", job.ID),
		zap.String("job_name", job.Name),
		zap.String("cron", job.Cron),
		zap.String("provider", job.Provider),
		zap.Time("next_run", job.NextRun),
	)

	return nil
}

// RemoveJob removes a scheduled job
func (ts *TaskScheduler) RemoveJob(jobID string) error {
	ts.jobsMutex.Lock()
	defer ts.jobsMutex.Unlock()

	job, exists := ts.jobs[jobID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}

	// Remove from cron scheduler
	ts.cron.Remove(job.EntryID)

	// Remove from jobs map
	delete(ts.jobs, jobID)

	logger.Info("Removed scheduled job", zap.String("job_id", jobID), zap.String("job_name", job.Name))
	return nil
}

// GetJobs returns all scheduled jobs
func (ts *TaskScheduler) GetJobs() []*ScheduledJob {
	ts.jobsMutex.RLock()
	defer ts.jobsMutex.RUnlock()

	jobs := make([]*ScheduledJob, 0, len(ts.jobs))
	for _, job := range ts.jobs {
		// Update next run time
		ts.updateJobNextRunTime(job)
		jobs = append(jobs, job)
	}

	return jobs
}

// GetJob returns a specific scheduled job
func (ts *TaskScheduler) GetJob(jobID string) (*ScheduledJob, error) {
	ts.jobsMutex.RLock()
	defer ts.jobsMutex.RUnlock()

	job, exists := ts.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}

	return job, nil
}

// GetStatus returns scheduler status
func (ts *TaskScheduler) GetStatus() map[string]interface{} {
	ts.jobsMutex.RLock()
	defer ts.jobsMutex.RUnlock()

	status := map[string]interface{}{
		"running":   ts.cron != nil,
		"job_count": len(ts.jobs),
		"entries":   len(ts.cron.Entries()),
		"timestamp": time.Now().UTC(),
	}

	return status
}

// loadConfiguredJobs loads predefined jobs from configuration
func (ts *TaskScheduler) loadConfiguredJobs() error {
	// First load tasks defined in configuration file
	if ts.config.Config.Scheduler != nil && ts.config.Config.Scheduler.Enabled && len(ts.config.Config.Scheduler.Jobs) > 0 {
		logger.Info("Loading jobs from configuration file", zap.Int("count", len(ts.config.Config.Scheduler.Jobs)))

		for _, configJob := range ts.config.Config.Scheduler.Jobs {
			// Convert ScheduledJob from configuration file to scheduler's ScheduledJob
			job := &ScheduledJob{
				Name:     configJob.Name,
				Provider: configJob.Provider,
				Cron:     configJob.Cron,
				Config: JobConfig{
					SyncMode:       configJob.Config.SyncMode,
					UseDistributed: configJob.Config.UseDistributed,
					CreateTable:    configJob.Config.CreateTable,
					ForceUpdate:    configJob.Config.ForceUpdate,
					Granularity:    configJob.Config.Granularity,
				},
			}

			if err := ts.AddJob(job); err != nil {
				logger.Warn("Failed to add configured job", zap.String("job_name", job.Name), zap.Error(err))
			} else {
				logger.Info("Added configured job", zap.String("job_name", job.Name), zap.String("provider", job.Provider), zap.String("cron", job.Cron))
			}
		}

		return nil
	}

	// Use default tasks if no tasks in configuration file or scheduler not enabled
	logger.Info("No jobs found in configuration, loading default jobs")
	defaultJobs := ts.getDefaultJobs()

	for _, job := range defaultJobs {
		if err := ts.AddJob(job); err != nil {
			logger.Warn("Failed to add default job", zap.String("job_name", job.Name), zap.Error(err))
		}
	}

	return nil
}

// getDefaultJobs returns default scheduled jobs based on configuration
func (ts *TaskScheduler) getDefaultJobs() []*ScheduledJob {
	var jobs []*ScheduledJob

	// VolcEngine daily sync job
	if volcCfg := ts.config.Config.GetVolcEngineConfig(); volcCfg.AccessKey != "" {
		jobs = append(jobs, &ScheduledJob{
			Name:     "volcengine_daily_sync",
			Cron:     "0 2 * * *", // Daily at 2 AM
			Provider: "volcengine",
			Config: JobConfig{
				SyncMode:    "sync-optimal",
				CreateTable: true,
				ForceUpdate: true,
			},
		})
	}

	// AliCloud daily sync job
	if aliCfg := ts.config.Config.GetAliCloudConfig(); aliCfg.AccessKeyID != "" {
		jobs = append(jobs, &ScheduledJob{
			Name:     "alicloud_daily_sync",
			Cron:     "0 3 * * *", // Daily at 3 AM
			Provider: "alicloud",
			Config: JobConfig{
				SyncMode:    "sync-optimal",
				CreateTable: true,
				ForceUpdate: true,
				Granularity: "both",
			},
		})
	}

	// WeChat notification job (only use default task when no configuration file is defined)
	if weChatCfg := ts.config.Config.GetWeChatConfig(); weChatCfg.Enabled && weChatCfg.WebhookURL != "" {
		jobs = append(jobs, &ScheduledJob{
			Name:     "daily_cost_report_notification",
			Cron:     "0 9 * * *", // Default daily at 9 AM
			Provider: "notification",
			Config: JobConfig{
				SyncMode: "cost_report", // Special identifier
			},
		})
	}

	logger.Info("Generated default jobs", zap.Int("count", len(jobs)))
	return jobs
}

// createJobFunction creates a function to execute for a scheduled job
func (ts *TaskScheduler) createJobFunction(job *ScheduledJob) func() {
	return func() {
		logger.Info("Executing scheduled job", zap.String("job_id", job.ID), zap.String("job_name", job.Name), zap.String("provider", job.Provider))

		// Update job status
		ts.updateJobStatus(job, JobStatusRunning)
		ts.updateJobLastRun(job, time.Now())

		// Create task request based on provider type
		var taskReq *tasks.TaskRequest
		if job.Provider == "notification" {
			// Create notification task
			taskReq = &tasks.TaskRequest{
				ID:       uuid.New().String(),
				Type:     tasks.TaskTypeNotification,
				Provider: job.Provider,
				Config: tasks.TaskConfig{
					SyncMode: job.Config.SyncMode,
				},
			}
		} else {
			// Create sync task
			taskReq = &tasks.TaskRequest{
				ID:       uuid.New().String(),
				Type:     tasks.TaskTypeSync,
				Provider: job.Provider,
				Config: tasks.TaskConfig{
					SyncMode:       job.Config.SyncMode,
					UseDistributed: job.Config.UseDistributed,
					CreateTable:    job.Config.CreateTable,
					ForceUpdate:    job.Config.ForceUpdate,
					Granularity:    job.Config.Granularity,
				},
			}
		}

		// Execute task
		result, err := ts.taskMgr.ExecuteTask(ts.ctx, taskReq)
		if err != nil {
			logger.Error("Scheduled job failed", zap.String("job_name", job.Name), zap.Error(err))
			ts.updateJobStatus(job, JobStatusFailed)
			return
		}

		// Check if result is not nil before accessing its fields
		if result != nil {
			logger.Info("Scheduled job completed successfully",
				zap.String("job_name", job.Name),
				zap.Duration("duration", result.Duration),
				zap.Int("records_processed", result.RecordsProcessed),
			)
		} else {
			logger.Info("Scheduled job completed successfully",
				zap.String("job_name", job.Name),
			)
		}

		// Update job status
		ts.updateJobStatus(job, JobStatusCompleted)
	}
}

// logScheduledJobs logs information about all scheduled jobs
func (ts *TaskScheduler) logScheduledJobs() {
	ts.jobsMutex.RLock()
	defer ts.jobsMutex.RUnlock()

	if len(ts.jobs) == 0 {
		logger.Info("No scheduled jobs configured")
		return
	}

	logger.Info("Active scheduled jobs:")
	for _, job := range ts.jobs {
		logger.Info("Scheduled job",
			zap.String("job_name", job.Name),
			zap.String("provider", job.Provider),
			zap.String("cron", job.Cron),
			zap.Time("next_run", job.NextRun),
			zap.String("status", job.Status),
		)
	}
}

// updateJobNextRunTime updates the next run time for a job
func (ts *TaskScheduler) updateJobNextRunTime(job *ScheduledJob) error {
	entries := ts.cron.Entries()
	for _, entry := range entries {
		if entry.ID == job.EntryID {
			job.NextRun = entry.Next
			return nil
		}
	}

	// If not found in entries, try to parse cron expression manually
	if schedule, err := cron.ParseStandard(job.Cron); err == nil {
		job.NextRun = schedule.Next(time.Now())
		return nil
	} else {
		return fmt.Errorf("failed to parse cron expression %s: %w", job.Cron, err)
	}
}

// updateJobStatus updates the status of a job
func (ts *TaskScheduler) updateJobStatus(job *ScheduledJob, status string) {
	ts.jobsMutex.Lock()
	defer ts.jobsMutex.Unlock()
	job.Status = status
}

// updateJobLastRun updates the last run time of a job
func (ts *TaskScheduler) updateJobLastRun(job *ScheduledJob, lastRun time.Time) {
	ts.jobsMutex.Lock()
	defer ts.jobsMutex.Unlock()
	job.LastRun = lastRun
}
