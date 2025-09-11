package scheduler

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/tasks"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
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
	taskMgr   *tasks.TaskManager
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
	slog.Info("Initializing task scheduler")

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

	slog.Info("Task scheduler initialized", "job_count", len(scheduler.jobs))
	return scheduler, nil
}

// Start starts the task scheduler
func (ts *TaskScheduler) Start() error {
	slog.Info("Starting task scheduler")

	ts.cron.Start()

	// Log scheduled jobs
	ts.logScheduledJobs()

	// Keep scheduler running until context is cancelled
	<-ts.ctx.Done()
	slog.Info("Task scheduler context cancelled")

	return nil
}

// Shutdown gracefully shuts down the task scheduler
func (ts *TaskScheduler) Shutdown(ctx context.Context) error {
	slog.Info("Shutting down task scheduler")

	// Stop accepting new jobs
	cronCtx := ts.cron.Stop()

	// Wait for running jobs to complete or timeout
	select {
	case <-cronCtx.Done():
		slog.Info("All scheduled jobs completed")
	case <-ctx.Done():
		slog.Warn("Scheduler shutdown timeout, some jobs may still be running")
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
	job.Status = "scheduled"

	// Update next run time
	entries := ts.cron.Entries()
	for _, entry := range entries {
		if entry.ID == entryID {
			job.NextRun = entry.Next
			break
		}
	}

	ts.jobs[job.ID] = job

	slog.Info("Added scheduled job",
		"id", job.ID,
		"name", job.Name,
		"cron", job.Cron,
		"provider", job.Provider,
		"next_run", job.NextRun,
	)

	return nil
}

// RemoveJob removes a scheduled job
func (ts *TaskScheduler) RemoveJob(jobID string) error {
	ts.jobsMutex.Lock()
	defer ts.jobsMutex.Unlock()

	job, exists := ts.jobs[jobID]
	if !exists {
		return fmt.Errorf("job with ID %s not found", jobID)
	}

	// Remove from cron scheduler
	ts.cron.Remove(job.EntryID)

	// Remove from jobs map
	delete(ts.jobs, jobID)

	slog.Info("Removed scheduled job", "id", jobID, "name", job.Name)
	return nil
}

// GetJobs returns all scheduled jobs
func (ts *TaskScheduler) GetJobs() []*ScheduledJob {
	ts.jobsMutex.RLock()
	defer ts.jobsMutex.RUnlock()

	jobs := make([]*ScheduledJob, 0, len(ts.jobs))
	for _, job := range ts.jobs {
		// Update next run time
		entries := ts.cron.Entries()
		for _, entry := range entries {
			if entry.ID == job.EntryID {
				job.NextRun = entry.Next
				break
			}
		}
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
		return nil, fmt.Errorf("job with ID %s not found", jobID)
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
	// Load scheduler configuration from config file
	// This would typically read from a configuration section like:
	// scheduler:
	//   jobs:
	//     - name: "volcengine_daily_sync"
	//       provider: "volcengine"
	//       cron: "0 2 * * *"

	// For now, add some default jobs based on available providers
	defaultJobs := ts.getDefaultJobs()

	for _, job := range defaultJobs {
		if err := ts.AddJob(job); err != nil {
			slog.Warn("Failed to add default job", "job", job.Name, "error", err)
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

	// WeChat notification job
	if weChatCfg := ts.config.Config.GetWeChatConfig(); weChatCfg.Enabled && weChatCfg.WebhookURL != "" {
		jobs = append(jobs, &ScheduledJob{
			Name:     "daily_cost_report_notification",
			Cron:     weChatCfg.SendTime, // 使用配置的发送时间
			Provider: "notification",
			Config: JobConfig{
				SyncMode: "cost_report", // 特殊标识
			},
		})
	}

	slog.Info("Generated default jobs", "count", len(jobs))
	return jobs
}

// createJobFunction creates a function to execute for a scheduled job
func (ts *TaskScheduler) createJobFunction(job *ScheduledJob) func() {
	return func() {
		slog.Info("Executing scheduled job", "id", job.ID, "name", job.Name, "provider", job.Provider)

		// Update job status
		ts.jobsMutex.Lock()
		job.Status = "running"
		job.LastRun = time.Now()
		ts.jobsMutex.Unlock()

		// Create task request based on provider type
		var taskReq *tasks.TaskRequest
		if job.Provider == "notification" {
			// 创建通知任务
			taskReq = &tasks.TaskRequest{
				ID:       uuid.New().String(),
				Type:     tasks.TaskTypeNotification,
				Provider: job.Provider,
				Config: tasks.TaskConfig{
					SyncMode: job.Config.SyncMode,
				},
			}
		} else {
			// 创建同步任务
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
			slog.Error("Scheduled job failed", "job", job.Name, "error", err)
			ts.jobsMutex.Lock()
			job.Status = "failed"
			ts.jobsMutex.Unlock()
			return
		}

		slog.Info("Scheduled job completed successfully",
			"job", job.Name,
			"duration", result.Duration,
			"records_processed", result.RecordsProcessed,
		)

		// Update job status
		ts.jobsMutex.Lock()
		job.Status = "completed"
		ts.jobsMutex.Unlock()
	}
}

// logScheduledJobs logs information about all scheduled jobs
func (ts *TaskScheduler) logScheduledJobs() {
	ts.jobsMutex.RLock()
	defer ts.jobsMutex.RUnlock()

	if len(ts.jobs) == 0 {
		slog.Info("No scheduled jobs configured")
		return
	}

	slog.Info("Active scheduled jobs:")
	for _, job := range ts.jobs {
		slog.Info("Scheduled job",
			"name", job.Name,
			"provider", job.Provider,
			"cron", job.Cron,
			"next_run", job.NextRun,
			"status", job.Status,
		)
	}
}
