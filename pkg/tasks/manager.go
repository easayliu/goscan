package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/cloudsync"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// TaskManagerImpl task manager implementation
type TaskManagerImpl struct {
	config               *config.Config
	ctx                  context.Context
	tasks                map[string]*Task
	tasksMutex           sync.RWMutex
	taskHistory          []*Task
	chClient             *clickhouse.Client
	notificationExecutor *NotificationTaskExecutor
	executorFactory      *ExecutorFactory
	maxTasks             int
}

// NewTaskManager creates a new task manager
func NewTaskManager(ctx context.Context, cfg *config.Config) (*TaskManagerImpl, error) {
	logger.Info("Initializing task manager")

	// Create ClickHouse client
	chClient, err := clickhouse.NewClient(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	// Test connection
	if err := chClient.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	// Initialize notification executor
	notificationExecutor, err := NewNotificationTaskExecutor(chClient, cfg)
	if err != nil {
		logger.Warn("Failed to initialize notification executor", zap.Error(err))
		// Don't return error, continue initializing task manager, only notification functionality unavailable
	}

	// Create executor factory
	executorFactory := NewExecutorFactory(cfg, chClient)

	tm := &TaskManagerImpl{
		config:               cfg,
		ctx:                  ctx,
		tasks:                make(map[string]*Task),
		taskHistory:          make([]*Task, 0),
		chClient:             chClient,
		notificationExecutor: notificationExecutor,
		executorFactory:      executorFactory,
		maxTasks:             10, // Maximum concurrent tasks
	}

	logger.Info("Task manager initialized")
	return tm, nil
}

// ExecuteTask executes a task
func (tm *TaskManagerImpl) ExecuteTask(ctx context.Context, req *TaskRequest) (*TaskResult, error) {
	// Generate ID (if not provided)
	if req.ID == "" {
		req.ID = uuid.New().String()
	}

	// Check if too many tasks are running
	if err := tm.checkTaskLimit(); err != nil {
		return nil, err
	}

	// Create task
	task := tm.createTask(req)

	// Add to task list
	if err := tm.addTask(task); err != nil {
		return nil, err
	}

	// Execute task asynchronously
	go tm.executeTaskInternal(ctx, task)

	// Return initial result
	return &TaskResult{
		ID:        task.ID,
		Type:      string(task.Type),
		Status:    string(task.Status),
		StartedAt: task.StartTime,
		Success:   true,
		Message:   "Task started successfully",
	}, nil
}

// ExecuteTaskSync runs a task on the calling goroutine and returns only once it
// has finished. ExecuteTask answers the moment the task is accepted, which is
// what the HTTP API wants (it polls for the result afterwards) but not what a
// caller that has to report the outcome wants: a cron job logging "completed"
// before the sync has fetched a single row, or a one-shot `goscan --once` whose
// exit code has to say whether the data landed.
func (tm *TaskManagerImpl) ExecuteTaskSync(ctx context.Context, req *TaskRequest) (*TaskResult, error) {
	if req.ID == "" {
		req.ID = uuid.New().String()
	}

	if err := tm.checkTaskLimit(); err != nil {
		return nil, err
	}

	task := tm.createTask(req)
	if err := tm.addTask(task); err != nil {
		return nil, err
	}

	tm.executeTaskInternal(ctx, task)

	if task.Status == TaskStatusFailed {
		return task.Result, fmt.Errorf("task %s failed: %s", task.ID, task.Error)
	}
	return task.Result, nil
}

// GetTask retrieves a specific task
func (tm *TaskManagerImpl) GetTask(taskID string) (*Task, error) {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	// Check running tasks
	if task, exists := tm.tasks[taskID]; exists {
		return task, nil
	}

	// Check historical tasks
	for _, task := range tm.taskHistory {
		if task.ID == taskID {
			return task, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
}

// GetTasks retrieves all active tasks
func (tm *TaskManagerImpl) GetTasks() []*Task {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	tasks := make([]*Task, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		tasks = append(tasks, task)
	}
	return tasks
}

// GetTaskHistory retrieves task history
func (tm *TaskManagerImpl) GetTaskHistory() []*Task {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	// Create copy to avoid concurrent modification
	history := make([]*Task, len(tm.taskHistory))
	copy(history, tm.taskHistory)
	return history
}

// CancelTask cancels a task
func (tm *TaskManagerImpl) CancelTask(taskID string) error {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	task, exists := tm.tasks[taskID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}

	if task.Status != TaskStatusRunning {
		return fmt.Errorf("task %s is not running (status: %s)", taskID, task.Status)
	}

	// Call cancel function
	if task.Cancel != nil {
		task.Cancel()
	}

	// Update task status
	task.Status = TaskStatusCancelled
	task.EndTime = time.Now()
	task.Duration = task.EndTime.Sub(task.StartTime)
	task.Error = "Task was cancelled"

	logger.Info("Task cancelled", zap.String("task_id", taskID))
	return nil
}

// GetRunningTaskCount retrieves count of running tasks
func (tm *TaskManagerImpl) GetRunningTaskCount() int {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	count := 0
	for _, task := range tm.tasks {
		if task.Status == TaskStatusRunning {
			count++
		}
	}
	return count
}

// GetTotalTaskCount retrieves total task count
func (tm *TaskManagerImpl) GetTotalTaskCount() int {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()
	return len(tm.tasks)
}

// Private methods

// checkTaskLimit checks task limits
func (tm *TaskManagerImpl) checkTaskLimit() error {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	runningCount := 0
	for _, task := range tm.tasks {
		if task.Status == TaskStatusRunning {
			runningCount++
		}
	}

	if runningCount >= tm.maxTasks {
		return fmt.Errorf("%w: %d running tasks (max: %d)", ErrTooManyTasks, runningCount, tm.maxTasks)
	}

	return nil
}

// createTask creates a task
func (tm *TaskManagerImpl) createTask(req *TaskRequest) *Task {
	_, cancel := context.WithCancel(tm.ctx)

	return &Task{
		ID:        req.ID,
		Type:      req.Type,
		Provider:  req.Provider,
		Status:    TaskStatusPending,
		StartTime: time.Now(),
		Config:    req.Config,
		Cancel:    cancel,
	}
}

// addTask registers the task, refusing one whose provider is already busy.
//
// Two syncs of the same cloud running at once pull and write the same periods
// twice: harmless in the end (the tables are ReplacingMergeTree) but a waste of
// API quota, and the pair of them report progress that is impossible to read.
// It happens easily enough — someone triggers a sync from the UI while the cron
// job is still running, or clicks the button twice. The check lives here, under
// the same lock as the insert, so the manual path and the scheduled one cannot
// slip past each other.
func (tm *TaskManagerImpl) addTask(task *Task) error {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	for _, existing := range tm.tasks {
		if existing.Provider != task.Provider || existing.Type != task.Type {
			continue
		}
		if existing.Status == TaskStatusPending || existing.Status == TaskStatusRunning {
			return fmt.Errorf("%w: %s %s task %s", ErrTaskAlreadyRunning,
				task.Provider, task.Type, existing.ID)
		}
	}

	tm.tasks[task.ID] = task
	return nil
}

// executeTaskInternal internal task execution
func (tm *TaskManagerImpl) executeTaskInternal(ctx context.Context, task *Task) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Task execution panicked", zap.String("task_id", task.ID), zap.Any("panic", r))
			tm.finishTask(task, nil, fmt.Errorf("task panicked: %v", r))
		}
	}()

	// Update task status to running
	tm.updateTaskStatus(task, TaskStatusRunning)

	logger.Info("Starting task execution", zap.String("task_id", task.ID), zap.String("type", string(task.Type)), zap.String("provider", task.Provider))

	var result *TaskResult
	var err error

	// Execute based on task type
	switch task.Type {
	case TaskTypeSync:
		result, err = tm.executeSyncTask(ctx, task)
	case TaskTypeNotification:
		result, err = tm.executeNotificationTask(ctx, task)
	default:
		err = fmt.Errorf("unsupported task type: %s", task.Type)
	}

	// Complete task
	tm.finishTask(task, result, err)
}

// executeSyncTask executes sync task
func (tm *TaskManagerImpl) executeSyncTask(ctx context.Context, task *Task) (*TaskResult, error) {
	// Create sync executor
	executor, err := tm.executorFactory.CreateExecutor(ctx, task.Provider)
	if err != nil {
		return nil, fmt.Errorf("failed to create executor for provider %s: %w", task.Provider, err)
	}

	// Convert task config to sync config
	syncConfig := &SyncConfig{
		Provider:       task.Provider,
		SyncMode:       task.Config.SyncMode,
		UseDistributed: task.Config.UseDistributed,
		CreateTable:    task.Config.CreateTable,
		ForceUpdate:    task.Config.ForceUpdate,
		Granularity:    task.Config.Granularity,
		BillPeriod:     task.Config.BillPeriod,
		StartPeriod:    task.Config.StartPeriod,
		EndPeriod:      task.Config.EndPeriod,
		Limit:          task.Config.Limit,
		Progress: func(p cloudsync.SyncProgress) {
			tm.updateTaskProgress(task, p)
		},
	}

	// Tables are not created here on purpose: schema changes belong to
	// `goscan --ddl` and the DDL Job that applies it, so a sync run needs no
	// DDL privileges and can never alter a production table by accident.
	if task.Config.CreateTable {
		logger.Warn("create_table is ignored: apply `goscan --ddl` with the DDL Job instead",
			zap.String("task_id", task.ID),
			zap.String("provider", task.Provider))
	}

	// Execute sync
	syncResult, err := executor.ExecuteSync(ctx, syncConfig)
	if err != nil {
		return nil, fmt.Errorf("sync execution failed: %w", err)
	}

	// Convert result
	return tm.convertSyncResult(syncResult, task), nil
}

// executeNotificationTask executes notification task
func (tm *TaskManagerImpl) executeNotificationTask(ctx context.Context, task *Task) (*TaskResult, error) {
	if tm.notificationExecutor == nil {
		return nil, fmt.Errorf("notification executor not available")
	}

	result, err := tm.notificationExecutor.SendNotification(ctx, &task.Config)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotificationFailed, err)
	}

	return result, nil
}

// convertSyncResult converts sync result
func (tm *TaskManagerImpl) convertSyncResult(syncResult *SyncResult, task *Task) *TaskResult {
	return &TaskResult{
		ID:               task.ID,
		Type:             string(task.Type),
		Status:           string(TaskStatusCompleted),
		RecordsProcessed: syncResult.RecordsProcessed,
		RecordsFetched:   syncResult.RecordsFetched,
		Duration:         syncResult.Duration,
		Success:          syncResult.Success,
		Message:          syncResult.Message,
		Error:            syncResult.Error,
		StartedAt:        syncResult.StartedAt,
		CompletedAt:      syncResult.CompletedAt,
	}
}

// updateTaskStatus updates task status
func (tm *TaskManagerImpl) updateTaskStatus(task *Task, status TaskStatus) {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()
	task.Status = status
}

// updateTaskProgress records how far the sync has got.
//
// The progress value is replaced, never edited in place: GetTask hands its
// caller the *Task itself and the HTTP handler serialises it after the lock is
// gone, so anything a running sync writes has to be immutable once published.
func (tm *TaskManagerImpl) updateTaskProgress(task *Task, p cloudsync.SyncProgress) {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()
	task.Progress = &TaskProgress{
		Period:      p.Period,
		Granularity: p.Granularity,
		Done:        p.Done,
		Total:       p.Total,
		UpdatedAt:   time.Now(),
	}
}

// finishTask completes a task
func (tm *TaskManagerImpl) finishTask(task *Task, result *TaskResult, err error) {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	// Set end time and duration
	task.EndTime = time.Now()
	task.Duration = task.EndTime.Sub(task.StartTime)

	// Set task status and result
	if err != nil {
		task.Status = TaskStatusFailed
		task.Error = err.Error()
		logger.Error("Task execution failed", zap.String("task_id", task.ID), zap.Error(err))
	} else {
		task.Status = TaskStatusCompleted
		task.Result = result
		logger.Info("Task execution completed", zap.String("task_id", task.ID), zap.Duration("duration", task.Duration))
	}

	// Remove from active tasks and add to history
	delete(tm.tasks, task.ID)
	tm.taskHistory = append(tm.taskHistory, task)

	// Limit history record count
	if len(tm.taskHistory) > 100 {
		tm.taskHistory = tm.taskHistory[1:]
	}
}
