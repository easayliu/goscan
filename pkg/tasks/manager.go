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

	// subs are the open task feeds (see Subscribe). Every change to a task is
	// published to them under tasksMutex; subsMu only guards the set itself and
	// is always taken after tasksMutex, never before.
	subsMu sync.Mutex
	subs   map[*TaskFeed]struct{}
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

	// Built before the task starts: from then on its goroutine owns the task
	// and reading Status here would race with it.
	accepted := &TaskResult{
		ID:        task.ID,
		Type:      string(task.Type),
		Status:    string(task.Status),
		StartedAt: task.StartTime,
		Success:   true,
		Message:   "Task started successfully",
	}

	// Execute task asynchronously
	go tm.executeTaskInternal(ctx, task)

	return accepted, nil
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

	switch task.Status {
	case TaskStatusFailed:
		return task.Result, fmt.Errorf("task %s failed: %s", task.ID, task.Error)
	case TaskStatusCancelled:
		return task.Result, fmt.Errorf("%w: %s", ErrTaskCancelled, task.ID)
	}
	return task.Result, nil
}

// GetTask retrieves a specific task, as a copy.
//
// Callers serialise the result after the lock is gone, while the sync keeps
// writing Status and Progress on the live task; handing out the task itself
// made every GET /tasks/{id} during a sync a data race.
func (tm *TaskManagerImpl) GetTask(taskID string) (*Task, error) {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	task := tm.findTaskLocked(taskID)
	if task == nil {
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	snap := snapshot(task)
	return &snap, nil
}

// GetTasks retrieves all active tasks, as copies (see GetTask).
func (tm *TaskManagerImpl) GetTasks() []*Task {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	tasks := make([]*Task, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		snap := snapshot(task)
		tasks = append(tasks, &snap)
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

// CancelTask asks a sync to stop after the pass it is on.
//
// It returns as soon as the request is recorded; the task goes on until that
// pass is written and then ends as cancelled (or completed, if it was the last
// pass). Stopping mid-pass is deliberately not offered: the period in flight
// has been cleared for the re-pull, and cutting it off would leave it half
// written with nothing scheduled to come back for it. Asking twice is fine.
func (tm *TaskManagerImpl) CancelTask(taskID string) error {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	task := tm.findTaskLocked(taskID)
	if task == nil {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if snapshot(task).IsFinal() {
		return fmt.Errorf("%w: task %s has already %s", ErrTaskNotCancellable, taskID, task.Status)
	}
	if task.Type != TaskTypeSync {
		return fmt.Errorf("%w: only syncs can be stopped, task %s is a %s", ErrTaskNotCancellable, taskID, task.Type)
	}
	if task.CancelRequested {
		return nil
	}

	task.CancelRequested = true
	close(task.stop)
	tm.publishLocked(task)

	logger.Info("Task cancel requested, stopping after the current pass", zap.String("task_id", taskID))
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
	return &Task{
		ID:        req.ID,
		Type:      req.Type,
		Provider:  req.Provider,
		Status:    TaskStatusPending,
		StartTime: time.Now(),
		Config:    req.Config,
		stop:      make(chan struct{}),
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
	tm.publishLocked(task)
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
		Stop: task.stop,
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
	status := TaskStatusCompleted
	if syncResult.Cancelled {
		status = TaskStatusCancelled
	}
	return &TaskResult{
		ID:               task.ID,
		Type:             string(task.Type),
		Status:           string(status),
		NotRun:           syncResult.NotRun,
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
	tm.publishLocked(task)
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
		Period:       p.Period,
		Granularity:  p.Granularity,
		Done:         p.Done,
		Total:        p.Total,
		Records:      p.Records,
		RecordsTotal: p.RecordsTotal,
		UpdatedAt:    time.Now(),
	}
	tm.publishLocked(task)
}

// finishTask completes a task
func (tm *TaskManagerImpl) finishTask(task *Task, result *TaskResult, err error) {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	// Set end time and duration
	task.EndTime = time.Now()
	task.Duration = task.EndTime.Sub(task.StartTime)

	// Set task status and result
	switch {
	case err != nil:
		task.Status = TaskStatusFailed
		task.Error = err.Error()
		logger.Error("Task execution failed", zap.String("task_id", task.ID), zap.Error(err))
	case result != nil && result.Status == string(TaskStatusCancelled):
		task.Status = TaskStatusCancelled
		task.Result = result
		logger.Info("Task stopped on request", zap.String("task_id", task.ID),
			zap.Strings("not_run", result.NotRun), zap.Duration("duration", task.Duration))
	default:
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

	tm.publishLocked(task)
}
