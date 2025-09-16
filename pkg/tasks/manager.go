package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TaskManagerImpl 任务管理器实现
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

// NewTaskManager 创建新的任务管理器
func NewTaskManager(ctx context.Context, cfg *config.Config) (*TaskManagerImpl, error) {
	slog.Info("Initializing task manager")

	// 创建ClickHouse客户端
	chClient, err := clickhouse.NewClient(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	// 测试连接
	if err := chClient.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	// 初始化通知执行器
	notificationExecutor, err := NewNotificationTaskExecutor(chClient, cfg)
	if err != nil {
		slog.Warn("Failed to initialize notification executor", "error", err)
		// 不返回错误，继续初始化任务管理器，只是通知功能不可用
	}

	// 创建执行器工厂
	executorFactory := NewExecutorFactory(cfg, chClient)

	tm := &TaskManagerImpl{
		config:               cfg,
		ctx:                  ctx,
		tasks:                make(map[string]*Task),
		taskHistory:          make([]*Task, 0),
		chClient:             chClient,
		notificationExecutor: notificationExecutor,
		executorFactory:      executorFactory,
		maxTasks:             10, // 最大并发任务数
	}

	slog.Info("Task manager initialized")
	return tm, nil
}

// ExecuteTask 执行任务
func (tm *TaskManagerImpl) ExecuteTask(ctx context.Context, req *TaskRequest) (*TaskResult, error) {
	// 生成ID（如果未提供）
	if req.ID == "" {
		req.ID = uuid.New().String()
	}

	// 检查是否有太多任务正在运行
	if err := tm.checkTaskLimit(); err != nil {
		return nil, err
	}

	// 创建任务
	task := tm.createTask(req)

	// 添加到任务列表
	tm.addTask(task)

	// 异步执行任务
	go tm.executeTaskInternal(ctx, task)

	// 返回初始结果
	return &TaskResult{
		ID:        task.ID,
		Type:      string(task.Type),
		Status:    string(task.Status),
		StartedAt: task.StartTime,
		Success:   true,
		Message:   "Task started successfully",
	}, nil
}

// GetTask 获取特定任务
func (tm *TaskManagerImpl) GetTask(taskID string) (*Task, error) {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	// 检查运行中的任务
	if task, exists := tm.tasks[taskID]; exists {
		return task, nil
	}

	// 检查历史任务
	for _, task := range tm.taskHistory {
		if task.ID == taskID {
			return task, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
}

// GetTasks 获取所有活动任务
func (tm *TaskManagerImpl) GetTasks() []*Task {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	tasks := make([]*Task, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		tasks = append(tasks, task)
	}
	return tasks
}

// GetTaskHistory 获取任务历史
func (tm *TaskManagerImpl) GetTaskHistory() []*Task {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	// 创建副本以避免并发修改
	history := make([]*Task, len(tm.taskHistory))
	copy(history, tm.taskHistory)
	return history
}

// CancelTask 取消任务
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

	// 调用取消函数
	if task.Cancel != nil {
		task.Cancel()
	}

	// 更新任务状态
	task.Status = TaskStatusCancelled
	task.EndTime = time.Now()
	task.Duration = task.EndTime.Sub(task.StartTime)
	task.Error = "Task was cancelled"

	slog.Info("Task cancelled", "task_id", taskID)
	return nil
}

// GetRunningTaskCount 获取运行中的任务数量
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

// GetTotalTaskCount 获取总任务数量
func (tm *TaskManagerImpl) GetTotalTaskCount() int {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()
	return len(tm.tasks)
}

// 私有方法

// checkTaskLimit 检查任务限制
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

// createTask 创建任务
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

// addTask 添加任务到列表
func (tm *TaskManagerImpl) addTask(task *Task) {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()
	tm.tasks[task.ID] = task
}

// executeTaskInternal 内部任务执行
func (tm *TaskManagerImpl) executeTaskInternal(ctx context.Context, task *Task) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Task execution panicked", "task_id", task.ID, "panic", r)
			tm.finishTask(task, nil, fmt.Errorf("task panicked: %v", r))
		}
	}()

	// 更新任务状态为运行中
	tm.updateTaskStatus(task, TaskStatusRunning)

	slog.Info("Starting task execution", "task_id", task.ID, "type", task.Type, "provider", task.Provider)

	var result *TaskResult
	var err error

	// 根据任务类型执行
	switch task.Type {
	case TaskTypeSync:
		result, err = tm.executeSyncTask(ctx, task)
	case TaskTypeNotification:
		result, err = tm.executeNotificationTask(ctx, task)
	default:
		err = fmt.Errorf("unsupported task type: %s", task.Type)
	}

	// 完成任务
	tm.finishTask(task, result, err)
}

// executeSyncTask 执行同步任务
func (tm *TaskManagerImpl) executeSyncTask(ctx context.Context, task *Task) (*TaskResult, error) {
	// 创建同步执行器
	executor, err := tm.executorFactory.CreateExecutor(ctx, task.Provider)
	if err != nil {
		return nil, fmt.Errorf("failed to create executor for provider %s: %w", task.Provider, err)
	}

	// 转换任务配置为同步配置
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
	}

	// 如果需要创建表，先创建表
	if task.Config.CreateTable {
		tableConfig := tm.createTableConfig(task)
		if err := executor.CreateTables(ctx, tableConfig); err != nil {
			return nil, fmt.Errorf("failed to create tables: %w", err)
		}
	}

	// 执行同步
	syncResult, err := executor.ExecuteSync(ctx, syncConfig)
	if err != nil {
		return nil, fmt.Errorf("sync execution failed: %w", err)
	}

	// 转换结果
	return tm.convertSyncResult(syncResult, task), nil
}

// executeNotificationTask 执行通知任务
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

// createTableConfig 创建表配置
func (tm *TaskManagerImpl) createTableConfig(task *Task) *TableConfig {
	useDistributed := task.Config.UseDistributed || tm.config.ClickHouse.Cluster != ""

	var localTableName, distributedTableName string
	switch task.Provider {
	case "volcengine":
		localTableName = "volcengine_bill_details_local"
		distributedTableName = "volcengine_bill_details_distributed"
	case "alicloud":
		// 阿里云的表名由billService管理，这里使用通用名称
		localTableName = "alicloud_bill_details_local"
		distributedTableName = "alicloud_bill_details_distributed"
	default:
		localTableName = fmt.Sprintf("%s_bill_details_local", task.Provider)
		distributedTableName = fmt.Sprintf("%s_bill_details_distributed", task.Provider)
	}

	return &TableConfig{
		UseDistributed:       useDistributed,
		LocalTableName:       localTableName,
		DistributedTableName: distributedTableName,
		ClusterName:          tm.config.ClickHouse.Cluster,
	}
}

// convertSyncResult 转换同步结果
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

// updateTaskStatus 更新任务状态
func (tm *TaskManagerImpl) updateTaskStatus(task *Task, status TaskStatus) {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()
	task.Status = status
}

// finishTask 完成任务
func (tm *TaskManagerImpl) finishTask(task *Task, result *TaskResult, err error) {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	// 设置结束时间和持续时间
	task.EndTime = time.Now()
	task.Duration = task.EndTime.Sub(task.StartTime)

	// 设置任务状态和结果
	if err != nil {
		task.Status = TaskStatusFailed
		task.Error = err.Error()
		slog.Error("Task execution failed", "task_id", task.ID, "error", err)
	} else {
		task.Status = TaskStatusCompleted
		task.Result = result
		slog.Info("Task execution completed", "task_id", task.ID, "duration", task.Duration)
	}

	// 从活动任务中移除并添加到历史
	delete(tm.tasks, task.ID)
	tm.taskHistory = append(tm.taskHistory, task)

	// 限制历史记录数量
	if len(tm.taskHistory) > 100 {
		tm.taskHistory = tm.taskHistory[1:]
	}
}