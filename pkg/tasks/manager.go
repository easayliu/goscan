package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/alicloud"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/volcengine"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TaskType represents the type of task
type TaskType string

const (
	TaskTypeSync         TaskType = "sync"
	TaskTypeNotification TaskType = "notification"
)

// TaskStatus represents the status of a task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TaskRequest represents a request to execute a task
type TaskRequest struct {
	ID       string     `json:"id"`
	Type     TaskType   `json:"type"`
	Provider string     `json:"provider"`
	Config   TaskConfig `json:"config"`
}

// TaskConfig holds task-specific configuration
type TaskConfig struct {
	SyncMode       string `json:"sync_mode"`
	UseDistributed bool   `json:"use_distributed"`
	CreateTable    bool   `json:"create_table"`
	ForceUpdate    bool   `json:"force_update"`
	Granularity    string `json:"granularity,omitempty"` // For AliCloud
	BillPeriod     string `json:"bill_period,omitempty"`
	StartPeriod    string `json:"start_period,omitempty"`
	EndPeriod      string `json:"end_period,omitempty"`
	Limit          int    `json:"limit,omitempty"`
}

// Task represents a running or completed task
type Task struct {
	ID        string             `json:"id"`
	Type      TaskType           `json:"type"`
	Provider  string             `json:"provider"`
	Status    TaskStatus         `json:"status"`
	StartTime time.Time          `json:"start_time"`
	EndTime   time.Time          `json:"end_time"`
	Duration  time.Duration      `json:"duration"`
	Config    TaskConfig         `json:"config"`
	Result    *TaskResult        `json:"result,omitempty"`
	Error     string             `json:"error,omitempty"`
	Cancel    context.CancelFunc `json:"-"`
}

// TaskResult holds the result of a completed task
type TaskResult struct {
	ID               string        `json:"id"`
	Type             string        `json:"type"`
	Status           string        `json:"status"`
	RecordsProcessed int           `json:"records_processed"`
	RecordsFetched   int           `json:"records_fetched"`
	Duration         time.Duration `json:"duration"`
	Success          bool          `json:"success"`
	Message          string        `json:"message"`
	Error            string        `json:"error,omitempty"`
	StartedAt        time.Time     `json:"started_at"`
	CompletedAt      time.Time     `json:"completed_at"`
}

// TaskManager manages task execution
type TaskManager struct {
	config               *config.Config
	ctx                  context.Context
	tasks                map[string]*Task
	tasksMutex           sync.RWMutex
	taskHistory          []*Task
	chClient             *clickhouse.Client
	notificationExecutor *NotificationTaskExecutor
	maxTasks             int
}

// NewTaskManager creates a new task manager
func NewTaskManager(ctx context.Context, cfg *config.Config) (*TaskManager, error) {
	slog.Info("Initializing task manager")

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
		slog.Warn("Failed to initialize notification executor", "error", err)
		// 不返回错误，继续初始化任务管理器，只是通知功能不可用
	}

	tm := &TaskManager{
		config:               cfg,
		ctx:                  ctx,
		tasks:                make(map[string]*Task),
		taskHistory:          make([]*Task, 0),
		chClient:             chClient,
		notificationExecutor: notificationExecutor,
		maxTasks:             10, // Maximum concurrent tasks
	}

	slog.Info("Task manager initialized")
	return tm, nil
}

// ExecuteTask executes a task asynchronously
func (tm *TaskManager) ExecuteTask(ctx context.Context, req *TaskRequest) (*TaskResult, error) {
	// Generate ID if not provided
	if req.ID == "" {
		req.ID = uuid.New().String()
	}

	// Check if too many tasks are running
	tm.tasksMutex.RLock()
	runningCount := 0
	for _, task := range tm.tasks {
		if task.Status == TaskStatusRunning {
			runningCount++
		}
	}
	tm.tasksMutex.RUnlock()

	if runningCount >= tm.maxTasks {
		return nil, fmt.Errorf("too many tasks running (%d/%d)", runningCount, tm.maxTasks)
	}

	// Create task context with cancellation
	taskCtx, cancel := context.WithCancel(ctx)

	// Create task
	task := &Task{
		ID:        req.ID,
		Type:      req.Type,
		Provider:  req.Provider,
		Status:    TaskStatusPending,
		StartTime: time.Now(),
		Config:    req.Config,
		Cancel:    cancel,
	}

	// Store task
	tm.tasksMutex.Lock()
	tm.tasks[task.ID] = task
	tm.tasksMutex.Unlock()

	slog.Info("Task created", "task_id", task.ID, "type", task.Type, "provider", task.Provider)

	// Execute task in goroutine
	go tm.executeTaskInternal(taskCtx, task)

	return nil, nil
}

// executeTaskInternal executes the actual task logic
func (tm *TaskManager) executeTaskInternal(ctx context.Context, task *Task) {
	// Update task status
	tm.tasksMutex.Lock()
	task.Status = TaskStatusRunning
	tm.tasksMutex.Unlock()

	slog.Info("Starting task execution", "task_id", task.ID, "provider", task.Provider)

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

	// Update task with results
	tm.tasksMutex.Lock()
	task.EndTime = time.Now()
	task.Duration = task.EndTime.Sub(task.StartTime)

	if err != nil {
		task.Status = TaskStatusFailed
		task.Error = err.Error()
		slog.Error("Task failed", "task_id", task.ID, "error", err, "duration", task.Duration)
	} else {
		task.Status = TaskStatusCompleted
		task.Result = result
		slog.Info("Task completed successfully", "task_id", task.ID, "duration", task.Duration)
	}

	// Move to history
	tm.taskHistory = append(tm.taskHistory, task)
	delete(tm.tasks, task.ID)

	// Keep only last 100 tasks in history
	if len(tm.taskHistory) > 100 {
		tm.taskHistory = tm.taskHistory[1:]
	}

	tm.tasksMutex.Unlock()
}

// executeSyncTask executes a sync task
func (tm *TaskManager) executeSyncTask(ctx context.Context, task *Task) (*TaskResult, error) {
	switch task.Provider {
	case "volcengine":
		return tm.executeVolcEngineSyncTask(ctx, task)
	case "alicloud":
		return tm.executeAliCloudSyncTask(ctx, task)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", task.Provider)
	}
}

// executeNotificationTask executes a notification task
func (tm *TaskManager) executeNotificationTask(ctx context.Context, task *Task) (*TaskResult, error) {
	if tm.notificationExecutor == nil {
		return nil, fmt.Errorf("notification executor not initialized")
	}

	if !tm.notificationExecutor.IsEnabled() {
		return &TaskResult{
			ID:               task.ID,
			Type:             string(TaskTypeNotification),
			Status:           "skipped",
			RecordsProcessed: 0,
			RecordsFetched:   0,
			Duration:         0,
			Success:          true,
			Message:          "微信通知未启用，跳过任务",
			StartedAt:        time.Now(),
			CompletedAt:      time.Now(),
		}, nil
	}

	// 执行费用报告通知任务
	result, err := tm.notificationExecutor.ExecuteCostReport(ctx)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// executeVolcEngineSyncTask executes a VolcEngine sync task
func (tm *TaskManager) executeVolcEngineSyncTask(ctx context.Context, task *Task) (*TaskResult, error) {
	volcConfig := tm.config.GetVolcEngineConfig()
	if volcConfig.AccessKey == "" || volcConfig.SecretKey == "" {
		return nil, fmt.Errorf("VolcEngine credentials not configured")
	}

	// Create bill service
	billService, err := volcengine.NewBillService(volcConfig, tm.chClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create VolcEngine bill service: %w", err)
	}

	// Create table if requested
	if task.Config.CreateTable {
		if task.Config.UseDistributed && tm.config.ClickHouse.Cluster != "" {
			localTableName := "volcengine_bill_details_local"
			distributedTableName := "volcengine_bill_details_distributed"
			if err := billService.CreateDistributedBillTable(ctx, localTableName, distributedTableName); err != nil {
				return nil, fmt.Errorf("failed to create distributed bill table: %w", err)
			}
		} else {
			if err := billService.CreateBillTable(ctx); err != nil {
				return nil, fmt.Errorf("failed to create bill table: %w", err)
			}
		}
	}

	// Determine sync mode and execute
	var syncResult *volcengine.SyncResult

	switch task.Config.SyncMode {
	case "sync-optimal":
		// Use optimal sync mode with intelligent time selection
		tableName := "volcengine_bill_details"
		if task.Config.UseDistributed && tm.config.ClickHouse.Cluster != "" {
			tableName = "volcengine_bill_details_distributed"
		}

		period := task.Config.BillPeriod
		if period == "" {
			// 🎯 智能时间选择：同步上个月的数据（火山云主要按月统计）
			smartSelection := getSmartTimeSelection("monthly")
			period = smartSelection.LastMonthPeriod
			slog.Info("智能选择同步时间", "模式", "sync-optimal", "提供商", "volcengine",
				"账期", period, "说明", "同步上个月数据")
		}

		syncResult, err = billService.SyncAllBillDataBestPractice(ctx, period, tableName,
			task.Config.UseDistributed && tm.config.ClickHouse.Cluster != "")
	default:
		// Standard sync mode
		req := &volcengine.ListBillDetailRequest{
			BillPeriod: task.Config.BillPeriod,
			Limit:      int32(task.Config.Limit),
			Offset:     0,
		}

		if task.Config.UseDistributed && tm.config.ClickHouse.Cluster != "" {
			distributedTableName := "volcengine_bill_details_distributed"
			syncResult, err = billService.SyncBillDataToDistributed(ctx, distributedTableName, req)
		} else {
			syncResult, err = billService.SyncBillData(ctx, req)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("sync failed: %w", err)
	}

	return &TaskResult{
		RecordsProcessed: syncResult.InsertedRecords,
		RecordsFetched:   syncResult.FetchedRecords,
		Duration:         syncResult.Duration,
		Success:          true,
		Message:          fmt.Sprintf("VolcEngine sync completed: %d records processed", syncResult.InsertedRecords),
	}, nil
}

// executeAliCloudSyncTask executes an AliCloud sync task
func (tm *TaskManager) executeAliCloudSyncTask(ctx context.Context, task *Task) (*TaskResult, error) {
	aliConfig := tm.config.GetAliCloudConfig()
	if aliConfig.AccessKeyID == "" || aliConfig.AccessKeySecret == "" {
		return nil, fmt.Errorf("AliCloud credentials not configured")
	}

	// Create bill service
	billService, err := alicloud.NewBillService(aliConfig, tm.chClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create AliCloud bill service: %w", err)
	}
	defer billService.Close()

	// Create tables if requested
	if task.Config.CreateTable {
		useDistributed := task.Config.UseDistributed || tm.config.ClickHouse.Cluster != ""

		if useDistributed && tm.config.ClickHouse.Cluster != "" {
			monthlyLocal := billService.GetMonthlyTableName() + "_local"
			monthlyDistributed := billService.GetMonthlyTableName() + "_distributed"
			dailyLocal := billService.GetDailyTableName() + "_local"
			dailyDistributed := billService.GetDailyTableName() + "_distributed"

			if err := billService.CreateDistributedMonthlyBillTable(ctx, monthlyLocal, monthlyDistributed); err != nil {
				return nil, fmt.Errorf("failed to create monthly distributed table: %w", err)
			}
			if err := billService.CreateDistributedDailyBillTable(ctx, dailyLocal, dailyDistributed); err != nil {
				return nil, fmt.Errorf("failed to create daily distributed table: %w", err)
			}

			billService.SetDistributedTableNames(monthlyDistributed, dailyDistributed)
		} else {
			if err := billService.CreateMonthlyBillTable(ctx); err != nil {
				return nil, fmt.Errorf("failed to create monthly table: %w", err)
			}
			if err := billService.CreateDailyBillTable(ctx); err != nil {
				return nil, fmt.Errorf("failed to create daily table: %w", err)
			}
		}
	}

	// Determine sync options
	syncOptions := &alicloud.SyncOptions{
		BatchSize:        task.Config.Limit,
		UseDistributed:   task.Config.UseDistributed || tm.config.ClickHouse.Cluster != "",
		EnableValidation: true,
		MaxWorkers:       4,
	}

	// Execute sync based on mode
	period := task.Config.BillPeriod
	granularity := task.Config.Granularity
	if granularity == "" {
		granularity = "monthly"
	}

	// 🎯 智能时间选择：sync-optimal 模式的特殊处理
	if task.Config.SyncMode == "sync-optimal" && period == "" {
		smartSelection := getSmartTimeSelection(granularity)

		switch granularity {
		case "daily":
			// 昨天的数据 (2006-01-02 格式)
			period = smartSelection.YesterdayPeriod
			slog.Info("智能选择同步时间", "模式", "sync-optimal", "提供商", "alicloud",
				"粒度", "daily", "账期", period, "说明", "同步昨天数据")
		case "monthly":
			// 上个月的数据 (2006-01 格式)
			period = smartSelection.LastMonthPeriod
			slog.Info("智能选择同步时间", "模式", "sync-optimal", "提供商", "alicloud",
				"粒度", "monthly", "账期", period, "说明", "同步上个月数据")
		case "both":
			// 🎯 智能对比模式：分别同步昨天和上个月的数据
			slog.Info("智能对比同步开始", "模式", "sync-optimal", "提供商", "alicloud",
				"昨天", smartSelection.YesterdayPeriod, "上个月", smartSelection.LastMonthPeriod)

			// 📊 执行同步前数据预检查
			preCheckPeriod := fmt.Sprintf("yesterday:%s,last_month:%s",
				smartSelection.YesterdayPeriod, smartSelection.LastMonthPeriod)
			preCheckResult, err := billService.PerformPreSyncCheck(ctx, "both", preCheckPeriod)
			if err != nil {
				slog.Warn("数据预检查失败，继续执行同步", "error", err)
			} else {
				slog.Info("数据预检查完成", "结果", preCheckResult.Summary)

				// 如果预检查建议跳过同步，则跳过
				if preCheckResult.ShouldSkip {
					slog.Info("智能预检查：数据已是最新，跳过同步", "昨天", smartSelection.YesterdayPeriod,
						"上个月", smartSelection.LastMonthPeriod)

					// 记录预检查的详细信息
					for _, result := range preCheckResult.Results {
						slog.Info("预检查详情", "时间段", result.Reason, "API数量", result.APICount,
							"数据库数量", result.DatabaseCount, "需要同步", result.NeedSync)
					}

					// 返回成功结果，表示跳过了同步
					return &TaskResult{
						RecordsProcessed: 0,
						RecordsFetched:   0,
						Duration:         time.Since(time.Now()),
						Success:          true,
						Message:          fmt.Sprintf("智能预检查：数据已最新，跳过同步 - %s", preCheckResult.Summary),
					}, nil
				} else {
					// 🧹 智能清理和同步：根据预检查结果执行必要的清理和同步
					slog.Info("智能预检查：检测到数据差异，执行智能清理和同步")

					totalProcessed := 0
					for _, result := range preCheckResult.Results {
						if result.NeedSync {
							slog.Info("执行智能同步", "时间段", result.Period, "粒度", result.Granularity,
								"需要清理", result.NeedCleanup, "原因", result.Reason)

							// 执行智能清理和同步
							if err := billService.ExecuteIntelligentCleanupAndSync(ctx, result, syncOptions); err != nil {
								return nil, fmt.Errorf("智能同步失败 (%s %s): %w",
									result.Granularity, result.Period, err)
							}

							// 这里应该从同步结果中获取实际处理的记录数，暂时用API数量
							totalProcessed += int(result.APICount)
						}
					}

					slog.Info("智能清理和同步完成", "总处理记录数", totalProcessed)

					// 返回智能同步的结果
					return &TaskResult{
						RecordsProcessed: totalProcessed,
						RecordsFetched:   totalProcessed,
						Duration:         time.Since(time.Now()),
						Success:          true,
						Message:          fmt.Sprintf("智能同步完成：%s", preCheckResult.Summary),
					}, nil
				}
			}

			// 📋 预检查失败时的fallback逻辑：执行常规同步
			slog.Warn("数据预检查失败，执行常规同步作为fallback")

			// 先同步上个月的按月数据
			if err = billService.SyncMonthlyBillData(ctx, smartSelection.LastMonthPeriod, syncOptions); err != nil {
				return nil, fmt.Errorf("fallback同步-上月数据同步失败: %w", err)
			}
			slog.Info("fallback同步-上月数据同步完成", "账期", smartSelection.LastMonthPeriod)

			// 再同步昨天的按天数据
			if err = billService.SyncDailyBillData(ctx, smartSelection.YesterdayPeriod, syncOptions); err != nil {
				return nil, fmt.Errorf("fallback同步-昨天数据同步失败: %w", err)
			}
			slog.Info("fallback同步-昨天数据同步完成", "账期", smartSelection.YesterdayPeriod)

			slog.Info("fallback同步完成", "上月账期", smartSelection.LastMonthPeriod,
				"昨天账期", smartSelection.YesterdayPeriod, "说明", "已同步昨天和上个月数据")

			// 返回fallback同步的结果
			return &TaskResult{
				RecordsProcessed: 0, // 这里需要从实际同步结果获取
				RecordsFetched:   0,
				Duration:         time.Since(time.Now()),
				Success:          true,
				Message: fmt.Sprintf("fallback同步完成: 昨天(%s) + 上个月(%s)",
					smartSelection.YesterdayPeriod, smartSelection.LastMonthPeriod),
			}, nil
		}
	} else if period == "" {
		// 非 sync-optimal 模式使用当前月
		period = time.Now().Format("2006-01")
	}

	var recordsProcessed int
	switch granularity {
	case "monthly":
		err = billService.SyncMonthlyBillData(ctx, period, syncOptions)
	case "daily":
		err = billService.SyncDailyBillData(ctx, period, syncOptions)
	case "both":
		err = billService.SyncBothGranularityData(ctx, period, syncOptions)
	default:
		err = fmt.Errorf("unsupported granularity: %s", granularity)
	}

	if err != nil {
		return nil, fmt.Errorf("AliCloud sync failed: %w", err)
	}

	return &TaskResult{
		RecordsProcessed: recordsProcessed,
		RecordsFetched:   recordsProcessed,
		Duration:         time.Since(time.Now()), // This would be calculated properly
		Success:          true,
		Message:          fmt.Sprintf("AliCloud sync completed: %d records processed", recordsProcessed),
	}, nil
}

// GetTask returns a specific task
func (tm *TaskManager) GetTask(taskID string) (*Task, error) {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	// Check running tasks
	if task, exists := tm.tasks[taskID]; exists {
		return task, nil
	}

	// Check history
	for _, task := range tm.taskHistory {
		if task.ID == taskID {
			return task, nil
		}
	}

	return nil, fmt.Errorf("task not found")
}

// GetTasks returns all active tasks
func (tm *TaskManager) GetTasks() []*Task {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	tasks := make([]*Task, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		tasks = append(tasks, task)
	}

	return tasks
}

// GetTaskHistory returns task history
func (tm *TaskManager) GetTaskHistory() []*Task {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	return tm.taskHistory
}

// CancelTask cancels a running task
func (tm *TaskManager) CancelTask(taskID string) error {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	task, exists := tm.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found")
	}

	if task.Status != TaskStatusRunning {
		return fmt.Errorf("task is not running")
	}

	// Cancel the task
	task.Cancel()
	task.Status = TaskStatusCancelled
	task.EndTime = time.Now()
	task.Duration = task.EndTime.Sub(task.StartTime)

	// Move to history
	tm.taskHistory = append(tm.taskHistory, task)
	delete(tm.tasks, taskID)

	slog.Info("Task cancelled", "task_id", taskID)
	return nil
}

// GetRunningTaskCount returns the number of running tasks
func (tm *TaskManager) GetRunningTaskCount() int {
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

// GetTotalTaskCount returns the total number of tasks (running + history)
func (tm *TaskManager) GetTotalTaskCount() int {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	return len(tm.tasks) + len(tm.taskHistory)
}

// SmartTimeSelection represents smart time selection for sync-optimal mode
type SmartTimeSelection struct {
	YesterdayPeriod string // Format: 2006-01-02 for daily
	LastMonthPeriod string // Format: 2006-01 for monthly
}

// DataComparisonResult represents the result of data comparison between API and database
type DataComparisonResult struct {
	Period      string // The period being compared
	Granularity string // monthly or daily
	APICount    int64  // Total records from API
	DBCount     int64  // Total records in database
	NeedSync    bool   // Whether sync is needed
	Message     string // Comparison message
}

// PreSyncCheckResult represents the result of pre-sync check
type PreSyncCheckResult struct {
	MonthlyCheck *DataComparisonResult
	DailyCheck   *DataComparisonResult
	ShouldSync   bool   // Whether any sync is needed
	Message      string // Overall message
}

// getSmartTimeSelection implements intelligent time selection for sync-optimal mode
// Returns yesterday and last month periods based on granularity
func getSmartTimeSelection(granularity string) *SmartTimeSelection {
	now := time.Now()

	// Calculate yesterday for daily data
	yesterday := now.AddDate(0, 0, -1)
	yesterdayPeriod := yesterday.Format("2006-01-02")

	// Calculate last month for monthly data
	lastMonth := now.AddDate(0, -1, 0)
	lastMonthPeriod := lastMonth.Format("2006-01")

	return &SmartTimeSelection{
		YesterdayPeriod: yesterdayPeriod,
		LastMonthPeriod: lastMonthPeriod,
	}
}
