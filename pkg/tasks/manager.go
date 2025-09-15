package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/alicloud"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/volcengine"
	"log"
	"log/slog"
	"strings"
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
		// 参考阿里云实现：只要配置了集群名称，就使用分布式表
		useDistributed := task.Config.UseDistributed || tm.config.ClickHouse.Cluster != ""
		
		if useDistributed && tm.config.ClickHouse.Cluster != "" {
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
		// 🎯 sync-optimal 模式：智能同步最近3个月的数据（包括当月）
		// 参考阿里云实现：只要配置了集群名称，就使用分布式表
		useDistributed := task.Config.UseDistributed || tm.config.ClickHouse.Cluster != ""
		
		tableName := "volcengine_bill_details"
		if useDistributed && tm.config.ClickHouse.Cluster != "" {
			tableName = "volcengine_bill_details_distributed"
		}
		
		// 获取最近3个月的月份列表（包括当月）
		recentMonths := getRecentMonths(3)
		slog.Info("sync-optimal模式：准备同步最近3个月数据", 
			"提供商", "volcengine",
			"月份列表", recentMonths,
			"表名", tableName)
		
		// 如果启用了强制更新，先清理最近3个月的旧数据（只清理，不同步）
		if task.Config.ForceUpdate {
			slog.Info("强制更新模式：清理最近3个月数据", "月份", recentMonths, "表名", tableName)
			// 火山引擎表按ExpenseDate分区，需要逐月清理以支持分区删除
			resolver := tm.chClient.GetTableNameResolver()
			actualTableName := resolver.ResolveQueryTarget(tableName)
			
			for _, month := range recentMonths {
				yearMonth := strings.Replace(month, "-", "", 1) // 2025-09 -> 202509
				cleanCondition := fmt.Sprintf("toYYYYMM(toDate(ExpenseDate)) = %s", yearMonth)
				slog.Info("强制更新：清理单月数据", "月份", month, "条件", cleanCondition)
				
				// 使用增强版清理（支持分区删除）- 只清理，不同步
				cleanupOpts := &clickhouse.CleanupOptions{
					Condition:   cleanCondition,
					Args:        nil,
					DryRun:      false,
					ProgressLog: true,
				}
				
				if _, cleanErr := tm.chClient.EnhancedCleanTableData(ctx, actualTableName, cleanupOpts); cleanErr != nil {
					slog.Warn("清理月份数据失败", "月份", month, "error", cleanErr)
				} else {
					slog.Info("月份数据清理完成", "月份", month)
				}
			}
		}
		
		// 逐月同步数据
		var totalRecords, totalInserted int
		var totalDuration time.Duration
		var lastErr error
		
		for _, month := range recentMonths {
			slog.Info("开始同步月份数据", "月份", month, "表名", tableName)
			
			var monthResult *volcengine.SyncResult
			if task.Config.ForceUpdate {
				// 强制更新模式下，数据已经被清理，直接同步（跳过内部预检查）
				monthResult, err = billService.SyncAllBillDataBestPracticeWithoutPreCheck(ctx, month, tableName,
					useDistributed)
			} else {
				// 非强制更新模式，先进行数据对比检查
				slog.Info("sync-optimal模式数据预检查", "月份", month, "提供商", "volcengine")
				
				// 执行数据对比
				comparisonResult, compErr := billService.PerformDataComparison(ctx, "monthly", month)
				if compErr != nil {
					slog.Warn("数据预检查失败，继续执行同步", "月份", month, "error", compErr)
					// 对比失败时，使用原有逻辑：清理后同步
					yearMonth := strings.Replace(month, "-", "", 1) // 2025-09 -> 202509
					cleanCondition := fmt.Sprintf("toYYYYMM(toDate(ExpenseDate)) = %s", yearMonth)
					monthResult, err = billService.SyncAllBillDataBestPracticeWithCleanup(ctx, month, tableName,
						useDistributed,
						cleanCondition, nil) // 不需要参数，条件已经格式化好了
				} else {
					slog.Info("数据预检查结果", 
						"月份", month,
						"API数量", comparisonResult.APICount,
						"数据库数量", comparisonResult.DatabaseCount,
						"需要同步", comparisonResult.NeedSync,
						"需要清理", comparisonResult.NeedCleanup)
					
					if !comparisonResult.NeedSync {
						// 数据已一致，跳过同步
						slog.Info("sync-optimal检查：月份数据已一致，跳过同步", 
							"月份", month, "原因", comparisonResult.Reason)
						// 创建一个空的结果表示跳过
						monthResult = &volcengine.SyncResult{
							TotalRecords:    0,
							InsertedRecords: 0,
							FetchedRecords:  0,
							Duration:        0,
						}
					} else {
						// 需要同步
						if comparisonResult.NeedCleanup {
							// 需要清理旧数据，清理后使用优化同步
							slog.Info("执行数据清理", "月份", month)
							yearMonth := strings.Replace(month, "-", "", 1) // 2025-09 -> 202509
							cleanCondition := fmt.Sprintf("toYYYYMM(toDate(ExpenseDate)) = %s", yearMonth)
							
							// 使用增强版清理（支持分区删除）
							resolver := tm.chClient.GetTableNameResolver()
							actualTableName := resolver.ResolveQueryTarget(tableName)
							cleanupOpts := &clickhouse.CleanupOptions{
								Condition:   cleanCondition,
								Args:        nil,
								DryRun:      false,
								ProgressLog: true,
							}
							
							_, err := tm.chClient.EnhancedCleanTableData(ctx, actualTableName, cleanupOpts)
							if err != nil {
								slog.Error("数据清理失败", "月份", month, "error", err)
								continue
							}
							slog.Info("数据清理完成", "月份", month)
							
							// 使用优化同步方法
							monthResult, err = billService.SyncAllBillDataWithFirstPage(ctx, month, tableName,
								useDistributed, comparisonResult)
						} else {
							// 不需要清理，使用优化同步方法
							monthResult, err = billService.SyncAllBillDataWithFirstPage(ctx, month, tableName,
								useDistributed, comparisonResult)
						}
					}
				}
			}
			
			if err != nil {
				slog.Error("同步月份数据失败", "月份", month, "error", err)
				lastErr = err
				continue
			}
			
			if monthResult != nil {
				totalRecords += monthResult.TotalRecords
				totalInserted += monthResult.InsertedRecords
				totalDuration += monthResult.Duration
				slog.Info("月份数据同步完成", 
					"月份", month,
					"总记录", monthResult.TotalRecords,
					"已插入", monthResult.InsertedRecords,
					"耗时", monthResult.Duration)
			}
		}
		
		// 构建汇总结果
		syncResult = &volcengine.SyncResult{
			TotalRecords:    totalRecords,
			InsertedRecords: totalInserted,
			FetchedRecords:  totalRecords,
			Duration:        totalDuration,
		}
		
		if lastErr != nil && totalInserted == 0 {
			// 如果所有月份都失败了，返回错误
			err = lastErr
		} else {
			// 至少有部分成功
			slog.Info("sync-optimal同步完成", 
				"总记录数", totalRecords,
				"已插入", totalInserted,
				"总耗时", totalDuration,
				"月份数", len(recentMonths))
		}
	default:
		// Standard sync mode
		// 参考阿里云实现：只要配置了集群名称，就使用分布式表
		useDistributed := task.Config.UseDistributed || tm.config.ClickHouse.Cluster != ""
		
		// 添加调试信息
		log.Printf("🔍 [Manager] 从TaskConfig读取到的BillPeriod: '%s' (长度: %d)", task.Config.BillPeriod, len(task.Config.BillPeriod))
		
		req := &volcengine.ListBillDetailRequest{
			BillPeriod: task.Config.BillPeriod,
			Limit:      int32(task.Config.Limit),
			Offset:     0,
		}

		// 检查是否需要强制更新（清理数据）
		if task.Config.ForceUpdate {
			log.Printf("🔄 [Manager] ForceUpdate模式：将清理账期 %s 的旧数据", req.BillPeriod)
			tableName := "volcengine_bill_details"
			if useDistributed && tm.config.ClickHouse.Cluster != "" {
				tableName = "volcengine_bill_details_distributed"
			}
			
			// 构建清理条件：使用分区删除方式（基于ExpenseDate分区）
			var cleanCondition string
			var cleanArgs []interface{}
			if req.BillPeriod != "" {
				// 火山引擎表按ExpenseDate分区：PARTITION BY toYYYYMM(toDate(ExpenseDate))
				// 使用分区函数条件来触发分区删除
				yearMonth := strings.Replace(req.BillPeriod, "-", "", 1) // 2025-09 -> 202509
				cleanCondition = fmt.Sprintf("toYYYYMM(toDate(ExpenseDate)) = %s", yearMonth)
				cleanArgs = nil // 分区删除不需要参数
			} else {
				// 如果没有BillPeriod，清理当前月数据
				currentMonth := time.Now().Format("200601") // 202509格式
				cleanCondition = fmt.Sprintf("toYYYYMM(toDate(ExpenseDate)) = %s", currentMonth)
				cleanArgs = nil
			}
			
			log.Printf("🧹 [Manager] 分区删除条件: %s（将触发DROP PARTITION）", cleanCondition)
			
			// 使用带清理功能的同步方法
			if useDistributed && tm.config.ClickHouse.Cluster != "" {
				syncResult, err = billService.SyncBillDataToDistributedWithCleanup(ctx, tableName, req, cleanCondition, cleanArgs...)
			} else {
				syncResult, err = billService.SyncBillDataWithCleanup(ctx, req, cleanCondition, cleanArgs...)
			}
		} else {
			// 标准同步，先进行数据对比检查
			if req.BillPeriod != "" {
				slog.Info("标准模式数据预检查", "账期", req.BillPeriod, "提供商", "volcengine")
				
				// 执行数据对比
				comparisonResult, compErr := billService.PerformDataComparison(ctx, "monthly", req.BillPeriod)
				if compErr != nil {
					slog.Warn("数据预检查失败，继续执行同步", "账期", req.BillPeriod, "error", compErr)
					// 对比失败时，继续执行原有逻辑
				} else {
					slog.Info("数据预检查结果", 
						"账期", req.BillPeriod,
						"API数量", comparisonResult.APICount,
						"数据库数量", comparisonResult.DatabaseCount,
						"需要同步", comparisonResult.NeedSync,
						"需要清理", comparisonResult.NeedCleanup)
					
					if !comparisonResult.NeedSync {
						// 数据已一致，跳过同步
						syncResult = &volcengine.SyncResult{
							TotalRecords:    0,
							InsertedRecords: 0,
							FetchedRecords:  0,
							Duration:        0,
						}
						goto skipSync // 跳过下面的同步逻辑
					}
					
					if comparisonResult.NeedCleanup {
						// 需要清理旧数据
						slog.Info("执行数据清理", "账期", req.BillPeriod)
						
						yearMonth := strings.Replace(req.BillPeriod, "-", "", 1) // 2025-09 -> 202509
						cleanCondition := fmt.Sprintf("toYYYYMM(toDate(ExpenseDate)) = %s", yearMonth)
						
						if useDistributed && tm.config.ClickHouse.Cluster != "" {
							tableName := "volcengine_bill_details_distributed"
							syncResult, err = billService.SyncBillDataToDistributedWithCleanup(ctx, tableName, req, cleanCondition)
						} else {
							syncResult, err = billService.SyncBillDataWithCleanup(ctx, req, cleanCondition)
						}
						goto skipSync // 跳过下面的同步逻辑
					}
				}
			}
			
			// 标准同步，不清理数据（这些方法本身就没有预检查）
			if useDistributed && tm.config.ClickHouse.Cluster != "" {
				distributedTableName := "volcengine_bill_details_distributed"
				syncResult, err = billService.SyncBillDataToDistributed(ctx, distributedTableName, req)
			} else {
				syncResult, err = billService.SyncBillData(ctx, req)
			}
		skipSync:
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

	// 处理 ForceUpdate 参数：如果设置了 ForceUpdate，先执行数据一致性检查
	if task.Config.ForceUpdate {
		// 处理 granularity: "both" 的特殊情况
		if granularity == "both" {
			slog.Info("ForceUpdate模式：both粒度，分别清理日表和月表数据")
			
			// 获取智能时间选择
			smartSelection := getSmartTimeSelection(granularity)
			
			// 分别清理日表和月表的数据
			// 1. 清理昨天的日表数据
			dailyPeriod := smartSelection.YesterdayPeriod
			slog.Info("清理日表数据", "日期", dailyPeriod)
			if err := billService.CleanSpecificPeriodData(ctx, "DAILY", dailyPeriod); err != nil {
				slog.Warn("清理日表数据失败", "error", err)
			}
			
			// 2. 清理上月的月表数据
			monthlyPeriod := smartSelection.LastMonthPeriod
			slog.Info("清理月表数据", "账期", monthlyPeriod)
			if err := billService.CleanSpecificPeriodData(ctx, "MONTHLY", monthlyPeriod); err != nil {
				slog.Warn("清理月表数据失败", "error", err)
			}
			
			// 清理完成后，后续会在 case "both" 中处理同步
			// 不需要设置特殊的period，因为case "both"中会自己处理
			slog.Info("ForceUpdate模式：数据清理完成，准备执行同步")
			
		} else {
			// 处理单一粒度（daily 或 monthly）
			if period == "" {
				if granularity == "daily" {
					period = time.Now().AddDate(0, 0, -1).Format("2006-01-02") // 昨天
				} else {
					period = time.Now().Format("2006-01") // 当前月
				}
			}

			slog.Info("ForceUpdate模式：执行数据一致性检查", "粒度", granularity, "时间段", period)
			
			// 将granularity转换为大写以匹配API要求
			apiGranularity := strings.ToUpper(granularity)
			if apiGranularity != "DAILY" && apiGranularity != "MONTHLY" {
				apiGranularity = "MONTHLY" // 默认使用MONTHLY
			}
			
			// 执行数据对比
			comparisonResult, err := billService.PerformDataComparison(ctx, apiGranularity, period)
			if err != nil {
				slog.Warn("数据对比失败，继续执行强制更新", "error", err)
				// 对比失败时，执行强制清理和同步
				if err := billService.CleanSpecificPeriodData(ctx, apiGranularity, period); err != nil {
					return nil, fmt.Errorf("failed to clean data: %w", err)
				}
			} else {
				slog.Info("数据对比结果", 
					"API数量", comparisonResult.APICount,
					"数据库数量", comparisonResult.DatabaseCount,
					"需要同步", comparisonResult.NeedSync,
					"需要清理", comparisonResult.NeedCleanup)
				
				if !comparisonResult.NeedSync {
					// 数据一致，无需同步
					return &TaskResult{
						RecordsProcessed: 0,
						RecordsFetched:   0,
						Duration:         time.Since(time.Now()),
						Success:          true,
						Message:          fmt.Sprintf("ForceUpdate检查：数据已一致，无需同步 - %s", comparisonResult.Reason),
					}, nil
				}
				
				if comparisonResult.NeedCleanup {
					// 需要清理旧数据
					slog.Info("执行数据清理", "粒度", apiGranularity, "时间段", period)
					if err := billService.CleanSpecificPeriodData(ctx, apiGranularity, period); err != nil {
						return nil, fmt.Errorf("failed to clean data: %w", err)
					}
				}
			}
		}
		// 继续执行后续的同步逻辑
	}
	
	// 🎯 智能时间选择：sync-optimal 模式的特殊处理
	if task.Config.SyncMode == "sync-optimal" && period == "" && !task.Config.ForceUpdate {
		smartSelection := getSmartTimeSelection(granularity)

		switch granularity {
		case "daily":
			// 昨天的数据 (2006-01-02 格式)
			period = smartSelection.YesterdayPeriod
			slog.Info("智能选择同步时间", "模式", "sync-optimal", "提供商", "alicloud",
				"粒度", "daily", "日期", period, "说明", "检查并同步昨天数据")
			
			// 执行数据预检查
			comparisonResult, err := billService.PerformDataComparison(ctx, "DAILY", period)
			if err != nil {
				slog.Warn("数据预检查失败，继续执行同步", "error", err)
			} else {
				slog.Info("数据预检查结果", 
					"日期", period,
					"API数量", comparisonResult.APICount,
					"数据库数量", comparisonResult.DatabaseCount,
					"需要同步", comparisonResult.NeedSync)
				
				if !comparisonResult.NeedSync {
					// 数据已一致，跳过同步
					return &TaskResult{
						RecordsProcessed: 0,
						RecordsFetched:   0,
						Duration:         time.Since(time.Now()),
						Success:          true,
						Message:          fmt.Sprintf("sync-optimal检查：昨天数据已一致，跳过同步 - %s", comparisonResult.Reason),
					}, nil
				}
				
				if comparisonResult.NeedCleanup {
					// 需要清理旧数据
					slog.Info("执行数据清理", "日期", period)
					if err := billService.CleanSpecificPeriodData(ctx, "DAILY", period); err != nil {
						return nil, fmt.Errorf("failed to clean daily data: %w", err)
					}
				}
			}
		case "monthly":
			// 上个月的数据 (2006-01 格式)
			period = smartSelection.LastMonthPeriod
			slog.Info("智能选择同步时间", "模式", "sync-optimal", "提供商", "alicloud",
				"粒度", "monthly", "账期", period, "说明", "检查并同步上个月数据")
			
			// 执行数据预检查
			comparisonResult, err := billService.PerformDataComparison(ctx, "MONTHLY", period)
			if err != nil {
				slog.Warn("数据预检查失败，继续执行同步", "error", err)
			} else {
				slog.Info("数据预检查结果", 
					"账期", period,
					"API数量", comparisonResult.APICount,
					"数据库数量", comparisonResult.DatabaseCount,
					"需要同步", comparisonResult.NeedSync)
				
				if !comparisonResult.NeedSync {
					// 数据已一致，跳过同步
					return &TaskResult{
						RecordsProcessed: 0,
						RecordsFetched:   0,
						Duration:         time.Since(time.Now()),
						Success:          true,
						Message:          fmt.Sprintf("sync-optimal检查：上月数据已一致，跳过同步 - %s", comparisonResult.Reason),
					}, nil
				}
				
				if comparisonResult.NeedCleanup {
					// 需要清理旧数据
					slog.Info("执行数据清理", "账期", period)
					if err := billService.CleanSpecificPeriodData(ctx, "MONTHLY", period); err != nil {
						return nil, fmt.Errorf("failed to clean monthly data: %w", err)
					}
				}
			}
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

			// 再同步昨天的按天数据（使用 SyncSpecificDayBillData 函数，因为只同步单个日期）
			if err = billService.SyncSpecificDayBillData(ctx, smartSelection.YesterdayPeriod, syncOptions); err != nil {
				return nil, fmt.Errorf("fallback同步-昨天数据同步失败: %w", err)
			}
			slog.Info("fallback同步-昨天数据同步完成", "日期", smartSelection.YesterdayPeriod)

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
	} else if period == "" && !task.Config.ForceUpdate {
		// 非 sync-optimal 模式且非 ForceUpdate 模式，使用当前月
		period = time.Now().Format("2006-01")
	}

	var recordsProcessed int
	switch granularity {
	case "monthly":
		err = billService.SyncMonthlyBillData(ctx, period, syncOptions)
	case "daily":
		// 判断 period 格式：如果是 YYYY-MM-DD 格式，使用 SyncSpecificDayBillData
		// 如果是 YYYY-MM 格式，使用 SyncDailyBillData 同步整月数据
		if _, parseErr := time.Parse("2006-01-02", period); parseErr == nil {
			// 是具体日期格式，同步单个日期
			err = billService.SyncSpecificDayBillData(ctx, period, syncOptions)
		} else {
			// 是月份格式，同步整月数据
			err = billService.SyncDailyBillData(ctx, period, syncOptions)
		}
	case "both":
		// 处理both粒度：需要区分不同的情况
		if task.Config.ForceUpdate && task.Config.SyncMode == "sync-optimal" {
			// ForceUpdate + sync-optimal 模式：只同步昨天的日表和上月的月表
			smartSelection := getSmartTimeSelection(granularity)
			
			// 1. 同步上月的月表数据
			slog.Info("同步上月月表数据", "账期", smartSelection.LastMonthPeriod)
			if err = billService.SyncMonthlyBillData(ctx, smartSelection.LastMonthPeriod, syncOptions); err != nil {
				return nil, fmt.Errorf("同步上月月表失败: %w", err)
			}
			
			// 2. 同步昨天的日表数据
			slog.Info("同步昨天日表数据", "日期", smartSelection.YesterdayPeriod)
			if err = billService.SyncSpecificDayBillData(ctx, smartSelection.YesterdayPeriod, syncOptions); err != nil {
				return nil, fmt.Errorf("同步昨天日表失败: %w", err)
			}
			
			slog.Info("ForceUpdate + sync-optimal 同步完成", 
				"上月账期", smartSelection.LastMonthPeriod,
				"昨天日期", smartSelection.YesterdayPeriod)
		} else {
			// 普通模式：同步整个月的月表和日表数据
			err = billService.SyncBothGranularityData(ctx, period, syncOptions)
		}
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

// getRecentMonths 获取最近N个月的月份列表（包括当月）
// 例如：当前是2025-01，n=3 返回 ["2024-11", "2024-12", "2025-01"]
func getRecentMonths(n int) []string {
	if n <= 0 {
		return []string{}
	}
	
	now := time.Now()
	months := make([]string, 0, n)
	
	// 从最早的月份开始（n-1个月前）
	for i := n - 1; i >= 0; i-- {
		t := now.AddDate(0, -i, 0)
		months = append(months, t.Format("2006-01"))
	}
	
	return months
}
