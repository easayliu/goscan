package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"log/slog"
	"sync"
)

// ExecutorRegistry 执行器注册表
type ExecutorRegistry struct {
	executors map[string]ExecutorCreator
	mu        sync.RWMutex
}

// NewExecutorRegistry 创建新的执行器注册表
func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{
		executors: make(map[string]ExecutorCreator),
	}
}

// RegisterExecutor 注册执行器
func (r *ExecutorRegistry) RegisterExecutor(provider string, creator ExecutorCreator) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if creator == nil {
		return fmt.Errorf("creator cannot be nil")
	}
	
	r.executors[provider] = creator
	slog.Info("注册同步执行器", "provider", provider)
	return nil
}

// CreateExecutor 创建执行器
func (r *ExecutorRegistry) CreateExecutor(ctx context.Context, provider string) (SyncExecutor, error) {
	r.mu.RLock()
	creator, exists := r.executors[provider]
	r.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotSupported, provider)
	}
	
	return creator(ctx)
}

// GetSupportedProviders 获取支持的提供商列表
func (r *ExecutorRegistry) GetSupportedProviders() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	providers := make([]string, 0, len(r.executors))
	for provider := range r.executors {
		providers = append(providers, provider)
	}
	return providers
}

// 全局执行器注册表
var globalRegistry = NewExecutorRegistry()

// RegisterExecutor 注册执行器（包级别函数）
func RegisterExecutor(provider string, creator ExecutorCreator) error {
	return globalRegistry.RegisterExecutor(provider, creator)
}

// CreateExecutor 创建执行器（包级别函数）
func CreateExecutor(ctx context.Context, provider string) (SyncExecutor, error) {
	return globalRegistry.CreateExecutor(ctx, provider)
}

// GetSupportedProviders 获取支持的提供商列表（包级别函数）
func GetSupportedProviders() []string {
	return globalRegistry.GetSupportedProviders()
}

// BaseSyncExecutor 基础同步执行器，提供通用功能
type BaseSyncExecutor struct {
	config       *config.Config
	chClient     *clickhouse.Client
	syncHelper   *CommonSyncHelper
	timeSelector TimeSelector
}

// NewBaseSyncExecutor 创建基础同步执行器
func NewBaseSyncExecutor(cfg *config.Config, chClient *clickhouse.Client) *BaseSyncExecutor {
	return &BaseSyncExecutor{
		config:       cfg,
		chClient:     chClient,
		syncHelper:   NewCommonSyncHelper(),
		timeSelector: NewTimeSelector(),
	}
}

// ValidateConfig 验证配置
func (b *BaseSyncExecutor) ValidateConfig(ctx context.Context, config *SyncConfig) error {
	if config == nil {
		return fmt.Errorf("%w: config is nil", ErrInvalidTaskConfig)
	}
	
	return b.syncHelper.ValidateSyncConfig(config)
}

// ValidateTableConfig 验证表配置
func (b *BaseSyncExecutor) ValidateTableConfig(ctx context.Context, config *TableConfig) error {
	if config == nil {
		return fmt.Errorf("%w: table config is nil", ErrInvalidTaskConfig)
	}
	
	return b.syncHelper.ValidateTableConfig(config)
}

// GetTimeSelector 获取时间选择器
func (b *BaseSyncExecutor) GetTimeSelector() TimeSelector {
	return b.timeSelector
}

// GetSyncHelper 获取同步辅助器
func (b *BaseSyncExecutor) GetSyncHelper() *CommonSyncHelper {
	return b.syncHelper
}

// GetClickHouseClient 获取ClickHouse客户端
func (b *BaseSyncExecutor) GetClickHouseClient() *clickhouse.Client {
	return b.chClient
}

// GetConfig 获取配置
func (b *BaseSyncExecutor) GetConfig() *config.Config {
	return b.config
}

// LogExecutionStart 记录执行开始
func (b *BaseSyncExecutor) LogExecutionStart(ctx context.Context, provider, operation string) {
	slog.InfoContext(ctx, "开始执行同步操作",
		"provider", provider,
		"operation", operation,
	)
}

// LogExecutionComplete 记录执行完成
func (b *BaseSyncExecutor) LogExecutionComplete(ctx context.Context, provider, operation string, success bool, duration string) {
	slog.InfoContext(ctx, "同步操作完成",
		"provider", provider,
		"operation", operation,
		"success", success,
		"duration", duration,
	)
}

// LogExecutionError 记录执行错误
func (b *BaseSyncExecutor) LogExecutionError(ctx context.Context, provider, operation string, err error) {
	slog.ErrorContext(ctx, "同步操作失败",
		"provider", provider,
		"operation", operation,
		"error", err.Error(),
	)
}

// CreateTableConfig 根据任务配置创建表配置
func (b *BaseSyncExecutor) CreateTableConfig(taskConfig *TaskConfig, localTableName, distributedTableName string) *TableConfig {
	// 参考阿里云实现：只要配置了集群名称，就使用分布式表
	useDistributed := taskConfig.UseDistributed || b.config.ClickHouse.Cluster != ""
	
	return &TableConfig{
		UseDistributed:       useDistributed,
		LocalTableName:       localTableName,
		DistributedTableName: distributedTableName,
		ClusterName:          b.config.ClickHouse.Cluster,
	}
}

// CreateSyncConfig 根据任务配置创建同步配置
func (b *BaseSyncExecutor) CreateSyncConfig(taskConfig *TaskConfig, provider string) *SyncConfig {
	return &SyncConfig{
		Provider:       provider,
		SyncMode:       taskConfig.SyncMode,
		UseDistributed: taskConfig.UseDistributed,
		CreateTable:    taskConfig.CreateTable,
		ForceUpdate:    taskConfig.ForceUpdate,
		Granularity:    taskConfig.Granularity,
		BillPeriod:     taskConfig.BillPeriod,
		StartPeriod:    taskConfig.StartPeriod,
		EndPeriod:      taskConfig.EndPeriod,
		Limit:          taskConfig.Limit,
	}
}

// ExecutorFactory 执行器工厂实现
type ExecutorFactory struct {
	registry *ExecutorRegistry
	config   *config.Config
	chClient *clickhouse.Client
}

// NewExecutorFactory 创建执行器工厂
func NewExecutorFactory(cfg *config.Config, chClient *clickhouse.Client) *ExecutorFactory {
	factory := &ExecutorFactory{
		registry: NewExecutorRegistry(),
		config:   cfg,
		chClient: chClient,
	}
	
	// 注册内置执行器
	factory.registerBuiltinExecutors()
	
	return factory
}

// registerBuiltinExecutors 注册内置执行器
func (f *ExecutorFactory) registerBuiltinExecutors() {
	// 注册火山引擎执行器
	f.registry.RegisterExecutor("volcengine", func(ctx context.Context) (SyncExecutor, error) {
		return NewVolcEngineSyncExecutor(f.config, f.chClient), nil
	})
	
	// 注册阿里云执行器
	f.registry.RegisterExecutor("alicloud", func(ctx context.Context) (SyncExecutor, error) {
		return NewAliCloudSyncExecutor(f.config, f.chClient), nil
	})
}

// CreateExecutor 创建执行器
func (f *ExecutorFactory) CreateExecutor(ctx context.Context, provider string) (SyncExecutor, error) {
	return f.registry.CreateExecutor(ctx, provider)
}

// GetSupportedProviders 获取支持的提供商
func (f *ExecutorFactory) GetSupportedProviders() []string {
	return f.registry.GetSupportedProviders()
}

// RegisterExecutor 注册执行器
func (f *ExecutorFactory) RegisterExecutor(provider string, creator ExecutorCreator) error {
	return f.registry.RegisterExecutor(provider, creator)
}