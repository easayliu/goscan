package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"sync"

	"go.uber.org/zap"
)

// ExecutorRegistry executor registry
type ExecutorRegistry struct {
	executors map[string]ExecutorCreator
	mu        sync.RWMutex
}

// NewExecutorRegistry creates a new executor registry
func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{
		executors: make(map[string]ExecutorCreator),
	}
}

// RegisterExecutor registers an executor
func (r *ExecutorRegistry) RegisterExecutor(provider string, creator ExecutorCreator) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if creator == nil {
		return fmt.Errorf("creator cannot be nil")
	}

	r.executors[provider] = creator
	logger.Info("Registered sync executor", zap.String("provider", provider))
	return nil
}

// CreateExecutor creates an executor
func (r *ExecutorRegistry) CreateExecutor(ctx context.Context, provider string) (SyncExecutor, error) {
	r.mu.RLock()
	creator, exists := r.executors[provider]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotSupported, provider)
	}

	return creator(ctx)
}

// GetSupportedProviders retrieves list of supported providers
func (r *ExecutorRegistry) GetSupportedProviders() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	providers := make([]string, 0, len(r.executors))
	for provider := range r.executors {
		providers = append(providers, provider)
	}
	return providers
}

// Global executor registry
var globalRegistry = NewExecutorRegistry()

// RegisterExecutor registers an executor（包级别函数）
func RegisterExecutor(provider string, creator ExecutorCreator) error {
	return globalRegistry.RegisterExecutor(provider, creator)
}

// CreateExecutor creates an executor（包级别函数）
func CreateExecutor(ctx context.Context, provider string) (SyncExecutor, error) {
	return globalRegistry.CreateExecutor(ctx, provider)
}

// GetSupportedProviders retrieves list of supported providers（包级别函数）
func GetSupportedProviders() []string {
	return globalRegistry.GetSupportedProviders()
}

// BaseSyncExecutor base sync executor providing common functionality
type BaseSyncExecutor struct {
	config        *config.Config
	chClient      *clickhouse.Client
	syncHelper    *CommonSyncHelper
	timeSelector  TimeSelector
	dataValidator *CommonDataValidator
	dataCleaner   *CommonDataCleaner
}

// NewBaseSyncExecutor creates base sync executor
func NewBaseSyncExecutor(cfg *config.Config, chClient *clickhouse.Client) *BaseSyncExecutor {
	return &BaseSyncExecutor{
		config:        cfg,
		chClient:      chClient,
		syncHelper:    NewCommonSyncHelper(),
		timeSelector:  NewTimeSelector(),
		dataValidator: NewCommonDataValidator(chClient),
		dataCleaner:   NewCommonDataCleaner(chClient),
	}
}

// ValidateConfig validates configuration
func (b *BaseSyncExecutor) ValidateConfig(ctx context.Context, config *SyncConfig) error {
	if config == nil {
		return fmt.Errorf("%w: config is nil", ErrInvalidTaskConfig)
	}

	return b.syncHelper.ValidateSyncConfig(config)
}

// ValidateTableConfig validates table configuration
func (b *BaseSyncExecutor) ValidateTableConfig(ctx context.Context, config *TableConfig) error {
	if config == nil {
		return fmt.Errorf("%w: table config is nil", ErrInvalidTaskConfig)
	}

	return b.syncHelper.ValidateTableConfig(config)
}

// GetTimeSelector retrieves time selector
func (b *BaseSyncExecutor) GetTimeSelector() TimeSelector {
	return b.timeSelector
}

// GetSyncHelper retrieves sync helper
func (b *BaseSyncExecutor) GetSyncHelper() *CommonSyncHelper {
	return b.syncHelper
}

// GetClickHouseClient retrieves ClickHouse client
func (b *BaseSyncExecutor) GetClickHouseClient() *clickhouse.Client {
	return b.chClient
}

// GetConfig retrieves configuration
func (b *BaseSyncExecutor) GetConfig() *config.Config {
	return b.config
}

// GetDataValidator retrieves data validator
func (b *BaseSyncExecutor) GetDataValidator() *CommonDataValidator {
	return b.dataValidator
}

// GetDataCleaner retrieves data cleaner
func (b *BaseSyncExecutor) GetDataCleaner() *CommonDataCleaner {
	return b.dataCleaner
}

// LogExecutionStart logs execution start
func (b *BaseSyncExecutor) LogExecutionStart(ctx context.Context, provider, operation string) {
	logger.Info("Starting sync operation execution",
		zap.String("provider", provider),
		zap.String("operation", operation),
	)
}

// LogExecutionComplete logs execution completion
func (b *BaseSyncExecutor) LogExecutionComplete(ctx context.Context, provider, operation string, success bool, duration string) {
	logger.Info("Sync operation completed",
		zap.String("provider", provider),
		zap.String("operation", operation),
		zap.Bool("success", success),
		zap.String("duration", duration),
	)
}

// LogExecutionError logs execution error
func (b *BaseSyncExecutor) LogExecutionError(ctx context.Context, provider, operation string, err error) {
	logger.Error("Sync operation failed",
		zap.String("provider", provider),
		zap.String("operation", operation),
		zap.Error(err),
	)
}

// CreateTableConfig creates table configuration based on task configuration
func (b *BaseSyncExecutor) CreateTableConfig(taskConfig *TaskConfig, localTableName, distributedTableName string) *TableConfig {
	// Reference Alibaba Cloud implementation: use distributed table if cluster name is configured
	useDistributed := taskConfig.UseDistributed || b.config.ClickHouse.Cluster != ""

	return &TableConfig{
		UseDistributed:       useDistributed,
		LocalTableName:       localTableName,
		DistributedTableName: distributedTableName,
		ClusterName:          b.config.ClickHouse.Cluster,
	}
}

// CreateSyncConfig creates sync configuration based on task configuration
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

// ExecutorFactory executor factory implementation
type ExecutorFactory struct {
	registry *ExecutorRegistry
	config   *config.Config
	chClient *clickhouse.Client
}

// NewExecutorFactory creates executor factory
func NewExecutorFactory(cfg *config.Config, chClient *clickhouse.Client) *ExecutorFactory {
	factory := &ExecutorFactory{
		registry: NewExecutorRegistry(),
		config:   cfg,
		chClient: chClient,
	}

	// Register built-in executors
	factory.registerBuiltinExecutors()

	return factory
}

// registerBuiltinExecutors registers built-in executors
func (f *ExecutorFactory) registerBuiltinExecutors() {
	// Register Volcengine executor
	f.registry.RegisterExecutor("volcengine", func(ctx context.Context) (SyncExecutor, error) {
		return NewVolcEngineSyncExecutor(f.config, f.chClient), nil
	})

	// Register Alibaba Cloud executor
	f.registry.RegisterExecutor("alicloud", func(ctx context.Context) (SyncExecutor, error) {
		return NewAliCloudSyncExecutor(f.config, f.chClient), nil
	})
}

// CreateExecutor creates an executor
func (f *ExecutorFactory) CreateExecutor(ctx context.Context, provider string) (SyncExecutor, error) {
	return f.registry.CreateExecutor(ctx, provider)
}

// GetSupportedProviders 获取支持的提供商
func (f *ExecutorFactory) GetSupportedProviders() []string {
	return f.registry.GetSupportedProviders()
}

// RegisterExecutor registers an executor
func (f *ExecutorFactory) RegisterExecutor(provider string, creator ExecutorCreator) error {
	return f.registry.RegisterExecutor(provider, creator)
}
