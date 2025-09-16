package analysis

import (
	"context"
	"time"
)

// CostCalculator 成本计算器接口
type CostCalculator interface {
	// CalculateCostChanges 计算成本变化
	CalculateCostChanges(rawData []*RawCostData, yesterday, today time.Time) (*CostAnalysisResult, error)
	
	// AnalyzeProviderCosts 分析单个服务商的费用
	AnalyzeProviderCosts(provider string, data []*RawCostData, yesterday, today time.Time) (*ProviderCostMetric, error)
}

// DataAggregator 数据聚合器接口
type DataAggregator interface {
	// GroupDataByProvider 按服务商分组数据
	GroupDataByProvider(rawData []*RawCostData) map[string][]*RawCostData
	
	// GetCostDataForDates 获取指定日期的费用数据
	GetCostDataForDates(ctx context.Context, dates []time.Time, providers []string) ([]*RawCostData, error)
	
	// BatchQueryCostData 批量查询费用数据
	BatchQueryCostData(ctx context.Context, tables []DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error)
}

// AlertGenerator 告警生成器接口
type AlertGenerator interface {
	// GenerateAlerts 生成告警信息
	GenerateAlerts(result *CostAnalysisResult, threshold float64) []string
}

// ResultFormatter 结果格式化器接口
type ResultFormatter interface {
	// ConvertToWeChatFormat 转换为企业微信消息格式
	ConvertToWeChatFormat(result *CostAnalysisResult) interface{}
}

// QueryExecutor 查询执行器接口
type QueryExecutor interface {
	// ExecuteQuery 执行查询
	ExecuteQuery(ctx context.Context, query string, args ...interface{}) ([]*RawCostData, error)
	
	// QueryCostDataFromTable 从单个表查询费用数据
	QueryCostDataFromTable(ctx context.Context, info DatabaseTableInfo, dates []time.Time) ([]*RawCostData, error)
}

// CacheManager 缓存管理器接口
type CacheManager interface {
	// CacheQuery 缓存查询语句信息
	CacheQuery(query string) string
	
	// ClearQueryCache 清理查询缓存
	ClearQueryCache()
	
	// GetCacheStats 获取缓存统计信息
	GetCacheStats() map[string]interface{}
}

// MetricsCollector 指标收集器接口
type MetricsCollector interface {
	// RecordQueryMetrics 记录查询指标
	RecordQueryMetrics(duration time.Duration, err error)
	
	// GetQueryMetrics 获取查询性能指标
	GetQueryMetrics() QueryMetrics
}

// Analyzer 分析器主接口
type Analyzer interface {
	// AnalyzeDailyCosts 分析每日费用对比
	AnalyzeDailyCosts(ctx context.Context, req *CostAnalysisRequest) (*CostAnalysisResult, error)
	
	// SetAlertThreshold 设置告警阈值
	SetAlertThreshold(threshold float64)
	
	// SetQueryTimeout 设置查询超时时间
	SetQueryTimeout(timeout time.Duration)
	
	// Close 关闭分析器并清理资源
	Close() error
	
	// GetConnectionInfo 获取连接信息
	GetConnectionInfo() map[string]interface{}
}