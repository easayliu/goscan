package analysis

import (
	"encoding/json"
	"fmt"
	"goscan/pkg/wechat"
	"strings"
	"time"
)

// resultFormatter 结果格式化器实现
type resultFormatter struct{}

// newResultFormatter 创建结果格式化器
func newResultFormatter() *resultFormatter {
	return &resultFormatter{}
}

// ConvertToWeChatFormat 转换为企业微信消息格式
func (f *resultFormatter) ConvertToWeChatFormat(result *CostAnalysisResult) interface{} {
	if result == nil {
		return nil
	}

	wechatData := &wechat.CostComparisonData{
		Date:        time.Now().Format("2006-01-02"), // 报告日期应该是今天
		Alerts:      result.Alerts,
		GeneratedAt: result.GeneratedAt,
	}

	// 转换总费用
	wechatData.TotalCost = f.convertCostMetricToWeChatFormat(result.TotalCost)

	// 转换各服务商数据
	wechatData.Providers = f.convertProvidersToWeChatFormat(result.Providers)

	return wechatData
}

// convertCostMetricToWeChatFormat 转换费用指标为企业微信格式
func (f *resultFormatter) convertCostMetricToWeChatFormat(metric *CostMetric) *wechat.CostChange {
	if metric == nil {
		return nil
	}

	return &wechat.CostChange{
		Name:          metric.Name,
		YesterdayCost: metric.YesterdayCost,
		TodayCost:     metric.TodayCost,
		ChangeAmount:  metric.ChangeAmount,
		ChangePercent: metric.ChangePercent,
		Currency:      metric.Currency,
	}
}

// convertProvidersToWeChatFormat 转换服务商数据为企业微信格式
func (f *resultFormatter) convertProvidersToWeChatFormat(providers []*ProviderCostMetric) []*wechat.ProviderCostData {
	if len(providers) == 0 {
		return nil
	}

	wechatProviders := make([]*wechat.ProviderCostData, len(providers))
	for i, provider := range providers {
		wechatProviders[i] = f.convertProviderToWeChatFormat(provider)
	}

	return wechatProviders
}

// convertProviderToWeChatFormat 转换单个服务商数据为企业微信格式
func (f *resultFormatter) convertProviderToWeChatFormat(provider *ProviderCostMetric) *wechat.ProviderCostData {
	if provider == nil {
		return nil
	}

	wechatProvider := &wechat.ProviderCostData{
		Provider:    provider.Provider,
		DisplayName: provider.DisplayName,
		Products:    f.convertProductsToWeChatFormat(provider.Products),
	}

	// 转换总费用
	wechatProvider.TotalCost = f.convertCostMetricToWeChatFormat(provider.TotalCost)

	return wechatProvider
}

// convertProductsToWeChatFormat 转换产品数据为企业微信格式
func (f *resultFormatter) convertProductsToWeChatFormat(products []*CostMetric) []*wechat.CostChange {
	if len(products) == 0 {
		return nil
	}

	wechatProducts := make([]*wechat.CostChange, len(products))
	for i, product := range products {
		wechatProducts[i] = f.convertCostMetricToWeChatFormat(product)
	}

	return wechatProducts
}

// FormatCostAnalysisToText 将分析结果格式化为文本
func (f *resultFormatter) FormatCostAnalysisToText(result *CostAnalysisResult) string {
	if result == nil {
		return "无分析结果"
	}

	text := f.formatHeader(result)
	text += f.formatTotalCost(result.TotalCost)
	text += f.formatProviders(result.Providers)
	text += f.formatAlerts(result.Alerts)

	return text
}

// formatHeader 格式化头部信息
func (f *resultFormatter) formatHeader(result *CostAnalysisResult) string {
	return "📊 云费用分析报告\n" +
		"分析日期: " + result.Date.Format("2006-01-02") + "\n" +
		"对比日期: " + result.YesterdayDate.Format("2006-01-02") + "\n" +
		"生成时间: " + result.GeneratedAt.Format("2006-01-02 15:04:05") + "\n\n"
}

// formatTotalCost 格式化总费用信息
func (f *resultFormatter) formatTotalCost(metric *CostMetric) string {
	if metric == nil {
		return ""
	}

	text := "💰 总费用变化\n"
	text += f.formatCostChange(metric) + "\n"

	return text
}

// formatProviders 格式化服务商信息
func (f *resultFormatter) formatProviders(providers []*ProviderCostMetric) string {
	if len(providers) == 0 {
		return ""
	}

	text := "🏢 各服务商费用详情\n"
	for _, provider := range providers {
		text += f.formatProviderCost(provider)
	}

	return text
}

// formatProviderCost 格式化单个服务商费用
func (f *resultFormatter) formatProviderCost(provider *ProviderCostMetric) string {
	if provider == nil {
		return ""
	}

	text := "▶ " + provider.DisplayName + "\n"
	if provider.TotalCost != nil {
		text += "  总计: " + f.formatCostChange(provider.TotalCost) + "\n"
	}

	for _, product := range provider.Products {
		text += "  • " + product.Name + ": " + f.formatCostChange(product) + "\n"
	}

	return text + "\n"
}

// formatCostChange 格式化费用变化
func (f *resultFormatter) formatCostChange(metric *CostMetric) string {
	if metric == nil {
		return "无数据"
	}

	changeSymbol := ""
	if metric.ChangeAmount > 0 {
		changeSymbol = "↗"
	} else if metric.ChangeAmount < 0 {
		changeSymbol = "↘"
	} else {
		changeSymbol = "→"
	}

	return f.formatAmount(metric.TodayCost, metric.Currency) +
		" (" + changeSymbol + f.formatAmount(metric.ChangeAmount, metric.Currency) +
		", " + f.formatPercent(metric.ChangePercent) + ")"
}

// formatAlerts 格式化告警信息
func (f *resultFormatter) formatAlerts(alerts []string) string {
	if len(alerts) == 0 {
		return "✅ 无异常告警\n"
	}

	text := "⚠️ 异常告警\n"
	for _, alert := range alerts {
		text += "• " + alert + "\n"
	}

	return text
}

// formatAmount 格式化金额
func (f *resultFormatter) formatAmount(amount float64, currency string) string {
	if currency == "" {
		currency = "CNY"
	}
	return f.formatFloat(amount, 2) + " " + currency
}

// formatPercent 格式化百分比
func (f *resultFormatter) formatPercent(percent float64) string {
	return f.formatFloat(percent, 1) + "%"
}

// formatFloat 格式化浮点数
func (f *resultFormatter) formatFloat(value float64, precision int) string {
	format := "%." + string(rune('0'+precision)) + "f"
	return fmt.Sprintf(format, value)
}

// FormatCostAnalysisToJSON 将分析结果格式化为JSON
func (f *resultFormatter) FormatCostAnalysisToJSON(result *CostAnalysisResult) ([]byte, error) {
	if result == nil {
		return nil, NewValidationError("result", result, "分析结果不能为空")
	}

	return json.Marshal(result)
}

// FormatConnectionInfo 格式化连接信息
func (f *resultFormatter) FormatConnectionInfo(info map[string]interface{}) string {
	if len(info) == 0 {
		return "无连接信息"
	}

	text := "🔗 连接信息\n"
	
	if directConn, ok := info["direct_connection"].(bool); ok {
		text += "直接连接: " + f.formatBool(directConn) + "\n"
	}
	
	if legacyClient, ok := info["legacy_client"].(bool); ok {
		text += "Legacy客户端: " + f.formatBool(legacyClient) + "\n"
	}
	
	if pooled, ok := info["connection_pooled"].(bool); ok {
		text += "连接池: " + f.formatBool(pooled) + "\n"
	}
	
	if timeout, ok := info["query_timeout"].(time.Duration); ok {
		text += "查询超时: " + timeout.String() + "\n"
	}
	
	if addresses, ok := info["addresses"].([]string); ok && len(addresses) > 0 {
		text += "地址: " + strings.Join(addresses, ", ") + "\n"
	}
	
	if database, ok := info["database"].(string); ok && database != "" {
		text += "数据库: " + database + "\n"
	}
	
	if protocol, ok := info["protocol"].(string); ok && protocol != "" {
		text += "协议: " + protocol + "\n"
	}

	// 格式化查询指标
	if metrics, ok := info["query_metrics"].(map[string]interface{}); ok {
		text += f.formatQueryMetrics(metrics)
	}

	// 格式化缓存信息
	if cacheSize, ok := info["query_cache_size"].(int); ok {
		text += "查询缓存大小: " + fmt.Sprintf("%d", cacheSize) + "\n"
	}

	return text
}

// formatQueryMetrics 格式化查询指标
func (f *resultFormatter) formatQueryMetrics(metrics map[string]interface{}) string {
	text := "\n📈 查询指标\n"
	
	if totalQueries, ok := metrics["total_queries"].(int64); ok {
		text += "总查询数: " + fmt.Sprintf("%d", totalQueries) + "\n"
	}
	
	if totalDuration, ok := metrics["total_duration"].(string); ok {
		text += "总耗时: " + totalDuration + "\n"
	}
	
	if avgDuration, ok := metrics["average_duration"].(string); ok {
		text += "平均耗时: " + avgDuration + "\n"
	}
	
	if slowQueries, ok := metrics["slow_queries"].(int64); ok {
		text += "慢查询数: " + fmt.Sprintf("%d", slowQueries) + "\n"
	}
	
	if errorCount, ok := metrics["error_count"].(int64); ok {
		text += "错误数: " + fmt.Sprintf("%d", errorCount) + "\n"
	}

	return text
}

// formatBool 格式化布尔值
func (f *resultFormatter) formatBool(value bool) string {
	if value {
		return "是"
	}
	return "否"
}

// FormatCacheStats 格式化缓存统计信息
func (f *resultFormatter) FormatCacheStats(stats map[string]interface{}) string {
	if len(stats) == 0 {
		return "无缓存统计信息"
	}

	text := "💾 缓存统计\n"
	
	if cacheSize, ok := stats["cache_size"].(int); ok {
		text += "缓存大小: " + fmt.Sprintf("%d", cacheSize) + "\n"
	}
	
	if cacheHits, ok := stats["cache_hits"].(int64); ok {
		text += "缓存命中: " + fmt.Sprintf("%d", cacheHits) + "\n"
	}
	
	if cacheMisses, ok := stats["cache_misses"].(int64); ok {
		text += "缓存未命中: " + fmt.Sprintf("%d", cacheMisses) + "\n"
	}
	
	if hitRate, ok := stats["hit_rate"].(string); ok {
		text += "命中率: " + hitRate + "\n"
	}

	return text
}