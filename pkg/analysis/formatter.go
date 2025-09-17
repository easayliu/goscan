package analysis

import (
	"encoding/json"
	"fmt"
	"goscan/pkg/wechat"
	"strings"
	"time"
)

// resultFormatter result formatter implementation
type resultFormatter struct{}

// newResultFormatter creates result formatter
func newResultFormatter() *resultFormatter {
	return &resultFormatter{}
}

// ConvertToWeChatFormat converts to WeChat message format
func (f *resultFormatter) ConvertToWeChatFormat(result *CostAnalysisResult) interface{} {
	if result == nil {
		return nil
	}

	wechatData := &wechat.CostComparisonData{
		Date:        time.Now().Format("2006-01-02"), // report date should be today
		Alerts:      result.Alerts,
		GeneratedAt: result.GeneratedAt,
	}

	// convert total cost
	wechatData.TotalCost = f.convertCostMetricToWeChatFormat(result.TotalCost)

	// convert provider data
	wechatData.Providers = f.convertProvidersToWeChatFormat(result.Providers)

	return wechatData
}

// convertCostMetricToWeChatFormat converts cost metrics to WeChat format
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

// convertProvidersToWeChatFormat converts provider data to WeChat format
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

// convertProviderToWeChatFormat converts single provider data to WeChat format
func (f *resultFormatter) convertProviderToWeChatFormat(provider *ProviderCostMetric) *wechat.ProviderCostData {
	if provider == nil {
		return nil
	}

	wechatProvider := &wechat.ProviderCostData{
		Provider:    provider.Provider,
		DisplayName: provider.DisplayName,
		Products:    f.convertProductsToWeChatFormat(provider.Products),
	}

	// convert total cost
	wechatProvider.TotalCost = f.convertCostMetricToWeChatFormat(provider.TotalCost)

	return wechatProvider
}

// convertProductsToWeChatFormat converts product data to WeChat format
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

// FormatCostAnalysisToText formats analysis results to text
func (f *resultFormatter) FormatCostAnalysisToText(result *CostAnalysisResult) string {
	if result == nil {
		return "No analysis results"
	}

	text := f.formatHeader(result)
	text += f.formatTotalCost(result.TotalCost)
	text += f.formatProviders(result.Providers)
	text += f.formatAlerts(result.Alerts)

	return text
}

// formatHeader formats header information
func (f *resultFormatter) formatHeader(result *CostAnalysisResult) string {
	return "📊 Cloud Cost Analysis Report\n" +
		"Analysis Date: " + result.Date.Format("2006-01-02") + "\n" +
		"Comparison Date: " + result.YesterdayDate.Format("2006-01-02") + "\n" +
		"Generated At: " + result.GeneratedAt.Format("2006-01-02 15:04:05") + "\n\n"
}

// formatTotalCost formats total cost information
func (f *resultFormatter) formatTotalCost(metric *CostMetric) string {
	if metric == nil {
		return ""
	}

	text := "💰 Total Cost Changes\n"
	text += f.formatCostChange(metric) + "\n"

	return text
}

// formatProviders formats provider information
func (f *resultFormatter) formatProviders(providers []*ProviderCostMetric) string {
	if len(providers) == 0 {
		return ""
	}

	text := "🏢 Provider Cost Details\n"
	for _, provider := range providers {
		text += f.formatProviderCost(provider)
	}

	return text
}

// formatProviderCost formats single provider cost
func (f *resultFormatter) formatProviderCost(provider *ProviderCostMetric) string {
	if provider == nil {
		return ""
	}

	text := "▶ " + provider.DisplayName + "\n"
	if provider.TotalCost != nil {
		text += "  Total: " + f.formatCostChange(provider.TotalCost) + "\n"
	}

	for _, product := range provider.Products {
		text += "  • " + product.Name + ": " + f.formatCostChange(product) + "\n"
	}

	return text + "\n"
}

// formatCostChange formats cost changes
func (f *resultFormatter) formatCostChange(metric *CostMetric) string {
	if metric == nil {
		return "No data"
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

// formatAlerts formats alert information
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

// formatAmount formats amount
func (f *resultFormatter) formatAmount(amount float64, currency string) string {
	if currency == "" {
		currency = "CNY"
	}
	return f.formatFloat(amount, 2) + " " + currency
}

// formatPercent formats percentage
func (f *resultFormatter) formatPercent(percent float64) string {
	return f.formatFloat(percent, 1) + "%"
}

// formatFloat formats float number
func (f *resultFormatter) formatFloat(value float64, precision int) string {
	format := "%." + string(rune('0'+precision)) + "f"
	return fmt.Sprintf(format, value)
}

// FormatCostAnalysisToJSON formats analysis results to JSON
func (f *resultFormatter) FormatCostAnalysisToJSON(result *CostAnalysisResult) ([]byte, error) {
	if result == nil {
		return nil, NewValidationError("result", result, "analysis result cannot be empty")
	}

	return json.Marshal(result)
}

// FormatConnectionInfo formats connection information
func (f *resultFormatter) FormatConnectionInfo(info map[string]interface{}) string {
	if len(info) == 0 {
		return "No connection information"
	}

	text := "🔗 Connection Information\n"

	if directConn, ok := info["direct_connection"].(bool); ok {
		text += "Direct Connection: " + f.formatBool(directConn) + "\n"
	}

	if legacyClient, ok := info["legacy_client"].(bool); ok {
		text += "Legacy Client: " + f.formatBool(legacyClient) + "\n"
	}

	if pooled, ok := info["connection_pooled"].(bool); ok {
		text += "Connection Pool: " + f.formatBool(pooled) + "\n"
	}

	if timeout, ok := info["query_timeout"].(time.Duration); ok {
		text += "Query Timeout: " + timeout.String() + "\n"
	}

	if addresses, ok := info["addresses"].([]string); ok && len(addresses) > 0 {
		text += "Addresses: " + strings.Join(addresses, ", ") + "\n"
	}

	if database, ok := info["database"].(string); ok && database != "" {
		text += "Database: " + database + "\n"
	}

	if protocol, ok := info["protocol"].(string); ok && protocol != "" {
		text += "Protocol: " + protocol + "\n"
	}

	// format query metrics
	if metrics, ok := info["query_metrics"].(map[string]interface{}); ok {
		text += f.formatQueryMetrics(metrics)
	}

	// format cache information
	if cacheSize, ok := info["query_cache_size"].(int); ok {
		text += "Query Cache Size: " + fmt.Sprintf("%d", cacheSize) + "\n"
	}

	return text
}

// formatQueryMetrics formats query metrics
func (f *resultFormatter) formatQueryMetrics(metrics map[string]interface{}) string {
	text := "\n📈 Query Metrics\n"

	if totalQueries, ok := metrics["total_queries"].(int64); ok {
		text += "Total Queries: " + fmt.Sprintf("%d", totalQueries) + "\n"
	}

	if totalDuration, ok := metrics["total_duration"].(string); ok {
		text += "Total Duration: " + totalDuration + "\n"
	}

	if avgDuration, ok := metrics["average_duration"].(string); ok {
		text += "Average Duration: " + avgDuration + "\n"
	}

	if slowQueries, ok := metrics["slow_queries"].(int64); ok {
		text += "Slow Queries: " + fmt.Sprintf("%d", slowQueries) + "\n"
	}

	if errorCount, ok := metrics["error_count"].(int64); ok {
		text += "Error Count: " + fmt.Sprintf("%d", errorCount) + "\n"
	}

	return text
}

// formatBool formats boolean value
func (f *resultFormatter) formatBool(value bool) string {
	if value {
		return "Yes"
	}
	return "No"
}

// FormatCacheStats formats cache statistics
func (f *resultFormatter) FormatCacheStats(stats map[string]interface{}) string {
	if len(stats) == 0 {
		return "No cache statistics"
	}

	text := "💾 Cache Statistics\n"

	if cacheSize, ok := stats["cache_size"].(int); ok {
		text += "Cache Size: " + fmt.Sprintf("%d", cacheSize) + "\n"
	}

	if cacheHits, ok := stats["cache_hits"].(int64); ok {
		text += "Cache Hits: " + fmt.Sprintf("%d", cacheHits) + "\n"
	}

	if cacheMisses, ok := stats["cache_misses"].(int64); ok {
		text += "Cache Misses: " + fmt.Sprintf("%d", cacheMisses) + "\n"
	}

	if hitRate, ok := stats["hit_rate"].(string); ok {
		text += "Hit Rate: " + hitRate + "\n"
	}

	return text
}
