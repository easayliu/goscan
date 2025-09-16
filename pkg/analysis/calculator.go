package analysis

import (
	"fmt"
	"sort"
	"time"
)

// costCalculator 成本计算器实现
type costCalculator struct {
	alertThreshold float64
}

// newCostCalculator 创建成本计算器
func newCostCalculator(alertThreshold float64) *costCalculator {
	return &costCalculator{
		alertThreshold: alertThreshold,
	}
}

// CalculateCostChanges 计算成本变化
func (c *costCalculator) CalculateCostChanges(rawData []*RawCostData, yesterday, today time.Time) (*CostAnalysisResult, error) {
	if len(rawData) == 0 {
		return nil, wrapError(ErrNoDataFound, "原始数据为空")
	}

	result := &CostAnalysisResult{
		Date:          today,
		YesterdayDate: yesterday,
		Providers:     make([]*ProviderCostMetric, 0),
		Alerts:        make([]string, 0),
		GeneratedAt:   time.Now(),
	}

	// 按服务商分组数据
	aggregator := newDataAggregator(nil, nil, nil)
	providerData := aggregator.GroupDataByProvider(rawData)

	// 计算总费用
	totalMetric := &CostMetric{
		Name:     "总费用",
		Currency: "CNY",
	}

	// 分析各服务商数据
	for provider, data := range providerData {
		providerMetric, err := c.AnalyzeProviderCosts(provider, data, yesterday, today)
		if err != nil {
			return nil, wrapError(err, "分析服务商 %s 费用失败", provider)
		}
		
		if providerMetric != nil && len(providerMetric.Products) > 0 {
			result.Providers = append(result.Providers, providerMetric)

			// 累加到总费用
			totalMetric.YesterdayCost += providerMetric.TotalCost.YesterdayCost
			totalMetric.TodayCost += providerMetric.TotalCost.TodayCost
		}
	}

	// 计算总费用变化
	totalMetric.CalculateChange()
	totalMetric.SetSignificant(c.alertThreshold)
	result.TotalCost = totalMetric

	// 按服务商名称排序
	sort.Slice(result.Providers, func(i, j int) bool {
		return result.Providers[i].Provider < result.Providers[j].Provider
	})

	return result, nil
}

// AnalyzeProviderCosts 分析单个服务商的费用
func (c *costCalculator) AnalyzeProviderCosts(provider string, data []*RawCostData, yesterday, today time.Time) (*ProviderCostMetric, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// 验证输入参数
	if provider == "" {
		return nil, NewValidationError("provider", provider, "服务商名称不能为空")
	}

	// 按产品分组
	productData, err := c.groupDataByProduct(data)
	if err != nil {
		return nil, wrapError(err, "按产品分组数据失败")
	}

	// 分析各产品费用
	products := make([]*CostMetric, 0)
	providerTotal := &CostMetric{
		Name:     GetProviderDisplayName(provider),
		Currency: "CNY",
	}

	for product, dates := range productData {
		metric, err := c.calculateProductCost(product, dates, yesterday, today)
		if err != nil {
			return nil, wrapError(err, "计算产品 %s 费用失败", product)
		}

		if metric != nil {
			// 累加到服务商总计
			providerTotal.YesterdayCost += metric.YesterdayCost
			providerTotal.TodayCost += metric.TodayCost
			products = append(products, metric)
		}
	}

	// 计算服务商总计变化
	providerTotal.CalculateChange()

	// 按产品名称排序
	sort.Slice(products, func(i, j int) bool {
		return products[i].Name < products[j].Name
	})

	return &ProviderCostMetric{
		Provider:    provider,
		DisplayName: GetProviderDisplayName(provider),
		TotalCost:   providerTotal,
		Products:    products,
	}, nil
}

// groupDataByProduct 按产品分组数据
func (c *costCalculator) groupDataByProduct(data []*RawCostData) (map[string]map[string]*RawCostData, error) {
	productData := make(map[string]map[string]*RawCostData) // product -> date -> data

	for _, item := range data {
		if item == nil {
			continue
		}

		// 验证必要字段
		if item.Product == "" {
			return nil, NewValidationError("product", item.Product, "产品名称不能为空")
		}

		if _, exists := productData[item.Product]; !exists {
			productData[item.Product] = make(map[string]*RawCostData)
		}

		// 使用日期字符串作为key
		dateKey := item.ExpenseDate.Format("2006-01-02")
		productData[item.Product][dateKey] = item
	}

	return productData, nil
}

// calculateProductCost 计算单个产品的费用
func (c *costCalculator) calculateProductCost(product string, dates map[string]*RawCostData, yesterday, today time.Time) (*CostMetric, error) {
	if product == "" {
		return nil, NewValidationError("product", product, "产品名称不能为空")
	}

	metric := &CostMetric{
		Name:     product,
		Currency: "CNY", // 默认货币
	}

	// 获取昨天和今天的数据
	yesterdayStr := yesterday.Format("2006-01-02")
	todayStr := today.Format("2006-01-02")

	if yesterdayData, exists := dates[yesterdayStr]; exists && yesterdayData != nil {
		metric.YesterdayCost = yesterdayData.TotalAmount
		metric.Currency = yesterdayData.Currency
	}

	if todayData, exists := dates[todayStr]; exists && todayData != nil {
		metric.TodayCost = todayData.TotalAmount
		metric.Currency = todayData.Currency
		metric.RecordCount = todayData.RecordCount
	}

	// 计算变化
	metric.CalculateChange()

	return metric, nil
}

// GenerateAlerts 生成告警信息
func (c *costCalculator) GenerateAlerts(result *CostAnalysisResult, threshold float64) []string {
	var alerts []string

	if result == nil {
		return alerts
	}

	// 总费用异常
	if result.TotalCost != nil && result.TotalCost.IsSignificant {
		if result.TotalCost.IsIncrease() {
			alerts = append(alerts,
				fmt.Sprintf("总费用增长 %.1f%%，超过告警阈值 %.1f%%",
					result.TotalCost.ChangePercent, threshold))
		} else if result.TotalCost.IsDecrease() {
			alerts = append(alerts,
				fmt.Sprintf("总费用下降 %.1f%%，超过告警阈值 %.1f%%",
					-result.TotalCost.ChangePercent, threshold))
		}
	}

	// 各产品异常
	alerts = c.generateProductAlerts(result.Providers, threshold, alerts)

	return alerts
}

// generateProductAlerts 生成产品级告警
func (c *costCalculator) generateProductAlerts(providers []*ProviderCostMetric, threshold float64, alerts []string) []string {
	for _, provider := range providers {
		if provider == nil {
			continue
		}

		for _, product := range provider.Products {
			if product == nil {
				continue
			}

			if product.ChangePercent >= threshold {
				alerts = append(alerts,
					fmt.Sprintf("%s %s 费用增长 %.1f%%",
						provider.DisplayName, product.Name, product.ChangePercent))
			} else if product.ChangePercent <= -threshold {
				alerts = append(alerts,
					fmt.Sprintf("%s %s 费用下降 %.1f%%",
						provider.DisplayName, product.Name, -product.ChangePercent))
			}
		}
	}

	return alerts
}

// calculateTotalCosts 计算总成本
func (c *costCalculator) calculateTotalCosts(providers []*ProviderCostMetric) *CostMetric {
	totalMetric := &CostMetric{
		Name:     "总费用",
		Currency: "CNY",
	}

	for _, provider := range providers {
		if provider != nil && provider.TotalCost != nil {
			totalMetric.YesterdayCost += provider.TotalCost.YesterdayCost
			totalMetric.TodayCost += provider.TotalCost.TodayCost
		}
	}

	totalMetric.CalculateChange()
	totalMetric.SetSignificant(c.alertThreshold)

	return totalMetric
}

// validateCostData 验证成本数据
func (c *costCalculator) validateCostData(data []*RawCostData) error {
	if len(data) == 0 {
		return ErrNoDataFound
	}

	for i, item := range data {
		if item == nil {
			return NewValidationError("data", i, fmt.Sprintf("第 %d 条数据为空", i))
		}

		if item.Provider == "" {
			return NewValidationError("provider", item.Provider, fmt.Sprintf("第 %d 条数据的服务商名称为空", i))
		}

		if item.Product == "" {
			return NewValidationError("product", item.Product, fmt.Sprintf("第 %d 条数据的产品名称为空", i))
		}

		if item.TotalAmount < 0 {
			return NewValidationError("amount", item.TotalAmount, fmt.Sprintf("第 %d 条数据的金额为负数", i))
		}
	}

	return nil
}