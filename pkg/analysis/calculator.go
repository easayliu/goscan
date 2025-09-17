package analysis

import (
	"fmt"
	"sort"
	"time"
)

// costCalculator cost calculator implementation
type costCalculator struct {
	alertThreshold float64
}


// newCostCalculator creates cost calculator
func newCostCalculator(alertThreshold float64) *costCalculator {
	return &costCalculator{
		alertThreshold: alertThreshold,
	}
}

// CalculateCostChanges calculates cost changes
func (c *costCalculator) CalculateCostChanges(rawData []*RawCostData, yesterday, today time.Time) (*CostAnalysisResult, error) {
	if len(rawData) == 0 {
		return nil, wrapError(ErrNoDataFound, "raw data is empty")
	}

	result := &CostAnalysisResult{
		Date:          today,
		YesterdayDate: yesterday,
		Providers:     make([]*ProviderCostMetric, 0),
		Alerts:        make([]string, 0),
		GeneratedAt:   time.Now(),
	}

	// group data by provider
	aggregator := newDataAggregator(nil, nil, nil)
	providerData := aggregator.GroupDataByProvider(rawData)

	// calculate total cost
	totalMetric := &CostMetric{
		Name:     "Total Cost",
		Currency: "CNY",
	}

	// analyze provider data
	for provider, data := range providerData {
		providerMetric, err := c.AnalyzeProviderCosts(provider, data, yesterday, today)
		if err != nil {
			return nil, wrapError(err, "failed to analyze provider %s costs", provider)
		}

		if providerMetric != nil && len(providerMetric.Products) > 0 {
			result.Providers = append(result.Providers, providerMetric)

			// accumulate to total cost
			totalMetric.YesterdayCost += providerMetric.TotalCost.YesterdayCost
			totalMetric.TodayCost += providerMetric.TotalCost.TodayCost
		}
	}

	// calculate total cost changes
	totalMetric.CalculateChange()
	totalMetric.SetSignificant(c.alertThreshold)
	result.TotalCost = totalMetric

	// sort by provider name
	sort.Slice(result.Providers, func(i, j int) bool {
		return result.Providers[i].Provider < result.Providers[j].Provider
	})

	return result, nil
}

// AnalyzeProviderCosts analyzes costs for a single provider
func (c *costCalculator) AnalyzeProviderCosts(provider string, data []*RawCostData, yesterday, today time.Time) (*ProviderCostMetric, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// validate input parameters
	if provider == "" {
		return nil, NewValidationError("provider", provider, "provider name cannot be empty")
	}

	// group by product
	productData, err := c.groupDataByProduct(data)
	if err != nil {
		return nil, wrapError(err, "failed to group data by product")
	}

	// analyze product costs
	products := make([]*CostMetric, 0)
	providerTotal := &CostMetric{
		Name:     GetProviderDisplayName(provider),
		Currency: "CNY",
	}

	for product, dates := range productData {
		metric, err := c.calculateProductCost(product, dates, yesterday, today)
		if err != nil {
			return nil, wrapError(err, "failed to calculate product %s cost", product)
		}

		if metric != nil {
			// accumulate to provider total
			providerTotal.YesterdayCost += metric.YesterdayCost
			providerTotal.TodayCost += metric.TodayCost
			products = append(products, metric)
		}
	}

	// calculate provider total changes
	providerTotal.CalculateChange()

	// sort by product name
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

// groupDataByProduct groups data by product
func (c *costCalculator) groupDataByProduct(data []*RawCostData) (map[string]map[string]*RawCostData, error) {
	productData := make(map[string]map[string]*RawCostData) // product -> date -> data

	for _, item := range data {
		if item == nil {
			continue
		}

		// validate required fields
		if item.Product == "" {
			return nil, NewValidationError("product", item.Product, "product name cannot be empty")
		}

		if _, exists := productData[item.Product]; !exists {
			productData[item.Product] = make(map[string]*RawCostData)
		}

		// use date string as key
		dateKey := item.ExpenseDate.Format("2006-01-02")
		productData[item.Product][dateKey] = item
	}

	return productData, nil
}

// calculateProductCost calculates cost for a single product
func (c *costCalculator) calculateProductCost(product string, dates map[string]*RawCostData, yesterday, today time.Time) (*CostMetric, error) {
	if product == "" {
		return nil, NewValidationError("product", product, "product name cannot be empty")
	}

	metric := &CostMetric{
		Name:     product,
		Currency: "CNY", // default currency
	}

	// get yesterday and today data
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

	// calculate changes
	metric.CalculateChange()

	return metric, nil
}

// GenerateAlerts generates alert messages
func (c *costCalculator) GenerateAlerts(result *CostAnalysisResult, threshold float64) []string {
	var alerts []string

	if result == nil {
		return alerts
	}

	// total cost anomalies
	if result.TotalCost != nil && result.TotalCost.IsSignificant {
		if result.TotalCost.IsIncrease() {
			alerts = append(alerts,
				fmt.Sprintf("总费用上涨 %.1f%%，超过告警阈值 %.1f%%",
					result.TotalCost.ChangePercent, threshold))
		} else if result.TotalCost.IsDecrease() {
			alerts = append(alerts,
				fmt.Sprintf("总费用下降 %.1f%%，超过告警阈值 %.1f%%",
					-result.TotalCost.ChangePercent, threshold))
		}
	}

	// product anomalies
	alerts = c.generateProductAlerts(result.Providers, threshold, alerts)

	return alerts
}

// generateProductAlerts generates product-level alerts
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
					fmt.Sprintf("%s %s 费用上涨 %.1f%%",
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

// calculateTotalCosts calculates total costs
func (c *costCalculator) calculateTotalCosts(providers []*ProviderCostMetric) *CostMetric {
	totalMetric := &CostMetric{
		Name:     "Total Cost",
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

// validateCostData validates cost data
func (c *costCalculator) validateCostData(data []*RawCostData) error {
	if len(data) == 0 {
		return ErrNoDataFound
	}

	for i, item := range data {
		if item == nil {
			return NewValidationError("data", i, fmt.Sprintf("data item %d is nil", i))
		}

		if item.Provider == "" {
			return NewValidationError("provider", item.Provider, fmt.Sprintf("provider name is empty for data item %d", i))
		}

		if item.Product == "" {
			return NewValidationError("product", item.Product, fmt.Sprintf("product name is empty for data item %d", i))
		}

		if item.TotalAmount < 0 {
			return NewValidationError("amount", item.TotalAmount, fmt.Sprintf("amount is negative for data item %d", i))
		}
	}

	return nil
}
