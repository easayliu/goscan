package cloudsync

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/logger"
	"time"

	"go.uber.org/zap"
)

// DefaultConsistencyChecker implements ConsistencyChecker interface
type DefaultConsistencyChecker struct {
	provider    CloudProvider
	chClient    *clickhouse.Client
	dataCleaner DataCleaner
}

// NewDefaultConsistencyChecker creates a new consistency checker
func NewDefaultConsistencyChecker(provider CloudProvider, chClient *clickhouse.Client, dataCleaner DataCleaner) *DefaultConsistencyChecker {
	return &DefaultConsistencyChecker{
		provider:    provider,
		chClient:    chClient,
		dataCleaner: dataCleaner,
	}
}

// CheckPeriodConsistency checks if data count in API matches database for a specific period
func (c *DefaultConsistencyChecker) CheckPeriodConsistency(ctx context.Context, period *PeriodInfo) (bool, error) {
	// Get API data count
	apiCount, err := c.provider.GetAPIDataCount(ctx, period.Period, period.Granularity)
	if err != nil {
		return false, fmt.Errorf("failed to get API data count for period %s: %w", period.Period, err)
	}

	// Get database data count
	dbCount, err := c.getDBCount(ctx, period.Period, period.Granularity)
	if err != nil {
		return false, fmt.Errorf("failed to get database count for period %s: %w", period.Period, err)
	}

	// Update period info
	period.APICount = apiCount
	period.DBCount = dbCount

	// Log comparison
	logger.Info("data count comparison",
		zap.String("provider", c.provider.GetProviderName()),
		zap.String("period", period.Period),
		zap.String("granularity", period.Granularity),
		zap.Int64("api_count", apiCount),
		zap.Int64("db_count", dbCount))

	// Compare counts
	consistent := apiCount == dbCount
	if !consistent {
		logger.Warn("data count inconsistency detected",
			zap.String("provider", c.provider.GetProviderName()),
			zap.String("period", period.Period),
			zap.String("granularity", period.Granularity),
			zap.Int64("api_count", apiCount),
			zap.Int64("db_count", dbCount))

		period.NeedSync = true
		period.NeedCleanup = dbCount > 0
		period.Reason = fmt.Sprintf("API count: %d, DB count: %d", apiCount, dbCount)
	} else {
		logger.Info("data count is consistent",
			zap.String("provider", c.provider.GetProviderName()),
			zap.String("period", period.Period),
			zap.String("granularity", period.Granularity),
			zap.Int64("count", apiCount))

		period.NeedSync = false
		period.NeedCleanup = false
		period.Reason = "data is consistent"
	}

	return consistent, nil
}

// FindInconsistentPeriods finds all periods that have inconsistent data
func (c *DefaultConsistencyChecker) FindInconsistentPeriods(ctx context.Context, config *SyncConfig) ([]*PeriodInfo, error) {
	var periods []*PeriodInfo

	if config.SyncMode == "sync-optimal" {
		// For sync-optimal mode, check predefined periods
		periods = c.calculatePeriodsToCheck(config)
	} else {
		// For standard mode, only check the specified period
		if config.BillPeriod == "" {
			config.BillPeriod = time.Now().Format("2006-01")
		}

		granularities := []string{config.Granularity}
		if config.Granularity == "both" {
			granularities = []string{"monthly", "daily"}
		}

		for _, granularity := range granularities {
			periods = append(periods, &PeriodInfo{
				Period:      config.BillPeriod,
				Granularity: granularity,
			})
		}
	}

	var inconsistentPeriods []*PeriodInfo

	// Check each period for consistency
	for _, period := range periods {
		consistent, err := c.CheckPeriodConsistency(ctx, period)
		if err != nil {
			logger.Warn("failed to check period consistency, marking as inconsistent",
				zap.String("provider", c.provider.GetProviderName()),
				zap.String("period", period.Period),
				zap.String("granularity", period.Granularity),
				zap.Error(err))
			period.NeedSync = true
			period.NeedCleanup = false
			period.Reason = fmt.Sprintf("check failed: %v", err)
			inconsistentPeriods = append(inconsistentPeriods, period)
		} else if !consistent {
			inconsistentPeriods = append(inconsistentPeriods, period)
		}
	}

	return inconsistentPeriods, nil
}

// CleanInconsistentData cleans data for inconsistent periods
func (c *DefaultConsistencyChecker) CleanInconsistentData(ctx context.Context, periods []*PeriodInfo) error {
	for _, period := range periods {
		if period.NeedCleanup {
			logger.Info("cleaning inconsistent data",
				zap.String("provider", c.provider.GetProviderName()),
				zap.String("period", period.Period),
				zap.String("granularity", period.Granularity))

			tableConfig := c.provider.GetTableConfig(period.Granularity)
			err := c.dataCleaner.CleanPeriodData(ctx,
				tableConfig.TableName,
				period.Period,
				tableConfig.PeriodField,
				c.provider.GetProviderName())

			if err != nil {
				logger.Error("failed to clean period data",
					zap.String("provider", c.provider.GetProviderName()),
					zap.String("period", period.Period),
					zap.String("granularity", period.Granularity),
					zap.Error(err))
				return fmt.Errorf("failed to clean period %s: %w", period.Period, err)
			}

			logger.Info("successfully cleaned period data",
				zap.String("provider", c.provider.GetProviderName()),
				zap.String("period", period.Period),
				zap.String("granularity", period.Granularity))
		}
	}
	return nil
}

// getDBCount gets the count of records in database for a specific period
func (c *DefaultConsistencyChecker) getDBCount(ctx context.Context, period, granularity string) (int64, error) {
	tableConfig := c.provider.GetTableConfig(granularity)
	if tableConfig == nil {
		return 0, fmt.Errorf("no table config found for granularity: %s", granularity)
	}

	resolver := c.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(tableConfig.TableName)

	var query string
	var args []interface{}

	if granularity == "monthly" {
		// For monthly data, check with period field and null date field (for alicloud compatibility)
		if tableConfig.DateField != "" {
			query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ? AND %s IS NULL",
				actualTableName, tableConfig.PeriodField, tableConfig.DateField)
		} else {
			query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?",
				actualTableName, tableConfig.PeriodField)
		}
		args = []interface{}{period}
	} else {
		// For daily data, check with date field
		if tableConfig.DateField != "" {
			query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?",
				actualTableName, tableConfig.DateField)
		} else {
			query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?",
				actualTableName, tableConfig.PeriodField)
		}
		args = []interface{}{period}
	}

	rows, err := c.chClient.Query(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to query database count: %w", err)
	}
	defer rows.Close()

	var count uint64
	if rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to scan database count: %w", err)
		}
	}

	return int64(count), nil
}

// calculatePeriodsToCheck calculates which periods to check for sync-optimal mode
func (c *DefaultConsistencyChecker) calculatePeriodsToCheck(config *SyncConfig) []*PeriodInfo {
	var periods []*PeriodInfo
	now := time.Now()

	providerName := c.provider.GetProviderName()

	if providerName == "alicloud" {
		// For AliCloud: check last month (monthly) and yesterday (daily)
		lastMonth := now.AddDate(0, -1, 0).Format("2006-01")
		yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")

		periods = append(periods, &PeriodInfo{
			Period:      lastMonth,
			Granularity: "monthly",
		})

		periods = append(periods, &PeriodInfo{
			Period:      yesterday,
			Granularity: "daily",
		})
	} else if providerName == "volcengine" {
		// For VolcEngine: check last 3 months (monthly only)
		for i := 2; i >= 0; i-- {
			period := now.AddDate(0, -i, 0).Format("2006-01")
			periods = append(periods, &PeriodInfo{
				Period:      period,
				Granularity: "monthly",
			})
		}
	}

	return periods
}
