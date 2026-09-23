package cloudsync

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/logger"
	"goscan/pkg/utils/dateutils"
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

	if config.SyncMode == "sync-optimal" && !hasExplicitPeriods(config) {
		// Nothing was asked for in particular, so sync-optimal picks the two
		// periods that move: the current-ish month and yesterday.
		periods = c.calculatePeriodsToCheck(config)
	} else {
		// The caller named the periods — a manual backfill from opdash, or a
		// standard run. Check exactly those, granularities included, so
		// "2026-04..2026-09, both" checks twelve entries and not one.
		var err error
		if periods, err = periodsToSync(c.provider, config); err != nil {
			return nil, err
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

// getDBCount counts the rows a period holds as the table will look once
// ReplacingMergeTree has merged, hence FINAL. Without it a period pulled a
// moment ago counts its old and new copies both, reads as inconsistent against
// the API, and gets cleaned and pulled again — the same symptom lost lines
// produce, for an unrelated reason. FINAL is correct on the Distributed table
// too: the sharding key is the sorting key, so all copies of a line share a
// shard.
func (c *DefaultConsistencyChecker) getDBCount(ctx context.Context, period, granularity string) (int64, error) {
	tableConfig := c.provider.GetTableConfig(granularity)
	if tableConfig == nil {
		return 0, fmt.Errorf("no table config found for granularity: %s", granularity)
	}

	resolver := c.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(tableConfig.TableName)

	condition, err := periodCondition(tableConfig, period, granularity)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("SELECT count() FROM %s FINAL WHERE %s", actualTableName, condition)
	args := []interface{}{period}

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

// periodCondition is the WHERE clause, with one ? for the period, that selects
// one period's rows.
//
// A monthly table is selected by its period column; where the table also has a
// date column (Alibaba Cloud's monthly table keeps billing_date NULL) that has
// to be NULL. A daily table is selected by its date column for one day, and by
// its cycle column for a whole YYYY-MM cycle — comparing a Date column to
// '2026-09' does not select the month, it fails.
func periodCondition(tc *TableConfig, period, granularity string) (string, error) {
	if granularity == "monthly" {
		if tc.DateField != "" {
			return fmt.Sprintf("%s = ? AND %s IS NULL", tc.PeriodField, tc.DateField), nil
		}
		return fmt.Sprintf("%s = ?", tc.PeriodField), nil
	}

	if !dateutils.IsValidBillingDate(period) {
		if tc.CycleField == "" {
			return "", fmt.Errorf("daily table %s cannot select the whole cycle %s: no cycle column", tc.TableName, period)
		}
		return fmt.Sprintf("%s = ?", tc.CycleField), nil
	}
	if tc.DateField != "" {
		return fmt.Sprintf("%s = ?", tc.DateField), nil
	}
	return fmt.Sprintf("%s = ?", tc.PeriodField), nil
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
