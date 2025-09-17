package alicloud

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"strings"
	"time"

	"go.uber.org/zap"
)

type BillService struct {
	aliClient        *Client
	chClient         *clickhouse.Client
	monthlyTableName string
	dailyTableName   string
	config           *config.AliCloudConfig
}

// NewBillService creates Alibaba Cloud billing service
func NewBillService(aliConfig *config.AliCloudConfig, chClient *clickhouse.Client) (*BillService, error) {
	aliClient, err := NewClient(aliConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create AliCloud client: %w", err)
	}

	service := &BillService{
		aliClient:        aliClient,
		chClient:         chClient,
		monthlyTableName: "alicloud_bill_monthly",
		dailyTableName:   "alicloud_bill_daily",
		config:           aliConfig,
	}

	// Use configured table names if specified in config
	if aliConfig.MonthlyTable != "" {
		service.monthlyTableName = aliConfig.MonthlyTable
	}
	if aliConfig.DailyTable != "" {
		service.dailyTableName = aliConfig.DailyTable
	}

	return service, nil
}

// CreateMonthlyBillTable creates monthly billing table (supports automatic table name resolution)
func (s *BillService) CreateMonthlyBillTable(ctx context.Context) error {
	// Get resolver to determine actual table name for checking and creation
	resolver := s.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(s.monthlyTableName)

	// Check if table already exists
	exists, err := s.chClient.TableExists(ctx, s.monthlyTableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if exists {
		logger.Debug("monthly table exists, skipping creation",
			zap.String("provider", "alicloud"),
			zap.String("table", actualTableName))
		return nil
	}

	schema := `(
		-- Core identification fields
		instance_id String,
		instance_name String,
		bill_account_id String,
		bill_account_name String,
		
		-- Time fields
		billing_date Nullable(Date), -- Only has value for DAILY granularity
		billing_cycle String,
		
		-- Product information
		product_code String,
		product_name String,
		product_type String,
		product_detail String,
		
		-- Billing information
		subscription_type String,
		pricing_unit String,
		currency String,
		billing_type String,
		
		-- Usage information
		usage String,
		usage_unit String,
		
		-- Amount information (core)
		pretax_gross_amount Float64,
		invoice_discount Float64,
		deducted_by_coupons Float64,
		pretax_amount Float64,
		currency_amount Float64,
		payment_amount Float64,
		outstanding_amount Float64,
		
		-- Region information
		region String,
		zone String,
		
		-- Specification information
		instance_spec String,
		internet_ip String,
		intranet_ip String,
		
		-- Tags and grouping
		resource_group String,
		tags Map(String, String),
		cost_unit String,
		
		-- Other information
		service_period String,
		service_period_unit String,
		list_price String,
		list_price_unit String,
		owner_id String,
		
		-- Cost allocation
		split_item_id String,
		split_item_name String,
		split_account_id String,
		split_account_name String,
		
		-- Order information
		nick_name String,
		product_detail_code String,
		
		-- Bill attribution
		biz_type String,
		adjust_type String,
		adjust_amount Float64,
		
		-- Metadata
		granularity String DEFAULT 'MONTHLY',
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (billing_cycle, product_code, instance_id, bill_account_id, subscription_type, payment_amount)
	PARTITION BY toYYYYMM(parseDateTimeBestEffort(billing_cycle || '-01'))`

	// Create table using automatic table name resolution mechanism
	if resolver.IsClusterEnabled() {
		// Cluster mode: create complete distributed table structure (local table + distributed table)
		logger.Info("creating distributed monthly table",
			zap.String("provider", "alicloud"),
			zap.String("table", s.monthlyTableName),
			zap.String("mode", "cluster"))
		return s.chClient.CreateDistributedTableWithResolver(ctx, s.monthlyTableName, schema)
	} else {
		// Single node mode: create table directly
		logger.Info("creating monthly table",
			zap.String("provider", "alicloud"),
			zap.String("table", actualTableName),
			zap.String("mode", "single"))
		return s.chClient.CreateTable(ctx, s.monthlyTableName, schema)
	}
}

// CreateDailyBillTable creates daily billing table (supports automatic table name resolution)
func (s *BillService) CreateDailyBillTable(ctx context.Context) error {
	// Get resolver to determine actual table name for checking and creation
	resolver := s.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(s.dailyTableName)

	// Check if table already exists
	exists, err := s.chClient.TableExists(ctx, s.dailyTableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if exists {
		logger.Debug("daily table exists, skipping creation",
			zap.String("provider", "alicloud"),
			zap.String("table", actualTableName))
		return nil
	}

	schema := `(
		-- Core identification fields
		instance_id String,
		instance_name String,
		bill_account_id String,
		bill_account_name String,
		
		-- Time fields (daily)
		billing_date Date, -- Must have value for DAILY granularity
		billing_cycle String,
		
		-- Product information
		product_code String,
		product_name String,
		product_type String,
		product_detail String,
		
		-- Billing information
		subscription_type String,
		pricing_unit String,
		currency String,
		billing_type String,
		
		-- Usage information
		usage String,
		usage_unit String,
		
		-- Amount information (core)
		pretax_gross_amount Float64,
		invoice_discount Float64,
		deducted_by_coupons Float64,
		pretax_amount Float64,
		currency_amount Float64,
		payment_amount Float64,
		outstanding_amount Float64,
		
		-- Region information
		region String,
		zone String,
		
		-- Specification information
		instance_spec String,
		internet_ip String,
		intranet_ip String,
		
		-- Tags and grouping
		resource_group String,
		tags Map(String, String),
		cost_unit String,
		
		-- Other information
		service_period String,
		service_period_unit String,
		list_price String,
		list_price_unit String,
		owner_id String,
		
		-- Cost allocation
		split_item_id String,
		split_item_name String,
		split_account_id String,
		split_account_name String,
		
		-- Order information
		nick_name String,
		product_detail_code String,
		
		-- Bill attribution
		biz_type String,
		adjust_type String,
		adjust_amount Float64,
		
		-- Metadata
		granularity String DEFAULT 'DAILY',
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (billing_date, product_code, instance_id, bill_account_id, subscription_type, payment_amount)
	PARTITION BY toYYYYMMDD(billing_date)`

	// Create table using automatic table name resolution mechanism
	if resolver.IsClusterEnabled() {
		// Cluster mode: create complete distributed table structure (local table + distributed table)
		logger.Info("creating distributed daily table",
			zap.String("provider", "alicloud"),
			zap.String("table", s.dailyTableName),
			zap.String("mode", "cluster"))
		return s.chClient.CreateDistributedTableWithResolver(ctx, s.dailyTableName, schema)
	} else {
		// Single node mode: create table directly
		logger.Info("creating daily table",
			zap.String("provider", "alicloud"),
			zap.String("table", actualTableName),
			zap.String("mode", "single"))
		return s.chClient.CreateTable(ctx, s.dailyTableName, schema)
	}
}

// CreateDistributedMonthlyBillTable creates distributed monthly billing table
func (s *BillService) CreateDistributedMonthlyBillTable(ctx context.Context, localTableName, distributedTableName string) error {
	// Check if distributed table already exists
	distributedExists, err := s.chClient.TableExists(ctx, distributedTableName)
	if err != nil {
		return fmt.Errorf("failed to check distributed table existence: %w", err)
	}

	// Check if local table already exists
	localExists, err := s.chClient.TableExists(ctx, localTableName)
	if err != nil {
		return fmt.Errorf("failed to check local table existence: %w", err)
	}

	if distributedExists && localExists {
		logger.Debug("distributed monthly table exists, skipping creation",
			zap.String("provider", "alicloud"),
			zap.String("local_table", localTableName),
			zap.String("distributed_table", distributedTableName))
		return nil
	}

	if distributedExists || localExists {
		logger.Warn("partial distributed monthly table detected",
			zap.String("provider", "alicloud"),
			zap.Bool("local_exists", localExists),
			zap.Bool("distributed_exists", distributedExists))
	}

	// Get local table schema (using ReplacingMergeTree engine)
	localSchema := s.getMonthlyTableSchema()

	// Create distributed table (including local table and distributed table)
	if err := s.chClient.CreateDistributedTable(ctx, localTableName, distributedTableName, localSchema); err != nil {
		return fmt.Errorf("failed to create distributed monthly table: %w", err)
	}

	logger.Info("distributed monthly table created",
		zap.String("provider", "alicloud"),
		zap.String("local_table", localTableName),
		zap.String("distributed_table", distributedTableName))
	return nil
}

// CreateDistributedDailyBillTable creates distributed daily billing table
func (s *BillService) CreateDistributedDailyBillTable(ctx context.Context, localTableName, distributedTableName string) error {
	// Check if distributed table already exists
	distributedExists, err := s.chClient.TableExists(ctx, distributedTableName)
	if err != nil {
		return fmt.Errorf("failed to check distributed table existence: %w", err)
	}

	// Check if local table already exists
	localExists, err := s.chClient.TableExists(ctx, localTableName)
	if err != nil {
		return fmt.Errorf("failed to check local table existence: %w", err)
	}

	if distributedExists && localExists {
		logger.Debug("distributed daily table exists, skipping creation",
			zap.String("provider", "alicloud"),
			zap.String("local_table", localTableName),
			zap.String("distributed_table", distributedTableName))
		return nil
	}

	if distributedExists || localExists {
		logger.Warn("partial distributed daily table detected, creating missing tables",
			zap.String("provider", "alicloud"),
			zap.Bool("local_exists", localExists),
			zap.Bool("distributed_exists", distributedExists))
	}

	// Get local table schema (using ReplacingMergeTree engine)
	localSchema := s.getDailyTableSchema()

	// Create distributed table (including local table and distributed table)
	if err := s.chClient.CreateDistributedTable(ctx, localTableName, distributedTableName, localSchema); err != nil {
		return fmt.Errorf("failed to create distributed daily table: %w", err)
	}

	logger.Info("distributed daily table created",
		zap.String("provider", "alicloud"),
		zap.String("local_table", localTableName),
		zap.String("distributed_table", distributedTableName))
	return nil
}

// getMonthlyTableSchema gets monthly table schema (for distributed tables)
func (s *BillService) getMonthlyTableSchema() string {
	return `(
		instance_id String,
		instance_name String,
		bill_account_id String,
		bill_account_name String,
		billing_date Nullable(Date), -- NULL for monthly table
		billing_cycle String,
		product_code String,
		product_name String,
		product_type String,
		product_detail String,
		subscription_type String,
		pricing_unit String,
		currency String,
		billing_type String,
		usage String,
		usage_unit String,
		pretax_gross_amount Float64,
		invoice_discount Float64,
		deducted_by_coupons Float64,
		pretax_amount Float64,
		currency_amount Float64,
		payment_amount Float64,
		outstanding_amount Float64,
		region String,
		zone String,
		instance_spec String,
		internet_ip String,
		intranet_ip String,
		resource_group String,
		tags Map(String, String),
		cost_unit String,
		service_period String,
		service_period_unit String,
		list_price String,
		list_price_unit String,
		owner_id String,
		split_item_id String,
		split_item_name String,
		split_account_id String,
		split_account_name String,
		nick_name String,
		product_detail_code String,
		biz_type String,
		adjust_type String,
		adjust_amount Float64,
		granularity String DEFAULT 'MONTHLY',
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (billing_cycle, product_code, instance_id, bill_account_id, subscription_type, payment_amount)
	PARTITION BY toYYYYMM(parseDateTimeBestEffort(billing_cycle || '-01'))`
}

// getDailyTableSchema gets daily table schema (for distributed tables)
func (s *BillService) getDailyTableSchema() string {
	return `(
		instance_id String,
		instance_name String,
		bill_account_id String,
		bill_account_name String,
		billing_date Date, -- Daily table must have value
		billing_cycle String,
		product_code String,
		product_name String,
		product_type String,
		product_detail String,
		subscription_type String,
		pricing_unit String,
		currency String,
		billing_type String,
		usage String,
		usage_unit String,
		pretax_gross_amount Float64,
		invoice_discount Float64,
		deducted_by_coupons Float64,
		pretax_amount Float64,
		currency_amount Float64,
		payment_amount Float64,
		outstanding_amount Float64,
		region String,
		zone String,
		instance_spec String,
		internet_ip String,
		intranet_ip String,
		resource_group String,
		tags Map(String, String),
		cost_unit String,
		service_period String,
		service_period_unit String,
		list_price String,
		list_price_unit String,
		owner_id String,
		split_item_id String,
		split_item_name String,
		split_account_id String,
		split_account_name String,
		nick_name String,
		product_detail_code String,
		biz_type String,
		adjust_type String,
		adjust_amount Float64,
		granularity String DEFAULT 'DAILY',
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (billing_date, product_code, instance_id, bill_account_id, subscription_type, payment_amount)
	PARTITION BY toYYYYMMDD(billing_date)`
}

// SyncMonthlyBillData synchronizes monthly billing data
// Maintains backward compatibility, delegates to new sync manager
func (s *BillService) SyncMonthlyBillData(ctx context.Context, billingCycle string, options *SyncOptions) error {
	sm := NewSyncManager(s)
	req := &SyncRequest{
		Granularity: "MONTHLY",
		Period:      billingCycle,
		Options:     options,
	}
	_, err := sm.SyncData(ctx, req)
	return err
}

// SyncDailyBillData synchronizes daily billing data
// Maintains backward compatibility, delegates to new sync manager
func (s *BillService) SyncDailyBillData(ctx context.Context, billingCycle string, options *SyncOptions) error {
	sm := NewSyncManager(s)
	req := &SyncRequest{
		Granularity: "DAILY",
		Period:      billingCycle,
		Options:     options,
	}
	_, err := sm.SyncData(ctx, req)
	return err
}

// SyncSpecificDayBillData synchronizes daily table data for specified date
// Maintains backward compatibility, delegates to new sync manager
func (s *BillService) SyncSpecificDayBillData(ctx context.Context, billingDate string, options *SyncOptions) error {
	sm := NewSyncManager(s)
	req := &SyncRequest{
		Granularity: "DAILY",
		Period:      billingDate,
		Options:     options,
	}
	_, err := sm.SyncData(ctx, req)
	return err
}

// SyncBothGranularityData synchronizes data for both granularities
// Maintains backward compatibility, delegates to new sync manager
func (s *BillService) SyncBothGranularityData(ctx context.Context, billingCycle string, options *SyncOptions) error {
	sm := NewSyncManager(s)
	req := &SyncRequest{
		Granularity: "BOTH",
		Period:      billingCycle,
		Options:     options,
	}
	_, err := sm.SyncData(ctx, req)
	return err
}

// CleanBillData cleans billing data
func (s *BillService) CleanBillData(ctx context.Context, granularity string, condition string, dryRun bool) error {
	var tableName string
	switch strings.ToUpper(granularity) {
	case "MONTHLY":
		tableName = s.monthlyTableName
	case "DAILY":
		tableName = s.dailyTableName
	case "BOTH":
		// Clean both tables
		if err := s.CleanBillData(ctx, "MONTHLY", condition, dryRun); err != nil {
			return err
		}
		return s.CleanBillData(ctx, "DAILY", condition, dryRun)
	default:
		return fmt.Errorf("unsupported granularity: %s", granularity)
	}

	logger.Info("data cleanup starting",
		zap.String("provider", "alicloud"),
		zap.String("table", tableName),
		zap.String("condition", condition),
		zap.Bool("dry_run", dryRun))

	// Build cleanup options
	opts := &clickhouse.CleanupOptions{
		Condition: condition,
		DryRun:    dryRun,
	}

	// Preview or execute cleanup
	result, err := s.chClient.EnhancedCleanTableData(ctx, tableName, opts)
	if err != nil {
		return fmt.Errorf("failed to clean table data: %w", err)
	}

	if dryRun {
		logger.Info("cleanup preview completed",
			zap.String("provider", "alicloud"),
			zap.String("table", tableName),
			zap.Int64("rows_to_delete", result.PreviewRows))
	} else {
		logger.Info("data cleanup completed",
			zap.String("provider", "alicloud"),
			zap.String("table", tableName),
			zap.Int64("deleted_rows", result.DeletedRows))
	}

	return nil
}

// DropTable drops table (supports distributed tables)
func (s *BillService) DropTable(ctx context.Context, granularity string) error {
	var tableName string
	switch strings.ToUpper(granularity) {
	case "MONTHLY":
		tableName = s.monthlyTableName
	case "DAILY":
		tableName = s.dailyTableName
	case "BOTH":
		// Drop both tables
		if err := s.DropTable(ctx, "MONTHLY"); err != nil {
			logger.Warn("failed to drop monthly table",
				zap.String("provider", "alicloud"),
				zap.Error(err))
		}
		return s.DropTable(ctx, "DAILY")
	default:
		return fmt.Errorf("unsupported granularity: %s", granularity)
	}

	logger.Info("dropping table",
		zap.String("provider", "alicloud"),
		zap.String("table", tableName))

	// Check if it's a distributed table
	if s.isDistributedTable(tableName) {
		return s.dropDistributedTable(ctx, tableName)
	}

	// Regular table deletion
	return s.chClient.DropTable(ctx, tableName)
}

// GetAvailableBillingCycles gets available billing cycles list
func (s *BillService) GetAvailableBillingCycles(ctx context.Context) ([]string, error) {
	return s.aliClient.GetAvailableBillingCycles(ctx)
}

// TestConnection tests connection
func (s *BillService) TestConnection(ctx context.Context) error {
	return s.aliClient.TestConnection(ctx)
}

// Close closes service
func (s *BillService) Close() error {
	if s.aliClient != nil {
		return s.aliClient.Close()
	}
	return nil
}

// GetTableName gets table name by granularity
func (s *BillService) GetTableName(granularity string) string {
	switch strings.ToUpper(granularity) {
	case "DAILY":
		return s.dailyTableName
	case "MONTHLY":
		return s.monthlyTableName
	default:
		return s.monthlyTableName // Default return monthly table
	}
}

// GetMonthlyTableName gets monthly table name
func (s *BillService) GetMonthlyTableName() string {
	return s.monthlyTableName
}

// GetDailyTableName gets daily table name
func (s *BillService) GetDailyTableName() string {
	return s.dailyTableName
}

// SetDistributedTableNames sets distributed table names
func (s *BillService) SetDistributedTableNames(monthlyDistributedTable, dailyDistributedTable string) {
	s.monthlyTableName = monthlyDistributedTable
	s.dailyTableName = dailyDistributedTable
}

// SetTableNames sets table names (for custom table names)
func (s *BillService) SetTableNames(monthlyTable, dailyTable string) {
	if monthlyTable != "" {
		s.monthlyTableName = monthlyTable
	}
	if dailyTable != "" {
		s.dailyTableName = dailyTable
	}
}

// isDistributedTable checks if table is distributed
func (s *BillService) isDistributedTable(tableName string) bool {
	return strings.HasSuffix(tableName, "_distributed")
}

// dropDistributedTable drops distributed table (including local table and distributed table)
func (s *BillService) dropDistributedTable(ctx context.Context, distributedTableName string) error {
	logger.Info("Alibaba Cloud distributed table deletion starting",
		zap.String("provider", "alicloud"),
		zap.String("distributed_table", distributedTableName))

	// Generate local table name (remove _distributed suffix and add _local)
	localTableName := strings.TrimSuffix(distributedTableName, "_distributed") + "_local"

	// First drop distributed table
	logger.Info("Alibaba Cloud distributed table deletion removing distributed table",
		zap.String("provider", "alicloud"),
		zap.String("distributed_table", distributedTableName))
	if err := s.chClient.DropTable(ctx, distributedTableName); err != nil {
		logger.Warn("Alibaba Cloud distributed table deletion failed",
			zap.String("provider", "alicloud"),
			zap.Error(err))
		// Continue trying to drop local table
	}

	// Then drop local table (using ON CLUSTER)
	logger.Info("Alibaba Cloud distributed table deletion removing local table",
		zap.String("provider", "alicloud"),
		zap.String("local_table", localTableName))
	if err := s.chClient.DropDistributedTable(ctx, localTableName, distributedTableName); err != nil {
		return fmt.Errorf("failed to drop local table %s: %w", localTableName, err)
	}

	logger.Info("Alibaba Cloud distributed table deletion completed",
		zap.String("provider", "alicloud"),
		zap.String("distributed_table", distributedTableName))
	return nil
}

// DropOldTable drops specified old table (supports distributed tables)
func (s *BillService) DropOldTable(ctx context.Context, tableName string) error {
	logger.Info("dropping old table",
		zap.String("provider", "alicloud"),
		zap.String("table", tableName))

	// Check if it's a distributed table
	if s.isDistributedTable(tableName) {
		return s.dropDistributedTable(ctx, tableName)
	}

	// Regular table deletion
	return s.chClient.DropTable(ctx, tableName)
}

// CheckDailyDataExists checks if daily table data exists for specified date
func (s *BillService) CheckDailyDataExists(ctx context.Context, tableName, billingDate string) (bool, int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE billing_date = '%s'", tableName, billingDate)

	rows, err := s.chClient.Query(ctx, query)
	if err != nil {
		return false, 0, fmt.Errorf("failed to query daily data existence: %w", err)
	}
	defer rows.Close()

	var count uint64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return false, 0, fmt.Errorf("failed to scan count: %w", err)
		}
	}

	return count > 0, int64(count), nil
}

// CheckMonthlyDataExists checks if monthly table data exists for specified billing cycle
func (s *BillService) CheckMonthlyDataExists(ctx context.Context, tableName, billingCycle string) (bool, int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE billing_cycle = '%s'", tableName, billingCycle)

	rows, err := s.chClient.Query(ctx, query)
	if err != nil {
		return false, 0, fmt.Errorf("failed to query monthly data existence: %w", err)
	}
	defer rows.Close()

	var count uint64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return false, 0, fmt.Errorf("failed to scan count: %w", err)
		}
	}

	return count > 0, int64(count), nil
}

// CheckDailyDataExistsWithOptimize checks if daily table data exists for specified date (force optimize before check)
func (s *BillService) CheckDailyDataExistsWithOptimize(ctx context.Context, tableName, billingDate string) (bool, int64, error) {
	// For ReplacingMergeTree, execute OPTIMIZE FINAL first to force deduplication
	if strings.Contains(tableName, "_distributed") {
		// Distributed table: optimize local table
		localTableName := strings.Replace(tableName, "_distributed", "_local", 1)
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s ON CLUSTER %s FINAL", localTableName, s.chClient.GetClusterName())
		logger.Debug("ReplacingMergeTree optimization executing forced deduplication",
			zap.String("provider", "alicloud"),
			zap.String("optimize_query", optimizeQuery))
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			logger.Warn("ReplacingMergeTree optimization failed, continuing query",
				zap.String("provider", "alicloud"),
				zap.Error(err))
		}
	} else {
		// Regular table: optimize directly
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
		logger.Debug("ReplacingMergeTree optimization executing forced deduplication",
			zap.String("provider", "alicloud"),
			zap.String("optimize_query", optimizeQuery))
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			logger.Warn("ReplacingMergeTree optimization failed, continuing query",
				zap.String("provider", "alicloud"),
				zap.Error(err))
		}
	}

	// Query data count after optimization
	return s.CheckDailyDataExists(ctx, tableName, billingDate)
}

// CheckMonthlyDataExistsWithOptimize checks if monthly table data exists for specified billing cycle (force optimize before check)
func (s *BillService) CheckMonthlyDataExistsWithOptimize(ctx context.Context, tableName, billingCycle string) (bool, int64, error) {
	// For ReplacingMergeTree, execute OPTIMIZE FINAL first to force deduplication
	if strings.Contains(tableName, "_distributed") {
		// Distributed table: optimize local table
		localTableName := strings.Replace(tableName, "_distributed", "_local", 1)
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s ON CLUSTER %s FINAL", localTableName, s.chClient.GetClusterName())
		logger.Debug("ReplacingMergeTree optimization executing forced deduplication",
			zap.String("provider", "alicloud"),
			zap.String("optimize_query", optimizeQuery))
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			logger.Warn("ReplacingMergeTree optimization failed, continuing query",
				zap.String("provider", "alicloud"),
				zap.Error(err))
		}
	} else {
		// Regular table: optimize directly
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
		logger.Debug("ReplacingMergeTree optimization executing forced deduplication",
			zap.String("provider", "alicloud"),
			zap.String("optimize_query", optimizeQuery))
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			logger.Warn("ReplacingMergeTree optimization failed, continuing query",
				zap.String("provider", "alicloud"),
				zap.Error(err))
		}
	}

	// Query data count after optimization
	return s.CheckMonthlyDataExists(ctx, tableName, billingCycle)
}

// GetDailyAPIDataCount gets API data total count for specified date
func (s *BillService) GetDailyAPIDataCount(ctx context.Context, billingDate string) (int32, error) {
	logger.Debug("getting daily API data count",
		zap.String("provider", "alicloud"),
		zap.String("billing_date", billingDate))

	// Validate date format
	date, err := time.Parse("2006-01-02", billingDate)
	if err != nil {
		return 0, fmt.Errorf("invalid billing date format (expected YYYY-MM-DD): %w", err)
	}

	// Get billing cycle (YYYY-MM format)
	billingCycle := date.Format("2006-01")

	// Validate billing cycle
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return 0, fmt.Errorf("invalid billing cycle: %w", err)
	}

	// Create paginator (for specified date data)
	paginator := NewPaginator(s.aliClient, &DescribeInstanceBillRequest{
		BillingCycle: billingCycle,
		Granularity:  "DAILY",
		BillingDate:  billingDate,
		MaxResults:   int32(s.config.BatchSize),
	})

	// Estimate total record count
	totalCount, err := paginator.EstimateTotal(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to estimate API data count: %w", err)
	}

	logger.Info("daily API data count retrieved",
		zap.String("provider", "alicloud"),
		zap.String("billing_date", billingDate),
		zap.Int32("count", totalCount))
	return totalCount, nil
}

// GetMonthlyAPIDataCount gets API data total count for specified month
func (s *BillService) GetMonthlyAPIDataCount(ctx context.Context, billingCycle string) (int32, error) {
	logger.Debug("getting monthly API data count",
		zap.String("provider", "alicloud"),
		zap.String("billing_cycle", billingCycle))

	// Validate billing cycle
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return 0, fmt.Errorf("invalid billing cycle: %w", err)
	}

	// Create paginator
	paginator := NewPaginator(s.aliClient, &DescribeInstanceBillRequest{
		BillingCycle: billingCycle,
		Granularity:  "MONTHLY",
		MaxResults:   int32(s.config.BatchSize),
	})

	// Estimate total record count
	totalCount, err := paginator.EstimateTotal(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to estimate API data count: %w", err)
	}

	logger.Info("monthly API data count retrieved",
		zap.String("provider", "alicloud"),
		zap.String("billing_cycle", billingCycle),
		zap.Int32("count", totalCount))
	return totalCount, nil
}

// CleanSpecificTableData cleans data from specified table (for distributed table local table cleanup)
func (s *BillService) CleanSpecificTableData(ctx context.Context, tableName, condition string, dryRun bool) error {
	logger.Info("cleaning specific table data",
		zap.String("provider", "alicloud"),
		zap.String("table", tableName),
		zap.String("condition", condition),
		zap.Bool("dry_run", dryRun),
		zap.String("mode", "distributed_local"))

	// Build cleanup options
	opts := &clickhouse.CleanupOptions{
		Condition: condition,
		DryRun:    dryRun,
	}

	// Preview or execute cleanup
	result, err := s.chClient.EnhancedCleanTableData(ctx, tableName, opts)
	if err != nil {
		return fmt.Errorf("failed to clean table data: %w", err)
	}

	if dryRun {
		logger.Info("cleanup preview completed",
			zap.String("provider", "alicloud"),
			zap.String("table", tableName),
			zap.Int64("rows_to_delete", result.PreviewRows))
	} else {
		logger.Info("data cleanup completed",
			zap.String("provider", "alicloud"),
			zap.String("table", tableName),
			zap.Int64("deleted_rows", result.DeletedRows))
	}

	return nil
}

// GetBillDataCount gets billing data total count (generic method)
func (s *BillService) GetBillDataCount(ctx context.Context, granularity, period string) (int32, error) {
	switch strings.ToUpper(granularity) {
	case "DAILY":
		return s.GetDailyAPIDataCount(ctx, period)
	case "MONTHLY":
		return s.GetMonthlyAPIDataCount(ctx, period)
	default:
		return 0, fmt.Errorf("unsupported granularity: %s", granularity)
	}
}

// GetDatabaseRecordCount gets database record total count (generic method)
func (s *BillService) GetDatabaseRecordCount(ctx context.Context, granularity, period string) (int64, error) {
	tableName := s.GetTableName(granularity)

	switch strings.ToUpper(granularity) {
	case "DAILY":
		_, count, err := s.CheckDailyDataExists(ctx, tableName, period)
		return count, err
	case "MONTHLY":
		_, count, err := s.CheckMonthlyDataExists(ctx, tableName, period)
		return count, err
	default:
		return 0, fmt.Errorf("unsupported granularity: %s", granularity)
	}
}

// PerformDataComparison performs data comparison (determines if sync is needed)
func (s *BillService) PerformDataComparison(ctx context.Context, granularity, period string) (*DataComparisonResult, error) {
	logger.Info("Alibaba Cloud data pre-check starting data comparison",
		zap.String("provider", "alicloud"),
		zap.String("granularity", granularity),
		zap.String("period", period))

	// 获取API数据总数
	apiCount, err := s.GetBillDataCount(ctx, granularity, period)
	if err != nil {
		return nil, fmt.Errorf("failed to get API data count: %w", err)
	}

	// 获取数据库记录总数
	dbCount, err := s.GetDatabaseRecordCount(ctx, granularity, period)
	if err != nil {
		return nil, fmt.Errorf("failed to get database record count: %w", err)
	}

	// 比较数据量并决定是否需要同步和清理
	result := &DataComparisonResult{
		APICount:      apiCount,
		DatabaseCount: dbCount,
		Period:        period,
		Granularity:   granularity,
		NeedCleanup:   false,
		NeedSync:      false,
	}

	if apiCount == 0 && dbCount == 0 {
		// Neither API nor database has data
		result.NeedSync = false
		result.NeedCleanup = false
		result.Reason = "No data from API or database; skipping sync"
	} else if apiCount == 0 && dbCount > 0 {
		// API returned nothing but database has data, keep state as-is
		result.NeedSync = false
		result.NeedCleanup = false
		result.Reason = "API returned no data but database has data; skipping sync"
	} else if apiCount > 0 && dbCount == 0 {
		// API has data but database is empty, sync without cleanup
		result.NeedSync = true
		result.NeedCleanup = false
		result.Reason = fmt.Sprintf("Database is empty but API returned %d records; sync required", apiCount)
	} else if apiCount > 0 && int64(apiCount) == dbCount {
		// Same record count, sync not required
		result.NeedSync = false
		result.NeedCleanup = false
		result.Reason = "API and database counts match; skipping sync"
	} else {
		// Counts differ, require cleanup before sync
		result.NeedSync = true
		result.NeedCleanup = true
		result.Reason = fmt.Sprintf("Data counts differ (API:%d vs DB:%d); clean before syncing", apiCount, dbCount)
	}

	logger.Info("Alibaba Cloud data pre-check comparison result",
		zap.String("provider", "alicloud"),
		zap.String("granularity", granularity),
		zap.String("period", period),
		zap.Int32("api_count", apiCount),
		zap.Int64("db_count", dbCount),
		zap.Bool("need_sync", result.NeedSync),
		zap.Bool("need_cleanup", result.NeedCleanup),
		zap.String("reason", result.Reason))

	return result, nil
}

// PerformPreSyncCheck 执行同步前的数据预检查
func (s *BillService) PerformPreSyncCheck(ctx context.Context, granularity, period string) (*PreSyncCheckResult, error) {
	logger.Info("Alibaba Cloud sync pre-check starting data pre-check execution",
		zap.String("provider", "alicloud"),
		zap.String("granularity", granularity),
		zap.String("period", period))

	var results []*DataComparisonResult

	switch strings.ToUpper(granularity) {
	case "BOTH":
		// 对于双粒度，需要分别检查昨天的天表数据和上月的月表数据
		// 这里假设period格式为 "yesterday:2024-01-15,last_month:2024-01"
		parts := strings.Split(period, ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid period format for BOTH granularity, expected 'yesterday:YYYY-MM-DD,last_month:YYYY-MM'")
		}

		// 解析昨天日期
		yesterdayPart := strings.TrimPrefix(parts[0], "yesterday:")
		dailyResult, err := s.PerformDataComparison(ctx, "DAILY", yesterdayPart)
		if err != nil {
			return nil, fmt.Errorf("failed to check daily data: %w", err)
		}
		results = append(results, dailyResult)

		// 解析上月账期
		lastMonthPart := strings.TrimPrefix(parts[1], "last_month:")
		monthlyResult, err := s.PerformDataComparison(ctx, "MONTHLY", lastMonthPart)
		if err != nil {
			return nil, fmt.Errorf("failed to check monthly data: %w", err)
		}
		results = append(results, monthlyResult)

	default:
		// 单粒度检查
		result, err := s.PerformDataComparison(ctx, granularity, period)
		if err != nil {
			return nil, fmt.Errorf("failed to perform data comparison: %w", err)
		}
		results = append(results, result)
	}

	// Determine whether all checks indicate that sync can be skipped
	shouldSkip := true
	for _, result := range results {
		if result.NeedSync {
			shouldSkip = false
			break
		}
	}

	// Build summary for reporting
	summary := fmt.Sprintf("Pre-check completed: inspected %d time windows", len(results))
	if shouldSkip {
		summary += ", all data is up to date, skipping sync"
	} else {
		summary += ", differences detected, sync required"
	}

	checkResult := &PreSyncCheckResult{
		ShouldSkip: shouldSkip,
		Results:    results,
		Summary:    summary,
	}

	logger.Info("Alibaba Cloud sync pre-check completed",
		zap.String("provider", "alicloud"),
		zap.String("summary", summary))
	return checkResult, nil
}

// CleanSpecificPeriodData 清理指定时间段的数据
func (s *BillService) CleanSpecificPeriodData(ctx context.Context, granularity, period string) error {
	tableName := s.GetTableName(granularity)

	var condition string
	switch strings.ToUpper(granularity) {
	case "DAILY":
		condition = fmt.Sprintf("billing_date = '%s'", period)
		logger.Info("Alibaba Cloud data cleanup preparing to clean daily table data",
			zap.String("provider", "alicloud"),
			zap.String("table_name", tableName),
			zap.String("date", period))
	case "MONTHLY":
		condition = fmt.Sprintf("billing_cycle = '%s'", period)
		logger.Info("Alibaba Cloud data cleanup preparing to clean monthly table data",
			zap.String("provider", "alicloud"),
			zap.String("table_name", tableName),
			zap.String("billing_cycle", period))
	default:
		return fmt.Errorf("unsupported granularity for period cleanup: %s", granularity)
	}

	return s.cleanTableDataWithCondition(ctx, tableName, condition)
}

// cleanTableDataWithCondition 按条件清理表数据的辅助方法
func (s *BillService) cleanTableDataWithCondition(ctx context.Context, tableName, condition string) error {
	// Build cleanup options
	opts := &clickhouse.CleanupOptions{
		Condition: condition,
		DryRun:    false, // 实际删除
	}

	// 执行清理
	result, err := s.chClient.EnhancedCleanTableData(ctx, tableName, opts)
	if err != nil {
		return fmt.Errorf("failed to clean table data: %w", err)
	}

	logger.Info("Alibaba Cloud data cleanup completed",
		zap.String("provider", "alicloud"),
		zap.String("table_name", tableName),
		zap.String("condition", condition),
		zap.Int64("deleted_rows", result.DeletedRows))

	return nil
}

// ExecuteIntelligentCleanupAndSync 执行智能清理和同步
// Maintains backward compatibility, delegates to new sync manager
func (s *BillService) ExecuteIntelligentCleanupAndSync(ctx context.Context, result *DataComparisonResult, syncOptions *SyncOptions) error {
	sm := NewSyncManager(s)
	params := &IntelligentSyncParams{
		Granularity:       result.Granularity,
		Period:            result.Period,
		EnablePreCheck:    false, // 已经做过检查
		EnableAutoCleanup: result.NeedCleanup,
		SyncOptions:       syncOptions,
	}
	_, err := sm.IntelligentSync(ctx, params)
	return err
}
