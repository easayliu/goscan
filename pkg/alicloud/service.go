package alicloud

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"log"
	"strings"
	"time"
)

type BillService struct {
	aliClient        *Client
	chClient         *clickhouse.Client
	monthlyTableName string
	dailyTableName   string
	config           *config.AliCloudConfig
}

// NewBillService 创建阿里云账单服务
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

	// 如果配置中指定了表名，则使用配置的表名
	if aliConfig.MonthlyTable != "" {
		service.monthlyTableName = aliConfig.MonthlyTable
	}
	if aliConfig.DailyTable != "" {
		service.dailyTableName = aliConfig.DailyTable
	}

	return service, nil
}

// CreateMonthlyBillTable 创建按月账单表（支持自动表名解析）
func (s *BillService) CreateMonthlyBillTable(ctx context.Context) error {
	// 获取解析器，用于确定实际要检查和创建的表名
	resolver := s.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(s.monthlyTableName)
	
	// 检查表是否已存在
	exists, err := s.chClient.TableExists(ctx, s.monthlyTableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if exists {
		log.Printf("[阿里云按月表] 表 %s 已存在，跳过创建", actualTableName)
		return nil
	}

	schema := `(
		-- 核心标识字段
		instance_id String,
		instance_name String,
		bill_account_id String,
		bill_account_name String,
		
		-- 时间字段
		billing_date Nullable(Date), -- 只有DAILY粒度才有值
		billing_cycle String,
		
		-- 产品信息
		product_code String,
		product_name String,
		product_type String,
		product_detail String,
		
		-- 计费信息
		subscription_type String,
		pricing_unit String,
		currency String,
		billing_type String,
		
		-- 用量信息
		usage String,
		usage_unit String,
		
		-- 金额信息（核心）
		pretax_gross_amount Float64,
		invoice_discount Float64,
		deducted_by_coupons Float64,
		pretax_amount Float64,
		currency_amount Float64,
		payment_amount Float64,
		outstanding_amount Float64,
		
		-- 地域信息
		region String,
		zone String,
		
		-- 规格信息
		instance_spec String,
		internet_ip String,
		intranet_ip String,
		
		-- 标签和分组
		resource_group String,
		tags Map(String, String),
		cost_unit String,
		
		-- 其他信息
		service_period String,
		service_period_unit String,
		list_price String,
		list_price_unit String,
		owner_id String,
		
		-- 成本分摊
		split_item_id String,
		split_item_name String,
		split_account_id String,
		split_account_name String,
		
		-- 订单信息
		nick_name String,
		product_detail_code String,
		
		-- 账单归属
		biz_type String,
		adjust_type String,
		adjust_amount Float64,
		
		-- 元数据
		granularity String DEFAULT 'MONTHLY',
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (billing_cycle, product_code, instance_id, bill_account_id, subscription_type, payment_amount)
	PARTITION BY toYYYYMM(parseDateTimeBestEffort(billing_cycle || '-01'))`

	// 使用自动表名解析机制创建表
	if resolver.IsClusterEnabled() {
		// 集群模式：创建完整的分布式表结构（本地表+分布式表）
		log.Printf("[阿里云按月表] 在集群模式下创建分布式表结构，基础表名: %s", s.monthlyTableName)
		return s.chClient.CreateDistributedTableWithResolver(ctx, s.monthlyTableName, schema)
	} else {
		// 单机模式：直接创建表
		log.Printf("[阿里云按月表] 在单机模式下创建表: %s", actualTableName)
		return s.chClient.CreateTable(ctx, s.monthlyTableName, schema)
	}
}

// CreateDailyBillTable 创建按天账单表（支持自动表名解析）
func (s *BillService) CreateDailyBillTable(ctx context.Context) error {
	// 获取解析器，用于确定实际要检查和创建的表名
	resolver := s.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(s.dailyTableName)
	
	// 检查表是否已存在
	exists, err := s.chClient.TableExists(ctx, s.dailyTableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if exists {
		log.Printf("[阿里云按天表] 表 %s 已存在，跳过创建", actualTableName)
		return nil
	}

	schema := `(
		-- 核心标识字段
		instance_id String,
		instance_name String,
		bill_account_id String,
		bill_account_name String,
		
		-- 时间字段（按天）
		billing_date Date, -- DAILY粒度必有值
		billing_cycle String,
		
		-- 产品信息
		product_code String,
		product_name String,
		product_type String,
		product_detail String,
		
		-- 计费信息
		subscription_type String,
		pricing_unit String,
		currency String,
		billing_type String,
		
		-- 用量信息
		usage String,
		usage_unit String,
		
		-- 金额信息（核心）
		pretax_gross_amount Float64,
		invoice_discount Float64,
		deducted_by_coupons Float64,
		pretax_amount Float64,
		currency_amount Float64,
		payment_amount Float64,
		outstanding_amount Float64,
		
		-- 地域信息
		region String,
		zone String,
		
		-- 规格信息
		instance_spec String,
		internet_ip String,
		intranet_ip String,
		
		-- 标签和分组
		resource_group String,
		tags Map(String, String),
		cost_unit String,
		
		-- 其他信息
		service_period String,
		service_period_unit String,
		list_price String,
		list_price_unit String,
		owner_id String,
		
		-- 成本分摊
		split_item_id String,
		split_item_name String,
		split_account_id String,
		split_account_name String,
		
		-- 订单信息
		nick_name String,
		product_detail_code String,
		
		-- 账单归属
		biz_type String,
		adjust_type String,
		adjust_amount Float64,
		
		-- 元数据
		granularity String DEFAULT 'DAILY',
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (billing_date, product_code, instance_id, bill_account_id, subscription_type, payment_amount)
	PARTITION BY toYYYYMMDD(billing_date)`

	// 使用自动表名解析机制创建表
	if resolver.IsClusterEnabled() {
		// 集群模式：创建完整的分布式表结构（本地表+分布式表）
		log.Printf("[阿里云按天表] 在集群模式下创建分布式表结构，基础表名: %s", s.dailyTableName)
		return s.chClient.CreateDistributedTableWithResolver(ctx, s.dailyTableName, schema)
	} else {
		// 单机模式：直接创建表
		log.Printf("[阿里云按天表] 在单机模式下创建表: %s", actualTableName)
		return s.chClient.CreateTable(ctx, s.dailyTableName, schema)
	}
}

// CreateDistributedMonthlyBillTable 创建分布式按月账单表
func (s *BillService) CreateDistributedMonthlyBillTable(ctx context.Context, localTableName, distributedTableName string) error {
	// 检查分布式表是否已存在
	distributedExists, err := s.chClient.TableExists(ctx, distributedTableName)
	if err != nil {
		return fmt.Errorf("failed to check distributed table existence: %w", err)
	}

	// 检查本地表是否已存在
	localExists, err := s.chClient.TableExists(ctx, localTableName)
	if err != nil {
		return fmt.Errorf("failed to check local table existence: %w", err)
	}

	if distributedExists && localExists {
		log.Printf("[阿里云分布式表] 按月表 %s -> %s 已存在，跳过创建", localTableName, distributedTableName)
		return nil
	}

	if distributedExists || localExists {
		log.Printf("[阿里云分布式表] 警告：按月表部分存在（本地表: %v, 分布式表: %v），继续创建缺失的表", localExists, distributedExists)
	}

	// 获取本地表的schema（使用ReplacingMergeTree引擎）
	localSchema := s.getMonthlyTableSchema()

	// 创建分布式表（包括本地表和分布式表）
	if err := s.chClient.CreateDistributedTable(ctx, localTableName, distributedTableName, localSchema); err != nil {
		return fmt.Errorf("failed to create distributed monthly table: %w", err)
	}

	log.Printf("[阿里云分布式表] 按月表创建完成: %s -> %s", localTableName, distributedTableName)
	return nil
}

// CreateDistributedDailyBillTable 创建分布式按天账单表
func (s *BillService) CreateDistributedDailyBillTable(ctx context.Context, localTableName, distributedTableName string) error {
	// 检查分布式表是否已存在
	distributedExists, err := s.chClient.TableExists(ctx, distributedTableName)
	if err != nil {
		return fmt.Errorf("failed to check distributed table existence: %w", err)
	}

	// 检查本地表是否已存在
	localExists, err := s.chClient.TableExists(ctx, localTableName)
	if err != nil {
		return fmt.Errorf("failed to check local table existence: %w", err)
	}

	if distributedExists && localExists {
		log.Printf("[阿里云分布式表] 按天表 %s -> %s 已存在，跳过创建", localTableName, distributedTableName)
		return nil
	}

	if distributedExists || localExists {
		log.Printf("[阿里云分布式表] 警告：按天表部分存在（本地表: %v, 分布式表: %v），继续创建缺失的表", localExists, distributedExists)
	}

	// 获取本地表的schema（使用ReplacingMergeTree引擎）
	localSchema := s.getDailyTableSchema()

	// 创建分布式表（包括本地表和分布式表）
	if err := s.chClient.CreateDistributedTable(ctx, localTableName, distributedTableName, localSchema); err != nil {
		return fmt.Errorf("failed to create distributed daily table: %w", err)
	}

	log.Printf("[阿里云分布式表] 按天表创建完成: %s -> %s", localTableName, distributedTableName)
	return nil
}

// getMonthlyTableSchema 获取按月表结构（用于分布式表）
func (s *BillService) getMonthlyTableSchema() string {
	return `(
		instance_id String,
		instance_name String,
		bill_account_id String,
		bill_account_name String,
		billing_date Nullable(Date), -- 月表为NULL
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

// getDailyTableSchema 获取按天表结构（用于分布式表）
func (s *BillService) getDailyTableSchema() string {
	return `(
		instance_id String,
		instance_name String,
		bill_account_id String,
		bill_account_name String,
		billing_date Date, -- 天表必须有值
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

// SyncMonthlyBillData 同步按月账单数据
func (s *BillService) SyncMonthlyBillData(ctx context.Context, billingCycle string, options *SyncOptions) error {
	log.Printf("[阿里云按月同步] 开始同步账期: %s", billingCycle)

	// 验证账期
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return fmt.Errorf("invalid billing cycle: %w", err)
	}

	// 创建分页器
	paginator := NewPaginator(s.aliClient, &DescribeInstanceBillRequest{
		BillingCycle: billingCycle,
		Granularity:  "MONTHLY",
		MaxResults:   int32(s.config.BatchSize),
	})

	// 创建数据处理器
	processor := NewProcessor(s.chClient, options)

	// 获取目标表名
	tableName := s.monthlyTableName
	if options != nil && options.UseDistributed && options.DistributedTableName != "" {
		tableName = options.DistributedTableName
	}

	// 开始同步
	totalRecords := 0
	for {
		// 获取下一批数据
		response, err := paginator.Next(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch data: %w", err)
		}

		if len(response.Data.Items) == 0 {
			break // 没有更多数据
		}

		// 批量处理数据
		if err := processor.ProcessBatchWithBillingCycle(ctx, tableName, response.Data.Items, response.Data.BillingCycle); err != nil {
			return fmt.Errorf("failed to process batch: %w", err)
		}

		totalRecords += len(response.Data.Items)
		log.Printf("[阿里云按月同步] 已同步 %d 条记录", totalRecords)

		// 检查是否还有更多数据
		if response.Data.NextToken == "" {
			break
		}
	}

	log.Printf("[阿里云按月同步] 账期 %s 同步完成，共同步 %d 条记录", billingCycle, totalRecords)
	return nil
}

// SyncDailyBillData 同步按天账单数据
func (s *BillService) SyncDailyBillData(ctx context.Context, billingCycle string, options *SyncOptions) error {
	log.Printf("[阿里云按天同步] 开始同步账期: %s", billingCycle)

	// 验证账期
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return fmt.Errorf("invalid billing cycle: %w", err)
	}

	// 生成该月份的所有日期
	dates, err := GenerateDatesInMonth(billingCycle)
	if err != nil {
		return fmt.Errorf("failed to generate dates for cycle %s: %w", billingCycle, err)
	}

	log.Printf("[阿里云按天同步] 账期 %s 包含 %d 天", billingCycle, len(dates))

	// 创建数据处理器
	processor := NewProcessor(s.chClient, options)

	// 获取目标表名
	tableName := s.dailyTableName
	if options != nil && options.UseDistributed && options.DistributedTableName != "" {
		tableName = options.DistributedTableName
	}

	totalRecords := 0

	// 按天循环获取数据
	for i, date := range dates {
		log.Printf("[阿里云按天同步] 同步日期 %s (%d/%d)", date, i+1, len(dates))

		// 创建分页器（每天的数据）
		paginator := NewPaginator(s.aliClient, &DescribeInstanceBillRequest{
			BillingCycle: billingCycle,
			Granularity:  "DAILY",
			BillingDate:  date,
			MaxResults:   int32(s.config.BatchSize),
		})

		dayRecords := 0

		// 分页获取该天的所有数据
		for {
			response, err := paginator.Next(ctx)
			if err != nil {
				log.Printf("[阿里云按天同步] 日期 %s 同步失败: %v", date, err)
				break // 跳过这一天，继续下一天
			}

			if len(response.Data.Items) == 0 {
				break // 这一天没有数据
			}

			// 批量处理数据
			if err := processor.ProcessBatchWithBillingCycle(ctx, tableName, response.Data.Items, response.Data.BillingCycle); err != nil {
				log.Printf("[阿里云按天同步] 日期 %s 数据处理失败: %v", date, err)
				break // 跳过这一天的剩余数据
			}

			dayRecords += len(response.Data.Items)

			// 检查是否还有更多数据
			if response.Data.NextToken == "" {
				break
			}
		}

		totalRecords += dayRecords
		if dayRecords > 0 {
			log.Printf("[阿里云按天同步] 日期 %s 同步完成，%d 条记录", date, dayRecords)
		}

		// 简单的延迟，避免API调用过于频繁
		if i < len(dates)-1 {
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	log.Printf("[阿里云按天同步] 账期 %s 同步完成，共同步 %d 条记录", billingCycle, totalRecords)
	return nil
}

// SyncSpecificDayBillData 同步指定日期的天表数据
func (s *BillService) SyncSpecificDayBillData(ctx context.Context, billingDate string, options *SyncOptions) error {
	log.Printf("[阿里云指定日期同步] 开始同步日期: %s", billingDate)

	// 验证日期格式
	date, err := time.Parse("2006-01-02", billingDate)
	if err != nil {
		return fmt.Errorf("invalid billing date format (expected YYYY-MM-DD): %w", err)
	}

	// 获取账期（YYYY-MM格式）
	billingCycle := date.Format("2006-01")

	// 验证账期
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return fmt.Errorf("invalid billing cycle: %w", err)
	}

	// 创建分页器（指定日期的数据）
	paginator := NewPaginator(s.aliClient, &DescribeInstanceBillRequest{
		BillingCycle: billingCycle,
		Granularity:  "DAILY",
		BillingDate:  billingDate,
		MaxResults:   int32(s.config.BatchSize),
	})

	// 创建数据处理器
	processor := NewProcessor(s.chClient, options)

	// 获取目标表名
	tableName := s.dailyTableName
	if options != nil && options.UseDistributed && options.DistributedTableName != "" {
		tableName = options.DistributedTableName
	}

	totalRecords := 0

	// 分页获取指定日期的所有数据
	for {
		response, err := paginator.Next(ctx)
		if err != nil {
			return fmt.Errorf("failed to fetch data for date %s: %w", billingDate, err)
		}

		if len(response.Data.Items) == 0 {
			break // 这个日期没有数据
		}

		// 批量处理数据
		if err := processor.ProcessBatchWithBillingCycle(ctx, tableName, response.Data.Items, response.Data.BillingCycle); err != nil {
			return fmt.Errorf("failed to process batch for date %s: %w", billingDate, err)
		}

		totalRecords += len(response.Data.Items)
		log.Printf("[阿里云指定日期同步] 已同步 %d 条记录", totalRecords)

		// 检查是否还有更多数据
		if response.Data.NextToken == "" {
			break
		}
	}

	log.Printf("[阿里云指定日期同步] 日期 %s 同步完成，共同步 %d 条记录", billingDate, totalRecords)
	return nil
}

// SyncBothGranularityData 同步两种粒度的数据
func (s *BillService) SyncBothGranularityData(ctx context.Context, billingCycle string, options *SyncOptions) error {
	log.Printf("[阿里云双粒度同步] 开始同步账期: %s", billingCycle)

	// 先同步按月数据
	monthlyOptions := *options
	if options.UseDistributed {
		monthlyOptions.DistributedTableName = strings.Replace(options.DistributedTableName, "daily", "monthly", 1)
	}

	if err := s.SyncMonthlyBillData(ctx, billingCycle, &monthlyOptions); err != nil {
		return fmt.Errorf("failed to sync monthly data: %w", err)
	}

	// 再同步按天数据
	dailyOptions := *options
	if options.UseDistributed {
		dailyOptions.DistributedTableName = strings.Replace(options.DistributedTableName, "monthly", "daily", 1)
	}

	if err := s.SyncDailyBillData(ctx, billingCycle, &dailyOptions); err != nil {
		return fmt.Errorf("failed to sync daily data: %w", err)
	}

	log.Printf("[阿里云双粒度同步] 账期 %s 双粒度同步完成", billingCycle)
	return nil
}

// CleanBillData 清理账单数据
func (s *BillService) CleanBillData(ctx context.Context, granularity string, condition string, dryRun bool) error {
	var tableName string
	switch strings.ToUpper(granularity) {
	case "MONTHLY":
		tableName = s.monthlyTableName
	case "DAILY":
		tableName = s.dailyTableName
	case "BOTH":
		// 清理两张表
		if err := s.CleanBillData(ctx, "MONTHLY", condition, dryRun); err != nil {
			return err
		}
		return s.CleanBillData(ctx, "DAILY", condition, dryRun)
	default:
		return fmt.Errorf("unsupported granularity: %s", granularity)
	}

	log.Printf("[阿里云数据清理] 表: %s, 条件: %s, 预览模式: %v", tableName, condition, dryRun)

	// 构建清理选项
	opts := &clickhouse.CleanupOptions{
		Condition: condition,
		DryRun:    dryRun,
	}

	// 预览或执行清理
	result, err := s.chClient.EnhancedCleanTableData(ctx, tableName, opts)
	if err != nil {
		return fmt.Errorf("failed to clean table data: %w", err)
	}

	if dryRun {
		log.Printf("[阿里云数据清理预览] 表: %s, 预计清理记录数: %d", tableName, result.PreviewRows)
	} else {
		log.Printf("[阿里云数据清理完成] 表: %s, 实际清理记录数: %d", tableName, result.DeletedRows)
	}

	return nil
}

// DropTable 删除表（支持分布式表）
func (s *BillService) DropTable(ctx context.Context, granularity string) error {
	var tableName string
	switch strings.ToUpper(granularity) {
	case "MONTHLY":
		tableName = s.monthlyTableName
	case "DAILY":
		tableName = s.dailyTableName
	case "BOTH":
		// 删除两张表
		if err := s.DropTable(ctx, "MONTHLY"); err != nil {
			log.Printf("[阿里云删表] 按月表删除失败: %v", err)
		}
		return s.DropTable(ctx, "DAILY")
	default:
		return fmt.Errorf("unsupported granularity: %s", granularity)
	}

	log.Printf("[阿里云删表] 删除表: %s", tableName)

	// 检查是否为分布式表
	if s.isDistributedTable(tableName) {
		return s.dropDistributedTable(ctx, tableName)
	}

	// 普通表删除
	return s.chClient.DropTable(ctx, tableName)
}

// GetAvailableBillingCycles 获取可用的账期列表
func (s *BillService) GetAvailableBillingCycles(ctx context.Context) ([]string, error) {
	return s.aliClient.GetAvailableBillingCycles(ctx)
}

// TestConnection 测试连接
func (s *BillService) TestConnection(ctx context.Context) error {
	return s.aliClient.TestConnection(ctx)
}

// Close 关闭服务
func (s *BillService) Close() error {
	if s.aliClient != nil {
		return s.aliClient.Close()
	}
	return nil
}

// SyncOptions 同步选项
type SyncOptions struct {
	BatchSize            int                        // 批次大小
	UseDistributed       bool                       // 是否使用分布式表
	DistributedTableName string                     // 分布式表名
	SkipZeroAmount       bool                       // 是否跳过零金额记录
	EnableValidation     bool                       // 是否启用数据验证
	MaxWorkers           int                        // 最大工作协程数
	ProgressCallback     func(processed, total int) // 进度回调
}

// DefaultSyncOptions 默认同步选项
func DefaultSyncOptions() *SyncOptions {
	return &SyncOptions{
		BatchSize:        1000,
		UseDistributed:   false,
		SkipZeroAmount:   false,
		EnableValidation: true,
		MaxWorkers:       4,
	}
}

// GetTableName 根据粒度获取表名
func (s *BillService) GetTableName(granularity string) string {
	switch strings.ToUpper(granularity) {
	case "DAILY":
		return s.dailyTableName
	case "MONTHLY":
		return s.monthlyTableName
	default:
		return s.monthlyTableName // 默认返回按月表
	}
}

// GetMonthlyTableName 获取按月表名
func (s *BillService) GetMonthlyTableName() string {
	return s.monthlyTableName
}

// GetDailyTableName 获取按天表名
func (s *BillService) GetDailyTableName() string {
	return s.dailyTableName
}

// SetDistributedTableNames 设置分布式表名
func (s *BillService) SetDistributedTableNames(monthlyDistributedTable, dailyDistributedTable string) {
	s.monthlyTableName = monthlyDistributedTable
	s.dailyTableName = dailyDistributedTable
}

// SetTableNames 设置表名（用于自定义表名）
func (s *BillService) SetTableNames(monthlyTable, dailyTable string) {
	if monthlyTable != "" {
		s.monthlyTableName = monthlyTable
	}
	if dailyTable != "" {
		s.dailyTableName = dailyTable
	}
}

// isDistributedTable 检查是否为分布式表
func (s *BillService) isDistributedTable(tableName string) bool {
	return strings.HasSuffix(tableName, "_distributed")
}

// dropDistributedTable 删除分布式表（包括本地表和分布式表）
func (s *BillService) dropDistributedTable(ctx context.Context, distributedTableName string) error {
	log.Printf("[阿里云分布式删表] 开始删除分布式表: %s", distributedTableName)

	// 生成本地表名（去掉 _distributed 后缀加上 _local）
	localTableName := strings.TrimSuffix(distributedTableName, "_distributed") + "_local"

	// 先删除分布式表
	log.Printf("[阿里云分布式删表] 删除分布式表: %s", distributedTableName)
	if err := s.chClient.DropTable(ctx, distributedTableName); err != nil {
		log.Printf("[阿里云分布式删表] 分布式表删除失败: %v", err)
		// 继续尝试删除本地表
	}

	// 再删除本地表（使用 ON CLUSTER）
	log.Printf("[阿里云分布式删表] 删除本地表: %s", localTableName)
	if err := s.chClient.DropDistributedTable(ctx, localTableName, distributedTableName); err != nil {
		return fmt.Errorf("failed to drop local table %s: %w", localTableName, err)
	}

	log.Printf("[阿里云分布式删表] 分布式表删除完成: %s", distributedTableName)
	return nil
}

// DropOldTable 删除指定的旧表（支持分布式表）
func (s *BillService) DropOldTable(ctx context.Context, tableName string) error {
	log.Printf("[阿里云删除旧表] 删除表: %s", tableName)

	// 检查是否为分布式表
	if s.isDistributedTable(tableName) {
		return s.dropDistributedTable(ctx, tableName)
	}

	// 普通表删除
	return s.chClient.DropTable(ctx, tableName)
}

// CheckDailyDataExists 检查天表指定日期的数据是否存在
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

// CheckMonthlyDataExists 检查月表指定账期的数据是否存在
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

// CheckDailyDataExistsWithOptimize 检查天表指定日期的数据是否存在（强制优化后再检查）
func (s *BillService) CheckDailyDataExistsWithOptimize(ctx context.Context, tableName, billingDate string) (bool, int64, error) {
	// 对于ReplacingMergeTree，先执行OPTIMIZE FINAL强制去重
	if strings.Contains(tableName, "_distributed") {
		// 分布式表优化本地表
		localTableName := strings.Replace(tableName, "_distributed", "_local", 1)
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s ON CLUSTER %s FINAL", localTableName, s.chClient.GetClusterName())
		log.Printf("[ReplacingMergeTree优化] 执行强制去重: %s", optimizeQuery)
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			log.Printf("[ReplacingMergeTree优化] 执行失败，继续查询: %v", err)
		}
	} else {
		// 普通表直接优化
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
		log.Printf("[ReplacingMergeTree优化] 执行强制去重: %s", optimizeQuery)
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			log.Printf("[ReplacingMergeTree优化] 执行失败，继续查询: %v", err)
		}
	}

	// 优化后再查询数据量
	return s.CheckDailyDataExists(ctx, tableName, billingDate)
}

// CheckMonthlyDataExistsWithOptimize 检查月表指定账期的数据是否存在（强制优化后再检查）
func (s *BillService) CheckMonthlyDataExistsWithOptimize(ctx context.Context, tableName, billingCycle string) (bool, int64, error) {
	// 对于ReplacingMergeTree，先执行OPTIMIZE FINAL强制去重
	if strings.Contains(tableName, "_distributed") {
		// 分布式表优化本地表
		localTableName := strings.Replace(tableName, "_distributed", "_local", 1)
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s ON CLUSTER %s FINAL", localTableName, s.chClient.GetClusterName())
		log.Printf("[ReplacingMergeTree优化] 执行强制去重: %s", optimizeQuery)
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			log.Printf("[ReplacingMergeTree优化] 执行失败，继续查询: %v", err)
		}
	} else {
		// 普通表直接优化
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
		log.Printf("[ReplacingMergeTree优化] 执行强制去重: %s", optimizeQuery)
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			log.Printf("[ReplacingMergeTree优化] 执行失败，继续查询: %v", err)
		}
	}

	// 优化后再查询数据量
	return s.CheckMonthlyDataExists(ctx, tableName, billingCycle)
}

// GetDailyAPIDataCount 获取指定日期的API数据总量
func (s *BillService) GetDailyAPIDataCount(ctx context.Context, billingDate string) (int32, error) {
	log.Printf("[阿里云API数据量检查] 开始获取日期 %s 的API数据总量", billingDate)

	// 验证日期格式
	date, err := time.Parse("2006-01-02", billingDate)
	if err != nil {
		return 0, fmt.Errorf("invalid billing date format (expected YYYY-MM-DD): %w", err)
	}

	// 获取账期（YYYY-MM格式）
	billingCycle := date.Format("2006-01")

	// 验证账期
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return 0, fmt.Errorf("invalid billing cycle: %w", err)
	}

	// 创建分页器（指定日期的数据）
	paginator := NewPaginator(s.aliClient, &DescribeInstanceBillRequest{
		BillingCycle: billingCycle,
		Granularity:  "DAILY",
		BillingDate:  billingDate,
		MaxResults:   int32(s.config.BatchSize),
	})

	// 估算总记录数
	totalCount, err := paginator.EstimateTotal(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to estimate API data count: %w", err)
	}

	log.Printf("[阿里云API数据量检查] 日期 %s 的API数据总量: %d 条", billingDate, totalCount)
	return totalCount, nil
}

// GetMonthlyAPIDataCount 获取指定月份的API数据总量
func (s *BillService) GetMonthlyAPIDataCount(ctx context.Context, billingCycle string) (int32, error) {
	log.Printf("[阿里云API数据量检查] 开始获取账期 %s 的API数据总量", billingCycle)

	// 验证账期
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return 0, fmt.Errorf("invalid billing cycle: %w", err)
	}

	// 创建分页器
	paginator := NewPaginator(s.aliClient, &DescribeInstanceBillRequest{
		BillingCycle: billingCycle,
		Granularity:  "MONTHLY",
		MaxResults:   int32(s.config.BatchSize),
	})

	// 估算总记录数
	totalCount, err := paginator.EstimateTotal(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to estimate API data count: %w", err)
	}

	log.Printf("[阿里云API数据量检查] 账期 %s 的API数据总量: %d 条", billingCycle, totalCount)
	return totalCount, nil
}

// CleanSpecificTableData 清理指定表的数据（用于分布式表的本地表清理）
func (s *BillService) CleanSpecificTableData(ctx context.Context, tableName, condition string, dryRun bool) error {
	log.Printf("[阿里云数据清理] 表: %s, 条件: %s, 预览模式: %v", tableName, condition, dryRun)
	log.Printf("[阿里云数据清理] 分布式表本地清理模式，将检查分区删除逻辑")

	// 构建清理选项
	opts := &clickhouse.CleanupOptions{
		Condition: condition,
		DryRun:    dryRun,
	}

	// 预览或执行清理
	result, err := s.chClient.EnhancedCleanTableData(ctx, tableName, opts)
	if err != nil {
		return fmt.Errorf("failed to clean table data: %w", err)
	}

	if dryRun {
		log.Printf("[阿里云数据清理预览] 表: %s, 预计清理记录数: %d", tableName, result.PreviewRows)
	} else {
		log.Printf("[阿里云数据清理完成] 表: %s, 实际清理记录数: %d", tableName, result.DeletedRows)
	}

	return nil
}

// DataComparisonResult 数据比较结果
type DataComparisonResult struct {
	APICount      int32  // API返回的数据总数
	DatabaseCount int64  // 数据库中的记录总数
	NeedSync      bool   // 是否需要同步
	NeedCleanup   bool   // 是否需要先清理数据
	Reason        string // 决策原因
	Period        string // 时间段
	Granularity   string // 粒度（daily/monthly）
}

// PreSyncCheckResult 同步前检查结果
type PreSyncCheckResult struct {
	ShouldSkip bool                    // 是否跳过同步
	Results    []*DataComparisonResult // 比较结果列表（支持多个时间段）
	Summary    string                  // 检查摘要
}

// GetBillDataCount 获取账单数据总数（通用方法）
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

// GetDatabaseRecordCount 获取数据库记录总数（通用方法）
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

// PerformDataComparison 执行数据比较（决定是否需要同步）
func (s *BillService) PerformDataComparison(ctx context.Context, granularity, period string) (*DataComparisonResult, error) {
	log.Printf("[阿里云数据预检查] 开始比较 %s %s 的数据量", granularity, period)

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
		// 双方都无数据
		result.NeedSync = false
		result.NeedCleanup = false
		result.Reason = "API和数据库都无数据，跳过同步"
	} else if apiCount == 0 && dbCount > 0 {
		// API无数据但数据库有数据，保持现状
		result.NeedSync = false
		result.NeedCleanup = false
		result.Reason = "API无数据但数据库有数据，跳过同步"
	} else if apiCount > 0 && dbCount == 0 {
		// API有数据但数据库无数据，需要同步但不需要清理
		result.NeedSync = true
		result.NeedCleanup = false
		result.Reason = fmt.Sprintf("数据库为空但API有%d条数据，需要同步", apiCount)
	} else if apiCount > 0 && int64(apiCount) == dbCount {
		// 数据量一致，跳过同步
		result.NeedSync = false
		result.NeedCleanup = false
		result.Reason = "API数据量与数据库记录数一致，跳过同步"
	} else {
		// 数据量不一致，需要先清理再同步
		result.NeedSync = true
		result.NeedCleanup = true
		result.Reason = fmt.Sprintf("数据量不一致(API:%d vs DB:%d)，需要先清理再同步", apiCount, dbCount)
	}

	log.Printf("[阿里云数据预检查] %s %s - API:%d, 数据库:%d, 需要同步:%v, 需要清理:%v, 结果:%s",
		granularity, period, apiCount, dbCount, result.NeedSync, result.NeedCleanup, result.Reason)

	return result, nil
}

// PerformPreSyncCheck 执行同步前的数据预检查
func (s *BillService) PerformPreSyncCheck(ctx context.Context, granularity, period string) (*PreSyncCheckResult, error) {
	log.Printf("[阿里云同步预检查] 开始执行数据预检查: %s %s", granularity, period)

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

	// 判断是否应该跳过同步（所有检查都不需要同步时才跳过）
	shouldSkip := true
	for _, result := range results {
		if result.NeedSync {
			shouldSkip = false
			break
		}
	}

	// 生成检查摘要
	summary := fmt.Sprintf("预检查完成: 共检查 %d 个时间段", len(results))
	if shouldSkip {
		summary += ", 所有数据已是最新，跳过同步"
	} else {
		summary += ", 检测到数据差异，需要执行同步"
	}

	checkResult := &PreSyncCheckResult{
		ShouldSkip: shouldSkip,
		Results:    results,
		Summary:    summary,
	}

	log.Printf("[阿里云同步预检查] %s", summary)
	return checkResult, nil
}

// CleanSpecificPeriodData 清理指定时间段的数据
func (s *BillService) CleanSpecificPeriodData(ctx context.Context, granularity, period string) error {
	tableName := s.GetTableName(granularity)

	var condition string
	switch strings.ToUpper(granularity) {
	case "DAILY":
		// 按天清理：清理特定日期的数据
		condition = fmt.Sprintf("billing_date = '%s'", period)
		log.Printf("[阿里云数据清理] 准备清理天表数据：%s, 日期:%s", tableName, period)
	case "MONTHLY":
		// 按月清理：清理特定账期的数据
		condition = fmt.Sprintf("billing_cycle = '%s'", period)
		log.Printf("[阿里云数据清理] 准备清理月表数据：%s, 账期:%s", tableName, period)
	default:
		return fmt.Errorf("unsupported granularity for period cleanup: %s", granularity)
	}

	// 构建清理选项
	opts := &clickhouse.CleanupOptions{
		Condition: condition,
		DryRun:    false, // 实际删除
	}

	// 执行清理
	result, err := s.chClient.EnhancedCleanTableData(ctx, tableName, opts)
	if err != nil {
		return fmt.Errorf("failed to clean %s data for period %s: %w", granularity, period, err)
	}

	log.Printf("[阿里云数据清理完成] 表:%s, 时间段:%s, 清理记录数:%d",
		tableName, period, result.DeletedRows)

	return nil
}

// ExecuteIntelligentCleanupAndSync 执行智能清理和同步
func (s *BillService) ExecuteIntelligentCleanupAndSync(ctx context.Context, result *DataComparisonResult, syncOptions *SyncOptions) error {
	if !result.NeedSync {
		// 不需要同步，直接返回
		return nil
	}

	if result.NeedCleanup {
		// 需要先清理数据
		log.Printf("[阿里云智能同步] 检测到数据不一致，先清理 %s %s 的数据",
			result.Granularity, result.Period)

		if err := s.CleanSpecificPeriodData(ctx, result.Granularity, result.Period); err != nil {
			return fmt.Errorf("failed to clean data before sync: %w", err)
		}

		log.Printf("[阿里云智能同步] 数据清理完成，开始同步新数据")
	}

	// 执行同步
	switch strings.ToUpper(result.Granularity) {
	case "DAILY":
		return s.SyncSpecificDayBillData(ctx, result.Period, syncOptions)
	case "MONTHLY":
		return s.SyncMonthlyBillData(ctx, result.Period, syncOptions)
	default:
		return fmt.Errorf("unsupported granularity for sync: %s", result.Granularity)
	}
}
