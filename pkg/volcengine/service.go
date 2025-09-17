package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"sort"
	"time"

	"go.uber.org/zap"
)

// billServiceImpl 实现 BillService 接口
type billServiceImpl struct {
	volcClient *Client
	chClient   *clickhouse.Client
	tableName  string
	config     *config.VolcEngineConfig
}

// NewBillService 创建账单服务实例
func NewBillService(volcConfig *config.VolcEngineConfig, chClient *clickhouse.Client) (BillService, error) {
	volcClient, err := NewClient(volcConfig)
	if err != nil {
		return nil, WrapError(err, "create volc client failed")
	}

	return &billServiceImpl{
		volcClient: volcClient,
		chClient:   chClient,
		tableName:  "volcengine_bill_details",
		config:     volcConfig,
	}, nil
}

// CreateBillTable 创建账单表
func (s *billServiceImpl) CreateBillTable(ctx context.Context) error {
	resolver := s.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(s.tableName)

	// 检查表是否已存在
	exists, err := s.chClient.TableExists(ctx, s.tableName)
	if err != nil {
		return WrapError(err, "failed to check table existence")
	}

	if exists {
		logger.Info("Volcengine bill table already exists, skipping creation",
			zap.String("provider", "volcengine"),
			zap.String("table_name", actualTableName))
		return nil
	}

	schema := s.getBillTableSchema()

	// 使用自动表名解析机制创建表
	if resolver.IsClusterEnabled() {
		logger.Info("Volcengine bill table creating distributed table structure in cluster mode",
			zap.String("provider", "volcengine"),
			zap.String("table_name", s.tableName))
		return s.chClient.CreateDistributedTableWithResolver(ctx, s.tableName, schema)
	} else {
		logger.Info("Volcengine bill table creating table in standalone mode",
			zap.String("provider", "volcengine"),
			zap.String("table_name", actualTableName))
		return s.chClient.CreateTable(ctx, s.tableName, schema)
	}
}

// getBillTableSchema 获取账单表的模式定义
func (s *billServiceImpl) getBillTableSchema() string {
	return `(
		-- 核心标识字段
		BillDetailId String,
		BillID String,
		InstanceNo String,
		
		-- 时间字段
		BillPeriod String,
		BusiPeriod String,
		ExpenseDate String,
		ExpenseBeginTime String,
		ExpenseEndTime String,
		TradeTime String,
		
		-- 用户信息字段
		PayerID String,
		PayerUserName String,
		PayerCustomerName String,
		SellerID String,
		SellerUserName String,
		SellerCustomerName String,
		OwnerID String,
		OwnerUserName String,
		OwnerCustomerName String,
		
		-- 产品信息字段
		Product String,
		ProductZh String,
		SolutionZh String,
		Element String,
		ElementCode String,
		Factor String,
		FactorCode String,
		
		-- 配置信息字段
		ConfigName String,
		ConfigurationCode String,
		InstanceName String,
		
		-- 地域信息字段
		Region String,
		RegionCode String,
		Zone String,
		ZoneCode String,
		CountryRegion String,
		
		-- 计费模式信息
		BillingMode String,
		BusinessMode String,
		BillingFunction String,
		BillingMethodCode String,
		SellingMode String,
		SettlementType String,
		
		-- 用量信息字段
		Count String,
		Unit String,
		UseDuration String,
		UseDurationUnit String,
		DeductionCount String,
		DeductionUseDuration String,
		
		-- 价格信息字段
		Price String,
		PriceUnit String,
		PriceInterval String,
		MarketPrice String,
		MeasureInterval String,
		Formula String,
		
		-- 金额信息字段
		OriginalBillAmount String,
		PreferentialBillAmount String,
		DiscountBillAmount String,
		RoundAmount Float64,
		PayableAmount String,
		PreTaxPayableAmount String,
		SettlePayableAmount String,
		SettlePreTaxPayableAmount String,
		PretaxAmount String,
		PosttaxAmount String,
		SettlePretaxAmount String,
		SettlePosttaxAmount String,
		Tax String,
		SettleTax String,
		TaxRate String,
		PaidAmount String,
		UnpaidAmount String,
		CreditCarriedAmount String,
		
		-- 实际价值和结算信息
		RealValue String,
		PretaxRealValue String,
		SettleRealValue String,
		SettlePretaxRealValue String,
		
		-- 优惠和抵扣信息
		CouponAmount String,
		DiscountInfo String,
		SavingPlanDeductionDiscountAmount String,
		SavingPlanDeductionSpID String,
		SavingPlanOriginalAmount String,
		ReservationInstance String,
		
		-- 货币信息
		Currency String,
		CurrencySettlement String,
		ExchangeRate String,
		
		-- 项目和分类信息
		Project String,
		ProjectDisplayName String,
		BillCategory String,
		SubjectName String,
		Tag String,
		
		-- 折扣相关业务信息
		DiscountBizBillingFunction String,
		DiscountBizMeasureInterval String,
		DiscountBizUnitPrice String,
		DiscountBizUnitPriceInterval String,
		
		-- 其他业务信息
		MainContractNumber String,
		OriginalOrderNo String,
		EffectiveFactor String,
		ExpandField String,
		
		-- 系统字段
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree
	PARTITION BY toYYYYMM(toDate(ExpenseDate))
	ORDER BY (BillPeriod, ExpenseDate, InstanceNo, ExpenseBeginTime, Product, ElementCode, PayableAmount)
	SETTINGS index_granularity = 8192`
}

// SyncBillData 同步账单数据
func (s *billServiceImpl) SyncBillData(ctx context.Context, req *ListBillDetailRequest) (*SyncResult, error) {
	return s.syncBillDataInternal(ctx, req, false, "")
}

// syncBillDataInternal 内部同步账单数据实现
func (s *billServiceImpl) syncBillDataInternal(ctx context.Context, req *ListBillDetailRequest, isDistributed bool, tableName string) (*SyncResult, error) {
	if tableName == "" {
		tableName = s.tableName
	}

	result := &SyncResult{StartTime: time.Now()}

	// 创建数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(s.getOptimalBatchSize())

	// 创建分页器
	paginator := s.createPaginator(processor)

	// 执行分页处理
	paginateResult, err := paginator.PaginateBillDetails(ctx, req)
	if err != nil {
		result.Error = err
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result, WrapError(err, "pagination processing failed")
	}

	// 填充结果
	result.TotalRecords = paginateResult.TotalRecords
	result.FetchedRecords = paginateResult.ProcessedRecords
	result.InsertedRecords = paginateResult.ProcessedRecords
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	s.logSyncResult(result, paginateResult.Errors)
	return result, nil
}

// syncAllBillDataInternal 内部同步所有账单数据实现
func (s *billServiceImpl) syncAllBillDataInternal(ctx context.Context, billPeriod, tableName string, isDistributed bool) (*SyncResult, error) {
	logger.Info("Volcengine starting bill data synchronization",
		zap.String("provider", "volcengine"),
		zap.String("bill_period", billPeriod),
		zap.String("table_name", tableName),
		zap.Bool("is_distributed", isDistributed))

	req := &ListBillDetailRequest{
		BillPeriod:  billPeriod,
		GroupPeriod: 1, // 默认按天分组
		GroupTerm:   1, // 分组条件
		IgnoreZero:  1, // 忽略零元账单
	}

	return s.syncBillDataInternal(ctx, req, isDistributed, tableName)
}

// performDataPreCheck 执行数据预检查
func (s *billServiceImpl) performDataPreCheck(ctx context.Context, billPeriod, tableName string, isDistributed bool) (*DataPreCheckResult, error) {
	result := &DataPreCheckResult{
		BillPeriod: billPeriod,
		NeedSync:   true,
	}

	// 检查数据库中是否已有数据
	exists, existingCount, err := s.CheckMonthlyDataExists(ctx, tableName, billPeriod)
	if err != nil {
		return result, WrapError(err, "failed to check existing data")
	}

	if exists && existingCount > 0 {
		result.DatabaseCount = existingCount
		result.NeedSync = false
		result.Reason = fmt.Sprintf("数据库中已存在 %d 条记录", existingCount)
		return result, nil
	}

	// 获取 API 数据数量
	apiCount, err := s.GetAPIDataCount(ctx, billPeriod)
	if err != nil {
		return result, WrapError(err, "failed to get API data count")
	}

	result.APICount = apiCount
	result.DatabaseCount = existingCount

	if apiCount == 0 {
		result.NeedSync = false
		result.Reason = "API 返回无数据"
	}

	return result, nil
}

// CheckMonthlyDataExists 检查月度数据是否存在
func (s *billServiceImpl) CheckMonthlyDataExists(ctx context.Context, tableName, billPeriod string) (bool, int64, error) {
	resolver := s.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(tableName)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE BillPeriod = ?", actualTableName)
	var count uint64 // 使用uint64来接收ClickHouse的UInt64

	rows, err := s.chClient.Query(ctx, query, billPeriod)
	if err != nil {
		return false, 0, err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&count)
	}
	if err != nil {
		return false, 0, err
	}

	return count > 0, int64(count), nil // 转换为int64返回
}

// GetAPIDataCount 获取API数据数量
func (s *billServiceImpl) GetAPIDataCount(ctx context.Context, billPeriod string) (int32, error) {
	req := &ListBillDetailRequest{
		BillPeriod:    billPeriod,
		Limit:         1,
		NeedRecordNum: 1,
		GroupTerm:     1, // 分组条件
		IgnoreZero:    1, // 忽略零元账单
	}

	resp, err := s.volcClient.ListBillDetail(ctx, req)
	if err != nil {
		return 0, err
	}

	return resp.Result.Total, nil
}

// createPaginator 创建分页器
func (s *billServiceImpl) createPaginator(processor DataProcessor) Paginator {
	config := &PaginatorConfig{
		BatchSize:      s.getOptimalBatchSize(),
		MaxRetries:     s.getMaxRetries(),
		RetryDelay:     s.getRetryDelay(),
		MaxConcurrency: 1, // 串行处理，避免并发冲突
	}

	paginator := NewBillPaginator(s.volcClient, processor, config)

	// 设置进度回调
	paginator.SetProgressCallback(func(current, total int, duration time.Duration) {
		percentage := float64(current) / float64(total) * 100
		logger.Debug("Volcengine sync progress",
			zap.String("provider", "volcengine"),
			zap.Int("current", current),
			zap.Int("total", total),
			zap.Float64("percentage", percentage),
			zap.Duration("duration", duration))
	})

	return paginator
}

// getOptimalBatchSize 获取最优批次大小
func (s *billServiceImpl) getOptimalBatchSize() int {
	if s.config.BatchSize > 0 {
		return s.config.BatchSize
	}
	return 50 // 默认值
}

// getMaxRetries 获取最大重试次数
func (s *billServiceImpl) getMaxRetries() int {
	if s.config.MaxRetries > 0 {
		return s.config.MaxRetries
	}
	return 3 // 默认值
}

// getRetryDelay 获取重试延迟
func (s *billServiceImpl) getRetryDelay() time.Duration {
	if s.config.RetryDelay > 0 {
		return time.Duration(s.config.RetryDelay) * time.Second
	}
	return 2 * time.Second // 默认值
}

// logSyncResult 记录同步结果
func (s *billServiceImpl) logSyncResult(result *SyncResult, errors []error) {
	if len(errors) > 0 {
		logger.Warn("Volcengine errors occurred during processing",
			zap.String("provider", "volcengine"),
			zap.Int("error_count", len(errors)))
		for i, err := range errors {
			logger.Warn("Volcengine processing error details",
				zap.String("provider", "volcengine"),
				zap.Int("error_index", i+1),
				zap.Error(err))
		}
	}

	logger.Info("Volcengine synchronization completed",
		zap.String("provider", "volcengine"),
		zap.Int("total_records", result.TotalRecords),
		zap.Int("inserted_records", result.InsertedRecords),
		zap.Duration("duration", result.Duration),
		zap.Int("error_count", len(errors)))
}

// GetAvailableBillPeriods 获取可用的账期列表
func (s *billServiceImpl) GetAvailableBillPeriods(ctx context.Context, maxMonths int) ([]string, error) {
	if maxMonths <= 0 {
		maxMonths = 12 // 默认12个月
	}

	var periods []string
	now := time.Now()

	for i := 0; i < maxMonths; i++ {
		period := now.AddDate(0, -i-1, 0) // 从上个月开始
		periodStr := period.Format("2006-01")

		// 验证账期格式
		if err := s.volcClient.ValidatePeriod(periodStr); err != nil {
			continue
		}

		periods = append(periods, periodStr)
	}

	// 按时间倒序排序（最新的在前）
	sort.Slice(periods, func(i, j int) bool {
		return periods[i] > periods[j]
	})

	return periods, nil
}

// BatchSync 批量同步多个账期数据
func (s *billServiceImpl) BatchSync(ctx context.Context, billPeriods []string, tableName string, isDistributed bool) ([]*SyncResult, error) {
	results := make([]*SyncResult, 0, len(billPeriods))

	for _, period := range billPeriods {
		logger.Info("Volcengine starting period synchronization",
			zap.String("provider", "volcengine"),
			zap.String("period", period))

		result, err := s.SmartSyncAllData(ctx, period, tableName, isDistributed)
		if err != nil {
			logger.Warn("Volcengine period synchronization failed",
				zap.String("provider", "volcengine"),
				zap.String("period", period),
				zap.Error(err))
			result = &SyncResult{
				StartTime: time.Now(),
				EndTime:   time.Now(),
				Error:     err,
			}
		}

		results = append(results, result)

		// 在批次之间添加短暂延迟，避免过于频繁的API调用
		select {
		case <-time.After(1 * time.Second):
		case <-ctx.Done():
			return results, ctx.Err()
		}
	}

	return results, nil
}
