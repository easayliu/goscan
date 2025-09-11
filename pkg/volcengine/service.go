package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"log"
	"time"
)

type BillService struct {
	volcClient *Client
	chClient   *clickhouse.Client
	tableName  string
}

func NewBillService(volcConfig *config.VolcEngineConfig, chClient *clickhouse.Client) (*BillService, error) {
	volcClient := NewClient(volcConfig)

	return &BillService{
		volcClient: volcClient,
		chClient:   chClient,
		tableName:  "volcengine_bill_details",
	}, nil
}

func (s *BillService) CreateBillTable(ctx context.Context) error {
	schema := `(
		-- 核心标识字段
		bill_detail_id String,
		bill_id String,
		instance_no String,
		
		-- 账期和时间字段  
		bill_period String,
		busi_period String,
		expense_date String,
		expense_begin_time String,
		expense_end_time String,
		trade_time String,
		
		-- 产品和服务信息
		product String,
		product_zh String,
		solution_zh String,
		element String,
		element_code String,
		factor String,
		factor_code String,
		
		-- 配置信息
		config_name String,
		configuration_code String,
		instance_name String,
		
		-- 地域信息
		region String,
		region_code String,
		zone String,
		zone_code String,
		country_region String,
		
		-- 用量和计费信息
		count String,
		unit String,
		use_duration String,
		use_duration_unit String,
		deduction_count String,
		deduction_use_duration String,
		
		-- 价格信息
		price String,
		price_unit String,
		price_interval String,
		market_price String,
		formula String,
		measure_interval String,
		
		-- 金额信息（核心）
		original_bill_amount String,
		preferential_bill_amount String,
		discount_bill_amount String,
		round_amount Float64,
		
		-- 实际价值和结算信息
		real_value String,
		pretax_real_value String,
		settle_real_value String,
		settle_pretax_real_value String,
		
		-- 应付金额信息
		payable_amount String,
		pre_tax_payable_amount String,
		settle_payable_amount String,
		settle_pre_tax_payable_amount String,
		
		-- 税费信息
		pretax_amount String,
		posttax_amount String,
		settle_pretax_amount String,
		settle_posttax_amount String,
		tax String,
		settle_tax String,
		tax_rate String,
		
		-- 付款信息
		paid_amount String,
		unpaid_amount String,
		credit_carried_amount String,
		
		-- 优惠和抵扣信息
		coupon_amount String,
		discount_info String,
		saving_plan_deduction_discount_amount String,
		saving_plan_deduction_sp_id String,
		saving_plan_original_amount String,
		reservation_instance String,
		
		-- 货币信息
		currency String,
		currency_settlement String,
		exchange_rate String,
		
		-- 计费模式信息
		billing_mode String,
		billing_method_code String,
		billing_function String,
		business_mode String,
		selling_mode String,
		settlement_type String,
		
		-- 折扣相关业务信息
		discount_biz_billing_function String,
		discount_biz_measure_interval String,
		discount_biz_unit_price String,
		discount_biz_unit_price_interval String,
		
		-- 用户和组织信息
		owner_id String,
		owner_user_name String,
		owner_customer_name String,
		payer_id String,
		payer_user_name String,
		payer_customer_name String,
		seller_id String,
		seller_user_name String,
		seller_customer_name String,
		
		-- 项目和分类信息
		project String,
		project_display_name String,
		bill_category String,
		subject_name String,
		tag String,
		
		-- 其他业务信息
		main_contract_number String,
		original_order_no String,
		effective_factor String,
		expand_field String,
		
		-- 系统字段
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (bill_period, expense_date, bill_detail_id)
	PARTITION BY toYYYYMM(toDate(expense_date))`

	return s.chClient.CreateTable(ctx, s.tableName, schema)
}

func (s *BillService) CreateDistributedBillTable(ctx context.Context, localTableName, distributedTableName string) error {
	schema := `(
		-- 核心标识字段
		bill_detail_id String,
		bill_id String,
		instance_no String,
		
		-- 账期和时间字段  
		bill_period String,
		busi_period String,
		expense_date String,
		expense_begin_time String,
		expense_end_time String,
		trade_time String,
		
		-- 产品和服务信息
		product String,
		product_zh String,
		solution_zh String,
		element String,
		element_code String,
		factor String,
		factor_code String,
		
		-- 配置信息
		config_name String,
		configuration_code String,
		instance_name String,
		
		-- 地域信息
		region String,
		region_code String,
		zone String,
		zone_code String,
		country_region String,
		
		-- 用量和计费信息
		count String,
		unit String,
		use_duration String,
		use_duration_unit String,
		deduction_count String,
		deduction_use_duration String,
		
		-- 价格信息
		price String,
		price_unit String,
		price_interval String,
		market_price String,
		formula String,
		measure_interval String,
		
		-- 金额信息（核心）
		original_bill_amount String,
		preferential_bill_amount String,
		discount_bill_amount String,
		round_amount Float64,
		
		-- 实际价值和结算信息
		real_value String,
		pretax_real_value String,
		settle_real_value String,
		settle_pretax_real_value String,
		
		-- 应付金额信息
		payable_amount String,
		pre_tax_payable_amount String,
		settle_payable_amount String,
		settle_pre_tax_payable_amount String,
		
		-- 税费信息
		pretax_amount String,
		posttax_amount String,
		settle_pretax_amount String,
		settle_posttax_amount String,
		tax String,
		settle_tax String,
		tax_rate String,
		
		-- 付款信息
		paid_amount String,
		unpaid_amount String,
		credit_carried_amount String,
		
		-- 优惠和抵扣信息
		coupon_amount String,
		discount_info String,
		saving_plan_deduction_discount_amount String,
		saving_plan_deduction_sp_id String,
		saving_plan_original_amount String,
		reservation_instance String,
		
		-- 货币信息
		currency String,
		currency_settlement String,
		exchange_rate String,
		
		-- 计费模式信息
		billing_mode String,
		billing_method_code String,
		billing_function String,
		business_mode String,
		selling_mode String,
		settlement_type String,
		
		-- 折扣相关业务信息
		discount_biz_billing_function String,
		discount_biz_measure_interval String,
		discount_biz_unit_price String,
		discount_biz_unit_price_interval String,
		
		-- 用户和组织信息
		owner_id String,
		owner_user_name String,
		owner_customer_name String,
		payer_id String,
		payer_user_name String,
		payer_customer_name String,
		seller_id String,
		seller_user_name String,
		seller_customer_name String,
		
		-- 项目和分类信息
		project String,
		project_display_name String,
		bill_category String,
		subject_name String,
		tag String,
		
		-- 其他业务信息
		main_contract_number String,
		original_order_no String,
		effective_factor String,
		expand_field String,
		
		-- 系统字段
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (bill_period, expense_date, bill_detail_id)
	PARTITION BY toYYYYMM(toDate(expense_date))`

	return s.chClient.CreateDistributedTable(ctx, localTableName, distributedTableName, schema)
}

// RecreateDistributedBillTable 重建分布式账单表（删除后重新创建）
func (s *BillService) RecreateDistributedBillTable(ctx context.Context, localTableName, distributedTableName string) error {
	log.Printf("🔄 开始重建分布式表: %s (本地表: %s)", distributedTableName, localTableName)

	// 1. 检查并删除现有的分布式表
	log.Printf("📋 检查分布式表 %s 是否存在", distributedTableName)
	exists, err := s.chClient.TableExists(ctx, distributedTableName)
	if err != nil {
		log.Printf("⚠️  检查分布式表存在性时出现警告: %v", err)
	}

	if exists {
		// 获取表信息用于日志记录
		if info, err := s.chClient.GetTableInfo(ctx, distributedTableName); err == nil {
			log.Printf("📊 分布式表 %s 当前状态: 记录数=%d, 大小=%d bytes",
				distributedTableName, info.TotalRows, info.TotalBytes)
		}

		log.Printf("🗑️  删除现有分布式表: %s", distributedTableName)
		if err := s.chClient.DropTable(ctx, distributedTableName); err != nil {
			return fmt.Errorf("删除分布式表失败: %w", err)
		}
		log.Printf("✅ 分布式表 %s 删除成功", distributedTableName)
	} else {
		log.Printf("ℹ️  分布式表 %s 不存在，跳过删除", distributedTableName)
	}

	// 2. 检查并删除现有的本地表
	log.Printf("📋 检查本地表 %s 是否存在", localTableName)
	localExists, err := s.chClient.TableExists(ctx, localTableName)
	if err != nil {
		log.Printf("⚠️  检查本地表存在性时出现警告: %v", err)
	}

	if localExists {
		// 获取表信息用于日志记录
		if info, err := s.chClient.GetTableInfo(ctx, localTableName); err == nil {
			log.Printf("📊 本地表 %s 当前状态: 记录数=%d, 大小=%d bytes",
				localTableName, info.TotalRows, info.TotalBytes)
		}

		log.Printf("🗑️  删除现有本地表: %s (集群范围)", localTableName)
		if err := s.chClient.DropTableOnCluster(ctx, localTableName); err != nil {
			return fmt.Errorf("删除本地表失败: %w", err)
		}
		log.Printf("✅ 本地表 %s 删除成功", localTableName)
	} else {
		log.Printf("ℹ️  本地表 %s 不存在，跳过删除", localTableName)
	}

	// 3. 等待一小段时间确保删除操作在集群中传播
	log.Printf("⏳ 等待 2 秒确保删除操作在集群中传播...")
	time.Sleep(2 * time.Second)

	// 4. 重新创建分布式表结构
	log.Printf("🏗️  重新创建分布式表结构")
	if err := s.CreateDistributedBillTable(ctx, localTableName, distributedTableName); err != nil {
		return fmt.Errorf("重新创建分布式表失败: %w", err)
	}

	// 5. 验证新表创建成功
	log.Printf("🔍 验证新表是否创建成功...")
	newExists, err := s.chClient.TableExists(ctx, distributedTableName)
	if err != nil {
		log.Printf("⚠️  验证分布式表创建时出现警告: %v", err)
	} else if !newExists {
		return fmt.Errorf("分布式表 %s 创建后验证失败，表不存在", distributedTableName)
	}

	newLocalExists, err := s.chClient.TableExists(ctx, localTableName)
	if err != nil {
		log.Printf("⚠️  验证本地表创建时出现警告: %v", err)
	} else if !newLocalExists {
		return fmt.Errorf("本地表 %s 创建后验证失败，表不存在", localTableName)
	}

	log.Printf("✅ 分布式表重建完成: %s (本地表: %s)", distributedTableName, localTableName)
	log.Printf("📋 重建操作总结:")
	log.Printf("   - 分布式表: %s ✓", distributedTableName)
	log.Printf("   - 本地表: %s ✓", localTableName)
	log.Printf("   - 状态: 就绪，可以开始数据写入")

	return nil
}

// SyncAllBillDataWithRecreateDistributed 重建分布式表后同步所有数据（串行写入）
func (s *BillService) SyncAllBillDataWithRecreateDistributed(ctx context.Context, billPeriod, distributedTableName string) (*SyncResult, error) {
	result := &SyncResult{
		StartTime: time.Now(),
	}

	localTableName := "volcengine_bill_details_local"

	log.Printf("📋 开始分布式表重建同步，账期: %s, 表名: %s", billPeriod, distributedTableName)

	// 1. 重建分布式表
	if err := s.RecreateDistributedBillTable(ctx, localTableName, distributedTableName); err != nil {
		result.Error = fmt.Errorf("重建分布式表失败: %w", err)
		return result, result.Error
	}

	// 2. 创建串行数据处理器（禁用并发）
	processor := NewClickHouseProcessor(s.chClient, distributedTableName, true)
	processor.SetBatchSize(200) // 使用较小的批次大小确保稳定性

	// 3. 创建分页器配置（完全串行）
	config := &PaginatorConfig{
		BatchSize:      50, // API批次大小
		MaxRetries:     5,
		RetryDelay:     3 * time.Second,
		MaxConcurrency: 1, // 强制串行处理
	}

	// 4. 创建分页器
	paginator := NewBillPaginator(s.volcClient, processor, config)

	// 5. 设置进度回调
	paginator.SetProgressCallback(func(current, total int, duration time.Duration) {
		percentage := float64(current) / float64(total) * 100
		log.Printf("[分布式重建] 进度: %d/%d (%.1f%%), 耗时: %v", current, total, percentage, duration)
	})

	// 6. 构建请求（获取所有数据）
	req := &ListBillDetailRequest{
		BillPeriod:    billPeriod,
		GroupPeriod:   1,
		NeedRecordNum: 1, // 需要获取总记录数
	}

	// 7. 执行分页处理
	log.Printf("📊 开始串行数据写入...")
	paginateResult, err := paginator.PaginateBillDetails(ctx, req)
	if err != nil {
		result.Error = err
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result, fmt.Errorf("串行数据写入失败: %w", err)
	}

	// 8. 填充结果
	result.TotalRecords = paginateResult.TotalRecords
	result.FetchedRecords = paginateResult.ProcessedRecords
	result.InsertedRecords = paginateResult.ProcessedRecords
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	log.Printf("✅ 分布式表重建同步完成: 获取 %d 条，插入 %d 条，耗时 %v",
		result.FetchedRecords, result.InsertedRecords, result.Duration)

	return result, nil
}

func (s *BillService) SyncBillData(ctx context.Context, req *ListBillDetailRequest) (*SyncResult, error) {
	return s.syncBillDataWithCleanupInternal(ctx, req, false, "", "", nil)
}

func (s *BillService) SyncBillDataToDistributed(ctx context.Context, distributedTableName string, req *ListBillDetailRequest) (*SyncResult, error) {
	return s.syncBillDataWithCleanupInternal(ctx, req, true, distributedTableName, "", nil)
}

func (s *BillService) SyncBillDataWithCleanup(ctx context.Context, req *ListBillDetailRequest, cleanCondition string, cleanArgs ...interface{}) (*SyncResult, error) {
	return s.syncBillDataWithCleanupInternal(ctx, req, false, "", cleanCondition, cleanArgs...)
}

func (s *BillService) SyncBillDataToDistributedWithCleanup(ctx context.Context, distributedTableName string, req *ListBillDetailRequest, cleanCondition string, cleanArgs ...interface{}) (*SyncResult, error) {
	return s.syncBillDataWithCleanupInternal(ctx, req, true, distributedTableName, cleanCondition, cleanArgs...)
}

func (s *BillService) syncBillDataWithCleanupInternal(ctx context.Context, req *ListBillDetailRequest, isDistributed bool, tableName string, cleanCondition string, cleanArgs ...interface{}) (*SyncResult, error) {
	result := &SyncResult{
		StartTime: time.Now(),
	}

	targetTable := s.tableName
	if isDistributed && tableName != "" {
		targetTable = tableName
	}

	if cleanCondition != "" {
		log.Printf("清理表 %s 中的数据，条件: %s", targetTable, cleanCondition)
		if err := s.chClient.CleanTableData(ctx, targetTable, cleanCondition, cleanArgs...); err != nil {
			result.Error = fmt.Errorf("清理数据失败: %w", err)
			return result, result.Error
		}
		log.Printf("数据清理完成")
	}

	// 获取账单数据
	resp, err := s.volcClient.ListBillDetail(ctx, req)
	if err != nil {
		result.Error = err
		return result, fmt.Errorf("failed to fetch bill data: %w", err)
	}

	if resp.ResponseMetadata.Error != nil {
		result.Error = fmt.Errorf("API error: %s - %s", resp.ResponseMetadata.Error.Code, resp.ResponseMetadata.Error.Message)
		return result, result.Error
	}

	result.TotalRecords = int(resp.Result.Total)
	result.FetchedRecords = len(resp.Result.List)

	if len(resp.Result.List) == 0 {
		result.EndTime = time.Now()
		return result, nil
	}

	// 转换数据格式
	var dataToInsert []map[string]interface{}
	for _, bill := range resp.Result.List {
		dbBill := bill.ToDBFormat()

		// 转换标签为ClickHouse Map格式
		tagsMap := make(map[string]string)
		for k, v := range dbBill.Tags {
			if str, ok := v.(string); ok {
				tagsMap[k] = str
			} else {
				tagsMap[k] = fmt.Sprintf("%v", v)
			}
		}

		record := map[string]interface{}{
			"id":                   dbBill.ID,
			"owner_id":             dbBill.OwnerID,
			"owner_user_name":      dbBill.OwnerUserName,
			"product":              dbBill.Product,
			"product_zh":           dbBill.ProductZh,
			"billing_mode":         dbBill.BillingMode,
			"expense_time":         dbBill.ExpenseTime,
			"expense_date":         dbBill.ExpenseDate,
			"bill_period":          dbBill.BillPeriod,
			"amount":               dbBill.Amount,
			"currency":             dbBill.Currency,
			"region":               dbBill.Region,
			"zone":                 dbBill.Zone,
			"instance_id":          dbBill.InstanceID,
			"instance_name":        dbBill.InstanceName,
			"resource_id":          dbBill.ResourceID,
			"resource_name":        dbBill.ResourceName,
			"config_name":          dbBill.ConfigName,
			"config_name_zh":       dbBill.ConfigNameZh,
			"element":              dbBill.Element,
			"element_zh":           dbBill.ElementZh,
			"price":                dbBill.Price,
			"price_unit":           dbBill.PriceUnit,
			"count":                dbBill.Count,
			"unit":                 dbBill.Unit,
			"deduction_amount":     dbBill.DeductionAmount,
			"preferential_info":    dbBill.PreferentialInfo,
			"project":              dbBill.Project,
			"project_display_name": dbBill.ProjectDisplayName,
			"tags":                 tagsMap,
			"round_amount":         dbBill.RoundAmount,
			"usage_start_time":     dbBill.UsageStartTime,
			"usage_end_time":       dbBill.UsageEndTime,
			"created_at":           dbBill.CreatedAt,
			"updated_at":           dbBill.UpdatedAt,
		}

		dataToInsert = append(dataToInsert, record)
	}

	// 写入ClickHouse
	if isDistributed {
		if err := s.chClient.InsertBatchToDistributed(ctx, targetTable, dataToInsert); err != nil {
			result.Error = err
			return result, fmt.Errorf("failed to insert data to distributed table: %w", err)
		}
	} else {
		if err := s.chClient.InsertBatch(ctx, targetTable, dataToInsert); err != nil {
			result.Error = err
			return result, fmt.Errorf("failed to insert data to ClickHouse: %w", err)
		}
	}

	result.InsertedRecords = len(dataToInsert)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result, nil
}

func (s *BillService) SyncAllBillData(ctx context.Context, billPeriod string, batchSize int) (*SyncResult, error) {
	totalResult := &SyncResult{
		StartTime: time.Now(),
	}

	offset := 0
	totalInserted := 0

	for {
		req := &ListBillDetailRequest{
			BillPeriod:  billPeriod,
			Limit:       int32(batchSize),
			Offset:      int32(offset),
			GroupPeriod: 1, // 默认按天分组
		}

		result, err := s.SyncBillData(ctx, req)
		if err != nil {
			totalResult.Error = err
			return totalResult, err
		}

		totalInserted += result.InsertedRecords
		totalResult.TotalRecords = result.TotalRecords

		// 如果没有更多数据，退出循环
		if result.FetchedRecords < batchSize {
			break
		}

		offset += batchSize
	}

	totalResult.InsertedRecords = totalInserted
	totalResult.FetchedRecords = totalInserted
	totalResult.EndTime = time.Now()
	totalResult.Duration = totalResult.EndTime.Sub(totalResult.StartTime)

	return totalResult, nil
}

// SyncAllBillDataBestPractice 使用最佳实践同步账单数据
func (s *BillService) SyncAllBillDataBestPractice(ctx context.Context, billPeriod, tableName string, isDistributed bool) (*SyncResult, error) {
	result := &SyncResult{
		StartTime: time.Now(),
	}

	log.Printf("开始同步账单数据，账期: %s, 表名: %s, 分布式: %v", billPeriod, tableName, isDistributed)

	// 创建数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(500) // 设置写入批次大小

	// 创建分页器配置（优化限流）
	config := &PaginatorConfig{
		BatchSize:      50,              // 降低批次大小，减少API压力
		MaxRetries:     5,               // 增加重试次数
		RetryDelay:     3 * time.Second, // 增加重试延迟
		MaxConcurrency: 1,               // 串行处理，避免并发冲突
	}

	// 创建分页器
	paginator := NewBillPaginator(s.volcClient, processor, config)

	// 设置进度回调
	paginator.SetProgressCallback(func(current, total int, duration time.Duration) {
		percentage := float64(current) / float64(total) * 100
		log.Printf("[进度] %d/%d (%.1f%%), 耗时: %v", current, total, percentage, duration)
	})

	// 构建请求
	req := &ListBillDetailRequest{
		BillPeriod:  billPeriod,
		GroupPeriod: 1, // 默认按天分组
	}

	// 执行分页处理
	paginateResult, err := paginator.PaginateBillDetails(ctx, req)
	if err != nil {
		result.Error = err
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result, fmt.Errorf("分页处理失败: %w", err)
	}

	// 填充结果
	result.TotalRecords = paginateResult.TotalRecords
	result.FetchedRecords = paginateResult.ProcessedRecords
	result.InsertedRecords = paginateResult.ProcessedRecords
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	// 记录错误信息
	if len(paginateResult.Errors) > 0 {
		log.Printf("处理过程中出现 %d 个错误:", len(paginateResult.Errors))
		for i, err := range paginateResult.Errors {
			log.Printf("  错误 %d: %v", i+1, err)
		}
	}

	log.Printf("同步完成: 总记录数=%d, 已处理=%d, 耗时=%v, 错误=%d",
		result.TotalRecords, result.InsertedRecords, result.Duration, len(paginateResult.Errors))

	return result, nil
}

func (s *BillService) SyncBillDataWithCleanupAndPreview(ctx context.Context, req *ListBillDetailRequest, cleanCondition string, isDryRun bool, cleanArgs ...interface{}) (*SyncResult, error) {
	return s.syncBillDataWithCleanupInternalV2(ctx, req, false, "", cleanCondition, isDryRun, cleanArgs...)
}

func (s *BillService) SyncBillDataToDistributedWithCleanupAndPreview(ctx context.Context, distributedTableName string, req *ListBillDetailRequest, cleanCondition string, isDryRun bool, cleanArgs ...interface{}) (*SyncResult, error) {
	return s.syncBillDataWithCleanupInternalV2(ctx, req, true, distributedTableName, cleanCondition, isDryRun, cleanArgs...)
}

func (s *BillService) SyncAllBillDataBestPracticeWithCleanupAndPreview(ctx context.Context, billPeriod, tableName string, isDistributed bool, cleanCondition string, isDryRun bool, cleanArgs []interface{}) (*SyncResult, error) {
	result := &SyncResult{
		StartTime: time.Now(),
	}

	log.Printf("开始同步账单数据，账期: %s, 表名: %s, 分布式: %v, 预览模式: %v", billPeriod, tableName, isDistributed, isDryRun)

	// 创建数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(500)

	// 如果需要清理数据，设置清理条件和预览模式
	if cleanCondition != "" {
		processor.SetCleanupWithDryRun(cleanCondition, isDryRun, cleanArgs...)
	}

	// 创建分页器配置（优化限流）
	config := &PaginatorConfig{
		BatchSize:      50,              // 降低批次大小，减少API压力
		MaxRetries:     5,               // 增加重试次数
		RetryDelay:     3 * time.Second, // 增加重试延迟
		MaxConcurrency: 1,               // 串行处理，避免并发冲突
	}

	// 创建分页器
	paginator := NewBillPaginator(s.volcClient, processor, config)

	// 设置进度回调
	paginator.SetProgressCallback(func(current, total int, duration time.Duration) {
		percentage := float64(current) / float64(total) * 100
		log.Printf("[进度] %d/%d (%.1f%%), 耗时: %v", current, total, percentage, duration)
	})

	// 构建请求
	req := &ListBillDetailRequest{
		BillPeriod:  billPeriod,
		GroupPeriod: 1, // 默认按天分组
	}

	// 执行分页处理
	paginateResult, err := paginator.PaginateBillDetails(ctx, req)
	if err != nil {
		result.Error = err
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result, fmt.Errorf("分页处理失败: %w", err)
	}

	// 填充结果
	result.TotalRecords = paginateResult.TotalRecords
	result.FetchedRecords = paginateResult.ProcessedRecords
	if !isDryRun {
		result.InsertedRecords = paginateResult.ProcessedRecords
	} else {
		result.InsertedRecords = 0 // 预览模式下没有实际插入
	}
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	// 记录错误信息
	if len(paginateResult.Errors) > 0 {
		log.Printf("处理过程中出现 %d 个错误:", len(paginateResult.Errors))
		for i, err := range paginateResult.Errors {
			log.Printf("  错误 %d: %v", i+1, err)
		}
	}

	if isDryRun {
		log.Printf("预览完成: 总记录数=%d, 将处理=%d, 耗时=%v, 错误=%d",
			result.TotalRecords, result.FetchedRecords, result.Duration, len(paginateResult.Errors))
	} else {
		log.Printf("同步完成: 总记录数=%d, 已处理=%d, 耗时=%v, 错误=%d",
			result.TotalRecords, result.InsertedRecords, result.Duration, len(paginateResult.Errors))
	}

	return result, nil
}

func (s *BillService) syncBillDataWithCleanupInternalV2(ctx context.Context, req *ListBillDetailRequest, isDistributed bool, tableName string, cleanCondition string, isDryRun bool, cleanArgs ...interface{}) (*SyncResult, error) {
	result := &SyncResult{
		StartTime: time.Now(),
	}

	targetTable := s.tableName
	if isDistributed && tableName != "" {
		targetTable = tableName
	}

	// 处理数据清理
	if cleanCondition != "" {
		cleanupOpts := &clickhouse.CleanupOptions{
			Condition:   cleanCondition,
			Args:        cleanArgs,
			DryRun:      isDryRun,
			ProgressLog: true,
		}

		if isDryRun {
			log.Printf("预览清理表 %s 中的数据，条件: %s", targetTable, cleanCondition)
		} else {
			log.Printf("清理表 %s 中的数据，条件: %s", targetTable, cleanCondition)
		}

		cleanupResult, err := s.chClient.EnhancedCleanTableData(ctx, targetTable, cleanupOpts)
		if err != nil {
			result.Error = fmt.Errorf("数据清理失败: %w", err)
			return result, result.Error
		}

		if isDryRun {
			log.Printf("清理预览完成: %s", cleanupResult.String())
		} else {
			log.Printf("数据清理完成: %s", cleanupResult.String())
		}

		// 如果是预览模式，在这里就返回
		if isDryRun {
			result.EndTime = time.Now()
			result.Duration = result.EndTime.Sub(result.StartTime)
			return result, nil
		}
	}

	// 获取账单数据
	resp, err := s.volcClient.ListBillDetail(ctx, req)
	if err != nil {
		result.Error = err
		return result, fmt.Errorf("failed to fetch bill data: %w", err)
	}

	if resp.ResponseMetadata.Error != nil {
		result.Error = fmt.Errorf("API error: %s - %s", resp.ResponseMetadata.Error.Code, resp.ResponseMetadata.Error.Message)
		return result, result.Error
	}

	result.TotalRecords = int(resp.Result.Total)
	result.FetchedRecords = len(resp.Result.List)

	if len(resp.Result.List) == 0 {
		result.EndTime = time.Now()
		return result, nil
	}

	// 转换数据格式
	var dataToInsert []map[string]interface{}
	for _, bill := range resp.Result.List {
		dbBill := bill.ToDBFormat()

		// 转换标签为ClickHouse Map格式
		tagsMap := make(map[string]string)
		for k, v := range dbBill.Tags {
			if str, ok := v.(string); ok {
				tagsMap[k] = str
			} else {
				tagsMap[k] = fmt.Sprintf("%v", v)
			}
		}

		record := map[string]interface{}{
			"id":                   dbBill.ID,
			"owner_id":             dbBill.OwnerID,
			"owner_user_name":      dbBill.OwnerUserName,
			"product":              dbBill.Product,
			"product_zh":           dbBill.ProductZh,
			"billing_mode":         dbBill.BillingMode,
			"expense_time":         dbBill.ExpenseTime,
			"expense_date":         dbBill.ExpenseDate,
			"bill_period":          dbBill.BillPeriod,
			"amount":               dbBill.Amount,
			"currency":             dbBill.Currency,
			"region":               dbBill.Region,
			"zone":                 dbBill.Zone,
			"instance_id":          dbBill.InstanceID,
			"instance_name":        dbBill.InstanceName,
			"resource_id":          dbBill.ResourceID,
			"resource_name":        dbBill.ResourceName,
			"config_name":          dbBill.ConfigName,
			"config_name_zh":       dbBill.ConfigNameZh,
			"element":              dbBill.Element,
			"element_zh":           dbBill.ElementZh,
			"price":                dbBill.Price,
			"price_unit":           dbBill.PriceUnit,
			"count":                dbBill.Count,
			"unit":                 dbBill.Unit,
			"deduction_amount":     dbBill.DeductionAmount,
			"preferential_info":    dbBill.PreferentialInfo,
			"project":              dbBill.Project,
			"project_display_name": dbBill.ProjectDisplayName,
			"tags":                 tagsMap,
			"round_amount":         dbBill.RoundAmount,
			"usage_start_time":     dbBill.UsageStartTime,
			"usage_end_time":       dbBill.UsageEndTime,
			"created_at":           dbBill.CreatedAt,
			"updated_at":           dbBill.UpdatedAt,
		}

		dataToInsert = append(dataToInsert, record)
	}

	// 写入ClickHouse
	if isDistributed {
		if err := s.chClient.InsertBatchToDistributed(ctx, targetTable, dataToInsert); err != nil {
			result.Error = err
			return result, fmt.Errorf("failed to insert data to distributed table: %w", err)
		}
	} else {
		if err := s.chClient.InsertBatch(ctx, targetTable, dataToInsert); err != nil {
			result.Error = err
			return result, fmt.Errorf("failed to insert data to ClickHouse: %w", err)
		}
	}

	result.InsertedRecords = len(dataToInsert)
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	return result, nil
}

func (s *BillService) SyncAllBillDataBestPracticeWithCleanup(ctx context.Context, billPeriod, tableName string, isDistributed bool, cleanCondition string, cleanArgs []interface{}) (*SyncResult, error) {
	result := &SyncResult{
		StartTime: time.Now(),
	}

	log.Printf("开始同步账单数据，账期: %s, 表名: %s, 分布式: %v", billPeriod, tableName, isDistributed)

	// 创建数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(500)

	// 如果需要清理数据，设置清理条件
	if cleanCondition != "" {
		processor.SetCleanup(cleanCondition, cleanArgs...)
	}

	// 创建分页器配置（优化限流）
	config := &PaginatorConfig{
		BatchSize:      50,              // 降低批次大小，减少API压力
		MaxRetries:     5,               // 增加重试次数
		RetryDelay:     3 * time.Second, // 增加重试延迟
		MaxConcurrency: 1,               // 串行处理，避免并发冲突
	}

	// 创建分页器
	paginator := NewBillPaginator(s.volcClient, processor, config)

	// 设置进度回调
	paginator.SetProgressCallback(func(current, total int, duration time.Duration) {
		percentage := float64(current) / float64(total) * 100
		log.Printf("[进度] %d/%d (%.1f%%), 耗时: %v", current, total, percentage, duration)
	})

	// 构建请求
	req := &ListBillDetailRequest{
		BillPeriod:  billPeriod,
		GroupPeriod: 1, // 默认按天分组
	}

	// 执行分页处理
	paginateResult, err := paginator.PaginateBillDetails(ctx, req)
	if err != nil {
		result.Error = err
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result, fmt.Errorf("分页处理失败: %w", err)
	}

	// 填充结果
	result.TotalRecords = paginateResult.TotalRecords
	result.FetchedRecords = paginateResult.ProcessedRecords
	result.InsertedRecords = paginateResult.ProcessedRecords
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)

	// 记录错误信息
	if len(paginateResult.Errors) > 0 {
		log.Printf("处理过程中出现 %d 个错误:", len(paginateResult.Errors))
		for i, err := range paginateResult.Errors {
			log.Printf("  错误 %d: %v", i+1, err)
		}
	}

	log.Printf("同步完成: 总记录数=%d, 已处理=%d, 耗时=%v, 错误=%d",
		result.TotalRecords, result.InsertedRecords, result.Duration, len(paginateResult.Errors))

	return result, nil
}

type SyncResult struct {
	StartTime       time.Time     `json:"start_time"`
	EndTime         time.Time     `json:"end_time"`
	Duration        time.Duration `json:"duration"`
	TotalRecords    int           `json:"total_records"`
	FetchedRecords  int           `json:"fetched_records"`
	InsertedRecords int           `json:"inserted_records"`
	Error           error         `json:"error,omitempty"`
}

// GetAvailableBillPeriods 获取API实际支持的账期列表（仅当月和上月）
func (s *BillService) GetAvailableBillPeriods(ctx context.Context, maxMonths int) ([]string, error) {
	// VolcEngine账单API只支持当月和上月，忽略maxMonths参数
	now := time.Now()
	currentMonth := now.Format("2006-01")
	lastMonth := now.AddDate(0, -1, 0).Format("2006-01")

	periods := []string{currentMonth, lastMonth}

	log.Printf("生成可用账期列表（仅API支持的月份）: %v", periods)
	log.Printf("注意: VolcEngine账单API仅支持查询当月和上月数据")
	return periods, nil
}

// GetAvailableBillPeriodsWithValidation 获取API支持的账期并验证数据存在性
func (s *BillService) GetAvailableBillPeriodsWithValidation(ctx context.Context, maxMonths int, skipEmpty bool) ([]string, error) {
	// VolcEngine账单API只支持当月和上月，忽略maxMonths参数
	now := time.Now()
	candidatePeriods := []string{
		now.Format("2006-01"),                   // 当月
		now.AddDate(0, -1, 0).Format("2006-01"), // 上月
	}

	var validPeriods []string

	// 验证每个候选账期
	for _, period := range candidatePeriods {
		if skipEmpty {
			// 测试该账期是否有数据（发送小批量请求测试）
			testReq := &ListBillDetailRequest{
				BillPeriod:  period,
				Limit:       1,
				Offset:      0,
				GroupPeriod: 1,
			}

			resp, err := s.volcClient.ListBillDetail(ctx, testReq)
			if err != nil {
				log.Printf("账期 %s 验证失败: %v", period, err)
				continue
			}

			if resp.ResponseMetadata.Error != nil {
				log.Printf("账期 %s API 错误: %s", period, resp.ResponseMetadata.Error.Message)
				continue
			}

			if resp.Result.Total > 0 {
				validPeriods = append(validPeriods, period)
				log.Printf("账期 %s 验证通过，包含 %d 条记录", period, resp.Result.Total)
			} else {
				log.Printf("账期 %s 无数据，跳过", period)
			}
		} else {
			validPeriods = append(validPeriods, period)
		}
	}

	log.Printf("最终可用账期列表: %v", validPeriods)
	log.Printf("注意: VolcEngine账单API仅支持查询当月和上月数据")
	return validPeriods, nil
}

// DropBillTable 删除账单表
func (s *BillService) DropBillTable(ctx context.Context, confirmation string) error {
	return s.chClient.DropTableSafely(ctx, s.tableName, confirmation)
}

// DropDistributedBillTable 删除分布式账单表
func (s *BillService) DropDistributedBillTable(ctx context.Context, localTableName, distributedTableName, confirmation string) error {
	return s.chClient.DropDistributedTableSafely(ctx, localTableName, distributedTableName, confirmation)
}
