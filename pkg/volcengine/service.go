package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"log"
	"sort"
	"strings"
	"time"
)

type BillService struct {
	volcClient *Client
	chClient   *clickhouse.Client
	tableName  string
	config     *config.VolcEngineConfig
}

func NewBillService(volcConfig *config.VolcEngineConfig, chClient *clickhouse.Client) (*BillService, error) {
	volcClient := NewClient(volcConfig)

	return &BillService{
		volcClient: volcClient,
		chClient:   chClient,
		tableName:  "volcengine_bill_details",
		config:     volcConfig,
	}, nil
}

// CreateBillTable 创建账单表（支持自动表名解析）
func (s *BillService) CreateBillTable(ctx context.Context) error {
	// 获取解析器，用于确定实际要检查和创建的表名
	resolver := s.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(s.tableName)

	// 检查表是否已存在
	exists, err := s.chClient.TableExists(ctx, s.tableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if exists {
		log.Printf("[火山云账单表] 表 %s 已存在，跳过创建", actualTableName)
		return nil
	}

	schema := `(
		-- 直接使用API返回的字段名（Pascal case）
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
		
		-- 金额信息字段
		OriginalBillAmount String,
		PreferentialBillAmount String,
		DiscountBillAmount String,
		RoundAmount Float64,
		PayableAmount String,
		PaidAmount String,
		UnpaidAmount String,
		CouponAmount String,
		CreditCarriedAmount String,
		
		-- 其他信息字段
		Currency String,
		Project String,
		ProjectDisplayName String,
		Tag String,
		BillCategory String,
		SubjectName String,
		ReservationInstance String,
		ExpandField String,
		EffectiveFactor String,
		
		-- 折扣相关字段
		DiscountBizBillingFunction String,
		DiscountBizUnitPrice String,
		DiscountBizUnitPriceInterval String,
		DiscountBizMeasureInterval String,
		
		-- 系统字段
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (BillPeriod, ExpenseDate, BillDetailId, InstanceNo, ExpenseBeginTime, Product, Element, OriginalBillAmount, TradeTime)  
	PARTITION BY toYYYYMM(toDate(ExpenseDate))`

	// 使用自动表名解析机制创建表
	if resolver.IsClusterEnabled() {
		// 集群模式：创建完整的分布式表结构（本地表+分布式表）
		log.Printf("[火山云账单表] 在集群模式下创建分布式表结构，基础表名: %s", s.tableName)
		return s.chClient.CreateDistributedTableWithResolver(ctx, s.tableName, schema)
	} else {
		// 单机模式：直接创建表
		log.Printf("[火山云账单表] 在单机模式下创建表: %s", actualTableName)
		return s.chClient.CreateTable(ctx, s.tableName, schema)
	}
}

func (s *BillService) CreateDistributedBillTable(ctx context.Context, localTableName, distributedTableName string) error {
	schema := `(
		-- 直接使用API返回的字段名（Pascal case）
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
		
		-- 金额信息字段
		OriginalBillAmount String,
		PreferentialBillAmount String,
		DiscountBillAmount String,
		RoundAmount Float64,
		PayableAmount String,
		PaidAmount String,
		UnpaidAmount String,
		CouponAmount String,
		CreditCarriedAmount String,
		
		-- 其他信息字段
		Currency String,
		Project String,
		ProjectDisplayName String,
		Tag String,
		BillCategory String,
		SubjectName String,
		ReservationInstance String,
		ExpandField String,
		EffectiveFactor String,
		
		-- 折扣相关字段
		DiscountBizBillingFunction String,
		DiscountBizUnitPrice String,
		DiscountBizUnitPriceInterval String,
		DiscountBizMeasureInterval String,
		
		-- 系统字段
		created_at DateTime64(3) DEFAULT now(),
		updated_at DateTime64(3) DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (BillPeriod, ExpenseDate, BillDetailId, InstanceNo, ExpenseBeginTime, Product, Element, OriginalBillAmount, TradeTime)  
	PARTITION BY toYYYYMM(toDate(ExpenseDate))`

	return s.chClient.CreateDistributedTable(ctx, localTableName, distributedTableName, schema)
}

// CheckMonthlyDataExistsWithOptimize 检查月数据是否存在（强制去重后再检查，借鉴阿里云机制）
func (s *BillService) CheckMonthlyDataExistsWithOptimize(ctx context.Context, tableName, billPeriod string) (bool, int64, error) {
	log.Printf("[火山云预去重检查] 开始检查月数据并优化，账期: %s, 表: %s", billPeriod, tableName)
	
	// 对于ReplacingMergeTree，先执行OPTIMIZE FINAL强制去重
	if strings.Contains(tableName, "_distributed") {
		// 分布式表优化本地表
		localTableName := strings.Replace(tableName, "_distributed", "_local", 1)
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s ON CLUSTER %s FINAL", localTableName, s.chClient.GetClusterName())
		log.Printf("[ReplacingMergeTree预优化] 执行强制去重: %s", optimizeQuery)
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			log.Printf("[ReplacingMergeTree预优化] 执行失败，继续查询: %v", err)
		}
	} else {
		// 普通表直接优化
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
		log.Printf("[ReplacingMergeTree预优化] 执行强制去重: %s", optimizeQuery)
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			log.Printf("[ReplacingMergeTree预优化] 执行失败，继续查询: %v", err)
		}
	}
	
	// 优化后再检查数据
	yearMonth := strings.Replace(billPeriod, "-", "", 1) // 2025-09 -> 202509
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE toYYYYMM(toDate(ExpenseDate)) = %s", tableName, yearMonth)
	
	log.Printf("[火山云预检查] 执行查询: %s", query)
	var count int64
	if err := s.chClient.QueryRow(ctx, query).Scan(&count); err != nil {
		return false, 0, fmt.Errorf("查询数据量失败: %w", err)
	}
	
	log.Printf("[火山云预检查] 表 %s 账期 %s 数据量: %d", tableName, billPeriod, count)
	return count > 0, count, nil
}

// CheckDistributedTableHealth 检查分布式表健康状态（借鉴阿里云）
func (s *BillService) CheckDistributedTableHealth(ctx context.Context, localTableName, distributedTableName string) (*TableHealthStatus, error) {
	log.Printf("🔍 开始检查分布式表健康状态: %s -> %s", localTableName, distributedTableName)
	
	status := &TableHealthStatus{
		LocalTableName:      localTableName,
		DistributedTableName: distributedTableName,
		CheckTime:           time.Now(),
	}
	
	// 检查本地表存在性
	localExists, err := s.chClient.TableExists(ctx, localTableName)
	if err != nil {
		status.Issues = append(status.Issues, fmt.Sprintf("无法检查本地表存在性: %v", err))
	} else {
		status.LocalTableExists = localExists
	}
	
	// 检查分布式表存在性
	distExists, err := s.chClient.TableExists(ctx, distributedTableName)
	if err != nil {
		status.Issues = append(status.Issues, fmt.Sprintf("无法检查分布式表存在性: %v", err))
	} else {
		status.DistributedTableExists = distExists
	}
	
	// 检查表结构一致性（如果两个表都存在）
	if localExists && distExists {
		// 获取本地表结构
		localInfo, err := s.chClient.GetTableInfo(ctx, localTableName)
		if err != nil {
			status.Issues = append(status.Issues, fmt.Sprintf("无法获取本地表信息: %v", err))
		} else {
			status.LocalTableRows = uint64(localInfo.TotalRows)
			status.LocalTableSize = uint64(localInfo.TotalBytes)
		}
		
		// 获取分布式表结构
		distInfo, err := s.chClient.GetTableInfo(ctx, distributedTableName)
		if err != nil {
			status.Issues = append(status.Issues, fmt.Sprintf("无法获取分布式表信息: %v", err))
		} else {
			status.DistributedTableRows = uint64(distInfo.TotalRows)
			status.DistributedTableSize = uint64(distInfo.TotalBytes)
		}
	}
	
	// 判断健康状态
	if len(status.Issues) == 0 && localExists && distExists {
		status.IsHealthy = true
		status.Summary = "分布式表健康状态良好"
	} else {
		status.IsHealthy = false
		if !localExists && !distExists {
			status.Summary = "本地表和分布式表都不存在，需要创建"
		} else if !localExists {
			status.Summary = "本地表不存在，分布式表创建不完整"
		} else if !distExists {
			status.Summary = "分布式表不存在，需要创建分布式表"
		} else {
			status.Summary = fmt.Sprintf("发现 %d 个问题", len(status.Issues))
		}
	}
	
	log.Printf("🏥 分布式表健康检查完成: %s", status.Summary)
	return status, nil
}

// AutoRepairDistributedTable 自动修复分布式表（借鉴阿里云）
func (s *BillService) AutoRepairDistributedTable(ctx context.Context, localTableName, distributedTableName string) error {
	log.Printf("🔧 开始自动修复分布式表: %s -> %s", localTableName, distributedTableName)
	
	// 先检查健康状态
	status, err := s.CheckDistributedTableHealth(ctx, localTableName, distributedTableName)
	if err != nil {
		return fmt.Errorf("健康检查失败: %w", err)
	}
	
	if status.IsHealthy {
		log.Printf("✅ 分布式表状态良好，无需修复")
		return nil
	}
	
	log.Printf("⚠️ 检测到问题: %s，开始修复", status.Summary)
	
	// 根据问题类型进行修复
	if !status.LocalTableExists && !status.DistributedTableExists {
		// 两个表都不存在，创建完整的分布式表结构
		log.Printf("🏗️ 创建完整的分布式表结构")
		return s.CreateDistributedBillTable(ctx, localTableName, distributedTableName)
	} else if !status.LocalTableExists {
		// 只有分布式表存在，删除后重新创建
		log.Printf("🗑️ 分布式表结构不完整，重新创建")
		if err := s.chClient.DropTable(ctx, distributedTableName); err != nil {
			log.Printf("⚠️ 删除分布式表时出现警告: %v", err)
		}
		return s.CreateDistributedBillTable(ctx, localTableName, distributedTableName)
	} else if !status.DistributedTableExists {
		// 只有本地表存在，创建分布式表
		log.Printf("🔗 创建缺失的分布式表")
		return s.CreateDistributedBillTable(ctx, localTableName, distributedTableName)
	}
	
	log.Printf("✅ 分布式表修复完成")
	return nil
}

// EnsureDistributedTableReady 确保分布式表就绪（借鉴阿里云完善实现）
func (s *BillService) EnsureDistributedTableReady(ctx context.Context, localTableName, distributedTableName string) error {
	log.Printf("🔧 确保分布式表就绪: %s -> %s", localTableName, distributedTableName)
	
	// 检查健康状态
	status, err := s.CheckDistributedTableHealth(ctx, localTableName, distributedTableName)
	if err != nil {
		return fmt.Errorf("健康检查失败: %w", err)
	}
	
	if status.IsHealthy {
		log.Printf("✅ 分布式表已就绪")
		return nil
	}
	
	// 自动修复
	if err := s.AutoRepairDistributedTable(ctx, localTableName, distributedTableName); err != nil {
		return fmt.Errorf("自动修复失败: %w", err)
	}
	
	// 再次检查
	status, err = s.CheckDistributedTableHealth(ctx, localTableName, distributedTableName)
	if err != nil {
		return fmt.Errorf("修复后健康检查失败: %w", err)
	}
	
	if !status.IsHealthy {
		return fmt.Errorf("修复后仍不健康: %s", status.Summary)
	}
	
	log.Printf("✅ 分布式表成功就绪")
	return nil
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
	// 使用配置中的 BatchSize
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 50 // 默认值
	}
	
	config := &PaginatorConfig{
		BatchSize:      batchSize,
		MaxRetries:     s.config.MaxRetries,
		RetryDelay:     time.Duration(s.config.RetryDelay) * time.Second,
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

// TableHealthStatus 表健康状态（借鉴阿里云）
type TableHealthStatus struct {
	LocalTableName           string    `json:"local_table_name"`
	DistributedTableName     string    `json:"distributed_table_name"`
	LocalTableExists         bool      `json:"local_table_exists"`
	DistributedTableExists   bool      `json:"distributed_table_exists"`
	LocalTableRows           uint64    `json:"local_table_rows"`
	DistributedTableRows     uint64    `json:"distributed_table_rows"`
	LocalTableSize           uint64    `json:"local_table_size"`
	DistributedTableSize     uint64    `json:"distributed_table_size"`
	IsHealthy                bool      `json:"is_healthy"`
	Summary                  string    `json:"summary"`
	Issues                   []string  `json:"issues"`
	CheckTime                time.Time `json:"check_time"`
}

// String 返回状态的字符串描述
func (ths *TableHealthStatus) String() string {
	healthIcon := "❌"
	if ths.IsHealthy {
		healthIcon = "✅"
	}
	
	return fmt.Sprintf("%s 分布式表健康状态 - %s -> %s: %s (本地表: %v, 分布式表: %v)",
		healthIcon, ths.LocalTableName, ths.DistributedTableName, ths.Summary,
		ths.LocalTableExists, ths.DistributedTableExists)
}

func (s *BillService) SyncBillData(ctx context.Context, req *ListBillDetailRequest) (*SyncResult, error) {
	return s.syncBillDataWithCleanupInternal(ctx, req, false, "", "", nil)
}

// PerformDataPreCheck 智能数据预检查（借鉴阿里云实现）
func (s *BillService) PerformDataPreCheck(ctx context.Context, req *ListBillDetailRequest) (*DataPreCheckResult, error) {
	log.Printf("[火山云预检查] 开始数据预检查，账期: %s", req.BillPeriod)
	
	// 获取API数据总量估算
	apiResp, err := s.volcClient.ListBillDetail(ctx, &ListBillDetailRequest{
		BillPeriod:    req.BillPeriod,
		Limit:         1, // 只获取1条用于总数统计
		Offset:        0,
		NeedRecordNum: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get API data count: %w", err)
	}
	
	apiCount := apiResp.Result.Total
	log.Printf("[火山云预检查] API数据总量: %d", apiCount)
	
	// 获取数据库记录数
	// 注意：火山引擎表使用Pascal case字段名
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE BillPeriod = '%s'", s.tableName, req.BillPeriod)
	rows, err := s.chClient.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query database count: %w", err)
	}
	defer rows.Close()
	
	var dbCount int64
	if rows.Next() {
		if err := rows.Scan(&dbCount); err != nil {
			return nil, fmt.Errorf("failed to scan count: %w", err)
		}
	}
	
	log.Printf("[火山云预检查] 数据库记录数: %d", dbCount)
	
	// 智能决策
	result := &DataPreCheckResult{
		APICount:      apiCount,
		DatabaseCount: dbCount,
		BillPeriod:    req.BillPeriod,
		NeedSync:      false,
		NeedCleanup:   false,
	}
	
	if apiCount == 0 && dbCount == 0 {
		result.Reason = "API和数据库都无数据，跳过同步"
	} else if apiCount == 0 && dbCount > 0 {
		result.Reason = "API无数据但数据库有数据，跳过同步"
	} else if apiCount > 0 && dbCount == 0 {
		result.NeedSync = true
		result.Reason = fmt.Sprintf("数据库为空但API有%d条数据，需要同步", apiCount)
	} else if int64(apiCount) == dbCount {
		result.Reason = "数据量一致，跳过同步"
	} else {
		result.NeedSync = true
		result.NeedCleanup = true
		result.Reason = fmt.Sprintf("数据量不一致(API:%d vs DB:%d)，需要先清理再同步", apiCount, dbCount)
	}
	
	log.Printf("[火山云预检查] 决策结果: %s", result.Reason)
	return result, nil
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
		// 使用增强版清理（支持分区删除）
		cleanupOpts := &clickhouse.CleanupOptions{
			Condition:   cleanCondition,
			Args:        cleanArgs,
			DryRun:      false,
			ProgressLog: true,
		}
		_, err := s.chClient.EnhancedCleanTableData(ctx, targetTable, cleanupOpts)
		if err != nil {
			result.Error = fmt.Errorf("清理数据失败: %w", err)
			return result, result.Error
		}
		log.Printf("数据清理完成")
	}

	// 使用智能同步获取所有账单数据（支持分页）
	log.Printf("📊 [火山云] 使用智能同步获取账期 %s 的所有数据", req.BillPeriod)
	return s.SmartSyncAllData(ctx, req.BillPeriod, targetTable, isDistributed)
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

// SyncAllBillDataWithPreCheck 带预检查的智能同步（借鉴阿里云）
func (s *BillService) SyncAllBillDataWithPreCheck(ctx context.Context, billPeriod, tableName string, isDistributed bool) (*SyncResult, error) {
	log.Printf("[火山云智能同步] 开始带预检查的同步，账期: %s", billPeriod)
	
	// 执行数据预检查
	preCheckReq := &ListBillDetailRequest{
		BillPeriod: billPeriod,
	}
	preCheckResult, err := s.PerformDataPreCheck(ctx, preCheckReq)
	if err != nil {
		return nil, fmt.Errorf("预检查失败: %w", err)
	}
	
	// 如果不需要同步，直接返回
	if !preCheckResult.NeedSync {
		log.Printf("[火山云智能同步] %s，跳过同步", preCheckResult.Reason)
		return &SyncResult{
			StartTime: time.Now(),
			EndTime:   time.Now(),
		}, nil
	}
	
	// 如果需要清理，先清理数据
	if preCheckResult.NeedCleanup {
		log.Printf("[火山云智能同步] 检测到数据不一致，先清理账期 %s 的数据", billPeriod)
		// 注意：火山引擎表按ExpenseDate分区，使用分区函数条件
		yearMonth := strings.Replace(billPeriod, "-", "", 1) // 2025-09 -> 202509
		cleanCondition := fmt.Sprintf("toYYYYMM(toDate(ExpenseDate)) = %s", yearMonth)
		// 使用增强版清理（支持分区删除）
		cleanupOpts := &clickhouse.CleanupOptions{
			Condition:   cleanCondition,
			Args:        nil,
			DryRun:      false,
			ProgressLog: true,
		}
		_, err := s.chClient.EnhancedCleanTableData(ctx, tableName, cleanupOpts)
		if err != nil {
			return nil, fmt.Errorf("数据清理失败: %w", err)
		}
		log.Printf("[火山云智能同步] 数据清理完成，开始同步新数据")
	}
	
	// 执行同步
	return s.SyncAllBillDataBestPractice(ctx, billPeriod, tableName, isDistributed)
}

// SyncAllBillDataBestPractice 使用最佳实践同步账单数据（集成预去重检查）
func (s *BillService) SyncAllBillDataBestPractice(ctx context.Context, billPeriod, tableName string, isDistributed bool) (*SyncResult, error) {
	return s.syncAllBillDataBestPracticeInternal(ctx, billPeriod, tableName, isDistributed, false)
}

// SyncAllBillDataBestPracticeWithoutPreCheck 使用最佳实践同步账单数据（跳过预去重检查）
func (s *BillService) SyncAllBillDataBestPracticeWithoutPreCheck(ctx context.Context, billPeriod, tableName string, isDistributed bool) (*SyncResult, error) {
	return s.syncAllBillDataBestPracticeInternal(ctx, billPeriod, tableName, isDistributed, true)
}

// syncAllBillDataBestPracticeInternal 内部实现
func (s *BillService) syncAllBillDataBestPracticeInternal(ctx context.Context, billPeriod, tableName string, isDistributed, skipPreCheck bool) (*SyncResult, error) {
	result := &SyncResult{
		StartTime: time.Now(),
	}

	log.Printf("开始同步账单数据，账期: %s, 表名: %s, 分布式: %v, 跳过预检查: %v", billPeriod, tableName, isDistributed, skipPreCheck)

	// 根据参数决定是否执行预去重检查
	if !skipPreCheck {
		// 执行预去重检查（借鉴阿里云机制）
		exists, existingCount, err := s.CheckMonthlyDataExistsWithOptimize(ctx, tableName, billPeriod)
		if err != nil {
			log.Printf("[预去重检查] 检查失败，继续同步: %v", err)
		} else if exists && existingCount > 0 {
			log.Printf("[预去重检查] 检测到已存在 %d 条数据，跳过同步以避免重复", existingCount)
			result.EndTime = time.Now()
			result.Duration = result.EndTime.Sub(result.StartTime)
			result.TotalRecords = 0
			result.InsertedRecords = 0
			log.Printf("✅ 同步跳过：数据已存在 (已有%d条记录)", existingCount)
			return result, nil
		} else {
			log.Printf("[预去重检查] 未检测到现有数据，继续同步")
		}
	} else {
		log.Printf("[跳过预检查] 任务管理器已完成数据对比，直接执行同步")
	}

	// 创建数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(500) // 设置写入批次大小

	// 创建分页器配置（优化限流）
	// 使用配置中的 BatchSize
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 50 // 默认值
	}
	
	config := &PaginatorConfig{
		BatchSize:      batchSize,
		MaxRetries:     s.config.MaxRetries,
		RetryDelay:     time.Duration(s.config.RetryDelay) * time.Second,
		MaxConcurrency: 1, // 串行处理，避免并发冲突
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
	// 使用配置中的 BatchSize
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 50 // 默认值
	}
	
	config := &PaginatorConfig{
		BatchSize:      batchSize,
		MaxRetries:     s.config.MaxRetries,
		RetryDelay:     time.Duration(s.config.RetryDelay) * time.Second,
		MaxConcurrency: 1, // 串行处理，避免并发冲突
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
		record := map[string]interface{}{
			"BillDetailId":                   bill.BillDetailID,
			"BillID":                        bill.BillID,
			"InstanceNo":                    bill.InstanceNo,
			"BillPeriod":                    bill.BillPeriod,
			"BusiPeriod":                    bill.BusiPeriod,
			"ExpenseDate":                   bill.ExpenseDate,
			"ExpenseBeginTime":              bill.ExpenseBeginTime,
			"ExpenseEndTime":                bill.ExpenseEndTime,
			"TradeTime":                     bill.TradeTime,
			"PayerID":                       bill.PayerID,
			"PayerUserName":                 bill.PayerUserName,
			"PayerCustomerName":             bill.PayerCustomerName,
			"SellerID":                      bill.SellerID,
			"SellerUserName":                bill.SellerUserName,
			"SellerCustomerName":            bill.SellerCustomerName,
			"OwnerID":                       bill.OwnerID,
			"OwnerUserName":                 bill.OwnerUserName,
			"OwnerCustomerName":             bill.OwnerCustomerName,
			"Product":                       bill.Product,
			"ProductZh":                     bill.ProductZh,
			"SolutionZh":                    bill.SolutionZh,
			"Element":                       bill.Element,
			"ElementCode":                   bill.ElementCode,
			"Factor":                        bill.Factor,
			"FactorCode":                    bill.FactorCode,
			"ConfigName":                    bill.ConfigName,
			"ConfigurationCode":             bill.ConfigurationCode,
			"InstanceName":                  bill.InstanceName,
			"Region":                        bill.Region,
			"RegionCode":                    bill.RegionCode,
			"Zone":                          bill.Zone,
			"ZoneCode":                      bill.ZoneCode,
			"BillingMode":                   bill.BillingMode,
			"BusinessMode":                  bill.BusinessMode,
			"BillingFunction":               bill.BillingFunction,
			"BillingMethodCode":             bill.BillingMethodCode,
			"SellingMode":                   bill.SellingMode,
			"SettlementType":                bill.SettlementType,
			"Count":                         bill.Count,
			"Unit":                          bill.Unit,
			"UseDuration":                   bill.UseDuration,
			"UseDurationUnit":               bill.UseDurationUnit,
			"DeductionCount":                bill.DeductionCount,
			"DeductionUseDuration":          bill.DeductionUseDuration,
			"Price":                         bill.Price,
			"PriceUnit":                     bill.PriceUnit,
			"PriceInterval":                 bill.PriceInterval,
			"MarketPrice":                   bill.MarketPrice,
			"MeasureInterval":               bill.MeasureInterval,
			"OriginalBillAmount":            bill.OriginalBillAmount,
			"PreferentialBillAmount":        bill.PreferentialBillAmount,
			"DiscountBillAmount":            bill.DiscountBillAmount,
			"RoundAmount":                   bill.RoundAmount,
			"PayableAmount":                 bill.PayableAmount,
			"PaidAmount":                    bill.PaidAmount,
			"UnpaidAmount":                  bill.UnpaidAmount,
			"CouponAmount":                  bill.CouponAmount,
			"CreditCarriedAmount":           bill.CreditCarriedAmount,
			"Currency":                      bill.Currency,
			"Project":                       bill.Project,
			"ProjectDisplayName":            bill.ProjectDisplayName,
			"Tag":                           bill.Tag,
			"BillCategory":                  bill.BillCategory,
			"SubjectName":                   bill.SubjectName,
			"ReservationInstance":           bill.ReservationInstance,
			"ExpandField":                   bill.ExpandField,
			"EffectiveFactor":               bill.EffectiveFactor,
			"DiscountBizBillingFunction":    bill.DiscountBizBillingFunction,
			"DiscountBizUnitPrice":          bill.DiscountBizUnitPrice,
			"DiscountBizUnitPriceInterval":  bill.DiscountBizUnitPriceInterval,
			"DiscountBizMeasureInterval":    bill.DiscountBizMeasureInterval,
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
	// 使用配置中的 BatchSize
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 50 // 默认值
	}
	
	config := &PaginatorConfig{
		BatchSize:      batchSize,
		MaxRetries:     s.config.MaxRetries,
		RetryDelay:     time.Duration(s.config.RetryDelay) * time.Second,
		MaxConcurrency: 1, // 串行处理，避免并发冲突
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

// DataPreCheckResult 数据预检查结果（借鉴阿里云）
type DataPreCheckResult struct {
	APICount      int32  `json:"api_count"`      // API返回的数据总数
	DatabaseCount int64  `json:"database_count"` // 数据库中的记录总数
	BillPeriod    string `json:"bill_period"`   // 账期
	NeedSync      bool   `json:"need_sync"`      // 是否需要同步
	NeedCleanup   bool   `json:"need_cleanup"`   // 是否需要先清理数据
	Reason        string `json:"reason"`         // 决策原因
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

// InitialDataPull 首次拉取上个月到昨天截止的数据（Ultra智能策略）
func (s *BillService) InitialDataPull(ctx context.Context, tableName string, isDistributed bool) (*InitialPullResult, error) {
	log.Printf("🚀 [火山云首次拉取] 开始执行首次数据拉取：上个月到昨天")
	
	result := &InitialPullResult{
		StartTime: time.Now(),
		PullPeriods: make([]string, 0),
		Results: make(map[string]*SyncResult),
	}
	
	// 1. 计算时间范围：上个月到昨天
	periods, dateRange, err := s.calculateInitialPullTimeRange()
	if err != nil {
		result.Error = err
		result.EndTime = time.Now()
		return result, fmt.Errorf("计算时间范围失败: %w", err)
	}
	
	result.PullPeriods = periods
	result.DateRange = dateRange
	log.Printf("📅 [时间范围] %s，涉及账期: %v", dateRange, periods)
	
	// 2. 预估数据量并制定拉取策略
	strategy, err := s.estimateAndPlanPullStrategy(ctx, periods)
	if err != nil {
		log.Printf("⚠️ 数据量预估失败，使用保守策略: %v", err)
		strategy = s.getConservativePullStrategy()
	}
	
	log.Printf("📊 [拉取策略] 预估总记录: %d, 预估耗时: %v, 批次大小: %d", 
		strategy.EstimatedRecords, strategy.EstimatedDuration, strategy.BatchSize)
	
	// 3. 按账期依次拉取数据
	totalRecords := 0
	for i, period := range periods {
		log.Printf("📦 [%d/%d] 开始拉取账期: %s", i+1, len(periods), period)
		
		syncResult, err := s.pullPeriodDataWithStrategy(ctx, period, tableName, isDistributed, strategy)
		if err != nil {
			result.Error = err
			result.EndTime = time.Now()
			return result, fmt.Errorf("账期 %s 拉取失败: %w", period, err)
		}
		
		result.Results[period] = syncResult
		totalRecords += syncResult.TotalRecords
		
		log.Printf("✅ [%d/%d] 账期 %s 完成: %d条记录, 耗时: %v", 
			i+1, len(periods), period, syncResult.TotalRecords, syncResult.Duration)
		
		// 账期间暂停，避免API压力过大
		if i < len(periods)-1 {
			pauseDuration := 5 * time.Second
			log.Printf("⏸️ 账期间暂停 %v...", pauseDuration)
			time.Sleep(pauseDuration)
		}
	}
	
	// 4. 数据完整性验证
	log.Printf("🔍 [验证] 开始数据完整性验证...")
	validationResult, err := s.validateInitialPullData(ctx, tableName, result)
	if err != nil {
		log.Printf("⚠️ 数据验证失败: %v", err)
	} else {
		result.ValidationResult = validationResult
		log.Printf("✅ [验证] 数据完整性验证通过: %s", validationResult.Summary)
	}
	
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	result.TotalRecords = totalRecords
	
	log.Printf("🎉 [完成] 首次拉取完成！总记录: %d, 总耗时: %v, 平均速度: %.1f记录/分钟",
		totalRecords, result.Duration, float64(totalRecords)/result.Duration.Minutes())
	
	return result, nil
}

// calculateInitialPullTimeRange 计算首次拉取的时间范围（上个月到昨天）
func (s *BillService) calculateInitialPullTimeRange() ([]string, string, error) {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	
	// 计算上个月第一天
	firstDayOfThisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	firstDayOfLastMonth := firstDayOfThisMonth.AddDate(0, -1, 0)
	
	startDate := firstDayOfLastMonth
	endDate := yesterday
	
	// 生成需要拉取的账期列表
	periods := make([]string, 0)
	periodMap := make(map[string]bool)
	
	// 从开始日期到结束日期，按月生成账期
	current := startDate
	for current.Before(endDate) || current.Equal(endDate) {
		period := current.Format("2006-01")
		if !periodMap[period] {
			periods = append(periods, period)
			periodMap[period] = true
		}
		
		// 移动到下个月第一天
		current = time.Date(current.Year(), current.Month()+1, 1, 0, 0, 0, 0, current.Location())
		if current.After(endDate) {
			break
		}
	}
	
	dateRange := fmt.Sprintf("%s 到 %s", 
		startDate.Format("2006-01-02"), 
		endDate.Format("2006-01-02"))
	
	return periods, dateRange, nil
}

// estimateAndPlanPullStrategy 预估数据量并制定拉取策略
func (s *BillService) estimateAndPlanPullStrategy(ctx context.Context, periods []string) (*PullStrategy, error) {
	totalEstimatedRecords := 0
	
	// 对每个账期进行快速采样预估
	for _, period := range periods {
		// 发起小批次请求获取总数
		req := &ListBillDetailRequest{
			BillPeriod:    period,
			Limit:         10, // 最小批次
			NeedRecordNum: 1,  // 需要总记录数
		}
		
		response, err := s.volcClient.ListBillDetail(ctx, req)
		if err != nil {
			// 如果单个账期预估失败，使用保守估算
			log.Printf("⚠️ 账期 %s 预估失败: %v", period, err)
			totalEstimatedRecords += 50000 // 保守估算每个月5万条
			continue
		}
		
		if response != nil && response.Result.Total > 0 {
			totalEstimatedRecords += int(response.Result.Total)
		}
		
		// 账期间短暂停顿，避免预估阶段就触发限流
		time.Sleep(500 * time.Millisecond)
	}
	
	// 基于预估数据量制定策略
	var strategy *PullStrategy
	if totalEstimatedRecords < 10000 {
		// 小量数据：快速策略
		strategy = &PullStrategy{
			EstimatedRecords:  totalEstimatedRecords,
			EstimatedDuration: time.Duration(totalEstimatedRecords/300) * time.Second, // 300记录/秒
			BatchSize:         100,
			ConcurrencyLevel:  1,
			RetryAttempts:     3,
			PauseInterval:     200 * time.Millisecond,
			Description:       "小量数据快速策略",
		}
	} else if totalEstimatedRecords < 100000 {
		// 中量数据：平衡策略
		strategy = &PullStrategy{
			EstimatedRecords:  totalEstimatedRecords,
			EstimatedDuration: time.Duration(totalEstimatedRecords/150) * time.Second, // 150记录/秒
			BatchSize:         50,
			ConcurrencyLevel:  1,
			RetryAttempts:     5,
			PauseInterval:     300 * time.Millisecond,
			Description:       "中量数据平衡策略",
		}
	} else {
		// 大量数据：保守策略
		strategy = &PullStrategy{
			EstimatedRecords:  totalEstimatedRecords,
			EstimatedDuration: time.Duration(totalEstimatedRecords/100) * time.Second, // 100记录/秒
			BatchSize:         30,
			ConcurrencyLevel:  1,
			RetryAttempts:     8,
			PauseInterval:     500 * time.Millisecond,
			Description:       "大量数据保守策略",
		}
	}
	
	return strategy, nil
}

// getConservativePullStrategy 获取保守拉取策略
func (s *BillService) getConservativePullStrategy() *PullStrategy {
	return &PullStrategy{
		EstimatedRecords:  100000, // 保守估算10万条
		EstimatedDuration: 20 * time.Minute,
		BatchSize:         30,
		ConcurrencyLevel:  1,
		RetryAttempts:     10,
		PauseInterval:     1 * time.Second,
		Description:       "保守备用策略",
	}
}

// pullPeriodDataWithStrategy 使用指定策略拉取账期数据
func (s *BillService) pullPeriodDataWithStrategy(ctx context.Context, period, tableName string, isDistributed bool, strategy *PullStrategy) (*SyncResult, error) {
	// 创建优化后的数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(strategy.BatchSize)
	
	// 创建优化后的分页器配置
	config := &PaginatorConfig{
		BatchSize:      strategy.BatchSize,
		MaxRetries:     strategy.RetryAttempts,
		RetryDelay:     strategy.PauseInterval,
		MaxConcurrency: strategy.ConcurrencyLevel,
	}
	
	// 创建分页器
	paginator := NewBillPaginator(s.volcClient, processor, config)
	
	// 设置详细的进度回调
	paginator.SetProgressCallback(func(current, total int, duration time.Duration) {
		percentage := float64(current) / float64(total) * 100
		speed := float64(current) / duration.Minutes()
		eta := time.Duration(float64(total-current)/speed) * time.Minute
		
		log.Printf("  📊 [%s] %d/%d (%.1f%%), 速度: %.0f记录/分钟, 预计剩余: %v", 
			period, current, total, percentage, speed, eta)
	})
	
	// 构建请求
	req := &ListBillDetailRequest{
		BillPeriod:    period,
		GroupPeriod:   1, // 按天分组
		NeedRecordNum: 1, // 需要总记录数
	}
	
	// 执行拉取
	result := &SyncResult{StartTime: time.Now()}
	
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
	
	return result, nil
}

// validateInitialPullData 验证首次拉取的数据完整性
func (s *BillService) validateInitialPullData(ctx context.Context, tableName string, pullResult *InitialPullResult) (*ValidationResult, error) {
	validation := &ValidationResult{
		Details: make(map[string]string),
	}
	
	// 1. 统计预期和实际记录数
	expectedTotal := 0
	for _, syncResult := range pullResult.Results {
		expectedTotal += syncResult.TotalRecords
	}
	validation.ExpectedRecords = expectedTotal
	
	// 2. 查询实际数据库记录数
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	row := s.chClient.QueryRow(ctx, countQuery)
	
	var actualCount int64
	if err := row.Scan(&actualCount); err != nil {
		return validation, fmt.Errorf("查询实际记录数失败: %w", err)
	}
	validation.ActualRecords = int(actualCount)
	
	// 3. 验证各账期数据
	missingPeriods := make([]string, 0)
	incompleteData := make([]string, 0)
	
	for _, period := range pullResult.PullPeriods {
		// 注意：火山引擎表使用Pascal case字段名
		periodCountQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE BillPeriod = '%s'", tableName, period)
		periodRow := s.chClient.QueryRow(ctx, periodCountQuery)
		
		var periodCount int64
		if err := periodRow.Scan(&periodCount); err != nil {
			validation.Details[period] = fmt.Sprintf("查询失败: %v", err)
			incompleteData = append(incompleteData, period)
			continue
		}
		
		expectedPeriodCount := pullResult.Results[period].TotalRecords
		if periodCount == 0 {
			missingPeriods = append(missingPeriods, period)
		} else if int(periodCount) != expectedPeriodCount {
			incompleteData = append(incompleteData, period)
			validation.Details[period] = fmt.Sprintf("预期%d条，实际%d条", expectedPeriodCount, periodCount)
		} else {
			validation.Details[period] = "✓ 数据完整"
		}
	}
	
	validation.MissingPeriods = missingPeriods
	validation.IncompleteData = incompleteData
	
	// 4. 生成验证结果
	if len(missingPeriods) == 0 && len(incompleteData) == 0 && validation.ActualRecords == validation.ExpectedRecords {
		validation.IsValid = true
		validation.Summary = fmt.Sprintf("数据完整性验证通过：%d条记录，%d个账期全部完整", 
			validation.ActualRecords, len(pullResult.PullPeriods))
	} else {
		validation.IsValid = false
		issues := make([]string, 0)
		if len(missingPeriods) > 0 {
			issues = append(issues, fmt.Sprintf("缺失账期: %v", missingPeriods))
		}
		if len(incompleteData) > 0 {
			issues = append(issues, fmt.Sprintf("数据不完整: %v", incompleteData))
		}
		if validation.ActualRecords != validation.ExpectedRecords {
			issues = append(issues, fmt.Sprintf("总数不匹配: 预期%d，实际%d", validation.ExpectedRecords, validation.ActualRecords))
		}
		validation.Summary = fmt.Sprintf("数据完整性验证失败：%s", strings.Join(issues, "；"))
	}
	
	return validation, nil
}

// ListBillDetailSmart 智能账单查询（自动处理上个月到昨天的跨期查询）
func (s *BillService) ListBillDetailSmart(ctx context.Context, req *ListBillDetailRequest) (*SmartBillResponse, error) {
	log.Printf("🧠 [智能查询] 开始智能账单查询")
	
	// 如果已经指定了BillPeriod，使用普通查询
	if req != nil && req.BillPeriod != "" {
		resp, err := s.volcClient.ListBillDetail(ctx, req)
		if err != nil {
			return nil, err
		}
		
		return &SmartBillResponse{
			CombinedResult: &CombinedBillResult{
				TotalRecords: int(resp.Result.Total),
				Bills:        resp.Result.List,
				Periods:      []string{req.BillPeriod},
			},
			QueryStrategy: "single_period",
			DateRange:     fmt.Sprintf("%s月数据", req.BillPeriod),
		}, nil
	}
	
	// 智能查询：上个月到昨天
	periods, dateRange, err := s.calculateSmartPeriods()
	if err != nil {
		return nil, fmt.Errorf("智能时间范围计算失败: %w", err)
	}
	
	log.Printf("🧠 [智能查询] 时间范围: %s, 涉及账期: %v", dateRange, periods)
	
	// 如果只涉及一个账期，使用单期查询
	if len(periods) == 1 {
		singleReq := &ListBillDetailRequest{
			BillPeriod: periods[0],
		}
		if req != nil {
			// 继承原请求的其他参数
			singleReq.Limit = req.Limit
			singleReq.Offset = req.Offset
			singleReq.Product = req.Product
			singleReq.BillingMode = req.BillingMode
			singleReq.OwnerID = req.OwnerID
			singleReq.PayerID = req.PayerID
		}
		
		resp, err := s.volcClient.ListBillDetail(ctx, singleReq)
		if err != nil {
			return nil, fmt.Errorf("单期查询失败: %w", err)
		}
		
		return &SmartBillResponse{
			CombinedResult: &CombinedBillResult{
				TotalRecords: int(resp.Result.Total),
				Bills:        resp.Result.List,
				Periods:      periods,
			},
			QueryStrategy: "single_period_smart",
			DateRange:     dateRange,
		}, nil
	}
	
	// 跨账期查询：分别查询每个账期然后合并
	log.Printf("🧠 [智能查询] 执行跨期查询，账期数: %d", len(periods))
	
	var allBills []BillDetail
	totalRecords := 0
	
	for i, period := range periods {
		log.Printf("🧠 [智能查询] [%d/%d] 查询账期: %s", i+1, len(periods), period)
		
		periodReq := &ListBillDetailRequest{
			BillPeriod: period,
			Limit:      1000, // 使用较大的限制以获取更多数据
		}
		if req != nil {
			// 继承原请求的筛选条件
			periodReq.Product = req.Product
			periodReq.BillingMode = req.BillingMode
			periodReq.OwnerID = req.OwnerID
			periodReq.PayerID = req.PayerID
		}
		
		resp, err := s.volcClient.ListBillDetail(ctx, periodReq)
		if err != nil {
			log.Printf("⚠️ [智能查询] 账期 %s 查询失败: %v", period, err)
			continue
		}
		
		// 筛选指定时间范围内的数据
		filteredBills := s.filterBillsByDateRange(resp.Result.List, period)
		allBills = append(allBills, filteredBills...)
		totalRecords += len(filteredBills)
		
		log.Printf("✅ [智能查询] 账期 %s 完成: %d/%d条记录", 
			period, len(filteredBills), len(resp.Result.List))
		
		// 账期间短暂暂停
		if i < len(periods)-1 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	
	// 按时间排序
	s.sortBillsByTime(allBills)
	
	log.Printf("🎉 [智能查询] 跨期查询完成: 共%d条记录, 覆盖%d个账期", totalRecords, len(periods))
	
	return &SmartBillResponse{
		CombinedResult: &CombinedBillResult{
			TotalRecords: totalRecords,
			Bills:        allBills,
			Periods:      periods,
		},
		QueryStrategy: "multi_period_smart",
		DateRange:     dateRange,
	}, nil
}

// calculateSmartPeriods 计算智能查询的账期列表（上个月到昨天）
func (s *BillService) calculateSmartPeriods() ([]string, string, error) {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	
	// 计算上个月第一天
	firstDayOfThisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	firstDayOfLastMonth := firstDayOfThisMonth.AddDate(0, -1, 0)
	
	startDate := firstDayOfLastMonth  // 上个月第一天
	endDate := yesterday              // 昨天
	
	// 生成账期列表
	periods := make([]string, 0)
	periodMap := make(map[string]bool)
	
	// 从开始日期到结束日期，按月生成账期
	current := startDate
	for current.Before(endDate) || current.Equal(endDate) {
		period := current.Format("2006-01")
		if !periodMap[period] {
			periods = append(periods, period)
			periodMap[period] = true
		}
		
		// 移动到下个月第一天
		current = time.Date(current.Year(), current.Month()+1, 1, 0, 0, 0, 0, current.Location())
		if current.After(endDate) {
			break
		}
	}
	
	dateRange := fmt.Sprintf("%s至%s", 
		startDate.Format("2006-01-02"), 
		endDate.Format("2006-01-02"))
	
	return periods, dateRange, nil
}

// filterBillsByDateRange 根据时间范围筛选账单（上个月到昨天）
func (s *BillService) filterBillsByDateRange(bills []BillDetail, period string) []BillDetail {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)
	
	// 计算上个月第一天
	firstDayOfThisMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	firstDayOfLastMonth := firstDayOfThisMonth.AddDate(0, -1, 0)
	
	startDate := firstDayOfLastMonth  // 上个月第一天
	endDate := yesterday              // 昨天
	
	var filtered []BillDetail
	
	for _, bill := range bills {
		// 解析账单日期
		expenseDate, err := time.Parse("2006-01-02", bill.ExpenseDate)
		if err != nil {
			// 如果解析失败，尝试其他格式
			if expenseDate, err = time.Parse("2006-01", bill.ExpenseDate); err != nil {
				log.Printf("⚠️ 无法解析费用日期: %s", bill.ExpenseDate)
				continue
			}
		}
		
		// 检查是否在目标时间范围内
		if (expenseDate.After(startDate) || expenseDate.Equal(startDate)) && 
		   (expenseDate.Before(endDate) || expenseDate.Equal(endDate)) {
			filtered = append(filtered, bill)
		}
	}
	
	return filtered
}

// sortBillsByTime 按时间排序账单（多级排序避免数据合并）
func (s *BillService) sortBillsByTime(bills []BillDetail) {
	sort.Slice(bills, func(i, j int) bool {
		// 1. 首先按 BillPeriod 排序
		if bills[i].BillPeriod != bills[j].BillPeriod {
			return bills[i].BillPeriod < bills[j].BillPeriod
		}
		
		// 2. 按 ExpenseDate 排序
		if bills[i].ExpenseDate != bills[j].ExpenseDate {
			dateI, errI := time.Parse("2006-01-02", bills[i].ExpenseDate)
			dateJ, errJ := time.Parse("2006-01-02", bills[j].ExpenseDate)
			
			if errI != nil || errJ != nil {
				// 如果日期解析失败，按字符串排序
				return bills[i].ExpenseDate < bills[j].ExpenseDate
			}
			
			return dateI.Before(dateJ)
		}
		
		// 3. 按 BillDetailID 排序
		if bills[i].BillDetailID != bills[j].BillDetailID {
			return bills[i].BillDetailID < bills[j].BillDetailID
		}
		
		// 4. 按 InstanceNo 排序（实例编号通常唯一）
		if bills[i].InstanceNo != bills[j].InstanceNo {
			return bills[i].InstanceNo < bills[j].InstanceNo
		}
		
		// 5. 按 ExpenseBeginTime 排序（精确到时间）
		if bills[i].ExpenseBeginTime != bills[j].ExpenseBeginTime {
			return bills[i].ExpenseBeginTime < bills[j].ExpenseBeginTime
		}
		
		// 6. 按 Product 产品名称排序
		if bills[i].Product != bills[j].Product {
			return bills[i].Product < bills[j].Product
		}
		
		// 7. 按 Element 计费项排序
		if bills[i].Element != bills[j].Element {
			return bills[i].Element < bills[j].Element
		}
		
		// 8. 按 OriginalBillAmount 原始金额排序
		if bills[i].OriginalBillAmount != bills[j].OriginalBillAmount {
			return bills[i].OriginalBillAmount < bills[j].OriginalBillAmount
		}
		
		// 9. 按 TradeTime 交易时间排序（最后的区分）
		return bills[i].TradeTime < bills[j].TradeTime
	})
}

// IntelligentSyncWithPreCheck 智能同步（借鉴阿里云增量同步策略）
func (s *BillService) IntelligentSyncWithPreCheck(ctx context.Context, billPeriod string, options *IntelligentSyncOptions) (*SyncResult, error) {
	log.Printf("[🤖 火山云智能同步] 开始智能同步，账期: %s", billPeriod)
	
	result := &SyncResult{
		StartTime: time.Now(),
	}
	
	// 1. 确保表结构就绪
	if options.UseDistributed && options.LocalTableName != "" && options.DistributedTableName != "" {
		log.Printf("[🔧 表结构] 确保分布式表就绪")
		if err := s.EnsureDistributedTableReady(ctx, options.LocalTableName, options.DistributedTableName); err != nil {
			result.Error = fmt.Errorf("分布式表就绪失败: %w", err)
			result.EndTime = time.Now()
			result.Duration = result.EndTime.Sub(result.StartTime)
			return result, result.Error
		}
	} else {
		log.Printf("[🔧 表结构] 确保单机表就绪")
		if err := s.CreateBillTable(ctx); err != nil {
			result.Error = fmt.Errorf("单机表创建失败: %w", err)
			result.EndTime = time.Now()
			result.Duration = result.EndTime.Sub(result.StartTime)
			return result, result.Error
		}
	}
	
	// 2. 执行数据预检查
	if options.EnablePreCheck {
		log.Printf("[🔍 数据预检查] 开始数据预检查")
		preCheckReq := &ListBillDetailRequest{
			BillPeriod: billPeriod,
		}
		preCheckResult, err := s.PerformDataPreCheck(ctx, preCheckReq)
		if err != nil {
			log.Printf("⚠️ 预检查失败，继续强制同步: %v", err)
		} else if !preCheckResult.NeedSync {
			// 不需要同步
			log.Printf("✅ %s，跳过同步", preCheckResult.Reason)
			result.EndTime = time.Now()
			result.Duration = result.EndTime.Sub(result.StartTime)
			return result, nil
		} else if preCheckResult.NeedCleanup {
			// 需要先清理数据
			log.Printf("🧹 %s，先清理数据", preCheckResult.Reason)
			targetTable := s.tableName
			if options.UseDistributed {
				targetTable = options.DistributedTableName
			}
			// 注意：火山引擎表按ExpenseDate分区，使用分区函数条件
			yearMonth := strings.Replace(billPeriod, "-", "", 1) // 2025-09 -> 202509
			cleanCondition := fmt.Sprintf("toYYYYMM(toDate(ExpenseDate)) = %s", yearMonth)
			// 使用增强版清理（支持分区删除）
			cleanupOpts := &clickhouse.CleanupOptions{
				Condition:   cleanCondition,
				Args:        nil,
				DryRun:      false,
				ProgressLog: true,
			}
			_, err := s.chClient.EnhancedCleanTableData(ctx, targetTable, cleanupOpts)
			if err != nil {
				log.Printf("⚠️ 数据清理失败，继续同步: %v", err)
			} else {
				log.Printf("✅ 数据清理完成")
			}
		}
	} else {
		log.Printf("⚙️ 跳过数据预检查，直接同步")
	}
	
	// 3. 执行智能同步
	log.Printf("🚀 开始执行同步")
	targetTable := s.tableName
	if options.UseDistributed {
		targetTable = options.DistributedTableName
	}
	
	syncResult, err := s.SyncAllBillDataBestPractice(ctx, billPeriod, targetTable, options.UseDistributed)
	if err != nil {
		result.Error = fmt.Errorf("同步执行失败: %w", err)
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		return result, result.Error
	}
	
	// 4. 填充结果
	result.TotalRecords = syncResult.TotalRecords
	result.FetchedRecords = syncResult.FetchedRecords
	result.InsertedRecords = syncResult.InsertedRecords
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	
	// 5. 后处理（如果启用）
	if options.EnablePostProcessing {
		log.Printf("📊 执行同步后处理")
		if err := s.performPostSyncProcessing(ctx, targetTable, billPeriod, result); err != nil {
			log.Printf("⚠️ 同步后处理警告: %v", err)
		}
	}
	
	log.Printf("🎉 智能同步完成: 获取 %d 条，插入 %d 条，耗时 %v",
		result.FetchedRecords, result.InsertedRecords, result.Duration)
	
	return result, nil
}

// IntelligentSyncOptions 智能同步选项（借鉴阿里云）
type IntelligentSyncOptions struct {
	UseDistributed       bool   `json:"use_distributed"`       // 是否使用分布式表
	LocalTableName       string `json:"local_table_name"`      // 本地表名
	DistributedTableName string `json:"distributed_table_name"` // 分布式表名
	EnablePreCheck       bool   `json:"enable_pre_check"`      // 启用数据预检查
	EnablePostProcessing bool   `json:"enable_post_processing"` // 启用后处理
	SkipEmptyPeriods     bool   `json:"skip_empty_periods"`    // 跳过空数据账期
	ForceCleanBeforeSync bool   `json:"force_clean_before_sync"` // 强制同步前清理
}

// DefaultIntelligentSyncOptions 默认智能同步选项
func DefaultIntelligentSyncOptions() *IntelligentSyncOptions {
	return &IntelligentSyncOptions{
		UseDistributed:       false,
		EnablePreCheck:       true,
		EnablePostProcessing: true,
		SkipEmptyPeriods:     true,
		ForceCleanBeforeSync: false,
	}
}

// performPostSyncProcessing 同步后处理（借鉴阿里云）
func (s *BillService) performPostSyncProcessing(ctx context.Context, tableName, billPeriod string, result *SyncResult) error {
	log.Printf("📊 开始同步后处理")
	
	// 1. 数据一致性检查
	log.Printf("🔍 数据一致性检查")
	// 注意：火山引擎表使用Pascal case字段名
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE BillPeriod = '%s'", tableName, billPeriod)
	rows, err := s.chClient.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("数据一致性检查失败: %w", err)
	}
	defer rows.Close()
	
	var finalCount int64
	if rows.Next() {
		if err := rows.Scan(&finalCount); err != nil {
			return fmt.Errorf("扫描统计失败: %w", err)
		}
	}
	
	if finalCount != int64(result.InsertedRecords) {
		log.Printf("⚠️ 数据一致性警告: 插入%d条，实际%d条", result.InsertedRecords, finalCount)
	} else {
		log.Printf("✅ 数据一致性检查通过")
	}
	
	// 2. 表结构优化（ReplacingMergeTree去重）
	if strings.Contains(tableName, "_distributed") {
		// 分布式表优化本地表
		localTableName := strings.Replace(tableName, "_distributed", "_local", 1)
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s ON CLUSTER %s FINAL", localTableName, s.chClient.GetClusterName())
		log.Printf("⚙️ 执行ReplacingMergeTree去重: %s", optimizeQuery)
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			log.Printf("⚠️ 表优化警告: %v", err)
		}
	} else {
		// 普通表优化
		optimizeQuery := fmt.Sprintf("OPTIMIZE TABLE %s FINAL", tableName)
		log.Printf("⚙️ 执行ReplacingMergeTree去重: %s", optimizeQuery)
		if err := s.chClient.Exec(ctx, optimizeQuery); err != nil {
			log.Printf("⚠️ 表优化警告: %v", err)
		}
	}
	
	// 3. 更新统计信息
	log.Printf("📋 更新表统计信息")
	if tableInfo, err := s.chClient.GetTableInfo(ctx, tableName); err == nil {
		log.Printf("📊 表统计: 记录数=%d, 大小=%d bytes", tableInfo.TotalRows, tableInfo.TotalBytes)
	}
	
	log.Printf("✅ 同步后处理完成")
	return nil
}

// BatchIntelligentSync 批量智能同步（多个账期）
func (s *BillService) BatchIntelligentSync(ctx context.Context, billPeriods []string, options *IntelligentSyncOptions) ([]*SyncResult, error) {
	log.Printf("🚀 开始批量智能同步，账期数量: %d", len(billPeriods))
	
	results := make([]*SyncResult, 0, len(billPeriods))
	var errors []error
	
	for i, period := range billPeriods {
		log.Printf("[进度 %d/%d] 同步账期: %s", i+1, len(billPeriods), period)
		
		result, err := s.IntelligentSyncWithPreCheck(ctx, period, options)
		if err != nil {
			log.Printf("⚠️ 账期 %s 同步失败: %v", period, err)
			errors = append(errors, fmt.Errorf("账期 %s: %w", period, err))
			// 继续同步其他账期
		}
		
		results = append(results, result)
		
		// 简单的间隔控制，避免对API造成过大压力
		if i < len(billPeriods)-1 {
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return results, ctx.Err()
			}
		}
	}
	
	// 统计总结果
	successCount := 0
	for _, result := range results {
		if result != nil && result.Error == nil {
			successCount++
		}
	}
	
	log.Printf("🎉 批量同步完成: %d/%d 成功，%d 失败", successCount, len(billPeriods), len(errors))
	
	if len(errors) > 0 {
		return results, fmt.Errorf("部分同步失败: %v", errors)
	}
	
	return results, nil
}

// DataComparisonResult 数据对比结果结构体
type DataComparisonResult struct {
	APICount      int32        // API返回的数据总数
	DatabaseCount int64        // 数据库中的记录总数
	NeedSync      bool         // 是否需要同步
	NeedCleanup   bool         // 是否需要先清理数据
	Reason        string       // 决策原因
	Period        string       // 时间段
	Granularity   string       // 粒度（daily/monthly）
	FirstPageData []BillDetail // 第一页数据（避免重复获取）
	FirstResponse *ListBillDetailResponse // 第一次API响应（包含总数信息）
}

// GetBillDataCount 获取账单数据总数（通用方法）
func (s *BillService) GetBillDataCount(ctx context.Context, granularity, period string) (int32, error) {
	log.Printf("[火山云API数据量检查] 开始获取 %s %s 的API数据总量", granularity, period)

	// 构建请求参数，只需要获取第一页和总记录数
	req := &ListBillDetailRequest{
		BillPeriod:    period,
		Limit:         1,           // 只需要1条记录来获取总数
		Offset:        0,
		NeedRecordNum: 1,           // 需要返回总记录数
	}

	// 执行请求
	resp, err := s.volcClient.ListBillDetail(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to get API data count: %w", err)
	}

	log.Printf("[火山云API数据量检查] 获取到API数据总量: %d", resp.Result.Total)
	return resp.Result.Total, nil
}

// GetDatabaseRecordCount 获取数据库记录总数（通用方法）
func (s *BillService) GetDatabaseRecordCount(ctx context.Context, granularity, period string) (int64, error) {
	log.Printf("[火山云数据库检查] 开始获取 %s %s 的数据库记录总数", granularity, period)

	// 获取实际表名
	resolver := s.chClient.GetTableNameResolver()
	actualTableName := resolver.ResolveQueryTarget(s.tableName)

	// 构建查询条件 - BillPeriod是String类型，直接字符串匹配
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE BillPeriod = '%s'",
		actualTableName, period) // 直接使用 2025-08 格式

	var count uint64  // 修改为uint64类型以匹配ClickHouse的COUNT()返回值
	err := s.chClient.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get database record count: %w", err)
	}

	log.Printf("[火山云数据库检查] 获取到数据库记录总数: %d", count)
	return int64(count), nil  // 转换为int64返回
}

// PerformDataComparison 执行数据比较（决定是否需要同步）
func (s *BillService) PerformDataComparison(ctx context.Context, granularity, period string) (*DataComparisonResult, error) {
	log.Printf("[火山云数据预检查] 开始比较 %s %s 的数据量", granularity, period)

	// 获取第一批数据（避免后续重复API调用）
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 50 // 默认批次大小
	}
	
	req := &ListBillDetailRequest{
		BillPeriod:    period,
		Limit:         int32(batchSize), // 获取第一批数据
		Offset:        0,
		NeedRecordNum: 1, // 需要返回总记录数
	}

	// 执行API请求，获取第一页数据和总数
	firstResp, err := s.volcClient.ListBillDetail(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get first page data: %w", err)
	}

	apiCount := firstResp.Result.Total
	log.Printf("[火山云数据预检查] 获取到API数据总量: %d, 第一页: %d条", apiCount, len(firstResp.Result.List))

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
		FirstPageData: firstResp.Result.List, // 保存第一页数据
		FirstResponse: firstResp,             // 保存完整响应
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
		result.Reason = fmt.Sprintf("数据量不一致(API:%d vs DB:%d)，需要清理并重新同步", apiCount, dbCount)
	}

	log.Printf("[火山云数据预检查] 对比结果: API:%d, DB:%d, 需要同步:%v, 需要清理:%v, 原因:%s",
		apiCount, dbCount, result.NeedSync, result.NeedCleanup, result.Reason)

	return result, nil
}

// SyncAllBillDataWithFirstPage 使用预先获取的第一页数据同步账单数据（避免重复API调用）
func (s *BillService) SyncAllBillDataWithFirstPage(ctx context.Context, billPeriod, tableName string, isDistributed bool, comparisonResult *DataComparisonResult) (*SyncResult, error) {
	log.Printf("[火山云优化同步] 开始同步账期 %s，使用缓存的第一页数据", billPeriod)
	
	result := &SyncResult{
		StartTime: time.Now(),
	}

	// 创建数据处理器
	processor := NewClickHouseProcessor(s.chClient, tableName, isDistributed)
	processor.SetBatchSize(500)

	// 处理第一页数据
	if len(comparisonResult.FirstPageData) > 0 {
		log.Printf("[火山云优化同步] 处理缓存的第一页数据: %d条", len(comparisonResult.FirstPageData))
		if err := processor.Process(ctx, comparisonResult.FirstPageData); err != nil {
			return result, fmt.Errorf("failed to process first page data: %w", err)
		}
	}

	totalRecords := int(comparisonResult.APICount)
	processedRecords := len(comparisonResult.FirstPageData)
	
	// 如果第一页就是全部数据，直接返回
	if processedRecords >= totalRecords {
		result.TotalRecords = processedRecords
		result.InsertedRecords = processedRecords
		result.FetchedRecords = processedRecords
		result.EndTime = time.Now()
		result.Duration = result.EndTime.Sub(result.StartTime)
		log.Printf("[火山云优化同步] 同步完成，只有一页数据: %d条", processedRecords)
		return result, nil
	}

	// 继续获取剩余页面
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 50
	}

	for offset := len(comparisonResult.FirstPageData); offset < totalRecords; offset += batchSize {
		req := &ListBillDetailRequest{
			BillPeriod:    billPeriod,
			Limit:         int32(batchSize),
			Offset:        int32(offset),
			NeedRecordNum: 0, // 不需要再获取总数
		}

		log.Printf("[火山云优化同步] 获取第 %d-%d 条记录", offset+1, min(offset+batchSize, totalRecords))
		resp, err := s.volcClient.ListBillDetail(ctx, req)
		if err != nil {
			return result, fmt.Errorf("failed to get page data at offset %d: %w", offset, err)
		}

		if len(resp.Result.List) > 0 {
			if err := processor.Process(ctx, resp.Result.List); err != nil {
				return result, fmt.Errorf("failed to process page data: %w", err)
			}
			processedRecords += len(resp.Result.List)
		}

		// 如果返回的数据少于请求的数量，说明已经到最后了
		if len(resp.Result.List) < batchSize {
			break
		}
	}

	result.TotalRecords = processedRecords
	result.InsertedRecords = processedRecords  
	result.FetchedRecords = processedRecords
	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(result.StartTime)
	
	log.Printf("[火山云优化同步] 同步完成: 总记录=%d, 已处理=%d, 耗时=%v", 
		totalRecords, processedRecords, result.Duration)
	
	return result, nil
}

// min 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
