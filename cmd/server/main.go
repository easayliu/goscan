package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"goscan/pkg/alicloud"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/volcengine"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	var (
		configPath     = flag.String("config", "", "配置文件路径")
		cloudProvider  = flag.String("provider", "volcengine", "云服务提供商 (volcengine|alicloud|aws|azure|gcp)")
		product        = flag.String("product", "", "产品名称")
		billingMode    = flag.String("billing-mode", "", "计费模式")
		ownerID        = flag.String("owner-id", "", "所有者ID")
		billPeriod     = flag.String("bill-period", "", "账期 (YYYY-MM)，为空则根据配置决定")
		limit          = flag.Int("limit", 100, "每次请求数量")
		createTable    = flag.Bool("create-table", false, "是否创建表")
		recreateTable  = flag.Bool("recreate-table", false, "重建分布式表（删除后重新创建）")
		syncAll        = flag.Bool("sync-all", false, "同步指定账期的所有数据")
		syncAllPeriods = flag.Bool("sync-all-periods", false, "同步所有可用账期的数据")
		startPeriod    = flag.String("start-period", "", "开始账期 (YYYY-MM)")
		endPeriod      = flag.String("end-period", "", "结束账期 (YYYY-MM)")
		useDistributed = flag.Bool("distributed", false, "使用分布式表")
		demoMode       = flag.Bool("demo", false, "运行演示模式")
		cleanBefore    = flag.Bool("clean-before", false, "入库前清理数据")
		cleanCondition = flag.String("clean-condition", "", "清理条件，为空则清理当前月数据")
		cleanPreview   = flag.Bool("clean-preview", false, "仅预览清理操作而不实际执行")
		cleanAll       = flag.Bool("clean-all", false, "清空整个表（慎用）")
		dropTable      = flag.Bool("drop-table", false, "删除表（危险操作）")
		dropOldTable   = flag.String("drop-old-table", "", "删除指定的旧表名")
		granularity    = flag.String("granularity", "monthly", "账单粒度 (monthly|daily|both)，仅阿里云支持")
		syncDays       = flag.Int("sync-days", 0, "同步最近N天的按天账单（0表示整月），仅阿里云按天粒度有效")
		syncYesterday  = flag.Bool("sync-yesterday", false, "仅同步昨天的天表数据，仅阿里云支持")
		syncLastMonth  = flag.Bool("sync-last-month", false, "仅同步上月的月表数据，仅阿里云支持")
		syncOptimal    = flag.Bool("sync-optimal", false, "智能同步模式：昨天的天表数据 + 上月的月表数据，仅阿里云支持")
		forceUpdate    = flag.Bool("force-update", false, "强制更新已存在的数据（会先清理再同步），仅阿里云支持")
		skipExisting   = flag.Bool("skip-existing", false, "跳过已存在的数据（智能检测避免重复同步），仅阿里云支持")
	)
	flag.Parse()

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 创建ClickHouse客户端
	chClient, err := clickhouse.NewClient(cfg.ClickHouse)
	if err != nil {
		log.Fatalf("Failed to create ClickHouse client: %v", err)
	}
	defer chClient.Close()

	ctx := context.Background()

	// 检查ClickHouse连接
	if err := chClient.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping ClickHouse: %v", err)
	}
	fmt.Println("ClickHouse 连接成功")

	// 如果是演示模式，运行演示代码
	if *demoMode {
		runDemo(ctx, cfg, chClient)
		return
	}

	// 根据云服务提供商执行相应操作
	switch strings.ToLower(*cloudProvider) {
	case "volcengine":
		runVolcEngineSync(ctx, cfg, chClient, &SyncParams{
			Product:        *product,
			BillingMode:    *billingMode,
			OwnerID:        *ownerID,
			BillPeriod:     *billPeriod,
			Limit:          *limit,
			CreateTable:    *createTable,
			RecreateTable:  *recreateTable,
			SyncAll:        *syncAll,
			SyncAllPeriods: *syncAllPeriods,
			StartPeriod:    *startPeriod,
			EndPeriod:      *endPeriod,
			UseDistributed: *useDistributed,
			CleanBefore:    *cleanBefore,
			CleanCondition: *cleanCondition,
			CleanPreview:   *cleanPreview,
			CleanAll:       *cleanAll,
			DropTable:      *dropTable,
			DropOldTable:   *dropOldTable,
		})
	case "alicloud":
		runAliCloudSync(ctx, cfg, chClient, &SyncParams{
			Product:        *product,
			BillingMode:    *billingMode,
			OwnerID:        *ownerID,
			BillPeriod:     *billPeriod,
			Limit:          *limit,
			CreateTable:    *createTable,
			RecreateTable:  *recreateTable,
			SyncAll:        *syncAll,
			SyncAllPeriods: *syncAllPeriods,
			StartPeriod:    *startPeriod,
			EndPeriod:      *endPeriod,
			UseDistributed: *useDistributed,
			CleanBefore:    *cleanBefore,
			CleanCondition: *cleanCondition,
			CleanPreview:   *cleanPreview,
			CleanAll:       *cleanAll,
			DropTable:      *dropTable,
			DropOldTable:   *dropOldTable,
			Granularity:    *granularity,
			SyncDays:       *syncDays,
			SyncYesterday:  *syncYesterday,
			SyncLastMonth:  *syncLastMonth,
			SyncOptimal:    *syncOptimal,
			ForceUpdate:    *forceUpdate,
			SkipExisting:   *skipExisting,
		})
	case "aws":
		log.Fatal("AWS 支持即将推出")
	case "azure":
		log.Fatal("Azure 支持即将推出")
	case "gcp":
		log.Fatal("GCP 支持即将推出")
	default:
		log.Fatalf("不支持的云服务提供商: %s。支持的提供商: volcengine, alicloud, aws, azure, gcp", *cloudProvider)
	}
}

type SyncParams struct {
	Product        string
	BillingMode    string
	OwnerID        string
	BillPeriod     string
	Limit          int
	CreateTable    bool
	RecreateTable  bool
	SyncAll        bool
	SyncAllPeriods bool
	StartPeriod    string
	EndPeriod      string
	UseDistributed bool
	CleanBefore    bool
	CleanCondition string
	CleanPreview   bool
	CleanAll       bool
	DropTable      bool
	DropOldTable   string
	Granularity    string // 阿里云特有：monthly, daily, both
	SyncDays       int    // 阿里云特有：同步最近N天
	SyncYesterday  bool   // 阿里云特有：仅同步昨天的天表数据
	SyncLastMonth  bool   // 阿里云特有：仅同步上月的月表数据
	SyncOptimal    bool   // 阿里云特有：智能同步模式
	ForceUpdate    bool   // 阿里云特有：强制更新已存在的数据
	SkipExisting   bool   // 阿里云特有：跳过已存在的数据
}

func runVolcEngineSync(ctx context.Context, cfg *config.Config, chClient *clickhouse.Client, params *SyncParams) {
	fmt.Println("=== 火山云账单数据同步 ===")

	// 获取火山云配置
	volcConfig := cfg.GetVolcEngineConfig()

	// 验证火山云配置
	if volcConfig.AccessKey == "" || volcConfig.SecretKey == "" {
		log.Fatal("火山云 AccessKey 和 SecretKey 必须配置")
	}

	// 创建账单服务
	billService, err := volcengine.NewBillService(volcConfig, chClient)
	if err != nil {
		log.Fatalf("Failed to create bill service: %v", err)
	}

	// 处理删除表操作
	if params.DropTable || params.DropOldTable != "" {
		handleDropTableOperations(ctx, billService, cfg, params)
		return // 删除表后直接返回，不执行其他操作
	}

	// 处理表创建/重建逻辑
	if params.RecreateTable && params.UseDistributed && cfg.ClickHouse.Cluster != "" {
		// 重建分布式表模式
		fmt.Println("🔄 正在重建火山云分布式表...")
		localTableName := "volcengine_bill_details_local"
		distributedTableName := "volcengine_bill_details_distributed"

		if err := billService.RecreateDistributedBillTable(ctx, localTableName, distributedTableName); err != nil {
			log.Fatalf("Failed to recreate distributed bill table: %v", err)
		}
		fmt.Printf("✅ 火山云分布式表 %s 重建完成\n", distributedTableName)
	} else if params.CreateTable {
		// 普通创建表模式
		fmt.Println("正在创建火山云账单表...")

		if params.UseDistributed && cfg.ClickHouse.Cluster != "" {
			localTableName := "volcengine_bill_details_local"
			distributedTableName := "volcengine_bill_details_distributed"

			if err := billService.CreateDistributedBillTable(ctx, localTableName, distributedTableName); err != nil {
				log.Fatalf("Failed to create distributed bill table: %v", err)
			}
			fmt.Printf("火山云分布式表 %s 创建成功\n", distributedTableName)
		} else {
			if err := billService.CreateBillTable(ctx); err != nil {
				log.Fatalf("Failed to create bill table: %v", err)
			}
			fmt.Println("火山云账单表创建成功")
		}
	}

	// 决定同步模式和账期
	var periodsToSync []string
	var syncMode string

	if params.SyncAllPeriods {
		// 显式指定同步所有账期
		syncMode = "all_periods"
		periods, err2 := getBillPeriods(ctx, billService, volcConfig)
		if err2 != nil {
			log.Printf("[错误处理] 无法获取可用账期，使用默认配置: %v", err2)
			periodsToSync = generateDefaultPeriods(volcConfig.MaxHistoricalMonths)
		} else {
			periodsToSync = periods
		}
	} else if params.StartPeriod != "" || params.EndPeriod != "" {
		// 指定时间范围同步
		syncMode = "range"
		log.Printf("[账期处理] 指定时间范围同步: %s -> %s", params.StartPeriod, params.EndPeriod)
		periodsToSync = generatePeriodRange(params.StartPeriod, params.EndPeriod)

		// 验证时间范围内的账期是否被火山云API支持
		validPeriods := []string{}
		for _, period := range periodsToSync {
			if err := volcengine.ValidateBillPeriod(period); err != nil {
				log.Printf("[账期验证] 跳过无效账期 %s: %v", period, err)
				continue
			}
			validPeriods = append(validPeriods, period)
		}
		periodsToSync = validPeriods
		log.Printf("[账期验证] 范围同步最终有效账期: %v", periodsToSync)
	} else if params.BillPeriod != "" {
		// 指定单个账期
		syncMode = "single_period"
		if err := volcengine.ValidateBillPeriod(params.BillPeriod); err != nil {
			log.Fatalf("[账期验证] 指定的账期无效 %s: %v", params.BillPeriod, err)
		}
		periodsToSync = []string{params.BillPeriod}
		log.Printf("[账期处理] 单个账期同步: %s", params.BillPeriod)
	} else {
		// 根据配置决定默认行为
		switch volcConfig.DefaultSyncMode {
		case "all_periods":
			syncMode = "all_periods"
			periods, err3 := getBillPeriods(ctx, billService, volcConfig)
			if err3 != nil {
				log.Printf("[错误处理] 无法获取可用账期，使用默认配置: %v", err3)
				periodsToSync = generateDefaultPeriods(volcConfig.MaxHistoricalMonths)
			} else {
				periodsToSync = periods
			}
		case "range":
			syncMode = "range"
			periodsToSync = generatePeriodRange(volcConfig.DefaultStartPeriod, volcConfig.DefaultEndPeriod)
		default: // current_period
			syncMode = "current_period"
			periodsToSync = []string{""} // 空字符串表示当前账期
		}
	}

	fmt.Printf("同步模式: %s\n", syncMode)
	fmt.Printf("账期列表: %v\n", periodsToSync)

	// 添加额外的账期验证和警告
	if len(periodsToSync) == 0 {
		log.Printf("[警告] 没有可用的有效账期，请检查配置或API限制")
		return
	}

	// 检查是否有重复账期（防止重复拉取）
	uniquePeriods := make(map[string]bool)
	finalPeriods := []string{}
	for _, period := range periodsToSync {
		if !uniquePeriods[period] {
			uniquePeriods[period] = true
			finalPeriods = append(finalPeriods, period)
		} else {
			log.Printf("[去重] 检测到重复账期 %s，已自动去重", period)
		}
	}
	periodsToSync = finalPeriods
	fmt.Printf("去重后账期列表: %v\n", periodsToSync)

	// 处理数据清理逻辑
	var cleanCondition string
	var cleanArgs []interface{}
	var isDryRun bool = params.CleanPreview

	if params.CleanBefore || params.CleanPreview {
		fmt.Printf("🔧 检测到清理参数: CleanBefore=%v, CleanPreview=%v\n", params.CleanBefore, params.CleanPreview)
		if params.CleanAll {
			// 清空整个表 - 需要额外确认
			if !params.CleanPreview && !confirmDangerousOperation("清空整个表") {
				log.Fatal("操作已取消")
			}
			cleanCondition = ""
			fmt.Printf("⚠️  将清空整个表的所有数据！\n")
		} else if params.CleanCondition != "" {
			// 使用用户指定的清理条件
			cleanCondition = params.CleanCondition
			fmt.Printf("清理条件: '%s'\n", cleanCondition)

			// 如果是危险的条件，需要确认
			if !params.CleanPreview && isDangerousCondition(cleanCondition) {
				if !confirmDangerousOperation(fmt.Sprintf("执行清理条件: %s", cleanCondition)) {
					log.Fatal("操作已取消")
				}
			}
		} else {
			// 默认清理当前月数据
			cleanCondition = "toYYYYMM(toDate(expense_date)) = toYYYYMM(now())"
			fmt.Printf("清理条件: '当前月数据' (%s)\n", cleanCondition)
		}
		fmt.Printf("🗑️  最终清理条件: '%s'\n", cleanCondition)
		fmt.Printf("🏃 isDryRun模式: %v\n", isDryRun)

		if params.CleanPreview {
			fmt.Printf("🔍 预览模式：将仅显示清理统计信息，不会实际删除数据\n")
		} else {
			fmt.Printf("🗑️  执行模式：将实际删除符合条件的数据\n")
		}
	}

	// 执行多账期同步
	var totalResult *volcengine.SyncResult
	var totalInserted int
	var totalFetched int
	startTime := time.Now()

	// 标记是否已经执行过清理（只在第一个账期执行清理）
	var hasCleanedBefore bool = false

	for i, period := range periodsToSync {
		fmt.Printf("\n=== 同步账期 %d/%d: %s ===\n", i+1, len(periodsToSync), getPeriodDisplay(period))

		// 只在第一个账期执行清理操作
		shouldCleanThisTime := (params.CleanBefore || params.CleanPreview) && !hasCleanedBefore
		if shouldCleanThisTime {
			hasCleanedBefore = true
			fmt.Printf("🧹 在第一个账期前执行清理操作\n")
		}

		// 构建请求参数
		req := &volcengine.ListBillDetailRequest{
			BillPeriod:  period,
			Limit:       int32(params.Limit),
			Offset:      0,
			GroupPeriod: 1, // 默认按天分组
		}

		// 处理可选参数（转换为新的数组格式）
		if params.Product != "" {
			req.Product = []string{params.Product}
		}
		if params.BillingMode != "" {
			req.BillingMode = []string{params.BillingMode}
		}
		if params.OwnerID != "" {
			if ownerID, err := strconv.ParseInt(params.OwnerID, 10, 64); err == nil {
				req.OwnerID = []int64{ownerID}
			} else {
				log.Printf("警告: 无法解析OwnerID '%s': %v", params.OwnerID, err)
			}
		}

		var result *volcengine.SyncResult

		// 检查是否使用重建+同步模式
		if params.RecreateTable && params.UseDistributed && cfg.ClickHouse.Cluster != "" {
			// 使用重建分布式表+串行同步模式
			distributedTableName := "volcengine_bill_details_distributed"
			fmt.Printf("🔄 使用重建+串行同步模式\n")
			fmt.Printf("📊 这种模式将删除旧表、重新创建、然后串行写入数据，确保数据一致性\n")

			result, err = billService.SyncAllBillDataWithRecreateDistributed(ctx, period, distributedTableName)
		} else if params.SyncAll {
			// 同步指定账期的所有数据
			fmt.Printf("🚀 使用SyncAll模式同步\n")
			if params.UseDistributed && cfg.ClickHouse.Cluster != "" {
				// 使用分布式表 + 最佳实践
				distributedTableName := "volcengine_bill_details_distributed"
				fmt.Println("使用最佳实践 + 分布式表方法获取所有数据...")
				if shouldCleanThisTime {
					fmt.Printf("🧹 使用带清理功能的SyncAll方法\n")
					result, err = billService.SyncAllBillDataBestPracticeWithCleanupAndPreview(ctx, period, distributedTableName, true, cleanCondition, isDryRun, cleanArgs)
				} else {
					result, err = billService.SyncAllBillDataBestPractice(ctx, period, distributedTableName, true)
				}
			} else {
				// 使用普通表 + 最佳实践
				tableName := "volcengine_bill_details"
				fmt.Println("使用最佳实践方法获取所有数据...")
				if shouldCleanThisTime {
					fmt.Printf("🧹 使用带清理功能的SyncAll方法\n")
					result, err = billService.SyncAllBillDataBestPracticeWithCleanupAndPreview(ctx, period, tableName, false, cleanCondition, isDryRun, cleanArgs)
				} else {
					result, err = billService.SyncAllBillDataBestPractice(ctx, period, tableName, false)
				}
			}
		} else if params.UseDistributed && cfg.ClickHouse.Cluster != "" {
			// 同步到分布式表
			distributedTableName := "volcengine_bill_details_distributed"
			if shouldCleanThisTime {
				fmt.Printf("🧹 使用带清理功能的分布式同步方法\n")
				result, err = billService.SyncBillDataToDistributedWithCleanupAndPreview(ctx, distributedTableName, req, cleanCondition, isDryRun, cleanArgs...)
			} else {
				result, err = billService.SyncBillDataToDistributed(ctx, distributedTableName, req)
			}
		} else {
			// 普通同步
			fmt.Printf("⚡ 使用普通同步模式\n")
			if shouldCleanThisTime {
				fmt.Printf("🧹 调用带清理功能的普通同步方法\n")
				result, err = billService.SyncBillDataWithCleanupAndPreview(ctx, req, cleanCondition, isDryRun, cleanArgs...)
			} else {
				fmt.Printf("📥 调用普通同步方法（无清理）\n")
				result, err = billService.SyncBillData(ctx, req)
			}
		}

		if err != nil {
			log.Printf("账期 %s 同步失败: %v", getPeriodDisplay(period), err)
			if !volcConfig.SkipEmptyPeriods {
				// 如果配置为不跳过错误，则终止整个同步过程
				log.Fatalf("同步过程因错误终止: %v", err)
			}
			continue
		}

		if result != nil {
			totalInserted += result.InsertedRecords
			totalFetched += result.FetchedRecords

			fmt.Printf("账期 %s 同步完成: 获取 %d 条，插入 %d 条，耗时 %v\n",
				getPeriodDisplay(period), result.FetchedRecords, result.InsertedRecords, result.Duration)

			// 保存第一个成功的结果作为模板
			if totalResult == nil {
				totalResult = result
			}
		}
	}

	// 构建总结果
	if totalResult == nil {
		totalResult = &volcengine.SyncResult{
			StartTime: startTime,
			EndTime:   time.Now(),
		}
	}
	totalResult.StartTime = startTime
	totalResult.EndTime = time.Now()
	totalResult.Duration = totalResult.EndTime.Sub(startTime)
	totalResult.InsertedRecords = totalInserted
	totalResult.FetchedRecords = totalFetched

	result := totalResult

	if err != nil {
		log.Fatalf("Failed to sync bill data: %v", err)
	}

	// 输出总结果
	fmt.Printf("\n=== 火山云同步总结 ===\n")
	fmt.Printf("同步模式: %s\n", syncMode)
	fmt.Printf("同步账期: %v\n", periodsToSync)
	fmt.Printf("开始时间: %s\n", result.StartTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("结束时间: %s\n", result.EndTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("总耗时: %v\n", result.Duration)
	fmt.Printf("总获取记录数: %d\n", result.FetchedRecords)
	fmt.Printf("总插入记录数: %d\n", result.InsertedRecords)

	if result.Error != nil {
		fmt.Printf("错误: %v\n", result.Error)
	} else {
		fmt.Println("🎉 所有账期同步完成!")
	}

	// 查询验证数据
	if result.InsertedRecords > 0 {
		fmt.Println("\n验证插入的数据...")
		tableName := "volcengine_bill_details"
		if params.UseDistributed && cfg.ClickHouse.Cluster != "" {
			tableName = "volcengine_bill_details_distributed"
		}

		query := fmt.Sprintf("SELECT count(*) as total, count(preferential_bill_amount) as amount_records FROM %s WHERE toYYYYMM(toDate(expense_date)) = toYYYYMM(now())", tableName)

		rows, err := chClient.Query(ctx, query)
		if err != nil {
			log.Printf("Failed to query validation data: %v", err)
		} else {
			defer rows.Close()

			if rows.Next() {
				var total uint64
				var amountRecords uint64

				if err := rows.Scan(&total, &amountRecords); err != nil {
					log.Printf("Failed to scan validation data: %v", err)
				} else {
					fmt.Printf("数据库中共有 %d 条记录，其中 %d 条有金额数据\n", total, amountRecords)
				}
			}
		}
	}
}

func runDemo(ctx context.Context, cfg *config.Config, chClient *clickhouse.Client) {
	fmt.Println("=== goscan 多云演示模式 ===")

	// ClickHouse 基本功能演示
	fmt.Println("\n1. ClickHouse 基本功能演示")

	// 示例：创建表
	tableName := "demo_table"
	schema := `(
		id UInt64,
		name String,
		timestamp DateTime,
		value Float64
	) ENGINE = MergeTree()
	ORDER BY id`

	if err := chClient.CreateTable(ctx, tableName, schema); err != nil {
		fmt.Printf("创建表失败: %v\n", err)
	} else {
		fmt.Printf("表 %s 创建成功\n", tableName)
	}

	// 示例：插入数据
	sampleData := []map[string]interface{}{
		{"id": 1, "name": "demo1", "timestamp": "2023-01-01 12:00:00", "value": 100.0},
		{"id": 2, "name": "demo2", "timestamp": "2023-01-01 13:00:00", "value": 200.0},
		{"id": 3, "name": "demo3", "timestamp": "2023-01-01 14:00:00", "value": 300.0},
	}

	if err := chClient.InsertBatch(ctx, tableName, sampleData); err != nil {
		fmt.Printf("插入数据失败: %v\n", err)
	} else {
		fmt.Printf("成功插入 %d 条记录\n", len(sampleData))
	}

	// 查询数据
	rows, err := chClient.Query(ctx, "SELECT id, name, value FROM "+tableName+" ORDER BY id")
	if err != nil {
		fmt.Printf("查询数据失败: %v\n", err)
	} else {
		defer rows.Close()
		fmt.Println("查询结果:")
		for rows.Next() {
			var id uint64
			var name string
			var value float64

			if err := rows.Scan(&id, &name, &value); err != nil {
				fmt.Printf("扫描行失败: %v\n", err)
				break
			}

			fmt.Printf("  ID: %d, Name: %s, Value: %.2f\n", id, name, value)
		}
	}

	// 集群功能演示
	if cfg.ClickHouse.Cluster != "" {
		fmt.Println("\n2. 集群功能演示")
		fmt.Printf("集群名称: %s\n", cfg.ClickHouse.Cluster)

		clusterInfo, err := chClient.GetClusterInfo(ctx)
		if err != nil {
			fmt.Printf("获取集群信息失败: %v\n", err)
		} else {
			fmt.Printf("集群节点数量: %d\n", len(clusterInfo))
		}
	}

	// 多云服务功能演示
	fmt.Println("\n3. 多云服务支持演示")

	// 火山云功能演示
	fmt.Println("\n3.1 火山云功能演示")
	volcConfig := cfg.GetVolcEngineConfig()
	if volcConfig.AccessKey != "" && volcConfig.SecretKey != "" {
		fmt.Println("✅ 火山云配置已设置")

		billService, err := volcengine.NewBillService(volcConfig, chClient)
		if err != nil {
			fmt.Printf("❌ 创建火山云账单服务失败: %v\n", err)
		} else {
			fmt.Println("✅ 火山云账单服务初始化成功")

			// 创建账单表（演示）
			billTableName := "volcengine_bill_details"
			exists, err := chClient.TableExists(ctx, billTableName)
			if err != nil {
				fmt.Printf("❌ 检查表存在性失败: %v\n", err)
			} else if !exists {
				fmt.Printf("📋 创建火山云账单表 %s...\n", billTableName)
				if err := billService.CreateBillTable(ctx); err != nil {
					fmt.Printf("❌ 创建账单表失败: %v\n", err)
				} else {
					fmt.Printf("✅ 火山云账单表 %s 创建成功\n", billTableName)
				}
			} else {
				fmt.Printf("✅ 火山云账单表 %s 已存在\n", billTableName)
			}
		}
	} else {
		fmt.Println("⚠️  火山云配置未设置")
		fmt.Println("   设置以下环境变量后可进行火山云账单数据同步:")
		fmt.Println("   export VOLCENGINE_ACCESS_KEY=your_access_key")
		fmt.Println("   export VOLCENGINE_SECRET_KEY=your_secret_key")
	}

	// 其他云服务商演示
	fmt.Println("\n3.2 其他云服务商支持")
	fmt.Println("⏳ AWS 支持即将推出")
	fmt.Println("⏳ Azure 支持即将推出")
	fmt.Println("⏳ GCP 支持即将推出")

	fmt.Println("\n=== 演示完成 ===")
	fmt.Println("\n🚀 使用说明:")
	fmt.Println("1. 默认同步（根据配置决定同步模式）:")
	fmt.Println("   go run cmd/server/main.go --provider volcengine --create-table")
	fmt.Println("2. 同步所有可用账期的数据:")
	fmt.Println("   go run cmd/server/main.go --provider volcengine --sync-all-periods --create-table")
	fmt.Println("3. 同步指定账期的所有数据:")
	fmt.Println("   go run cmd/server/main.go --provider volcengine --bill-period \"2024-08\" --sync-all --limit 500")
	fmt.Println("4. 同步指定时间范围的数据:")
	fmt.Println("   go run cmd/server/main.go --provider volcengine --start-period \"2024-01\" --end-period \"2024-08\" --create-table")
	fmt.Println("5. 数据清理选项:")
	fmt.Println("   5.1 入库前清理当前月数据:")
	fmt.Println("       go run cmd/server/main.go --provider volcengine --create-table --clean-before")
	fmt.Println("   5.2 预览清理操作（不实际删除）:")
	fmt.Println("       go run cmd/server/main.go --provider volcengine --clean-preview")
	fmt.Println("   5.3 按条件清理数据:")
	fmt.Println("       go run cmd/server/main.go --provider volcengine --clean-before --clean-condition \"product = 'ECS'\"")
	fmt.Println("   5.4 清空整个表（慎用）:")
	fmt.Println("       go run cmd/server/main.go --provider volcengine --clean-all --clean-before")
	fmt.Println("6. 使用分布式表:")
	fmt.Println("   go run cmd/server/main.go --provider volcengine --distributed --create-table")
	fmt.Println("7. 重建分布式表模式:")
	fmt.Println("   7.1 重建分布式表后同步数据（确保数据一致性）:")
	fmt.Println("       go run cmd/server/main.go --provider volcengine --distributed --recreate-table --bill-period \"2024-08\"")
	fmt.Println("   7.2 重建所有可用账期数据:")
	fmt.Println("       go run cmd/server/main.go --provider volcengine --distributed --recreate-table --sync-all-periods")
	fmt.Println("8. 阿里云智能同步模式:")
	fmt.Println("   8.1 仅同步昨天的天表数据:")
	fmt.Println("       go run cmd/server/main.go --provider alicloud --sync-yesterday --create-table")
	fmt.Println("   8.2 仅同步上月的月表数据:")
	fmt.Println("       go run cmd/server/main.go --provider alicloud --sync-last-month --create-table")
	fmt.Println("   8.3 智能同步模式（昨天天表+上月月表）:")
	fmt.Println("       go run cmd/server/main.go --provider alicloud --sync-optimal --create-table")
	fmt.Println("   8.4 智能同步模式（跳过已存在数据，适合每日自动化）:")
	fmt.Println("       go run cmd/server/main.go --provider alicloud --sync-optimal --skip-existing --create-table")
	fmt.Println("   8.5 智能同步模式（强制更新，适合手动完整同步）:")
	fmt.Println("       go run cmd/server/main.go --provider alicloud --sync-optimal --force-update --create-table")
	fmt.Println("9. 其他云服务商 (即将支持):")
	fmt.Println("   go run cmd/server/main.go --provider aws")
	fmt.Println("   go run cmd/server/main.go --provider azure")
	fmt.Println("   go run cmd/server/main.go --provider gcp")
}

// confirmDangerousOperation 确认危险操作
func confirmDangerousOperation(operation string) bool {
	fmt.Printf("\n⚠️  危险操作确认: %s\n", operation)
	fmt.Printf("这个操作将永久删除数据，无法恢复！\n")
	fmt.Printf("请输入 'YES' (大写) 以确认继续: ")

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return false
	}

	input = strings.TrimSpace(input)
	return input == "YES"
}

// isDangerousCondition 判断是否为危险的清理条件
func isDangerousCondition(condition string) bool {
	// 检查一些危险的模式
	dangerousPatterns := []string{
		"1=1",
		"true",
		"expense_date >",             // 可能删除大量历史数据
		"preferential_bill_amount >", // 可能删除大量高价值记录
		"bill_period <",              // 可能删除多个账期数据
	}

	lowerCondition := strings.ToLower(condition)
	for _, pattern := range dangerousPatterns {
		if strings.Contains(lowerCondition, pattern) {
			return true
		}
	}

	return false
}

// getBillPeriods 获取可用的账期列表
func getBillPeriods(ctx context.Context, billService *volcengine.BillService, config *config.VolcEngineConfig) ([]string, error) {
	log.Printf("[账期获取] 开始获取可用账期列表，AutoDetectPeriods=%v, SkipEmptyPeriods=%v",
		config.AutoDetectPeriods, config.SkipEmptyPeriods)

	// 优先使用API检测，因为火山云API有严格的账期限制
	if config.AutoDetectPeriods {
		log.Printf("[账期获取] 使用API自动检测可用账期")

		if config.SkipEmptyPeriods {
			periods, err := billService.GetAvailableBillPeriodsWithValidation(ctx, config.MaxHistoricalMonths, true)
			if err != nil {
				log.Printf("[账期获取] API验证失败，回退到默认账期: %v", err)
				return generateDefaultPeriods(config.MaxHistoricalMonths), nil
			}
			log.Printf("[账期获取] API验证完成，获得有效账期: %v", periods)
			return periods, nil
		} else {
			periods, err := billService.GetAvailableBillPeriods(ctx, config.MaxHistoricalMonths)
			if err != nil {
				log.Printf("[账期获取] API查询失败，回退到默认账期: %v", err)
				return generateDefaultPeriods(config.MaxHistoricalMonths), nil
			}
			log.Printf("[账期获取] API查询完成，获得账期: %v", periods)
			return periods, nil
		}
	} else {
		log.Printf("[账期获取] 使用默认账期生成逻辑")
		periods := generateDefaultPeriods(config.MaxHistoricalMonths)
		return periods, nil
	}
}

// generateDefaultPeriods 生成默认的账期列表
// 注意：火山云API仅支持当月和上月，所以最多生成2个账期
func generateDefaultPeriods(maxMonths int) []string {
	// 火山云API限制：最多只能查询当月和上月
	if maxMonths <= 0 || maxMonths > 2 {
		maxMonths = 2
	}

	var periods []string
	now := time.Now()

	// 只生成当月和上月（如果需要）
	for i := 0; i < maxMonths; i++ {
		period := now.AddDate(0, -i, 0).Format("2006-01")
		periods = append(periods, period)
	}

	log.Printf("[账期生成] 生成默认账期列表: %v（火山云API仅支持当月和上月）", periods)
	return periods
}

// generatePeriodRange 生成指定范围的账期列表
func generatePeriodRange(startPeriod, endPeriod string) []string {
	var periods []string

	// 如果都为空，返回当前月
	if startPeriod == "" && endPeriod == "" {
		return []string{time.Now().Format("2006-01")}
	}

	// 如果只指定了开始时间，默认到当前月
	if endPeriod == "" {
		endPeriod = time.Now().Format("2006-01")
	}

	// 如果只指定了结束时间，默认从12个月前开始
	if startPeriod == "" {
		start := time.Now().AddDate(0, -12, 0)
		startPeriod = start.Format("2006-01")
	}

	// 解析开始和结束时间
	startTime, err := time.Parse("2006-01", startPeriod)
	if err != nil {
		log.Printf("无效的开始账期格式: %s, 使用当前月", startPeriod)
		return []string{time.Now().Format("2006-01")}
	}

	endTime, err := time.Parse("2006-01", endPeriod)
	if err != nil {
		log.Printf("无效的结束账期格式: %s, 使用当前月", endPeriod)
		endTime = time.Now()
	}

	// 确保开始时间不晚于结束时间
	if startTime.After(endTime) {
		startTime, endTime = endTime, startTime
	}

	// 生成账期列表
	current := startTime
	for !current.After(endTime) {
		periods = append(periods, current.Format("2006-01"))
		current = current.AddDate(0, 1, 0)
	}

	return periods
}

// getPeriodDisplay 获取账期的显示名称
func getPeriodDisplay(period string) string {
	if period == "" {
		return "当前账期"
	}
	return period
}

// handleDropTableOperations 处理删除表操作
func handleDropTableOperations(ctx context.Context, billService *volcengine.BillService, cfg *config.Config, params *SyncParams) {
	fmt.Println("=== 删除表操作 ===")

	if params.DropOldTable != "" {
		// 删除指定的旧表
		fmt.Printf("准备删除指定的旧表: %s\n", params.DropOldTable)

		if !confirmDangerousOperation(fmt.Sprintf("删除表 '%s'", params.DropOldTable)) {
			fmt.Println("操作已取消")
			return
		}

		// 检查是否为分布式表
		isDistributed := strings.Contains(params.DropOldTable, "distributed")

		if isDistributed && cfg.ClickHouse.Cluster != "" {
			// 构造本地表名
			localTableName := strings.Replace(params.DropOldTable, "distributed", "local", 1)

			fmt.Printf("删除分布式表结构:\n")
			fmt.Printf("  - 分布式表: %s\n", params.DropOldTable)
			fmt.Printf("  - 本地表: %s\n", localTableName)

			if err := billService.DropDistributedBillTable(ctx, localTableName, params.DropOldTable, "YES"); err != nil {
				log.Fatalf("删除分布式表失败: %v", err)
			}
		} else {
			// 删除普通表
			if err := billService.DropBillTable(ctx, "YES"); err != nil {
				log.Fatalf("删除表失败: %v", err)
			}
		}

		fmt.Printf("表 %s 删除成功\n", params.DropOldTable)

	} else if params.DropTable {
		// 删除默认的火山云账单表
		tableName := "volcengine_bill_details"

		fmt.Printf("准备删除火山云账单表: %s\n", tableName)

		if !confirmDangerousOperation(fmt.Sprintf("删除表 '%s'", tableName)) {
			fmt.Println("操作已取消")
			return
		}

		if params.UseDistributed && cfg.ClickHouse.Cluster != "" {
			// 删除分布式表
			localTableName := "volcengine_bill_details_local"
			distributedTableName := "volcengine_bill_details_distributed"

			fmt.Printf("删除分布式表结构:\n")
			fmt.Printf("  - 分布式表: %s\n", distributedTableName)
			fmt.Printf("  - 本地表: %s\n", localTableName)

			if err := billService.DropDistributedBillTable(ctx, localTableName, distributedTableName, "YES"); err != nil {
				log.Fatalf("删除分布式表失败: %v", err)
			}

			fmt.Println("分布式表删除成功")
		} else {
			// 删除普通表
			if err := billService.DropBillTable(ctx, "YES"); err != nil {
				log.Fatalf("删除表失败: %v", err)
			}

			fmt.Println("表删除成功")
		}
	}
}

func runAliCloudSync(ctx context.Context, cfg *config.Config, chClient *clickhouse.Client, params *SyncParams) {
	fmt.Println("=== 阿里云账单数据同步 ===")

	// 获取阿里云配置
	aliConfig := cfg.GetAliCloudConfig()

	// 验证阿里云配置
	if aliConfig.AccessKeyID == "" || aliConfig.AccessKeySecret == "" {
		log.Fatal("阿里云 AccessKeyID 和 AccessKeySecret 必须配置")
	}

	// 创建账单服务
	billService, err := alicloud.NewBillService(aliConfig, chClient)
	if err != nil {
		log.Fatalf("Failed to create AliCloud bill service: %v", err)
	}
	defer billService.Close()

	// 测试连接
	if err := billService.TestConnection(ctx); err != nil {
		log.Printf("阿里云连接测试警告: %v", err)
		// 对于阿里云，连接测试失败不阻断流程，继续尝试
	}

	// 处理删除表操作
	if params.DropTable || params.DropOldTable != "" {
		handleAliCloudDropTableOperations(ctx, billService, cfg, params)
		return // 删除表后直接返回，不执行其他操作
	}

	// 处理表创建逻辑
	if params.CreateTable {
		handleAliCloudTableCreation(ctx, billService, cfg, params)
	}

	// 检查是否使用特殊同步模式
	if params.SyncOptimal {
		fmt.Println("🔄 使用智能同步模式：昨天的天表数据 + 上月的月表数据")
		runOptimalSync(ctx, billService, cfg, params)
		return
	} else if params.SyncYesterday {
		fmt.Println("📅 仅同步昨天的天表数据")
		runYesterdaySync(ctx, billService, cfg, params)
		return
	} else if params.SyncLastMonth {
		fmt.Println("📊 仅同步上月的月表数据")
		runLastMonthSync(ctx, billService, cfg, params)
		return
	}

	// 决定同步模式和粒度
	granularity := determineGranularity(params, aliConfig)
	fmt.Printf("同步粒度: %s\n", granularity)

	// 决定同步的账期列表
	periodsToSync := determineSyncPeriods(ctx, billService, cfg, params, aliConfig)
	if len(periodsToSync) == 0 {
		log.Printf("[警告] 没有可用的有效账期，请检查配置或API限制")
		return
	}

	fmt.Printf("账期列表: %v\n", periodsToSync)

	// 处理数据清理逻辑
	if params.CleanBefore || params.CleanPreview {
		handleAliCloudDataCleaning(ctx, billService, params, granularity)
	}

	// 自动检测集群环境，如果配置了集群但没有显式指定distributed，则自动使用分布式表
	useDistributed := params.UseDistributed || cfg.ClickHouse.Cluster != ""

	// 同步数据
	for _, period := range periodsToSync {
		fmt.Printf("开始同步账期: %s\n", period)

		syncOptions := &alicloud.SyncOptions{
			BatchSize:        params.Limit,
			UseDistributed:   useDistributed,
			EnableValidation: true,
			MaxWorkers:       4,
		}

		if useDistributed && cfg.ClickHouse.Cluster != "" {
			// 设置分布式表名
			switch granularity {
			case "monthly":
				syncOptions.DistributedTableName = billService.GetMonthlyTableName()
			case "daily":
				syncOptions.DistributedTableName = billService.GetDailyTableName()
			case "both":
				// 两种粒度都同步，分别处理
			}
		}

		// 根据粒度同步数据
		switch granularity {
		case "monthly":
			if err := billService.SyncMonthlyBillData(ctx, period, syncOptions); err != nil {
				log.Printf("账期 %s 按月同步失败: %v", period, err)
				continue
			}
		case "daily":
			if err := billService.SyncDailyBillData(ctx, period, syncOptions); err != nil {
				log.Printf("账期 %s 按天同步失败: %v", period, err)
				continue
			}
		case "both":
			if err := billService.SyncBothGranularityData(ctx, period, syncOptions); err != nil {
				log.Printf("账期 %s 双粒度同步失败: %v", period, err)
				continue
			}
		}

		fmt.Printf("账期 %s 同步完成\n", period)
	}

	fmt.Println("=== 阿里云账单数据同步完成 ===")
}

// handleAliCloudDropTableOperations 处理阿里云删除表操作
func handleAliCloudDropTableOperations(ctx context.Context, billService *alicloud.BillService, cfg *config.Config, params *SyncParams) {
	if params.DropOldTable != "" {
		// 删除指定的旧表
		fmt.Printf("准备删除指定的阿里云旧表: %s\n", params.DropOldTable)

		if !confirmDangerousOperation(fmt.Sprintf("删除表 '%s'", params.DropOldTable)) {
			fmt.Println("操作已取消")
			return
		}

		// 删除指定的旧表
		if err := billService.DropOldTable(ctx, params.DropOldTable); err != nil {
			log.Fatalf("删除阿里云表失败: %v", err)
		}

		fmt.Printf("阿里云表 %s 删除成功\n", params.DropOldTable)

	} else if params.DropTable {
		// 删除默认的阿里云账单表
		// 自动检测集群环境，如果配置了集群但没有显式指定distributed，则自动使用分布式表
		useDistributed := params.UseDistributed || cfg.ClickHouse.Cluster != ""

		if useDistributed && cfg.ClickHouse.Cluster != "" {
			// 删除分布式表
			fmt.Printf("准备删除阿里云分布式账单表\n")
			if !confirmDangerousOperation("删除阿里云分布式账单表（包括本地表和分布式表）") {
				fmt.Println("操作已取消")
				return
			}

			// 删除按月分布式表
			monthlyDistributedTable := billService.GetMonthlyTableName() + "_distributed"
			if err := billService.DropOldTable(ctx, monthlyDistributedTable); err != nil {
				log.Printf("删除按月分布式表失败: %v", err)
			} else {
				fmt.Printf("阿里云按月分布式表 %s 删除成功\n", monthlyDistributedTable)
			}

			// 删除按天分布式表
			dailyDistributedTable := billService.GetDailyTableName() + "_distributed"
			if err := billService.DropOldTable(ctx, dailyDistributedTable); err != nil {
				log.Printf("删除按天分布式表失败: %v", err)
			} else {
				fmt.Printf("阿里云按天分布式表 %s 删除成功\n", dailyDistributedTable)
			}

			fmt.Println("阿里云分布式账单表删除成功")
		} else {
			// 删除普通表
			fmt.Printf("准备删除阿里云账单表\n")
			if !confirmDangerousOperation("删除阿里云账单表（包括按月和按天表）") {
				fmt.Println("操作已取消")
				return
			}

			if err := billService.DropTable(ctx, "both"); err != nil {
				log.Fatalf("删除阿里云表失败: %v", err)
			}

			fmt.Println("阿里云账单表删除成功")
		}
	}
}

// handleAliCloudTableCreation 处理阿里云表创建
func handleAliCloudTableCreation(ctx context.Context, billService *alicloud.BillService, cfg *config.Config, params *SyncParams) {
	fmt.Println("正在创建阿里云账单表...")

	// 自动检测集群环境，如果配置了集群但没有显式指定distributed，则自动使用分布式表
	useDistributed := params.UseDistributed || cfg.ClickHouse.Cluster != ""

	if useDistributed && cfg.ClickHouse.Cluster != "" {
		// 创建分布式表
		monthlyLocalTable := billService.GetMonthlyTableName() + "_local"
		monthlyDistributedTable := billService.GetMonthlyTableName() + "_distributed"
		dailyLocalTable := billService.GetDailyTableName() + "_local"
		dailyDistributedTable := billService.GetDailyTableName() + "_distributed"

		// 创建按月分布式表
		if err := billService.CreateDistributedMonthlyBillTable(ctx, monthlyLocalTable, monthlyDistributedTable); err != nil {
			log.Fatalf("Failed to create AliCloud distributed monthly table: %v", err)
		}
		fmt.Printf("阿里云按月分布式表 %s 创建成功\n", monthlyDistributedTable)

		// 创建按天分布式表
		if err := billService.CreateDistributedDailyBillTable(ctx, dailyLocalTable, dailyDistributedTable); err != nil {
			log.Fatalf("Failed to create AliCloud distributed daily table: %v", err)
		}
		fmt.Printf("阿里云按天分布式表 %s 创建成功\n", dailyDistributedTable)

		// 更新BillService中的表名为分布式表名
		billService.SetDistributedTableNames(monthlyDistributedTable, dailyDistributedTable)

	} else {
		// 创建普通表
		if err := billService.CreateMonthlyBillTable(ctx); err != nil {
			log.Fatalf("Failed to create AliCloud monthly table: %v", err)
		}
		fmt.Printf("阿里云按月表 %s 创建成功\n", billService.GetMonthlyTableName())

		if err := billService.CreateDailyBillTable(ctx); err != nil {
			log.Fatalf("Failed to create AliCloud daily table: %v", err)
		}
		fmt.Printf("阿里云按天表 %s 创建成功\n", billService.GetDailyTableName())
	}
}

// determineGranularity 确定同步粒度
func determineGranularity(params *SyncParams, aliConfig *config.AliCloudConfig) string {
	// 命令行参数优先
	if params.Granularity != "" {
		return params.Granularity
	}

	// 使用配置文件中的默认粒度
	if aliConfig.DefaultGranularity != "" {
		return aliConfig.DefaultGranularity
	}

	// 默认按月
	return "monthly"
}

// determineSyncPeriods 确定同步账期列表
func determineSyncPeriods(ctx context.Context, billService *alicloud.BillService, cfg *config.Config, params *SyncParams, aliConfig *config.AliCloudConfig) []string {
	var periodsToSync []string

	if params.SyncAllPeriods {
		// 显式指定同步所有账期
		periods, err := billService.GetAvailableBillingCycles(ctx)
		if err != nil {
			log.Printf("[错误处理] 无法获取可用账期，使用默认配置: %v", err)
			periodsToSync = generateDefaultAliCloudPeriods(aliConfig.MaxHistoricalMonths)
		} else {
			periodsToSync = periods
		}
	} else if params.StartPeriod != "" || params.EndPeriod != "" {
		// 指定时间范围同步
		log.Printf("[账期处理] 指定时间范围同步: %s -> %s", params.StartPeriod, params.EndPeriod)
		periodsToSync = generatePeriodRange(params.StartPeriod, params.EndPeriod)

		// 验证时间范围内的账期
		validPeriods := []string{}
		for _, period := range periodsToSync {
			if err := alicloud.ValidateBillingCycle(period); err != nil {
				log.Printf("[账期验证] 跳过无效账期 %s: %v", period, err)
				continue
			}
			validPeriods = append(validPeriods, period)
		}
		periodsToSync = validPeriods
	} else if params.BillPeriod != "" {
		// 指定单个账期
		if err := alicloud.ValidateBillingCycle(params.BillPeriod); err != nil {
			log.Fatalf("[账期验证] 指定的账期无效 %s: %v", params.BillPeriod, err)
		}
		periodsToSync = []string{params.BillPeriod}
	} else {
		// 根据配置决定默认行为
		switch aliConfig.DefaultSyncMode {
		case "all_periods":
			periods, err := billService.GetAvailableBillingCycles(ctx)
			if err != nil {
				log.Printf("[错误处理] 无法获取可用账期，使用默认配置: %v", err)
				periodsToSync = generateDefaultAliCloudPeriods(aliConfig.MaxHistoricalMonths)
			} else {
				periodsToSync = periods
			}
		case "range":
			periodsToSync = generatePeriodRange(aliConfig.DefaultStartPeriod, aliConfig.DefaultEndPeriod)
		default: // current_period
			periodsToSync = []string{""} // 空字符串表示当前账期
		}
	}

	// 去重处理
	uniquePeriods := make(map[string]bool)
	finalPeriods := []string{}
	for _, period := range periodsToSync {
		if period == "" {
			period = time.Now().Format("2006-01") // 当前账期
		}
		if !uniquePeriods[period] {
			uniquePeriods[period] = true
			finalPeriods = append(finalPeriods, period)
		}
	}

	return finalPeriods
}

// handleAliCloudDataCleaning 处理阿里云数据清理
func handleAliCloudDataCleaning(ctx context.Context, billService *alicloud.BillService, params *SyncParams, granularity string) {
	fmt.Printf("🔧 检测到清理参数: CleanBefore=%v, CleanPreview=%v\n", params.CleanBefore, params.CleanPreview)

	var cleanCondition string
	var isDryRun bool = params.CleanPreview

	if params.CleanAll {
		// 清空整个表 - 需要额外确认
		if !params.CleanPreview && !confirmDangerousOperation("清空阿里云账单表的所有数据") {
			log.Fatal("操作已取消")
		}
		cleanCondition = ""
		fmt.Printf("⚠️  将清空阿里云账单表的所有数据！\n")
	} else if params.CleanCondition != "" {
		// 使用用户指定的清理条件
		cleanCondition = params.CleanCondition
		fmt.Printf("清理条件: '%s'\n", cleanCondition)

		// 如果是危险的条件，需要确认
		if !params.CleanPreview && isDangerousCondition(cleanCondition) {
			if !confirmDangerousOperation(fmt.Sprintf("执行清理条件: %s", cleanCondition)) {
				log.Fatal("操作已取消")
			}
		}
	} else {
		// 默认清理当前月数据
		if granularity == "daily" {
			cleanCondition = "toYYYYMM(billing_date) = toYYYYMM(now())"
		} else {
			// 月表使用billing_cycle字段进行清理，使用简单的等值条件以支持分区删除
			currentMonth := time.Now().Format("2006-01") // 生成当前年月 YYYY-MM 格式
			cleanCondition = fmt.Sprintf("billing_cycle = '%s'", currentMonth)
		}
		fmt.Printf("清理条件: '当前月数据' (%s)\n", cleanCondition)
	}

	fmt.Printf("🗑️  最终清理条件: '%s'\n", cleanCondition)
	fmt.Printf("🏃 isDryRun模式: %v\n", isDryRun)

	if isDryRun {
		fmt.Printf("🔍 预览模式：将仅显示清理统计信息，不会实际删除数据\n")
	} else {
		fmt.Printf("🗑️  执行模式：将实际删除符合条件的数据\n")
	}

	// 执行清理（支持不同粒度）
	if err := billService.CleanBillData(ctx, granularity, cleanCondition, isDryRun); err != nil {
		if isDryRun {
			log.Printf("数据清理预览失败: %v", err)
		} else {
			log.Fatalf("数据清理失败: %v", err)
		}
	}

	if isDryRun {
		fmt.Println("🔍 数据清理预览完成")
	} else {
		fmt.Println("🗑️  数据清理完成")
	}
}

// generateDefaultAliCloudPeriods 生成阿里云默认账期列表
func generateDefaultAliCloudPeriods(maxMonths int) []string {
	var periods []string
	now := time.Now()

	if maxMonths <= 0 {
		maxMonths = 18 // 阿里云默认支持18个月
	}
	if maxMonths > 18 {
		maxMonths = 18 // 阿里云最大支持18个月
	}

	for i := 0; i < maxMonths; i++ {
		period := now.AddDate(0, -i, 0).Format("2006-01")
		periods = append(periods, period)
	}

	return periods
}

// runOptimalSync 智能同步模式：昨天的天表数据 + 上月的月表数据
func runOptimalSync(ctx context.Context, billService *alicloud.BillService, cfg *config.Config, params *SyncParams) {
	fmt.Println("=== 智能同步模式 ===")

	yesterday := time.Now().AddDate(0, 0, -1)
	lastMonth := time.Now().AddDate(0, -1, 0)

	yesterdayDate := yesterday.Format("2006-01-02")
	lastMonthPeriod := lastMonth.Format("2006-01")

	fmt.Printf("📅 昨天日期: %s\n", yesterdayDate)
	fmt.Printf("📊 上月账期: %s\n", lastMonthPeriod)

	// 检查重复数据处理策略
	if params.ForceUpdate {
		fmt.Println("🔄 使用强制更新模式：先清理已存在数据，再重新同步")
	} else if params.SkipExisting {
		fmt.Println("⏭️ 使用跳过模式：智能检测已存在数据，避免重复同步")
	} else {
		fmt.Println("📝 使用默认模式：依赖ReplacingMergeTree引擎去重（推荐每月初执行）")
	}

	// 自动检测集群环境，如果配置了集群但没有显式指定distributed，则自动使用分布式表
	useDistributed := params.UseDistributed || cfg.ClickHouse.Cluster != ""

	// 准备同步选项
	syncOptions := &alicloud.SyncOptions{
		BatchSize:        params.Limit,
		UseDistributed:   useDistributed,
		EnableValidation: true,
		MaxWorkers:       4,
	}

	// 1. 同步昨天的天表数据
	fmt.Println("\n--- 1. 同步昨天的天表数据 ---")
	dailyTableName := billService.GetDailyTableName()
	if useDistributed && cfg.ClickHouse.Cluster != "" {
		// 智能添加 _distributed 后缀：避免重复添加
		if !strings.HasSuffix(dailyTableName, "_distributed") {
			dailyTableName += "_distributed"
		}
		syncOptions.DistributedTableName = dailyTableName
	}

	// 检查昨天数据是否已存在
	if params.SkipExisting {
		exists, count, err := checkDailyDataExists(ctx, billService, dailyTableName, yesterdayDate)
		if err != nil {
			log.Printf("检查昨天数据失败: %v", err)
		} else if exists {
			fmt.Printf("⏭️ 昨天(%s)的数据已存在 (%d 条记录)，跳过同步\n", yesterdayDate, count)
			goto syncMonthly
		}
	}

	// 如果需要强制更新，先清理昨天的数据
	if params.ForceUpdate {
		fmt.Printf("🧹 清理昨天(%s)的旧数据...\n", yesterdayDate)
		if err := cleanSpecificDayData(ctx, billService, dailyTableName, yesterdayDate); err != nil {
			log.Printf("清理昨天数据失败: %v", err)
		}
	}

	if err := billService.SyncSpecificDayBillData(ctx, yesterdayDate, syncOptions); err != nil {
		log.Printf("昨天天表数据同步失败: %v", err)
	} else {
		fmt.Printf("✅ 昨天(%s)的天表数据同步成功\n", yesterdayDate)
	}

syncMonthly:
	// 2. 同步上月的月表数据
	fmt.Println("\n--- 2. 同步上月的月表数据 ---")
	monthlyTableName := billService.GetMonthlyTableName()
	if useDistributed && cfg.ClickHouse.Cluster != "" {
		// 智能添加 _distributed 后缀：避免重复添加
		if !strings.HasSuffix(monthlyTableName, "_distributed") {
			monthlyTableName += "_distributed"
		}
		syncOptions.DistributedTableName = monthlyTableName
	}

	// 检查上月数据是否已存在
	if params.SkipExisting {
		exists, count, err := checkMonthlyDataExists(ctx, billService, monthlyTableName, lastMonthPeriod)
		if err != nil {
			log.Printf("检查上月数据失败: %v", err)
		} else if exists {
			fmt.Printf("⏭️ 上月(%s)的数据已存在 (%d 条记录)，跳过同步\n", lastMonthPeriod, count)
			goto completed
		}
	}

	// 如果需要强制更新，先清理上月的数据
	if params.ForceUpdate {
		fmt.Printf("🧹 清理上月(%s)的旧数据...\n", lastMonthPeriod)
		if err := cleanSpecificMonthData(ctx, billService, monthlyTableName, lastMonthPeriod); err != nil {
			log.Printf("清理上月数据失败: %v", err)
		}
	}

	if err := billService.SyncMonthlyBillData(ctx, lastMonthPeriod, syncOptions); err != nil {
		log.Printf("上月月表数据同步失败: %v", err)
	} else {
		fmt.Printf("✅ 上月(%s)的月表数据同步成功\n", lastMonthPeriod)
	}

completed:
	fmt.Println("\n🎉 智能同步模式完成")

	// 给出使用建议
	if !params.ForceUpdate && !params.SkipExisting {
		fmt.Println("\n💡 使用建议:")
		fmt.Println("   - 每日自动同步建议使用: --sync-optimal --skip-existing")
		fmt.Println("   - 手动完整同步建议使用: --sync-optimal --force-update")
		fmt.Println("   - ReplacingMergeTree 会在后台自动去重相同数据")
	}
}

// runYesterdaySync 仅同步昨天的天表数据
func runYesterdaySync(ctx context.Context, billService *alicloud.BillService, cfg *config.Config, params *SyncParams) {
	fmt.Println("=== 昨天天表数据同步 ===")

	yesterday := time.Now().AddDate(0, 0, -1)
	yesterdayDate := yesterday.Format("2006-01-02")

	fmt.Printf("📅 昨天日期: %s\n", yesterdayDate)

	// 自动检测集群环境，如果配置了集群但没有显式指定distributed，则自动使用分布式表
	useDistributed := params.UseDistributed || cfg.ClickHouse.Cluster != ""

	// 准备同步选项
	syncOptions := &alicloud.SyncOptions{
		BatchSize:        params.Limit,
		UseDistributed:   useDistributed,
		EnableValidation: true,
		MaxWorkers:       4,
	}

	if useDistributed && cfg.ClickHouse.Cluster != "" {
		syncOptions.DistributedTableName = billService.GetDailyTableName()
	}

	// 同步昨天的天表数据
	if err := billService.SyncSpecificDayBillData(ctx, yesterdayDate, syncOptions); err != nil {
		log.Fatalf("昨天天表数据同步失败: %v", err)
	}

	fmt.Printf("✅ 昨天(%s)的天表数据同步成功\n", yesterdayDate)
	fmt.Println("🎉 昨天天表数据同步完成")
}

// runLastMonthSync 仅同步上月的月表数据
func runLastMonthSync(ctx context.Context, billService *alicloud.BillService, cfg *config.Config, params *SyncParams) {
	fmt.Println("=== 上月月表数据同步 ===")

	lastMonth := time.Now().AddDate(0, -1, 0)
	lastMonthPeriod := lastMonth.Format("2006-01")

	fmt.Printf("📊 上月账期: %s\n", lastMonthPeriod)

	// 自动检测集群环境，如果配置了集群但没有显式指定distributed，则自动使用分布式表
	useDistributed := params.UseDistributed || cfg.ClickHouse.Cluster != ""

	// 准备同步选项
	syncOptions := &alicloud.SyncOptions{
		BatchSize:        params.Limit,
		UseDistributed:   useDistributed,
		EnableValidation: true,
		MaxWorkers:       4,
	}

	if useDistributed && cfg.ClickHouse.Cluster != "" {
		syncOptions.DistributedTableName = billService.GetMonthlyTableName()
	}

	// 同步上月的月表数据
	if err := billService.SyncMonthlyBillData(ctx, lastMonthPeriod, syncOptions); err != nil {
		log.Fatalf("上月月表数据同步失败: %v", err)
	}

	fmt.Printf("✅ 上月(%s)的月表数据同步成功\n", lastMonthPeriod)
	fmt.Println("🎉 上月月表数据同步完成")
}

// checkDailyDataExists 检查天表指定日期的数据是否存在
func checkDailyDataExists(ctx context.Context, billService *alicloud.BillService, tableName, billingDate string) (bool, int64, error) {
	return billService.CheckDailyDataExists(ctx, tableName, billingDate)
}

// checkMonthlyDataExists 检查月表指定账期的数据是否存在
func checkMonthlyDataExists(ctx context.Context, billService *alicloud.BillService, tableName, billingCycle string) (bool, int64, error) {
	return billService.CheckMonthlyDataExists(ctx, tableName, billingCycle)
}

// cleanSpecificDayData 清理指定日期的天表数据
func cleanSpecificDayData(ctx context.Context, billService *alicloud.BillService, tableName, billingDate string) error {
	// 构建清理条件：billing_date = '指定日期'
	condition := fmt.Sprintf("billing_date = '%s'", billingDate)

	// 如果是分布式表，需要清理本地表
	if strings.HasSuffix(tableName, "_distributed") {
		localTableName := strings.Replace(tableName, "_distributed", "_local", 1)
		return billService.CleanSpecificTableData(ctx, localTableName, condition, false)
	}

	return billService.CleanBillData(ctx, "DAILY", condition, false)
}

// cleanSpecificMonthData 清理指定账期的月表数据
func cleanSpecificMonthData(ctx context.Context, billService *alicloud.BillService, tableName, billingCycle string) error {
	// 构建清理条件：billing_cycle = '指定账期'
	condition := fmt.Sprintf("billing_cycle = '%s'", billingCycle)

	// 如果是分布式表，需要清理本地表
	if strings.HasSuffix(tableName, "_distributed") {
		localTableName := strings.Replace(tableName, "_distributed", "_local", 1)
		return billService.CleanSpecificTableData(ctx, localTableName, condition, false)
	}

	return billService.CleanBillData(ctx, "MONTHLY", condition, false)
}
