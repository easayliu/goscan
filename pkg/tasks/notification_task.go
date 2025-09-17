package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/analysis"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"goscan/pkg/wechat"
	"time"

	"go.uber.org/zap"
)

// NotificationTaskExecutor 通知任务执行器
type NotificationTaskExecutor struct {
	chClient     *clickhouse.Client
	analyzer     *analysis.CostAnalyzer
	wechatClient *wechat.Client
	config       *config.WeChatConfig
}

// NewNotificationTaskExecutor 创建通知任务执行器
func NewNotificationTaskExecutor(chClient *clickhouse.Client, cfg *config.Config) (*NotificationTaskExecutor, error) {
	wechatConfig := cfg.GetWeChatConfig()
	if wechatConfig == nil {
		return nil, fmt.Errorf("%w: wechat config is nil", ErrInvalidTaskConfig)
	}

	// 创建费用分析器
	analyzer := analysis.NewCostAnalyzer(chClient)
	if analyzer == nil {
		return nil, fmt.Errorf("%w: failed to create cost analyzer", ErrNotificationFailed)
	}
	analyzer.SetAlertThreshold(wechatConfig.AlertThreshold)

	// 创建微信客户端
	var wechatClient *wechat.Client
	if wechatConfig.Enabled && wechatConfig.WebhookURL != "" {
		wechatClient = wechat.NewClient(&wechat.Config{
			WebhookURL:   wechatConfig.WebhookURL,
			MaxRetries:   wechatConfig.MaxRetries,
			RetryDelay:   time.Duration(wechatConfig.RetryDelay) * time.Second,
			Timeout:      30 * time.Second,
			MentionUsers: wechatConfig.MentionUsers,
		})
		if wechatClient == nil {
			return nil, fmt.Errorf("%w: failed to create wechat client", ErrNotificationFailed)
		}
	}

	return &NotificationTaskExecutor{
		chClient:     chClient,
		analyzer:     analyzer,
		wechatClient: wechatClient,
		config:       wechatConfig,
	}, nil
}

// NotificationParams 通知参数
type NotificationParams struct {
	Date           time.Time `json:"date"`            // 分析日期
	Providers      []string  `json:"providers"`       // 云服务商列表
	AlertThreshold float64   `json:"alert_threshold"` // 告警阈值
	ForceNotify    bool      `json:"force_notify"`    // 强制发送
}

// ExecuteCostReport 执行费用报告任务（使用默认参数）
func (nte *NotificationTaskExecutor) ExecuteCostReport(ctx context.Context) (*TaskResult, error) {
	// 使用默认参数，Date不设置，让AnalyzeDailyCosts使用默认值（昨天vs前天）
	params := &NotificationParams{
		// Date留空，让AnalyzeDailyCosts使用默认值（昨天）
		Providers:      []string{"volcengine", "alicloud"}, // 默认支持的云服务商
		AlertThreshold: nte.config.AlertThreshold,
		ForceNotify:    false,
	}
	return nte.ExecuteCostReportWithParams(ctx, params)
}

// ExecuteCostReportWithParams 执行费用报告任务（带参数）
func (nte *NotificationTaskExecutor) ExecuteCostReportWithParams(ctx context.Context, params *NotificationParams) (*TaskResult, error) {
	startTime := time.Now()
	result := &TaskResult{
		ID:        fmt.Sprintf("notification_%d", startTime.Unix()),
		Type:      "notification",
		Status:    "running",
		StartedAt: startTime,
		Message:   "执行费用报告通知任务",
	}

	logger.Info("Starting cost report notification task execution")

	// 检查微信通知是否启用
	if !nte.config.Enabled {
		result.Status = "skipped"
		result.Message = "微信通知未启用，跳过任务"
		result.CompletedAt = time.Now()
		result.Duration = time.Since(startTime)
		logger.Info("WeChat notification not enabled, skipping task")
		return result, nil
	}

	if nte.wechatClient == nil {
		err := fmt.Errorf("%w: wechat client not initialized, please check webhook URL configuration", ErrNotificationFailed)
		result.Status = "failed"
		result.Message = "微信客户端未初始化"
		result.Error = err.Error()
		result.CompletedAt = time.Now()
		result.Duration = time.Since(startTime)
		return result, err
	}

	// 分析费用数据
	analysisReq := &analysis.CostAnalysisRequest{
		Date:           params.Date,
		Providers:      params.Providers,
		AlertThreshold: params.AlertThreshold,
	}

	// 处理日期显示
	dateStr := "默认（昨天）"
	if !params.Date.IsZero() {
		dateStr = params.Date.Format("2006-01-02")
	}

	logger.Info("Executing cost report notification task, parameter details",
		zap.String("date", dateStr),
		zap.Strings("providers", params.Providers),
		zap.Float64("alert_threshold", params.AlertThreshold),
		zap.Bool("force_notify", params.ForceNotify))

	analysisDateStr := "默认（将使用昨天）"
	if !analysisReq.Date.IsZero() {
		analysisDateStr = analysisReq.Date.Format("2006-01-02")
	}

	logger.Info("Calling CostAnalyzer.AnalyzeDailyCosts, analysis request parameters",
		zap.String("analysis_date", analysisDateStr),
		zap.Strings("analysis_providers", analysisReq.Providers),
		zap.Float64("analysis_alert_threshold", analysisReq.AlertThreshold))

	analysisResult, err := nte.analyzer.AnalyzeDailyCosts(ctx, analysisReq)
	if err != nil {
		wrappedErr := fmt.Errorf("%w: cost analysis failed: %v", ErrDataValidationFailed, err)
		logger.Error("Cost analysis failed", zap.Error(wrappedErr))
		result.Status = "failed"
		result.Message = "费用分析失败"
		result.Error = wrappedErr.Error()
		result.CompletedAt = time.Now()
		result.Duration = time.Since(startTime)
		return result, wrappedErr
	}

	// 转换为微信消息格式
	wechatData := nte.analyzer.ConvertToWeChatFormat(analysisResult)

	// 发送微信通知
	if wechatCostData, ok := wechatData.(*wechat.CostComparisonData); ok {
		if err := nte.wechatClient.SendCostReport(ctx, wechatCostData); err != nil {
			wrappedErr := fmt.Errorf("%w: send wechat notification failed: %v", ErrNotificationFailed, err)
			logger.Error("Failed to send WeChat notification", zap.Error(wrappedErr))
			result.Status = "failed"
			result.Message = "发送微信通知失败"
			result.Error = wrappedErr.Error()
			result.CompletedAt = time.Now()
			result.Duration = time.Since(startTime)
			return result, wrappedErr
		}
	} else {
		wrappedErr := fmt.Errorf("%w: failed to convert wechat data format", ErrNotificationFailed)
		logger.Error("Failed to convert WeChat data format", zap.Error(wrappedErr))
		result.Status = "failed"
		result.Message = "转换微信数据格式失败"
		result.Error = wrappedErr.Error()
		result.CompletedAt = time.Now()
		result.Duration = time.Since(startTime)
		return result, wrappedErr
	}

	// 任务完成
	result.Status = "completed"
	result.Message = fmt.Sprintf("费用报告通知发送成功，共分析 %d 个云服务商，发现 %d 个告警",
		len(analysisResult.Providers), len(analysisResult.Alerts))
	result.CompletedAt = time.Now()
	result.Duration = time.Since(startTime)
	result.RecordsProcessed = nte.calculateTotalRecords(analysisResult)

	logger.Info("Cost report notification task completed",
		zap.Duration("duration", result.Duration),
		zap.Int("providers", len(analysisResult.Providers)),
		zap.Int("alerts", len(analysisResult.Alerts)))

	return result, nil
}

// TestWebhook 测试微信webhook连接
func (nte *NotificationTaskExecutor) TestWebhook(ctx context.Context) error {
	if !nte.config.Enabled {
		return fmt.Errorf("%w: wechat notification disabled", ErrNotificationFailed)
	}

	if nte.wechatClient == nil {
		return fmt.Errorf("%w: wechat client not initialized, please check webhook URL configuration", ErrNotificationFailed)
	}

	if err := nte.wechatClient.TestConnection(ctx); err != nil {
		return fmt.Errorf("%w: webhook test failed: %v", ErrNotificationFailed, err)
	}

	return nil
}

// IsEnabled 检查通知是否启用
func (nte *NotificationTaskExecutor) IsEnabled() bool {
	return nte.config.Enabled && nte.wechatClient != nil
}

// calculateTotalRecords 计算处理的总记录数（用于统计）
func (nte *NotificationTaskExecutor) calculateTotalRecords(result *analysis.CostAnalysisResult) int {
	totalRecords := uint64(0)
	for _, provider := range result.Providers {
		for _, product := range provider.Products {
			totalRecords += product.RecordCount
		}
	}
	return int(totalRecords)
}

// GetConfig 获取微信配置（用于调试）
func (nte *NotificationTaskExecutor) GetConfig() *config.WeChatConfig {
	return nte.config
}

// SendNotification 发送通知（实现NotificationExecutor接口）
func (nte *NotificationTaskExecutor) SendNotification(ctx context.Context, config *TaskConfig) (*TaskResult, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: task config is nil", ErrInvalidTaskConfig)
	}

	// 创建通知参数
	params := &NotificationParams{
		Providers:      []string{"volcengine", "alicloud"}, // 默认支持的云服务商
		AlertThreshold: nte.config.AlertThreshold,
		ForceNotify:    config.ForceUpdate, // 使用ForceUpdate作为ForceNotify
	}

	// 如果指定了账期，转换为日期
	if config.BillPeriod != "" {
		if date, err := time.Parse("2006-01-02", config.BillPeriod); err == nil {
			params.Date = date
		} else if date, err := time.Parse("2006-01", config.BillPeriod); err == nil {
			// 如果是月份格式，使用该月的最后一天
			params.Date = date.AddDate(0, 1, -1)
		}
	}

	return nte.ExecuteCostReportWithParams(ctx, params)
}

// ValidateNotificationConfig 验证通知配置（实现NotificationExecutor接口）
func (nte *NotificationTaskExecutor) ValidateNotificationConfig(ctx context.Context, config *TaskConfig) error {
	if config == nil {
		return fmt.Errorf("%w: task config is nil", ErrInvalidTaskConfig)
	}

	if !nte.config.Enabled {
		return fmt.Errorf("%w: wechat notification disabled", ErrNotificationFailed)
	}

	if nte.wechatClient == nil {
		return fmt.Errorf("%w: wechat client not initialized", ErrNotificationFailed)
	}

	return nil
}
