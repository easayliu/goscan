package tasks

import (
	"context"
	"fmt"
	"goscan/pkg/analysis"
	"goscan/pkg/clickhouse"
	"goscan/pkg/config"
	"goscan/pkg/wechat"
	"log/slog"
	"time"
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
		return nil, fmt.Errorf("微信配置不能为空")
	}

	// 创建费用分析器
	analyzer := analysis.NewCostAnalyzer(chClient)
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

	slog.Info("开始执行费用报告通知任务")

	// 检查微信通知是否启用
	if !nte.config.Enabled {
		result.Status = "skipped"
		result.Message = "微信通知未启用，跳过任务"
		result.CompletedAt = time.Now()
		result.Duration = time.Since(startTime)
		slog.Info("微信通知未启用，跳过任务")
		return result, nil
	}

	if nte.wechatClient == nil {
		err := fmt.Errorf("微信客户端未初始化，请检查webhook URL配置")
		result.Status = "failed"
		result.Message = err.Error()
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
	
	slog.Info("执行费用报告通知任务，参数详情",
		"date", dateStr,
		"providers", params.Providers,
		"alert_threshold", params.AlertThreshold,
		"force_notify", params.ForceNotify)

	analysisDateStr := "默认（将使用昨天）"
	if !analysisReq.Date.IsZero() {
		analysisDateStr = analysisReq.Date.Format("2006-01-02")
	}
	
	slog.Info("调用CostAnalyzer.AnalyzeDailyCosts，分析请求参数",
		"analysis_date", analysisDateStr,
		"analysis_providers", analysisReq.Providers,
		"analysis_alert_threshold", analysisReq.AlertThreshold)

	analysisResult, err := nte.analyzer.AnalyzeDailyCosts(ctx, analysisReq)
	if err != nil {
		slog.Error("费用分析失败", "error", err)
		result.Status = "failed"
		result.Message = "费用分析失败"
		result.Error = err.Error()
		result.CompletedAt = time.Now()
		result.Duration = time.Since(startTime)
		return result, err
	}

	// 转换为微信消息格式
	wechatData := nte.analyzer.ConvertToWeChatFormat(analysisResult)

	// 发送微信通知
	if err := nte.wechatClient.SendCostReport(ctx, wechatData); err != nil {
		slog.Error("发送微信通知失败", "error", err)
		result.Status = "failed"
		result.Message = "发送微信通知失败"
		result.Error = err.Error()
		result.CompletedAt = time.Now()
		result.Duration = time.Since(startTime)
		return result, err
	}

	// 任务完成
	result.Status = "completed"
	result.Message = fmt.Sprintf("费用报告通知发送成功，共分析 %d 个云服务商，发现 %d 个告警",
		len(analysisResult.Providers), len(analysisResult.Alerts))
	result.CompletedAt = time.Now()
	result.Duration = time.Since(startTime)
	result.RecordsProcessed = nte.calculateTotalRecords(analysisResult)

	slog.Info("费用报告通知任务完成",
		"duration", result.Duration,
		"providers", len(analysisResult.Providers),
		"alerts", len(analysisResult.Alerts))

	return result, nil
}

// TestWebhook 测试微信webhook连接
func (nte *NotificationTaskExecutor) TestWebhook(ctx context.Context) error {
	if !nte.config.Enabled {
		return fmt.Errorf("微信通知未启用")
	}

	if nte.wechatClient == nil {
		return fmt.Errorf("微信客户端未初始化，请检查webhook URL配置")
	}

	return nte.wechatClient.TestConnection(ctx)
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
