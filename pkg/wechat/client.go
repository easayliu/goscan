package wechat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client 企业微信客户端
type Client struct {
	webhookURL         string
	httpClient         *http.Client
	maxRetries         int
	retryDelay         time.Duration
	mentionUsers       []string
	notificationFormat NotificationFormat
}

// NotificationFormat 通知格式类型
type NotificationFormat string

const (
	FormatMarkdown     NotificationFormat = "markdown"      // Markdown表格格式（默认）
	FormatTemplateCard NotificationFormat = "template_card" // 模板卡片格式
	FormatAuto         NotificationFormat = "auto"          // 自动选择（优先模板卡片）
)

// Config 客户端配置
type Config struct {
	WebhookURL         string             `json:"webhook_url"`
	MaxRetries         int                `json:"max_retries"`
	RetryDelay         time.Duration      `json:"retry_delay"`
	Timeout            time.Duration      `json:"timeout"`
	MentionUsers       []string           `json:"mention_users"`
	NotificationFormat NotificationFormat `json:"notification_format"` // 通知格式
}

// NewClient 创建企业微信客户端
func NewClient(config *Config) *Client {
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 2 * time.Second
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.NotificationFormat == "" {
		config.NotificationFormat = FormatMarkdown // 默认使用Markdown格式
	}

	return &Client{
		webhookURL: config.WebhookURL,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
		maxRetries:         config.MaxRetries,
		retryDelay:         config.RetryDelay,
		mentionUsers:       config.MentionUsers,
		notificationFormat: config.NotificationFormat,
	}
}

// SendText 发送文本消息
func (c *Client) SendText(ctx context.Context, content string) error {
	msg := &WebhookMessage{
		MsgType: MessageTypeText,
		Text: &TextMsg{
			Content:       content,
			MentionedList: c.mentionUsers,
		},
	}
	return c.sendMessage(ctx, msg)
}

// SendMarkdown 发送Markdown消息（markdown_v2支持表格）
func (c *Client) SendMarkdown(ctx context.Context, content string) error {
	msg := &WebhookMessage{
		MsgType: MessageTypeMarkdown, // markdown_v2
		MarkdownV2: &MarkdownMsg{ // 使用MarkdownV2字段
			Content: content,
		},
	}
	return c.sendMessage(ctx, msg)
}

// SendTemplateCard 发送模板卡片消息
func (c *Client) SendTemplateCard(ctx context.Context, card *TemplateCard) error {
	msg := &WebhookMessage{
		MsgType:      MessageTypeTemplateCard,
		TemplateCard: card,
	}
	return c.sendMessage(ctx, msg)
}

// SendCostReport 发送费用对比报告
func (c *Client) SendCostReport(ctx context.Context, data *CostComparisonData) error {
	return c.SendCostReportWithFormat(ctx, data, c.notificationFormat)
}

// SendCostReportWithFormat 使用指定格式发送费用对比报告
func (c *Client) SendCostReportWithFormat(ctx context.Context, data *CostComparisonData, format NotificationFormat) error {
	switch format {
	case FormatTemplateCard:
		// 使用模板卡片格式
		card := c.formatCostReportCard(data)
		return c.SendTemplateCard(ctx, card)

	case FormatMarkdown:
		// 使用Markdown表格格式
		content := c.formatCostReport(data)
		return c.SendMarkdown(ctx, content)

	case FormatAuto:
		// 自动选择：优先尝试模板卡片，失败则降级到Markdown
		card := c.formatCostReportCard(data)
		if err := c.SendTemplateCard(ctx, card); err != nil {
			// 模板卡片失败，降级使用Markdown格式
			fmt.Printf("模板卡片发送失败，降级使用Markdown格式: %v\n", err)
			content := c.formatCostReport(data)
			return c.SendMarkdown(ctx, content)
		}
		return nil

	default:
		// 默认使用Markdown格式
		content := c.formatCostReport(data)
		return c.SendMarkdown(ctx, content)
	}
}

// sendMessage 发送消息（带重试）
func (c *Client) sendMessage(ctx context.Context, msg *WebhookMessage) error {
	var lastError error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			// 等待重试延迟
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.retryDelay):
			}
		}

		err := c.doSendMessage(ctx, msg)
		if err == nil {
			return nil // 发送成功
		}

		lastError = err
		if attempt < c.maxRetries {
			fmt.Printf("发送企业微信消息失败，将在 %v 后重试 (尝试 %d/%d): %v\n",
				c.retryDelay, attempt+1, c.maxRetries, err)
		}
	}

	return fmt.Errorf("发送企业微信消息失败，已重试 %d 次: %w", c.maxRetries, lastError)
}

// doSendMessage 执行实际的消息发送
func (c *Client) doSendMessage(ctx context.Context, msg *WebhookMessage) error {
	if c.webhookURL == "" {
		return fmt.Errorf("企业微信webhook URL未配置")
	}

	// 序列化消息
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息失败: %w", err)
	}

	// 创建HTTP请求
	req, err := http.NewRequestWithContext(ctx, "POST", c.webhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("创建HTTP请求失败: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("发送HTTP请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("读取响应失败: %w", err)
	}

	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP请求失败: %d %s, 响应: %s",
			resp.StatusCode, resp.Status, string(respBody))
	}

	// 解析响应
	var webhookResp WebhookResponse
	if err := json.Unmarshal(respBody, &webhookResp); err != nil {
		return fmt.Errorf("解析响应失败: %w", err)
	}

	// 检查业务状态码
	if !webhookResp.IsSuccess() {
		return fmt.Errorf("企业微信API错误: %d %s", webhookResp.ErrCode, webhookResp.ErrMsg)
	}

	return nil
}

// formatCostReport 格式化费用报告为Markdown
func (c *Client) formatCostReport(data *CostComparisonData) string {
	var builder strings.Builder

	// 标题 - 使用emoji增强视觉效果
	builder.WriteString("## 📊 云服务费用日报\n\n")

	// 日期信息
	builder.WriteString(fmt.Sprintf("📅 **报告日期：%s**\n\n", data.Date))

	// 各云服务商明细
	for _, provider := range data.Providers {
		if len(provider.Products) == 0 {
			continue
		}

		// 服务商图标
		var icon string
		switch strings.ToLower(provider.Provider) {
		case "volcengine":
			icon = "🔥"
		case "alicloud":
			icon = "☁️"
		case "aws":
			icon = "🚀"
		case "azure":
			icon = "💙"
		case "gcp":
			icon = "🌐"
		default:
			icon = "☁️"
		}

		// 服务商标题
		builder.WriteString(fmt.Sprintf("### %s %s\n", icon, provider.DisplayName))

		// 服务商总计 - 使用表格格式
		if provider.TotalCost != nil {
			total := provider.TotalCost

			changeDesc := ""
			if total.ChangeAmount != 0 {
				changeIcon := total.GetChangeIcon()
				if total.ChangeAmount > 0 {
					changeDesc = fmt.Sprintf("%s **+%.2f元 (%+.1f%%)**",
						changeIcon, total.ChangeAmount, total.ChangePercent)
				} else {
					changeDesc = fmt.Sprintf("%s %.2f元 (%.1f%%)",
						changeIcon, total.ChangeAmount, total.ChangePercent)
				}
			} else {
				changeDesc = "➡️ 无变化"
			}

			// 构建完整表格字符串
			tableStr := fmt.Sprintf("| 时间 | 总费用 | 变化 |\n| :--- | ---: | ---: |\n| **前天 → 昨天** | **¥%.2f → ¥%.2f** | %s |",
				total.YesterdayCost, total.TodayCost, changeDesc)
			builder.WriteString(tableStr + "\n\n")
		}

		// 产品费用列表 - 使用表格格式显示所有产品
		if len(provider.Products) > 0 {
			builder.WriteString("**📦 产品明细：**\n\n")

			// 构建完整表格字符串
			var tableBuilder strings.Builder
			tableBuilder.WriteString("| 产品名称 | 前天费用 | 昨天费用 | 变化趋势 | 变化幅度 |\n| :--- | ---: | ---: | :---: | ---: |")

			// 显示所有产品
			for _, product := range provider.Products {
				// 变化趋势图标
				changeIcon := "➡️"
				if product.ChangeAmount > 0 {
					changeIcon = "📈"
				} else if product.ChangeAmount < 0 {
					changeIcon = "📉"
				}

				// 变化幅度
				changeStr := ""
				if product.ChangeAmount != 0 {
					if product.ChangePercent > 20 {
						// 超过20%增长，使用加粗
						changeStr = fmt.Sprintf("**%+.1f%%**", product.ChangePercent)
					} else if product.ChangePercent < -20 {
						// 超过20%下降
						changeStr = fmt.Sprintf("%.1f%%", product.ChangePercent)
					} else {
						// 正常变化
						changeStr = fmt.Sprintf("%+.1f%%", product.ChangePercent)
					}
				} else {
					changeStr = "无变化"
				}

				// 添加产品行
				tableBuilder.WriteString(fmt.Sprintf("\n| **%s** | ¥%.2f | ¥%.2f | %s | %s |",
					product.Name, product.YesterdayCost, product.TodayCost, changeIcon, changeStr))
			}

			builder.WriteString(tableBuilder.String())
			builder.WriteString("\n\n")
		}
	}

	// 异常提醒
	if len(data.Alerts) > 0 {
		builder.WriteString("### ⚠️ 费用异常提醒\n")

		// 根据告警数量使用不同的提示
		if len(data.Alerts) > 5 {
			builder.WriteString(fmt.Sprintf("**发现 %d 个异常变化，请关注！**\n\n",
				len(data.Alerts)))
		}

		// 显示前5个最重要的告警
		maxAlerts := 5
		if len(data.Alerts) < maxAlerts {
			maxAlerts = len(data.Alerts)
		}

		for i := 0; i < maxAlerts; i++ {
			builder.WriteString(fmt.Sprintf("> %s\n", data.Alerts[i]))
		}

		if len(data.Alerts) > maxAlerts {
			builder.WriteString(fmt.Sprintf("> *...还有 %d 个告警*\n", len(data.Alerts)-maxAlerts))
		}
		builder.WriteString("\n")
	} else {
		// 没有异常时的提示
		builder.WriteString("### ✅ 费用状态\n")
		builder.WriteString("费用变化在正常范围内，无异常告警\n\n")
	}

	// 生成时间和说明
	builder.WriteString("\n---\n")
	builder.WriteString(fmt.Sprintf("*⏰ 生成时间: %s*\n",
		data.GeneratedAt.Format("2006-01-02 15:04:05")))
	builder.WriteString("*💡 说明: 对比昨天与前天的费用数据*")

	return builder.String()
}

// TestConnection 测试连接
func (c *Client) TestConnection(ctx context.Context) error {
	// 使用符合企业微信规范的Markdown格式
	testMsg := fmt.Sprintf(`## 🤖 企业微信通知测试
> 📅 测试时间：**%s**

### ✅ 连接状态
连接正常，配置有效

### 支持的功能
- 📊 费用日报推送
- ⚠️ 异常告警通知
- 📈 数据分析报告

---
*这是一条测试消息，用于验证通知配置是否正确*`,
		time.Now().Format("2006-01-02 15:04:05"))

	return c.SendMarkdown(ctx, testMsg)
}

// formatCostReportCard 格式化费用报告为模板卡片
func (c *Client) formatCostReportCard(data *CostComparisonData) *TemplateCard {
	card := &TemplateCard{
		CardType: "text_notice",
	}

	// 设置来源信息
	card.Source = &CardSource{
		Desc:      "费用分析系统",
		DescColor: 0, // 灰色
	}

	// 设置主标题
	card.MainTitle = &CardMainTitle{
		Title: "☁️ 云服务费用日报",
		Desc:  fmt.Sprintf("📅 %s", data.Date),
	}

	// 设置关键数据（总费用变化）
	if data.TotalCost != nil {
		total := data.TotalCost
		emphasisTitle := fmt.Sprintf("¥%.2f", total.TodayCost)

		var emphasisDesc string
		if total.ChangeAmount != 0 {
			changeIcon := total.GetChangeIcon()
			if total.ChangeAmount > 0 {
				emphasisDesc = fmt.Sprintf("%s 较前天增长 %.2f元 (%.1f%%)",
					changeIcon, total.ChangeAmount, total.ChangePercent)
			} else {
				emphasisDesc = fmt.Sprintf("%s 较前天减少 %.2f元 (%.1f%%)",
					changeIcon, -total.ChangeAmount, -total.ChangePercent)
			}
		} else {
			emphasisDesc = "与前天持平"
		}

		card.EmphasisContent = &CardEmphasisContent{
			Title: emphasisTitle,
			Desc:  emphasisDesc,
		}
	}

	// 设置二级标题
	card.SubTitleText = "📊 各云服务商费用明细"

	// 设置水平内容列表（各服务商费用）
	var horizontalList []CardHorizontalContent
	for _, provider := range data.Providers {
		if provider.TotalCost == nil {
			continue
		}

		// 服务商图标
		var icon string
		switch strings.ToLower(provider.Provider) {
		case "volcengine":
			icon = "🔥"
		case "alicloud":
			icon = "☁️"
		default:
			icon = "☁️"
		}

		// 费用变化描述
		total := provider.TotalCost
		var changeDesc string
		if total.ChangeAmount != 0 {
			if total.ChangeAmount > 0 {
				changeDesc = fmt.Sprintf("📈 +%.1f%%", total.ChangePercent)
			} else {
				changeDesc = fmt.Sprintf("📉 %.1f%%", total.ChangePercent)
			}
		} else {
			changeDesc = "➡️ 无变化"
		}

		horizontalList = append(horizontalList, CardHorizontalContent{
			KeyName: fmt.Sprintf("%s %s", icon, provider.DisplayName),
			Value: fmt.Sprintf("¥%.2f → ¥%.2f %s",
				total.YesterdayCost, total.TodayCost, changeDesc),
			Type: 0,
		})

		// 添加主要产品明细（前3个）
		maxProducts := 3
		if len(provider.Products) < maxProducts {
			maxProducts = len(provider.Products)
		}

		for i := 0; i < maxProducts; i++ {
			product := provider.Products[i]
			productChange := ""
			if product.ChangePercent > 20 {
				productChange = fmt.Sprintf("⚠️ +%.1f%%", product.ChangePercent)
			} else if product.ChangePercent < -20 {
				productChange = fmt.Sprintf("✅ %.1f%%", product.ChangePercent)
			} else if product.ChangeAmount != 0 {
				productChange = fmt.Sprintf("%+.1f%%", product.ChangePercent)
			}

			horizontalList = append(horizontalList, CardHorizontalContent{
				KeyName: fmt.Sprintf("  └ %s", product.Name),
				Value: fmt.Sprintf("¥%.0f → ¥%.0f %s",
					product.YesterdayCost, product.TodayCost, productChange),
				Type: 0,
			})
		}

		if len(provider.Products) > maxProducts {
			horizontalList = append(horizontalList, CardHorizontalContent{
				KeyName: fmt.Sprintf("  └ 其他%d个产品", len(provider.Products)-maxProducts),
				Value:   "点击查看详情",
				Type:    0,
			})
		}
	}
	card.HorizontalContentList = horizontalList

	// 设置引用区域（异常提醒）
	if len(data.Alerts) > 0 {
		alertText := ""
		maxAlerts := 3
		if len(data.Alerts) < maxAlerts {
			maxAlerts = len(data.Alerts)
		}

		for i := 0; i < maxAlerts; i++ {
			if i > 0 {
				alertText += "\n"
			}
			alertText += fmt.Sprintf("• %s", data.Alerts[i])
		}

		if len(data.Alerts) > maxAlerts {
			alertText += fmt.Sprintf("\n• ...还有%d个告警", len(data.Alerts)-maxAlerts)
		}

		card.QuoteArea = &CardQuoteArea{
			Type:      0,
			Title:     fmt.Sprintf("⚠️ 发现 %d 个费用异常", len(data.Alerts)),
			QuoteText: alertText,
		}
	} else {
		card.QuoteArea = &CardQuoteArea{
			Type:      0,
			Title:     "✅ 费用状态正常",
			QuoteText: "所有费用变化都在正常范围内",
		}
	}

	// 添加跳转指引
	card.JumpList = []CardJump{
		{
			Type:  1,
			Title: "查看详细报告",
			URL:   "http://goscan.example.com/",
		},
	}

	// card_action是必需的，设置整体卡片跳转
	card.CardAction = &CardAction{
		Type: 1,
		URL:  "http://goscan.example.com/",
	}

	return card
}
