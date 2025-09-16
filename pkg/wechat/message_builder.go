package wechat

import (
	"fmt"
	"strings"
	"time"
)

// DefaultMessageBuilder 默认消息构建器
type DefaultMessageBuilder struct{}

// NewMessageBuilder 创建消息构建器
func NewMessageBuilder() MessageBuilder {
	return &DefaultMessageBuilder{}
}

// BuildTextMessage 构建文本消息
func (b *DefaultMessageBuilder) BuildTextMessage(content string, mentionUsers []string) *WebhookMessage {
	return &WebhookMessage{
		MsgType: MessageTypeText,
		Text: &TextMsg{
			Content:       content,
			MentionedList: mentionUsers,
		},
	}
}

// BuildMarkdownMessage 构建Markdown消息
func (b *DefaultMessageBuilder) BuildMarkdownMessage(content string) *WebhookMessage {
	return &WebhookMessage{
		MsgType: MessageTypeMarkdown,
		MarkdownV2: &MarkdownMsg{
			Content: content,
		},
	}
}

// BuildTemplateCardMessage 构建模板卡片消息
func (b *DefaultMessageBuilder) BuildTemplateCardMessage(card *TemplateCard) *WebhookMessage {
	return &WebhookMessage{
		MsgType:      MessageTypeTemplateCard,
		TemplateCard: card,
	}
}

// DefaultMessageFormatter 默认消息格式化器
type DefaultMessageFormatter struct{}

// NewMessageFormatter 创建消息格式化器
func NewMessageFormatter() MessageFormatter {
	return &DefaultMessageFormatter{}
}

// FormatTestMessage 格式化测试消息
func (f *DefaultMessageFormatter) FormatTestMessage() string {
	return fmt.Sprintf(`## 🤖 企业微信通知测试
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
}

// FormatCostReport 格式化费用报告为Markdown
func (f *DefaultMessageFormatter) FormatCostReport(data *CostComparisonData) string {
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

		f.buildProviderSection(&builder, provider)
	}

	// 异常提醒
	f.buildAlertsSection(&builder, data.Alerts)

	// 生成时间和说明
	f.buildFooterSection(&builder, data.GeneratedAt)

	return builder.String()
}

// buildProviderSection 构建服务商费用段落
func (f *DefaultMessageFormatter) buildProviderSection(builder *strings.Builder, provider *ProviderCostData) {
	// 服务商图标
	icon := f.getProviderIcon(provider.Provider)

	// 服务商标题
	builder.WriteString(fmt.Sprintf("### %s %s\n", icon, provider.DisplayName))

	// 服务商总计 - 使用表格格式
	if provider.TotalCost != nil {
		f.buildTotalCostTable(builder, provider.TotalCost)
	}

	// 产品费用列表 - 使用表格格式显示所有产品
	if len(provider.Products) > 0 {
		f.buildProductsTable(builder, provider.Products)
	}
}

// buildTotalCostTable 构建总费用表格
func (f *DefaultMessageFormatter) buildTotalCostTable(builder *strings.Builder, total *CostChange) {
	// 变化趋势图标
	changeIcon := "➡️"
	if total.ChangeAmount > 0 {
		changeIcon = "📈"
	} else if total.ChangeAmount < 0 {
		changeIcon = "📉"
	}

	// 变化幅度
	changeStr := f.formatProductChange(total)

	// 构建与产品明细一致的表格格式
	tableStr := fmt.Sprintf("| 服务商 | 前天费用 | 昨天费用 | 变化趋势 | 变化幅度 |\n| :--- | ---: | ---: | :---: | ---: |\n| **总计** | ¥%.2f | ¥%.2f | %s | %s |",
		total.YesterdayCost, total.TodayCost, changeIcon, changeStr)
	builder.WriteString(tableStr + "\n\n")
}

// buildProductsTable 构建产品费用表格
func (f *DefaultMessageFormatter) buildProductsTable(builder *strings.Builder, products []*CostChange) {
	builder.WriteString("**📦 产品明细：**\n\n")

	// 构建完整表格字符串
	var tableBuilder strings.Builder
	tableBuilder.WriteString("| 产品名称 | 前天费用 | 昨天费用 | 变化趋势 | 变化幅度 |\n| :--- | ---: | ---: | :---: | ---: |")

	// 显示所有产品
	for _, product := range products {
		f.buildProductRow(&tableBuilder, product)
	}

	builder.WriteString(tableBuilder.String())
	builder.WriteString("\n\n")
}

// buildProductRow 构建产品费用行
func (f *DefaultMessageFormatter) buildProductRow(builder *strings.Builder, product *CostChange) {
	// 变化趋势图标
	changeIcon := "➡️"
	if product.ChangeAmount > 0 {
		changeIcon = "📈"
	} else if product.ChangeAmount < 0 {
		changeIcon = "📉"
	}

	// 变化幅度
	changeStr := f.formatProductChange(product)

	// 添加产品行
	builder.WriteString(fmt.Sprintf("\n| **%s** | ¥%.2f | ¥%.2f | %s | %s |",
		product.Name, product.YesterdayCost, product.TodayCost, changeIcon, changeStr))
}

// buildAlertsSection 构建异常提醒段落
func (f *DefaultMessageFormatter) buildAlertsSection(builder *strings.Builder, alerts []string) {
	if len(alerts) > 0 {
		builder.WriteString("### ⚠️ 费用异常提醒\n")

		// 根据告警数量使用不同的提示
		if len(alerts) > 5 {
			builder.WriteString(fmt.Sprintf("**发现 %d 个异常变化，请关注！**\n\n",
				len(alerts)))
		}

		// 显示前5个最重要的告警
		maxAlerts := 5
		if len(alerts) < maxAlerts {
			maxAlerts = len(alerts)
		}

		for i := 0; i < maxAlerts; i++ {
			builder.WriteString(fmt.Sprintf("> %s\n", alerts[i]))
		}

		if len(alerts) > maxAlerts {
			builder.WriteString(fmt.Sprintf("> *...还有 %d 个告警*\n", len(alerts)-maxAlerts))
		}
		builder.WriteString("\n")
	} else {
		// 没有异常时的提示
		builder.WriteString("### ✅ 费用状态\n")
		builder.WriteString("费用变化在正常范围内，无异常告警\n\n")
	}
}

// buildFooterSection 构建页脚段落
func (f *DefaultMessageFormatter) buildFooterSection(builder *strings.Builder, generatedAt time.Time) {
	builder.WriteString("\n---\n")
	builder.WriteString(fmt.Sprintf("*⏰ 生成时间: %s*\n",
		generatedAt.Format("2006-01-02 15:04:05")))
	builder.WriteString("*💡 说明: 对比昨天与前天的费用数据*")
}

// getProviderIcon 获取服务商图标
func (f *DefaultMessageFormatter) getProviderIcon(provider string) string {
	switch strings.ToLower(provider) {
	case "volcengine":
		return "🌋"
	case "alicloud":
		return "🐾"
	case "aws":
		return "🚀"
	case "azure":
		return "💙"
	case "gcp":
		return "🌐"
	default:
		return "☁️"
	}
}

// formatCostChange 格式化费用变化
func (f *DefaultMessageFormatter) formatCostChange(total *CostChange) string {
	if total.ChangeAmount != 0 {
		changeIcon := total.GetChangeIcon()
		if total.ChangeAmount > 0 {
			return fmt.Sprintf("%s **+%.2f元 (%+.1f%%)**",
				changeIcon, total.ChangeAmount, total.ChangePercent)
		} else {
			return fmt.Sprintf("%s %.2f元 (%.1f%%)",
				changeIcon, total.ChangeAmount, total.ChangePercent)
		}
	}
	return "➡️ 无变化"
}

// formatProductChange 格式化产品费用变化
func (f *DefaultMessageFormatter) formatProductChange(product *CostChange) string {
	if product.ChangeAmount != 0 {
		if product.ChangePercent > 20 {
			// 超过20%增长，使用加粗
			return fmt.Sprintf("**%+.1f%%**", product.ChangePercent)
		} else if product.ChangePercent < -20 {
			// 超过20%下降
			return fmt.Sprintf("%.1f%%", product.ChangePercent)
		} else {
			// 正常变化
			return fmt.Sprintf("%+.1f%%", product.ChangePercent)
		}
	}
	return "无变化"
}

// FormatCostReportCard 格式化费用报告为模板卡片
func (f *DefaultMessageFormatter) FormatCostReportCard(data *CostComparisonData) *TemplateCard {
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
		f.buildCardEmphasisContent(card, data.TotalCost)
	}

	// 设置二级标题
	card.SubTitleText = "📊 各云服务商费用明细"

	// 设置水平内容列表（各服务商费用）
	card.HorizontalContentList = f.buildHorizontalContentList(data.Providers)

	// 设置引用区域（异常提醒）
	f.buildCardQuoteArea(card, data.Alerts)

	// 添加跳转指引
	f.buildCardJumpList(card)

	// card_action是必需的，设置整体卡片跳转
	f.buildCardAction(card)

	return card
}

// buildCardEmphasisContent 构建卡片关键数据内容
func (f *DefaultMessageFormatter) buildCardEmphasisContent(card *TemplateCard, total *CostChange) {
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

// buildHorizontalContentList 构建水平内容列表
func (f *DefaultMessageFormatter) buildHorizontalContentList(providers []*ProviderCostData) []CardHorizontalContent {
	var horizontalList []CardHorizontalContent

	for _, provider := range providers {
		if provider.TotalCost == nil {
			continue
		}

		// 添加服务商总计
		f.addProviderContent(&horizontalList, provider)

		// 添加主要产品明细（前3个）
		f.addProductsContent(&horizontalList, provider.Products)
	}

	return horizontalList
}

// addProviderContent 添加服务商内容
func (f *DefaultMessageFormatter) addProviderContent(horizontalList *[]CardHorizontalContent, provider *ProviderCostData) {
	// 服务商图标
	icon := f.getProviderIcon(provider.Provider)

	// 费用变化描述
	total := provider.TotalCost
	changeDesc := f.formatCardCostChange(total)

	*horizontalList = append(*horizontalList, CardHorizontalContent{
		KeyName: fmt.Sprintf("%s %s", icon, provider.DisplayName),
		Value: fmt.Sprintf("¥%.2f → ¥%.2f %s",
			total.YesterdayCost, total.TodayCost, changeDesc),
		Type: 0,
	})
}

// addProductsContent 添加产品内容
func (f *DefaultMessageFormatter) addProductsContent(horizontalList *[]CardHorizontalContent, products []*CostChange) {
	maxProducts := 3
	if len(products) < maxProducts {
		maxProducts = len(products)
	}

	for i := 0; i < maxProducts; i++ {
		product := products[i]
		productChange := f.formatCardProductChange(product)

		*horizontalList = append(*horizontalList, CardHorizontalContent{
			KeyName: fmt.Sprintf("  └ %s", product.Name),
			Value: fmt.Sprintf("¥%.0f → ¥%.0f %s",
				product.YesterdayCost, product.TodayCost, productChange),
			Type: 0,
		})
	}

	if len(products) > maxProducts {
		*horizontalList = append(*horizontalList, CardHorizontalContent{
			KeyName: fmt.Sprintf("  └ 其他%d个产品", len(products)-maxProducts),
			Value:   "点击查看详情",
			Type:    0,
		})
	}
}

// formatCardCostChange 格式化卡片费用变化
func (f *DefaultMessageFormatter) formatCardCostChange(total *CostChange) string {
	if total.ChangeAmount != 0 {
		if total.ChangeAmount > 0 {
			return fmt.Sprintf("📈 +%.1f%%", total.ChangePercent)
		} else {
			return fmt.Sprintf("📉 %.1f%%", total.ChangePercent)
		}
	}
	return "➡️ 无变化"
}

// formatCardProductChange 格式化卡片产品变化
func (f *DefaultMessageFormatter) formatCardProductChange(product *CostChange) string {
	if product.ChangePercent > 20 {
		return fmt.Sprintf("⚠️ +%.1f%%", product.ChangePercent)
	} else if product.ChangePercent < -20 {
		return fmt.Sprintf("✅ %.1f%%", product.ChangePercent)
	} else if product.ChangeAmount != 0 {
		return fmt.Sprintf("%+.1f%%", product.ChangePercent)
	}
	return ""
}

// buildCardQuoteArea 构建卡片引用区域
func (f *DefaultMessageFormatter) buildCardQuoteArea(card *TemplateCard, alerts []string) {
	if len(alerts) > 0 {
		alertText := f.buildCardAlertText(alerts)
		card.QuoteArea = &CardQuoteArea{
			Type:      0,
			Title:     fmt.Sprintf("⚠️ 发现 %d 个费用异常", len(alerts)),
			QuoteText: alertText,
		}
	} else {
		card.QuoteArea = &CardQuoteArea{
			Type:      0,
			Title:     "✅ 费用状态正常",
			QuoteText: "所有费用变化都在正常范围内",
		}
	}
}

// buildCardAlertText 构建卡片告警文本
func (f *DefaultMessageFormatter) buildCardAlertText(alerts []string) string {
	alertText := ""
	maxAlerts := 3
	if len(alerts) < maxAlerts {
		maxAlerts = len(alerts)
	}

	for i := 0; i < maxAlerts; i++ {
		if i > 0 {
			alertText += "\n"
		}
		alertText += fmt.Sprintf("• %s", alerts[i])
	}

	if len(alerts) > maxAlerts {
		alertText += fmt.Sprintf("\n• ...还有%d个告警", len(alerts)-maxAlerts)
	}

	return alertText
}

// buildCardJumpList 构建卡片跳转列表
func (f *DefaultMessageFormatter) buildCardJumpList(card *TemplateCard) {
	card.JumpList = []CardJump{
		{
			Type:  1,
			Title: "查看详细报告",
			URL:   "", // TODO: 配置实际的报告URL
		},
	}
}

// buildCardAction 构建卡片动作
func (f *DefaultMessageFormatter) buildCardAction(card *TemplateCard) {
	card.CardAction = &CardAction{
		Type: 1,
		URL:  "", // TODO: 配置实际的跳转URL
	}
}
