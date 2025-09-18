package wechat

import (
	"fmt"
	"strings"
	"time"
)

// DefaultMessageBuilder default message builder
type DefaultMessageBuilder struct{}

// NewMessageBuilder creates message builder
func NewMessageBuilder() MessageBuilder {
	return &DefaultMessageBuilder{}
}

// BuildTextMessage builds text message
func (b *DefaultMessageBuilder) BuildTextMessage(content string, mentionUsers []string) *WebhookMessage {
	return &WebhookMessage{
		MsgType: MessageTypeText,
		Text: &TextMsg{
			Content:       content,
			MentionedList: mentionUsers,
		},
	}
}

// BuildMarkdownMessage builds Markdown message
func (b *DefaultMessageBuilder) BuildMarkdownMessage(content string) *WebhookMessage {
	return &WebhookMessage{
		MsgType: MessageTypeMarkdown,
		MarkdownV2: &MarkdownMsg{
			Content: content,
		},
	}
}

// BuildTemplateCardMessage builds template card message
func (b *DefaultMessageBuilder) BuildTemplateCardMessage(card *TemplateCard) *WebhookMessage {
	return &WebhookMessage{
		MsgType:      MessageTypeTemplateCard,
		TemplateCard: card,
	}
}

// BuildImageMessage builds image message
func (b *DefaultMessageBuilder) BuildImageMessage(base64, md5 string) *WebhookMessage {
	return &WebhookMessage{
		MsgType: MessageTypeImage,
		Image: &ImageMsg{
			Base64: base64,
			MD5:    md5,
		},
	}
}

// DefaultMessageFormatter default message formatter
type DefaultMessageFormatter struct{}

// NewMessageFormatter creates message formatter
func NewMessageFormatter() MessageFormatter {
	return &DefaultMessageFormatter{}
}

// FormatTestMessage formats test message
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

// FormatCostReport formats cost report as Markdown
func (f *DefaultMessageFormatter) FormatCostReport(data *CostComparisonData) string {
	var builder strings.Builder

	// Title - use emoji to enhance visual effect
	builder.WriteString("## 📊 云服务费用日报\n\n")

	// Date information
	builder.WriteString(fmt.Sprintf("📅 **报告日期：%s**\n\n", data.Date))

	// Details of each cloud provider
	for _, provider := range data.Providers {
		if len(provider.Products) == 0 {
			continue
		}

		f.buildProviderSection(&builder, provider)
	}

	// Exception alerts
	f.buildAlertsSection(&builder, data.Alerts)

	// Generation time and description
	f.buildFooterSection(&builder, data.GeneratedAt)

	return builder.String()
}

// buildProviderSection builds provider cost section
func (f *DefaultMessageFormatter) buildProviderSection(builder *strings.Builder, provider *ProviderCostData) {
	// Provider icon
	icon := f.getProviderIcon(provider.Provider)

	// Provider title
	builder.WriteString(fmt.Sprintf("### %s %s\n", icon, provider.DisplayName))

	// Provider total - use table format
	if provider.TotalCost != nil {
		f.buildTotalCostTable(builder, provider.TotalCost)
	}

	// Product cost list - use table format to display all products
	if len(provider.Products) > 0 {
		f.buildProductsTable(builder, provider.Products)
	}
}

// buildTotalCostTable builds total cost table
func (f *DefaultMessageFormatter) buildTotalCostTable(builder *strings.Builder, total *CostChange) {
	// Change trend icon
	changeIcon := "➡️"
	if total.ChangeAmount > 0 {
		changeIcon = "📈"
	} else if total.ChangeAmount < 0 {
		changeIcon = "📉"
	}

	// Change magnitude
	changeStr := f.formatProductChange(total)

	// Build table format consistent with product details
	tableStr := fmt.Sprintf("| 服务商 | 前天费用 | 昨天费用 | 变化趋势 | 变化幅度 |\n| :--- | ---: | ---: | :---: | ---: |\n| **总计** | ¥%.2f | ¥%.2f | %s | %s |",
		total.YesterdayCost, total.TodayCost, changeIcon, changeStr)
	builder.WriteString(tableStr + "\n\n")
}

// buildProductsTable builds product cost table
func (f *DefaultMessageFormatter) buildProductsTable(builder *strings.Builder, products []*CostChange) {
	builder.WriteString("**📦 产品明细：**\n\n")

	// Build complete table string
	var tableBuilder strings.Builder
	tableBuilder.WriteString("| 产品名称 | 前天费用 | 昨天费用 | 变化趋势 | 变化幅度 |\n| :--- | ---: | ---: | :---: | ---: |")

	// Display all products
	for _, product := range products {
		f.buildProductRow(&tableBuilder, product)
	}

	builder.WriteString(tableBuilder.String())
	builder.WriteString("\n\n")
}

// buildProductRow builds product cost row
func (f *DefaultMessageFormatter) buildProductRow(builder *strings.Builder, product *CostChange) {
	// Change trend icon
	changeIcon := "➡️"
	if product.ChangeAmount > 0 {
		changeIcon = "📈"
	} else if product.ChangeAmount < 0 {
		changeIcon = "📉"
	}

	// Change magnitude
	changeStr := f.formatProductChange(product)

	// Add product row
	builder.WriteString(fmt.Sprintf("\n| %s | ¥%.2f | ¥%.2f | %s | %s |",
		product.Name, product.YesterdayCost, product.TodayCost, changeIcon, changeStr))
}

// buildAlertsSection builds exception alerts section
func (f *DefaultMessageFormatter) buildAlertsSection(builder *strings.Builder, alerts []string) {
	if len(alerts) > 0 {
		builder.WriteString("### ⚠️ 费用异常提醒\n")

		// Use different prompts based on alert count
		if len(alerts) > 5 {
			builder.WriteString(fmt.Sprintf("**发现 %d 个异常变化，请关注！**\n\n",
				len(alerts)))
		}

		// Display the top 5 most important alerts
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
		// Prompt when there are no exceptions
		builder.WriteString("### ✅ 费用状态\n")
		builder.WriteString("费用变化在正常范围内，无异常告警\n\n")
	}
}

// buildFooterSection builds footer section
func (f *DefaultMessageFormatter) buildFooterSection(builder *strings.Builder, generatedAt time.Time) {
	builder.WriteString("\n---\n")
	builder.WriteString(fmt.Sprintf("*⏰ 生成时间: %s*\n",
		generatedAt.Format("2006-01-02 15:04:05")))
	builder.WriteString("*💡 说明: 对比昨天与前天的费用数据*")
}

// getProviderIcon gets provider icon
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

// formatProductChange formats product cost change
func (f *DefaultMessageFormatter) formatProductChange(product *CostChange) string {
	if product.ChangeAmount != 0 {
		if product.ChangePercent > 20 {
			// Over 20% growth, use bold
			return fmt.Sprintf("**%+.1f%%**", product.ChangePercent)
		} else if product.ChangePercent < -20 {
			// Over 20% decrease
			return fmt.Sprintf("%.1f%%", product.ChangePercent)
		} else {
			// Normal change
			return fmt.Sprintf("%+.1f%%", product.ChangePercent)
		}
	}
	return "无变化"
}

// FormatCostReportCard formats cost report as template card
func (f *DefaultMessageFormatter) FormatCostReportCard(data *CostComparisonData) *TemplateCard {
	card := &TemplateCard{
		CardType: "text_notice",
	}

	// Set source information
	card.Source = &CardSource{
		Desc:      "费用分析系统",
		DescColor: 0, // Gray color
	}

	// Set main title
	card.MainTitle = &CardMainTitle{
		Title: "☁️ 云服务费用日报",
		Desc:  fmt.Sprintf("📅 %s", data.Date),
	}

	// Set key data (total cost change)
	if data.TotalCost != nil {
		f.buildCardEmphasisContent(card, data.TotalCost)
	}

	// Set secondary title
	card.SubTitleText = "📊 各云服务商费用明细"

	// Set horizontal content list (provider costs)
	card.HorizontalContentList = f.buildHorizontalContentList(data.Providers)

	// Set quote area (exception alerts)
	f.buildCardQuoteArea(card, data.Alerts)

	// Add jump guide
	f.buildCardJumpList(card)

	// card_action is required, set overall card jump
	f.buildCardAction(card)

	return card
}

// buildCardEmphasisContent builds card key data content
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

// buildHorizontalContentList builds horizontal content list
func (f *DefaultMessageFormatter) buildHorizontalContentList(providers []*ProviderCostData) []CardHorizontalContent {
	var horizontalList []CardHorizontalContent

	for _, provider := range providers {
		if provider.TotalCost == nil {
			continue
		}

		// Add provider total
		f.addProviderContent(&horizontalList, provider)

		// Add main product details (top 3)
		f.addProductsContent(&horizontalList, provider.Products)
	}

	return horizontalList
}

// addProviderContent adds provider content
func (f *DefaultMessageFormatter) addProviderContent(horizontalList *[]CardHorizontalContent, provider *ProviderCostData) {
	// Provider icon
	icon := f.getProviderIcon(provider.Provider)

	// Cost change description
	total := provider.TotalCost
	changeDesc := f.formatCardCostChange(total)

	*horizontalList = append(*horizontalList, CardHorizontalContent{
		KeyName: fmt.Sprintf("%s %s", icon, provider.DisplayName),
		Value: fmt.Sprintf("¥%.2f → ¥%.2f %s",
			total.YesterdayCost, total.TodayCost, changeDesc),
		Type: 0,
	})
}

// addProductsContent adds product content
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

// formatCardCostChange formats card cost change
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

// formatCardProductChange formats card product change
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

// buildCardQuoteArea builds card quote area
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

// buildCardAlertText builds card alert text
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

// buildCardJumpList builds card jump list
func (f *DefaultMessageFormatter) buildCardJumpList(card *TemplateCard) {
	card.JumpList = []CardJump{
		{
			Type:  1,
			Title: "查看详细报告",
			URL:   "", // TODO: 配置实际的报告URL
		},
	}
}

// buildCardAction builds card action
func (f *DefaultMessageFormatter) buildCardAction(card *TemplateCard) {
	card.CardAction = &CardAction{
		Type: 1,
		URL:  "", // TODO: 配置实际的跳转URL
	}
}
