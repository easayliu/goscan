package wechat

import "time"

// MessageType 消息类型
type MessageType string

const (
	MessageTypeText         MessageType = "text"
	MessageTypeMarkdown     MessageType = "markdown_v2" // 只使用支持表格的v2版本
	MessageTypeTemplateCard MessageType = "template_card"
)

// WebhookMessage 企业微信webhook消息结构
type WebhookMessage struct {
	MsgType      MessageType   `json:"msgtype"`
	Text         *TextMsg      `json:"text,omitempty"`
	Markdown     *MarkdownMsg  `json:"markdown,omitempty"`    // 旧版markdown
	MarkdownV2   *MarkdownMsg  `json:"markdown_v2,omitempty"` // 新版markdown_v2
	TemplateCard *TemplateCard `json:"template_card,omitempty"`
}

// TextMsg 文本消息
type TextMsg struct {
	Content             string   `json:"content"`
	MentionedList       []string `json:"mentioned_list,omitempty"`        // @特定用户
	MentionedMobileList []string `json:"mentioned_mobile_list,omitempty"` // @手机号
}

// MarkdownMsg Markdown消息
type MarkdownMsg struct {
	Content string `json:"content"`
}

// TemplateCard 模板卡片消息
type TemplateCard struct {
	CardType              string                  `json:"card_type"`                         // 模板卡片类型，文本通知型为text_notice
	Source                *CardSource             `json:"source,omitempty"`                  // 卡片来源
	MainTitle             *CardMainTitle          `json:"main_title,omitempty"`              // 主标题
	EmphasisContent       *CardEmphasisContent    `json:"emphasis_content,omitempty"`        // 关键数据
	QuoteArea             *CardQuoteArea          `json:"quote_area,omitempty"`              // 引用文献
	SubTitleText          string                  `json:"sub_title_text,omitempty"`          // 二级标题
	HorizontalContentList []CardHorizontalContent `json:"horizontal_content_list,omitempty"` // 二级标题+文本列表
	JumpList              []CardJump              `json:"jump_list,omitempty"`               // 跳转指引
	CardAction            *CardAction             `json:"card_action,omitempty"`             // 卡片跳转
}

// CardSource 卡片来源
type CardSource struct {
	IconURL   string `json:"icon_url,omitempty"`   // 来源图标url
	Desc      string `json:"desc,omitempty"`       // 来源描述
	DescColor int    `json:"desc_color,omitempty"` // 来源文字颜色 0:灰色(默认) 1:黑色 2:红色 3:绿色
}

// CardMainTitle 主标题
type CardMainTitle struct {
	Title string `json:"title"`          // 标题内容
	Desc  string `json:"desc,omitempty"` // 标题辅助信息
}

// CardEmphasisContent 关键数据
type CardEmphasisContent struct {
	Title string `json:"title"`          // 关键数据名称
	Desc  string `json:"desc,omitempty"` // 关键数据描述
}

// CardQuoteArea 引用文献
type CardQuoteArea struct {
	Type      int    `json:"type,omitempty"`       // 引用文献类型 0:默认 1:会议
	URL       string `json:"url,omitempty"`        // 引用文献跳转链接
	Title     string `json:"title,omitempty"`      // 引用文献标题
	QuoteText string `json:"quote_text,omitempty"` // 引用文献内容
}

// CardHorizontalContent 二级标题+文本
type CardHorizontalContent struct {
	KeyName string `json:"keyname"`            // 二级标题
	Value   string `json:"value,omitempty"`    // 二级文本，如果type是1，则不需要value字段
	Type    int    `json:"type,omitempty"`     // 类型 0:默认 1:URL 2:文件附件 3:点击跳转
	URL     string `json:"url,omitempty"`      // 跳转链接
	MediaID string `json:"media_id,omitempty"` // 附件media_id
}

// CardJump 跳转指引
type CardJump struct {
	Type  int    `json:"type,omitempty"` // 跳转类型 0:默认 1:URL
	Title string `json:"title"`          // 跳转标题
	URL   string `json:"url,omitempty"`  // 跳转链接
}

// CardAction 卡片跳转
type CardAction struct {
	Type int    `json:"type"`          // 跳转类型 1:URL
	URL  string `json:"url,omitempty"` // 跳转链接
}

// WebhookResponse 企业微信webhook响应
type WebhookResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// IsSuccess 判断响应是否成功
func (r *WebhookResponse) IsSuccess() bool {
	return r.ErrCode == 0
}

// CostComparisonData 费用对比数据
type CostComparisonData struct {
	Date        string              `json:"date"`         // 对比日期
	TotalCost   *CostChange         `json:"total_cost"`   // 总费用变化
	Providers   []*ProviderCostData `json:"providers"`    // 各云服务商费用
	Alerts      []string            `json:"alerts"`       // 异常提醒
	GeneratedAt time.Time           `json:"generated_at"` // 生成时间
}

// ProviderCostData 单个云服务商费用数据
type ProviderCostData struct {
	Provider    string        `json:"provider"`     // 服务商名称
	DisplayName string        `json:"display_name"` // 显示名称
	TotalCost   *CostChange   `json:"total_cost"`   // 该服务商总费用变化
	Products    []*CostChange `json:"products"`     // 产品费用变化列表
}

// CostChange 费用变化数据
type CostChange struct {
	Name          string  `json:"name"`           // 产品或服务商名称
	YesterdayCost float64 `json:"yesterday_cost"` // 昨日费用
	TodayCost     float64 `json:"today_cost"`     // 今日费用
	ChangeAmount  float64 `json:"change_amount"`  // 变化金额
	ChangePercent float64 `json:"change_percent"` // 变化百分比
	Currency      string  `json:"currency"`       // 币种
}

// IsIncrease 判断是否为费用增长
func (cc *CostChange) IsIncrease() bool {
	return cc.ChangeAmount > 0
}

// IsDecrease 判断是否为费用降低
func (cc *CostChange) IsDecrease() bool {
	return cc.ChangeAmount < 0
}

// IsSignificantChange 判断是否为显著变化（超过指定阈值）
func (cc *CostChange) IsSignificantChange(thresholdPercent float64) bool {
	return cc.ChangePercent >= thresholdPercent || cc.ChangePercent <= -thresholdPercent
}

// GetChangeIcon 获取变化图标
func (cc *CostChange) GetChangeIcon() string {
	if cc.IsIncrease() {
		return "⬆️"
	} else if cc.IsDecrease() {
		return "⬇️"
	}
	return "➡️"
}

// FormatCurrency 格式化货币显示
func (cc *CostChange) FormatCurrency(amount float64) string {
	switch cc.Currency {
	case "CNY", "RMB", "":
		return "¥"
	case "USD":
		return "$"
	case "EUR":
		return "€"
	default:
		return cc.Currency + " "
	}
}
