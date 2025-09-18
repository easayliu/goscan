package wechat

import (
	"context"
	"time"
)

// Sender 消息发送器接口
type Sender interface {
	// SendText 发送文本消息
	SendText(ctx context.Context, content string) error

	// SendMarkdown 发送Markdown消息
	SendMarkdown(ctx context.Context, content string) error

	// SendTemplateCard 发送模板卡片消息
	SendTemplateCard(ctx context.Context, card *TemplateCard) error
}

// CostReporter 费用报告发送器接口
type CostReporter interface {
	// SendCostReport 发送费用对比报告
	SendCostReport(ctx context.Context, data *CostComparisonData) error

	// SendCostReportWithFormat 使用指定格式发送费用对比报告
	SendCostReportWithFormat(ctx context.Context, data *CostComparisonData, format NotificationFormat) error
}

// ConnectionTester 连接测试器接口
type ConnectionTester interface {
	// TestConnection 测试连接
	TestConnection(ctx context.Context) error
}

// MessageFormatter 消息格式化器接口
type MessageFormatter interface {
	// FormatCostReport 格式化费用报告为Markdown
	FormatCostReport(data *CostComparisonData) string

	// FormatCostReportCard 格式化费用报告为模板卡片
	FormatCostReportCard(data *CostComparisonData) *TemplateCard

	// FormatTestMessage 格式化测试消息
	FormatTestMessage() string
}

// HTTPClient HTTP客户端接口
type HTTPClient interface {
	// Do 执行HTTP请求
	Do(req *HTTPRequest) (*HTTPResponse, error)
}

// HTTPRequest HTTP请求接口
type HTTPRequest interface {
	// GetMethod 获取请求方法
	GetMethod() string

	// GetURL 获取请求URL
	GetURL() string

	// GetHeaders 获取请求头
	GetHeaders() map[string]string

	// GetBody 获取请求体
	GetBody() []byte
}

// HTTPResponse HTTP响应接口
type HTTPResponse interface {
	// GetStatusCode 获取状态码
	GetStatusCode() int

	// GetHeaders 获取响应头
	GetHeaders() map[string]string

	// GetBody 获取响应体
	GetBody() []byte

	// Close 关闭响应
	Close() error
}

// WechatClient 企业微信客户端完整接口
type WechatClient interface {
	Sender
	CostReporter
	ConnectionTester
}

// MessageBuilder 消息构建器接口
type MessageBuilder interface {
	// BuildTextMessage 构建文本消息
	BuildTextMessage(content string, mentionUsers []string) *WebhookMessage

	// BuildMarkdownMessage 构建Markdown消息
	BuildMarkdownMessage(content string) *WebhookMessage

	// BuildTemplateCardMessage 构建模板卡片消息
	BuildTemplateCardMessage(card *TemplateCard) *WebhookMessage

	// BuildImageMessage 构建图片消息
	BuildImageMessage(base64, md5 string) *WebhookMessage
}

// RetryPolicy 重试策略接口
type RetryPolicy interface {
	// ShouldRetry 判断是否应该重试
	ShouldRetry(attempt int, err error) bool

	// GetDelay 获取重试延迟时间
	GetDelay(attempt int) time.Duration

	// MaxAttempts 获取最大重试次数
	MaxAttempts() int
}

// Logger interface defining logging methods
type Logger interface {
	// Debug logs debug information
	Debug(format string, args ...interface{})

	// Info logs informational messages
	Info(format string, args ...interface{})

	// Warn logs warnings
	Warn(format string, args ...interface{})

	// Error logs errors
	Error(format string, args ...interface{})
}
