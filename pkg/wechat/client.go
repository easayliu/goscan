package wechat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	messageBuilder     MessageBuilder
	messageFormatter   MessageFormatter
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
	applyDefaultConfig(config)
	
	return &Client{
		webhookURL:         config.WebhookURL,
		httpClient:         createHTTPClient(config.Timeout),
		maxRetries:         config.MaxRetries,
		retryDelay:         config.RetryDelay,
		mentionUsers:       config.MentionUsers,
		notificationFormat: config.NotificationFormat,
		messageBuilder:     NewMessageBuilder(),
		messageFormatter:   NewMessageFormatter(),
	}
}

// SendText 发送文本消息
func (c *Client) SendText(ctx context.Context, content string) error {
	msg := c.messageBuilder.BuildTextMessage(content, c.mentionUsers)
	return c.sendMessage(ctx, msg)
}

// SendMarkdown 发送Markdown消息（markdown_v2支持表格）
func (c *Client) SendMarkdown(ctx context.Context, content string) error {
	msg := c.messageBuilder.BuildMarkdownMessage(content)
	return c.sendMessage(ctx, msg)
}

// SendTemplateCard 发送模板卡片消息
func (c *Client) SendTemplateCard(ctx context.Context, card *TemplateCard) error {
	msg := c.messageBuilder.BuildTemplateCardMessage(card)
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
		return c.sendTemplateCardReport(ctx, data)
	case FormatMarkdown:
		return c.sendMarkdownReport(ctx, data)
	case FormatAuto:
		return c.sendAutoFormatReport(ctx, data)
	default:
		return c.sendMarkdownReport(ctx, data)
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
	if err := c.validateWebhookURL(); err != nil {
		return err
	}
	
	jsonData, err := c.marshalMessage(msg)
	if err != nil {
		return err
	}
	
	resp, respBody, err := c.doHTTPRequest(ctx, jsonData)
	if err != nil {
		return err
	}
	
	if err := c.checkHTTPStatus(resp, respBody); err != nil {
		return err
	}
	
	return c.checkAPIResponse(respBody)
}


// TestConnection 测试连接
func (c *Client) TestConnection(ctx context.Context) error {
	testMsg := c.messageFormatter.FormatTestMessage()
	return c.SendMarkdown(ctx, testMsg)
}

// applyDefaultConfig 应用默认配置
func applyDefaultConfig(config *Config) {
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
}

// createHTTPClient 创建HTTP客户端
func createHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
	}
}

// sendTemplateCardReport 发送模板卡片格式报告
func (c *Client) sendTemplateCardReport(ctx context.Context, data *CostComparisonData) error {
	card := c.messageFormatter.FormatCostReportCard(data)
	return c.SendTemplateCard(ctx, card)
}

// sendMarkdownReport 发送Markdown格式报告
func (c *Client) sendMarkdownReport(ctx context.Context, data *CostComparisonData) error {
	content := c.messageFormatter.FormatCostReport(data)
	return c.SendMarkdown(ctx, content)
}

// sendAutoFormatReport 发送自动格式报告
func (c *Client) sendAutoFormatReport(ctx context.Context, data *CostComparisonData) error {
	// 优先尝试模板卡片，失败则降级到Markdown
	card := c.messageFormatter.FormatCostReportCard(data)
	if err := c.SendTemplateCard(ctx, card); err != nil {
		// 模板卡片失败，降级使用Markdown格式
		fmt.Printf("模板卡片发送失败，降级使用Markdown格式: %v\n", err)
		content := c.messageFormatter.FormatCostReport(data)
		return c.SendMarkdown(ctx, content)
	}
	return nil
}

// validateWebhookURL 验证webhook URL
func (c *Client) validateWebhookURL() error {
	if c.webhookURL == "" {
		return fmt.Errorf("%w", ErrWebhookURLEmpty)
	}
	return nil
}

// marshalMessage 序列化消息
func (c *Client) marshalMessage(msg *WebhookMessage) ([]byte, error) {
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMarshalMessage, err)
	}
	return jsonData, nil
}

// doHTTPRequest 执行HTTP请求
func (c *Client) doHTTPRequest(ctx context.Context, jsonData []byte) (*http.Response, []byte, error) {
	// 创建HTTP请求
	req, err := http.NewRequestWithContext(ctx, "POST", c.webhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrCreateRequest, err)
	}

	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrSendRequest, err)
	}
	defer resp.Body.Close()

	// 读取响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("%w: %v", ErrReadResponse, err)
	}

	return resp, respBody, nil
}

// checkHTTPStatus 检查HTTP状态码
func (c *Client) checkHTTPStatus(resp *http.Response, respBody []byte) error {
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s", ErrHTTPStatusError, 
			NewHTTPError(resp.StatusCode, resp.Status, string(respBody)).Error())
	}
	return nil
}

// checkAPIResponse 检查API响应
func (c *Client) checkAPIResponse(respBody []byte) error {
	var webhookResp WebhookResponse
	if err := json.Unmarshal(respBody, &webhookResp); err != nil {
		return fmt.Errorf("%w: %v", ErrUnmarshalResponse, err)
	}

	if !webhookResp.IsSuccess() {
		return fmt.Errorf("%w: %s", ErrAPIError, 
			NewAPIError(webhookResp.ErrCode, webhookResp.ErrMsg).Error())
	}

	return nil
}

