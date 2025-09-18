package wechat

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"goscan/pkg/logger"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Client WeChat Enterprise client
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

// NotificationFormat notification format type
type NotificationFormat string

const (
	FormatMarkdown     NotificationFormat = "markdown"      // Markdown table format (default)
	FormatTemplateCard NotificationFormat = "template_card" // Template card format
	FormatAuto         NotificationFormat = "auto"          // Auto selection (prefer template card)
)

// Config client configuration
type Config struct {
	WebhookURL         string             `json:"webhook_url"`
	MaxRetries         int                `json:"max_retries"`
	RetryDelay         time.Duration      `json:"retry_delay"`
	Timeout            time.Duration      `json:"timeout"`
	MentionUsers       []string           `json:"mention_users"`
	NotificationFormat NotificationFormat `json:"notification_format"` // Notification format
}

// NewClient creates WeChat Enterprise client
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

// SendText sends text message
func (c *Client) SendText(ctx context.Context, content string) error {
	msg := c.messageBuilder.BuildTextMessage(content, c.mentionUsers)
	return c.sendMessage(ctx, msg)
}

// SendMarkdown sends Markdown message (markdown_v2 supports tables)
func (c *Client) SendMarkdown(ctx context.Context, content string) error {
	msg := c.messageBuilder.BuildMarkdownMessage(content)
	return c.sendMessage(ctx, msg)
}

// SendTemplateCard sends template card message
func (c *Client) SendTemplateCard(ctx context.Context, card *TemplateCard) error {
	msg := c.messageBuilder.BuildTemplateCardMessage(card)
	return c.sendMessage(ctx, msg)
}

// SendImage sends image message
func (c *Client) SendImage(ctx context.Context, imageData []byte) error {
	// Validate image size (max 2MB)
	if len(imageData) > 2*1024*1024 {
		return fmt.Errorf("image size %d bytes exceeds 2MB limit", len(imageData))
	}

	// Convert to base64 and calculate MD5
	base64Str := base64.StdEncoding.EncodeToString(imageData)
	hash := md5.Sum(imageData)
	md5Str := hex.EncodeToString(hash[:])

	msg := c.messageBuilder.BuildImageMessage(base64Str, md5Str)
	return c.sendMessage(ctx, msg)
}

// SendCostReport sends cost comparison report
func (c *Client) SendCostReport(ctx context.Context, data *CostComparisonData) error {
	return c.SendCostReportWithFormat(ctx, data, c.notificationFormat)
}

// SendCostReportWithFormat sends cost comparison report with specified format
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

// SendCostReportAsImage sends cost comparison report as image
func (c *Client) SendCostReportAsImage(ctx context.Context, imageData []byte) error {
	return c.SendImage(ctx, imageData)
}

// sendMessage sends message with retry
func (c *Client) sendMessage(ctx context.Context, msg *WebhookMessage) error {
	var lastError error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			// Wait for retry delay
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.retryDelay):
			}
		}

		err := c.doSendMessage(ctx, msg)
		if err == nil {
			return nil // Send successful
		}

		lastError = err
		if attempt < c.maxRetries {
			logger.Warn("WeChat message send failed, retrying",
				zap.Duration("retry_delay", c.retryDelay),
				zap.Int("attempt", attempt+1),
				zap.Int("max_retries", c.maxRetries),
				zap.Error(err))
		}
	}

	return fmt.Errorf("failed to send WeChat message after %d retries: %w", c.maxRetries, lastError)
}

// doSendMessage executes actual message sending
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

// TestConnection tests connection
func (c *Client) TestConnection(ctx context.Context) error {
	testMsg := c.messageFormatter.FormatTestMessage()
	return c.SendMarkdown(ctx, testMsg)
}

// applyDefaultConfig applies default configuration
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
		config.NotificationFormat = FormatMarkdown // Default to Markdown format
	}
}

// createHTTPClient creates HTTP client
func createHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
	}
}

// sendTemplateCardReport sends template card format report
func (c *Client) sendTemplateCardReport(ctx context.Context, data *CostComparisonData) error {
	card := c.messageFormatter.FormatCostReportCard(data)
	return c.SendTemplateCard(ctx, card)
}

// sendMarkdownReport sends Markdown format report
func (c *Client) sendMarkdownReport(ctx context.Context, data *CostComparisonData) error {
	content := c.messageFormatter.FormatCostReport(data)
	return c.SendMarkdown(ctx, content)
}

// sendAutoFormatReport sends auto format report
func (c *Client) sendAutoFormatReport(ctx context.Context, data *CostComparisonData) error {
	// Try template card first, fallback to Markdown if failed
	card := c.messageFormatter.FormatCostReportCard(data)
	if err := c.SendTemplateCard(ctx, card); err != nil {
		// Template card failed, fallback to Markdown format
		logger.Warn("Template card send failed, falling back to Markdown format", zap.Error(err))
		content := c.messageFormatter.FormatCostReport(data)
		return c.SendMarkdown(ctx, content)
	}
	return nil
}

// validateWebhookURL validates webhook URL
func (c *Client) validateWebhookURL() error {
	if c.webhookURL == "" {
		return fmt.Errorf("%w", ErrWebhookURLEmpty)
	}
	return nil
}

// marshalMessage serializes message
func (c *Client) marshalMessage(msg *WebhookMessage) ([]byte, error) {
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMarshalMessage, err)
	}
	return jsonData, nil
}

// doHTTPRequest executes HTTP request
func (c *Client) doHTTPRequest(ctx context.Context, jsonData []byte) (*http.Response, []byte, error) {
	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", c.webhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrCreateRequest, err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrSendRequest, err)
	}
	defer resp.Body.Close()

	// Read response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, nil, fmt.Errorf("%w: %v", ErrReadResponse, err)
	}

	return resp, respBody, nil
}

// checkHTTPStatus checks HTTP status code
func (c *Client) checkHTTPStatus(resp *http.Response, respBody []byte) error {
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s", ErrHTTPStatusError,
			NewHTTPError(resp.StatusCode, resp.Status, string(respBody)).Error())
	}
	return nil
}

// checkAPIResponse checks API response
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
