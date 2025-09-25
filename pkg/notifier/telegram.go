package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"goscan/pkg/logger"

	"go.uber.org/zap"
)

// TelegramConfig represents Telegram notification configuration
type TelegramConfig struct {
	Enabled  bool   `json:"enabled"`
	BotToken string `json:"bot_token"`
	ChatID   string `json:"chat_id"`
	Timeout  int    `json:"timeout"`
}

// TelegramNotifier handles Telegram notifications
type TelegramNotifier struct {
	config     *TelegramConfig
	httpClient *http.Client
}

// TelegramMessage represents a message to be sent via Telegram
type TelegramMessage struct {
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode,omitempty"`
}

// TelegramResponse represents Telegram API response
type TelegramResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description,omitempty"`
	ErrorCode   int    `json:"error_code,omitempty"`
}

// NewTelegramNotifier creates a new Telegram notifier
func NewTelegramNotifier(config *TelegramConfig) *TelegramNotifier {
	return &TelegramNotifier{
		config: config,
		httpClient: &http.Client{
			Timeout: time.Duration(config.Timeout) * time.Second,
		},
	}
}

// SendMessage sends a message via Telegram
func (t *TelegramNotifier) SendMessage(ctx context.Context, message string) error {
	if !t.config.Enabled {
		logger.Debug("Telegram notifications disabled")
		return nil
	}

	if t.config.BotToken == "" || t.config.ChatID == "" {
		logger.Warn("Telegram bot token or chat ID not configured")
		return fmt.Errorf("telegram bot token or chat ID not configured")
	}

	telegramMsg := TelegramMessage{
		ChatID:    t.config.ChatID,
		Text:      message,
		ParseMode: "Markdown",
	}

	return t.sendTelegramMessage(ctx, &telegramMsg)
}

// SendStockAlert sends a stock availability alert
func (t *TelegramNotifier) SendStockAlert(ctx context.Context, productName, storage, storeName, status, quote string) error {
	var message string
	var emoji string

	switch status {
	case "available":
		emoji = "🎉"
		message = fmt.Sprintf("%s *有库存啦！*\n\n📱 *产品:* %s [%s]\n🏪 *店铺:* %s\n✅ *状态:* 有库存\n⏰ *取货时间:* %s",
			emoji, productName, storage, storeName, quote)
	case "limited":
		emoji = "⚠️"
		message = fmt.Sprintf("%s *库存有限*\n\n📱 *产品:* %s [%s]\n🏪 *店铺:* %s\n📊 *状态:* 库存有限\n⏰ *取货时间:* %s",
			emoji, productName, storage, storeName, quote)
	default:
		emoji = "😞"
		message = fmt.Sprintf("%s *库存更新*\n\n📱 *产品:* %s [%s]\n🏪 *店铺:* %s\n❌ *状态:* %s",
			emoji, productName, storage, storeName, status)
	}

	return t.SendMessage(ctx, message)
}

// SendSummaryReport sends a monitoring summary report
func (t *TelegramNotifier) SendSummaryReport(ctx context.Context, productsChecked, storesChecked, totalChecks int, availabilityChanges int) error {
	message := fmt.Sprintf("📊 *苹果库存监控汇总*\n\n"+
		"📦 检查产品: %d\n"+
		"🏪 检查店铺: %d\n"+
		"🔍 总检查次数: %d\n"+
		"🔄 库存变化: %d\n"+
		"⏰ 时间: %s",
		productsChecked, storesChecked, totalChecks, availabilityChanges,
		time.Now().Format("2006-01-02 15:04:05"))

	return t.SendMessage(ctx, message)
}

// sendTelegramMessage sends message to Telegram API
func (t *TelegramNotifier) sendTelegramMessage(ctx context.Context, message *TelegramMessage) error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.config.BotToken)

	jsonData, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	logger.Debug("Sending Telegram message",
		zap.String("chat_id", message.ChatID),
		zap.String("text", message.Text[:min(100, len(message.Text))]))

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	var telegramResp TelegramResponse
	if err := json.NewDecoder(resp.Body).Decode(&telegramResp); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !telegramResp.OK {
		return fmt.Errorf("telegram API error: %s (code: %d)", telegramResp.Description, telegramResp.ErrorCode)
	}

	logger.Info("Telegram message sent successfully")
	return nil
}

// ValidateConfig validates Telegram configuration
func (t *TelegramNotifier) ValidateConfig() error {
	if !t.config.Enabled {
		return nil
	}

	if t.config.BotToken == "" {
		return fmt.Errorf("telegram bot token is required when enabled")
	}

	if t.config.ChatID == "" {
		return fmt.Errorf("telegram chat ID is required when enabled")
	}

	return nil
}

// TestConnection tests Telegram bot connection
func (t *TelegramNotifier) TestConnection(ctx context.Context) error {
	if !t.config.Enabled {
		return fmt.Errorf("telegram notifications are disabled")
	}

	testMessage := "🍎 *苹果库存监控测试*\n\nTelegram通知功能正常工作！"
	return t.SendMessage(ctx, testMessage)
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}