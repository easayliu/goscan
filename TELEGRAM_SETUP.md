# Telegram 通知设置指南

## 1. 创建 Telegram Bot

1. 在 Telegram 中搜索 `@BotFather`
2. 发送 `/newbot` 命令
3. 按照提示输入 bot 名称和用户名
4. 获取 Bot Token (格式: `123456789:AABBccDDeeFFggHHiiJJkkLLmmNNooP-Pqq`)

## 2. 获取 Chat ID

### 方法一：使用个人聊天
1. 向你的 bot 发送任意消息
2. 访问: `https://api.telegram.org/bot<BOT_TOKEN>/getUpdates`
3. 在返回的 JSON 中找到 `chat.id` 字段

### 方法二：使用群组
1. 将 bot 添加到群组
2. 在群组中发送消息 `/start`
3. 访问: `https://api.telegram.org/bot<BOT_TOKEN>/getUpdates`
4. 找到群组的 chat ID (通常是负数)

## 3. 配置监控系统

在 `config/apple_monitor.json` 中添加 Telegram 配置：

```json
{
  "monitoring": {
    "enabled": true,
    "default_interval": 10,
    "max_concurrent": 5,
    "log_level": "info",
    "rate_limit_per_minute": 10,
    "request_timeout": 30
  },
  "telegram": {
    "enabled": true,
    "bot_token": "你的_BOT_TOKEN",
    "chat_id": "你的_CHAT_ID",
    "timeout": 30
  },
  "products": [
    // ... 产品配置
  ],
  "stores": [
    // ... 店铺配置
  ]
}
```

## 4. 测试 Telegram 通知

使用测试程序验证配置：

```bash
# 方法一：使用命令行参数
go run cmd/test_telegram/main.go "你的_BOT_TOKEN" "你的_CHAT_ID"

# 方法二：使用环境变量
export TELEGRAM_BOT_TOKEN="你的_BOT_TOKEN"
export TELEGRAM_CHAT_ID="你的_CHAT_ID"
go run cmd/test_telegram/main.go
```

## 5. 启动监控

```bash
go run cmd/simple_apple_monitor/main.go
```

## 6. 通知类型

### 库存可用通知
当检测到产品有库存时发送：
```
🎉 STOCK AVAILABLE!

📱 Product: iPhone 17 Pro Max 256GB 银色 [256GB]
🏪 Store: Apple 珠江新城
✅ Status: Available
⏰ Quote: 今天下午 2:00 可取货
```

### 有限库存通知
当检测到有限库存时发送：
```
⚠️ LIMITED STOCK

📱 Product: iPhone 17 Pro Max 256GB 银色 [256GB]
🏪 Store: Apple 珠江新城
📊 Status: Limited Stock
⏰ Quote: 今天下午 2:00 可取货
```

### 汇总报告
定期发送监控汇总：
```
📊 Apple Stock Monitor Summary

📦 Products Checked: 3
🏪 Stores Checked: 2
🔍 Total Checks: 6
🔄 Availability Changes: 1
⏰ Time: 2025-09-22 16:30:00
```

## 7. 故障排除

### 常见错误
- `telegram bot token or chat ID not configured`: 配置文件中缺少必要字段
- `telegram API error`: Bot Token 或 Chat ID 错误
- `failed to send request`: 网络连接问题

### 调试技巧
1. 使用测试程序验证配置
2. 检查日志中的错误信息
3. 确认 bot 有发送消息的权限
4. 在群组中确保 bot 是管理员（如使用群组）

## 8. 安全建议

1. 不要在代码中硬编码 Bot Token
2. 使用环境变量或配置文件管理凭据
3. 定期轮换 Bot Token
4. 限制 bot 的权限范围
5. 不要将 Bot Token 提交到版本控制系统