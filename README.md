# Goscan - 云账单数据同步工具

## 🎯 项目概述

Goscan 是一个现代化的云账单数据同步后端服务，支持多云服务商的账单数据自动同步到 ClickHouse 数据库。提供完整的 RESTful API 接口和定时任务管理功能。

### ✨ 核心特性

- 🌐 **RESTful API** - 基于 Gin 框架的高性能 HTTP API 服务
- ☁️ **多云支持** - 支持火山引擎、阿里云（AWS、Azure、GCP 即将支持）
- 📊 **状态监控** - 提供任务执行状态和系统健康检查接口
- ⏰ **定时任务** - 基于 Cron 表达式的自动化数据同步
- 🗄️ **数据存储** - ClickHouse 高性能列式数据库
- 💬 **企业微信通知** - 支持 Markdown v2 格式的费用日报推送
- 🔒 **安全可靠** - 密钥安全管理，优雅的错误处理
- 🚀 **高性能** - 智能限流和批量处理机制

## 🚀 快速开始

### 环境要求

- **Go**: 1.24+ 
- **ClickHouse**: 用于存储账单数据
- **操作系统**: Linux / macOS / Windows

### 安装部署

```bash
# 1. 克隆代码
git clone <repository-url>
cd goscan

# 2. 安装依赖
go mod download

# 3. 配置文件
cp config.daemon.yaml config.yaml
vim config.yaml  # 编辑配置

# 4. 构建项目
make build

# 5. 启动服务
./bin/goscan

# 访问 API
# http://localhost:8080
```

### 配置说明

配置文件示例（参考 [`config.daemon.yaml`](config.daemon.yaml)）：

```yaml
# ClickHouse 数据库配置
clickhouse:
  hosts:
    - localhost
  port: 9000
  database: default

# 云服务商配置
providers:
  volcengine:
    access_key: ""
    secret_key: ""
  alicloud:
    access_key_id: ""
    access_key_secret: ""

# 企业微信通知
wechat:
  webhook_url: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"
```

## 🏗️ 架构设计

### 技术栈

- **后端框架**: Gin (高性能 HTTP 框架)
- **数据库**: ClickHouse (数据存储) + SQLite (配置管理)
- **任务调度**: robfig/cron (定时任务)
- **API文档**: Swagger 2.0
- **日志系统**: slog (结构化日志)

### 项目结构

```
goscan/
├── cmd/server/          # 服务入口
├── pkg/                 # 业务包
│   ├── alicloud/       # 阿里云 SDK
│   ├── volcengine/     # 火山引擎 SDK
│   ├── clickhouse/     # ClickHouse 客户端
│   ├── analysis/       # 费用分析
│   ├── wechat/         # 企业微信通知
│   ├── scheduler/      # 任务调度
│   └── handlers/       # API 处理器
├── docs/               # API 文档
└── config.daemon.yaml  # 配置示例
```

## 📚 文档

- [API 使用指南](API_USAGE.md) - 详细的 API 接口说明
- [企业微信通知](WECHAT_NOTIFICATION.md) - 配置和使用企业微信通知
- [更新日志](CHANGELOG.md) - 版本更新记录

## 🛠️ 开发指南

```bash
# 开发模式
make dev

# 运行测试
make test

# 代码格式化
make fmt

# 构建 Swagger 文档
swag init -g cmd/server/main.go
```

## 🔧 故障排除

**端口被占用？**
```bash
./bin/goscan -port 9000
```

**数据库连接失败？**
- 确保 ClickHouse 服务运行正常
- 检查配置文件中的连接参数

**API 无响应？**
```bash
curl http://localhost:8080/api/v1/health
```

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

<div align="center">
Made with ❤️ by Goscan Team
</div>