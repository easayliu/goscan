# Goscan - 云账单数据同步工具

现代化的多云账单同步后端服务，帮助企业将火山引擎、阿里云等云服务商的费用数据自动落库到 ClickHouse，并提供完整的 API、调度与告警能力。

## 目录

- [项目简介](#项目简介)
- [功能亮点](#功能亮点)
- [快速开始](#快速开始)
- [配置指南](#配置指南)
- [架构与目录说明](#架构与目录说明)
- [开发与运维](#开发与运维)
- [常见问题](#常见问题)
- [文档与资源](#文档与资源)
- [贡献指南](#贡献指南)
- [许可证](#许可证)

## 项目简介

Goscan 旨在提供一个可插拔、可观测的云账单数据同步平台：

- 🧩 支撑多云账单接入，统一落地到 ClickHouse，便于后续分析与可视化。
- 🔄 通过 RESTful API 与定时任务调度，保障账单数据按需、按时同步。
- 📣 支持企业微信 Markdown 通知，实现费用日报与异常提醒。

## 功能亮点

- 🌐 **高性能 API**：基于 Gin 框架，实现标准化 RESTful 接口。
- ☁️ **多云适配**：当前支持火山引擎与阿里云，后续计划扩展 AWS / Azure / GCP。
- 📊 **可观测性**：提供任务状态、健康检查与执行日志，便于运维监控。
- ⏰ **定时调度**：内置 Cron 调度器，可配置分钟级同步策略。
- 🗄️ **高效存储**：账单数据写入 ClickHouse，配置管理使用嵌入式 SQLite。
- 💬 **企微通知**：支持 Markdown v2，推送日报、异常与指标信息。
- 🔐 **安全治理**：密钥隔离、错误处理友好，符合生产环境要求。

## 快速开始

### 环境要求

- Go 1.24 及以上
- ClickHouse 数据库实例
- Linux、macOS 或 Windows 运行环境

### 安装部署

```bash
# 1. 克隆仓库
git clone <repository-url>
cd goscan

# 2. 安装 Go 依赖
go mod download

# 3. 准备配置
cp config.daemon.yaml config.yaml
vim config.yaml  # 修改云账号、数据库等信息

# 4. 构建二进制
make build

# 5. 启动服务
./bin/goscan

# 6. 验证服务
curl http://localhost:8080/api/v1/health
```

若使用 Docker，可参考 `Dockerfile` 进行镜像构建与部署。

## 配置指南

完整配置示例参见 [`config.daemon.yaml`](config.daemon.yaml)。以下为关键项说明：

```yaml
# ClickHouse 连接
clickhouse:
  hosts:
    - localhost
  port: 9000
  username: default
  password: ""
  database: default

# 支持的云服务商
providers:
  volcengine:
    access_key: "YOUR_VOLC_KEY"
    secret_key: "YOUR_VOLC_SECRET"
  alicloud:
    access_key_id: "YOUR_ALI_KEY"
    access_key_secret: "YOUR_ALI_SECRET"

# 企业微信机器人
wechat:
  webhook_url: "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY"

# 调度配置示例
scheduler:
  enabled: true
  cron: "0 0 * * *"  # 每天 00:00 执行
```

- 提前在 ClickHouse 中创建数据库与表结构。
- 对于生产环境，建议通过环境变量或密钥管理系统注入敏感配置。
- Cron 表达式遵循 robfig/cron v3 语法，可支持到秒级调度。

## 架构与目录说明

### 技术栈

- Gin：HTTP API 与路由
- ClickHouse Go Driver：账单数据存储
- SQLite：轻量配置存储
- robfig/cron：定时任务调度
- Swagger 2.0：接口文档生成
- slog：结构化日志

### 目录结构

```
goscan/
├── cmd/server/          # 服务入口 main 程序
├── internal/            # 内部共享逻辑（不对外暴露）
├── pkg/
│   ├── alicloud/        # 阿里云账单 SDK 封装
│   ├── volcengine/      # 火山引擎账单 SDK 封装
│   ├── clickhouse/      # ClickHouse 客户端与表结构
│   ├── analysis/        # 费用分析与指标计算
│   ├── scheduler/       # 调度与任务管理
│   ├── handlers/        # API 分层处理逻辑
│   └── wechat/          # 企业微信通知模块
├── docs/                # Swagger 等文档输出
├── examples/            # 配置与调用示例
├── config.daemon.yaml   # 默认配置模板
└── Makefile             # 常用开发脚本
```

## 开发与运维

```bash
# 本地开发热启动
make dev

# 代码格式化
make fmt

# 运行单元测试
make test

# 生成 Swagger 文档（需安装 swag）
swag init -g cmd/server/main.go
```

- 日志默认输出到标准输出，可通过配置定向到文件。
- 生产环境部署建议结合 systemd、Supervisor 或容器编排系统。
- 监控建议对接 Prometheus/Alertmanager 或企业微信的自定义告警。

## 常见问题

- **端口被占用**：使用 `./bin/goscan -port <新端口>` 覆盖默认端口。
- **ClickHouse 无法连接**：确认数据库服务可达，检查 host、port、用户和防火墙策略。
- **调度未触发**：确认 `scheduler.enabled` 为 `true` 且 Cron 表达式合法，可通过 API 查看任务状态。
- **企微通知失败**：检查 webhook 是否仍然有效，必要时重置机器人 Key。

## 文档与资源

- [`docs/`](docs/)：Swagger 生成的接口文档
- [`API_USAGE.md`](API_USAGE.md)：API 调用示例
- [`WECHAT_NOTIFICATION.md`](WECHAT_NOTIFICATION.md)：企业微信通知配置说明
- [`CHANGELOG.md`](CHANGELOG.md)：版本记录与特性变更

## 贡献指南

欢迎提交 Issue、Pull Request 或功能建议。建议在提交前：

1. 先讨论需求或问题，确认实现路径。
2. 提交代码前运行 `make test` 与基础静态检查。
3. 为新增能力补充文档、示例或测试用例。

## 许可证

本项目采用 MIT 协议，详见 [LICENSE](LICENSE)。
