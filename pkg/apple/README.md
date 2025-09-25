# Apple Stock Monitor - 苹果库存监控模块

## 概述

基于现有的goscan项目架构，为苹果手机库存监控功能设计的完整数据模型和服务层实现。该模块完全集成到现有的任务调度系统中，支持多产品、多门店的实时库存监控和智能通知。

## 核心特性

### 🍎 产品管理
- 支持完整的苹果产品信息管理
- 产品代码、型号、存储、颜色、运营商等详细属性
- 灵活的产品筛选和查询机制

### 🏪 门店管理
- 苹果零售门店信息管理
- 支持地理位置、时区、联系方式等详细信息
- 门店状态管理和筛选功能

### 📊 库存监控
- 实时库存状态检查（可用/不可用/有限/未知）
- 库存变化历史追踪
- 可用性统计和成功率分析
- 智能重试和错误处理机制

### 🔔 智能通知
- 多渠道通知支持（邮件、短信、Webhook）
- 库存变化事件驱动的通知系统
- 通知频率控制和冷却期管理
- 自定义消息模板和筛选规则

### ⚙️ 灵活配置
- 监控间隔、超时、重试次数等可配置
- 速率限制和代理支持
- 详细的筛选和过滤选项
- 支持多个监控配置并行运行

## 架构设计

### 数据模型 (models.go)

#### 核心实体
- **AppleProduct** - 苹果产品信息
- **StockInfo** - 库存状态信息  
- **StockMonitorConfig** - 监控配置
- **StockMonitorResult** - 监控执行结果
- **StockChangeEvent** - 库存变化事件
- **AppleStoreInfo** - 门店信息

#### 配置结构
- **NotificationConfig** - 通知配置
- **RateLimitConfig** - 速率限制配置
- **ProxyConfig** - 代理配置
- **FilterConfig** - 筛选规则配置

### 接口定义 (types.go)

#### 核心接口
- **StockMonitor** - 库存监控服务接口
- **StockNotifier** - 通知服务接口
- **StockRepository** - 数据仓储接口

#### 任务类型
- **StockMonitorTaskConfig** - 任务配置
- **StockMonitorTaskResult** - 任务结果
- **StockCheckResult** - 单次检查结果

### 服务实现

#### StockMonitorService (service.go)
- 实现核心库存监控逻辑
- 支持并发检查多个产品和门店
- 集成速率限制和错误处理
- 自动更新数据库中的库存信息

#### DefaultStockNotifier (notifier.go)
- 多渠道通知服务实现
- 支持邮件、短信、Webhook通知
- 消息模板和格式化支持
- 通知发送状态追踪

#### GormStockRepository (repository.go)
- 基于GORM的数据持久化实现
- 完整的CRUD操作支持
- 复杂查询和筛选功能
- 事务安全和错误处理

#### StockMonitorExecutor (executor.go)
- 与现有任务系统的集成层
- 任务配置转换和验证
- 执行结果格式化和存储
- 完整的任务生命周期管理

## 集成方式

### 1. 任务类型扩展
在 `pkg/tasks/types.go` 中添加：
```go
const (
    TaskTypeStockMonitor TaskType = "stock_monitor"
)
```

### 2. 配置扩展
在 `TaskConfig` 中增加苹果库存监控相关字段：
```go
type TaskConfig struct {
    // Apple stock monitor configuration
    MonitorConfigID   uint     `json:"monitor_config_id,omitempty"`
    ProductCodes      []string `json:"product_codes,omitempty"`
    StoreNumbers      []string `json:"store_numbers,omitempty"`
    CheckInterval     int      `json:"check_interval,omitempty"`
    MaxRetries        int      `json:"max_retries,omitempty"`
    Timeout           int      `json:"timeout,omitempty"`
    NotifyOnAvailable bool     `json:"notify_on_available,omitempty"`
}
```

### 3. 数据库表结构
需要创建以下数据表：
- `apple_products` - 产品信息
- `apple_stock_infos` - 库存信息
- `apple_stock_monitor_configs` - 监控配置
- `apple_stock_monitor_results` - 监控结果
- `apple_stock_change_events` - 库存变化事件
- `apple_store_infos` - 门店信息

## 使用示例

### 创建产品和监控配置
```go
// 创建产品
product := &AppleProduct{
    ProductCode: "MU6D3LL/A",
    Name:        "iPhone 15 Pro 128GB Natural Titanium",
    Model:       "iPhone 15 Pro",
    Storage:     "128GB",
    Color:       "Natural Titanium",
    Price:       999.00,
    Active:      true,
}

// 创建监控配置
config := &StockMonitorConfig{
    Name:          "iPhone 15 Pro NYC Monitor",
    ProductIDs:    []string{"MU6D3LL/A"},
    StoreNumbers:  []string{"R031", "R020"},
    CheckInterval: 300,
    Active:        true,
}
```

### 执行监控任务
```go
// 创建任务
task := &tasks.Task{
    ID:       "apple-stock-001",
    Type:     tasks.TaskTypeStockMonitor,
    Provider: "apple",
    Config: tasks.TaskConfig{
        ProductCodes:  []string{"MU6D3LL/A"},
        StoreNumbers:  []string{"R031", "R020"},
        CheckInterval: 300,
    },
}

// 执行任务
result, err := executor.Execute(ctx, task)
```

## 技术特点

### 🚀 高性能
- 并发处理多个产品和门店检查
- 智能速率限制避免API限制
- 连接池和超时控制
- 批量数据库操作优化

### 🛡️ 可靠性
- 完善的错误处理和重试机制
- 事务安全的数据操作
- 详细的日志记录和监控
- 优雅的降级和恢复策略

### 🔧 可扩展性
- 模块化设计，易于扩展新功能
- 接口驱动，支持不同实现
- 配置驱动，无需代码更改
- 插件化的通知渠道

### 📱 领域驱动
- 遵循DDD设计原则
- 清晰的业务领域边界
- 丰富的领域模型
- 业务逻辑与技术细节分离

## 注意事项

1. **API限制**: 苹果官方API有速率限制，建议合理设置检查间隔
2. **数据量**: 大规模监控时注意数据库性能和存储空间
3. **通知频率**: 避免过于频繁的通知，建议设置冷却期
4. **错误处理**: 网络异常时的重试策略和降级方案
5. **合规性**: 遵守苹果的服务条款和robots.txt

## 扩展建议

1. **缓存优化**: 添加Redis缓存减少数据库查询
2. **消息队列**: 使用消息队列处理高并发通知
3. **监控面板**: 开发Web界面用于配置管理和结果查看
4. **机器学习**: 基于历史数据预测库存变化趋势
5. **多区域支持**: 扩展支持不同国家和地区的Apple Store

## 结论

该苹果库存监控模块完全符合现有项目架构要求，使用领域驱动设计和切片化架构，提供了完整的库存监控解决方案。模块设计具有高度的可扩展性和可维护性，可以无缝集成到现有的任务调度系统中。通过合理的配置和使用，可以实现高效、可靠的苹果产品库存监控功能。