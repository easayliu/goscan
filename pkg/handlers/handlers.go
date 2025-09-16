package handlers

import (
	_ "goscan/pkg/models" // imported for swagger documentation
)

// 这个文件现在作为handlers包的统一入口点
// 所有的处理器功能已经按功能域拆分到以下文件：
// - base.go: 基础处理器结构和通用方法
// - errors.go: 错误定义和处理
// - middleware.go: 通用中间件和辅助函数
// - health_handlers.go: 健康检查和状态相关API
// - sync_handlers.go: 同步相关的API处理器
// - analysis_handlers.go: 分析相关的API处理器（包括微信通知）
// - schedule_handlers.go: 调度相关的API处理器

// 重要提示：
// 1. HandlerService 结构体现在定义在 base.go 中
// 2. NewHandlerService 构造函数在 base.go 中
// 3. 所有API处理器方法都作为 HandlerService 的方法分布在各个文件中
// 4. 统一的错误处理在 errors.go 中
// 5. 通用辅助函数在 middleware.go 中

// 通过Go的包机制，所有这些方法仍然可以通过 handlers.HandlerService 访问
// 保持了向后兼容性，现有的路由注册代码无需修改

