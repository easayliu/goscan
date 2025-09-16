package handlers

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/scheduler"
	"goscan/pkg/tasks"
	"log/slog"
)

// HandlerService provides HTTP handlers for the API
// 基础处理器服务结构体，包含所有处理器的通用依赖
type HandlerService struct {
	config    *config.Config
	ctx       context.Context
	taskMgr   tasks.TaskManager
	scheduler *scheduler.TaskScheduler
}

// NewHandlerService creates a new handler service
// 创建新的处理器服务实例
func NewHandlerService(ctx context.Context, cfg *config.Config) (*HandlerService, error) {
	slog.Info("Initializing handler service")

	// Create task manager
	taskMgr, err := tasks.NewTaskManager(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create task manager: %w", err)
	}

	return &HandlerService{
		config:  cfg,
		ctx:     ctx,
		taskMgr: taskMgr,
	}, nil
}

// SetScheduler sets the scheduler reference (called after scheduler is created)
// 设置调度器引用（在调度器创建后调用）
func (h *HandlerService) SetScheduler(schedulerInterface interface{}) {
	if s, ok := schedulerInterface.(*scheduler.TaskScheduler); ok {
		h.scheduler = s
	}
}

// GetConfig returns the handler service configuration
// 获取处理器服务配置
func (h *HandlerService) GetConfig() *config.Config {
	return h.config
}

// GetTaskManager returns the task manager instance
// 获取任务管理器实例
func (h *HandlerService) GetTaskManager() tasks.TaskManager {
	return h.taskMgr
}

// GetScheduler returns the scheduler instance
// 获取调度器实例
func (h *HandlerService) GetScheduler() *scheduler.TaskScheduler {
	return h.scheduler
}

// IsSchedulerAvailable checks if scheduler is available
// 检查调度器是否可用
func (h *HandlerService) IsSchedulerAvailable() bool {
	return h.scheduler != nil
}