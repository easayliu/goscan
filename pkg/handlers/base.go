package handlers

import (
	"context"
	"fmt"

	"goscan/pkg/config"
	"goscan/pkg/logger"
	"goscan/pkg/scheduler"
	"goscan/pkg/tasks"
)

// HandlerService provides HTTP handlers for the API
// Base handler service structure containing common dependencies for all handlers
type HandlerService struct {
	config    *config.Config
	ctx       context.Context
	taskMgr   tasks.TaskManager
	scheduler *scheduler.TaskScheduler
}

// NewHandlerService creates a new handler service
// Creates new handler service instance
func NewHandlerService(ctx context.Context, cfg *config.Config) (*HandlerService, error) {
	logger.Info("Initializing handler service")

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
// Sets scheduler reference (called after scheduler is created)
func (h *HandlerService) SetScheduler(schedulerInterface interface{}) {
	if s, ok := schedulerInterface.(*scheduler.TaskScheduler); ok {
		h.scheduler = s
	}
}

// GetConfig returns the handler service configuration
// Gets handler service configuration
func (h *HandlerService) GetConfig() *config.Config {
	return h.config
}

// GetTaskManager returns the task manager instance
// Gets task manager instance
func (h *HandlerService) GetTaskManager() tasks.TaskManager {
	return h.taskMgr
}

// GetScheduler returns the scheduler instance
// Gets scheduler instance
func (h *HandlerService) GetScheduler() *scheduler.TaskScheduler {
	return h.scheduler
}

// IsSchedulerAvailable checks if scheduler is available
// Checks if scheduler is available
func (h *HandlerService) IsSchedulerAvailable() bool {
	return h.scheduler != nil
}
