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
//
// taskMgr is the one the scheduler runs its jobs on as well: /tasks, its event
// streams and DELETE /tasks/{id} only see tasks of the manager they are given,
// and the one-sync-per-provider guard only holds within a manager. nil creates
// a private one, for callers that have no scheduler.
func NewHandlerService(ctx context.Context, cfg *config.Config, taskMgr tasks.TaskManager) (*HandlerService, error) {
	logger.Info("Initializing handler service")

	if taskMgr == nil {
		mgr, err := tasks.NewTaskManager(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create task manager: %w", err)
		}
		taskMgr = mgr
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
