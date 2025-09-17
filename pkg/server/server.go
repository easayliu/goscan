package server

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/handlers"
	"goscan/pkg/logger"
	"goscan/pkg/middleware"
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"

	_ "goscan/docs" // swagger docs
)

// Server constants
const (
	DefaultReadTimeout  = 30 * time.Second
	DefaultWriteTimeout = 30 * time.Second
	DefaultIdleTimeout  = 120 * time.Second
	DefaultVersion      = "1.0.0"
	ServiceName         = "goscan"
)

// Config holds HTTP server configuration
type Config struct {
	Address string
	Port    int
	Config  *config.Config
}

// HTTPServer represents the HTTP server component
type HTTPServer struct {
	server     *http.Server
	router     *gin.Engine
	config     *Config
	logger     *zap.Logger
	ctx        context.Context
	handlerSvc *handlers.HandlerService
}

// NewHTTPServer creates a new HTTP server instance
func NewHTTPServer(ctx context.Context, config *Config) (*HTTPServer, error) {
	// Initialize zap logger
	isDev := config.Config.App.Environment != "production"
	logPath := config.Config.App.LogFile
	// Use relative path as default to avoid permission issues
	if logPath == "" {
		logPath = "./logs/app.log"
	}

	if err := logger.InitLogger(isDev, logPath, config.Config.App.LogLevel); err != nil {
		return nil, fmt.Errorf("failed to initialize logger: %w", err)
	}

	logger.Info("Initializing HTTP server",
		zap.String("address", config.Address),
		zap.Int("port", config.Port),
		zap.String("environment", config.Config.App.Environment),
	)

	// Set Gin mode based on environment
	if !isDev {
		gin.SetMode(gin.ReleaseMode)
		logger.Debug("Set Gin to release mode")
	} else {
		gin.SetMode(gin.DebugMode)
		logger.Debug("Set Gin to debug mode")
	}

	// Create Gin router without default middleware
	router := gin.New()

	// Add middleware
	router.Use(middleware.RequestID())                 // Add request ID first
	router.Use(middleware.GinZapLogger(logger.Logger)) // Logging middleware with zap
	router.Use(middleware.Recovery())                  // Panic recovery
	router.Use(middleware.ErrorHandler())              // Error handling
	router.Use(cors.Default())                         // CORS

	// Create handler service
	handlerSvc, err := handlers.NewHandlerService(ctx, config.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create handler service: %w", err)
	}

	server := &HTTPServer{
		router:     router,
		config:     config,
		ctx:        ctx,
		logger:     logger.Logger,
		handlerSvc: handlerSvc,
	}

	// Setup routes
	server.setupRoutes()

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", config.Address, config.Port)
	server.server = &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  DefaultReadTimeout,
		WriteTimeout: DefaultWriteTimeout,
		IdleTimeout:  DefaultIdleTimeout,
	}

	logger.Info("HTTP server initialized", zap.String("listen_addr", addr))
	return server, nil
}

// SetScheduler sets the scheduler reference in the handler service
func (s *HTTPServer) SetScheduler(scheduler any) {
	// Set scheduler in handler service using any
	s.handlerSvc.SetScheduler(scheduler)
}

// setupRoutes configures all HTTP routes
func (s *HTTPServer) setupRoutes() {
	// Health check endpoint (no logging)
	s.router.GET("/health", s.handlerSvc.HealthCheck)

	// Swagger documentation
	s.router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// Setup API routes
	s.setupAPIRoutes()

	logger.Debug("HTTP routes configured")
}

// Start starts the HTTP server
func (s *HTTPServer) Start() error {
	logger.Info("Starting HTTP server", zap.String("addr", s.server.Addr))

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server failed: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the HTTP server
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down HTTP server")

	// Sync logger before shutdown
	defer logger.Sync()

	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("HTTP server shutdown failed: %w", err)
	}

	return nil
}

// setupAPIRoutes configures API routes with RESTful design
func (s *HTTPServer) setupAPIRoutes() {
	// System management routes
	system := s.router.Group("/system")
	{
		system.GET("/status", s.handlerSvc.GetStatus)
		system.GET("/config", s.handlerSvc.GetAppConfig)
		system.PUT("/config", s.handlerSvc.UpdateConfig)
	}

	// Task management routes (RESTful)
	tasks := s.router.Group("/tasks")
	{
		tasks.GET("", s.handlerSvc.GetTasks)          // List all tasks
		tasks.POST("", s.handlerSvc.CreateTask)       // Create a new task
		tasks.GET("/:id", s.handlerSvc.GetTask)       // Get a specific task
		tasks.DELETE("/:id", s.handlerSvc.DeleteTask) // Delete a specific task
	}

	// Sync management routes
	sync := s.router.Group("/sync")
	{
		sync.POST("", s.handlerSvc.TriggerSync)           // Trigger new sync
		sync.GET("", s.handlerSvc.GetSyncStatus)          // Get sync status
		sync.GET("/history", s.handlerSvc.GetSyncHistory) // Get sync history
	}

	// Scheduler management routes
	scheduler := s.router.Group("/scheduler")
	{
		scheduler.GET("/status", s.handlerSvc.GetSchedulerStatus)
		scheduler.GET("/metrics", s.handlerSvc.GetSchedulerMetrics)

		// Scheduled jobs sub-routes
		jobs := scheduler.Group("/jobs")
		{
			jobs.GET("", s.handlerSvc.GetScheduledJobs)                 // List all jobs
			jobs.POST("", s.handlerSvc.CreateScheduledJob)              // Create a new job
			jobs.GET("/:id", s.handlerSvc.GetScheduledJob)              // Get a specific job
			jobs.PUT("/:id", s.handlerSvc.UpdateScheduledJob)           // Update a job
			jobs.DELETE("/:id", s.handlerSvc.DeleteScheduledJob)        // Delete a job
			jobs.POST("/:id/trigger", s.handlerSvc.TriggerScheduledJob) // Trigger a job
		}
	}

	// Notification management routes
	notifications := s.router.Group("/notifications")
	{
		// WeChat notification sub-routes
		wechat := notifications.Group("/wechat")
		{
			wechat.POST("", s.handlerSvc.TriggerWeChatNotification)         // Send notification
			wechat.GET("/status", s.handlerSvc.GetWeChatNotificationStatus) // Get status
			wechat.POST("/test", s.handlerSvc.TestWeChatWebhook)            // Test webhook
		}
	}
}
