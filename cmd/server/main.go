package main

import (
	"context"
	"flag"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/logger"
	"goscan/pkg/scheduler"
	"goscan/pkg/server"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// @title Goscan API
// @version 1.0
// @description Cloud billing data synchronization service API documentation
// @description Supports automatic synchronization of billing data from multi-cloud platforms like VolcEngine and Alibaba Cloud to ClickHouse database
// @termsOfService http://swagger.io/terms/
// @contact.name API Support
// @contact.email support@goscan.com
// @license.name MIT
// @license.url https://opensource.org/licenses/MIT
// @host localhost:8080
// @BasePath /
// @schemes http https

const (
	defaultPort    = 8080
	defaultAddress = "0.0.0.0"
)

func main() {
	var (
		configPath = flag.String("config", "", "Configuration file path")
		port       = flag.Int("port", defaultPort, "HTTP server port")
		address    = flag.String("address", defaultAddress, "HTTP server address")
	)
	flag.Parse()

	// Initialize logger
	if err := logger.InitLogger(true, ""); err != nil {
		// Use stderr since logger initialization failed
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("Starting goscan service", zap.String("version", "1.0.0"))

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Error("Failed to load configuration", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("Configuration loaded successfully", zap.String("config_path", getConfigPath(*configPath)))

	// Create main context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	app := &DaemonApp{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}

	// Override config with command line flags if provided
	if *port != defaultPort {
		logger.Info("Overriding server port from command line", zap.Int("port", *port))
	}
	if *address != defaultAddress {
		logger.Info("Overriding server address from command line", zap.String("address", *address))
	}

	// Start the daemon
	if err := app.Start(*address, *port); err != nil {
		logger.Error("Failed to start goscan service", zap.Error(err))
		os.Exit(1)
	}

	// Wait for shutdown signal
	app.WaitForShutdown()

	logger.Info("Goscan service stopped")
}

// DaemonApp represents the main application
type DaemonApp struct {
	cfg       *config.Config
	ctx       context.Context
	cancel    context.CancelFunc
	server    *server.HTTPServer
	scheduler *scheduler.TaskScheduler
	wg        sync.WaitGroup
}

// Start initializes and starts all components of the application
func (app *DaemonApp) Start(address string, port int) error {
	logger.Info("Initializing goscan components...")

	// Initialize HTTP server
	serverConfig := &server.Config{
		Address: address,
		Port:    port,
		Config:  app.cfg,
	}

	httpServer, err := server.NewHTTPServer(app.ctx, serverConfig)
	if err != nil {
		return fmt.Errorf("failed to create HTTP server: %w", err)
	}
	app.server = httpServer

	// Initialize task scheduler
	schedulerConfig := &scheduler.Config{
		Config: app.cfg,
	}

	taskScheduler, err := scheduler.NewTaskScheduler(app.ctx, schedulerConfig)
	if err != nil {
		return fmt.Errorf("failed to create task scheduler: %w", err)
	}
	app.scheduler = taskScheduler

	// Set scheduler reference in handler service
	app.server.SetScheduler(taskScheduler)

	// Start HTTP server
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting HTTP server", zap.String("address", address), zap.Int("port", port))
		if err := app.server.Start(); err != nil {
			logger.Error("HTTP server error", zap.Error(err))
			app.cancel() // Trigger graceful shutdown
		}
	}()

	// Start task scheduler
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		logger.Info("Starting task scheduler")
		if err := app.scheduler.Start(); err != nil {
			logger.Error("Task scheduler error", zap.Error(err))
			app.cancel() // Trigger graceful shutdown
		}
	}()

	// Wait a moment for services to start
	time.Sleep(100 * time.Millisecond)
	logger.Info("Goscan service started successfully")

	return nil
}

// WaitForShutdown waits for shutdown signals and performs graceful shutdown
func (app *DaemonApp) WaitForShutdown() {
	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal or context cancellation
	select {
	case sig := <-sigChan:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
	case <-app.ctx.Done():
		logger.Info("Context cancelled, initiating shutdown")
	}

	// Start graceful shutdown
	app.Shutdown()
}

// Shutdown performs graceful shutdown of all components
func (app *DaemonApp) Shutdown() {
	logger.Info("Starting graceful shutdown...")

	// Cancel context to signal all components to stop
	app.cancel()

	// Create shutdown timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown components in parallel
	var shutdownWg sync.WaitGroup

	// Shutdown HTTP server
	if app.server != nil {
		shutdownWg.Add(1)
		go func() {
			defer shutdownWg.Done()
			logger.Info("Shutting down HTTP server...")
			if err := app.server.Shutdown(shutdownCtx); err != nil {
				logger.Error("Error shutting down HTTP server", zap.Error(err))
			} else {
				logger.Info("HTTP server shut down successfully")
			}
		}()
	}

	// Shutdown task scheduler
	if app.scheduler != nil {
		shutdownWg.Add(1)
		go func() {
			defer shutdownWg.Done()
			logger.Info("Shutting down task scheduler...")
			if err := app.scheduler.Shutdown(shutdownCtx); err != nil {
				logger.Error("Error shutting down task scheduler", zap.Error(err))
			} else {
				logger.Info("Task scheduler shut down successfully")
			}
		}()
	}

	// Wait for all components to shutdown with timeout
	shutdownComplete := make(chan struct{})
	go func() {
		shutdownWg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		logger.Info("All components shut down successfully")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timeout exceeded, some components may not have shut down gracefully")
	}

	// Wait for all goroutines to complete
	app.wg.Wait()
	logger.Info("Graceful shutdown completed")
}

// getConfigPath returns the actual config path that was used
func getConfigPath(configPath string) string {
	if configPath != "" {
		return configPath
	}
	return "default config search path"
}
