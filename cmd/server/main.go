package main

import (
	"context"
	"flag"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/scheduler"
	"goscan/pkg/server"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// @title Goscan API
// @version 1.0
// @description 云账单数据同步服务API文档
// @description 支持火山云、阿里云等多云平台的账单数据自动同步到ClickHouse数据库
// @termsOfService http://swagger.io/terms/
// @contact.name API Support
// @contact.email support@goscan.com
// @license.name MIT
// @license.url https://opensource.org/licenses/MIT
// @host localhost:8080
// @BasePath /api/v1
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

	// Initialize structured logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("Starting goscan daemon service", "version", "1.0.0")

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	slog.Info("Configuration loaded successfully", "config_path", getConfigPath(*configPath))

	// Create main context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	app := &DaemonApp{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
	}

	// Override config with command line flags if provided
	if *port != defaultPort {
		slog.Info("Overriding server port from command line", "port", *port)
	}
	if *address != defaultAddress {
		slog.Info("Overriding server address from command line", "address", *address)
	}

	// Start the daemon
	if err := app.Start(*address, *port); err != nil {
		slog.Error("Failed to start daemon", "error", err)
		os.Exit(1)
	}

	// Wait for shutdown signal
	app.WaitForShutdown()

	slog.Info("Goscan daemon service stopped")
}

// DaemonApp represents the main daemon application
type DaemonApp struct {
	cfg       *config.Config
	ctx       context.Context
	cancel    context.CancelFunc
	logger    *slog.Logger
	server    *server.HTTPServer
	scheduler *scheduler.TaskScheduler
	wg        sync.WaitGroup
}

// Start initializes and starts all components of the daemon
func (app *DaemonApp) Start(address string, port int) error {
	slog.Info("Initializing daemon components...")

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
		slog.Info("Starting HTTP server", "address", address, "port", port)
		if err := app.server.Start(); err != nil {
			slog.Error("HTTP server error", "error", err)
			app.cancel() // Trigger graceful shutdown
		}
	}()

	// Start task scheduler
	app.wg.Add(1)
	go func() {
		defer app.wg.Done()
		slog.Info("Starting task scheduler")
		if err := app.scheduler.Start(); err != nil {
			slog.Error("Task scheduler error", "error", err)
			app.cancel() // Trigger graceful shutdown
		}
	}()

	// Wait a moment for services to start
	time.Sleep(100 * time.Millisecond)
	slog.Info("Daemon started successfully")

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
		slog.Info("Received shutdown signal", "signal", sig.String())
	case <-app.ctx.Done():
		slog.Info("Context cancelled, initiating shutdown")
	}

	// Start graceful shutdown
	app.Shutdown()
}

// Shutdown performs graceful shutdown of all components
func (app *DaemonApp) Shutdown() {
	slog.Info("Starting graceful shutdown...")

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
			slog.Info("Shutting down HTTP server...")
			if err := app.server.Shutdown(shutdownCtx); err != nil {
				slog.Error("Error shutting down HTTP server", "error", err)
			} else {
				slog.Info("HTTP server shut down successfully")
			}
		}()
	}

	// Shutdown task scheduler
	if app.scheduler != nil {
		shutdownWg.Add(1)
		go func() {
			defer shutdownWg.Done()
			slog.Info("Shutting down task scheduler...")
			if err := app.scheduler.Shutdown(shutdownCtx); err != nil {
				slog.Error("Error shutting down task scheduler", "error", err)
			} else {
				slog.Info("Task scheduler shut down successfully")
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
		slog.Info("All components shut down successfully")
	case <-shutdownCtx.Done():
		slog.Warn("Shutdown timeout exceeded, some components may not have shut down gracefully")
	}

	// Wait for all goroutines to complete
	app.wg.Wait()
	slog.Info("Graceful shutdown completed")
}

// getConfigPath returns the actual config path that was used
func getConfigPath(configPath string) string {
	if configPath != "" {
		return configPath
	}
	return "default config search path"
}
