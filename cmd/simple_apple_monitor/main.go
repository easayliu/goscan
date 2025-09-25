package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goscan/pkg/apple"
	"goscan/pkg/logger"
	"goscan/pkg/notifier"

	"go.uber.org/zap"
)

func main() {
	// Load configuration first to get log level
	var configPath string
	if _, err := os.Stat("apple_monitor.json"); err == nil {
		configPath = "apple_monitor.json"
	} else if _, err := os.Stat("config/apple_monitor.json"); err == nil {
		configPath = "config/apple_monitor.json"
	} else {
		// Use default logger for error message
		fmt.Println("Error: Configuration file not found in current directory or config/ directory")
		os.Exit(1)
	}

	// Load config to get log level
	configData, err := loadConfigData(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Get log level from config, default to "info" if not specified
	logLevel := configData.Monitoring.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}

	// Initialize logger with config log level
	if err := logger.InitLogger(true, "", logLevel); err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	logger.Info("🍎 Simple Apple Stock Monitor")
	logger.Info("Press Ctrl+C to stop")

	// Create monitor
	monitor := apple.NewSimpleMonitor()

	// Convert config data to monitor config
	config, err := convertConfig(configData)
	if err != nil {
		logger.Error("Failed to convert config", zap.String("path", configPath), zap.Error(err))
		os.Exit(1)
	}

	logger.Info("📋 Monitor Configuration",
		zap.Int("products", len(config.Products)),
		zap.Int("stores", len(config.Stores)),
		zap.Duration("interval", config.Interval))

	// Print products being monitored
	logger.Info("📦 Products to monitor:")
	for _, p := range config.Products {
		logger.Info(fmt.Sprintf("  • %s [%s] (%s)", p.Name, p.Storage, p.Code))
	}

	// Print stores being monitored
	logger.Info("🏪 Stores to monitor:")
	for _, s := range config.Stores {
		logger.Info("  • " + s.Name + " (" + s.Code + ")")
	}

	// Setup context and signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("🛑 Shutdown signal received")
		cancel()
	}()

	// Start monitoring
	if err := monitor.StartMonitoring(ctx, config); err != nil {
		logger.Error("Monitor failed", zap.Error(err))
		monitor.Stop() // Clean up browser context
		os.Exit(1)
	}

	// Clean up browser context
	monitor.Stop()

	// Show final status before exit
	status := monitor.GetCurrentStatus()
	if len(status) > 0 {
		logger.Info("📊 Final Status Summary:")
		statusJSON, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(statusJSON))
	}

	logger.Info("👋 Monitor stopped")
}

// Config file structure
type ConfigFile struct {
	Monitoring struct {
		DefaultInterval int    `json:"default_interval"`
		LogLevel       string `json:"log_level"`
	} `json:"monitoring"`
	AppleAuth struct {
		Cookie       string `json:"cookie"`
		AutoCookie   *struct {
			Enabled bool `json:"enabled"`
		} `json:"auto_cookie,omitempty"`
		UsePickupAPI bool `json:"use_pickup_api,omitempty"`
	} `json:"apple_auth,omitempty"`
	Telegram *notifier.TelegramConfig `json:"telegram,omitempty"`
	Products []struct {
		ProductCode string `json:"product_code"`
		ProductName string `json:"product_name"`
		Storage     string `json:"storage"`
		Enabled     bool   `json:"enabled"`
	} `json:"products"`
	Stores []struct {
		StoreCode string `json:"store_code"`
		StoreName string `json:"store_name"`
		Enabled   bool   `json:"enabled"`
	} `json:"stores"`
}

// loadConfigData loads raw configuration data from JSON file
func loadConfigData(filename string) (*ConfigFile, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var configFile ConfigFile
	if err := json.Unmarshal(data, &configFile); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &configFile, nil
}

// convertConfig converts ConfigFile to apple.MonitorConfig
func convertConfig(configFile *ConfigFile) (*apple.MonitorConfig, error) {
	// Convert to monitor config
	config := &apple.MonitorConfig{
		Products:     make([]apple.ProductConfig, 0),
		Stores:       make([]apple.StoreConfig, 0),
		Interval:     time.Duration(configFile.Monitoring.DefaultInterval) * time.Second,
		Telegram:     configFile.Telegram,
		Cookie:       configFile.AppleAuth.Cookie,
		UsePickupAPI: configFile.AppleAuth.UsePickupAPI,
	}

	if configFile.AppleAuth.AutoCookie != nil {
		config.AutoCookie = &apple.AutoCookieConfig{
			Enabled: configFile.AppleAuth.AutoCookie.Enabled,
		}
	}

	// Add enabled products
	for _, p := range configFile.Products {
		if p.Enabled {
			config.Products = append(config.Products, apple.ProductConfig{
				Code:    p.ProductCode,
				Name:    p.ProductName,
				Storage: p.Storage,
			})
		}
	}

	// Add enabled stores
	for _, s := range configFile.Stores {
		if s.Enabled {
			config.Stores = append(config.Stores, apple.StoreConfig{
				Code: s.StoreCode,
				Name: s.StoreName,
			})
		}
	}

	return config, nil
}
