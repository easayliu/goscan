package main

import (
	"context"
	"fmt"
	"time"

	"goscan/pkg/apple"
	"goscan/pkg/logger"
)

func main() {
	// Initialize logger
	if err := logger.InitLogger(true, "", "info"); err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}

	logger.Info("🔍 Testing Pickup API")

	// Create monitor
	monitor := apple.NewSimpleMonitor()

	// Test parameters
	productCode := "MG034CH/A"
	productName := "iPhone 17 Pro Max 256GB 银色"
	storeCode := "R639"
	storeName := "Apple 珠江新城"

	// Create context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test pickup API directly (no cookie needed)
	logger.Info("🏪 Testing pickup-message-recommendations API...")
	result, err := monitor.CheckPickupAvailability(ctx, productCode, storeCode, productName, storeName)
	
	if err != nil {
		logger.Error("❌ Pickup API test failed: " + err.Error())
		return
	}

	// Print result
	logger.Info("📊 Pickup API Result:")
	logger.Info(fmt.Sprintf("  Product: %s", productCode))
	logger.Info(fmt.Sprintf("  Store: %s", storeCode))
	logger.Info(fmt.Sprintf("  Available: %v", result.IsAvailable))
	logger.Info(fmt.Sprintf("  Pickup Available: %v", result.PickupAvailable))
	logger.Info(fmt.Sprintf("  Status: %s", result.Status))
	logger.Info(fmt.Sprintf("  ETA: %s", result.PickupETA))

	logger.Info("✅ Test completed")
}