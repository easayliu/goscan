package apple

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"goscan/pkg/notifier"
)

// BrowserMonitor monitors Apple Store stock using browser automation
type BrowserMonitor struct {
	extractor       *PageExtractor
	products        []Product
	stores          []Store
	notifier        *notifier.TelegramNotifier
	checkInterval   time.Duration
	lastStockStatus map[string]bool
	mu              sync.RWMutex
	isRunning       bool
	stopChan        chan struct{}
	config          *BrowserMonitorConfig
}

// BrowserMonitorConfig configuration for browser monitor
type BrowserMonitorConfig struct {
	Products      []Product
	Stores        []Store
	CheckInterval time.Duration // default 5 seconds
	TelegramToken string
	TelegramChat  string
	EnableNotify  bool
	Headless      bool // Run browser in headless mode
}

// NewBrowserMonitor creates a new browser-based monitor
func NewBrowserMonitor(config *BrowserMonitorConfig) (*BrowserMonitor, error) {
	if config.CheckInterval == 0 {
		config.CheckInterval = 5 * time.Second
	}
	
	extractor, err := NewPageExtractorWithOptions(config.Headless)
	if err != nil {
		return nil, fmt.Errorf("failed to create page extractor: %w", err)
	}
	
	monitor := &BrowserMonitor{
		extractor:       extractor,
		products:        config.Products,
		stores:          config.Stores,
		checkInterval:   config.CheckInterval,
		lastStockStatus: make(map[string]bool),
		stopChan:        make(chan struct{}),
		config:          config,
	}
	
	// Setup Telegram notifier if enabled
	if config.EnableNotify && config.TelegramToken != "" && config.TelegramChat != "" {
		telegramConfig := &notifier.TelegramConfig{
			Enabled:  true,
			BotToken: config.TelegramToken,
			ChatID:   config.TelegramChat,
			Timeout:  30,
		}
		tn := notifier.NewTelegramNotifier(telegramConfig)
		if tn != nil {
			monitor.notifier = tn
			log.Println("Telegram notifier initialized successfully")
		} else {
			log.Printf("Warning: Failed to setup Telegram notifier")
		}
	}
	
	return monitor, nil
}

// Start starts the monitoring loop
func (m *BrowserMonitor) Start() error {
	m.mu.Lock()
	if m.isRunning {
		m.mu.Unlock()
		return fmt.Errorf("monitor is already running")
	}
	m.isRunning = true
	m.mu.Unlock()
	
	log.Println("Starting browser monitor...")
	log.Printf("Monitoring %d products across %d stores", len(m.products), len(m.stores))
	log.Printf("Check interval: %v", m.checkInterval)
	
	// Initial check with longer delays
	log.Println("Performing initial stock check...")
	m.performInitialCheck()
	
	// Start monitoring loop
	go m.monitorLoop()
	
	return nil
}

// Stop stops the monitoring loop
func (m *BrowserMonitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.isRunning {
		return
	}
	
	log.Println("Stopping browser monitor...")
	close(m.stopChan)
	m.isRunning = false
	
	// Close browser
	if m.extractor != nil {
		m.extractor.Close()
	}
	
	log.Println("Browser monitor stopped")
}

// performInitialCheck performs initial stock check with longer delays
func (m *BrowserMonitor) performInitialCheck() {
	for _, product := range m.products {
		for _, store := range m.stores {
			select {
			case <-m.stopChan:
				return
			default:
				// Check stock
				m.checkStock(product, store)
				
				// Longer delay for initial check (5-10 seconds)
				delay := time.Duration(5+randIntn(5)) * time.Second
				log.Printf("Initial check delay: %v", delay)
				time.Sleep(delay)
			}
		}
	}
}

// monitorLoop main monitoring loop
func (m *BrowserMonitor) monitorLoop() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()
	
	retryCount := 0
	maxRetries := 3
	
	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			// Check all products and stores
			hasError := false
			
			for _, product := range m.products {
				for _, store := range m.stores {
					select {
					case <-m.stopChan:
						return
					default:
						// Check stock with error handling
						err := m.checkStock(product, store)
						
						if err != nil {
							log.Printf("Error checking stock: %v", err)
							
							// Check for 541 error
							// Error is returned as string, check if it contains "541"
							if err != nil && (strings.Contains(err.Error(), "541 error") || strings.Contains(err.Error(), "Error 541")) {
								log.Println("Detected 541 error, refreshing cookie...")
								
								if refreshErr := m.extractor.RefreshCookie(); refreshErr != nil {
									log.Printf("Failed to refresh cookie: %v", refreshErr)
									retryCount++
									
									if retryCount >= maxRetries {
										log.Println("Max retries reached, recreating extractor...")
										m.recreateExtractor()
										retryCount = 0
									}
								} else {
									log.Println("Cookie refreshed successfully")
									retryCount = 0
								}
								
								hasError = true
								break // Break inner loop to retry
							}
						}
						
						// Random delay between checks (3-8 seconds)
						delay := time.Duration(3+randIntn(5)) * time.Second
						time.Sleep(delay)
					}
				}
				
				if hasError {
					// Additional delay after error
					time.Sleep(10 * time.Second)
					hasError = false
				}
			}
		}
	}
}

// checkStock checks stock for a specific product and store
func (m *BrowserMonitor) checkStock(product Product, store Store) error {
	startTime := time.Now()
	key := fmt.Sprintf("%s_%s", product.Code, store.Code)
	
	log.Printf("Checking stock - Product: %s, Store: %s", product.Name, store.Name)
	
	// Extract stock info using fulfillment API via browser with product info
	stockInfo, err := m.extractor.ExtractStockInfoWithProduct(product, store.Code)
	
	if err != nil {
		log.Printf("Failed to extract stock info: %v", err)
		return err
	}
	
	// Check if status changed
	m.mu.RLock()
	lastStatus, exists := m.lastStockStatus[key]
	m.mu.RUnlock()
	
	statusChanged := !exists || lastStatus != stockInfo.IsAvailable
	
	if statusChanged {
		m.mu.Lock()
		m.lastStockStatus[key] = stockInfo.IsAvailable
		m.mu.Unlock()
		
		// Send notification if available
		if stockInfo.IsAvailable {
			m.sendNotification(product, store, stockInfo)
		}
		
		// Log status change
		statusText := "UNAVAILABLE"
		if stockInfo.IsAvailable {
			statusText = "AVAILABLE"
		}
		
		log.Printf("📱 Stock Status Changed: %s at %s is now %s",
			product.Name, store.Name, statusText)
	}
	
	// Log check result
	responseTime := time.Since(startTime).Milliseconds()
	log.Printf("Check completed - Status: %s, Quote: %s, Time: %dms",
		stockInfo.Status, stockInfo.PickupQuote, responseTime)
	
	return nil
}

// recreateExtractor recreates the page extractor (browser restart)
func (m *BrowserMonitor) recreateExtractor() {
	log.Println("Recreating page extractor...")
	
	// Close old extractor
	if m.extractor != nil {
		m.extractor.Close()
	}
	
	// Create new extractor with same headless setting
	extractor, err := NewPageExtractorWithOptions(m.config.Headless)
	if err != nil {
		log.Printf("Failed to recreate extractor: %v", err)
		// Wait before retry
		time.Sleep(30 * time.Second)
		return
	}
	
	m.extractor = extractor
	log.Println("Page extractor recreated successfully")
}

// sendNotification sends stock alert notification
func (m *BrowserMonitor) sendNotification(product Product, store Store, stockInfo *BrowserStockInfo) {
	if m.notifier == nil {
		return
	}
	
	ctx := context.Background()
	if err := m.notifier.SendStockAlert(ctx, product.Name, product.Storage, store.Name, stockInfo.Status, stockInfo.PickupQuote); err != nil {
		log.Printf("Failed to send notification: %v", err)
	} else {
		log.Println("Stock alert notification sent successfully")
	}
}

// CheckSingleProduct checks stock for a single product across all stores
func (m *BrowserMonitor) CheckSingleProduct(productCode string) ([]*BrowserStockInfo, error) {
	log.Printf("Checking single product: %s", productCode)
	
	stockInfos, err := m.extractor.ExtractAllStoresStock(productCode)
	if err != nil {
		return nil, fmt.Errorf("failed to extract all stores stock: %w", err)
	}
	
	// Log results
	availableCount := 0
	for _, info := range stockInfos {
		if info.IsAvailable {
			availableCount++
			log.Printf("✅ Store %s: %s", info.StoreCode, info.PickupQuote)
		}
	}
	
	log.Printf("Found %d available stores out of %d total", availableCount, len(stockInfos))
	
	return stockInfos, nil
}

// GetStatus returns current monitor status
func (m *BrowserMonitor) GetStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	status := map[string]interface{}{
		"isRunning":       m.isRunning,
		"productsCount":   len(m.products),
		"storesCount":     len(m.stores),
		"checkInterval":   m.checkInterval.String(),
		"lastCheckCount":  len(m.lastStockStatus),
		"notifierEnabled": m.notifier != nil,
	}
	
	// Count available items
	availableCount := 0
	for _, isAvailable := range m.lastStockStatus {
		if isAvailable {
			availableCount++
		}
	}
	status["availableCount"] = availableCount
	
	return status
}

// randIntn generates a random integer between 0 and n-1
func randIntn(n int) int {
	return rand.Intn(n)
}

