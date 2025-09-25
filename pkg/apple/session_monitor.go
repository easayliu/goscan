package apple

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// SessionMonitor manages persistent browser sessions for monitoring
type SessionMonitor struct {
	extractor        *PageExtractor
	mu               sync.RWMutex
	checkInterval    time.Duration
	isRunning        bool
	stopChan         chan struct{}
}

// NewSessionMonitor creates a new session-aware monitor
func NewSessionMonitor(headless bool, checkInterval time.Duration) (*SessionMonitor, error) {
	extractor, err := NewPageExtractorWithOptions(headless)
	if err != nil {
		return nil, fmt.Errorf("failed to create extractor: %w", err)
	}
	
	if checkInterval == 0 {
		checkInterval = 5 * time.Second
	}
	
	return &SessionMonitor{
		extractor:          extractor,
		checkInterval:      checkInterval,
		stopChan:          make(chan struct{}),
	}, nil
}

// CheckStock checks stock for a product at a store, using shared session
func (sm *SessionMonitor) CheckStock(product Product, storeCode string) (*BrowserStockInfo, error) {
	// Establish session if not already established (will be skipped if already done)
	if err := sm.extractor.EstablishSession(); err != nil {
		return nil, fmt.Errorf("failed to establish session: %w", err)
	}
	
	// Always use direct API call with established session
	log.Printf("Checking stock for %s at store %s", product.Code, storeCode)
	stockInfo, err := sm.extractor.ExtractStockInfoDirect(product.Code, storeCode)
	
	// If session expired, reset and try again
	if err != nil && stockInfo != nil && stockInfo.Status == "session_expired" {
		log.Printf("Session expired, re-establishing...")
		sm.extractor.sessionEstablished = false
		
		// Re-establish session
		if err := sm.extractor.EstablishSession(); err != nil {
			return nil, fmt.Errorf("failed to re-establish session: %w", err)
		}
		
		// Retry the check
		stockInfo, err = sm.extractor.ExtractStockInfoDirect(product.Code, storeCode)
	}
	
	return stockInfo, err
}

// MonitorProducts continuously monitors products in the same session
func (sm *SessionMonitor) MonitorProducts(products []Product, stores []Store, notifyFunc func(Product, Store, *BrowserStockInfo)) {
	sm.mu.Lock()
	if sm.isRunning {
		sm.mu.Unlock()
		log.Println("Monitor is already running")
		return
	}
	sm.isRunning = true
	sm.mu.Unlock()
	
	log.Printf("Starting session monitor with %d products and %d stores", len(products), len(stores))
	log.Printf("Check interval: %v", sm.checkInterval)
	
	ticker := time.NewTicker(sm.checkInterval)
	defer ticker.Stop()
	
	// Initial check for all products
	log.Println("Performing initial check...")
	for _, product := range products {
		for _, store := range stores {
			select {
			case <-sm.stopChan:
				return
			default:
				stockInfo, err := sm.CheckStock(product, store.Code)
				if err != nil {
					log.Printf("Error checking %s at %s: %v", product.Name, store.Name, err)
				} else {
					log.Printf("%s at %s: %s - %s", 
						product.Name, store.Name, stockInfo.Status, stockInfo.PickupQuote)
					if notifyFunc != nil && stockInfo.IsAvailable {
						notifyFunc(product, store, stockInfo)
					}
				}
				
				// Small delay between initial checks
				time.Sleep(2 * time.Second)
			}
		}
	}
	
	log.Println("Initial check complete. Starting continuous monitoring...")
	
	// Continuous monitoring
	for {
		select {
		case <-sm.stopChan:
			log.Println("Stopping session monitor")
			return
		case <-ticker.C:
			// Check all products using established sessions (sequentially to avoid browser conflicts)
			for _, product := range products {
				for _, store := range stores {
					select {
					case <-sm.stopChan:
						return
					default:
						// Check stock sequentially to avoid concurrent browser access
						stockInfo, err := sm.CheckStock(product, store.Code)
						if err != nil {
							log.Printf("Error checking %s at %s: %v", product.Name, store.Name, err)
						} else {
							if stockInfo.IsAvailable {
								log.Printf("🎯 AVAILABLE: %s at %s - %s", 
									product.Name, store.Name, stockInfo.PickupQuote)
								if notifyFunc != nil {
									notifyFunc(product, store, stockInfo)
								}
							}
						}
						
						// Small delay between checks to avoid overwhelming the browser
						time.Sleep(500 * time.Millisecond)
					}
				}
			}
		}
	}
}

// Stop stops the monitor
func (sm *SessionMonitor) Stop() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	if !sm.isRunning {
		return
	}
	
	log.Println("Stopping session monitor...")
	close(sm.stopChan)
	sm.isRunning = false
	
	if sm.extractor != nil {
		sm.extractor.Close()
	}
	
	log.Println("Session monitor stopped")
}

// GetSessionStatus returns the current session status
func (sm *SessionMonitor) GetSessionStatus() bool {
	return sm.extractor.sessionEstablished
}