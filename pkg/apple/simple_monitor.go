package apple

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/chromedp"
	"goscan/pkg/logger"
	"goscan/pkg/notifier"

	"go.uber.org/zap"
)

// SimpleMonitor implements a simple stock monitoring without database
type SimpleMonitor struct {
	httpClient              *http.Client
	userAgent               string
	baseURL                 string
	cookie                  string
	capturedHeaders         map[string]string // Headers captured from browser requests
	browserCtx              context.Context   // Persistent browser context
	browserCancel           context.CancelFunc // Cancel function for browser
	allocCancel             context.CancelFunc // Cancel function for allocator
	mu                      sync.RWMutex
	lastStatus              map[string]*ProductStatus // productCode_storeCode -> status
	telegramNotifier        *notifier.TelegramNotifier
	last541ErrorTime        time.Time      // Track last 541 error notification time
	cookieJar               http.CookieJar // Cookie jar for auto cookie management
	cookieRefreshInProgress bool
	baseCtx                 context.Context
	autoCookieEnabled       bool           // Track if auto cookie is enabled
	cookieExpireTime        time.Time      // Cookie expiration time parsed from shld_bt_ck
	usePickupAPI            bool           // Use pickup API instead of fulfillment API
}

var errAppleAuth = errors.New("apple api returned 541")

// PlatformHeaders contains platform-specific headers
type PlatformHeaders struct {
	UserAgent     string
	SecChUa       string
	SecChUaMobile string
	SecChUaPlatform string
}

// getPlatformHeaders returns appropriate headers based on runtime platform
func getPlatformHeaders() PlatformHeaders {
	// Check environment variable to force Mac user agent
	if os.Getenv("FORCE_MAC_USERAGENT") == "true" {
		// Always return Mac headers when environment variable is set
		return PlatformHeaders{
			UserAgent:       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
			SecChUa:         `"Chromium";v="131", "Not_A Brand";v="24"`,
			SecChUaMobile:   "?0",
			SecChUaPlatform: `"macOS"`,
		}
	}
	
	// Default behavior based on runtime OS
	switch runtime.GOOS {
	case "linux":
		// For Linux, use real Linux user agent - Chrome will capture actual headers
		return PlatformHeaders{
			UserAgent:       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
			SecChUa:         `"Chromium";v="131", "Not_A Brand";v="24"`,
			SecChUaMobile:   "?0",
			SecChUaPlatform: `"Linux"`,
		}
	case "windows":
		return PlatformHeaders{
			UserAgent:       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
			SecChUa:         `"Chromium";v="131", "Not_A Brand";v="24"`,
			SecChUaMobile:   "?0",
			SecChUaPlatform: `"Windows"`,
		}
	default: // darwin and others default to macOS
		return PlatformHeaders{
			UserAgent:       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
			SecChUa:         `"Chromium";v="131", "Not_A Brand";v="24"`,
			SecChUaMobile:   "?0",
			SecChUaPlatform: `"macOS"`,
		}
	}
}

// ProductStatus represents the current status of a product at a store
type ProductStatus struct {
	ProductCode  string    `json:"product_code"`
	ProductName  string    `json:"product_name"`
	StoreCode    string    `json:"store_code"`
	StoreName    string    `json:"store_name"`
	IsAvailable  bool      `json:"is_available"`
	Status       string    `json:"status"`
	CheckTime    time.Time `json:"check_time"`
	ResponseTime int64     `json:"response_time_ms"`
	PickupQuote  string    `json:"pickup_quote,omitempty"`
}

// MonitorConfig represents simple monitoring configuration
type MonitorConfig struct {
	Products   []ProductConfig          `json:"products"`
	Stores     []StoreConfig            `json:"stores"`
	Interval   time.Duration            `json:"interval"`
	Telegram   *notifier.TelegramConfig `json:"telegram,omitempty"`
	Cookie     string                   `json:"cookie,omitempty"`
	AutoCookie *AutoCookieConfig        `json:"auto_cookie,omitempty"`
	UsePickupAPI bool                   `json:"use_pickup_api,omitempty"` // Use pickup API instead of fulfillment API
}

// AutoCookieConfig controls headless cookie retrieval strategy
type AutoCookieConfig struct {
	Enabled bool `json:"enabled"`
}

type ProductConfig struct {
	Code    string `json:"code"`
	Name    string `json:"name"`
	Storage string `json:"storage"`
}

type StoreConfig struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

// NewSimpleMonitor creates a new simple monitor
func NewSimpleMonitor() *SimpleMonitor {
	// Create cookie jar for automatic cookie management
	jar, _ := cookiejar.New(nil)
	
	// Get platform-specific headers
	platformHeaders := getPlatformHeaders()

	return &SimpleMonitor{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Jar:     jar,
		},
		userAgent:       platformHeaders.UserAgent,
		baseURL:         "https://www.apple.com.cn",
		capturedHeaders: make(map[string]string),
		lastStatus:      make(map[string]*ProductStatus),
		cookieJar:       jar,
	}
}

func (m *SimpleMonitor) clearCookieCache() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cookie = ""
	newJar, err := cookiejar.New(nil)
	if err != nil {
		logger.Warn("Failed to reset cookie jar", zap.Error(err))
		return
	}
	m.cookieJar = newJar
	m.httpClient.Jar = newJar
	logger.Debug("Cleared cached cookies before monitoring")
}

// StartMonitoring starts the monitoring loop
func (m *SimpleMonitor) StartMonitoring(ctx context.Context, config *MonitorConfig) error {
	logger.Info("🍎 Starting simple Apple stock monitoring")
	// Remember base context for background refresh routines
	m.mu.Lock()
	m.baseCtx = ctx
	m.usePickupAPI = config.UsePickupAPI
	m.mu.Unlock()

	// Don't clear cookie cache at start - we'll get fresh cookies anyway

	// If using pickup API, skip cookie initialization
	if config.UsePickupAPI {
		logger.Info("📦 Using pickup API for monitoring (no authentication required)")
		m.autoCookieEnabled = false
		m.cookie = ""
	} else if config.AutoCookie != nil && config.AutoCookie.Enabled {
		// Auto cookie enabled - ignore config cookie and get fresh one from browser
		m.autoCookieEnabled = true
		logger.Debug("Auto cookie enabled - will get fresh cookies from browser")
		
		// Try to get and validate cookie with retries
		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			if i > 0 {
				logger.Debug("Retrying cookie acquisition", zap.Int("attempt", i+1))
				time.Sleep(5 * time.Second)
			}
			
			if err := m.RefreshCookieWithBrowser(ctx); err != nil {
				logger.Warn("Failed to get/validate browser cookies", 
					zap.Error(err),
					zap.Int("attempt", i+1))
				
				if strings.Contains(err.Error(), "cookie validation failed") {
					// Cookie was obtained but failed validation, retry
					continue
				}
				
				if i == maxRetries-1 {
					logger.Error("Failed to get browser cookies after all retries", zap.Error(err))
					return fmt.Errorf("auto_cookie enabled but failed to get valid cookies: %w", err)
				}
				continue
			}
			
			// Success - check if we have a cookie
			currentCookie := m.CurrentCookie()
			if currentCookie == "" {
				if i == maxRetries-1 {
					return fmt.Errorf("auto_cookie enabled but no cookies obtained after %d attempts", maxRetries)
				}
				continue
			}
			
			logger.Debug("Browser cookie obtained and validated successfully", 
				zap.Bool("has_dssid2", strings.Contains(currentCookie, "dssid2")),
				zap.Bool("has_shld_bt_ck", strings.Contains(currentCookie, "shld_bt_ck")))
			
			// Parse cookie expiration time
			m.parseCookieExpireTime(currentCookie)
			break
		}
	} else if config.Cookie != "" {
		// Auto cookie is disabled, use the provided cookie directly
		m.autoCookieEnabled = false
		m.cookie = config.Cookie
		logger.Debug("Using provided cookie from configuration (auto_cookie disabled)", 
			zap.Int("cookie_length", len(config.Cookie)),
			zap.Bool("has_shld_bt_ck", strings.Contains(config.Cookie, "shld_bt_ck")))
		
		// Parse cookie expiration time
		m.parseCookieExpireTime(config.Cookie)
	} else {
		// No cookie and auto cookie disabled
		m.autoCookieEnabled = false
		logger.Warn("No cookie provided and auto_cookie disabled")
		logger.Debug("Will try to generate session automatically")
		go m.initializeSession()
	}


	// Initialize Telegram notifier if configured
	if config.Telegram != nil {
		m.telegramNotifier = notifier.NewTelegramNotifier(config.Telegram)
		if err := m.telegramNotifier.ValidateConfig(); err != nil {
			logger.Warn("Telegram configuration invalid", zap.Error(err))
			m.telegramNotifier = nil
		} else {
			logger.Debug("Telegram notifications enabled")

			// Send detailed startup notification
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				var productList, storeList string

				// Build product list
				for i, p := range config.Products {
					if i > 0 {
						productList += "\n"
					}
					productList += fmt.Sprintf("  • %s", p.Name)
				}

				// Build store list
				for i, s := range config.Stores {
					if i > 0 {
						storeList += "\n"
					}
					storeList += fmt.Sprintf("  • %s", s.Name)
				}

				apiMode := "Fulfillment API (需要认证)"
				if config.UsePickupAPI {
					apiMode = "Pickup API (无需认证)"
				}
				
				startupMessage := fmt.Sprintf("🍎 *苹果库存监控已启动*\n\n"+
					"📦 *监控产品 (%d个):*\n%s\n\n"+
					"🏪 *监控店铺 (%d个):*\n%s\n\n"+
					"⏰ 检查间隔: %d秒\n"+
					"🔌 API模式: %s\n"+
					"🤖 通知状态: 已启用\n\n"+
					"准备好在有库存时通知您！",
					len(config.Products), productList, len(config.Stores), storeList, int(config.Interval.Seconds()), apiMode)

				if err := m.telegramNotifier.SendMessage(ctx, startupMessage); err != nil {
					logger.Error("Failed to send startup notification", zap.Error(err))
				}
			}()
		}
	}

	ticker := time.NewTicker(config.Interval)
	defer ticker.Stop()

	// Initial check
	m.checkAllProducts(ctx, config, true)

	for {
		select {
		case <-ctx.Done():
			logger.Info("🛑 Monitoring stopped")
			return nil
		case <-ticker.C:
			m.checkAllProducts(ctx, config, false)
		}
	}
}

// checkAllProducts checks all product-store combinations sequentially
func (m *SimpleMonitor) checkAllProducts(ctx context.Context, config *MonitorConfig, isInitialCheck bool) {
	logger.Debug("🔍 Starting stock check", zap.Int("products", len(config.Products)), zap.Int("stores", len(config.Stores)))

	// Sequential execution without delays
	for _, product := range config.Products {
		for _, store := range config.Stores {
			m.checkProduct(ctx, product, store)
		}
	}

	logger.Debug("✅ Stock check completed")

	// Send initial check results notification
	if isInitialCheck && m.telegramNotifier != nil {
		go m.sendInitialCheckResults(ctx, config)
	}
}

// checkProduct checks a single product at a specific store
func (m *SimpleMonitor) checkProduct(ctx context.Context, product ProductConfig, store StoreConfig) {
	startTime := time.Now()

	// Call appropriate API based on configuration
	var result *APIAvailabilityResult
	var err error
	
	if m.usePickupAPI {
		// Use pickup API (no authentication required)
		result, err = m.CheckPickupAvailability(ctx, product.Code, store.Code, product.Name, store.Name)
	} else {
		// Use fulfillment API (requires authentication)
		result, err = m.callAppleAPI(ctx, product.Code, store.Code, product.Name, store.Name)
	}
	if err != nil {
		logger.Error("Failed to check product availability",
			zap.String("product", product.Name),
			zap.String("store", store.Name),
			zap.Error(err))

		if errors.Is(err, errAppleAuth) {
			// Only trigger browser refresh if auto cookie is enabled
			if m.autoCookieEnabled {
				// Don't clear cookie cache immediately - let refresh complete first
				m.triggerBrowserCookieRefresh()
			}

			if m.telegramNotifier != nil {
				// Only send notification if we haven't sent one in the last 30 minutes
				m.mu.Lock()
				shouldNotify := time.Since(m.last541ErrorTime) > 30*time.Minute
				if shouldNotify {
					m.last541ErrorTime = time.Now()
				}
				m.mu.Unlock()

				if shouldNotify {
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						
						var message string
						if m.autoCookieEnabled {
							message = fmt.Sprintf("⚠️ *认证失败通知*\n\n"+
								"❌ API返回541错误\n"+
								"📱 产品: %s\n"+
								"🏪 店铺: %s\n"+
								"🔑 原因: Cookie可能已过期\n"+
								"🤖 已启动自动刷新 cookie（Chromedp）\n"+
								"⏰ 时间: %s\n\n"+
								"若自动刷新失败，请手动更新配置文件中的 Cookie\n"+
								"（30分钟内不会重复提醒）",
								product.Name, store.Name,
								time.Now().Format("2006-01-02 15:04:05"))
						} else {
							message = fmt.Sprintf("⚠️ *认证失败通知*\n\n"+
								"❌ API返回541错误\n"+
								"📱 产品: %s\n"+
								"🏪 店铺: %s\n"+
								"🔑 原因: Cookie已过期\n"+
								"⏰ 时间: %s\n\n"+
								"请手动更新配置文件中的 Cookie\n"+
								"（30分钟内不会重复提醒）",
								product.Name, store.Name,
								time.Now().Format("2006-01-02 15:04:05"))
						}

						if err := m.telegramNotifier.SendMessage(ctx, message); err != nil {
							logger.Error("Failed to send 541 error notification", zap.Error(err))
						}
					}()
				}
			}
		}
		return
	}

	responseTime := time.Since(startTime)

	// Create current status
	currentStatus := &ProductStatus{
		ProductCode:  product.Code,
		ProductName:  product.Name,
		StoreCode:    store.Code,
		StoreName:    store.Name,
		IsAvailable:  result.IsAvailable,
		Status:       string(result.Status),
		CheckTime:    time.Now(),
		ResponseTime: responseTime.Milliseconds(),
		PickupQuote:  result.PickupETA,
	}

	// Check for status changes
	key := fmt.Sprintf("%s_%s", product.Code, store.Code)
	m.mu.Lock()
	lastStatus, exists := m.lastStatus[key]
	m.lastStatus[key] = currentStatus
	m.mu.Unlock()

	// Create product display name with storage highlight
	productDisplay := fmt.Sprintf("%s [%s]", product.Name, product.Storage)

	// Log status changes
	if !exists {
		logger.Debug("Initial status",
			zap.String("product", productDisplay),
			zap.String("storage", product.Storage),
			zap.String("store", store.Name),
			zap.Bool("available", result.IsAvailable),
			zap.String("status", string(result.Status)),
			zap.String("quote", result.PickupETA))
	} else if lastStatus.IsAvailable != currentStatus.IsAvailable || lastStatus.Status != currentStatus.Status || lastStatus.PickupQuote != currentStatus.PickupQuote {
		// Status changed - either availability, status text, or pickup quote changed
		if currentStatus.IsAvailable && !lastStatus.IsAvailable {
			// Stock became available
			logger.Info("🎉 STOCK AVAILABLE!",
				zap.String("product", productDisplay),
				zap.String("storage", product.Storage),
				zap.String("store", store.Name),
				zap.String("status", string(result.Status)),
				zap.String("quote", result.PickupETA))

			// Send Telegram notification for stock availability
			if m.telegramNotifier != nil {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					if err := m.telegramNotifier.SendStockAlert(ctx, product.Name, product.Storage, store.Name, string(result.Status), result.PickupETA); err != nil {
						logger.Error("Failed to send Telegram notification", zap.Error(err))
					}
				}()
			}
		} else if !currentStatus.IsAvailable && lastStatus.IsAvailable {
			// Stock became unavailable
			logger.Info("😞 Stock no longer available",
				zap.String("product", productDisplay),
				zap.String("storage", product.Storage),
				zap.String("store", store.Name),
				zap.String("status", string(result.Status)))

			// Send Telegram notification for stock becoming unavailable
			if m.telegramNotifier != nil {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					message := fmt.Sprintf("😞 *库存售罄通知*\n\n"+
						"📱 产品: %s\n"+
						"💾 容量: %s\n"+
						"🏪 店铺: %s\n"+
						"❌ 状态: 已售罄\n"+
						"⏰ 时间: %s\n\n"+
						"将继续为您监控库存变化...",
						product.Name, product.Storage, store.Name,
						time.Now().Format("2006-01-02 15:04:05"))

					if err := m.telegramNotifier.SendMessage(ctx, message); err != nil {
						logger.Error("Failed to send Telegram notification", zap.Error(err))
					}
				}()
			}
		} else if currentStatus.IsAvailable && lastStatus.IsAvailable &&
			(lastStatus.Status != currentStatus.Status || lastStatus.PickupQuote != currentStatus.PickupQuote) {
			// Stock still available but status or pickup time changed
			logger.Info("📝 Stock status updated",
				zap.String("product", productDisplay),
				zap.String("storage", product.Storage),
				zap.String("store", store.Name),
				zap.String("old_status", lastStatus.Status),
				zap.String("new_status", currentStatus.Status),
				zap.String("old_quote", lastStatus.PickupQuote),
				zap.String("new_quote", currentStatus.PickupQuote))

			// Send Telegram notification for status update
			if m.telegramNotifier != nil {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					message := fmt.Sprintf("📝 *库存状态更新*\n\n"+
						"📱 产品: %s\n"+
						"💾 容量: %s\n"+
						"🏪 店铺: %s\n"+
						"📊 状态: %s → %s\n"+
						"📅 取货时间: %s → %s\n"+
						"⏰ 更新时间: %s",
						product.Name, product.Storage, store.Name,
						lastStatus.Status, currentStatus.Status,
						lastStatus.PickupQuote, currentStatus.PickupQuote,
						time.Now().Format("2006-01-02 15:04:05"))

					if err := m.telegramNotifier.SendMessage(ctx, message); err != nil {
						logger.Error("Failed to send Telegram notification", zap.Error(err))
					}
				}()
			}
		}
	}
}

// callAppleAPI makes API call to Apple
func (m *SimpleMonitor) callAppleAPI(ctx context.Context, productCode, storeCode, productName, storeName string) (*APIAvailabilityResult, error) {
	// Check if cookie needs proactive refresh
	if m.refreshCookieIfNeeded(ctx) {
		logger.Debug("Cookie was refreshed proactively before API call")
	}
	
	m.mu.RLock()
	browserCtx := m.browserCtx
	autoCookieEnabled := m.autoCookieEnabled
	m.mu.RUnlock()
	
	// If auto cookie is enabled and we have a browser context, use browser method
	if autoCookieEnabled && browserCtx != nil {
		// Check if browser context is still valid
		select {
		case <-browserCtx.Done():
			logger.Warn("Browser context cancelled, refreshing browser and cookie")
			// Trigger a new browser session
			m.triggerBrowserCookieRefresh()
			// Fall back to HTTP for this request
			return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
		default:
			logger.Debug("Using browser method (auto_cookie enabled)")
			return m.callAppleAPIWithBrowser(ctx, productCode, storeCode, productName, storeName)
		}
	}
	
	// Use HTTP method with current cookie
	logger.Debug("Using HTTP method")
	
	return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
}

// refreshCookieForProduct gets fresh cookies by visiting specific product page
func (m *SimpleMonitor) refreshCookieForProduct(ctx context.Context, productCode string) error {
	m.mu.RLock()
	browserCtx := m.browserCtx
	m.mu.RUnlock()
	
	if browserCtx == nil {
		// Try to create browser context if not exists
		if err := m.RefreshCookieWithBrowser(ctx); err != nil {
			return fmt.Errorf("failed to create browser context: %w", err)
		}
		m.mu.RLock()
		browserCtx = m.browserCtx
		m.mu.RUnlock()
	}
	
	// Check if browser context is valid
	select {
	case <-browserCtx.Done():
		// Browser context expired, refresh it
		if err := m.RefreshCookieWithBrowser(ctx); err != nil {
			return fmt.Errorf("failed to refresh browser context: %w", err)
		}
		m.mu.RLock()
		browserCtx = m.browserCtx
		m.mu.RUnlock()
	default:
		// Browser context is valid
	}
	
	// Visit specific product page to get cookies
	productPageURL := fmt.Sprintf("%s/shop/buy-iphone/iphone-17-pro/%s", m.baseURL, productCode)
	
	opCtx, opCancel := context.WithTimeout(browserCtx, 15*time.Second)
	defer opCancel()
	
	var cookieString string
	err := chromedp.Run(opCtx,
		// Navigate to the specific product page
		chromedp.Navigate(productPageURL),
		chromedp.WaitReady("body"),
		chromedp.Sleep(2*time.Second),
		
		// Wait for cookies to be set
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Check for required cookies
			for i := 0; i < 10; i++ {
				var result map[string]interface{}
				script := `
					(function() {
						const cookies = document.cookie;
						const hasDssid2 = cookies.includes('dssid2=');
						const hasShldBtCk = cookies.includes('shld_bt_ck=');
						return {
							cookies: cookies,
							hasDssid2: hasDssid2,
							hasShldBtCk: hasShldBtCk,
							ready: hasDssid2 && hasShldBtCk
						};
					})()
				`
				if err := chromedp.Evaluate(script, &result).Do(ctx); err == nil {
					if ready, ok := result["ready"].(bool); ok && ready {
						if cookies, ok := result["cookies"].(string); ok {
							cookieString = cookies
							logger.Debug("Got both required cookies from product page",
								zap.String("product", productCode))
							return nil
						}
					}
				}
				chromedp.Sleep(1 * time.Second).Do(ctx)
			}
			return fmt.Errorf("timeout waiting for required cookies")
		}),
	)
	
	if err != nil {
		return fmt.Errorf("failed to get cookies from product page: %w", err)
	}
	
	// Update the monitor's cookie
	if cookieString != "" {
		m.mu.Lock()
		m.cookie = cookieString
		m.mu.Unlock()
		logger.Debug("Updated monitor cookie from product page", 
			zap.Int("length", len(cookieString)))
	}
	
	return nil
}

// callAppleAPIWithBrowser uses Chrome browser to fetch API data
func (m *SimpleMonitor) callAppleAPIWithBrowser(ctx context.Context, productCode, storeCode, productName, storeName string) (*APIAvailabilityResult, error) {
	// Try with page refresh retry if needed
	return m.callAppleAPIWithBrowserRetry(ctx, productCode, storeCode, productName, storeName, false)
}

// callAppleAPIWithBrowserRetry uses Chrome browser to fetch API data with retry logic
func (m *SimpleMonitor) callAppleAPIWithBrowserRetry(ctx context.Context, productCode, storeCode, productName, storeName string, isRetry bool) (*APIAvailabilityResult, error) {
	// Construct API URL with parameters
	apiURL := fmt.Sprintf("%s/shop/fulfillment-messages?fae=true&pl=true&mts.0=regular&mts.1=compact&parts.0=%s&store=%s", 
		m.baseURL, productCode, storeCode)

	logger.Debug("Using browser to fetch API data",
		zap.String("url", apiURL),
		zap.String("product", productCode),
		zap.String("store", storeCode))

	m.mu.RLock()
	browserCtx := m.browserCtx
	m.mu.RUnlock()
	
	if browserCtx == nil {
		logger.Warn("Browser context not available, falling back to HTTP")
		return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
	}
	
	// Check if browser context is still valid
	select {
	case <-browserCtx.Done():
		logger.Warn("Browser context was cancelled, falling back to HTTP")
		return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
	default:
		// Context is still valid, continue
	}

	var responseText string
	var statusCode int
	var cookiesAfterFulfillment string
	
	// Create a timeout context for this specific operation
	opCtx, opCancel := context.WithTimeout(browserCtx, 30*time.Second)
	defer opCancel()
	
	// Check cookie expiration status for logging
	m.mu.RLock()
	hasValidCookie := !m.isCookieExpiringSoon()
	m.mu.RUnlock()
	
	var err error
	
	if hasValidCookie {
		logger.Debug("Cookie still valid, skipping product page visit - going directly to API")
		// Skip product page visit and go directly to API
		err = chromedp.Run(opCtx,
			chromedp.Navigate(apiURL),
			chromedp.Sleep(3 * time.Second),
			
			// Get the response content
			chromedp.ActionFunc(func(ctx context.Context) error {
				logger.Debug("Reading API response (cookie was valid)")
				
				// Get page content
				var pageContent string
				if err := chromedp.OuterHTML("html", &pageContent, chromedp.ByQuery).Do(ctx); err != nil {
					return err
				}
				
				// Extract JSON from <pre> tag if present
				if strings.Contains(pageContent, "<pre>") {
					start := strings.Index(pageContent, "<pre>")
					end := strings.Index(pageContent, "</pre>")
					if start != -1 && end != -1 && end > start {
						jsonContent := pageContent[start+5:end]
						responseText = strings.TrimSpace(jsonContent)
						statusCode = 200 // Assume success if we got JSON content
					}
				} else {
					responseText = pageContent
					statusCode = 200
				}
				
				// Check for error indicators
				if strings.Contains(responseText, "Access Denied") || 
				   strings.Contains(responseText, "403") {
					statusCode = 403
				} else if strings.Contains(responseText, "541") {
					statusCode = 541
				}
				
				return nil
			}),
		)
	} else {
		logger.Debug("Cookie expires soon or is missing - visiting product page first")
		// First navigate to the buy page to establish session, then call API
		buyPageURL := fmt.Sprintf("%s/shop/buy-iphone/iphone-17-pro/%s", strings.TrimRight(m.baseURL, "/"), productCode)
		
		err = chromedp.Run(opCtx,
			// Step 1: Navigate to buy page to establish session
			chromedp.Navigate(buyPageURL),
			chromedp.WaitReady("body"),
			chromedp.Sleep(2 * time.Second),
		
			// Step 1.5: Trigger interactive elements to generate shld_bt_ck cookie
		chromedp.ActionFunc(func(ctx context.Context) error {
			logger.Debug("Attempting to trigger shld_bt_ck cookie generation")
			
			// Try clicking on model selection elements to trigger cookie generation
			var clickAttempts = []string{
				`[data-autom="dimensionScreensize6_3inch"]`, // Pro model
				`[data-autom="dimensionScreensize6_9inch"]`, // Pro Max model
				`button[data-autom="dimensionScreensize6_3inch"]`,
				`.rf-dimensions-picker button:first-child`,
				`.dimensiontype_screensize button`,
			}
			
			for _, selector := range clickAttempts {
				var exists bool
				err := chromedp.Evaluate(fmt.Sprintf(`document.querySelector('%s') !== null`, selector), &exists).Do(ctx)
				if err == nil && exists {
					logger.Debug("Clicking selector:", zap.String("selector", selector))
					chromedp.Click(selector, chromedp.ByQuery).Do(ctx)
					chromedp.Sleep(2 * time.Second)
					break
				}
			}
			
			// Try clicking on color selection
			var colorSelectors = []string{
				`[data-autom="dimensionColor沙色钛金属"]`,
				`[data-autom="dimensionColor黑色钛金属"]`,
				`.rf-dimensions-picker [data-autom*="dimensionColor"]`,
			}
			
			for _, selector := range colorSelectors {
				var exists bool
				err := chromedp.Evaluate(fmt.Sprintf(`document.querySelector('%s') !== null`, selector), &exists).Do(ctx)
				if err == nil && exists {
					logger.Debug("Clicking color selector:", zap.String("selector", selector))
					chromedp.Click(selector, chromedp.ByQuery).Do(ctx)
					chromedp.Sleep(2 * time.Second)
					break
				}
			}
			
			// Try clicking on storage capacity
			var storageSelectors = []string{
				`[data-autom="dimensionCapacity256gb"]`,
				`[data-autom="dimensionCapacity512gb"]`,
				`.rf-dimensions-picker [data-autom*="dimensionCapacity"]`,
			}
			
			for _, selector := range storageSelectors {
				var exists bool
				err := chromedp.Evaluate(fmt.Sprintf(`document.querySelector('%s') !== null`, selector), &exists).Do(ctx)
				if err == nil && exists {
					logger.Debug("Clicking storage selector:", zap.String("selector", selector))
					chromedp.Click(selector, chromedp.ByQuery).Do(ctx)
					chromedp.Sleep(2 * time.Second)
					break
				}
			}
			
			// Scroll down to trigger additional cookie generation
			chromedp.Evaluate(`window.scrollBy(0, 500)`, nil).Do(ctx)
			chromedp.Sleep(1 * time.Second)
			chromedp.Evaluate(`window.scrollBy(0, 500)`, nil).Do(ctx)
			chromedp.Sleep(1 * time.Second)
			
			// Check if shld_bt_ck cookie was generated
			var cookieCheck map[string]interface{}
			checkScript := `
				(function() {
					const cookies = document.cookie;
					const hasShldBtCk = cookies.includes('shld_bt_ck=');
					return {
						hasCriticalCookie: hasShldBtCk,
						cookieCount: cookies ? cookies.split(';').length : 0,
						cookies: cookies
					};
				})()
			`
			if err := chromedp.Evaluate(checkScript, &cookieCheck).Do(ctx); err == nil {
				if hasCookie, ok := cookieCheck["hasCriticalCookie"].(bool); ok && hasCookie {
					logger.Debug("Critical shld_bt_ck cookie detected")
				} else {
					logger.Warn("⚠️ Critical shld_bt_ck cookie not yet generated, may need more interaction")
					
					// Try additional interaction - simulate interest in purchasing
					chromedp.Evaluate(`
						// Simulate user engagement
						window.dispatchEvent(new Event('scroll'));
						window.dispatchEvent(new Event('mousemove'));
						
						// Try to find and interact with any available buttons
						const buttons = document.querySelectorAll('button, [role="button"]');
						if (buttons.length > 0) {
							buttons[0].click();
						}
					`, nil).Do(ctx)
					chromedp.Sleep(2 * time.Second)
				}
			}
			
			return nil
		}),
		chromedp.Sleep(2 * time.Second),
		
		// Check if page loaded successfully
		chromedp.ActionFunc(func(ctx context.Context) error {
			var pageInfo map[string]interface{}
			pageScript := `
				(function() {
					return {
						url: window.location.href,
						title: document.title,
						readyState: document.readyState,
						hasBody: !!document.body,
						bodyContent: document.body ? document.body.innerText.substring(0, 100) : 'No body'
					};
				})()
			`
			err := chromedp.Evaluate(pageScript, &pageInfo).Do(ctx)
			if err == nil {
				logger.Debug("Buy page status check:")
				if url, ok := pageInfo["url"].(string); ok {
					logger.Debug("  Current URL:", zap.String("url", url))
				}
				if title, ok := pageInfo["title"].(string); ok {
					logger.Debug("  Page title:", zap.String("title", title))
				}
				if readyState, ok := pageInfo["readyState"].(string); ok {
					logger.Debug("  Ready state:", zap.String("state", readyState))
				}
				if hasBody, ok := pageInfo["hasBody"].(bool); ok {
					logger.Debug("  Has body:", zap.Bool("hasBody", hasBody))
				}
				if bodyContent, ok := pageInfo["bodyContent"].(string); ok {
					logger.Debug("  Body preview:", zap.String("content", bodyContent))
				}
			}
			return err
		}),
		
		// Print current cookies after visiting buy page
		chromedp.ActionFunc(func(ctx context.Context) error {
			script := `
				(function() {
					return {
						cookies: document.cookie,
						url: window.location.href,
						userAgent: navigator.userAgent
					};
				})()
			`
			var result map[string]interface{}
			err := chromedp.Evaluate(script, &result).Do(ctx)
			if err == nil {
				logger.Debug("Current browser cookies after visiting buy page:")
				if cookies, ok := result["cookies"].(string); ok {
					if cookies == "" {
						logger.Debug("No cookies found")
					} else {
						logger.Debug("Raw cookie string:", zap.String("cookies", cookies))
						// Split and display each cookie
						parts := strings.Split(cookies, "; ")
						for i, part := range parts {
							logger.Debug(fmt.Sprintf("Cookie %d: %s", i+1, part))
						}
					}
				}
				if url, ok := result["url"].(string); ok {
					logger.Debug("Current URL:", zap.String("url", url))
				}
				if ua, ok := result["userAgent"].(string); ok {
					logger.Debug("User Agent:", zap.String("userAgent", ua))
				}
			}
			return err
		}),
		
		// Step 2: Directly navigate to API endpoint in browser session
		chromedp.ActionFunc(func(ctx context.Context) error {
			logger.Debug("Visited buy page, now navigating directly to API endpoint")
			
			// Print cookies before API call
			var cookieResult map[string]interface{}
			cookieScript := `
				(function() {
					return {
						cookies: document.cookie,
						cookieCount: document.cookie ? document.cookie.split(';').length : 0
					};
				})()
			`
			err := chromedp.Evaluate(cookieScript, &cookieResult).Do(ctx)
			if err == nil {
				logger.Debug("Cookies before API call:")
				if cookies, ok := cookieResult["cookies"].(string); ok {
					logger.Debug("Cookie string:", zap.String("cookies", cookies))
					if count, ok := cookieResult["cookieCount"].(float64); ok {
						logger.Debug("Cookie count:", zap.Int("count", int(count)))
					}
				}
			}
			
			return nil
		}),
		
		// Step 3: Navigate directly to the API URL to get cookies and response
		chromedp.Navigate(apiURL),
		chromedp.Sleep(3 * time.Second),
		
		// Step 4: Get the response content and extract cookies after successful fulfillment access
		chromedp.ActionFunc(func(ctx context.Context) error {
			logger.Debug("Navigated to API endpoint, reading response and extracting cookies")
			
			// Get the page source which should contain the JSON response
			var pageContent string
			if err := chromedp.OuterHTML("html", &pageContent, chromedp.ByQuery).Do(ctx); err != nil {
				return err
			}
			
			// Extract cookies after successful fulfillment page access
			var cookieResult map[string]interface{}
			cookieScript := `
				(function() {
					return {
						cookies: document.cookie,
						cookieCount: document.cookie ? document.cookie.split(';').length : 0,
						url: window.location.href
					};
				})()
			`
			if err := chromedp.Evaluate(cookieScript, &cookieResult).Do(ctx); err == nil {
				if cookies, ok := cookieResult["cookies"].(string); ok {
					cookiesAfterFulfillment = cookies
					logger.Debug("document.cookie after fulfillment (excludes httpOnly):", 
						zap.String("cookies", cookies),
						zap.Int("count", int(cookieResult["cookieCount"].(float64))))
					
					// DO NOT update the monitor's cookie from document.cookie!
					// document.cookie doesn't include httpOnly cookies like dssid2
					// The proper cookies were already set by RefreshCookieWithBrowser
					logger.Debug("Not updating monitor cookie (would lose httpOnly cookies)")
				}
			}
			
			// Check if this looks like a JSON response or an error page
			if strings.Contains(pageContent, "<pre>") {
				// Extract JSON content from <pre> tag
				start := strings.Index(pageContent, "<pre>")
				end := strings.Index(pageContent, "</pre>")
				if start != -1 && end != -1 && end > start {
					jsonContent := pageContent[start+5:end]
					responseText = strings.TrimSpace(jsonContent)
					statusCode = 200 // Assume success if we got JSON in <pre>
				} else {
					responseText = pageContent
				}
			} else if strings.Contains(pageContent, "{") && strings.Contains(pageContent, "}") {
				// Try to extract JSON content directly
				start := strings.Index(pageContent, "{")
				end := strings.LastIndex(pageContent, "}")
				if start != -1 && end != -1 && end > start {
					responseText = strings.TrimSpace(pageContent[start:end+1])
					statusCode = 200
				} else {
					responseText = pageContent
				}
			} else {
				responseText = pageContent
			}
			
			// Check for error indicators in the page
			if strings.Contains(pageContent, "Access Denied") || 
			   strings.Contains(pageContent, "403") ||
			   strings.Contains(pageContent, "541") {
				statusCode = 541 // Authentication error
			} else if strings.Contains(pageContent, "404") {
				statusCode = 404
			} else if statusCode == 0 {
				statusCode = 200 // Default to 200 if we got content
			}
			
			// Log current URL and page info
			var currentURL string
			chromedp.Location(&currentURL).Do(ctx)
			logger.Debug("Current URL after navigation:", zap.String("url", currentURL))
			
			// Log response preview
			preview := responseText
			if len(preview) > 300 {
				preview = preview[:300] + "..."
			}
			logger.Debug("Response content preview:", zap.String("content", preview))
			
			return nil
		}),
		)
	}
	
	if err != nil {
		// Check if this is a context cancellation and fall back to HTTP
		if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {
			logger.Warn("Browser operation timed out or was cancelled, falling back to HTTP")
			return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
		}
		return nil, fmt.Errorf("browser navigation failed: %w", err)
	}

	logger.Debug("Browser API response", 
		zap.Int("status_code", statusCode),
		zap.Int("response_length", len(responseText)))

	// Log response content for debugging (first 200 chars)
	responsePreview := responseText
	if len(responsePreview) > 200 {
		responsePreview = responsePreview[:200] + "..."
	}
	logger.Debug("Response content preview:", zap.String("content", responsePreview))

	// Check HTTP status code
	if statusCode == 403 || statusCode == 541 {
		logger.Error("❌ Browser API returned authentication error", zap.Int("status_code", statusCode))
		return nil, errAppleAuth
	}
	
	// If we successfully got cookies from fulfillment page, use HTTP with those cookies
	if cookiesAfterFulfillment != "" && statusCode == 200 {
		logger.Debug("Got fresh cookies from browser fulfillment page, using HTTP with browser cookies")
		// The cookies were already updated in Step 4, so HTTP method will use them
		return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
	}
	
	if statusCode != 200 {
		if !isRetry && statusCode != 403 && statusCode != 541 {
			// For non-authentication errors, try page refresh once
			logger.Warn("⚠️ Browser API returned non-200 status, refreshing page and retrying", zap.Int("status_code", statusCode))
			return m.retryWithPageRefresh(ctx, productCode, storeCode, productName, storeName)
		} else {
			// Authentication errors or already retried - fall back to HTTP
			logger.Warn("⚠️ Browser API returned non-200 status, falling back to HTTP", zap.Int("status_code", statusCode))
			return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
		}
	}

	// Check if response looks like an error page
	if strings.Contains(responseText, "Access Denied") || 
	   strings.Contains(responseText, "403") ||
	   strings.Contains(responseText, "541") ||
	   strings.Contains(responseText, "<html") ||
	   strings.Contains(responseText, "<!DOCTYPE") {
		
		if !isRetry {
			// First failure - try refreshing page and retry once
			logger.Warn("⚠️ Browser returned HTML error page, refreshing page and retrying once")
			return m.retryWithPageRefresh(ctx, productCode, storeCode, productName, storeName)
		} else {
			// Already retried - fall back to HTTP
			logger.Error("❌ Browser returned HTML error page after retry, falling back to HTTP client")
			return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
		}
	}

	// Check if response looks like JSON
	responseText = strings.TrimSpace(responseText)
	if !strings.HasPrefix(responseText, "{") {
		logger.Error("❌ Response is not JSON format - FULL CONTENT:")
		// Removed separator
		logger.Error(responseText)
		// Removed separator
		logger.Debug("Falling back to HTTP client")
		return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
	}

	// Parse JSON response
	var apiResponse AppleAPIResponse
	if err := json.Unmarshal([]byte(responseText), &apiResponse); err != nil {
		logger.Error("❌ Failed to parse JSON response - FULL CONTENT:")
		// Removed separator
		logger.Error(responseText)
		// Removed separator
		logger.Error("Parse error:", zap.Error(err))
		logger.Debug("Falling back to HTTP client")
		return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
	}

	return m.parseResponse(productCode, productName, storeName, &apiResponse)
}

// CheckPickupAvailability checks product availability using pickup-message-recommendations API
func (m *SimpleMonitor) CheckPickupAvailability(ctx context.Context, productCode, storeCode, productName, storeName string) (*APIAvailabilityResult, error) {
	// Build pickup API URL
	apiURL := fmt.Sprintf("%s/shop/pickup-message-recommendations", m.baseURL)
	
	params := url.Values{}
	params.Add("fae", "true")
	params.Add("mts.0", "regular")
	params.Add("mts.1", "compact")
	params.Add("searchNearby", "true")
	params.Add("store", storeCode)
	params.Add("product", productCode)
	
	fullURL := fmt.Sprintf("%s?%s", apiURL, params.Encode())
	
	logger.Debug("🏪 Checking pickup availability via HTTP",
		zap.String("product", productCode),
		zap.String("store", storeCode),
		zap.String("url", fullURL))
	
	// Create HTTP client
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	
	// Create request
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create pickup API request: %w", err)
	}
	
	// Set headers (no cookie needed for pickup API)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	
	logger.Debug("📡 Pickup API doesn't require authentication cookies")
	
	// Make request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pickup API request failed: %w", err)
	}
	defer resp.Body.Close()
	
	logger.Debug("Pickup API Response", 
		zap.Int("status_code", resp.StatusCode),
		zap.String("status", resp.Status))
	
	if resp.StatusCode == 541 {
		logger.Error("❌ Pickup API returned 541 - Authentication failed")
		return nil, errAppleAuth
	}
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pickup API returned status %d", resp.StatusCode)
	}
	
	// Handle compressed response
	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}
	
	// Read response
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	
	// Log response preview
	responsePreview := string(body)
	if len(responsePreview) > 300 {
		responsePreview = responsePreview[:300] + "..."
	}
	logger.Debug("Pickup API response preview", zap.String("content", responsePreview))
	
	// Parse pickup response (different format from fulfillment)
	result := &APIAvailabilityResult{
		ProductCode:     productCode,
		IsAvailable:     false,
		Status:          StatusUnavailable,
		PickupAvailable: false,
		PickupETA:       "暂无供应",
	}
	
	// Log the full response for debugging
	bodyStr := string(body)
	logger.Debug("📋 Pickup API Full Response:",
		zap.String("product", productCode),
		zap.String("store", storeCode),
		zap.String("response", bodyStr))
	
	// Parse JSON response to check availability
	// Check for availableStoresText field - when it contains a number, product is available
	// Example: "Available at 2 stores" means available
	// Example: "Available at [X] stores" means not available
	// Also check Chinese version: "在 2 家零售店有货" vs "在 [X] 家零售店有货"
	availablePattern := `"availableStoresText"\s*:\s*"Available at (\d+) store`
	availablePatternCN := `"availableStoreText"\s*:\s*"在 (\d+) 家零售店有货"`
	
	var numStores string
	if match := regexp.MustCompile(availablePattern).FindStringSubmatch(bodyStr); match != nil {
		numStores = match[1]
	} else if match := regexp.MustCompile(availablePatternCN).FindStringSubmatch(bodyStr); match != nil {
		numStores = match[1]
	}
	
	if numStores != "" {
		// Found a number in availableStoresText - product is available
		result.IsAvailable = true
		result.PickupAvailable = true
		result.Status = StatusAvailable
		result.PickupETA = fmt.Sprintf("可在 %s 家店取货", numStores)
		logger.Info("🎉 Product available for pickup!",
			zap.String("product", productName),
			zap.String("store", storeName),
			zap.String("available_stores", numStores))
	} else if strings.Contains(bodyStr, `"availableStoresText":"Available at [X] stores"`) || 
	          strings.Contains(bodyStr, `"availableStoreText":"在 [X] 家零售店有货"`) {
		// Placeholder [X] means not available
		logger.Debug("Product not available (placeholder in availableStoresText)",
			zap.String("product", productName),
			zap.String("store", storeName))
	}
	
	// Also check for specific store availability
	storePattern := fmt.Sprintf(`"storeNumber":"%s"[^}]*"availableNow":true`, storeCode)
	if regexp.MustCompile(storePattern).MatchString(bodyStr) {
		result.IsAvailable = true
		result.PickupAvailable = true
		result.Status = StatusAvailable
		if result.PickupETA == "暂无供应" {
			result.PickupETA = "可取货"
		}
		logger.Info("✅ Product available at this specific store!",
			zap.String("product", productName),
			zap.String("store", storeName))
	}
	
	// Always log the final pickup status
	if result.IsAvailable {
		logger.Info("📦 Pickup API Status: AVAILABLE",
			zap.String("product", productName),
			zap.String("store", storeName),
			zap.String("status", string(result.Status)),
			zap.String("pickup_eta", result.PickupETA))
	} else {
		logger.Info("📦 Pickup API Status: NOT AVAILABLE",
			zap.String("product", productName),
			zap.String("store", storeName),
			zap.String("status", string(result.Status)))
	}
	
	return result, nil
}

// callAppleAPIWithHTTP is the fallback HTTP client method
func (m *SimpleMonitor) callAppleAPIWithHTTP(ctx context.Context, productCode, storeCode, productName, storeName string) (*APIAvailabilityResult, error) {
	// Log function entry for debugging
	logger.Debug("📞 Entering callAppleAPIWithHTTP function",
		zap.String("product", productCode),
		zap.String("store", storeCode),
		zap.String("platform", runtime.GOOS))
	
	// Construct API URL
	apiURL := fmt.Sprintf("%s/shop/fulfillment-messages", m.baseURL)
	
	params := url.Values{}
	params.Add("fae", "true")
	params.Add("pl", "true")
	params.Add("mts.0", "regular")
	params.Add("mts.1", "compact")
	params.Add("parts.0", productCode)
	params.Add("store", storeCode)

	fullURL := fmt.Sprintf("%s?%s", apiURL, params.Encode())

	// Log the API request
	logger.Debug("🔗 Requesting Apple API via HTTP method",
		zap.String("product", productCode),
		zap.String("store", storeCode))

	// Create fresh HTTP client for each request (like curl does)
	freshClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Check if we have captured headers from browser, otherwise use platform defaults
	m.mu.RLock()
	hasCapturedHeaders := len(m.capturedHeaders) > 0
	capturedHeaders := make(map[string]string)
	for k, v := range m.capturedHeaders {
		capturedHeaders[k] = v
	}
	m.mu.RUnlock()
	
	// Linux platform: always use captured headers from Chrome
	if runtime.GOOS == "linux" {
		if hasCapturedHeaders {
			// Use captured headers from real browser requests
			logger.Info("🐧 Linux: Using captured headers from browser CDP")
			for key, value := range capturedHeaders {
				// Skip cookie header as we'll set it separately
				if strings.ToLower(key) != "cookie" && strings.ToLower(key) != "host" {
					req.Header.Set(key, value)
					logger.Debug(fmt.Sprintf("  Header: %s = %s", key, value))
				}
			}
			// Log important headers for debugging
			logger.Debug("Key headers set:",
				zap.String("User-Agent", req.Header.Get("User-Agent")),
				zap.String("Accept", req.Header.Get("Accept")),
				zap.String("Platform", req.Header.Get("Sec-Ch-Ua-Platform")))
		} else {
			// On Linux, we must have captured headers to work properly
			logger.Warn("⚠️ Linux: No captured headers available, using minimal headers")
			req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
			req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
			req.Header.Set("Cache-Control", "no-cache")
			// Let Chrome's actual user agent be captured
			req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
		}
	} else {
		// Non-Linux platforms: use Mac headers for compatibility
		if hasCapturedHeaders {
			logger.Info("📋 Using captured headers from browser CDP")
			for key, value := range capturedHeaders {
				if strings.ToLower(key) != "cookie" && strings.ToLower(key) != "host" {
					req.Header.Set(key, value)
				}
			}
		} else {
			logger.Debug("Using Mac headers for compatibility")
			req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
			req.Header.Set("Accept-Encoding", "gzip, deflate, br, zstd")
			req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
			req.Header.Set("Cache-Control", "no-cache")
			req.Header.Set("Pragma", "no-cache")
			req.Header.Set("Sec-Ch-Ua", `"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"`)
			req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
			req.Header.Set("Sec-Ch-Ua-Platform", `"macOS"`)
			req.Header.Set("Sec-Fetch-Dest", "document")
			req.Header.Set("Sec-Fetch-Mode", "navigate")
			req.Header.Set("Sec-Fetch-Site", "none")
			req.Header.Set("Sec-Fetch-User", "?1")
			req.Header.Set("Upgrade-Insecure-Requests", "1")
			req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
		}
	}

	// Use original fresh cookie for each independent request
	m.mu.RLock()
	originalCookie := m.cookie
	m.mu.RUnlock()

	if originalCookie != "" {
		req.Header.Set("Cookie", originalCookie)
		
		// Count cookies for summary
		cookieParts := strings.Split(originalCookie, ";")
		
		// On Linux, use debug level for cookie info
		if runtime.GOOS == "linux" {
			logger.Debug("🐧 Linux: Sending cookie to fulfillment API",
				zap.String("url", fullURL),
				zap.Int("total_cookies", len(cookieParts)),
				zap.Int("cookie_length", len(originalCookie)),
				zap.Bool("has_dssid2", strings.Contains(originalCookie, "dssid2=")),
				zap.Bool("has_shld_bt_ck", strings.Contains(originalCookie, "shld_bt_ck=")))
			
			// Also print first 200 chars of cookie for verification
			cookiePreview := originalCookie
			if len(cookiePreview) > 200 {
				cookiePreview = cookiePreview[:200] + "..."
			}
			logger.Debug("🍪 Cookie preview", zap.String("cookie", cookiePreview))
			
			// Print key cookies separately for debugging
			if strings.Contains(originalCookie, "dssid2=") {
				start := strings.Index(originalCookie, "dssid2=")
				end := strings.Index(originalCookie[start:], ";")
				if end == -1 {
					end = len(originalCookie) - start
				}
				logger.Debug("✅ dssid2 cookie found", zap.String("value_length", fmt.Sprintf("%d chars", end-7)))
			}
			
			if strings.Contains(originalCookie, "shld_bt_ck=") {
				start := strings.Index(originalCookie, "shld_bt_ck=")
				end := strings.Index(originalCookie[start:], ";")
				if end == -1 {
					end = len(originalCookie) - start
				}
				logger.Debug("✅ shld_bt_ck cookie found", zap.String("value_length", fmt.Sprintf("%d chars", end-11)))
			}
		} else {
			// Non-Linux platforms use debug level
			logger.Debug("Sending cookie to fulfillment API",
				zap.String("url", fullURL),
				zap.Int("total_cookies", len(cookieParts)),
				zap.Int("total_length", len(originalCookie)),
				zap.Bool("has_dssid2", strings.Contains(originalCookie, "dssid2=")),
				zap.Bool("has_shld_bt_ck", strings.Contains(originalCookie, "shld_bt_ck=")))
		}
	} else {
		logger.Warn("❌ No cookie available for fulfillment API request!")
	}


	// Make request with fresh independent client (like curl)
	resp, err := freshClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	logger.Debug("API Response", 
		zap.Int("status_code", resp.StatusCode),
		zap.String("status", resp.Status))

	if resp.StatusCode == 541 {
		logger.Error("❌ Apple API returned 541 - Authentication failed", 
			zap.String("response_status", resp.Status),
			zap.Int("cookie_length", len(originalCookie)))
		return nil, errAppleAuth
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	// Don't update cookies - keep each request completely independent like curl

	// Handle compressed response
	var reader io.Reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Parse response
	var apiResponse AppleAPIResponse
	if err := json.NewDecoder(reader).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return m.parseResponse(productCode, productName, storeName, &apiResponse)
}

// parseResponse parses the Apple API response
func (m *SimpleMonitor) parseResponse(productCode, productName, storeName string, response *AppleAPIResponse) (*APIAvailabilityResult, error) {
	result := &APIAvailabilityResult{
		ProductCode:     productCode,
		IsAvailable:     false,
		Status:          StatusUnavailable,
		PickupAvailable: false,
	}

	if response.Head.Status != "200" || response.Body == nil || response.Body.Content == nil {
		return result, nil
	}

	pickup := response.Body.Content.PickupMessage
	if pickup == nil || len(pickup.Stores) == 0 {
		return result, nil
	}

	// Check availability in any store
	for _, store := range pickup.Stores {
		if partInfo, exists := store.PartsAvailability[productCode]; exists {
			switch partInfo.PickupDisplay {
			case "available":
				result.IsAvailable = true
				result.Status = StatusAvailable
				result.PickupAvailable = true
				if partInfo.PickupSearchQuote != "" && partInfo.PickupSearchQuote != "暂无供应" {
					result.PickupETA = partInfo.PickupSearchQuote
				}
			case "limited":
				result.IsAvailable = true
				result.Status = StatusLimitedStock
				result.PickupAvailable = true
				if partInfo.PickupSearchQuote != "" && partInfo.PickupSearchQuote != "暂无供应" {
					result.PickupETA = partInfo.PickupSearchQuote
				}
			case "unavailable":
				result.Status = StatusUnavailable
				if partInfo.PickupSearchQuote == "暂无供应" {
					result.PickupETA = "暂无供应"
				}
			}

			// If we found availability, we can break
			if result.IsAvailable {
				break
			}
		}
	}

	// Print current stock status for each fulfillment request
	logger.Info("📦 Stock Status", 
		zap.String("product", productName),
		zap.String("store", storeName),
		zap.Bool("available", result.IsAvailable),
		zap.String("status", string(result.Status)),
		zap.String("pickup_eta", result.PickupETA))

	return result, nil
}

// retryWithPageRefresh refreshes the current page and retries the API call
func (m *SimpleMonitor) retryWithPageRefresh(ctx context.Context, productCode, storeCode, productName, storeName string) (*APIAvailabilityResult, error) {
	m.mu.RLock()
	browserCtx := m.browserCtx
	m.mu.RUnlock()
	
	if browserCtx == nil {
		logger.Warn("Browser context not available for page refresh, falling back to HTTP")
		return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
	}
	
	// Create a timeout context for the refresh operation
	opCtx, opCancel := context.WithTimeout(browserCtx, 15*time.Second)
	defer opCancel()
	
	logger.Debug("Refreshing page and retrying fulfillment API call")
	
	// Refresh the current page
	err := chromedp.Run(opCtx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Get current URL first
			var currentURL string
			if err := chromedp.Evaluate(`window.location.href`, &currentURL).Do(ctx); err != nil {
				logger.Debug("Could not get current URL for refresh, using product page")
				currentURL = fmt.Sprintf("%s/shop/buy-iphone/iphone-17-pro/%s", strings.TrimRight(m.baseURL, "/"), productCode)
			}
			
			logger.Debug("Refreshing current page", zap.String("url", currentURL))
			return chromedp.Navigate(currentURL).Do(ctx)
		}),
		chromedp.WaitReady("body"),
		chromedp.Sleep(2 * time.Second),
	)
	
	if err != nil {
		logger.Warn("Failed to refresh page, falling back to HTTP", zap.Error(err))
		return m.callAppleAPIWithHTTP(ctx, productCode, storeCode, productName, storeName)
	}
	
	logger.Debug("Page refreshed successfully, retrying API call")
	
	// Retry the API call with isRetry=true to prevent infinite recursion
	return m.callAppleAPIWithBrowserRetry(ctx, productCode, storeCode, productName, storeName, true)
}

// initializeSession initializes a session by visiting the main page to get cookies
func (m *SimpleMonitor) initializeSession() {
	logger.Info("🍪 Initializing session by visiting Apple Store homepage")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Visit a specific product page to establish session
	// Use a common product as default (can be any valid product code)
	mainURL := "https://www.apple.com.cn/shop/buy-iphone/iphone-17-pro/MG034CH/A"
	req, err := http.NewRequestWithContext(ctx, "GET", mainURL, nil)
	if err != nil {
		logger.Error("Failed to create session request", zap.Error(err))
		return
	}

	// Set basic headers
	req.Header.Set("User-Agent", m.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		logger.Error("Failed to initialize session", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	// Read response to complete the request
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode == http.StatusOK {
		logger.Info("✅ Session initialized successfully",
			zap.Int("cookies", len(m.httpClient.Jar.Cookies(req.URL))))
	} else {
		logger.Warn("Session initialization returned non-OK status",
			zap.Int("status", resp.StatusCode))
	}
}

// GetCurrentStatus returns current status of all monitored products
func (m *SimpleMonitor) GetCurrentStatus() map[string]*ProductStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*ProductStatus)
	for k, v := range m.lastStatus {
		result[k] = v
	}
	return result
}

// Apple API response structures
type AppleAPIResponse struct {
	Head *APIHead `json:"head"`
	Body *APIBody `json:"body"`
}

type APIHead struct {
	Status string `json:"status"`
}

type APIBody struct {
	Content *APIContent `json:"content"`
}

type APIContent struct {
	PickupMessage *APIPickupMessage `json:"pickupMessage"`
}

type APIPickupMessage struct {
	Stores []APIStore `json:"stores"`
}

type APIStore struct {
	PartsAvailability map[string]*APIPartAvailability `json:"partsAvailability"`
}

type APIPartAvailability struct {
	PickupDisplay     string `json:"pickupDisplay"`
	PickupSearchQuote string `json:"pickupSearchQuote"`
}

type APIAvailabilityResult struct {
	ProductCode     string             `json:"product_code"`
	IsAvailable     bool               `json:"is_available"`
	Status          AvailabilityStatus `json:"status"`
	PickupAvailable bool               `json:"pickup_available"`
	PickupETA       string             `json:"pickup_eta,omitempty"`
}

// sendInitialCheckResults sends initial stock check results notification
func (m *SimpleMonitor) sendInitialCheckResults(ctx context.Context, config *MonitorConfig) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	m.mu.RLock()
	defer m.mu.RUnlock()

	var availableCount, unavailableCount int
	var resultDetails string

	for _, product := range config.Products {
		for _, store := range config.Stores {
			key := fmt.Sprintf("%s_%s", product.Code, store.Code)
			if status, exists := m.lastStatus[key]; exists {
				if status.IsAvailable {
					availableCount++
					resultDetails += fmt.Sprintf("✅ %s - %s: *有库存* (%s)\n",
						product.Name, store.Name, status.PickupQuote)
				} else {
					unavailableCount++
					resultDetails += fmt.Sprintf("❌ %s - %s: 暂无库存\n",
						product.Name, store.Name)
				}
			} else {
				unavailableCount++
				resultDetails += fmt.Sprintf("⚠️ %s - %s: 检查失败\n",
					product.Name, store.Name)
			}
		}
	}

	message := fmt.Sprintf("📋 *初次库存检查结果*\n\n"+
		"📊 *统计:*\n"+
		"✅ 有库存: %d\n"+
		"❌ 无库存: %d\n\n"+
		"📝 *详细结果:*\n%s\n"+
		"🔄 将每%d秒自动检查一次，有变化时立即通知您！",
		availableCount, unavailableCount, resultDetails, int(config.Interval.Seconds()))

	if err := m.telegramNotifier.SendMessage(ctx, message); err != nil {
		logger.Error("Failed to send initial check results", zap.Error(err))
	}
}

// updateCookiesFromResponse updates the cookie from response headers
func (m *SimpleMonitor) updateCookiesFromResponse(resp *http.Response) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var newCookies []string

	// Keep existing base cookies (dssid2 if not replaced)
	existingCookies := strings.Split(m.cookie, "; ")
	cookieMap := make(map[string]string)

	for _, cookie := range existingCookies {
		if parts := strings.SplitN(cookie, "=", 2); len(parts) == 2 {
			cookieMap[parts[0]] = parts[1]
		}
	}

	// Update with new cookies from response
	for _, setCookie := range resp.Header["Set-Cookie"] {
		parts := strings.SplitN(setCookie, "=", 2)
		if len(parts) == 2 {
			name := parts[0]
			value := strings.SplitN(parts[1], ";", 2)[0] // Get value before first semicolon

			// Only update certain cookies, ignore expired ones
			if name == "dssid2" && value != "" {
				cookieMap[name] = value
				logger.Debug("Updated session cookie", zap.String("cookie", name))
			} else if name == "shld_bt_ck" && value != "" {
				cookieMap[name] = value
				logger.Debug("Updated auth cookie", zap.String("cookie", name))
			} else if name == "shld_bt_m" && value != "" {
				cookieMap[name] = value
				logger.Debug("Updated bt_m cookie", zap.String("cookie", name))
			}
		}
	}

	// Rebuild cookie string
	for name, value := range cookieMap {
		if value != "" {
			newCookies = append(newCookies, fmt.Sprintf("%s=%s", name, value))
		}
	}

	if len(newCookies) > 0 {
		m.cookie = strings.Join(newCookies, "; ")
		logger.Debug("Cookie updated", zap.Int("cookies", len(newCookies)))
		
		// Parse and store cookie expiration time
		m.parseCookieExpireTime(m.cookie)
	}
}

// parseCookieExpireTime extracts expire time from shld_bt_ck cookie
func (m *SimpleMonitor) parseCookieExpireTime(cookieStr string) {
	// Find shld_bt_ck cookie in the cookie string
	parts := strings.Split(cookieStr, "; ")
	for _, part := range parts {
		if strings.HasPrefix(part, "shld_bt_ck=") {
			cookieValue := strings.TrimPrefix(part, "shld_bt_ck=")
			
			// Parse the cookie value format: prefix|timestamp|suffix
			segments := strings.Split(cookieValue, "|")
			if len(segments) >= 2 {
				// Second segment should be the timestamp
				if timestamp := segments[1]; timestamp != "" {
					// Convert to int64 and then to time
					var ts int64
					if _, err := fmt.Sscanf(timestamp, "%d", &ts); err == nil {
						m.mu.Lock()
						m.cookieExpireTime = time.Unix(ts, 0)
						m.mu.Unlock()
						
						logger.Info("🕒 Cookie expiration time parsed",
							zap.Time("expire_time", m.cookieExpireTime),
							zap.Duration("time_remaining", time.Until(m.cookieExpireTime)))
						return
					}
				}
			}
			break
		}
	}
	logger.Debug("Failed to parse cookie expiration time")
}

// isCookieExpiringSoon checks if cookie expires within 10 minutes
func (m *SimpleMonitor) isCookieExpiringSoon() bool {
	m.mu.RLock()
	expireTime := m.cookieExpireTime
	m.mu.RUnlock()
	
	if expireTime.IsZero() {
		return true // If we don't know expire time, assume we need refresh
	}
	
	timeUntilExpire := time.Until(expireTime)
	return timeUntilExpire <= 10*time.Minute
}

// refreshCookieIfNeeded checks and refreshes cookie proactively  
func (m *SimpleMonitor) refreshCookieIfNeeded(ctx context.Context) bool {
	if !m.autoCookieEnabled {
		return false // Auto cookie disabled
	}
	
	if !m.isCookieExpiringSoon() {
		return false // Cookie still valid
	}
	
	m.mu.Lock()
	if m.cookieRefreshInProgress {
		m.mu.Unlock()
		logger.Debug("Cookie refresh already in progress, skipping")
		return false
	}
	m.cookieRefreshInProgress = true
	m.mu.Unlock()
	
	defer func() {
		m.mu.Lock()
		m.cookieRefreshInProgress = false
		m.mu.Unlock()
	}()
	
	logger.Info("🔄 Proactively refreshing cookie (expires soon)",
		zap.Time("current_expire_time", m.cookieExpireTime),
		zap.Duration("time_remaining", time.Until(m.cookieExpireTime)))
	
	// Refresh cookie using browser
	if err := m.RefreshCookieWithBrowser(ctx); err != nil {
		logger.Error("Failed to refresh cookie proactively", zap.Error(err))
		return false
	}
	
	// Parse new expiration time
	m.parseCookieExpireTime(m.cookie)
	
	logger.Info("✅ Cookie refreshed successfully",
		zap.Time("new_expire_time", m.cookieExpireTime),
		zap.Duration("time_remaining", time.Until(m.cookieExpireTime)))
	
	return true
}
