package apple

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"go.uber.org/zap"

	"goscan/pkg/logger"
)

var requiredCookieNames = []string{"dssid2", "shld_bt_ck"} // Both cookies are required

func hasRequiredCookies(cookies []*network.Cookie) bool {
	found := make(map[string]bool, len(requiredCookieNames))
	for _, c := range cookies {
		if !strings.Contains(c.Domain, "apple.com") {
			continue
		}
		found[c.Name] = true
	}

	for _, name := range requiredCookieNames {
		if !found[name] {
			return false
		}
	}

	return true
}

func (m *SimpleMonitor) RefreshCookieWithBrowser(ctx context.Context) error {
	logger.Debug("Launching headless browser to refresh cookies")

	// Get platform-specific headers for consistent cookie generation
	platformHeaders := getPlatformHeaders()
	
	// Log browser configuration for debugging
	logger.Debug("🌐 Browser Configuration")
	logger.Debug(fmt.Sprintf("  Platform: %s", runtime.GOOS))
	logger.Debug(fmt.Sprintf("  User-Agent: %s", platformHeaders.UserAgent))
	logger.Debug(fmt.Sprintf("  Sec-Ch-Ua-Platform: %s", platformHeaders.SecChUaPlatform))
	
	// More sophisticated browser flags to avoid detection
	allocOpts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true), // Use headless mode
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("excludeSwitches", "enable-automation"),
		chromedp.Flag("useAutomationExtension", false),
		chromedp.Flag("disable-gpu", false),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-default-browser-check", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-features", "TranslateUI"),
		chromedp.Flag("disable-ipc-flooding-protection", true),
		chromedp.Flag("disable-background-timer-throttling", true),
		chromedp.Flag("disable-renderer-backgrounding", true),
		chromedp.Flag("disable-backgrounding-occluded-windows", true),
		chromedp.Flag("window-size", "1920,1080"),
		chromedp.Flag("start-maximized", true),
		chromedp.Flag("user-agent", platformHeaders.UserAgent),
	)

	tmpProfile, err := os.MkdirTemp("", "chromedp-profile-*")
	if err != nil {
		logger.Warn("Failed to create temp profile for chromedp", zap.Error(err))
	} else {
		allocOpts = append(allocOpts, chromedp.UserDataDir(tmpProfile))
		defer func() {
			if err := os.RemoveAll(tmpProfile); err != nil {
				logger.Warn("Failed to remove temp profile", zap.Error(err))
			}
		}()
	}

	// Create independent allocator context that doesn't depend on the passed context
	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), allocOpts...)
	
	// Create browser context with longer timeout for persistent use
	browserCtx, browserCancel := chromedp.NewContext(allocCtx)
	
	// Start the browser immediately
	if err := chromedp.Run(browserCtx); err != nil {
		allocCancel()
		browserCancel()
		return fmt.Errorf("failed to start browser: %w", err)
	}
	
	// Store browser context for later API calls
	m.mu.Lock()
	// Clean up previous browser context if exists
	if m.browserCancel != nil {
		m.browserCancel()
	}
	if m.allocCancel != nil {
		m.allocCancel()
	}
	m.browserCtx = browserCtx
	m.browserCancel = browserCancel
	m.allocCancel = allocCancel
	m.mu.Unlock()
	
	logger.Debug("Browser context established and ready for API calls")

	// Use a separate context for the cookie refresh operation with the browser context as parent
	cookieCtx, cookieCancel := context.WithTimeout(browserCtx, 120*time.Second)
	defer cookieCancel()

	// Navigate to the specific product page to trigger cookie generation
	mainPageURL := fmt.Sprintf("%s/shop/buy-iphone/iphone-17-pro", strings.TrimRight(m.baseURL, "/"))
	// Use the specific product URL that we need cookies for
	productURL := fmt.Sprintf("%s/shop/buy-iphone/iphone-17-pro/MG034CH/A", strings.TrimRight(m.baseURL, "/"))
	
	var cookies []*network.Cookie

	// Capture headers from real browser requests
	var capturedRequestHeaders map[string]interface{}
	
	tasks := chromedp.Tasks{
		network.Enable(),
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Listen for network requests to capture real headers
			chromedp.ListenTarget(ctx, func(ev interface{}) {
				switch ev := ev.(type) {
				case *network.EventRequestWillBeSent:
					// Check if this is a fulfillment-messages request
					if strings.Contains(ev.Request.URL, "fulfillment-messages") {
						logger.Debug("Captured fulfillment-messages request headers from browser")
						capturedRequestHeaders = ev.Request.Headers
						
						// Store the captured headers in monitor
						m.mu.Lock()
						m.capturedHeaders = make(map[string]string)
						for key, value := range capturedRequestHeaders {
							if strValue, ok := value.(string); ok {
								m.capturedHeaders[key] = strValue
								logger.Debug(fmt.Sprintf("  Captured: %s = %s", key, strValue))
							}
						}
						m.mu.Unlock()
					}
				}
			})
			// Clear existing cookies first
			if err := network.ClearBrowserCookies().Do(ctx); err != nil {
				logger.Warn("Failed to clear browser cookies", zap.Error(err))
			}
			
			// Set viewport
			if err := emulation.SetDeviceMetricsOverride(1920, 1080, 1, false).Do(ctx); err != nil {
				logger.Warn("Failed to set viewport", zap.Error(err))
			}
			
			// Override navigator.webdriver property and platform-specific settings
			var platformInfo string
			switch runtime.GOOS {
			case "linux":
				platformInfo = "Linux x86_64"
			case "windows":
				platformInfo = "Win32"
			default:
				platformInfo = "MacIntel"
			}
			
			expr := fmt.Sprintf(`
				Object.defineProperty(navigator, 'webdriver', {
					get: () => undefined
				});
				Object.defineProperty(navigator, 'plugins', {
					get: () => [1, 2, 3, 4, 5]
				});
				Object.defineProperty(navigator, 'languages', {
					get: () => ['zh-CN', 'zh', 'en']
				});
				Object.defineProperty(navigator, 'platform', {
					get: () => '%s'
				});
			`, platformInfo)
			return chromedp.Evaluate(expr, nil).Do(ctx)
		}),
		// First navigate to main shop page
		chromedp.Navigate(m.baseURL),
		chromedp.Sleep(2 * time.Second),
		// Navigate to iPhone selection page
		chromedp.Navigate(mainPageURL),
		chromedp.WaitReady("body", chromedp.ByQuery),
		chromedp.Sleep(5 * time.Second),
		// Click on a specific model to go to configuration page
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Try clicking on Pro model selection
			var exists bool
			err := chromedp.Evaluate(`document.querySelector('[data-autom="dimensionScreensize6_3inch"]') !== null`, &exists).Do(ctx)
			if err == nil && exists {
				chromedp.Click(`[data-autom="dimensionScreensize6_3inch"]`, chromedp.ByQuery).Do(ctx)
				logger.Debug("Clicked on 6.3 inch model")
			}
			return nil
		}),
		chromedp.Sleep(3 * time.Second),
		// Select a color option
		chromedp.ActionFunc(func(ctx context.Context) error {
			var exists bool
			// Try to click on Desert Titanium color
			err := chromedp.Evaluate(`document.querySelector('[data-autom="dimensionColor沙色钛金属"]') !== null`, &exists).Do(ctx)
			if err == nil && exists {
				chromedp.Click(`[data-autom="dimensionColor沙色钛金属"]`, chromedp.ByQuery).Do(ctx)
				logger.Debug("Clicked on Desert Titanium color")
			}
			return nil
		}),
		chromedp.Sleep(3 * time.Second),
		// Select storage capacity
		chromedp.ActionFunc(func(ctx context.Context) error {
			var exists bool
			err := chromedp.Evaluate(`document.querySelector('[data-autom="dimensionCapacity256gb"]') !== null`, &exists).Do(ctx)
			if err == nil && exists {
				chromedp.Click(`[data-autom="dimensionCapacity256gb"]`, chromedp.ByQuery).Do(ctx)
				logger.Debug("Clicked on 256GB capacity")
			}
			return nil
		}),
		chromedp.Sleep(3 * time.Second),
		// Try to click continue button to proceed with configuration
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Try to find and click continue/proceed button
			var exists bool
			err := chromedp.Evaluate(`
				let btn = document.querySelector('[data-autom="proceed"]') || 
				          document.querySelector('.rf-pdp-continue-button button') ||
				          document.querySelector('button[type="submit"]');
				btn !== null
			`, &exists).Do(ctx)
			if err == nil && exists {
				err = chromedp.Evaluate(`
					let btn = document.querySelector('[data-autom="proceed"]') || 
					          document.querySelector('.rf-pdp-continue-button button') ||
					          document.querySelector('button[type="submit"]');
					if (btn) btn.click();
				`, nil).Do(ctx)
				logger.Debug("Clicked continue button")
			}
			return nil
		}),
		chromedp.Sleep(5 * time.Second),
		// Navigate to specific product URL 
		chromedp.Navigate(productURL),
		chromedp.Sleep(5 * time.Second),
		// Trigger a fulfillment-messages request to capture headers
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Make a direct fetch request to capture headers
			fulfillmentURL := fmt.Sprintf("%s/shop/fulfillment-messages?fae=true&pl=true&mts.0=regular&mts.1=compact&parts.0=MG034CH/A&store=R639", strings.TrimRight(m.baseURL, "/"))
			
			script := fmt.Sprintf(`
				fetch('%s', {
					method: 'GET',
					credentials: 'include'
				}).then(response => {
					console.log('Fulfillment request completed:', response.status);
				}).catch(error => {
					console.log('Fulfillment request failed:', error);
				});
			`, fulfillmentURL)
			
			return chromedp.Evaluate(script, nil).Do(ctx)
		}),
		chromedp.Sleep(3 * time.Second),
	}
	
	// On Linux, ensure we visit fulfillment API to capture headers
	if runtime.GOOS == "linux" {
		tasks = append(tasks,
			chromedp.ActionFunc(func(ctx context.Context) error {
				logger.Info("🐧 Linux: Visiting fulfillment API to capture request headers")
				testFulfillmentURL := fmt.Sprintf("%s/shop/fulfillment-messages?fae=true&pl=true&mts.0=regular&mts.1=compact&parts.0=MG034CH/A&store=R639", 
					strings.TrimRight(m.baseURL, "/"))
				
				// Navigate to fulfillment API to trigger header capture
				if err := chromedp.Navigate(testFulfillmentURL).Do(ctx); err != nil {
					logger.Warn("Failed to navigate to fulfillment API for header capture", zap.Error(err))
				}
				chromedp.Sleep(2 * time.Second).Do(ctx)
				
				// Navigate back to product page
				if err := chromedp.Navigate(productURL).Do(ctx); err != nil {
					logger.Warn("Failed to navigate back to product page", zap.Error(err))
				}
				chromedp.Sleep(1 * time.Second).Do(ctx)
				
				logger.Debug("Header capture navigation completed")
				return nil
			}),
		)
	}

	// Collect cookies with multiple attempts and page refresh
	tasks = append(tasks,
		chromedp.ActionFunc(func(ctx context.Context) error {
			targets := []string{
				strings.TrimRight(m.baseURL, "/"),
				productURL,
			}

			deadline := time.Now().Add(60 * time.Second) // Increased timeout
			attempts := 0
			pageRefreshCount := 0
			maxPageRefreshes := 5
			
			for {
				attempts++
				if err := ctx.Err(); err != nil {
					return err
				}
				
				result, err := network.GetCookies().WithURLs(targets).Do(ctx)
				if err != nil {
					logger.Warn("Failed to get cookies", zap.Error(err), zap.Int("attempt", attempts))
					if time.Now().After(deadline) {
						return err
					}
					time.Sleep(2 * time.Second)
					continue
				}
				
				cookies = result
				
				// Check what cookies we have
				var foundDssid2, foundShldBtCk bool
				var dssid2Preview, shldBtCkPreview string
				
				for _, c := range result {
					if c.Name == "dssid2" && strings.Contains(c.Domain, "apple.com") {
						foundDssid2 = true
						dssid2Preview = c.Value
						if len(dssid2Preview) > 20 {
							dssid2Preview = dssid2Preview[:20] + "..."
						}
					}
					if c.Name == "shld_bt_ck" && strings.Contains(c.Domain, "apple.com") {
						foundShldBtCk = true
						shldBtCkPreview = c.Value
						if len(shldBtCkPreview) > 20 {
							shldBtCkPreview = shldBtCkPreview[:20] + "..."
						}
					}
				}
				
				// Log current status
				if foundDssid2 {
					logger.Debug("Found dssid2 cookie", zap.String("preview", dssid2Preview))
				}
				if foundShldBtCk {
					logger.Debug("Found shld_bt_ck cookie", zap.String("preview", shldBtCkPreview))
				}
				
				logger.Debug("Cookie collection status", 
					zap.Int("total", len(cookies)),
					zap.Bool("has_dssid2", foundDssid2),
					zap.Bool("has_shld_bt_ck", foundShldBtCk),
					zap.Int("attempt", attempts),
					zap.Int("page_refreshes", pageRefreshCount))
				
				// Check if we have both required cookies
				if foundDssid2 && foundShldBtCk {
					logger.Debug("Both required cookies found successfully")
					return nil
				}
				
				// If missing cookies and haven't exceeded refresh limit, refresh the page
				if (!foundDssid2 || !foundShldBtCk) && pageRefreshCount < maxPageRefreshes {
					pageRefreshCount++
					logger.Debug("Missing required cookies, refreshing page", 
						zap.Bool("missing_dssid2", !foundDssid2),
						zap.Bool("missing_shld_bt_ck", !foundShldBtCk),
						zap.Int("refresh_count", pageRefreshCount))
					
					// Refresh the current page
					if err := chromedp.Reload().Do(ctx); err != nil {
						logger.Warn("Failed to refresh page", zap.Error(err))
					}
					
					// Wait for page to load after refresh
					chromedp.Sleep(5 * time.Second).Do(ctx)
					
					// Try some interactions after refresh
					chromedp.Evaluate(`window.scrollBy(0, 300)`, nil).Do(ctx)
					chromedp.Sleep(2 * time.Second).Do(ctx)
					
					continue
				}
				
				if time.Now().After(deadline) {
					if !foundDssid2 || !foundShldBtCk {
						return fmt.Errorf("timeout: failed to get required cookies (dssid2=%v, shld_bt_ck=%v)", 
							foundDssid2, foundShldBtCk)
					}
					break
				}
				
				// Try scrolling or other interactions to trigger cookie generation
				if attempts % 3 == 0 {
					chromedp.Evaluate(`window.scrollBy(0, 200)`, nil).Do(ctx)
				}
				
				time.Sleep(2 * time.Second)
			}
			return nil
		}),
	)

	if err := chromedp.Run(cookieCtx, tasks...); err != nil {
		return fmt.Errorf("chromedp run failed: %w", err)
	}

	if len(cookies) == 0 {
		return errors.New("headless browser did not return any cookies")
	}

	// Log total cookies found
	logger.Info("🍪 Cookie collection summary:",
		zap.Int("total_cookies", len(cookies)))
	
	// Extract all Apple-related cookies
	cookieMap := make(map[string]string)
	var dssid2Value string
	var httpOnlyCookies []string
	var regularCookies []string
	
	for _, c := range cookies {
		if strings.Contains(c.Domain, "apple.com") {
			cookieMap[c.Name] = c.Value
			
			// Track httpOnly vs regular cookies
			if c.HTTPOnly {
				httpOnlyCookies = append(httpOnlyCookies, c.Name)
			} else {
				regularCookies = append(regularCookies, c.Name)
			}
			
			// Special check for dssid2
			if c.Name == "dssid2" {
				dssid2Value = c.Value
				logger.Debug("Found dssid2 cookie", 
					zap.String("value", c.Value),
					zap.String("domain", c.Domain))
			}
			
			logger.Debug("Found cookie", 
				zap.String("name", c.Name),
				zap.String("domain", c.Domain),
				zap.Int("value_length", len(c.Value)))
		}
	}
	
	// Verify dssid2 is loaded
	if dssid2Value == "" {
		logger.Warn("⚠️ Warning: dssid2 cookie not found in final extraction")
		// Don't return error here, we already checked in the collection loop
	}

	// Log cookie types summary
	logger.Info("📊 Cookie types collected:",
		zap.Int("httpOnly_cookies", len(httpOnlyCookies)),
		zap.Int("regular_cookies", len(regularCookies)),
		zap.Strings("httpOnly_names", httpOnlyCookies),
		zap.Strings("regular_names", regularCookies))
	
	// Check for missing required cookies
	var missing []string
	for _, name := range requiredCookieNames {
		if cookieMap[name] == "" {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		logger.Warn("Some required cookies are missing",
			zap.Strings("missing", missing),
			zap.Int("total_cookies", len(cookieMap)))
	}
	
	// Check for important cookies
	if cookieMap["dssid2"] == "" {
		logger.Warn("⚠️ Critical cookie 'dssid2' not found, API calls may fail")
	} else {
		logger.Debug("Cookie obtained: dssid2' found successfully")
	}
	
	if cookieMap["shld_bt_ck"] == "" {
		logger.Warn("⚠️ Important auth cookie 'shld_bt_ck' not found, API calls may fail with 541")
	} else {
		logger.Debug("Auth cookie 'shld_bt_ck' found successfully")
	}

	// Sort cookie names for consistent ordering
	names := make([]string, 0, len(cookieMap))
	for name := range cookieMap {
		names = append(names, name)
	}
	sort.Strings(names)

	// Build cookie header string
	cookieParts := make([]string, 0, len(names))
	var hasDssid2InFinal bool
	for _, name := range names {
		cookieParts = append(cookieParts, fmt.Sprintf("%s=%s", name, cookieMap[name]))
		if name == "dssid2" {
			hasDssid2InFinal = true
		}
	}

	cookieHeader := strings.Join(cookieParts, "; ")
	
	// Final verification that dssid2 is in the cookie string
	if !hasDssid2InFinal {
		logger.Error("❌ Critical: dssid2 not included in final cookie string")
		return errors.New("dssid2 must be included in cookie string")
	}
	
	if !strings.Contains(cookieHeader, "dssid2=") {
		logger.Error("❌ Critical: Final cookie string missing dssid2")
		return errors.New("final cookie string does not contain dssid2")
	}

	// Update the monitor's cookie
	m.mu.Lock()
	m.cookie = cookieHeader
	m.mu.Unlock()

	logger.Info("✅ Cookie ready",
		zap.Int("cookies", len(cookieParts)),
		zap.Int("cookie_length", len(cookieHeader)),
		zap.Bool("has_dssid2", strings.Contains(cookieHeader, "dssid2=")),
		zap.Bool("has_shld_bt_ck", strings.Contains(cookieHeader, "shld_bt_ck=")))
	
	// Print full cookie string for debugging
	logger.Debug("Final cookie string obtained:")
	logger.Debug(cookieHeader)
	
	// Linux platform also needs browser session validation
	if runtime.GOOS == "linux" {
		logger.Info("🐧 Linux platform detected, testing cookie in browser session")
		
		// Test the cookie with fulfillment API using the SAME browser session
		logger.Info("🔍 Testing cookie with fulfillment API in same browser session...")
		
		// Try up to 3 times (1 initial + 2 retries)
		maxRetries := 3
		var testSuccess bool
		var lastError error
		
		for attempt := 1; attempt <= maxRetries; attempt++ {
			if attempt > 1 {
				logger.Info(fmt.Sprintf("🔄 Retry attempt %d/%d for browser session test", attempt-1, maxRetries-1))
				// Refresh page before retry
				if err := chromedp.Navigate(productURL).Do(cookieCtx); err == nil {
					chromedp.Sleep(3 * time.Second).Do(cookieCtx)
				}
			}
			
			testSuccess = m.testCookieWithFulfillmentInSameSession(cookieCtx)
			
			if testSuccess {
				logger.Info("✅ Cookie validated successfully in browser session")
				break
			}
			
			lastError = errors.New("browser session test failed")
			logger.Warn(fmt.Sprintf("⚠️ Browser session test attempt %d failed", attempt))
		}
		
		if !testSuccess {
			// After all retries failed, log the captured headers for debugging
			logger.Error("❌ Cookie validation failed after all retries, logging request headers for debugging")
			
			// Log captured headers
			m.mu.RLock()
			if len(m.capturedHeaders) > 0 {
				logger.Error("📋 Captured headers that were used:")
				for key, value := range m.capturedHeaders {
					logger.Error(fmt.Sprintf("  %s: %s", key, value))
				}
			} else {
				logger.Error("⚠️ No captured headers available")
			}
			m.mu.RUnlock()
			
			// Also test with HTTP to get more details
			logger.Info("🔍 Attempting HTTP validation for additional diagnostics...")
			httpSuccess := m.validateCookieWithHTTP(cookieHeader)
			if httpSuccess {
				logger.Warn("⚠️ HTTP validation succeeded but browser session failed - may be session-specific issue")
			} else {
				logger.Error("❌ Both browser session and HTTP validation failed")
			}
			
			// Clear the cookie and return error to trigger retry
			m.mu.Lock()
			m.cookie = ""
			m.mu.Unlock()
			return lastError
		}
	} else {
		// Test the cookie with fulfillment API using the SAME browser session
		logger.Debug("Testing cookie with fulfillment API in the same browser session...")
		
		// Use the same browser context (cookieCtx) that was used to get cookies
		testSuccess := m.testCookieWithFulfillmentInSameSession(cookieCtx)
		
		if !testSuccess {
			logger.Error("❌ Cookie test failed with fulfillment API, clearing and retrying")
			// Clear the cookie and return error to trigger retry
			m.mu.Lock()
			m.cookie = ""
			m.mu.Unlock()
			return errors.New("cookie validation failed with fulfillment API")
		}
		
		logger.Debug("Cookie validated successfully with fulfillment API")
	}
	
	// Re-verify that dssid2 is still in the saved cookie
	m.mu.RLock()
	finalCookie := m.cookie
	m.mu.RUnlock()
	
	if !strings.Contains(finalCookie, "dssid2=") {
		logger.Error("❌ Critical: dssid2 missing after validation")
		return errors.New("dssid2 missing after validation")
	}
	
	if !strings.Contains(finalCookie, "shld_bt_ck=") {
		logger.Error("❌ Critical: shld_bt_ck missing after validation")
		return errors.New("shld_bt_ck missing after validation")
	}
	
	logger.Debug("Both required cookies confirmed in final saved state")
	
	// Verify the cookie was saved
	m.mu.RLock()
	savedCookie := m.cookie
	m.mu.RUnlock()
	logger.Debug("Cookie saved for subsequent requests:", 
		zap.Bool("matches", savedCookie == cookieHeader),
		zap.Int("saved_length", len(savedCookie)))

	return nil
}

// validateCookieWithHTTP validates the cookie by making an HTTP request to pickup API
func (m *SimpleMonitor) validateCookieWithHTTP(cookieHeader string) bool {
	// Use pickup-message-recommendations API for validation (more reliable than fulfillment)
	testURL := fmt.Sprintf("%s/shop/pickup-message-recommendations?fae=true&mts.0=regular&mts.1=compact&searchNearby=true&store=R639&product=MG034CH/A", 
		strings.TrimRight(m.baseURL, "/"))
	
	logger.Info("📡 Testing cookie via HTTP request to pickup API", zap.String("url", testURL))
	
	// Create HTTP client
	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	
	// Create request
	req, err := http.NewRequest("GET", testURL, nil)
	if err != nil {
		logger.Error("Failed to create test request", zap.Error(err))
		return false
	}
	
	// Set headers using captured headers if available
	m.mu.RLock()
	hasCapturedHeaders := len(m.capturedHeaders) > 0
	if hasCapturedHeaders {
		// Use captured headers from CDP
		for key, value := range m.capturedHeaders {
			if strings.ToLower(key) != "cookie" && strings.ToLower(key) != "host" {
				req.Header.Set(key, value)
			}
		}
		logger.Info("🐧 Using captured Chrome headers for validation",
			zap.String("User-Agent", req.Header.Get("User-Agent")),
			zap.String("Platform", req.Header.Get("Sec-Ch-Ua-Platform")))
	} else {
		// Minimal headers for Linux when no captured headers available
		logger.Warn("⚠️ No captured headers for validation, using minimal headers")
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
		req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	}
	m.mu.RUnlock()
	
	// Set cookie
	req.Header.Set("Cookie", cookieHeader)
	
	// Log cookie info for debugging
	logger.Debug("🍪 Validating with cookie",
		zap.Int("cookie_length", len(cookieHeader)),
		zap.Bool("has_dssid2", strings.Contains(cookieHeader, "dssid2=")),
		zap.Bool("has_shld_bt_ck", strings.Contains(cookieHeader, "shld_bt_ck=")))
	
	// Make request
	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Cookie validation request failed", zap.Error(err))
		return false
	}
	defer resp.Body.Close()
	
	logger.Info("📊 Validation response status", 
		zap.Int("status_code", resp.StatusCode),
		zap.String("status", resp.Status))
	
	// Check status code
	if resp.StatusCode == 541 || resp.StatusCode == 403 {
		logger.Error("❌ Cookie validation failed - authentication error", zap.Int("status", resp.StatusCode))
		return false
	}
	
	if resp.StatusCode != 200 {
		logger.Error("❌ Cookie validation failed - unexpected status", zap.Int("status", resp.StatusCode))
		return false
	}
	
	// Read response body to verify it's JSON
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("Failed to read validation response", zap.Error(err))
		return false
	}
	
	// Check if response looks like valid JSON
	bodyStr := string(body)
	if strings.HasPrefix(strings.TrimSpace(bodyStr), "{") && 
	   (strings.Contains(bodyStr, "head") || strings.Contains(bodyStr, "body")) {
		logger.Info("✅ Cookie validation successful - got valid JSON response")
		
		// Log response preview
		preview := bodyStr
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		logger.Debug("Valid response preview", zap.String("content", preview))
		return true
	}
	
	logger.Error("❌ Cookie validation failed - invalid response format",
		zap.String("preview", bodyStr[:min(200, len(bodyStr))]))
	return false
}


// testCookieWithFulfillmentInSameSession tests if the cookie works with fulfillment API in the same browser session
func (m *SimpleMonitor) testCookieWithFulfillmentInSameSession(ctx context.Context) bool {
	// First visit the product page to establish proper session
	productCode := "MG034CH/A"
	productPageURL := fmt.Sprintf("%s/shop/buy-iphone/iphone-17-pro/%s", strings.TrimRight(m.baseURL, "/"), productCode)
	
	logger.Info("📱 Visiting product page first to establish session", zap.String("url", productPageURL))
	
	// Navigate to product page first
	err := chromedp.Run(ctx,
		chromedp.Navigate(productPageURL),
		chromedp.Sleep(3 * time.Second),
	)
	
	if err != nil {
		logger.Warn("Failed to visit product page", zap.Error(err))
	}
	
	// Test with a sample fulfillment URL - using same session that visited product page
	testURL := fmt.Sprintf("%s/shop/fulfillment-messages?fae=true&pl=true&mts.0=regular&mts.1=compact&parts.0=%s&store=R639", 
		strings.TrimRight(m.baseURL, "/"), productCode)
	
	logger.Info("🔍 Testing fulfillment API in same session:", zap.String("url", testURL))
	
	var responseText string
	var statusCode int
	
	// Navigate to fulfillment API in the same session
	err = chromedp.Run(ctx,
		// First check all cookies via CDP (includes httpOnly cookies)
		chromedp.ActionFunc(func(ctx context.Context) error {
			logger.Debug("Checking all cookies in current session before fulfillment API call")
			
			// Get all cookies including httpOnly via CDP
			cookies, err := network.GetCookies().Do(ctx)
			if err != nil {
				logger.Error("Failed to get cookies via CDP", zap.Error(err))
				return err
			}
			
			// Check for required cookies
			var hasDssid2, hasShldBtCk bool
			
			// On Linux, log more details
			if runtime.GOOS == "linux" {
				logger.Info("🐧 Linux: Checking cookies in browser session")
				totalCookies := 0
				for _, cookie := range cookies {
					if strings.Contains(cookie.Domain, "apple.com") {
						totalCookies++
					}
				}
				logger.Info(fmt.Sprintf("Found %d Apple cookies in session", totalCookies))
			}
			
			for _, cookie := range cookies {
				if strings.Contains(cookie.Domain, "apple.com") {
					if cookie.Name == "dssid2" {
						hasDssid2 = true
						logger.Info("✅ Found dssid2 in CDP cookies", 
							zap.Bool("httpOnly", cookie.HTTPOnly),
							zap.Int("value_length", len(cookie.Value)))
					}
					if cookie.Name == "shld_bt_ck" {
						hasShldBtCk = true
						logger.Info("✅ Found shld_bt_ck in CDP cookies",
							zap.Bool("httpOnly", cookie.HTTPOnly),
							zap.Int("value_length", len(cookie.Value)))
					}
				}
			}
			
			if !hasDssid2 {
				logger.Error("❌ dssid2 not found in CDP cookies - cannot proceed with fulfillment API")
				if runtime.GOOS == "linux" {
					// Log all cookie names for debugging
					logger.Error("Available cookies in session:")
					for _, cookie := range cookies {
						if strings.Contains(cookie.Domain, "apple.com") {
							logger.Error(fmt.Sprintf("  - %s (httpOnly: %v)", cookie.Name, cookie.HTTPOnly))
						}
					}
				}
				return errors.New("dssid2 cookie missing in browser session")
			}
			
			if !hasShldBtCk {
				logger.Warn("⚠️ shld_bt_ck not found in CDP cookies - API may fail")
			}
			
			logger.Info("🎯 Required cookies verified in browser session, proceeding to fulfillment API")
			return nil
		}),
		
		// Also check document.cookie for comparison (won't include httpOnly)
		chromedp.ActionFunc(func(ctx context.Context) error {
			var cookieResult map[string]interface{}
			cookieScript := `
				(function() {
					const cookies = document.cookie;
					return {
						cookies: cookies,
						hasDssid2: cookies.includes('dssid2='),
						hasShldBtCk: cookies.includes('shld_bt_ck=')
					};
				})()
			`
			if err := chromedp.Evaluate(cookieScript, &cookieResult).Do(ctx); err == nil {
				hasDssid2InDOM := cookieResult["hasDssid2"].(bool)
				hasShldBtCkInDOM := cookieResult["hasShldBtCk"].(bool)
				
				if !hasDssid2InDOM {
					logger.Info("ℹ️ dssid2 not visible in document.cookie (expected - it's httpOnly)")
					logger.Debug("This is normal behavior - httpOnly cookies cannot be accessed by JavaScript")
				}
				if !hasShldBtCkInDOM {
					logger.Warn("⚠️ shld_bt_ck not visible in document.cookie (unexpected - should be accessible)")
				}
				
				logger.Debug("document.cookie visibility check:",
					zap.Bool("has_dssid2", hasDssid2InDOM),
					zap.Bool("has_shld_bt_ck", hasShldBtCkInDOM),
					zap.String("note", "dssid2 is httpOnly, won't show in document.cookie"))
			}
			return nil
		}),
		
		// Log request details before navigating
		chromedp.ActionFunc(func(ctx context.Context) error {
			logger.Info("📤 Sending browser request to fulfillment API")
			
			// Get all cookies from CDP
			cookies, _ := network.GetCookies().Do(ctx)
			cookieStrs := []string{}
			for _, cookie := range cookies {
				if strings.Contains(cookie.Domain, "apple.com") {
					cookieStrs = append(cookieStrs, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
				}
			}
			fullCookie := strings.Join(cookieStrs, "; ")
			
			logger.Info("📋 Browser request headers:")
			logger.Info("  URL:", zap.String("value", testURL))
			logger.Info("  Cookie:", zap.String("value", fullCookie))
			
			// Log captured headers
			if len(m.capturedHeaders) > 0 {
				for key, value := range m.capturedHeaders {
					logger.Info(fmt.Sprintf("  %s: %s", key, value))
				}
			} else {
				logger.Info("  User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
				logger.Info("  Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
			}
			
			return nil
		}),
		
		// Use fetch API to test fulfillment endpoint instead of navigate
		chromedp.ActionFunc(func(ctx context.Context) error {
			logger.Info("🔄 Testing fulfillment API with fetch request")
			
			// Use fetch to make AJAX request like the real site does
			fetchScript := fmt.Sprintf(`
				(async function() {
					try {
						console.log('Attempting fetch to:', '%s');
						const response = await fetch('%s', {
							method: 'GET',
							credentials: 'include',
							headers: {
								'Accept': 'application/json',
								'X-Requested-With': 'XMLHttpRequest'
							}
						});
						console.log('Fetch completed with status:', response.status);
						const text = await response.text();
						console.log('Response length:', text.length);
						return {
							status: response.status,
							statusText: response.statusText,
							text: text,
							headers: response.headers.get('content-type'),
							ok: response.ok,
							url: response.url
						};
					} catch(error) {
						console.error('Fetch failed:', error);
						return {
							status: 0,
							statusText: error.toString(),
							text: '',
							headers: '',
							error: error.message || error.toString(),
							ok: false
						};
					}
				})()
			`, testURL, testURL)
			
			var fetchResult map[string]interface{}
			if err := chromedp.Evaluate(fetchScript, &fetchResult).Do(ctx); err != nil {
				logger.Error("Failed to execute fetch", zap.Error(err))
				return err
			}
			
			// Extract results safely
			if status, ok := fetchResult["status"].(float64); ok {
				statusCode = int(status)
			}
			if text, ok := fetchResult["text"].(string); ok {
				responseText = text
			}
			
			// Get statusText and error info safely
			statusText := ""
			if st, ok := fetchResult["statusText"].(string); ok {
				statusText = st
			}
			
			errorMsg := ""
			if err, ok := fetchResult["error"].(string); ok {
				errorMsg = err
			}
			
			responseURL := ""
			if url, ok := fetchResult["url"].(string); ok {
				responseURL = url
			}
			
			logger.Info("📊 Fetch API response",
				zap.Int("status", statusCode),
				zap.String("statusText", statusText),
				zap.Int("response_length", len(responseText)),
				zap.String("error", errorMsg),
				zap.String("final_url", responseURL))
			
			// If fulfillment API failed, try pickup-message-recommendations API
			if statusCode != 200 {
				logger.Info("🔄 Trying pickup-message-recommendations API as alternative")
				
				pickupURL := fmt.Sprintf("%s/shop/pickup-message-recommendations?fae=true&mts.0=regular&mts.1=compact&searchNearby=true&store=R639&product=%s", 
					strings.TrimRight(m.baseURL, "/"), productCode)
				
				pickupScript := fmt.Sprintf(`
					(async function() {
						try {
							console.log('Attempting pickup API fetch to:', '%s');
							const response = await fetch('%s', {
								method: 'GET',
								credentials: 'include',
								headers: {
									'Accept': 'application/json',
									'X-Requested-With': 'XMLHttpRequest'
								}
							});
							console.log('Pickup API status:', response.status);
							const text = await response.text();
							return {
								status: response.status,
								statusText: response.statusText,
								text: text,
								ok: response.ok
							};
						} catch(error) {
							console.error('Pickup API fetch failed:', error);
							return {
								status: 0,
								statusText: error.toString(),
								text: '',
								ok: false
							};
						}
					})()
				`, pickupURL, pickupURL)
				
				var pickupResult map[string]interface{}
				if err := chromedp.Evaluate(pickupScript, &pickupResult).Do(ctx); err == nil {
					if status, ok := pickupResult["status"].(float64); ok {
						pickupStatus := int(status)
						pickupResponseText := ""
						if text, ok := pickupResult["text"].(string); ok {
							pickupResponseText = text
						}
						
						logger.Info("📊 Pickup API response",
							zap.Int("status", pickupStatus),
							zap.Int("response_length", len(pickupResponseText)))
						
						// If pickup API succeeds, consider it valid
						if pickupStatus == 200 && strings.Contains(pickupResponseText, "{") {
							logger.Info("✅ Pickup API test successful - cookies are valid")
							statusCode = 200
							responseText = pickupResponseText
						}
					}
				}
			}
			
			// If fetch failed completely, try XMLHttpRequest as fallback
			if statusCode == 0 && errorMsg != "" {
				logger.Warn("⚠️ Fetch failed, trying XMLHttpRequest as fallback")
				
				xhrScript := fmt.Sprintf(`
					(function() {
						return new Promise(function(resolve) {
							var xhr = new XMLHttpRequest();
							xhr.open('GET', '%s', false); // Synchronous request
							xhr.setRequestHeader('Accept', 'application/json');
							xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
							xhr.withCredentials = true;
							try {
								xhr.send();
								resolve({
									status: xhr.status,
									statusText: xhr.statusText,
									text: xhr.responseText,
									headers: xhr.getResponseHeader('content-type')
								});
							} catch(e) {
								resolve({
									status: 0,
									statusText: e.toString(),
									text: '',
									headers: ''
								});
							}
						});
					})()
				`, testURL)
				
				var xhrResult map[string]interface{}
				if err := chromedp.Evaluate(xhrScript, &xhrResult).Do(ctx); err == nil {
					if status, ok := xhrResult["status"].(float64); ok {
						statusCode = int(status)
					}
					if text, ok := xhrResult["text"].(string); ok {
						responseText = text
					}
					logger.Info("📊 XMLHttpRequest response",
						zap.Int("status", statusCode),
						zap.Int("response_length", len(responseText)))
				}
			}
			
			// Check response
			if statusCode == 200 && strings.Contains(responseText, "{") {
				logger.Info("✅ Fulfillment API test successful")
				return nil
			} else if statusCode == 541 || statusCode == 403 {
				logger.Error("❌ Fulfillment API returned authentication error")
				// Log details for 541 error
				if statusCode == 541 {
					logger.Error("📋 541 Error details:")
					// Get cookies
					cookies, _ := network.GetCookies().Do(ctx)
					cookieStrs := []string{}
					hasDssid2 := false
					hasShldBtCk := false
					for _, cookie := range cookies {
						if strings.Contains(cookie.Domain, "apple.com") {
							cookieStrs = append(cookieStrs, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
							if cookie.Name == "dssid2" {
								hasDssid2 = true
							}
							if cookie.Name == "shld_bt_ck" {
								hasShldBtCk = true
							}
						}
					}
					logger.Error("  Has dssid2:", zap.Bool("value", hasDssid2))
					logger.Error("  Has shld_bt_ck:", zap.Bool("value", hasShldBtCk))
					logger.Error("  Response:", zap.String("text", responseText[:min(500, len(responseText))]))
				}
				return nil
			} else if statusCode == 404 {
				logger.Error("❌ Fulfillment API returned 404")
				logger.Error("  Response:", zap.String("text", responseText[:min(500, len(responseText))]))
				return nil
			} else {
				logger.Warn("⚠️ Unexpected response from fulfillment API",
					zap.Int("status", statusCode),
					zap.String("text", responseText[:min(200, len(responseText))]))
				return nil
			}
		}),
	)
	
	if err != nil {
		logger.Error("Fulfillment test failed with error", zap.Error(err))
		return false
	}
	
	// Check if we got a valid response
	if statusCode == 541 || statusCode == 403 || statusCode == 404 {
		logger.Error("❌ Cookie validation failed", zap.Int("status", statusCode))
		return false
	}
	
	if statusCode == 200 && strings.Contains(responseText, "{") {
		logger.Debug("Cookie validation successful - fulfillment API accessible in same session")
		return true
	}
	
	logger.Warn("⚠️ Cookie validation unclear", 
		zap.Int("status", statusCode),
		zap.Int("response_length", len(responseText)))
	
	// If we got some response but unclear, check if it looks like JSON
	if len(responseText) > 0 && strings.HasPrefix(responseText, "{") {
		return true
	}
	
	return false
}

// testCookieWithFulfillment tests if the cookie works with fulfillment API (deprecated - use testCookieWithFulfillmentInSameSession)
func (m *SimpleMonitor) testCookieWithFulfillment(browserCtx context.Context, cookieHeader string) bool {
	// Test with a sample fulfillment URL
	testURL := fmt.Sprintf("%s/shop/fulfillment-messages?fae=true&pl=true&mts.0=regular&mts.1=compact&parts.0=MG034CH/A&store=R639", 
		strings.TrimRight(m.baseURL, "/"))
	
	logger.Debug("Testing cookie with URL:", zap.String("url", testURL))
	
	// Create a timeout context for the test
	testCtx, testCancel := context.WithTimeout(browserCtx, 20*time.Second)
	defer testCancel()
	
	var responseText string
	var statusCode int
	
	err := chromedp.Run(testCtx,
		// Log request details before navigating
		chromedp.ActionFunc(func(ctx context.Context) error {
			logger.Info("📤 Sending browser request to fulfillment API")
			
			// Get all cookies from CDP
			cookies, _ := network.GetCookies().Do(ctx)
			cookieStrs := []string{}
			for _, cookie := range cookies {
				if strings.Contains(cookie.Domain, "apple.com") {
					cookieStrs = append(cookieStrs, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
				}
			}
			fullCookie := strings.Join(cookieStrs, "; ")
			
			logger.Info("📋 Browser request headers:")
			logger.Info("  URL:", zap.String("value", testURL))
			logger.Info("  Cookie:", zap.String("value", fullCookie))
			
			// Log captured headers
			if len(m.capturedHeaders) > 0 {
				for key, value := range m.capturedHeaders {
					logger.Info(fmt.Sprintf("  %s: %s", key, value))
				}
			} else {
				logger.Info("  User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
				logger.Info("  Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
			}
			
			return nil
		}),
		
		// Navigate to the fulfillment API endpoint
		chromedp.Navigate(testURL),
		chromedp.Sleep(3 * time.Second),
		
		// Check the response
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Get the page content
			var pageContent string
			if err := chromedp.OuterHTML("html", &pageContent, chromedp.ByQuery).Do(ctx); err != nil {
				return err
			}
			
			// Check if it's a JSON response (success) or error page
			if strings.Contains(pageContent, "<pre>") {
				// Extract JSON from <pre> tag
				start := strings.Index(pageContent, "<pre>")
				end := strings.Index(pageContent, "</pre>")
				if start != -1 && end != -1 && end > start {
					responseText = pageContent[start+5:end]
					statusCode = 200
				}
			} else if strings.Contains(pageContent, "{") && strings.Contains(pageContent, "}") {
				// Direct JSON response
				start := strings.Index(pageContent, "{")
				end := strings.LastIndex(pageContent, "}")
				if start != -1 && end != -1 && end > start {
					responseText = pageContent[start:end+1]
					statusCode = 200
				}
			}
			
			// Check for error indicators
			if strings.Contains(pageContent, "Access Denied") || 
			   strings.Contains(pageContent, "403") ||
			   strings.Contains(pageContent, "541") {
				statusCode = 541
				logger.Warn("❌ Fulfillment test returned authentication error")
				return nil
			}
			
			// Check if response is valid JSON
			responseText = strings.TrimSpace(responseText)
			if strings.HasPrefix(responseText, "{") && strings.Contains(responseText, "head") {
				logger.Info("✅ Fulfillment test successful - got valid JSON response")
				return nil
			}
			
			return nil
		}),
	)
	
	if err != nil {
		logger.Error("Fulfillment test failed with error", zap.Error(err))
		return false
	}
	
	// Check if we got a valid response
	if statusCode == 541 || statusCode == 403 {
		logger.Error("❌ Cookie validation failed - API returned authentication error", zap.Int("status", statusCode))
		return false
	}
	
	if statusCode == 200 && strings.Contains(responseText, "{") {
		logger.Debug("Cookie validation successful - fulfillment API accessible")
		return true
	}
	
	logger.Warn("⚠️ Cookie validation unclear", 
		zap.Int("status", statusCode),
		zap.String("response_preview", responseText[:min(100, len(responseText))]))
	
	// If unclear, assume it's working to avoid infinite retries
	return true
}

func (m *SimpleMonitor) triggerBrowserCookieRefresh() {
	m.mu.Lock()
	if m.cookieRefreshInProgress {
		m.mu.Unlock()
		logger.Debug("Cookie refresh already running, skip new request")
		return
	}
	m.cookieRefreshInProgress = true
	baseCtx := m.baseCtx
	m.mu.Unlock()

	if baseCtx == nil {
		baseCtx = context.Background()
	}

	go func() {
		defer func() {
			m.mu.Lock()
			m.cookieRefreshInProgress = false
			m.mu.Unlock()
		}()

		ctx, cancel := context.WithTimeout(baseCtx, 90*time.Second)
		defer cancel()

		if err := m.RefreshCookieWithBrowser(ctx); err != nil {
			logger.Error("Headless cookie refresh failed", zap.Error(err))
		}
	}()
}

func (m *SimpleMonitor) CurrentCookie() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cookie
}

// Stop cleanly shuts down the monitor and closes browser context
func (m *SimpleMonitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.browserCancel != nil {
		logger.Info("🔒 Closing browser context")
		m.browserCancel()
		m.browserCancel = nil
		m.browserCtx = nil
	}
	
	if m.allocCancel != nil {
		logger.Info("🔒 Closing allocator context")
		m.allocCancel()
		m.allocCancel = nil
	}
}
