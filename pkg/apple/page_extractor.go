package apple

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// PageExtractor handles extraction of stock data directly from Apple Store web pages
type PageExtractor struct {
	ctx          context.Context
	cancel       context.CancelFunc
	browserCtx   context.Context
	headers      map[string]string
	waitStrategy *WaitStrategy
	sessionEstablished bool // Track if session has been established
	httpClient   *http.Client // Reusable HTTP client
	cookieCache  []*network.Cookie // Cache cookies to avoid repeated extraction
	lastCookieUpdate time.Time // Track when cookies were last updated
}

// NewPageExtractor creates a new page data extractor
func NewPageExtractor() (*PageExtractor, error) {
	return NewPageExtractorWithOptions(false)
}

// NewPageExtractorWithOptions creates a new page data extractor with options
func NewPageExtractorWithOptions(headless bool) (*PageExtractor, error) {
	// Create browser context
	ctx := context.Background()
	
	// Get Chrome executable path based on OS
	chromePath := getChromePath()
	
	// Get OS-specific browser options
	opts := GetBrowserOptions(headless)
	
	// Add Chrome path if found
	if chromePath != "" {
		opts = append(opts, chromedp.ExecPath(chromePath))
		log.Printf("Using Chrome path: %s", chromePath)
	} else {
		log.Println("Warning: Chrome path not specified, using system default")
	}
	
	allocCtx, cancel := chromedp.NewExecAllocator(ctx, opts...)
	browserCtx, _ := chromedp.NewContext(allocCtx, chromedp.WithLogf(log.Printf))
	
	// Start browser
	if err := chromedp.Run(browserCtx); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to start browser: %w", err)
	}
	
	pe := &PageExtractor{
		ctx:          ctx,
		cancel:       cancel,
		browserCtx:   browserCtx,
		headers:      make(map[string]string),
		waitStrategy: NewWaitStrategy(),
		sessionEstablished: false,
		httpClient:   &http.Client{Timeout: 10 * time.Second},
		cookieCache:  nil,
		lastCookieUpdate: time.Time{},
	}
	
	// Setup anti-detection
	if err := pe.setupAntiDetection(); err != nil {
		pe.Close()
		return nil, fmt.Errorf("failed to setup anti-detection: %w", err)
	}
	
	return pe, nil
}

// setupAntiDetection configures browser to avoid detection
func (pe *PageExtractor) setupAntiDetection() error {
	return chromedp.Run(pe.browserCtx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			err := chromedp.Evaluate(`
				Object.defineProperty(navigator, 'webdriver', {
					get: () => undefined
				});
				Object.defineProperty(navigator, 'plugins', {
					get: () => [1, 2, 3, 4, 5]
				});
				Object.defineProperty(navigator, 'languages', {
					get: () => ['zh-CN', 'zh', 'en']
				});
				window.chrome = {
					runtime: {}
				};
				Object.defineProperty(navigator, 'permissions', {
					get: () => ({
						query: () => Promise.resolve({ state: 'granted' })
					})
				});
			`, nil).Do(ctx)
			return err
		}),
	)
}

// ExtractStockInfo extracts stock information for a product at specific stores
func (pe *PageExtractor) ExtractStockInfo(productCode string, storeCode string) (*BrowserStockInfo, error) {
	log.Printf("Starting stock extraction - Product: %s, Store: %s", productCode, storeCode)
	
	stockInfo := &BrowserStockInfo{
		ProductCode: productCode,
		StoreCode:   storeCode,
		CheckTime:   time.Now(),
	}
	
	startTime := time.Now()
	
	// First navigate to iPhone 17 Pro page to establish session
	productURL := "https://www.apple.com.cn/shop/buy-iphone/iphone-17-pro"
	log.Printf("Step 1: Navigating to product page: %s", productURL)
	
	// Create context with timeout
	ctx, cancel := context.WithTimeout(pe.browserCtx, 30*time.Second)
	defer cancel()
	
	// Variables for stock status
	var stockStatus string
	var pickupQuote string
	
	// Navigate to product page first
	err := chromedp.Run(ctx,
		chromedp.Navigate(productURL),
		chromedp.Sleep(3*time.Second), // Wait for page load
		chromedp.ActionFunc(func(ctx context.Context) error {
			log.Println("Product page loaded, session established")
			return nil
		}),
		
		// Check stock availability
		chromedp.ActionFunc(func(ctx context.Context) error {
			log.Println("Checking stock availability...")
			
			// Get all text from the page to look for stock indicators
			var pageText string
			if err := chromedp.Text(`body`, &pageText, chromedp.ByQuery).Do(ctx); err == nil {
				// Look for stock-related keywords in page text
				if strings.Contains(pageText, "暂无供应") || 
				   strings.Contains(pageText, "不可取货") || 
				   strings.Contains(pageText, "目前缺货") ||
				   strings.Contains(pageText, "暂不提供") {
					stockStatus = "unavailable"
					stockInfo.IsAvailable = false
					pickupQuote = "暂无供应"
					log.Println("Stock status: UNAVAILABLE")
				} else if strings.Contains(pageText, "今日可取") || 
				          strings.Contains(pageText, "有货") ||
				          strings.Contains(pageText, "可取货") {
					stockStatus = "available"
					stockInfo.IsAvailable = true
					pickupQuote = "有货"
					log.Println("Stock status: AVAILABLE")
				} else {
					// Default to unavailable if no clear indicator
					stockStatus = "unknown"
					stockInfo.IsAvailable = false
					pickupQuote = "状态未知"
					log.Println("Stock status: UNKNOWN")
				}
			}
			
			// Try to get more specific pickup quote from common selectors
			selectors := []string{
				`.rf-pickup-quote`,
				`[class*="fulfillment"]`,
				`[class*="pickup"]`,
				`.as-pickup-quote`,
			}
			
			for _, selector := range selectors {
				var text string
				if err := chromedp.Text(selector, &text, chromedp.ByQuery).Do(ctx); err == nil && text != "" {
					pickupQuote = strings.TrimSpace(text)
					log.Printf("Found pickup quote: %s", pickupQuote)
					break
				}
			}
			
			return nil
		}),
		
		// Try to get store-specific information if store selector is available
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Check if we can select a specific store
			storeSelector := fmt.Sprintf(`[data-store-id="%s"]`, storeCode)
			var nodes []*cdp.Node
			if err := chromedp.Nodes(storeSelector, &nodes).Do(ctx); err == nil && len(nodes) > 0 {
				// Click on specific store
				if err := chromedp.Click(storeSelector, chromedp.ByQuery).Do(ctx); err == nil {
					chromedp.Sleep(1 * time.Second).Do(ctx)
					
					// Re-check stock status for specific store
					var storeQuote string
					selectors := []string{
						`.rf-pickup-quote`,
						`[class*="rf-fulfillmentquote"]`,
						`[class*="pickup-quote"]`,
					}
					
					for _, selector := range selectors {
						if err := chromedp.Text(selector, &storeQuote, chromedp.ByQuery).Do(ctx); err == nil && storeQuote != "" {
							pickupQuote = strings.TrimSpace(storeQuote)
							
							// Update stock status based on store-specific info
							if strings.Contains(pickupQuote, "暂无供应") || 
							   strings.Contains(pickupQuote, "不可取货") {
								stockStatus = "unavailable"
								stockInfo.IsAvailable = false
							} else if strings.Contains(pickupQuote, "今日可取") {
								stockStatus = "available"
								stockInfo.IsAvailable = true
							} else {
								stockStatus = "limited"
								stockInfo.IsAvailable = true
							}
							break
						}
					}
				}
			}
			return nil
		}),
	)
	
	if err != nil {
		// Check for 541 error
		if pe.is541Error() {
			stockInfo.Has541Error = true
			return stockInfo, fmt.Errorf("541 error detected: %w", err)
		}
		return stockInfo, fmt.Errorf("failed to extract stock info: %w", err)
	}
	
	stockInfo.Status = stockStatus
	stockInfo.PickupQuote = pickupQuote
	stockInfo.ResponseTime = time.Since(startTime).Milliseconds()
	
	log.Printf("Extracted stock info - Product: %s, Store: %s, Status: %s, Quote: %s",
		productCode, storeCode, stockStatus, pickupQuote)
	
	return stockInfo, nil
}

// selectProductConfiguration selects product options based on product code
func (pe *PageExtractor) selectProductConfiguration(productCode string) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		log.Printf("Selecting configuration for product code: %s", productCode)
		
		// Parse product code to determine selections
		// Format: MGXX3CH/A where XX indicates config
		
		// Try to select screen size - use actual iPhone 16 Pro selectors
		// Note: Selectors may vary, trying multiple options
		sizeSelectors := []string{
			`[data-autom="dimensionScreensize6_9inch"]`,  // Pro Max
			`[data-autom="dimensionScreensize6_7inch"]`,  // Pro Max alternative
			`[data-autom="dimensionScreensize6_3inch"]`,  // Pro
			`[data-autom="dimensionScreensize6_1inch"]`,  // Pro alternative
		}
		
		// Try Pro Max selectors first for MG codes
		if strings.Contains(productCode, "MG") {
			log.Println("Trying to select Pro Max size...")
			for _, selector := range sizeSelectors[:2] {
				var nodes []*cdp.Node
				if err := chromedp.Nodes(selector, &nodes, chromedp.ByQuery).Do(ctx); err == nil && len(nodes) > 0 {
					log.Printf("Found size selector: %s", selector)
					chromedp.Click(selector, chromedp.ByQuery).Do(ctx)
					break
				}
			}
		}
		
		time.Sleep(500 * time.Millisecond) // Simple delay instead of smart delay
		
		// Select color based on code patterns
		colorSelectors := map[string]string{
			"MG03": `[data-autom="dimensionColor黑色钛金属"]`,
			"MG04": `[data-autom="dimensionColor白色钛金属"]`,
			"MG05": `[data-autom="dimensionColor原色钛金属"]`,
			"MG06": `[data-autom="dimensionColor沙色钛金属"]`,
		}
		
		for code, selector := range colorSelectors {
			if strings.Contains(productCode, code) {
				pe.waitStrategy.WaitAndClick(selector, chromedp.ByQuery).Do(ctx)
				break
			}
		}
		
		time.Sleep(500 * time.Millisecond) // Simple delay instead of smart delay
		
		// Select capacity
		capacitySelectors := map[string]string{
			"256": `[data-autom="dimensionCapacity256gb"]`,
			"512": `[data-autom="dimensionCapacity512gb"]`,
			"1tb": `[data-autom="dimensionCapacity1tb"]`,
		}
		
		for key, selector := range capacitySelectors {
			if strings.Contains(strings.ToLower(productCode), key) {
				pe.waitStrategy.WaitAndClick(selector, chromedp.ByQuery).Do(ctx)
				break
			}
		}
		
		time.Sleep(500 * time.Millisecond) // Simple delay instead of smart delay
		
		return nil
	})
}

// getProductURLPath maps product code to URL path
func (pe *PageExtractor) getProductURLPath(productCode string) string {
	log.Printf("Mapping product code %s to URL path", productCode)
	
	// Map product codes to URL paths
	urlMappings := map[string]string{
		"MG": "iphone-17-pro", // iPhone 17 Pro series
		"MH": "iphone-17",     // iPhone 17
		"MJ": "iphone-16-pro", // iPhone 16 Pro series
		"MK": "iphone-16",     // iPhone 16
		"ML": "iphone-15-pro", // iPhone 15 Pro series
		"MM": "iphone-15",     // iPhone 15
	}
	
	// Check first 2 characters of product code
	if len(productCode) >= 2 {
		prefix := productCode[:2]
		if path, ok := urlMappings[prefix]; ok {
			log.Printf("Mapped to URL path: %s", path)
			return path
		}
	}
	
	// Default to iPhone 17 Pro
	log.Printf("Using default URL path: iphone-17-pro")
	return "iphone-17-pro"
}

// is541Error checks if current page shows 541 error
func (pe *PageExtractor) is541Error() bool {
	var errorText string
	err := chromedp.Run(pe.browserCtx,
		chromedp.Text(`body`, &errorText, chromedp.ByQuery),
	)
	
	if err == nil {
		return strings.Contains(errorText, "541") || 
		       strings.Contains(errorText, "Error 541") ||
		       strings.Contains(errorText, "请稍后再试")
	}
	
	return false
}

// RefreshCookie refreshes browser cookies when 541 error occurs
func (pe *PageExtractor) RefreshCookie() error {
	log.Println("Refreshing cookies due to 541 error...")
	
	// Clear cookies
	err := chromedp.Run(pe.browserCtx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			return network.ClearBrowserCookies().Do(ctx)
		}),
	)
	
	if err != nil {
		return fmt.Errorf("failed to clear cookies: %w", err)
	}
	
	// Navigate to home page and rebuild cookies
	err = chromedp.Run(pe.browserCtx,
		chromedp.Navigate("https://www.apple.com.cn"),
		chromedp.Sleep(2*time.Second),
		
		// Navigate through pages to build cookies
		chromedp.Navigate("https://www.apple.com.cn/shop/buy-iphone/iphone-17-pro"),
		chromedp.Sleep(3*time.Second),
		
		// Perform some interactions to generate cookies
		chromedp.ActionFunc(func(ctx context.Context) error {
			// Scroll to trigger lazy loading
			chromedp.Evaluate(`window.scrollBy(0, 500)`, nil).Do(ctx)
			chromedp.Sleep(1 * time.Second).Do(ctx)
			chromedp.Evaluate(`window.scrollBy(0, -500)`, nil).Do(ctx)
			return nil
		}),
	)
	
	if err != nil {
		return fmt.Errorf("failed to refresh cookies: %w", err)
	}
	
	log.Println("Cookies refreshed successfully")
	return nil
}

// Close closes the browser and cleanup resources
func (pe *PageExtractor) Close() {
	if pe.cancel != nil {
		pe.cancel()
	}
}

// BrowserStockInfo represents extracted stock information from browser
type BrowserStockInfo struct {
	ProductCode  string
	StoreCode    string
	IsAvailable  bool
	Status       string // available, limited, unavailable
	PickupQuote  string
	CheckTime    time.Time
	ResponseTime int64 // in milliseconds
	Has541Error  bool
	Error        string // Error message if any
}

// ExtractAllStoresStock extracts stock info for all stores on current page
func (pe *PageExtractor) ExtractAllStoresStock(productCode string) ([]*BrowserStockInfo, error) {
	log.Printf("ExtractAllStoresStock for product: %s", productCode)
	
	// For now, just return a single stock info with default status
	// This is a simplified version to avoid timeout
	stockInfo := &BrowserStockInfo{
		ProductCode:  productCode,
		StoreCode:    "default",
		IsAvailable:  false,
		Status:       "unknown",
		PickupQuote:  "检查中",
		CheckTime:    time.Now(),
		ResponseTime: 1000,
	}
	
	// Try to get basic availability from product page
	urlPath := pe.getProductURLPath(productCode)
	if urlPath == "" {
		return []*BrowserStockInfo{stockInfo}, nil
	}
	
	url := fmt.Sprintf("https://www.apple.com.cn/shop/buy-iphone/%s", urlPath)
	
	ctx, cancel := context.WithTimeout(pe.browserCtx, 15*time.Second)
	defer cancel()
	
	err := chromedp.Run(ctx,
		chromedp.Navigate(url),
		chromedp.Sleep(3*time.Second),
		
		// Quick check for availability
		chromedp.ActionFunc(func(ctx context.Context) error {
			var pageText string
			if err := chromedp.Text(`body`, &pageText, chromedp.ByQuery).Do(ctx); err == nil {
				if strings.Contains(pageText, "有货") || strings.Contains(pageText, "今日可取") {
					stockInfo.IsAvailable = true
					stockInfo.Status = "available"
					stockInfo.PickupQuote = "有货"
				} else if strings.Contains(pageText, "暂无供应") || strings.Contains(pageText, "缺货") {
					stockInfo.IsAvailable = false
					stockInfo.Status = "unavailable"
					stockInfo.PickupQuote = "暂无供应"
				}
			}
			return nil
		}),
	)
	
	if err != nil {
		log.Printf("Error during extraction: %v", err)
		// Return default stock info even on error
		return []*BrowserStockInfo{stockInfo}, nil
	}
	
	log.Printf("Stock check result: %s - %s", stockInfo.Status, stockInfo.PickupQuote)
	return []*BrowserStockInfo{stockInfo}, nil
}

// FulfillmentResponse represents the API response structure
type FulfillmentResponse struct {
	Head struct {
		Status string `json:"status"`
		Data   interface{} `json:"data"`
	} `json:"head"`
	Body struct {
		Content struct {
			PickupMessage struct {
				Stores []struct {
					StoreNumber      string `json:"storeNumber"`
					StoreName        string `json:"storeName"`
					PickupQuote      string `json:"pickupQuote"`
					StoreEnabled     bool   `json:"storeEnabled"`
					PartsAvailability map[string]struct {
						StorePickEligible bool   `json:"storePickEligible"`
						PickupDisplay     string `json:"pickupDisplay"`
						PickupSearchQuote string `json:"pickupSearchQuote"`
					} `json:"partsAvailability"`
					PickupDisplay string `json:"pickupDisplay"`
				} `json:"stores"`
			} `json:"pickupMessage"`
		} `json:"content"`
	} `json:"body"`
}

// ExtractStockInfoWithProduct extracts stock info using fulfillment API with product info
func (pe *PageExtractor) ExtractStockInfoWithProduct(product Product, storeCode string) (*BrowserStockInfo, error) {
	// All iPhone 17 Pro and Pro Max use the same product page
	productPagePath := "iphone-17-pro"
	
	return pe.ExtractStockInfoWithAPI(product.Code, storeCode, productPagePath)
}

// refreshCookiesIfNeeded checks if cookies need to be refreshed
func (pe *PageExtractor) refreshCookiesIfNeeded() bool {
	// Refresh cookies if:
	// 1. No cookies cached
	// 2. Cookies are older than 5 minutes
	// 3. Session not established
	if pe.cookieCache == nil || 
	   time.Since(pe.lastCookieUpdate) > 5*time.Minute ||
	   !pe.sessionEstablished {
		return true
	}
	return false
}

// getCookies gets cookies from browser, using cache if available
func (pe *PageExtractor) getCookies(ctx context.Context) ([]*network.Cookie, error) {
	// Check if we need to refresh cookies
	if pe.refreshCookiesIfNeeded() {
		log.Println("Refreshing browser cookies...")
		var cookies []*network.Cookie
		err := chromedp.Run(ctx,
			chromedp.ActionFunc(func(ctx context.Context) error {
				var err error
				cookies, err = network.GetCookies().Do(ctx)
				if err != nil {
					return fmt.Errorf("failed to get cookies: %w", err)
				}
				return nil
			}),
		)
		if err != nil {
			return nil, err
		}
		pe.cookieCache = cookies
		pe.lastCookieUpdate = time.Now()
		log.Printf("Got %d cookies from browser", len(cookies))
	} else {
		log.Printf("Using cached cookies (%d cookies, age: %v)", 
			len(pe.cookieCache), time.Since(pe.lastCookieUpdate))
	}
	return pe.cookieCache, nil
}

// makeAPIRequest makes HTTP request with cookies
func (pe *PageExtractor) makeAPIRequest(fulfillmentURL string, cookies []*network.Cookie) (string, int, error) {
	req, err := http.NewRequest("GET", fulfillmentURL, nil)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create request: %w", err)
	}
	
	// Set headers
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Referer", "https://www.apple.com.cn/shop/buy-iphone/iphone-17-pro")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	
	// Add cookies
	for _, cookie := range cookies {
		req.AddCookie(&http.Cookie{
			Name:  cookie.Name,
			Value: cookie.Value,
		})
	}
	
	// Send request
	resp, err := pe.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", resp.StatusCode, fmt.Errorf("failed to read response: %w", err)
	}
	
	return string(body), resp.StatusCode, nil
}

// ExtractStockInfoDirect directly requests fulfillment API without visiting product page
// This should be used when session is already established
func (pe *PageExtractor) ExtractStockInfoDirect(productCode string, storeCode string) (*BrowserStockInfo, error) {
	log.Printf("Direct stock check - Product: %s, Store: %s", productCode, storeCode)
	
	stockInfo := &BrowserStockInfo{
		ProductCode: productCode,
		StoreCode:   storeCode,
		CheckTime:   time.Now(),
		IsAvailable: false,
		Status:      "unknown",
	}
	
	startTime := time.Now()
	
	// Create context with timeout
	ctx, cancel := context.WithTimeout(pe.browserCtx, 15*time.Second)
	defer cancel()
	
	// Build fulfillment URL
	fulfillmentURL := fmt.Sprintf("https://www.apple.com.cn/shop/fulfillment-messages?fae=true&mts.0=regular&mts.1=compact&parts.0=%s&pl=true&store=%s", productCode, storeCode)
	log.Printf("Fetching fulfillment data directly: %s", fulfillmentURL)
	
	var responseBody string
	var statusCode int
	var gotResponse bool
	
	// Get cookies (will use cache if available)
	cookies, err := pe.getCookies(ctx)
	if err != nil {
		log.Printf("Error getting cookies: %v", err)
		stockInfo.Error = fmt.Sprintf("cookie error: %v", err)
		return stockInfo, err
	}
	
	// Make API request
	responseBody, statusCode, err = pe.makeAPIRequest(fulfillmentURL, cookies)
	if err != nil {
		log.Printf("Error making API request: %v", err)
		stockInfo.Error = fmt.Sprintf("request error: %v", err)
		return stockInfo, err
	}
	
	log.Printf("Response status: %d", statusCode)
	
	// Handle 541 error - invalidate cache and optionally retry
	if statusCode == 541 {
		log.Println("Got 541 error - invalidating cookie cache")
		pe.cookieCache = nil
		pe.sessionEstablished = false
		pe.lastCookieUpdate = time.Time{}
		
		// Return error to trigger session re-establishment
		stockInfo.Has541Error = true
		stockInfo.Status = "session_expired"
		stockInfo.Error = "Session expired (541)"
		return stockInfo, fmt.Errorf("session expired (541)")
	}
	
	gotResponse = true
	
	if err != nil {
		log.Printf("Error fetching fulfillment data: %v", err)
		stockInfo.Error = err.Error()
		return stockInfo, err
	}
	
	if !gotResponse || responseBody == "" {
		log.Println("No response received from fulfillment API")
		stockInfo.Error = "No response from API"
		stockInfo.Status = "error"
		return stockInfo, nil
	}
	
	log.Printf("Got fulfillment response, length: %d chars", len(responseBody))
	
	// Check if it's a 404 - if so, session expired
	if strings.Contains(responseBody, "Page Not Found") || strings.Contains(responseBody, "404") {
		log.Println("Session expired (404) - need to re-establish")
		stockInfo.Status = "session_expired"
		stockInfo.Error = "Session expired"
		return stockInfo, fmt.Errorf("session expired")
	}
	
	// Parse the response
	pe.parseStockResponse(responseBody, stockInfo, productCode, storeCode)
	
	stockInfo.ResponseTime = int64(time.Since(startTime).Milliseconds())
	log.Printf("Direct check completed in %dms - Status: %s, Available: %v", 
		stockInfo.ResponseTime, stockInfo.Status, stockInfo.IsAvailable)
	
	return stockInfo, nil
}

// parseStockResponse parses the fulfillment API response
func (pe *PageExtractor) parseStockResponse(responseBody string, stockInfo *BrowserStockInfo, productCode, storeCode string) {
	// Try to parse as JSON
	var fulfillmentResp FulfillmentResponse
	if err := json.Unmarshal([]byte(responseBody), &fulfillmentResp); err != nil {
		log.Printf("Failed to parse JSON response: %v", err)
		// Fall back to text analysis
		if strings.Contains(responseBody, "pickupMessage") {
			if strings.Contains(responseBody, "今") || strings.Contains(responseBody, "可取") {
				stockInfo.IsAvailable = true
				stockInfo.Status = "available"
				stockInfo.PickupQuote = "今天可取货"
				log.Println("Stock available for pickup!")
			} else if strings.Contains(responseBody, "暂无") || strings.Contains(responseBody, "不可") {
				stockInfo.Status = "unavailable"
				stockInfo.PickupQuote = "暂无货"
				log.Println("Stock not available")
			}
		}
	} else {
		// Successfully parsed JSON
		log.Printf("Successfully parsed fulfillment response")
		
		// Check for store-specific availability
		if fulfillmentResp.Body.Content.PickupMessage.Stores != nil {
			for _, store := range fulfillmentResp.Body.Content.PickupMessage.Stores {
				if store.StoreNumber == storeCode {
					// Check if product is available in this store
					if partInfo, exists := store.PartsAvailability[productCode]; exists {
						stockInfo.PickupQuote = partInfo.PickupSearchQuote
						// Check both StorePickEligible and the actual quote text
						if partInfo.StorePickEligible && 
						   !strings.Contains(partInfo.PickupSearchQuote, "暂无供应") && 
						   !strings.Contains(partInfo.PickupSearchQuote, "不可") {
							stockInfo.IsAvailable = true
							stockInfo.Status = "available"
							log.Printf("Store %s: Available - %s", storeCode, partInfo.PickupSearchQuote)
						} else {
							stockInfo.IsAvailable = false
							stockInfo.Status = "unavailable"
							log.Printf("Store %s: Unavailable - %s", storeCode, partInfo.PickupSearchQuote)
						}
					} else {
						// Product not found in parts availability
						stockInfo.Status = "unavailable"
						stockInfo.PickupQuote = "产品信息未找到"
						log.Printf("Store %s: Product %s not found in availability", storeCode, productCode)
					}
					break
				}
			}
			
			// If store not found in response, check first store
			if stockInfo.Status == "unknown" && len(fulfillmentResp.Body.Content.PickupMessage.Stores) > 0 {
				firstStore := fulfillmentResp.Body.Content.PickupMessage.Stores[0]
				if partInfo, exists := firstStore.PartsAvailability[productCode]; exists {
					stockInfo.PickupQuote = partInfo.PickupSearchQuote
					if partInfo.StorePickEligible && 
					   !strings.Contains(partInfo.PickupSearchQuote, "暂无供应") && 
					   !strings.Contains(partInfo.PickupSearchQuote, "不可") {
						stockInfo.IsAvailable = true
						stockInfo.Status = "available"
					} else {
						stockInfo.IsAvailable = false
						stockInfo.Status = "unavailable"
					}
					log.Printf("Using first store status: %s", stockInfo.Status)
				}
			}
		}
	}
	
	// Log if status still unknown
	if stockInfo.Status == "unknown" {
		log.Printf("Unknown status. Response snippet: %s", responseBody[:min(500, len(responseBody))])
		stockInfo.PickupQuote = "状态未知"
	}
}

// EstablishSession establishes a session by visiting the product page
func (pe *PageExtractor) EstablishSession() error {
	if pe.sessionEstablished {
		log.Println("Session already established, skipping...")
		return nil
	}
	
	log.Println("Establishing new session...")
	ctx, cancel := context.WithTimeout(pe.browserCtx, 30*time.Second)
	defer cancel()
	
	// Navigate to iPhone 17 Pro page to establish session
	productURL := "https://www.apple.com.cn/shop/buy-iphone/iphone-17-pro"
	log.Printf("Navigating to product page: %s", productURL)
	
	err := chromedp.Run(ctx,
		chromedp.Navigate(productURL),
		chromedp.Sleep(5*time.Second), // Wait for page load and session establishment
		chromedp.ActionFunc(func(ctx context.Context) error {
			log.Println("Product page loaded, session established")
			return nil
		}),
	)
	
	if err != nil {
		return fmt.Errorf("failed to establish session: %w", err)
	}
	
	pe.sessionEstablished = true
	log.Println("Session successfully established")
	return nil
}

// ExtractStockInfoWithAPI extracts stock info using fulfillment API
func (pe *PageExtractor) ExtractStockInfoWithAPI(productCode string, storeCode string, productPagePath ...string) (*BrowserStockInfo, error) {
	log.Printf("Starting stock extraction with API - Product: %s, Store: %s", productCode, storeCode)
	
	stockInfo := &BrowserStockInfo{
		ProductCode: productCode,
		StoreCode:   storeCode,
		CheckTime:   time.Now(),
		IsAvailable: false,
		Status:      "unknown",
	}
	
	startTime := time.Now()
	
	// Create context with timeout
	ctx, cancel := context.WithTimeout(pe.browserCtx, 30*time.Second)
	defer cancel()
	
	// Only navigate to product page if session not established
	if !pe.sessionEstablished {
		// Step 1: Navigate to iPhone 17 Pro page (both Pro and Pro Max use the same page)
		pagePath := "iphone-17-pro"
		if len(productPagePath) > 0 && productPagePath[0] != "" {
			pagePath = productPagePath[0]
		}
		
		productURL := fmt.Sprintf("https://www.apple.com.cn/shop/buy-iphone/%s", pagePath)
		
		log.Printf("Step 1: Navigating to product page: %s", productURL)
		
		err := chromedp.Run(ctx,
			chromedp.Navigate(productURL),
			chromedp.Sleep(5*time.Second), // Longer wait for page load and session establishment
			chromedp.ActionFunc(func(ctx context.Context) error {
				log.Println("Product page loaded, waiting for session to establish...")
				return nil
			}),
			chromedp.Sleep(2*time.Second), // Additional wait for session
		)
		
		if err != nil {
			log.Printf("Error navigating to product page: %v", err)
			stockInfo.Error = err.Error()
			return stockInfo, err
		}
		
		pe.sessionEstablished = true
		log.Println("Session established")
	}
	
	// Build fulfillment URL
	fulfillmentURL := fmt.Sprintf("https://www.apple.com.cn/shop/fulfillment-messages?fae=true&mts.0=regular&mts.1=compact&parts.0=%s&pl=true&store=%s", productCode, storeCode)
	log.Printf("Fetching fulfillment data: %s", fulfillmentURL)
	
	var responseBody string
	var statusCode int
	var gotResponse bool
	
	// Get cookies (will use cache if available)
	cookies, err := pe.getCookies(ctx)
	if err != nil {
		log.Printf("Error getting cookies: %v", err)
		stockInfo.Error = fmt.Sprintf("cookie error: %v", err)
		return stockInfo, err
	}
	
	// Make API request
	responseBody, statusCode, err = pe.makeAPIRequest(fulfillmentURL, cookies)
	if err != nil {
		log.Printf("Error making API request: %v", err)
		stockInfo.Error = fmt.Sprintf("request error: %v", err)
		return stockInfo, err
	}
	
	log.Printf("Response status: %d", statusCode)
	
	// Handle 541 error - invalidate cache but continue since we just established session
	if statusCode == 541 {
		log.Println("Got 541 error even after establishing session, will retry...")
		pe.cookieCache = nil
		pe.lastCookieUpdate = time.Time{}
		
		// Retry once with fresh cookies
		cookies, err = pe.getCookies(ctx)
		if err == nil {
			responseBody, statusCode, err = pe.makeAPIRequest(fulfillmentURL, cookies)
			if err == nil && statusCode != 541 {
				log.Printf("Retry successful, status: %d", statusCode)
			}
		}
	}
	
	gotResponse = true
	
	if err != nil {
		log.Printf("Error during fetch: %v", err)
		stockInfo.Error = fmt.Sprintf("fetch error: %v", err)
		return stockInfo, err
	}
	
	if gotResponse && responseBody != "" {
		log.Printf("Got fulfillment response, length: %d chars", len(responseBody))
	} else {
		log.Printf("No response received - gotResponse: %v, responseBody length: %d", gotResponse, len(responseBody))
	}
	
	if err != nil {
		log.Printf("Error fetching fulfillment data: %v", err)
		stockInfo.Error = err.Error()
		return stockInfo, err
	}
	
	if !gotResponse || responseBody == "" {
		log.Println("No response received from fulfillment API")
		stockInfo.Error = "No response from API"
		stockInfo.Status = "error"
		return stockInfo, nil
	}
	
	log.Printf("Got fulfillment response, length: %d chars", len(responseBody))
	
	// Check if it's a 404 or error page - retry once if needed
	if strings.Contains(responseBody, "Page Not Found") || strings.Contains(responseBody, "404") {
		log.Println("Got 404 error - retrying once...")
		
		// Wait a bit and retry
		time.Sleep(3 * time.Second)
		
		// Retry the fulfillment request
		err = chromedp.Run(ctx,
			chromedp.Navigate(fulfillmentURL),
			chromedp.Sleep(2*time.Second),
			chromedp.ActionFunc(func(ctx context.Context) error {
				var content string
				if err := chromedp.Evaluate(`document.body ? (document.body.innerText || document.body.textContent || document.documentElement.innerText) : ''`, &content).Do(ctx); err != nil {
					return err
				}
				if content != "" {
					responseBody = content
				}
				return nil
			}),
		)
		
		if err != nil {
			log.Printf("Retry failed: %v", err)
			stockInfo.Status = "error"
			stockInfo.Error = "Retry failed"
			return stockInfo, err
		}
		
		log.Printf("Retry got response, length: %d chars", len(responseBody))
		
		// Check again if still 404
		if strings.Contains(responseBody, "Page Not Found") || strings.Contains(responseBody, "404") {
			log.Println("Still got 404 after retry")
			stockInfo.Status = "error"
			stockInfo.PickupQuote = "需要刷新会话"
			stockInfo.Error = "404 - Session invalid after retry"
			return stockInfo, fmt.Errorf("session invalid after retry")
		}
	}
	
	// Parse the response using shared method
	pe.parseStockResponse(responseBody, stockInfo, productCode, storeCode)
	
	stockInfo.ResponseTime = int64(time.Since(startTime).Milliseconds())
	log.Printf("Stock check completed in %dms - Status: %s, Available: %v", 
		stockInfo.ResponseTime, stockInfo.Status, stockInfo.IsAvailable)
	
	return stockInfo, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}