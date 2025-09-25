package apple

import (
	"context"
	"fmt"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/chromedp"
)

// WaitStrategy provides intelligent wait mechanisms for browser automation
type WaitStrategy struct {
	DefaultTimeout time.Duration
	MaxRetries     int
}

// NewWaitStrategy creates a new wait strategy with defaults
func NewWaitStrategy() *WaitStrategy {
	return &WaitStrategy{
		DefaultTimeout: 10 * time.Second,
		MaxRetries:     3,
	}
}

// WaitForElement waits for an element to be present and visible
func (ws *WaitStrategy) WaitForElement(selector string, opts ...chromedp.QueryOption) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		// Create timeout context
		timeoutCtx, cancel := context.WithTimeout(ctx, ws.DefaultTimeout)
		defer cancel()
		
		// Wait for element to be present
		if err := chromedp.WaitReady(selector, opts...).Do(timeoutCtx); err != nil {
			return fmt.Errorf("element not ready within %v: %s", ws.DefaultTimeout, selector)
		}
		
		// Check if element is visible
		var visible bool
		if err := chromedp.Evaluate(fmt.Sprintf(`
			(function() {
				const el = document.querySelector('%s');
				if (!el) return false;
				const rect = el.getBoundingClientRect();
				return rect.width > 0 && rect.height > 0 && 
					   rect.top < window.innerHeight && rect.bottom > 0;
			})()
		`, selector), &visible).Do(ctx); err != nil {
			return err
		}
		
		if !visible {
			return fmt.Errorf("element not visible: %s", selector)
		}
		
		return nil
	})
}

// WaitForText waits for specific text to appear on the page
func (ws *WaitStrategy) WaitForText(text string, timeout ...time.Duration) chromedp.Action {
	t := ws.DefaultTimeout
	if len(timeout) > 0 {
		t = timeout[0]
	}
	
	return chromedp.ActionFunc(func(ctx context.Context) error {
		timeoutCtx, cancel := context.WithTimeout(ctx, t)
		defer cancel()
		
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-timeoutCtx.Done():
				return fmt.Errorf("text not found within %v: %s", t, text)
			case <-ticker.C:
				var pageText string
				if err := chromedp.Text(`body`, &pageText, chromedp.ByQuery).Do(ctx); err == nil {
					if contains(pageText, text) {
						return nil
					}
				}
			}
		}
	})
}

// WaitForAnyElement waits for any of the given selectors to appear
func (ws *WaitStrategy) WaitForAnyElement(selectors []string) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		timeoutCtx, cancel := context.WithTimeout(ctx, ws.DefaultTimeout)
		defer cancel()
		
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-timeoutCtx.Done():
				return fmt.Errorf("none of the elements found within %v", ws.DefaultTimeout)
			case <-ticker.C:
				for _, selector := range selectors {
					var nodes []*cdp.Node
					if err := chromedp.Nodes(selector, &nodes, chromedp.ByQuery).Do(ctx); err == nil && len(nodes) > 0 {
						return nil
					}
				}
			}
		}
	})
}

// WaitForNetworkIdle waits for network to be idle (no requests for specified duration)
func (ws *WaitStrategy) WaitForNetworkIdle(idleDuration time.Duration) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		// Simple implementation: wait for JavaScript to report document ready and then add delay
		if err := chromedp.WaitReady(`body`, chromedp.ByQuery).Do(ctx); err != nil {
			return err
		}
		
		// Check document ready state
		var readyState string
		if err := chromedp.Evaluate(`document.readyState`, &readyState).Do(ctx); err != nil {
			return err
		}
		
		if readyState != "complete" {
			// Wait for complete state
			if err := chromedp.Poll(`document.readyState === "complete"`, nil).Do(ctx); err != nil {
				return err
			}
		}
		
		// Additional wait for network idle
		time.Sleep(idleDuration)
		return nil
	})
}

// WaitAndClick waits for element and clicks it with retry logic
func (ws *WaitStrategy) WaitAndClick(selector string, opts ...chromedp.QueryOption) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		// First wait for element
		if err := ws.WaitForElement(selector, opts...).Do(ctx); err != nil {
			return err
		}
		
		// Try to click with retries
		var lastErr error
		for i := 0; i < ws.MaxRetries; i++ {
			if err := chromedp.Click(selector, opts...).Do(ctx); err == nil {
				return nil
			} else {
				lastErr = err
				time.Sleep(500 * time.Millisecond)
				
				// Scroll element into view
				chromedp.Evaluate(fmt.Sprintf(`
					document.querySelector('%s')?.scrollIntoView({behavior: 'smooth', block: 'center'})
				`, selector), nil).Do(ctx)
			}
		}
		
		return fmt.Errorf("failed to click after %d retries: %w", ws.MaxRetries, lastErr)
	})
}

// WaitForPageLoad waits for page to be fully loaded
func (ws *WaitStrategy) WaitForPageLoad() chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		// Wait for body to be present
		if err := chromedp.WaitReady(`body`, chromedp.ByQuery).Do(ctx); err != nil {
			return err
		}
		
		// Wait for document to be ready
		return chromedp.Poll(`document.readyState === "complete"`, nil).Do(ctx)
	})
}

// SmartDelay provides intelligent delay based on context
func (ws *WaitStrategy) SmartDelay(min, max time.Duration) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		// Check if page is still loading
		var isLoading bool
		chromedp.Evaluate(`document.readyState !== "complete"`, &isLoading).Do(ctx)
		
		if isLoading {
			// If still loading, wait longer
			time.Sleep(max)
		} else {
			// If loaded, use minimum delay
			time.Sleep(min)
		}
		return nil
	})
}

// RetryAction retries an action with exponential backoff
func (ws *WaitStrategy) RetryAction(action chromedp.Action) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		backoff := 1 * time.Second
		
		for i := 0; i < ws.MaxRetries; i++ {
			if err := action.Do(ctx); err == nil {
				return nil
			}
			
			if i < ws.MaxRetries-1 {
				time.Sleep(backoff)
				backoff *= 2 // Exponential backoff
			}
		}
		
		// Final attempt
		return action.Do(ctx)
	})
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr || 
		   len(s) > len(substr) && containsHelper(s[1:], substr)
}

func containsHelper(s, substr string) bool {
	if len(s) < len(substr) {
		return false
	}
	if s[:len(substr)] == substr {
		return true
	}
	return containsHelper(s[1:], substr)
}