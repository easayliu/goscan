package apple

import (
	"log"
	"os"
	"runtime"

	"github.com/chromedp/chromedp"
)

// getChromePath returns the Chrome/Chromium executable path based on the operating system
func getChromePath() string {
	// Check OS type
	switch runtime.GOOS {
	case "darwin": // macOS
		// Try different possible locations on macOS
		paths := []string{
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"/Applications/Chromium.app/Contents/MacOS/Chromium",
			"/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
		}
		for _, path := range paths {
			if _, err := os.Stat(path); err == nil {
				return path
			}
		}
		
	case "linux":
		// Try different possible locations on Linux
		paths := []string{
			"/usr/bin/google-chrome",
			"/usr/bin/google-chrome-stable",
			"/usr/bin/google-chrome-beta",
			"/usr/bin/chromium",
			"/usr/bin/chromium-browser",
			"/snap/bin/chromium",
			"/usr/local/bin/chrome",
			"/opt/google/chrome/google-chrome",
		}
		for _, path := range paths {
			if _, err := os.Stat(path); err == nil {
				return path
			}
		}
		
	case "windows":
		// Try different possible locations on Windows
		paths := []string{
			`C:\Program Files\Google\Chrome\Application\chrome.exe`,
			`C:\Program Files (x86)\Google\Chrome\Application\chrome.exe`,
			`C:\Program Files\Chromium\Application\chrome.exe`,
			`C:\Program Files (x86)\Chromium\Application\chrome.exe`,
			os.Getenv("LOCALAPPDATA") + `\Google\Chrome\Application\chrome.exe`,
			os.Getenv("PROGRAMFILES") + `\Google\Chrome\Application\chrome.exe`,
			os.Getenv("PROGRAMFILES(X86)") + `\Google\Chrome\Application\chrome.exe`,
		}
		for _, path := range paths {
			if path != "" {
				if _, err := os.Stat(path); err == nil {
					return path
				}
			}
		}
	}
	
	// Return empty string if no Chrome found (will use system default)
	return ""
}

// GetBrowserOptions returns OS-specific browser options (exported for testing)
func GetBrowserOptions(headless bool) []chromedp.ExecAllocatorOption {
	// Common options for all platforms
	opts := []chromedp.ExecAllocatorOption{
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.DisableGPU,
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("excludeSwitches", "enable-automation"),
		chromedp.Flag("useAutomationExtension", false),
		chromedp.UserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
		chromedp.WindowSize(1920, 1080),
	}
	
	// OS-specific options
	switch runtime.GOOS {
	case "linux":
		log.Println("Configuring browser for Linux")
		// Linux specific flags - more permissive for containers/servers
		opts = append(opts,
			chromedp.Flag("no-sandbox", true),
			chromedp.Flag("disable-setuid-sandbox", true),
			chromedp.Flag("disable-dev-shm-usage", true),
			chromedp.Flag("disable-accelerated-2d-canvas", true),
			chromedp.Flag("no-zygote", true),
			chromedp.Flag("single-process", false), // Don't use single-process in production
			chromedp.Flag("disable-gpu", true),
			chromedp.Flag("disable-software-rasterizer", true),
		)
		
		// For headless mode on Linux
		if headless {
			opts = append(opts,
				chromedp.Headless,
				chromedp.Flag("disable-background-networking", true),
				chromedp.Flag("disable-background-timer-throttling", true),
				chromedp.Flag("disable-backgrounding-occluded-windows", true),
				chromedp.Flag("disable-breakpad", true),
				chromedp.Flag("disable-client-side-phishing-detection", true),
				chromedp.Flag("disable-component-extensions-with-background-pages", true),
				chromedp.Flag("disable-features", "TranslateUI,BlinkGenPropertyTrees"),
				chromedp.Flag("disable-ipc-flooding-protection", true),
				chromedp.Flag("disable-popup-blocking", true),
				chromedp.Flag("disable-prompt-on-repost", true),
				chromedp.Flag("disable-renderer-backgrounding", true),
				chromedp.Flag("disable-sync", true),
				chromedp.Flag("force-color-profile", "srgb"),
				chromedp.Flag("metrics-recording-only", true),
				chromedp.Flag("safebrowsing-disable-auto-update", true),
				chromedp.Flag("enable-automation", false),
				chromedp.Flag("password-store", "basic"),
				chromedp.Flag("use-mock-keychain", true),
			)
		} else {
			opts = append(opts, chromedp.Flag("headless", false))
		}
		
	case "darwin":
		log.Println("Configuring browser for macOS")
		// macOS specific flags - less restrictive
		opts = append(opts,
			chromedp.Flag("no-sandbox", true),
			chromedp.Flag("disable-dev-shm-usage", true),
		)
		
		if headless {
			opts = append(opts, chromedp.Headless)
		} else {
			opts = append(opts, chromedp.Flag("headless", false))
		}
		
	case "windows":
		log.Println("Configuring browser for Windows")
		// Windows specific flags
		opts = append(opts,
			chromedp.Flag("no-sandbox", true),
			chromedp.Flag("disable-dev-shm-usage", true),
		)
		
		if headless {
			opts = append(opts, chromedp.Headless)
		} else {
			opts = append(opts, chromedp.Flag("headless", false))
		}
		
	default:
		log.Printf("Unknown OS: %s, using default options", runtime.GOOS)
		opts = append(opts,
			chromedp.Flag("no-sandbox", true),
			chromedp.Flag("disable-dev-shm-usage", true),
		)
		
		if headless {
			opts = append(opts, chromedp.Headless)
		} else {
			opts = append(opts, chromedp.Flag("headless", false))
		}
	}
	
	return opts
}

// GetChromePathWithFallback returns Chrome path with optional fallback path
func GetChromePathWithFallback(fallback string) string {
	// First try to auto-detect
	path := getChromePath()
	if path != "" {
		return path
	}
	
	// If no path found and fallback provided, check if fallback exists
	if fallback != "" {
		if _, err := os.Stat(fallback); err == nil {
			return fallback
		}
	}
	
	return ""
}