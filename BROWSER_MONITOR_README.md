# Browser Monitor - Apple Store Stock Monitoring

## Overview

This is a completely refactored implementation of the Apple Store stock monitoring system that uses browser automation instead of API calls to extract stock information directly from web pages.

**✅ Full Linux Support**: The monitor now includes complete Linux compatibility with automatic Chrome detection and OS-specific optimizations.

## Key Features

### 1. Direct DOM Extraction
- No dependency on API authentication or cookies
- Real-time data extraction from actual web pages
- More resilient to API changes

### 2. Smart Wait Strategies
- Intelligent waiting mechanisms replace fixed delays
- Adaptive timeouts based on page load status
- Reduced overall monitoring time

### 3. Automatic Error Recovery
- 541 error detection and automatic recovery
- Cookie refresh when needed
- Browser restart on critical failures

### 4. Enhanced Anti-Detection
- Simulates real user behavior
- Random delays between actions
- Stealth browser configuration

## Architecture

```
pkg/apple/
├── page_extractor.go      # Core DOM extraction logic
├── browser_monitor.go     # Monitoring orchestration
├── wait_strategy.go       # Smart wait mechanisms
└── types_browser.go       # Type definitions

cmd/
├── browser_apple_monitor/ # Main monitoring program
└── test_browser_monitor/  # Test utilities
```

## Platform Support

### Linux
- **Auto Chrome Detection**: Automatically finds Chrome/Chromium in standard paths
- **OS-Specific Optimization**: Special flags for Linux containers and servers
- **Headless Mode**: Optimized for server environments without display
- **Chrome Paths Supported**:
  - `/usr/bin/google-chrome`
  - `/usr/bin/google-chrome-stable`
  - `/usr/bin/chromium`
  - `/usr/bin/chromium-browser`
  - `/snap/bin/chromium`

### macOS
- **Auto Detection**: Finds Chrome in `/Applications/`
- **Native Support**: Optimized for macOS environment

### Windows
- **Auto Detection**: Finds Chrome in Program Files
- **Native Support**: Windows-specific optimizations

## Usage

### Running the Browser Monitor

```bash
# Build the monitor
go build -o browser_monitor cmd/browser_apple_monitor/main.go

# Run with default configuration
./browser_monitor

# Run with custom config
./browser_monitor -config config/apple_monitor.json

# Check single product
./browser_monitor -product MG034CH/A -once
```

### Testing

```bash
# Run test script
./scripts/test_browser_monitor.sh single  # Test single extraction
./scripts/test_browser_monitor.sh extract # Test all stores
./scripts/test_browser_monitor.sh full    # Full monitor test (30s)
```

## Configuration

```json
{
  "monitoring": {
    "default_interval": 5,
    "use_browser": true
  },
  "telegram": {
    "enabled": true,
    "token": "YOUR_BOT_TOKEN",
    "chat_id": "YOUR_CHAT_ID"
  },
  "products": [
    {
      "product_code": "MG034CH/A",
      "product_name": "iPhone 17 Pro Max 256GB",
      "storage": "256GB",
      "color": "沙色钛金属",
      "enabled": true
    }
  ],
  "stores": [
    {
      "store_code": "R639",
      "store_name": "Apple 珠江新城",
      "city": "广州",
      "enabled": true
    }
  ]
}
```

## Key Components

### PageExtractor
- Handles browser automation
- Extracts stock information from DOM
- Manages browser lifecycle

### BrowserMonitor
- Orchestrates monitoring loop
- Manages multiple products/stores
- Sends notifications

### WaitStrategy
- Provides intelligent wait mechanisms
- Reduces unnecessary delays
- Improves reliability

## Error Handling

### 541 Error Recovery
When a 541 error is detected:
1. Automatically clears cookies
2. Refreshes browser session
3. Retries the extraction

### Browser Crash Recovery
If browser crashes:
1. Automatically restarts browser
2. Restores monitoring state
3. Continues monitoring

## Performance Optimizations

1. **Smart Delays**: Dynamic delays based on page state
2. **Parallel Extraction**: Support for concurrent store checks
3. **Resource Management**: Efficient browser memory usage
4. **Caching**: Reuses browser context across checks

## Advantages Over API Method

| Feature | API Method | Browser Method |
|---------|------------|----------------|
| Authentication | Requires valid cookies | No authentication needed |
| Data Freshness | API cache delays | Real-time page data |
| Error Recovery | Manual intervention | Automatic recovery |
| Anti-Detection | Easy to detect | Simulates real user |
| Maintenance | Cookie updates needed | Self-maintaining |

## Troubleshooting

### Browser Not Starting
- Ensure Chrome/Chromium is installed
- Check system resources (RAM/CPU)
- Verify network connectivity

### Extraction Failures
- Check product/store codes
- Verify Apple Store page structure hasn't changed
- Review logs for specific errors

### 541 Errors
- Monitor automatically handles these
- If persistent, check network/proxy settings
- Consider increasing delays

## Future Improvements

- [ ] Headless mode optimization
- [ ] Multi-browser instance support
- [ ] Visual debugging mode
- [ ] Performance metrics dashboard
- [ ] ML-based pattern recognition

## License

This implementation is for educational purposes only. Please respect Apple's terms of service when using this tool.