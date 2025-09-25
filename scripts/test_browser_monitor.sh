#!/bin/bash

# Test Browser Monitor Script
# This script tests the new browser-based monitoring implementation

set -e

echo "🔧 Building browser monitor test..."
cd /Users/easayliu/Documents/go/goscan

# Build the test program
go build -o test_browser_monitor cmd/test_browser_monitor/main.go

echo "✅ Build complete"
echo ""

# Run tests based on parameter
if [ "$1" = "single" ]; then
    echo "🧪 Running single extraction test..."
    ./test_browser_monitor -mode single -product "MG034CH/A" -store "R639"
elif [ "$1" = "extract" ]; then
    echo "🧪 Running page extractor test (all stores)..."
    ./test_browser_monitor -mode extract -product "MG034CH/A"
elif [ "$1" = "full" ]; then
    echo "🧪 Running full monitor test (30 seconds)..."
    ./test_browser_monitor -mode full
else
    echo "Usage: $0 [single|extract|full]"
    echo ""
    echo "  single  - Test single product/store extraction"
    echo "  extract - Test extraction for all stores"
    echo "  full    - Test full monitoring loop (30 seconds)"
    echo ""
    echo "Running default single test..."
    ./test_browser_monitor -mode single
fi

echo ""
echo "✨ Test completed"