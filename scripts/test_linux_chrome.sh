#!/bin/bash

# Test script for Linux Chrome compatibility
# This script tests the browser monitor on Linux systems

set -e

echo "🐧 Linux Chrome Compatibility Test"
echo "=================================="

# Check OS
echo "OS Information:"
uname -a
echo ""

# Check if Chrome is installed
echo "Checking for Chrome/Chromium..."
if command -v google-chrome &> /dev/null; then
    echo "✅ Google Chrome found: $(which google-chrome)"
    google-chrome --version
elif command -v chromium &> /dev/null; then
    echo "✅ Chromium found: $(which chromium)"
    chromium --version
elif command -v chromium-browser &> /dev/null; then
    echo "✅ Chromium Browser found: $(which chromium-browser)"
    chromium-browser --version
else
    echo "❌ No Chrome/Chromium found!"
    echo ""
    echo "Please install Chrome with one of these commands:"
    echo ""
    echo "For Ubuntu/Debian:"
    echo "  sudo apt update && sudo apt install -y google-chrome-stable"
    echo "  or"
    echo "  sudo apt install -y chromium-browser"
    echo ""
    echo "For CentOS/RHEL/Fedora:"
    echo "  sudo dnf install -y google-chrome-stable"
    echo "  or"
    echo "  sudo dnf install -y chromium"
    exit 1
fi
echo ""

# Check display (for non-headless mode)
echo "Display Configuration:"
if [ -z "$DISPLAY" ]; then
    echo "⚠️  No DISPLAY variable set (headless mode required)"
    echo "   To run with GUI, set: export DISPLAY=:0"
else
    echo "✅ DISPLAY is set to: $DISPLAY"
fi
echo ""

# Check required libraries
echo "Checking required libraries..."
missing_libs=0

# Common required libraries
libs=(
    "libx11-xcb.so"
    "libxcomposite.so"
    "libxdamage.so"
    "libxrandr.so"
    "libnss3.so"
    "libnspr4.so"
    "libatk-bridge-2.0.so"
    "libdrm.so"
    "libxkbcommon.so"
    "libgbm.so"
)

for lib in "${libs[@]}"; do
    if ldconfig -p | grep -q "$lib"; then
        echo "✅ $lib found"
    else
        echo "❌ $lib missing"
        missing_libs=$((missing_libs + 1))
    fi
done

if [ $missing_libs -gt 0 ]; then
    echo ""
    echo "⚠️  Some libraries are missing. Install them with:"
    echo "  sudo apt install -y libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libnss3 libnspr4 libatk-bridge2.0-0 libdrm2 libxkbcommon0 libgbm1"
fi
echo ""

# Build the test program
echo "Building test program..."
cd /Users/easayliu/Documents/go/goscan

if go build -o test_browser_linux cmd/test_chrome_detector/main.go; then
    echo "✅ Build successful"
else
    echo "❌ Build failed"
    exit 1
fi
echo ""

# Run Chrome detector test
echo "Running Chrome path detection test..."
./test_browser_linux
echo ""

# Test Chrome directly
echo "Testing Chrome/Chromium directly..."
if command -v google-chrome &> /dev/null; then
    CHROME_CMD="google-chrome"
elif command -v chromium &> /dev/null; then
    CHROME_CMD="chromium"
else
    CHROME_CMD="chromium-browser"
fi

echo "Testing headless mode..."
if timeout 5 $CHROME_CMD \
    --headless \
    --no-sandbox \
    --disable-dev-shm-usage \
    --disable-gpu \
    --dump-dom \
    https://www.apple.com.cn > /dev/null 2>&1; then
    echo "✅ Chrome headless mode works"
else
    echo "❌ Chrome headless mode failed"
    echo "   Trying with more flags..."
    
    if timeout 5 $CHROME_CMD \
        --headless \
        --no-sandbox \
        --disable-setuid-sandbox \
        --disable-dev-shm-usage \
        --disable-gpu \
        --disable-software-rasterizer \
        --single-process \
        --dump-dom \
        https://www.apple.com.cn > /dev/null 2>&1; then
        echo "✅ Chrome works with additional flags"
    else
        echo "❌ Chrome still failing, check error messages above"
    fi
fi
echo ""

# Test the actual monitor (quick test)
echo "Building browser monitor..."
if go build -o browser_monitor_test cmd/browser_apple_monitor/main.go; then
    echo "✅ Browser monitor built successfully"
    
    echo ""
    echo "Running quick browser monitor test (5 seconds)..."
    timeout 5 ./browser_monitor_test -headless=true -once || true
else
    echo "❌ Failed to build browser monitor"
fi

echo ""
echo "🎉 Test complete!"
echo ""
echo "Recommendations:"
echo "1. Always use -headless=true on servers without display"
echo "2. If running in Docker, use the provided Dockerfile"
echo "3. For debugging, use -headless=false with Xvfb"