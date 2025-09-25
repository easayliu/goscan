# Linux Browser Monitor Setup Guide

## Chrome Installation & Path Support

The browser monitor now automatically detects Chrome/Chromium on Linux systems. It checks the following paths:

### Supported Chrome Paths (Auto-detected)
- `/usr/bin/google-chrome` (Primary)
- `/usr/bin/google-chrome-stable`
- `/usr/bin/google-chrome-beta`
- `/usr/bin/chromium`
- `/usr/bin/chromium-browser`
- `/snap/bin/chromium`
- `/opt/google/chrome/google-chrome`

## Quick Start

### 1. Install Chrome on Linux

#### Ubuntu/Debian:
```bash
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
sudo sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list'
sudo apt update
sudo apt install google-chrome-stable
```

#### CentOS/RHEL/Fedora:
```bash
sudo dnf install -y fedora-workstation-repositories
sudo dnf config-manager --set-enabled google-chrome
sudo dnf install google-chrome-stable
```

#### Arch Linux:
```bash
yay -S google-chrome
# or
sudo pacman -S chromium
```

### 2. Install Required Dependencies

```bash
# For Debian/Ubuntu
sudo apt install -y fonts-noto-cjk libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libxss1 libxtst6 libnss3 libnspr4 libatk-bridge2.0-0 libdrm2 libxkbcommon0 libgbm1

# For CentOS/RHEL
sudo yum install -y liberation-fonts alsa-lib atk cups-libs dbus-glib libXScrnSaver libXrandr libXcomposite libXdamage
```

### 3. Build and Run

```bash
# Build the monitor
go build -o browser_monitor cmd/browser_apple_monitor/main.go

# Run in headless mode (recommended for servers)
./browser_monitor -headless=true

# Run with GUI (for debugging)
./browser_monitor -headless=false
```

## Cross-Platform Compatibility

The browser monitor now works seamlessly across:
- **macOS**: Auto-detects Chrome in `/Applications/`
- **Linux**: Auto-detects Chrome/Chromium in standard paths
- **Windows**: Auto-detects Chrome in Program Files

## Running on Headless Servers

For servers without display:

```bash
# Install Xvfb for virtual display
sudo apt install xvfb

# Run with virtual display
xvfb-run -a ./browser_monitor -headless=true

# Or set display environment
export DISPLAY=:99
Xvfb :99 -screen 0 1920x1080x24 &
./browser_monitor -headless=true
```

## Systemd Service Example

Create `/etc/systemd/system/apple-browser-monitor.service`:

```ini
[Unit]
Description=Apple Browser Monitor
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/goscan
ExecStart=/path/to/goscan/browser_monitor -headless=true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable apple-browser-monitor
sudo systemctl start apple-browser-monitor
```

## Docker Deployment

```dockerfile
FROM golang:1.21 AS builder
WORKDIR /app
COPY . .
RUN go build -o browser_monitor cmd/browser_apple_monitor/main.go

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y \
    wget gnupg2 ca-certificates fonts-noto-cjk \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/browser_monitor .
COPY config/ ./config/

CMD ["./browser_monitor", "-headless=true"]
```

## Troubleshooting

### Chrome Not Found
If Chrome is installed but not detected:
```bash
# Check Chrome location
which google-chrome

# Create symlink if in non-standard location
sudo ln -s /actual/path/to/chrome /usr/bin/google-chrome
```

### Missing Libraries
```bash
# Check Chrome dependencies
ldd /usr/bin/google-chrome | grep "not found"

# Install missing libraries based on output
```

### Chinese Font Issues
```bash
# Install CJK fonts
sudo apt install fonts-noto-cjk
# or
sudo yum install google-noto-sans-cjk-fonts
```

## Features

✅ **Auto Chrome Detection** - Automatically finds Chrome/Chromium  
✅ **Cross-Platform** - Works on Linux, macOS, and Windows  
✅ **Headless Support** - Run without display on servers  
✅ **541 Error Recovery** - Auto-handles authentication errors  
✅ **Smart Waiting** - Intelligent page load detection  

## Configuration

The monitor respects the following environment variables:

```bash
# Custom Chrome path (if auto-detection fails)
export CHROME_PATH="/custom/path/to/chrome"

# Proxy settings
export HTTP_PROXY="http://proxy.example.com:8080"
export HTTPS_PROXY="http://proxy.example.com:8080"
```