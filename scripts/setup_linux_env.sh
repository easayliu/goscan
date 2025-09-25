#!/bin/bash

# Setup script for Linux environment
# This script configures the environment for running the browser monitor on Linux

echo "🐧 Linux Environment Setup for Browser Monitor"
echo "=============================================="
echo ""

# Function to detect Linux distribution
detect_distro() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        echo $ID
    elif [ -f /etc/redhat-release ]; then
        echo "rhel"
    elif [ -f /etc/debian_version ]; then
        echo "debian"
    else
        echo "unknown"
    fi
}

DISTRO=$(detect_distro)
echo "Detected Linux distribution: $DISTRO"
echo ""

# Install Chrome based on distribution
install_chrome() {
    echo "📦 Installing Google Chrome..."
    
    case $DISTRO in
        ubuntu|debian)
            echo "Installing for Ubuntu/Debian..."
            wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
            sudo sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
            sudo apt-get update
            sudo apt-get install -y google-chrome-stable
            ;;
            
        fedora|centos|rhel)
            echo "Installing for Fedora/CentOS/RHEL..."
            sudo dnf install -y fedora-workstation-repositories
            sudo dnf config-manager --set-enabled google-chrome
            sudo dnf install -y google-chrome-stable
            ;;
            
        arch)
            echo "Installing for Arch Linux..."
            yay -S google-chrome
            ;;
            
        *)
            echo "⚠️  Unknown distribution. Please install Chrome manually:"
            echo "   Visit: https://www.google.com/chrome/"
            return 1
            ;;
    esac
}

# Install dependencies
install_dependencies() {
    echo "📦 Installing required dependencies..."
    
    case $DISTRO in
        ubuntu|debian)
            sudo apt-get update
            sudo apt-get install -y \
                fonts-liberation \
                fonts-noto-cjk \
                fonts-noto-cjk-extra \
                libasound2 \
                libatk-bridge2.0-0 \
                libatk1.0-0 \
                libatspi2.0-0 \
                libcups2 \
                libdbus-1-3 \
                libdrm2 \
                libgbm1 \
                libgtk-3-0 \
                libnspr4 \
                libnss3 \
                libx11-xcb1 \
                libxcomposite1 \
                libxdamage1 \
                libxfixes3 \
                libxkbcommon0 \
                libxrandr2 \
                xdg-utils \
                xvfb
            ;;
            
        fedora|centos|rhel)
            sudo dnf install -y \
                liberation-fonts \
                google-noto-sans-cjk-fonts \
                google-noto-serif-cjk-fonts \
                alsa-lib \
                atk \
                cups-libs \
                dbus-libs \
                gtk3 \
                libXcomposite \
                libXdamage \
                libXrandr \
                libxkbcommon \
                nspr \
                nss \
                xdg-utils \
                xorg-x11-server-Xvfb
            ;;
            
        arch)
            sudo pacman -S --needed \
                noto-fonts-cjk \
                alsa-lib \
                atk \
                cups \
                dbus \
                gtk3 \
                libxcomposite \
                libxdamage \
                libxrandr \
                libxkbcommon \
                nspr \
                nss \
                xdg-utils \
                xorg-server-xvfb
            ;;
            
        *)
            echo "⚠️  Unknown distribution. Please install dependencies manually."
            return 1
            ;;
    esac
}

# Setup virtual display for headless servers
setup_virtual_display() {
    echo "🖥️  Setting up virtual display..."
    
    # Check if X is already running
    if [ -n "$DISPLAY" ]; then
        echo "Display already set: $DISPLAY"
        return 0
    fi
    
    # Check if Xvfb is installed
    if ! command -v Xvfb &> /dev/null; then
        echo "Installing Xvfb..."
        case $DISTRO in
            ubuntu|debian)
                sudo apt-get install -y xvfb
                ;;
            fedora|centos|rhel)
                sudo dnf install -y xorg-x11-server-Xvfb
                ;;
            arch)
                sudo pacman -S xorg-server-xvfb
                ;;
        esac
    fi
    
    # Start Xvfb
    echo "Starting Xvfb on display :99..."
    Xvfb :99 -screen 0 1920x1080x24 &
    export DISPLAY=:99
    echo "export DISPLAY=:99" >> ~/.bashrc
    echo "✅ Virtual display configured"
}

# Create systemd service
create_systemd_service() {
    echo "🔧 Creating systemd service..."
    
    SERVICE_FILE="/etc/systemd/system/apple-browser-monitor.service"
    MONITOR_PATH="$(pwd)/browser_monitor"
    WORK_DIR="$(pwd)"
    
    sudo tee $SERVICE_FILE > /dev/null <<EOF
[Unit]
Description=Apple Browser Monitor Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$WORK_DIR
ExecStart=$MONITOR_PATH -headless=true -config=config/apple_monitor.json
Restart=always
RestartSec=30
StandardOutput=append:/var/log/apple-browser-monitor.log
StandardError=append:/var/log/apple-browser-monitor.error.log

# Environment
Environment="HOME=$HOME"
Environment="DISPLAY=:99"

[Install]
WantedBy=multi-user.target
EOF
    
    echo "Service file created at: $SERVICE_FILE"
    echo ""
    echo "To enable and start the service:"
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl enable apple-browser-monitor"
    echo "  sudo systemctl start apple-browser-monitor"
    echo "  sudo systemctl status apple-browser-monitor"
}

# Main menu
echo "Select setup options:"
echo "1) Install Chrome only"
echo "2) Install dependencies only"
echo "3) Setup virtual display (for headless servers)"
echo "4) Create systemd service"
echo "5) Full setup (all of the above)"
echo "6) Exit"
echo ""
read -p "Enter your choice (1-6): " choice

case $choice in
    1)
        install_chrome
        ;;
    2)
        install_dependencies
        ;;
    3)
        setup_virtual_display
        ;;
    4)
        create_systemd_service
        ;;
    5)
        install_chrome
        install_dependencies
        setup_virtual_display
        create_systemd_service
        ;;
    6)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "✅ Setup completed!"
echo ""
echo "Next steps:"
echo "1. Build the monitor: go build -o browser_monitor cmd/browser_apple_monitor/main.go"
echo "2. Run in headless mode: ./browser_monitor -headless=true"
echo "3. Or use the systemd service for automatic startup"