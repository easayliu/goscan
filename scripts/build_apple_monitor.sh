#!/bin/bash

# Apple Stock Monitor Build Script
# This script builds the Apple stock monitor for multiple platforms

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project information
PROJECT_NAME="apple-stock-monitor"
VERSION=$(date +"%Y%m%d-%H%M%S")
BUILD_DIR="dist"
BINARY_NAME="apple_monitor"

echo -e "${BLUE}🍎 Building Apple Stock Monitor${NC}"
echo -e "${YELLOW}Version: ${VERSION}${NC}"
echo ""

# Clean previous builds
echo -e "${YELLOW}🧹 Cleaning previous builds...${NC}"
rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}

# Build information
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GO_VERSION=$(go version | cut -d' ' -f3)

# Build flags
LDFLAGS="-s -w -X main.Version=${VERSION} -X main.BuildTime=${BUILD_TIME} -X main.GitCommit=${GIT_COMMIT}"

echo -e "${YELLOW}📋 Build Information:${NC}"
echo "  Version: ${VERSION}"
echo "  Build Time: ${BUILD_TIME}"
echo "  Git Commit: ${GIT_COMMIT}"
echo "  Go Version: ${GO_VERSION}"
echo ""

# Build for different platforms
build_platform() {
    local os=$1
    local arch=$2
    local output_dir="${BUILD_DIR}/${PROJECT_NAME}_${os}_${arch}"
    local binary_name="${BINARY_NAME}"
    
    if [ "$os" = "windows" ]; then
        binary_name="${BINARY_NAME}.exe"
    fi
    
    echo -e "${BLUE}🔨 Building for ${os}/${arch}...${NC}"
    
    mkdir -p "${output_dir}"
    
    # Set environment variables
    export GOOS=$os
    export GOARCH=$arch
    export CGO_ENABLED=1
    
    # Special handling for cross-compilation
    if [ "$os" = "linux" ] && [ "$(uname)" = "Darwin" ]; then
        echo -e "${YELLOW}  ⚠️  Cross-compiling for Linux from macOS - CGO disabled${NC}"
        export CGO_ENABLED=0
    fi
    
    # Build the binary
    go build -ldflags="${LDFLAGS}" -o "${output_dir}/${binary_name}" ./cmd/simple_apple_monitor/
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}  ✅ Successfully built ${os}/${arch}${NC}"
        
        # Copy configuration files
        cp config/apple_monitor.json "${output_dir}/"
        
        # Create README for this platform
        create_platform_readme "$os" "$arch" "${output_dir}"
        
        # Create run script
        create_run_script "$os" "${output_dir}" "${binary_name}"
        
        # Get binary size
        local size=$(ls -lh "${output_dir}/${binary_name}" | awk '{print $5}')
        echo -e "${GREEN}  📦 Binary size: ${size}${NC}"
        
    else
        echo -e "${RED}  ❌ Failed to build ${os}/${arch}${NC}"
        return 1
    fi
    
    # Reset environment
    unset GOOS GOARCH CGO_ENABLED
}

create_platform_readme() {
    local os=$1
    local arch=$2
    local dir=$3
    
    cat > "${dir}/README.md" << EOF
# Apple Stock Monitor - ${os}/${arch}

## 快速开始

### 1. 配置
编辑 \`apple_monitor.json\` 文件：
- 设置要监控的产品和店铺
- 配置Telegram通知（可选）
- 启用auto_cookie自动获取认证

### 2. 运行
EOF

    if [ "$os" = "windows" ]; then
        cat >> "${dir}/README.md" << EOF

#### Windows:
\`\`\`
run.bat
\`\`\`

或直接运行：
\`\`\`
${BINARY_NAME}.exe
\`\`\`
EOF
    else
        cat >> "${dir}/README.md" << EOF

#### ${os^}:
\`\`\`
./run.sh
\`\`\`

或直接运行：
\`\`\`
./${BINARY_NAME}
\`\`\`
EOF
    fi

    cat >> "${dir}/README.md" << EOF

### 3. 配置说明

#### 自动Cookie获取
程序支持自动获取Apple网站的认证cookie：
\`\`\`json
{
  "apple_auth": {
    "auto_cookie": {
      "enabled": true
    }
  }
}
\`\`\`

#### Telegram通知
配置Telegram机器人进行库存通知：
\`\`\`json
{
  "telegram": {
    "enabled": true,
    "bot_token": "your_bot_token",
    "chat_id": "your_chat_id"
  }
}
\`\`\`

### 4. 注意事项

- 首次运行需要安装Chrome浏览器（用于自动获取cookie）
- 程序会在检测到库存变化时发送通知
- 建议设置合理的检查间隔避免被限流

### 5. 故障排除

- 如果遇到541错误，程序会自动刷新cookie
- 检查网络连接和防火墙设置
- 查看日志文件了解详细错误信息

---
构建版本: ${VERSION}
构建时间: ${BUILD_TIME}
EOF
}

create_run_script() {
    local os=$1
    local dir=$2
    local binary_name=$3
    
    if [ "$os" = "windows" ]; then
        cat > "${dir}/run.bat" << EOF
@echo off
echo Starting Apple Stock Monitor...
echo Press Ctrl+C to stop
echo.
${binary_name}
pause
EOF
    else
        cat > "${dir}/run.sh" << 'EOF'
#!/bin/bash
echo "🍎 Starting Apple Stock Monitor..."
echo "Press Ctrl+C to stop"
echo ""
./${BINARY_NAME}
EOF
        chmod +x "${dir}/run.sh"
    fi
}

# Build for target platforms
echo -e "${YELLOW}🚀 Starting builds...${NC}"
echo ""

# macOS (current platform)
build_platform "darwin" "amd64"
build_platform "darwin" "arm64"

# Linux
build_platform "linux" "amd64"
build_platform "linux" "arm64"

# Windows
# build_platform "windows" "amd64"

echo ""
echo -e "${GREEN}✅ All builds completed successfully!${NC}"
echo ""

# Create release archive
echo -e "${YELLOW}📦 Creating release archives...${NC}"

cd ${BUILD_DIR}
for dir in */; do
    if [ -d "$dir" ]; then
        dirname=${dir%/}
        echo -e "${BLUE}  Creating ${dirname}.tar.gz...${NC}"
        tar -czf "${dirname}.tar.gz" "$dirname"
        echo -e "${GREEN}  ✅ Created ${dirname}.tar.gz${NC}"
    fi
done

cd ..

echo ""
echo -e "${GREEN}🎉 Build process completed!${NC}"
echo -e "${YELLOW}📁 Built packages in: ${BUILD_DIR}/${NC}"
echo ""

# List built packages
echo -e "${YELLOW}📋 Built packages:${NC}"
ls -la ${BUILD_DIR}/*.tar.gz 2>/dev/null || echo "No archives found"
echo ""

echo -e "${BLUE}📝 Next steps:${NC}"
echo "1. Test the built binaries"
echo "2. Update configuration files as needed"
echo "3. Distribute to target systems"
echo ""