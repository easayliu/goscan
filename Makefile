.PHONY: build run dev clean test swagger swagger-fmt help build-apple-monitor build-apple-monitor-linux package-apple-monitor-linux build-apple-monitor-windows package-apple-monitor-windows

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build Go binary
build: ## Build the server application
	@echo "Building Go server binary..."
	@CGO_ENABLED=1 go build -o bin/goscan cmd/server/main.go

# Run the application
run: build ## Build and run the server application
	@echo "Starting Goscan Server..."
	@./bin/goscan

# Development mode
dev: ## Start development mode
	@echo "Starting development mode..."
	@go run cmd/server/main.go

# Test the application
test: ## Run tests
	@echo "Running tests..."
	@go test -v ./internal/...
	@go test -v ./pkg/...

# Clean build artifacts
clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f data/goscan.db

# Install dependencies
deps: ## Install dependencies
	@echo "Installing Go dependencies..."
	@go mod download

# Format code
fmt: ## Format Go code
	@echo "Formatting code..."
	@go fmt ./...
	@gofmt -s -w .

# Lint code
lint: ## Lint Go code
	@echo "Linting code..."
	@go vet ./...

# Show project status
status: ## Show project status
	@echo "Project: Goscan Server Service"
	@echo "Version: 2.0.0"
	@echo "Go version: $(shell go version)"
	@echo "API Framework: Gin"
# Generate Swagger documentation
swagger: ## Generate Swagger documentation
	@echo "Generating Swagger documentation..."
	@$$GOPATH/bin/swag init -g cmd/server/main.go --output docs
	@echo "Swagger docs generated at docs/"

swagger-fmt: ## Format Swagger comments
	@echo "Formatting Swagger comments..."
	@$$GOPATH/bin/swag fmt

# Build Apple Monitor for macOS
build-apple-monitor: ## Build Apple Monitor for macOS
	@echo "Building Apple Monitor for macOS..."
	@go build -o simple_apple_monitor -ldflags="-s -w" ./cmd/simple_apple_monitor/main.go
	@echo "Build complete: simple_apple_monitor"

# Build Apple Monitor for Linux
build-apple-monitor-linux: ## Build Apple Monitor for Linux (AMD64)
	@echo "Building Apple Monitor for Linux AMD64..."
	@mkdir -p dist/apple_monitor_linux
	@GOOS=linux GOARCH=amd64 go build -o ./dist/apple_monitor_linux/apple_monitor -ldflags="-s -w" ./cmd/simple_apple_monitor/main.go
	@echo "Build complete: dist/apple_monitor_linux/apple_monitor"
	@echo "File size: $$(du -h dist/apple_monitor_linux/apple_monitor | cut -f1)"

# Package Apple Monitor for Linux with config
package-apple-monitor-linux: build-apple-monitor-linux ## Build and package Apple Monitor for Linux with config
	@echo "Packaging Apple Monitor for Linux..."
	@cp config/apple_monitor.json dist/apple_monitor_linux/
	@cd dist && tar -czf apple_monitor_linux.tar.gz apple_monitor_linux/
	@echo "Package created: dist/apple_monitor_linux.tar.gz"
	@echo "Package size: $$(du -h dist/apple_monitor_linux.tar.gz | cut -f1)"

# Build Apple Monitor for Windows
build-apple-monitor-windows: ## Build Apple Monitor for Windows (AMD64)
	@echo "Building Apple Monitor for Windows AMD64..."
	@mkdir -p dist/apple_monitor_windows
	@GOOS=windows GOARCH=amd64 go build -o ./dist/apple_monitor_windows/apple_monitor.exe -ldflags="-s -w" ./cmd/simple_apple_monitor/main.go
	@echo "Build complete: dist/apple_monitor_windows/apple_monitor.exe"
	@echo "File size: $$(du -h dist/apple_monitor_windows/apple_monitor.exe | cut -f1)"

# Package Apple Monitor for Windows with config
package-apple-monitor-windows: build-apple-monitor-windows ## Build and package Apple Monitor for Windows with config
	@echo "Packaging Apple Monitor for Windows..."
	@cp config/apple_monitor.json dist/apple_monitor_windows/
	@echo "Creating batch file..."
	@echo '@echo off' > dist/apple_monitor_windows/start.bat
	@echo 'echo Starting Apple Monitor...' >> dist/apple_monitor_windows/start.bat
	@echo 'apple_monitor.exe' >> dist/apple_monitor_windows/start.bat
	@echo 'pause' >> dist/apple_monitor_windows/start.bat
	@cd dist && zip -r apple_monitor_windows.zip apple_monitor_windows/
	@echo "Package created: dist/apple_monitor_windows.zip"
	@echo "Package size: $$(du -h dist/apple_monitor_windows.zip | cut -f1)"
