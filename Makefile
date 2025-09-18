.PHONY: build run dev clean test swagger swagger-fmt help

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
