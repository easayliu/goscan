.PHONY: build dist image run dev ddl check clean test deps fmt lint status swagger swagger-fmt help

CONFIG ?= configs/config.daemon.yaml
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

# Default target
help: ## Show this help message
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Build Go binary. CGO is off so the binary matches the one in the image.
build: ## Build the server application
	@echo "Building Go server binary..."
	@CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/goscan ./cmd/server

# Build the binary the image is assembled from. Same flags as CI, so
# `make image` produces the same thing the release pipeline does.
dist: ## Build the linux binary the container image installs
	@echo "Building linux/amd64 binary..."
	@mkdir -p dist/linux/amd64
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" \
		-o dist/linux/amd64/goscan ./cmd/server

# The Dockerfile only installs a prebuilt binary, so the binaries have to exist
# before it runs — plain `docker build .` will not work on its own.
image: dist ## Build the container image locally
	@docker build -t goscan:$(VERSION) .

# Run the application
run: build ## Build and run the server application
	@echo "Starting Goscan Server..."
	@./bin/goscan $(CONFIG)

# Development mode
dev: ## Start development mode
	@echo "Starting development mode..."
	@go run ./cmd/server $(CONFIG)

# Print the ClickHouse DDL. Connects to nothing, so it is safe to pipe anywhere:
#   make ddl | clickhouse-client --host ... --queries-file -
ddl: build ## Print ClickHouse DDL for the configured tables
	@./bin/goscan --ddl $(CONFIG)

# The sample config ships without credentials on purpose, and --check now
# refuses a config whose scheduled jobs target a cloud with no keys. Placeholders
# let the structural part of the check run; a real config gets its keys from the
# environment anyway.
check: build ## Validate the configuration without connecting to anything
	@VOLCENGINE_ACCESS_KEY=$${VOLCENGINE_ACCESS_KEY:-placeholder} \
	 VOLCENGINE_SECRET_KEY=$${VOLCENGINE_SECRET_KEY:-placeholder} \
	 ALICLOUD_ACCESS_KEY_ID=$${ALICLOUD_ACCESS_KEY_ID:-placeholder} \
	 ALICLOUD_ACCESS_KEY_SECRET=$${ALICLOUD_ACCESS_KEY_SECRET:-placeholder} \
	 ./bin/goscan --check $(CONFIG)

# Test the application
test: ## Run tests
	@echo "Running tests..."
	@go test ./...

# Clean build artifacts
clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -rf bin/ dist/

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
	@echo "Project: Goscan"
	@echo "Version: $(VERSION)"
	@echo "Go version: $(shell go version)"

# Generate Swagger documentation
swagger: ## Generate Swagger documentation
	@echo "Generating Swagger documentation..."
	@$$(go env GOPATH)/bin/swag init -g cmd/server/main.go --output docs
	@echo "Swagger docs generated at docs/"

swagger-fmt: ## Format Swagger comments
	@echo "Formatting Swagger comments..."
	@$$(go env GOPATH)/bin/swag fmt
