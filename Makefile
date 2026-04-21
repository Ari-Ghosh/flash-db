.PHONY: help build run test test-coverage lint fmt clean docker-build docker-run setup

# Variables
BINARY_NAME=flashdb
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT) -s -w"

help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

setup: ## Install development dependencies
	@echo "Installing golangci-lint..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "Installing govulncheck..."
	@go install golang.org/x/vuln/cmd/govulncheck@latest
	@echo "Development dependencies installed"

build: ## Build the application
	@echo "Building $(BINARY_NAME)..."
	@go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./src

run: build ## Build and run the application
	@./bin/$(BINARY_NAME)

test: ## Run tests
	@echo "Running tests..."
	@go test -v -race -timeout 30s ./...

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	@go test -v -race -timeout 30s -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

test-short: ## Run tests (short mode)
	@go test -short -timeout 10s ./...

lint: ## Run golangci-lint
	@echo "Running linters..."
	@golangci-lint run ./...

lint-fix: ## Run golangci-lint with fixes
	@echo "Running linters with fixes..."
	@golangci-lint run --fix ./...

fmt: ## Format code
	@echo "Formatting code..."
	@go fmt ./...
	@goimports -w ./src

vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...

vuln: ## Check for vulnerabilities
	@echo "Checking for vulnerabilities..."
	@govulncheck ./...

deps: ## Update and tidy dependencies
	@echo "Tidying dependencies..."
	@go mod tidy
	@echo "Verifying dependencies..."
	@go mod verify

deps-graph: ## Display dependency graph
	@go mod graph

generate: ## Run go generate
	@echo "Running go generate..."
	@go generate ./...

docker-build: ## Build Docker image
	@echo "Building Docker image: $(BINARY_NAME):$(VERSION)"
	@docker build -t $(BINARY_NAME):$(VERSION) -t $(BINARY_NAME):latest .

docker-run: docker-build ## Build and run Docker container
	@echo "Running Docker container..."
	@docker-compose up

docker-down: ## Stop Docker container
	@docker-compose down

docker-clean: ## Remove Docker image and volumes
	@docker-compose down -v
	@docker rmi $(BINARY_NAME):latest 2>/dev/null || true

clean: ## Clean build artifacts and test files
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f coverage.out coverage.html
	@go clean -testcache
	@echo "Clean complete"

all: clean fmt lint test build ## Run full pipeline

ci: setup fmt lint vet vuln test ## Run CI pipeline (for GitHub Actions)

.DEFAULT_GOAL := help
