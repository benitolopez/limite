VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

# ==================================================================================== #
# HELPERS
# ==================================================================================== #

## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

# ==================================================================================== #
# DEVELOPMENT
# ==================================================================================== #

## run/limite-server: run the cmd/limite-server application
.PHONY: run/limite-server
run/limite-server:
	go run ./cmd/limite-server

## run/limite-check: run the cmd/limite-check application
.PHONY: run/limite-check
run/limite-check:
	go run ./cmd/limite-check

# ==================================================================================== #
# BUILD
# ==================================================================================== #

## build/limite-server: build the cmd/limite-server application
.PHONY: build/limite-server
build/limite-server:
	@echo 'Building cmd/limite-server...'
	go build -ldflags='-s -X main.version=$(VERSION)' -o=./bin/limite-server ./cmd/limite-server
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags='-s -X main.version=$(VERSION)' -o=./bin/linux_amd64/limite-server ./cmd/limite-server

## build/limite-check: build the cmd/limite-check application
.PHONY: build/limite-check
build/limite-check:
	@echo 'Building cmd/limite-check...'
	go build -ldflags='-s' -o=./bin/limite-check ./cmd/limite-check
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags='-s' -o=./bin/linux_amd64/limite-check ./cmd/limite-check


# ==================================================================================== #
# QUALITY CONTROL
# ==================================================================================== #

## tidy: tidy module dependencies and format all .go files
.PHONY: tidy
tidy:
	@echo 'Tidying module dependencies...'
	go mod tidy
	@echo 'Formatting .go files...'
	go fmt ./...

## audit: run quality control checks
.PHONY: audit
audit:
	@echo 'Checking module dependencies...'
	go mod tidy -diff
	go mod verify
	@echo 'Linting code...'
	golangci-lint run
	@echo 'Running tests...'
	go test -race ./...
