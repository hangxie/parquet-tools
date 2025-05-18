.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

BUILD_TIME	= $(shell date +%FT%T%z)
BUILD_DIR	= $(CURDIR)/build
PKG_PREFIX	= github.com/hangxie/parquet-tools
REL_TARGET	= \
				darwin-amd64 darwin-arm64 \
				linux-amd64 linux-arm linux-arm64 \
				windows-amd64 windows-arm64 \
				freebsd-amd64
VERSION		= $(shell git describe --tags --always)

# go option
CGO_ENABLED := 0
GO			?= go
GOBIN		= $(shell go env GOPATH)/bin
GOFLAGS		:= -trimpath
GOSOURCES	:= $(shell find . -type f -name '*.go')
LDFLAGS		:= -w -s
LDFLAGS		+= \
				-extldflags "-static" \
				-X $(PKG_PREFIX)/cmd.version=$(VERSION) \
				-X $(PKG_PREFIX)/cmd.build=$(BUILD_TIME) \
				-X $(PKG_PREFIX)/cmd.source=Makefile

.EXPORT_ALL_VARIABLES:

.PHONY: all
all: deps tools format lint test build  ## Build all common targets

.PHONY: format
format: tools  ## Format all go code
	@echo "==> Formatting all go code"
	@$(GOBIN)/gofumpt -w -extra $(GOSOURCES)
	@$(GOBIN)/goimports -w -local $(PKG_PREFIX) $(GOSOURCES)

.PHONY: lint
lint: tools  ## Run static code analysis
	@echo "==> Running static code analysis"
	@$(GOBIN)/golangci-lint cache clean
	@$(GOBIN)/golangci-lint run ./... \
		--timeout 5m \
		--exclude-use-default=false
	@$(GOBIN)/gocyclo -over 20 . > /tmp/gocyclo.output; \
		if [[ -s /tmp/gocyclo.output ]]; then \
			echo functions with gocyclo score higher than 20; \
			cat /tmp/gocyclo.output | sed 's/^/    /'; \
			false; \
		fi

.PHONY: deps
deps:  ## Install prerequisite for build
	@echo "==> Installing prerequisite for build"
	@go mod tidy

.PHONY: tools
tools:  ## Install build tools
	@echo "==> Installing build tools"
	@(cd /tmp; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
		go install github.com/jstemmer/go-junit-report/v2@latest; \
		go install mvdan.cc/gofumpt@latest; \
		go install github.com/fzipp/gocyclo/cmd/gocyclo@latest; \
		go install golang.org/x/tools/cmd/goimports@latest; \
	)

.PHONY: build
build: deps  ## Build locally for local os/arch creating $(BUILD_DIR) in ./
	@echo "==> Building executable"
	@mkdir -p $(BUILD_DIR)
	@CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build $(GOFLAGS) \
			-ldflags '$(LDFLAGS)' \
			-o $(BUILD_DIR) ./

.PHONY: clean
clean:  ## Clean up the build dirs
	@echo "==> Cleaning up build dirs"
	@rm -rf $(BUILD_DIR) vendor .venv

.PHONY: docker-build
docker-build:  ## Build docker image for local test
	@echo "==> Building docker image"
	@mkdir -p $(BUILD_DIR)/release/
	@package/scripts/build-bin.sh
	@sleep 2 # to address podman volume issue on MacOS
	@docker build . -f package/container/Dockerfile -t parquet-tools:local

.PHONY: test
test: deps tools  ## Run unit tests
	@echo "==> Running unit tests"
	@mkdir -p $(BUILD_DIR)/test
	@set -euo pipefail ; \
		cd $(BUILD_DIR)/test; \
		CGO_ENABLED=1 go test -v -race -count 1 -trimpath \
			-coverprofile=coverage.out $(CURDIR)/... \
			| tee go-test.output ; \
		go tool cover -html=coverage.out -o coverage.html ; \
		go tool cover -func=coverage.out -o coverage.txt ; \
		cat go-test.output | $(GOBIN)/go-junit-report > junit.xml ; \
		cat coverage.txt

.PHONY: benchmark
benchmark:  ## Run benchmark
	@echo "==> Running benchmark"
	@mkdir -p build
	@test -f ./build/benchmark.parquet \
	    || curl -sLo ./build/benchmark.parquet \
	       https://huggingface.co/datasets/hangxie/parquet-tools/resolve/main/benchmark-10K.parquet?download=true
	@go test -bench ^Benchmar -run=^$$ -count 1 -benchtime 1000000x -benchmem ./...

.PHONY: release-build
release-build: deps ## Build release binaries
	@echo "==> Building release binaries"
	@mkdir -p $(BUILD_DIR)/release/
	@package/scripts/build-bin.sh

	@echo "==> generate RPM and deb packages"
	@package/scripts/build-rpm.sh
	@package/scripts/build-deb.sh

	@echo "==> generate build meta data"
	@package/scripts/gen-meta.sh

	@echo "==> release info"
	@cat $(BUILD_DIR)/release/checksum-sha512.txt
	@echo
	@cat $(BUILD_DIR)/CHANGELOG

.PHONY: help
help:  ## Print list of Makefile targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
