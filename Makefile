.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

BUILD_TIME	= $(shell date +%FT%T%z)
BUILD_DIR	= $(CURDIR)/build
PAGES_DIR		= $(BUILD_DIR)/pages
COLLECT_ARGS	?=
FUZZTIME		?= 10s
COVERAGE_CSV	?= $(BUILD_DIR)/coverage.csv
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
PYTHON		?= python3
GOBIN		= $(shell $(GO) env GOPATH)/bin
GOFLAGS		:= -trimpath
GOSOURCES	:= $(shell find . -type f -name '*.go')
LDFLAGS		:= -w -s \
				-extldflags "-static" \
				-X $(PKG_PREFIX)/cmd/version.version=$(VERSION) \
				-X $(PKG_PREFIX)/cmd/version.build=$(BUILD_TIME) \
				-X $(PKG_PREFIX)/cmd/version.source=Makefile

.EXPORT_ALL_VARIABLES:

.PHONY: all
all: deps tools format lint test build  ## Build all common targets

.PHONY: format
format: tools  ## Format source codes
	@echo "==> Formatting source codes"
	@$(GOBIN)/gofumpt -w -extra $(GOSOURCES)
	@$(GOBIN)/goimports -w -local $(PKG_PREFIX) $(GOSOURCES)

.PHONY: lint
lint: tools  ## Run static code analysis
	@echo "==> Running static code analysis"
	@$(GOBIN)/golangci-lint cache clean
	@$(GOBIN)/golangci-lint run ./... --timeout 5m --enable gocognit

.PHONY: deps
deps:  ## Install prerequisite for build
	@echo "==> Installing prerequisite for build"
	@$(GO) mod tidy

.PHONY: tools
tools:  ## Install build tools
	@echo "==> Installing build tools"
	@(cd /tmp; \
		$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest; \
		$(GO) install mvdan.cc/gofumpt@latest; \
		$(GO) install golang.org/x/tools/cmd/goimports@latest; \
	)

.PHONY: build
build: deps  ## Build locally for local os/arch creating $(BUILD_DIR) in ./
	@echo "==> Building executable"
	@mkdir -p $(BUILD_DIR)
	@CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build $(GOFLAGS) \
			-ldflags '$(LDFLAGS)' \
			-o $(BUILD_DIR) ./

.PHONY: collect-coverage
collect-coverage:  ## Run tests and append current coverage to coverage.csv
	@echo "==> Collecting coverage data"
	@mkdir -p $(BUILD_DIR)/test
	@set -euo pipefail ; \
		cd $(BUILD_DIR)/test ; \
		CGO_ENABLED=1 $(GO) test -parallel 4 -race -count 1 -trimpath \
			-coverprofile=coverage.out.tmp $(CURDIR)/... ; \
		grep -v "cmd/internal/testutils" coverage.out.tmp \
			| grep -v "parquet-go" > coverage.out ; \
		$(GO) tool cover -func=coverage.out > coverage.txt
	@TIMESTAMP=$$($(PYTHON) -c "import time; print(int(time.time()))") ; \
		COVERAGE=$$(grep "^total:" $(BUILD_DIR)/test/coverage.txt | awk '{print $$NF}' | tr -d '%') ; \
		echo "$$TIMESTAMP,$$COVERAGE" >> coverage.csv ; \
		echo "Appended: $$TIMESTAMP,$$COVERAGE"

.PHONY: pages
pages: pages-coverage pages-star  ## Generate all GitHub Pages content to build/pages/

.PHONY: pages-coverage
pages-coverage:  ## Collect coverage and generate chart (COLLECT_ARGS="--start 2024-01-01 --end 2024-06-01")
	@echo "==> Generating coverage history page"
	@mkdir -p $(PAGES_DIR)
	@$(PYTHON) scripts/coverage-history.py $(COLLECT_ARGS) $(PAGES_DIR)/coverage-history.html $(COVERAGE_CSV)
	@echo "==> Generating Go coverage report"
	@mkdir -p $(BUILD_DIR)/test
	@set -euo pipefail; \
		CGO_ENABLED=1 $(GO) test -parallel 4 -count 1 -trimpath \
			-coverprofile=$(BUILD_DIR)/test/coverage.out.tmp ./...; \
		grep -v "cmd/internal/testutils" $(BUILD_DIR)/test/coverage.out.tmp \
			| grep -v "parquet-go" > $(BUILD_DIR)/test/coverage.out; \
		$(GO) tool cover -html=$(BUILD_DIR)/test/coverage.out -o $(PAGES_DIR)/coverage.html

.PHONY: pages-star
pages-star:  ## Generate star history charts to build/pages/ (requires GITHUB_TOKEN)
	@echo "==> Generating star history page"
	@mkdir -p $(PAGES_DIR)
	@$(PYTHON) scripts/star-history.py $(PAGES_DIR)/star-history.html

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
		CGO_ENABLED=1 $(GO) test -parallel 4 -race -count 1 -trimpath -coverprofile=coverage.out.tmp $(CURDIR)/... ; \
		cat coverage.out.tmp | grep -v "cmd/internal/testutils" > coverage.out ; \
		$(GO) tool cover -html=coverage.out -o coverage.html ; \
		$(GO) tool cover -func=coverage.out -o coverage.txt ; \
		cat coverage.txt

.PHONY: fuzz
fuzz: deps  ## Run every fuzz test for FUZZTIME each (default 10s)
	@echo "==> Running fuzz tests (FUZZTIME=$(FUZZTIME) each)"
	@rc=0; \
	for pkg in $$($(GO) list ./...); do \
		for fn in $$(CGO_ENABLED=1 $(GO) test -list '^Fuzz' $$pkg 2>/dev/null | grep '^Fuzz'); do \
			echo "--> $$pkg $$fn"; \
			CGO_ENABLED=1 $(GO) test -run='^$$' -fuzz="^$$fn$$" -fuzztime=$(FUZZTIME) $$pkg 2>&1 \
				| grep -vE "^(fuzz: |PASS$$|ok )"; \
			s=$${PIPESTATUS[0]}; \
			if [ $$s -ne 0 ]; then rc=$$s; fi; \
		done; \
	done; \
	exit $$rc

.PHONY: benchmark
benchmark:  ## Run benchmark
	@echo "==> Running benchmark"
	@mkdir -p build
	@test -f ./build/benchmark.parquet \
	    || curl -sLo ./build/benchmark.parquet \
	       https://huggingface.co/datasets/hangxie/parquet-tools/resolve/main/benchmark-10K.parquet?download=true
	@test -f ./build/flat.parquet \
	    || curl -sLo ./build/flat.parquet \
	       https://huggingface.co/datasets/hangxie/parquet-tools/resolve/main/flat-100K.parquet?download=true
	@$(GO) test -bench ^Benchmark -run=^$$ -count 1 -benchtime 10x -benchmem ./...

.PHONY: profile
profile:  ## Run benchmark with profile
	@mkdir -p build/pprof
	@test -f ./build/benchmark.parquet \
	    || curl -sLo ./build/benchmark.parquet \
	       https://huggingface.co/datasets/hangxie/parquet-tools/resolve/main/benchmark-10K.parquet?download=true
	@test -f ./build/flat.parquet \
	    || curl -sLo ./build/flat.parquet \
	       https://huggingface.co/datasets/hangxie/parquet-tools/resolve/main/flat-100K.parquet?download=true
	@for CMD in Cat Merge RowCount Schema Size Version; do \
		$(GO) test -bench ^Benchmark_$${CMD}Cmd_Run/ -run=^$$ \
			-count 1 -benchtime 10x -benchmem \
			-cpuprofile build/pprof/cpu-$${CMD}.out \
			-memprofile build/pprof/mem-$${CMD}.out \
			-o build/cmd.pprof ./cmd/; \
	done

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
