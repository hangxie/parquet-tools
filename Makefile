.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

VERSION     = $(shell git describe --tags)
BUILD       = $(shell date +%FT%T%z)
BUILDDIR    = $(CURDIR)/build
GOBIN       = $(shell go env GOPATH)/bin
REL_TARGET  = \
	darwin-amd64 darwin-arm64 \
	linux-amd64 linux-arm linux-arm64 \
	windows-amd64 windows-arm64

# go option
GO          ?= go
PKG         :=
TAGS        :=
TESTS       := .
TESTFLAGS   :=
LDFLAGS     := -w -s
GOFLAGS     :=
GOSOURCES   := $(shell find . -type f -name '*.go')
CGO_ENABLED := 0
LDFLAGS     += -extldflags "-static"
LDFLAGS     += -X main.version=$(VERSION) -X main.build=$(BUILD)

.EXPORT_ALL_VARIABLES:

.PHONY: all deps tools format lint test build docker-build clean release-build help

all: deps tools format lint test build  ## Build all common targets

format: tools  ## Format all go code
	@echo "==> Formatting all go code"
	@$(GOBIN)/gofumpt -w -extra $(GOSOURCES)

lint: tools  ## Run static code analysis
	@echo "==> Running static code analysis"
	@$(GOBIN)/golangci-lint cache clean
	@$(GOBIN)/golangci-lint run --timeout 5m ./...
	@gocyclo -over 15 . > /tmp/gocyclo.output; \
		if [[ -s /tmp/gocyclo.output ]]; then \
			echo functions with gocyclo score higher than 15; \
			cat /tmp/gocyclo.output | sed 's/^/    /'; \
			false; \
		fi

deps:  ## Install prerequisite for build
	@echo "==> Installing prerequisite for build"
	@go mod tidy

tools:  ## Install build tools
	@echo "==> Installing build tools"
	@(cd /tmp; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.50.1; \
		go install github.com/jstemmer/go-junit-report/v2@v2.0.0; \
		go install mvdan.cc/gofumpt@latest; \
		go install github.com/fzipp/gocyclo/cmd/gocyclo@latest; \
	)

build: deps  ## Build locally for local os/arch creating $(BUILDDIR) in ./
	@echo "==> Building executable"
	@mkdir -p $(BUILDDIR)
	@CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $(BUILDDIR) ./

clean:  ## Clean up the build dirs
	@echo "==> Cleaning up build dirs"
	@rm -rf $(BUILDDIR) vendor .venv

docker-build:  ## Build docker image
	@echo "==> Building docker image"
	@.circleci/build-img.sh

test: deps tools  ## Run unit tests
	@echo "==> Running unit tests"
	@mkdir -p $(BUILDDIR)/test $(BUILDDIR)/junit
	@set -euo pipefail; \
	go test -v -coverprofile=$(BUILDDIR)/test/cover.out ./... \
		| tee /tmp/go-test.output \
		&& cat /tmp/go-test.output | $(GOBIN)/go-junit-report > $(BUILDDIR)/junit/junit.xml \
		&& go tool cover -html=$(BUILDDIR)/test/cover.out -o $(BUILDDIR)/test/coverage.html

release-build: deps ## Build release binaries
	@echo "==> Building release binaries"
	@mkdir -p $(BUILDDIR)/release/
	@.circleci/build-bin.sh

	@echo "==> generate RPM and deb packages"
	@.circleci/build-rpm.sh
	@.circleci/build-deb.sh

	@echo "==> generate build meta data"
	@.circleci/gen-meta.sh

	@echo "==> release info"
	@cat $(BUILDDIR)/release/checksum-md5.txt
	@echo
	@cat $(BUILDDIR)/CHANGELOG

help:  ## Print list of Makefile targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
