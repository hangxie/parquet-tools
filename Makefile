.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

VERSION     = $(shell git describe --tags)
BUILD       = $(shell date +%FT%T%z)
BUILDDIR    = $(CURDIR)/build
GOBIN       = $(shell go env GOPATH)/bin

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

.PHONY: all deps format lint test build docker-build clean help

all: deps format lint test build  ## Build all common targets

format:  ## Format all golang code
	@echo "==> Formatting all golang code"
	@gofmt -w -s $(GOSOURCES)

lint:  ## Run static code analysis
	@echo "==> Running static code analysis"
	@$(GOBIN)/golint ./...

deps:  ## Install prerequisite for build
	@echo "==> Installing prerequisite for build"
	@go mod tidy
	@test -x $(GOBIN)/golint || \
		(cd /tmp; go get golint)

build: deps  ## Build locally for local os/arch creating $(BUILDDIR) in ./
	@echo "==> Building executable"
	@mkdir -p $(BUILDDIR)
	@$(GO) build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $(BUILDDIR) ./

clean:  ## Clean up the build dirs
	@echo "==> Cleaning up build dirs"
	@rm -rf $(BUILDDIR) vendor .venv

docker-build:  ## Build docker image
	@echo "==> Building docker image"
	@echo TBD

test:  ## Run unit tests
	@echo "==> Running unit tests"
	@mkdir -p build
	@go test -coverprofile=build/cover.out ./...
	@go tool cover -html=build/cover.out -o build/coverage.html

help:  ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
