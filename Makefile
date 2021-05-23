.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

VERSION     = $(shell git describe --tags)
BUILD       = $(shell date +%FT%T%z)
BUILDDIR    = $(CURDIR)/build
GOBIN       = $(shell go env GOPATH)/bin
REL_TARGET  = darwin-amd64 darwin-arm64 linux-386 linux-amd64 linux-arm linux-arm64 windows-386 windows-amd64

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

.PHONY: all deps tools format lint test build docker-build clean release-build help

all: deps tools format lint test build  ## Build all common targets

format:  ## Format all golang code
	@echo "==> Formatting all golang code"
	@gofmt -w -s $(GOSOURCES)

lint: tools  ## Run static code analysis
	@echo "==> Running static code analysis"
	@$(GOBIN)/golangci-lint run ./...

deps:  ## Install prerequisite for build
	@echo "==> Installing prerequisite for build"
	@go mod tidy

tools:  ## Install build tools
	@echo "==> Installing build tools"
	@test -x $(GOBIN)/golangci-lint || \
		(cd /tmp; GO111MODULE=on go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.40.1)
	@test -x $(GOBIN)/go-junit-report || \
		(cd /tmp; go get -u github.com/jstemmer/go-junit-report)


build: deps  ## Build locally for local os/arch creating $(BUILDDIR) in ./
	@echo "==> Building executable"
	@mkdir -p $(BUILDDIR)
	@$(GO) build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $(BUILDDIR) ./

clean:  ## Clean up the build dirs
	@echo "==> Cleaning up build dirs"
	@rm -rf $(BUILDDIR) vendor .venv

docker-build:  ## Build docker image
	@echo "==> Building docker image"
	@docker build . -f package/Dockerfile -t parquet-tools:latest

test: deps tools  ## Run unit tests
	@echo "==> Running unit tests"
	@mkdir -p $(BUILDDIR)/test $(BUILDDIR)/junit
	@set -eou pipefail \
		&& go test -v -coverprofile=$(BUILDDIR)/test/cover.out ./... \
	   	| $(GOBIN)/go-junit-report > $(BUILDDIR)/junit/junit.xml \
		&& go tool cover -html=$(BUILDDIR)/test/cover.out -o $(BUILDDIR)/test/coverage.html

release-build: deps ## Build release binaries
	@mkdir -p $(BUILDDIR)/release/
	@echo "==> Building release binaries"
	@set -eou pipefail; \
	for TARGET in $(REL_TARGET); do \
		echo "    $${TARGET}"; \
		BINARY=$(BUILDDIR)/release/parquet-tools-$(VERSION)-$${TARGET}; \
		rm -f $${BINARY} $${BINARY}.gz; \
		GOOS=$$(echo $${TARGET} | cut -f 1 -d \-); \
		GOARCH=$$(echo $${TARGET} | cut -f 2 -d \-); \
		BINARY=$(BUILDDIR)/release/parquet-tools-$(VERSION)-$${TARGET}; \
		GOOS=$${GOOS} GOARCH=$${GOARCH} \
		    $(GO) build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $${BINARY} ./; \
		if [ $${GOOS} == "windows" ]; then \
			(cd $$(dirname $${BINARY}); \
				BASE_NAME=$$(basename $${BINARY}); \
				mv $${BASE_NAME} $${BASE_NAME}.exe; \
				zip -m $${BASE_NAME}.zip $${BASE_NAME}.exe); \
		else \
			gzip $${BINARY}; \
		fi; \
	done; \
	echo "==> generate build meta data"; \
	echo $(VERSION) > $(BUILDDIR)/VERSION; \
	PREV_VERSION=$$(git tag --sort=-committerdate | head -2 | tail -1); \
	echo "Changes since [$${PREV_VERSION}](https://github.com/hangxie/parquet-tools/releases/tag/$${PREV_VERSION}):" > $(BUILDDIR)/CHANGELOG; \
	echo >> $(BUILDDIR)/CHANGELOG; \
	git log --pretty=format:"* %h %s" $(VERISON)...$${PREV_VERSION} >> $(BUILDDIR)/CHANGELOG; \
	cp LICENSE $(BUILDDIR)/release/LICENSE; \
	cat $(BUILDDIR)/CHANGELOG

help:  ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
