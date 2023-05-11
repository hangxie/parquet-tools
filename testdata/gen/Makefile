.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

.PHONY: all help

gen:  ## Generate all test parquet files
	@for SRC in *.go; do \
		echo === $${SRC} ===; \
		go run $${SRC}; \
	done

clean:  ## Clean up generated parquet files
	@echo "==> Cleaning up build dirs"
	@rm -rf *.parquet

help:  ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
