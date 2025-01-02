.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

.PHONY: all help

gen:  ## Generate all test parquet files
	@for SRC in */main.go; do \
		echo === $${SRC} ===; \
		(cd $$(dirname $${SRC}) && go run .); \
	done

clean:  ## Clean up generated parquet files
	@echo "==> Cleaning up build dirs"
	@find . -name \*.parquet | xargs rm -f

help:  ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
