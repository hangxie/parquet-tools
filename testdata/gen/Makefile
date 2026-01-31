.DEFAULT_GOAL=help

# Required for globs to work correctly
SHELL:=/bin/bash

.PHONY: all help

gen:  ## Generate all test parquet files
	@for SRC in */main.go; do \
		echo === $$(dirname $${SRC}) ===; \
		(cd $$(dirname $${SRC}) && go run .); \
	done

	@# gen in Java
	@echo === old-style-list ===
	@(cd old-style-list; ./run-build.sh)

	@# gen in python
	@echo === unknown-type ===
	@(cd unknown-type; uv run main.py)

clean:  ## Clean up generated parquet files
	@echo "==> Cleaning up build dirs"
	@find . -name \*.parquet | xargs -r rm -f
	@find . -name .venv | xargs -r rm -fr
	@(cd old-style-list; ./run-clean.sh)

help:  ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
