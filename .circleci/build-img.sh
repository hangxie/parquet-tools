#!/bin/bash

set -eou pipefail

docker build . -f package/Dockerfile -t parquet-tools:latest
