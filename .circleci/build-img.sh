#!/bin/bash

set -euo pipefail

docker build . -f package/container/Dockerfile -t parquet-tools:latest
