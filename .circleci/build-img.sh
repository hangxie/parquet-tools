#!/bin/bash

set -euo pipefail

docker build . -f package/Dockerfile -t parquet-tools:latest
