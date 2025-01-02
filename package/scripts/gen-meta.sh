#!/bin/bash

set -euo pipefail

(cd ${BUILD_DIR}/release; \
    sha512sum *parquet-tools* > checksum-sha512.txt)

# version file
echo ${VERSION} > ${BUILD_DIR}/VERSION
PREV_VERSION=$(git describe --abbrev=0 --tags ${VERSION}^)

# changelog file
(
    echo "Changes since [${PREV_VERSION}](https://github.com/hangxie/parquet-tools/releases/tag/${PREV_VERSION}):"
    echo
    git log --pretty=format:"* %h %s" ${VERSION}...${PREV_VERSION}
    echo
) > ${BUILD_DIR}/CHANGELOG

# license file
cp LICENSE ${BUILD_DIR}/release/LICENSE
