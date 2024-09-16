#!/bin/bash

set -euo pipefail

(cd ${BUILD_DIR}/release; \
    sha512sum *parquet-tools* > checksum-sha512.txt)

# version file
echo ${VERSION} > ${BUILD_DIR}/VERSION
PREV_VERSION=$(git tag --sort=-committerdate | head -2 | tail -1)

# changelog file
echo "Changes since [${PREV_VERSION}](https://github.com/hangxie/parquet-tools/releases/tag/${PREV_VERSION}):" > ${BUILD_DIR}/CHANGELOG
echo >> ${BUILD_DIR}/CHANGELOG
git log --pretty=format:"* %h %s" ${VERSION}...${PREV_VERSION} >> ${BUILD_DIR}/CHANGELOG
echo >> ${BUILD_DIR}/CHANGELOG

# license file
cp LICENSE ${BUILD_DIR}/release/LICENSE
