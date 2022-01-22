#!/bin/bash

set -euo pipefail

(cd ${BUILDDIR}/release; \
    sha512sum parquet-tools* > checksum-sha512.txt; \
    md5sum parquet-tools* > checksum-md5.txt)

# version file
echo ${VERSION} > ${BUILDDIR}/VERSION
PREV_VERSION=$(git tag --sort=-committerdate | head -2 | tail -1)

# changelog file
echo "Changes since [${PREV_VERSION}](https://github.com/hangxie/parquet-tools/releases/tag/${PREV_VERSION}):" > ${BUILDDIR}/CHANGELOG
echo >> ${BUILDDIR}/CHANGELOG
git log --pretty=format:"* %h %s" ${VERSION}...${PREV_VERSION} >> ${BUILDDIR}/CHANGELOG
echo >> ${BUILDDIR}/CHANGELOG

# license file
cp LICENSE ${BUILDDIR}/release/LICENSE
