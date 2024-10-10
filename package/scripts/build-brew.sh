#!/bin/bash

set -euo pipefail

# this script builds brew bottles but for Mac only (arm64 and amd64)
# tarball name is like go-parquet-tools-1.22.0.arm64_monterey.bottle.tar.gz,
# with content:
# go-parquet-tools
# └── 1.22.0
#     ├── LICENSE
#     ├── README.md
#     └── bin
#         └── parquet-tools
#

SOURCE_DIR=${BUILD_DIR}/../
for ARCH in arm64 amd64; do
    ARCH_DIR=${BUILD_DIR}/brew/${ARCH}/
    BOTTLE_DIR=${ARCH_DIR}/go-parquet-tools/${VERSION:1}/
    mkdir -p ${BOTTLE_DIR}/bin

    # rebuild just in case we need any special setting for homebrew
    GOOS=darwin GOARCH=${ARCH} \
        ${GO} build ${GOFLAGS} \
            -ldflags "${LDFLAGS} -X ${PKG_PREFIX}/cmd.source=bottle" \
            -o ${BOTTLE_DIR}/bin/parquet-tools ${SOURCE_DIR}

    # nice-to-have files
    cp ${SOURCE_DIR}/LICENSE ${BOTTLE_DIR}/
    cp ${SOURCE_DIR}/README.md ${BOTTLE_DIR}/

    # tarball
    tar zcf ${BUILD_DIR}/brew/${ARCH}.tar.gz -C ${ARCH_DIR} go-parquet-tools/
done

for OSX in monterey sequoia sonoma ventura; do
    # for Apple Silicon
    cp ${BUILD_DIR}/brew/arm64.tar.gz \
        ${BUILD_DIR}/release/go-parquet-tools-${VERSION:1}.arm64_${OSX}.bottle.tar.gz

    # for Intel
    cp ${BUILD_DIR}/brew/amd64.tar.gz \
            ${BUILD_DIR}/release/go-parquet-tools-${VERSION:1}.${OSX}.bottle.tar.gz
done
