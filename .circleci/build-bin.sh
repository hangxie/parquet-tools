#!/bin/bash

set -eou pipefail

for TARGET in ${REL_TARGET}; do
    echo "    ${TARGET}"
    BINARY=${BUILDDIR}/release/parquet-tools-${VERSION}-${TARGET}
    rm -f ${BINARY} ${BINARY}.gz ${BINARY}.zip
    export GOOS=$(echo ${TARGET} | cut -f 1 -d \-)
    export GOARCH=$(echo ${TARGET} | cut -f 2 -d \-)
    ${GO} build ${GOFLAGS} -tags "${TAGS}" -ldflags "${LDFLAGS}" -o ${BINARY} ./
    if [ ${GOOS} == "windows" ]; then
        (cd $(dirname ${BINARY});
            BASE_NAME=$(basename ${BINARY});
            mv ${BASE_NAME} ${BASE_NAME}.exe;
            zip -qm ${BASE_NAME}.zip ${BASE_NAME}.exe)
    else
        gzip ${BINARY}
    fi
done
