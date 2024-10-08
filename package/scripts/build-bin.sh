#!/bin/bash

set -euo pipefail

for TARGET in ${REL_TARGET}; do
    (
        BINARY=${BUILD_DIR}/release/parquet-tools-${VERSION}-${TARGET}
        rm -f ${BINARY} ${BINARY}.gz ${BINARY}.zip
        export GOOS=$(echo ${TARGET} | cut -f 1 -d \-)
        export GOARCH=$(echo ${TARGET} | cut -f 2 -d \-)
        ${GO} build ${GOFLAGS} \
            -ldflags "${LDFLAGS} -X ${PKG_PREFIX}/cmd.source=github" \
            -o ${BINARY} ./
        if [ ${GOOS} == "windows" ]; then
            (cd $(dirname ${BINARY});
                BASE_NAME=$(basename ${BINARY});
                mv ${BASE_NAME} ${BASE_NAME}.exe;
                zip -qm ${BASE_NAME}.zip ${BASE_NAME}.exe) &
        else
            gzip ${BINARY} &
        fi
        echo "    ${TARGET} built"
    ) &
done
wait
