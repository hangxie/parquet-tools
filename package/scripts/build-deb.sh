#!/bin/bash

set -euo pipefail

function build() {
    PKG_ARCH=$1

    case ${PKG_ARCH} in
        amd64)
            BIN_ARCH=amd64
            ;;
        arm64)
            BIN_ARCH=arm64
            ;;
        *)
            echo package for architecture ${PKG_ARCH} is not currently supported
            exit 0
            ;;
    esac

    DEB_VER=$(echo ${VERSION} | cut -f 1 -d \- | tr -d 'a-z')
    PKG_NAME=parquet-tools
    DOCKER_NAME=deb-build-${BIN_ARCH}
    SOURCE_DIR=$(dirname $0)/../..

    # Launch build container
    docker ps -a | grep ${DOCKER_NAME} && docker rm -f ${DOCKER_NAME}
    docker run -di --rm --name ${DOCKER_NAME} debian:13-slim

    # CCI does not support volume mount, so use docker cp instead
    docker cp ${SOURCE_DIR}/build/release/${PKG_NAME}-${VERSION}-linux-${BIN_ARCH}.gz ${DOCKER_NAME}:/tmp/${PKG_NAME}.gz
    docker cp ${SOURCE_DIR}/package/deb ${DOCKER_NAME}:/tmp/
    cat ${SOURCE_DIR}/package/deb/DEBIAN/control \
	| sed "s/^Version:.*/Version: ${DEB_VER}/; s/^Architecture:.*/Architecture: ${PKG_ARCH}/" > /tmp/control
    docker cp /tmp/control ${DOCKER_NAME}:/tmp/deb/DEBIAN/control

    # Build deb
    docker exec -t ${DOCKER_NAME} bash -c "
        set -euo pipefail;
        mkdir -p /tmp/deb/usr/bin;
        gunzip /tmp/${PKG_NAME}.gz;
        mv /tmp/${PKG_NAME} /tmp/deb/usr/bin/${PKG_NAME};
        cd /tmp;
        dpkg-deb --build /tmp/deb;
    "
    docker cp ${DOCKER_NAME}:/tmp/deb.deb ${SOURCE_DIR}/build/release/${PKG_NAME}_${DEB_VER}_${PKG_ARCH}.deb

    # Clean up
    docker ps -a | grep ${DOCKER_NAME} && docker rm -f ${DOCKER_NAME}
}

build amd64
build arm64
