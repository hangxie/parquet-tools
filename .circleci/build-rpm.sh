#!/bin/bash

set -eou pipefail

PKG_NAME=parquet-tools
DOCKER_NAME=rpm-build
GIT_TAG=$1
SOURCE_DIR=$(dirname $0)/..
VERSION=$(echo ${GIT_TAG} | cut -f 1 -d \- | tr -d 'a-z')

# Launch build container
docker ps | grep ${DOCKER_NAME} && docker rm -f ${DOCKER_NAME}
docker run -dit --rm --name ${DOCKER_NAME} ubuntu:20.04

# CCI does not support volume mount, so use docker cp instead
git -C ${SOURCE_DIR} archive --format=tar.gz --prefix=${PKG_NAME}-${VERSION}/ -o /tmp/${PKG_NAME}-${VERSION}.tar.gz ${GIT_TAG}
docker cp /tmp/${PKG_NAME}-${VERSION}.tar.gz ${DOCKER_NAME}:/tmp/
docker cp ${SOURCE_DIR}/build/release/${PKG_NAME}-${GIT_TAG}-linux-amd64.gz ${DOCKER_NAME}:/tmp/${PKG_NAME}-${VERSION}-linux-amd64.gz
cat ${SOURCE_DIR}/package/rpm/${PKG_NAME}.spec | sed "s/^Version:.*/Version: ${VERSION}/" > /tmp/${PKG_NAME}.spec
docker cp /tmp/${PKG_NAME}.spec ${DOCKER_NAME}:/tmp/${PKG_NAME}.spec

# Build RPM
docker exec -t ${DOCKER_NAME} bash -c "
    set -eou pipefail;
    apt update && DEBIAN_FRONTEND=noninteractive apt install -y git rpm file binutils;
    mkdir -p ~/rpmbuild/SOURCES;
    cp /tmp/${PKG_NAME}-${VERSION}.tar.gz ~/rpmbuild/SOURCES/;
    rpmbuild -bb --target x86_64 /tmp/${PKG_NAME}.spec;
    cp /root/rpmbuild/RPMS/x86_64/${PKG_NAME}-${VERSION}-1.x86_64.rpm /tmp/;
"
docker cp ${DOCKER_NAME}:/tmp/${PKG_NAME}-${VERSION}-1.x86_64.rpm ${SOURCE_DIR}/build/release/

# Clean up
docker ps | grep ${DOCKER_NAME} && docker rm -f ${DOCKER_NAME}
