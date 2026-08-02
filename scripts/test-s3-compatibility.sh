#!/usr/bin/env bash

set -euo pipefail

readonly bucket="compatibility"
readonly minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
readonly minio_client_image="quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z"
readonly ceph_image="quay.io/ceph/demo:main-30dc8b9a-squid-centos-stream9"
readonly seaweedfs_image="chrislusf/seaweedfs:4.40"

# S3-compatible endpoints must work without access to Amazon S3. Route any
# unexpected HTTPS request through a closed local port while allowing the test
# services on the loopback interface to be reached directly.
export HTTPS_PROXY="http://127.0.0.1:1"
export NO_PROXY="127.0.0.1,localhost"

containers=()

cleanup() {
	for container in "${containers[@]}"; do
		docker rm -f "$container" >/dev/null 2>&1 || true
	done
}
trap cleanup EXIT

container_port() {
	local container=$1
	local port=$2
	docker port "$container" "$port/tcp" | sed 's/.*://'
}

pull_image() {
	docker pull --quiet "$@" >/dev/null
}

wait_for_http() {
	local container=$1
	local url=$2
	for attempt in $(seq 1 60); do
		if curl -sS "$url" >/dev/null 2>&1; then
			return
		fi
		if ! docker inspect -f '{{.State.Running}}' "$container" | grep -q true; then
			docker logs "$container"
			return 1
		fi
		if [[ $attempt -eq 60 ]]; then
			docker logs "$container"
			return 1
		fi
		sleep 1
	done
}

verify_parquet_io() {
	local endpoint=$1
	local region=$2
	local access_key=$3
	local secret_key=$4

	export AWS_ENDPOINT_URL_S3=$endpoint
	export AWS_REGION=$region
	export AWS_ACCESS_KEY_ID=$access_key
	export AWS_SECRET_ACCESS_KEY=$secret_key

	./build/parquet-tools transcode -s testdata/good.parquet "s3://$bucket/good.parquet"
	test "$(./build/parquet-tools row-count "s3://$bucket/good.parquet")" = "3"
	./build/parquet-tools import -f jsonl -m testdata/jsonl.schema -s testdata/jsonl.source "s3://$bucket/generated.parquet"
	test "$(./build/parquet-tools row-count "s3://$bucket/generated.parquet")" = "10"
}

test_minio() {
	local container="parquet-tools-minio-${RANDOM}"
	pull_image "$minio_image"
	docker run -d --name "$container" -p 127.0.0.1::9000 \
		-e MINIO_ROOT_USER=minioadmin \
		-e MINIO_ROOT_PASSWORD=minioadmin \
		"$minio_image" server /data >/dev/null
	containers+=("$container")

	local port
	port=$(container_port "$container" 9000)
	wait_for_http "$container" "http://127.0.0.1:$port/minio/health/live"
	pull_image "$minio_client_image"
	docker run --rm --network "container:$container" --entrypoint /bin/sh \
		"$minio_client_image" -c \
		"mc alias set local http://127.0.0.1:9000 minioadmin minioadmin && mc mb local/$bucket" >/dev/null
	verify_parquet_io "http://127.0.0.1:$port" "us-east-1" "minioadmin" "minioadmin"
}

test_ceph() {
	local container="parquet-tools-ceph-${RANDOM}"
	# The demo image defaults to BlueStore, which needs a block device and native
	# AIO. MemStore keeps this endpoint compatibility test self-contained.
	pull_image --platform linux/amd64 "$ceph_image"
	docker run -d --platform linux/amd64 --name "$container" -p 127.0.0.1::8080 \
		-e MON_IP=127.0.0.1 \
		-e CEPH_PUBLIC_NETWORK=0.0.0.0/0 \
		-e DEMO_DAEMONS=osd,rgw \
		-e CEPH_DEMO_UID=demo \
		-e CEPH_DEMO_ACCESS_KEY=demo \
		-e CEPH_DEMO_SECRET_KEY=demo \
		--entrypoint /bin/bash "$ceph_image" -lc \
		"sed -i 's/osd objectstore = bluestore/osd objectstore = memstore/g' /opt/ceph-container/bin/demo && exec /opt/ceph-container/bin/demo" >/dev/null
	containers+=("$container")

	for attempt in $(seq 1 100); do
		if docker exec "$container" s3cmd ls >/dev/null 2>&1; then
			break
		fi
		if ! docker inspect -f '{{.State.Running}}' "$container" | grep -q true; then
			docker logs "$container"
			return 1
		fi
		if [[ $attempt -eq 100 ]]; then
			docker logs "$container"
			return 1
		fi
		sleep 6
	done

	docker exec "$container" s3cmd mb "s3://$bucket" >/dev/null
	local port
	port=$(container_port "$container" 8080)
	verify_parquet_io "http://127.0.0.1:$port" "us-east-1" "demo" "demo"
}

test_seaweedfs() {
	local container="parquet-tools-seaweedfs-${RANDOM}"
	pull_image "$seaweedfs_image"
	docker run -d --name "$container" -p 127.0.0.1::8333 \
		-e AWS_ACCESS_KEY_ID=admin \
		-e AWS_SECRET_ACCESS_KEY=secret \
		-e S3_BUCKET="$bucket" \
		"$seaweedfs_image" >/dev/null
	containers+=("$container")

	local port
	port=$(container_port "$container" 8333)
	wait_for_http "$container" "http://127.0.0.1:$port/"
	verify_parquet_io "http://127.0.0.1:$port" "us-east-1" "admin" "secret"
}

echo "==> MinIO endpoint ..."
test_minio
echo "==> Ceph RGW endpoint ..."
test_ceph
echo "==> SeaweedFS endpoint ..."
test_seaweedfs
echo "All S3-compatible endpoint tests passed"
