#!/usr/bin/env bash

set -euo pipefail

readonly bucket="compatibility"
# RustFS stands in for MinIO, which ended community releases and withdrew every
# published image, client included. Buckets are created with the AWS CLI below
# because no store here ships a usable client image.
readonly rustfs_image="docker.io/rustfs/rustfs:1.0.0"
readonly ceph_image="quay.io/benjamin_holmes/ceph-aio:v20"
readonly garage_image="dxflrs/garage:v2.3.0"
readonly seaweedfs_image="chrislusf/seaweedfs:4.41"

# S3-compatible endpoints must work without access to Amazon S3. Route any
# unexpected HTTPS request through a closed local port while allowing the test
# services on the loopback interface to be reached directly.
export HTTPS_PROXY="http://127.0.0.1:1"
export NO_PROXY="127.0.0.1,localhost"

containers=()
work_dir=$(mktemp -d)

cleanup() {
	for container in "${containers[@]}"; do
		docker rm -f -v "$container" >/dev/null 2>&1 || true
	done
	rm -rf "$work_dir"
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

make_bucket() {
	local endpoint=$1
	local region=$2
	local access_key=$3
	local secret_key=$4

	AWS_REGION=$region \
		AWS_ACCESS_KEY_ID=$access_key \
		AWS_SECRET_ACCESS_KEY=$secret_key \
		aws --endpoint-url "$endpoint" s3api create-bucket --bucket "$bucket" >/dev/null
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

test_rustfs() {
	local container="parquet-tools-rustfs-${RANDOM}"
	local access_key="rustfsaccesskey"
	local secret_key="rustfssecretkey"

	# The image declares /data as a volume but runs as a non-root user, and
	# Docker creates that anonymous volume owned by root, so RustFS cannot write
	# its backend there. RUSTFS_VOLUMES keeps the data outside the volume.
	pull_image "$rustfs_image"
	docker run -d --name "$container" -p 127.0.0.1::9000 \
		-e RUSTFS_VOLUMES=/tmp/data \
		-e RUSTFS_ACCESS_KEY="$access_key" \
		-e RUSTFS_SECRET_KEY="$secret_key" \
		"$rustfs_image" >/dev/null
	containers+=("$container")

	local port
	port=$(container_port "$container" 9000)
	wait_for_http "$container" "http://127.0.0.1:$port/health"
	make_bucket "http://127.0.0.1:$port" "us-east-1" "$access_key" "$secret_key"
	verify_parquet_io "http://127.0.0.1:$port" "us-east-1" "$access_key" "$secret_key"
}

test_ceph() {
	local container="parquet-tools-ceph-${RANDOM}"
	# Only the RADOS gateway matters here, so the dashboard, CephFS, and RBD
	# subsystems stay off to keep startup fast and the container lean.
	pull_image "$ceph_image"
	docker run -d --name "$container" -p 127.0.0.1::8000 \
		-e ENABLE_DASHBOARD=false \
		-e ENABLE_CEPHFS=false \
		-e ENABLE_RBD=false \
		-e DISABLE_MON_DISK_WARNINGS=true \
		"$ceph_image" >/dev/null
	containers+=("$container")

	local port
	port=$(container_port "$container" 8000)
	wait_for_http "$container" "http://127.0.0.1:$port/"

	# The image ships no S3 user, and the gateway answers before the cluster can
	# serve writes, so retry until user creation goes through.
	for attempt in $(seq 1 60); do
		if docker exec "$container" radosgw-admin user create \
			--uid=demo --display-name=demo \
			--access-key=cephdemo --secret-key=cephdemosecret >/dev/null 2>&1; then
			break
		fi
		if ! docker inspect -f '{{.State.Running}}' "$container" | grep -q true; then
			docker logs "$container"
			return 1
		fi
		if [[ $attempt -eq 60 ]]; then
			docker logs "$container"
			return 1
		fi
		sleep 5
	done

	make_bucket "http://127.0.0.1:$port" "us-east-1" "cephdemo" "cephdemosecret"
	verify_parquet_io "http://127.0.0.1:$port" "us-east-1" "cephdemo" "cephdemosecret"
}

test_garage() {
	local container="parquet-tools-garage-${RANDOM}"
	local config="$work_dir/garage.toml"
	local access_key="garageaccesskey"
	# Garage rejects secrets shorter than 16 characters.
	local secret_key="garagesecretkey0"

	# Garage refuses to start without a config file. Single-node mode builds the
	# cluster layout on its own, and --default-bucket provisions the access key
	# and the bucket, so no post-start CLI setup is needed.
	cat >"$config" <<-EOF
		metadata_dir = "/tmp/meta"
		data_dir = "/tmp/data"
		db_engine = "sqlite"
		replication_factor = 1

		rpc_bind_addr = "[::]:3901"
		rpc_public_addr = "127.0.0.1:3901"
		rpc_secret = "$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"

		[s3_api]
		s3_region = "garage"
		api_bind_addr = "[::]:3900"
	EOF

	pull_image "$garage_image"
	docker run -d --name "$container" -p 127.0.0.1::3900 \
		-v "$config:/etc/garage.toml:ro" \
		-e GARAGE_DEFAULT_ACCESS_KEY="$access_key" \
		-e GARAGE_DEFAULT_SECRET_KEY="$secret_key" \
		-e GARAGE_DEFAULT_BUCKET="$bucket" \
		"$garage_image" /garage server --single-node --default-bucket >/dev/null
	containers+=("$container")

	local port
	port=$(container_port "$container" 3900)
	wait_for_http "$container" "http://127.0.0.1:$port/"
	verify_parquet_io "http://127.0.0.1:$port" "garage" "$access_key" "$secret_key"
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

if ! command -v aws >/dev/null; then
	echo "aws CLI is required to create test buckets" >&2
	exit 1
fi

echo "==> RustFS endpoint ..."
test_rustfs
echo "==> Ceph RGW endpoint ..."
test_ceph
echo "==> Garage endpoint ..."
test_garage
echo "==> SeaweedFS endpoint ..."
test_seaweedfs
echo "All S3-compatible endpoint tests passed"
