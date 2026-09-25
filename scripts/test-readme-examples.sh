#!/usr/bin/env bash

# Validates the storage-location examples in README.md against live endpoints.
# Every assertion here mirrors a command shown in README.md, so a failure means
# either the tool regressed or the documented example went stale.
#
# Usage:
#   scripts/test-readme-examples.sh [section ...]
#
# Sections default to all of them; naming one or more runs just those, which is
# how you iterate on a single backend without paying for the docker-based ones.
# Available: local s3 s3-compatible azure gcs homebrew http hdfs

set -euo pipefail

readonly parquet_tools="${PARQUET_TOOLS:-./build/parquet-tools}"

readonly s3_object='s3://daylight-openstreetmap/parquet/osm_features/release=v1.58/type=way/20241112_191814_00139_grr7u_0041fe64-a5ba-4375-88bf-ef790dfedfff'
readonly gcs_object='gs://cloud-samples-data/bigquery/us-states/us-states.parquet'
readonly azure_account='azureopendatastorage'
readonly azure_container='laborstatisticscontainer'
readonly azure_blob='lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet'
readonly azure_wasbs="wasbs://${azure_container}@${azure_account}.blob.core.windows.net/${azure_blob}"
readonly dpla_object='https://dpla-provider-export.s3.amazonaws.com/2021/04/all.parquet/part-00000-471427c6-8097-428d-9703-a751a6572cca-c000.snappy.parquet'
readonly hadoop_image='hangxie/hadoop-all-in-one:3.5.0-v0.6.0'

cleanups=()

# errexit exits the shell outright, so a RETURN trap in a section would not run.
# Cleanups are registered here and drained from a single EXIT trap instead.
defer() {
	cleanups+=("$1")
}

run_cleanups() {
	local cmd
	[[ ${#cleanups[@]} -eq 0 ]] && return 0
	for cmd in "${cleanups[@]}"; do
		eval "${cmd}" || true
	done
}
trap run_cleanups EXIT

fail() {
	echo "FAIL: $1" >&2
	echo "      $2" >&2
	exit 1
}

# Asserts the command succeeds and prints exactly $1.
assert_output() {
	local expected=$1 actual
	shift
	if ! actual=$("$@" 2>&1); then
		fail "$*" "exited non-zero: ${actual}"
	fi
	[[ ${actual} == "${expected}" ]] || fail "$*" "expected [${expected}], got [${actual}]"
}

# Asserts the command fails and that its output contains the $1 literal, which
# is how README's documented error messages are kept honest. Matching is fixed
# string so that messages quoting a bracketed value need no escaping.
assert_error() {
	local literal=$1 actual
	shift
	if actual=$("$@" 2>&1); then
		fail "$*" "expected failure, got [${actual}]"
	fi
	grep -qF -- "${literal}" <<<"${actual}" || fail "$*" "expected [${literal}] in [${actual}]"
}

section_local() {
	assert_output 3 "${parquet_tools}" row-count testdata/good.parquet
	assert_output 3 "${parquet_tools}" row-count file://testdata/good.parquet
	assert_output 3 "${parquet_tools}" row-count file://./testdata/good.parquet

	# A colon-bearing path is a file when its prefix is not a known scheme.
	cp testdata/good.parquet 'foo:bar.parquet'
	defer "rm -f 'foo:bar.parquet'"
	assert_output 3 "${parquet_tools}" row-count 'foo:bar.parquet'
	assert_output 3 "${parquet_tools}" row-count 'file://./foo:bar.parquet'

	# A known scheme wins over a local path even when the URL is malformed.
	assert_error 'invalid URL escape "%zz"' "${parquet_tools}" row-count 's3://somewhere/%zz'
}

section_s3() {
	assert_output 7124019 "${parquet_tools}" row-count --anonymous "${s3_object}"
	assert_error 'StatusCode: 400' "${parquet_tools}" row-count --anonymous "${s3_object}" \
		--object-version non-existent-version
}

section_s3_compatible() {
	scripts/test-s3-compatibility.sh
}

section_azure() {
	# Absent credentials are a precondition of the assertions below, not an
	# assumption about the runner.
	unset AZURE_STORAGE_ACCOUNT_NAME AZURE_STORAGE_ACCESS_KEY

	# Every scheme takes either endpoint suffix, a DFS host resolves to the blob host.
	local scheme host
	for scheme in wasbs wasb abfss abfs az; do
		for host in blob dfs; do
			echo "    ${scheme} / ${host} ..."
			assert_output 6582726 "${parquet_tools}" row-count --anonymous \
				"${scheme}://${azure_container}@${azure_account}.${host}.core.windows.net/${azure_blob}"
		done
	done

	# az:// alone may omit the account, which then comes from the environment.
	assert_output 6582726 env "AZURE_STORAGE_ACCOUNT_NAME=${azure_account}" \
		"${parquet_tools}" row-count --anonymous "az://${azure_container}/${azure_blob}"
	assert_error 'requires environment variable AZURE_STORAGE_ACCOUNT_NAME' \
		env -u AZURE_STORAGE_ACCOUNT_NAME \
		"${parquet_tools}" row-count --anonymous "az://${azure_container}/${azure_blob}"

	# An empty access key selects anonymous access without --anonymous.
	assert_output 6582726 env 'AZURE_STORAGE_ACCESS_KEY=' \
		"${parquet_tools}" row-count "${azure_wasbs}"

	# Azure distinguishes a malformed version id from one that does not exist.
	assert_error 'RESPONSE 400' "${parquet_tools}" row-count --anonymous "${azure_wasbs}" \
		--object-version foo-bar
	assert_error 'ERROR CODE: BlobNotFound' "${parquet_tools}" row-count --anonymous "${azure_wasbs}" \
		--object-version 2025-05-20T01:27:08.0552942Z
}

section_gcs() {
	assert_output 50 "${parquet_tools}" row-count --anonymous "${gcs_object}"
	assert_output 50 "${parquet_tools}" row-count --anonymous --object-version=-1 "${gcs_object}"
	assert_error "storage: object doesn't exist" "${parquet_tools}" row-count --anonymous \
		--object-version=123 "${gcs_object}"
	assert_error 'invalid GCS generation [foo-bar]' "${parquet_tools}" row-count --anonymous \
		--object-version=foo-bar "${gcs_object}"
}

section_homebrew() {
	# The formula smoke-tests schema output against this pinned commit.
	"${parquet_tools}" schema \
		https://github.com/hangxie/parquet-tools/raw/950d21759ff3bd398d2432d10243e1bace3502c5/testdata/good.parquet |
		grep -q 'name=parquet_go_root'
}

section_http() {
	assert_output 588 "${parquet_tools}" size \
		https://github.com/hangxie/parquet-tools/raw/refs/heads/main/testdata/good.parquet
	assert_output 6582726 "${parquet_tools}" row-count \
		"https://${azure_account}.blob.core.windows.net/${azure_container}/${azure_blob}"
	assert_output 4632041101 "${parquet_tools}" size "${dpla_object}"
	assert_output '{"Raw":4632041101}' "${parquet_tools}" size -j "${dpla_object}"
}

section_hdfs() {
	docker run -dq --rm --name hadoop -p 9000:9000 -p 9866:9866 "${hadoop_image}"
	defer 'docker stop hadoop >/dev/null 2>&1'
	sleep 10
	docker exec hadoop hdfs dfs -mkdir /temp
	sleep 3
	"${parquet_tools}" import -f jsonl -m testdata/jsonl.schema -s testdata/jsonl.source \
		hdfs://root@localhost:9000/temp/good.parquet
	assert_output 10 "${parquet_tools}" row-count hdfs://localhost:9000/temp/good.parquet
}

readonly all_sections=(local s3 s3-compatible azure gcs homebrew http hdfs)

main() {
	local requested=("$@")
	[[ ${#requested[@]} -eq 0 ]] && requested=("${all_sections[@]}")

	local section
	for section in "${requested[@]}"; do
		if [[ ! " ${all_sections[*]} " == *" ${section} "* ]]; then
			echo "unknown section [${section}], pick from: ${all_sections[*]}" >&2
			exit 1
		fi
		echo "==> ${section} ..."
		"section_${section//-/_}"
	done
	echo "==> all requested sections passed"
}

main "$@"
