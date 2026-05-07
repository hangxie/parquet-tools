#!/bin/bash
#
# Script to update golden files for tests
# Run from the project root directory: ./scripts/update-golden.sh
#
# Prerequisites:
#   - Go toolchain installed
#   - jq installed (for JSON formatting)
#
# Usage:
#   ./scripts/update-golden.sh
#

set -e

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Usage: ./scripts/update-golden.sh"
    echo ""
    echo "Regenerates all golden files in testdata/golden/ by running"
    echo "parquet-tools commands and capturing their output."
    echo ""
    echo "Prerequisites:"
    echo "  - Go toolchain installed"
    echo "  - jq installed (for JSON formatting)"
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GOLDEN_DIR="$PROJECT_ROOT/testdata/golden"
TESTDATA_DIR="$PROJECT_ROOT/testdata"

# Build the CLI first
echo "Building parquet-tools..."
cd "$PROJECT_ROOT"
mkdir -p "$PROJECT_ROOT/build"
go build -o "$PROJECT_ROOT/build/parquet-tools" .

PT="$PROJECT_ROOT/build/parquet-tools"

# Encryption keys for encrypted test files
ENC_FOOTER_KEY="MDEyMzQ1Njc4OTAxMjM0NQ=="
ENC_DOUBLE_KEY="MTIzNDU2Nzg5MDEyMzQ1MA=="
ENC_FLOAT_KEY="MTIzNDU2Nzg5MDEyMzQ1MQ=="
ENC_AAD_PREFIX="dGVzdGVy"

# Helper function to format JSON with jq
format_json() {
    jq '.' 2>/dev/null || cat
}

# Helper function to format JSONL with jq (each line separately, pretty printed)
format_jsonl() {
    while IFS= read -r line; do
        echo "$line" | jq '.' 2>/dev/null || echo "$line"
    done
}

# Helper function for static golden files that are source fixtures rather than
# command output. Listing them here keeps the coverage check complete.
require_static_golden() {
    if [[ ! -f "$1" ]]; then
        echo "Missing static golden fixture: $1" >&2
        exit 1
    fi
}

# Ensure every file in testdata/golden is either generated below or explicitly
# declared as a static fixture.
assert_all_golden_files_covered() {
    local existing_file
    local covered_file

    existing_file=$(mktemp)
    covered_file=$(mktemp)

    find "$GOLDEN_DIR" -type f -exec basename {} \; | sort > "$existing_file"
    grep -Eo '\$GOLDEN_DIR/[[:alnum:]_.-]+' "$0" | sed 's|.*GOLDEN_DIR/||' | sort -u > "$covered_file"

    if ! diff -u "$existing_file" "$covered_file"; then
        echo "Not all golden files are covered by scripts/update-golden.sh" >&2
        rm -f "$existing_file" "$covered_file"
        exit 1
    fi

    rm -f "$existing_file" "$covered_file"
}

echo "Updating golden files..."

# ============================================================================
# cat command golden files
# ============================================================================
echo "  cat command..."

# cat-good-json.json
$PT cat --format json "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/cat-good-json.json"

# cat-good-json-limit-2.json
$PT cat --format json --limit 2 "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/cat-good-json-limit-2.json"

# cat-good-json-skip-2.json
$PT cat --format json --skip 2 "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/cat-good-json-skip-2.json"

# cat-good-jsonl.jsonl
$PT cat --format jsonl "$TESTDATA_DIR/good.parquet" | format_jsonl > "$GOLDEN_DIR/cat-good-jsonl.jsonl"

# cat-good-csv.txt
$PT cat --format csv "$TESTDATA_DIR/good.parquet" > "$GOLDEN_DIR/cat-good-csv.txt"

# cat-good-csv-no-header.txt
$PT cat --format csv --no-header "$TESTDATA_DIR/good.parquet" > "$GOLDEN_DIR/cat-good-csv-no-header.txt"

# cat-good-tsv.txt
$PT cat --format tsv "$TESTDATA_DIR/good.parquet" > "$GOLDEN_DIR/cat-good-tsv.txt"

# cat-good-tsv-no-header.txt
$PT cat --format tsv --no-header "$TESTDATA_DIR/good.parquet" > "$GOLDEN_DIR/cat-good-tsv-no-header.txt"

# cat-all-types.jsonl
$PT cat --format jsonl "$TESTDATA_DIR/all-types.parquet" | format_jsonl > "$GOLDEN_DIR/cat-all-types.jsonl"

# cat-geospatial-hex.jsonl
$PT cat --format jsonl --geo-format hex "$TESTDATA_DIR/geospatial.parquet" | format_jsonl > "$GOLDEN_DIR/cat-geospatial-hex.jsonl"

# cat-geospatial-base64.jsonl
$PT cat --format jsonl --geo-format base64 "$TESTDATA_DIR/geospatial.parquet" | format_jsonl > "$GOLDEN_DIR/cat-geospatial-base64.jsonl"

# cat-geospatial-geojson.jsonl
$PT cat --format jsonl "$TESTDATA_DIR/geospatial.parquet" | format_jsonl > "$GOLDEN_DIR/cat-geospatial-geojson.jsonl"

# cat-old-style-list.jsonl
$PT cat --format jsonl "$TESTDATA_DIR/old-style-list.parquet" | format_jsonl > "$GOLDEN_DIR/cat-old-style-list.jsonl"

# cat-row-group.jsonl
$PT cat --format jsonl "$TESTDATA_DIR/row-group.parquet" | format_jsonl > "$GOLDEN_DIR/cat-row-group.jsonl"

# cat-dict-page.jsonl
$PT cat --format jsonl "$TESTDATA_DIR/dict-page.parquet" | format_jsonl > "$GOLDEN_DIR/cat-dict-page.jsonl"

# cat-high-compression.jsonl
$PT cat --format jsonl --limit 1 "$TESTDATA_DIR/high-compression.parquet" | format_jsonl > "$GOLDEN_DIR/cat-high-compression.jsonl"

# empty-json.txt
$PT cat --format json "$TESTDATA_DIR/empty.parquet" > "$GOLDEN_DIR/empty-json.txt"

# ============================================================================
# schema command golden files
# ============================================================================
echo "  schema command..."

# schema-all-types-raw.json
$PT schema --format raw "$TESTDATA_DIR/all-types.parquet" | format_json > "$GOLDEN_DIR/schema-all-types-raw.json"

# schema-all-types-json.json
$PT schema --format json "$TESTDATA_DIR/all-types.parquet" | format_json > "$GOLDEN_DIR/schema-all-types-json.json"

# schema-all-types-go.txt
$PT schema --format go "$TESTDATA_DIR/all-types.parquet" > "$GOLDEN_DIR/schema-all-types-go.txt"

# schema-csv-good.txt
$PT schema --format csv "$TESTDATA_DIR/csv-good.parquet" > "$GOLDEN_DIR/schema-csv-good.txt"

# schema-map-composite-value-raw.json
$PT schema --format raw "$TESTDATA_DIR/map-composite-value.parquet" | format_json > "$GOLDEN_DIR/schema-map-composite-value-raw.json"

# schema-map-composite-value-json.json
$PT schema --format json "$TESTDATA_DIR/map-composite-value.parquet" | format_json > "$GOLDEN_DIR/schema-map-composite-value-json.json"

# schema-map-value-map-json.json
$PT schema --format json "$TESTDATA_DIR/map-value-map.parquet" | format_json > "$GOLDEN_DIR/schema-map-value-map-json.json"

# schema-pargo-prefix-flat-go.txt
$PT schema --format go "$TESTDATA_DIR/pargo-prefix-flat.parquet" > "$GOLDEN_DIR/schema-pargo-prefix-flat-go.txt"

# schema-pargo-prefix-nested-go.txt
$PT schema --format go "$TESTDATA_DIR/pargo-prefix-nested.parquet" > "$GOLDEN_DIR/schema-pargo-prefix-nested-go.txt"

# schema-geospatial-go.txt
$PT schema --format go "$TESTDATA_DIR/geospatial.parquet" > "$GOLDEN_DIR/schema-geospatial-go.txt"

# schema-geospatial-json.json
$PT schema --format json "$TESTDATA_DIR/geospatial.parquet" | format_json > "$GOLDEN_DIR/schema-geospatial-json.json"

# schema-geospatial-raw.json
$PT schema --format raw "$TESTDATA_DIR/geospatial.parquet" | format_json > "$GOLDEN_DIR/schema-geospatial-raw.json"

# schema-good-go-camel-case.txt
$PT schema --format go --camel-case "$TESTDATA_DIR/good.parquet" > "$GOLDEN_DIR/schema-good-go-camel-case.txt"

# schema-gostruct-list-go.txt
$PT schema --format go "$TESTDATA_DIR/gostruct-list.parquet" > "$GOLDEN_DIR/schema-gostruct-list-go.txt"

# schema-unknown-type-raw.json
$PT schema --format raw "$TESTDATA_DIR/unknown-type.parquet" | format_json > "$GOLDEN_DIR/schema-unknown-type-raw.json"

# schema-unknown-type-json.json
$PT schema --format json "$TESTDATA_DIR/unknown-type.parquet" | format_json > "$GOLDEN_DIR/schema-unknown-type-json.json"

# schema-unknown-type-go.txt
$PT schema --format go "$TESTDATA_DIR/unknown-type.parquet" > "$GOLDEN_DIR/schema-unknown-type-go.txt"

# schema-bloom-filter-raw.json
$PT schema --format raw "$TESTDATA_DIR/bloom-filter.parquet" | format_json > "$GOLDEN_DIR/schema-bloom-filter-raw.json"

# schema-bloom-filter-json.json
$PT schema --format json "$TESTDATA_DIR/bloom-filter.parquet" | format_json > "$GOLDEN_DIR/schema-bloom-filter-json.json"

# schema-bloom-filter-go.txt
$PT schema --format go "$TESTDATA_DIR/bloom-filter.parquet" > "$GOLDEN_DIR/schema-bloom-filter-go.txt"

# schema-good-skip-page-encoding-raw.json
$PT schema --format raw --skip-page-encoding "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/schema-good-skip-page-encoding-raw.json"

# schema-good-skip-page-encoding.json
$PT schema --format json --skip-page-encoding "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/schema-good-skip-page-encoding.json"

# schema-good-skip-page-encoding-go.txt
$PT schema --format go --skip-page-encoding "$TESTDATA_DIR/good.parquet" > "$GOLDEN_DIR/schema-good-skip-page-encoding-go.txt"

# schema-list-variants-* files are manually maintained source fixtures used to
# test JSON schema parsing for list encodings that are not backed by a parquet
# file in testdata.
require_static_golden "$GOLDEN_DIR/schema-list-variants-raw.json"
require_static_golden "$GOLDEN_DIR/schema-list-variants-json.json"
require_static_golden "$GOLDEN_DIR/schema-list-variants-go.txt"

# ============================================================================
# meta command golden files
# ============================================================================
echo "  meta command..."

# meta-good-raw.json
$PT meta "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/meta-good-raw.json"

# meta-nil-statistics-raw.json
$PT meta "$TESTDATA_DIR/nil-statistics.parquet" | format_json > "$GOLDEN_DIR/meta-nil-statistics-raw.json"

# meta-sorting-col-raw.json
$PT meta "$TESTDATA_DIR/sorting-col.parquet" | format_json > "$GOLDEN_DIR/meta-sorting-col-raw.json"

# meta-all-types-raw.json
$PT meta "$TESTDATA_DIR/all-types.parquet" | format_json > "$GOLDEN_DIR/meta-all-types-raw.json"

# meta-geospatial-raw.json
$PT meta "$TESTDATA_DIR/geospatial.parquet" | format_json > "$GOLDEN_DIR/meta-geospatial-raw.json"

# meta-row-group-raw.json
$PT meta "$TESTDATA_DIR/row-group.parquet" | format_json > "$GOLDEN_DIR/meta-row-group-raw.json"

# meta-bloom-filter-raw.json
$PT meta "$TESTDATA_DIR/bloom-filter.parquet" | format_json > "$GOLDEN_DIR/meta-bloom-filter-raw.json"

# meta-enc-columns-raw.json
$PT meta --footer-key "$ENC_FOOTER_KEY" --column-key "double_field=$ENC_DOUBLE_KEY" --column-key "float_field=$ENC_FLOAT_KEY" "$TESTDATA_DIR/encrypted-columns.parquet" | format_json > "$GOLDEN_DIR/meta-enc-columns-raw.json"

# meta-enc-footer-raw.json
$PT meta --footer-key "$ENC_FOOTER_KEY" --column-key "double_field=$ENC_DOUBLE_KEY" --column-key "float_field=$ENC_FLOAT_KEY" "$TESTDATA_DIR/encrypted-footer.parquet" | format_json > "$GOLDEN_DIR/meta-enc-footer-raw.json"

# meta-enc-uniform-raw.json
$PT meta --footer-key "$ENC_FOOTER_KEY" "$TESTDATA_DIR/uniform-encryption.parquet" | format_json > "$GOLDEN_DIR/meta-enc-uniform-raw.json"

# meta-enc-no-key-footer-raw.json
$PT meta --show-key-metadata "$TESTDATA_DIR/encrypted-footer.parquet" | format_json > "$GOLDEN_DIR/meta-enc-no-key-footer-raw.json"

# meta-enc-no-key-columns-raw.json
$PT meta --show-key-metadata "$TESTDATA_DIR/encrypted-columns.parquet" | format_json > "$GOLDEN_DIR/meta-enc-no-key-columns-raw.json"

# meta-enc-no-key-uniform-raw.json
$PT meta --show-key-metadata "$TESTDATA_DIR/uniform-encryption.parquet" | format_json > "$GOLDEN_DIR/meta-enc-no-key-uniform-raw.json"

# meta-enc-no-key-aad-raw.json
$PT meta --show-key-metadata "$TESTDATA_DIR/encrypted-aad.parquet" | format_json > "$GOLDEN_DIR/meta-enc-no-key-aad-raw.json"

# meta-enc-aad-raw.json
$PT meta --footer-key "$ENC_FOOTER_KEY" --column-key "double_field=$ENC_DOUBLE_KEY" --column-key "float_field=$ENC_FLOAT_KEY" --aad-prefix "$ENC_AAD_PREFIX" "$TESTDATA_DIR/encrypted-aad.parquet" | format_json > "$GOLDEN_DIR/meta-enc-aad-raw.json"

# ============================================================================
# inspect command golden files
# ============================================================================
echo "  inspect command..."

# inspect-good-file.json
$PT inspect "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/inspect-good-file.json"

# inspect-enc-columns-file.json
$PT inspect --footer-key "$ENC_FOOTER_KEY" --column-key "double_field=$ENC_DOUBLE_KEY" --column-key "float_field=$ENC_FLOAT_KEY" "$TESTDATA_DIR/encrypted-columns.parquet" | format_json > "$GOLDEN_DIR/inspect-enc-columns-file.json"

# inspect-enc-footer-file.json
$PT inspect --footer-key "$ENC_FOOTER_KEY" --column-key "double_field=$ENC_DOUBLE_KEY" --column-key "float_field=$ENC_FLOAT_KEY" "$TESTDATA_DIR/encrypted-footer.parquet" | format_json > "$GOLDEN_DIR/inspect-enc-footer-file.json"

# inspect-enc-uniform-file.json
$PT inspect --footer-key "$ENC_FOOTER_KEY" "$TESTDATA_DIR/uniform-encryption.parquet" | format_json > "$GOLDEN_DIR/inspect-enc-uniform-file.json"

# inspect-enc-columns-rg0.json
$PT inspect --footer-key "$ENC_FOOTER_KEY" --column-key "double_field=$ENC_DOUBLE_KEY" --column-key "float_field=$ENC_FLOAT_KEY" --row-group 0 "$TESTDATA_DIR/encrypted-columns.parquet" | format_json > "$GOLDEN_DIR/inspect-enc-columns-rg0.json"

# inspect-enc-uniform-rg0.json
$PT inspect --footer-key "$ENC_FOOTER_KEY" --row-group 0 "$TESTDATA_DIR/uniform-encryption.parquet" | format_json > "$GOLDEN_DIR/inspect-enc-uniform-rg0.json"

# inspect-dict-page-file.json
$PT inspect "$TESTDATA_DIR/dict-page.parquet" | format_json > "$GOLDEN_DIR/inspect-dict-page-file.json"

# inspect-row-group-file.json
$PT inspect "$TESTDATA_DIR/row-group.parquet" | format_json > "$GOLDEN_DIR/inspect-row-group-file.json"

# inspect-good-rg0.json
$PT inspect --row-group 0 "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/inspect-good-rg0.json"

# inspect-row-group-rg0.json
$PT inspect --row-group 0 "$TESTDATA_DIR/row-group.parquet" | format_json > "$GOLDEN_DIR/inspect-row-group-rg0.json"

# inspect-row-group-rg1.json
$PT inspect --row-group 1 "$TESTDATA_DIR/row-group.parquet" | format_json > "$GOLDEN_DIR/inspect-row-group-rg1.json"

# inspect-geospatial-rg0.json
$PT inspect --row-group 0 "$TESTDATA_DIR/geospatial.parquet" | format_json > "$GOLDEN_DIR/inspect-geospatial-rg0.json"

# inspect-nil-statistics-rg0.json
$PT inspect --row-group 0 "$TESTDATA_DIR/nil-statistics.parquet" | format_json > "$GOLDEN_DIR/inspect-nil-statistics-rg0.json"

# inspect-all-types-rg0.json
$PT inspect --row-group 0 "$TESTDATA_DIR/all-types.parquet" | format_json > "$GOLDEN_DIR/inspect-all-types-rg0.json"

# inspect-bloom-filter-rg0.json
$PT inspect --row-group 0 "$TESTDATA_DIR/bloom-filter.parquet" | format_json > "$GOLDEN_DIR/inspect-bloom-filter-rg0.json"

# inspect-bloom-filter-file.json
$PT inspect "$TESTDATA_DIR/bloom-filter.parquet" | format_json > "$GOLDEN_DIR/inspect-bloom-filter-file.json"

# inspect-good-rg0-cc0.json
$PT inspect --row-group 0 --column-chunk 0 "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/inspect-good-rg0-cc0.json"

# inspect-dict-page-rg0-cc0.json
$PT inspect --row-group 0 --column-chunk 0 "$TESTDATA_DIR/dict-page.parquet" | format_json > "$GOLDEN_DIR/inspect-dict-page-rg0-cc0.json"

# inspect-all-types-rg0-cc39.json
$PT inspect --row-group 0 --column-chunk 39 "$TESTDATA_DIR/all-types.parquet" | format_json > "$GOLDEN_DIR/inspect-all-types-rg0-cc39.json"

# inspect-bloom-filter-rg0-cc0.json
$PT inspect --row-group 0 --column-chunk 0 "$TESTDATA_DIR/bloom-filter.parquet" | format_json > "$GOLDEN_DIR/inspect-bloom-filter-rg0-cc0.json"

# inspect-good-rg0-cc0-pg0.json
$PT inspect --row-group 0 --column-chunk 0 --page 0 "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/inspect-good-rg0-cc0-pg0.json"

# inspect-dict-page-rg0-cc0-pg0.json
$PT inspect --row-group 0 --column-chunk 0 --page 0 "$TESTDATA_DIR/dict-page.parquet" | format_json > "$GOLDEN_DIR/inspect-dict-page-rg0-cc0-pg0.json"

# inspect-dict-page-rg0-cc0-pg1.json
$PT inspect --row-group 0 --column-chunk 0 --page 1 "$TESTDATA_DIR/dict-page.parquet" | format_json > "$GOLDEN_DIR/inspect-dict-page-rg0-cc0-pg1.json"

# inspect-row-group-rg1-cc0-pg0.json
$PT inspect --row-group 1 --column-chunk 0 --page 0 "$TESTDATA_DIR/row-group.parquet" | format_json > "$GOLDEN_DIR/inspect-row-group-rg1-cc0-pg0.json"

# inspect-data-page-v2-rg0-cc0-pg0.json
$PT inspect --row-group 0 --column-chunk 0 --page 0 "$TESTDATA_DIR/data-page-v2.parquet" | format_json > "$GOLDEN_DIR/inspect-data-page-v2-rg0-cc0-pg0.json"

# inspect-good-rg0-cc0-pg1.json
$PT inspect --row-group 0 --column-chunk 0 --page 1 "$TESTDATA_DIR/good.parquet" | format_json > "$GOLDEN_DIR/inspect-good-rg0-cc0-pg1.json"

# inspect-row-group-rg0-cc0-pg5.json
$PT inspect --row-group 0 --column-chunk 0 --page 5 "$TESTDATA_DIR/row-group.parquet" | format_json > "$GOLDEN_DIR/inspect-row-group-rg0-cc0-pg5.json"

# inspect-crc32-rg0-cc0-pg0
$PT inspect --row-group 0 --column-chunk 0 --page 0 "$TESTDATA_DIR/crc32.parquet" | format_json > "$GOLDEN_DIR/inspect-crc32-rg0-cc0-pg0.json"

# ============================================================================
# split/merge command golden files (these use cat to verify output)
# ============================================================================
echo "  split/merge commands..."

# These are generated by running split/merge and then cat on the result
# For now, we regenerate them using a temporary directory

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# split-optional-fields-json.json
$PT split --file-count 1 --name-format "$TEMP_DIR/split-%d.parquet" "$TESTDATA_DIR/optional-fields.parquet"
$PT cat --format json "$TEMP_DIR/split-0.parquet" | format_json > "$GOLDEN_DIR/split-optional-fields-json.json"
rm -f "$TEMP_DIR"/*.parquet

# merge-optional-fields-json.json
$PT merge "$TEMP_DIR/merged.parquet" --source "$TESTDATA_DIR/optional-fields.parquet" --source "$TESTDATA_DIR/optional-fields.parquet"
$PT cat --format json "$TEMP_DIR/merged.parquet" | format_json > "$GOLDEN_DIR/merge-optional-fields-json.json"
rm -f "$TEMP_DIR"/*.parquet

# ============================================================================
# retype command golden files
# ============================================================================
echo "  retype command..."

# Temporary file for retype output
RETYPE_OUTPUT="$TEMP_DIR/retype-output.parquet"

# retype-schema.json (no retype, original schema)
$PT retype --source "$TESTDATA_DIR/retype.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-schema.json"

# retype-data.json (no retype, original data)
$PT cat --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-data.json"
rm -f "$RETYPE_OUTPUT"

# retype-schema-int96-to-timestamp.json and retype-data-int96-to-timestamp.json
$PT retype --int96-to-timestamp --source "$TESTDATA_DIR/retype.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-schema-int96-to-timestamp.json"
$PT cat --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-data-int96-to-timestamp.json"
rm -f "$RETYPE_OUTPUT"

# retype-schema-bson-to-string.json and retype-data-bson-to-string.json
$PT retype --bson-to-string --source "$TESTDATA_DIR/retype.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-schema-bson-to-string.json"
$PT cat --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-data-bson-to-string.json"
rm -f "$RETYPE_OUTPUT"

# retype-schema-float16-to-float32.json and retype-data-float16-to-float32.json
$PT retype --float16-to-float32 --source "$TESTDATA_DIR/retype.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-schema-float16-to-float32.json"
$PT cat --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-data-float16-to-float32.json"
rm -f "$RETYPE_OUTPUT"

# retype-schema-json-to-string.json
$PT retype --json-to-string --source "$TESTDATA_DIR/retype.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-schema-json-to-string.json"
rm -f "$RETYPE_OUTPUT"

# retype-all-types-variant-to-string-schema.json and retype-all-types-variant-to-string-data.json
$PT retype --variant-to-string --source "$TESTDATA_DIR/all-types.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-all-types-variant-to-string-schema.json"
$PT cat --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-all-types-variant-to-string-data.json"
rm -f "$RETYPE_OUTPUT"

# retype-all-types-uuid-to-string-schema.json and retype-all-types-uuid-to-string-data.json
$PT retype --uuid-to-string --source "$TESTDATA_DIR/all-types.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-all-types-uuid-to-string-schema.json"
$PT cat --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-all-types-uuid-to-string-data.json"
rm -f "$RETYPE_OUTPUT"

# retype-all-types-repeated-to-list-schema.json and retype-all-types-repeated-to-list-data.json
$PT retype --repeated-to-list --source "$TESTDATA_DIR/all-types.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-all-types-repeated-to-list-schema.json"
$PT cat --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-all-types-repeated-to-list-data.json"
rm -f "$RETYPE_OUTPUT"

# retype-geospatial-geo-to-binary-schema.json and retype-geospatial-geo-to-binary-data.json
$PT retype --geo-to-binary --source "$TESTDATA_DIR/geospatial.parquet" "$RETYPE_OUTPUT"
$PT schema --format json "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-geospatial-geo-to-binary-schema.json"
# use --geo-format hex to avoid json output trying to decode WKB as utf8 string if we just use plain json output for byte array
$PT cat --format json --geo-format hex "$RETYPE_OUTPUT" | format_json > "$GOLDEN_DIR/retype-geospatial-geo-to-binary-data.json"
rm -f "$RETYPE_OUTPUT"

# ============================================================================
# int96-nil-min-max.json (special case from int96 test)
# ============================================================================
echo "  special cases..."

# int96-nil-min-max.json - meta output for int96 file
$PT meta "$TESTDATA_DIR/int96-nil-min-max.parquet" | format_json > "$GOLDEN_DIR/int96-nil-min-max.json"

assert_all_golden_files_covered

echo "Done! Golden files updated in $GOLDEN_DIR"
