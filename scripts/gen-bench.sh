#!/bin/bash

# Script to benchmark a specific version and update benchmarks.md
# Usage: ./gen-bench.sh <version>
# Example: ./gen-bench.sh v1.38.0

set -euo pipefail

VERSION="${1:-}"
if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 v1.38.0"
    exit 1
fi

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "Error: Not in a git repository"
    exit 1
fi

# Save current branch/commit
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_COMMIT=$(git rev-parse HEAD)

# Function to restore original state
restore_state() {
    echo ""
    echo "Restoring to original state..."
    git checkout "$CURRENT_BRANCH" 2>/dev/null || git checkout "$CURRENT_COMMIT"
}

# Set up trap to restore state on exit
trap restore_state EXIT

# Checkout the version
echo "Checking out version: $VERSION"
if ! git checkout "$VERSION" 2>/dev/null; then
    echo "Error: Failed to checkout version $VERSION"
    exit 1
fi

# Create temporary directory for benchmark results
TEMP_DIR=$(mktemp -d)
echo "Using temporary directory: $TEMP_DIR"

# Function to extract benchmark value from output
extract_benchmark() {
    local benchmark_name="$1"
    local output_file="$2"

    # Extract the ns/op value for the given benchmark
    grep "^${benchmark_name}" "$output_file" | awk '{print $(NF-5)}'
}

# Function to calculate median
calculate_median() {
    local values=("$@")
    local sorted=($(printf '%s\n' "${values[@]}" | sort -n))
    local count=${#sorted[@]}

    if [ $count -eq 0 ]; then
        echo "0"
        return
    fi

    if [ $((count % 2)) -eq 1 ]; then
        # Odd number of values
        echo "${sorted[$((count / 2))]}"
    else
        # Even number of values - average the two middle values
        local mid1=${sorted[$((count / 2 - 1))]}
        local mid2=${sorted[$((count / 2))]}
        echo "scale=0; ($mid1 + $mid2) / 2" | bc
    fi
}

# Function to convert ns to ms with appropriate formatting
ns_to_ms() {
    local ns="$1"
    local benchmark_type="$2"

    # Convert ns to ms
    if [ "$benchmark_type" = "cat" ] || [ "$benchmark_type" = "merge" ]; then
        # For cat and merge, round to nearest integer
        echo "scale=0; $ns / 1000000" | bc
    else
        # For others, show 3 decimal places
        echo "scale=3; $ns / 1000000" | bc | awk '{printf "%.3f", $0}'
    fi
}

# Run benchmark 3 times
echo ""
echo "Running benchmarks 3 times for version $VERSION..."
for i in 1 2 3; do
    echo "Run $i/3..."
    make benchmark > "$TEMP_DIR/benchmark_run_$i.txt" 2>&1
done

# Detect the CPU core suffix for concurrent benchmarks
CORE_SUFFIX=$(grep "BenchmarkCatCmd/concurrent" "$TEMP_DIR/benchmark_run_1.txt" | sed 's/.*concurrent-\([0-9]*\).*/\1/' | head -1)
if [ -z "$CORE_SUFFIX" ]; then
    CORE_SUFFIX="8"  # Default fallback
fi

echo ""
echo "Detected core suffix: $CORE_SUFFIX"

# Define benchmark mappings
declare -A BENCHMARKS=(
    ["cat"]="BenchmarkCatCmd/concurrent-${CORE_SUFFIX}"
    ["merge"]="BenchmarkMergeCmd/concurrent-${CORE_SUFFIX}"
    ["meta"]="BenchmarkMetaCmd/default-${CORE_SUFFIX}"
    ["row-count"]="BenchmarkRowCountCmd/default-${CORE_SUFFIX}"
    ["schema"]="BenchmarkSchemaCmd/default-${CORE_SUFFIX}"
    ["size"]="BenchmarkSizeCmd/default-${CORE_SUFFIX}"
    ["version"]="BenchmarkVersionCmd/default-${CORE_SUFFIX}"
)

# Extract values and calculate medians
echo ""
echo "Calculating median values..."
declare -A MEDIAN_VALUES

for key in "${!BENCHMARKS[@]}"; do
    benchmark_name="${BENCHMARKS[$key]}"
    values=()

    for i in 1 2 3; do
        value=$(extract_benchmark "$benchmark_name" "$TEMP_DIR/benchmark_run_$i.txt")
        if [ -n "$value" ]; then
            values+=("$value")
        fi
    done

    if [ ${#values[@]} -gt 0 ]; then
        median_ns=$(calculate_median "${values[@]}")
        median_ms=$(ns_to_ms "$median_ns" "$key")
        MEDIAN_VALUES[$key]="$median_ms"
        echo "  $key: $median_ms ms (from ${values[*]} ns)"
    else
        echo "  Warning: No values found for $key"
        MEDIAN_VALUES[$key]="N/A"
    fi
done

# Clean up temp directory
rm -rf "$TEMP_DIR"

# Now update benchmarks.md
echo ""
echo "Updating benchmarks.md..."

# Restore to original state to update benchmarks.md
git checkout "$CURRENT_BRANCH" 2>/dev/null || git checkout "$CURRENT_COMMIT"

# Read benchmarks.md
BENCHMARKS_FILE="benchmarks.md"
if [ ! -f "$BENCHMARKS_FILE" ]; then
    echo "Error: $BENCHMARKS_FILE not found"
    exit 1
fi

# Create a temporary file for the updated content
TEMP_BENCHMARKS=$(mktemp)

# Check if version already exists in the table
# Escape special regex characters in VERSION for grep
ESCAPED_VERSION=$(printf '%s\n' "$VERSION" | sed 's/[]\/$*.^[]/\\&/g')
if grep -q "^|[[:space:]]*${ESCAPED_VERSION}[[:space:]]*|" "$BENCHMARKS_FILE"; then
    echo "Version $VERSION already exists in benchmarks.md, updating values..."

    # Update existing line
    # Use awk variables instead of embedding version in regex to handle "/" in version names
    awk -v version="$VERSION" \
        -v cat="${MEDIAN_VALUES[cat]}" \
        -v merge="${MEDIAN_VALUES[merge]}" \
        -v meta="${MEDIAN_VALUES[meta]}" \
        -v rowcount="${MEDIAN_VALUES[row-count]}" \
        -v schema="${MEDIAN_VALUES[schema]}" \
        -v size="${MEDIAN_VALUES[size]}" \
        -v ver="${MEDIAN_VALUES[version]}" \
        'BEGIN {FS=OFS="|"}
         {
             # Strip leading/trailing whitespace from field 2 for comparison
             gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2)
             if ($2 == version) {
                 $2 = " " version " "
                 $3 = " " cat " "
                 $4 = " " merge " "
                 $5 = " " meta " "
                 $6 = " " rowcount " "
                 $7 = " " schema " "
                 $8 = " " size " "
                 $9 = " " ver " "
             }
             print
         }' "$BENCHMARKS_FILE" > "$TEMP_BENCHMARKS"
else
    echo "Version $VERSION not found, adding to top of table..."

    # Find the line with the header separator (the line with "|----:")
    # Insert new row after the separator line
    awk -v version="$VERSION" \
        -v cat="${MEDIAN_VALUES[cat]}" \
        -v merge="${MEDIAN_VALUES[merge]}" \
        -v meta="${MEDIAN_VALUES[meta]}" \
        -v rowcount="${MEDIAN_VALUES[row-count]}" \
        -v schema="${MEDIAN_VALUES[schema]}" \
        -v size="${MEDIAN_VALUES[size]}" \
        -v ver="${MEDIAN_VALUES[version]}" \
        'BEGIN {FS=OFS="|"; done=0}
         /^\|[[:space:]]*-------:/ && done==0 {
             print
             printf "| %s | %s | %s | %s | %s | %s | %s | %s |\n", version, cat, merge, meta, rowcount, schema, size, ver
             done=1
             next
         }
         {print}' "$BENCHMARKS_FILE" > "$TEMP_BENCHMARKS"
fi

# Replace original file with updated content
mv "$TEMP_BENCHMARKS" "$BENCHMARKS_FILE"

echo ""
echo "Summary for $VERSION:"
echo "  cat:       ${MEDIAN_VALUES[cat]} ms"
echo "  merge:     ${MEDIAN_VALUES[merge]} ms"
echo "  meta:      ${MEDIAN_VALUES[meta]} ms"
echo "  row-count: ${MEDIAN_VALUES[row-count]} ms"
echo "  schema:    ${MEDIAN_VALUES[schema]} ms"
echo "  size:      ${MEDIAN_VALUES[size]} ms"
echo "  version:   ${MEDIAN_VALUES[version]} ms"
echo ""
echo "benchmarks.md has been updated successfully!"
