#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT_UNDER_TEST="$SCRIPT_DIR/gen-bench.sh"
TEST_ROOT=$(mktemp -d)

cleanup() {
    rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local expected="$1"
    local file="$2"

    grep -Fq "$expected" "$file" ||
        fail "expected $file to contain: $expected"
}

create_test_repo() {
    local repo="$1"

    mkdir -p "$repo/scripts" "$repo/test-bin"
    cp "$SCRIPT_UNDER_TEST" "$repo/scripts/gen-bench.sh"
    cat > "$repo/benchmarks.md" <<'EOF'
| **Tag** | **cat** | **merge** | **meta** | **row-count** | **schema** | **size** | **version** |
| -------: | -------: | --------: | -------: | ------------: | ---------: | -------: | ----------: |
| HEAD | 1 | 1 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
EOF
    cat > "$repo/Makefile" <<'EOF'
benchmark:
	@false
EOF
    cat > "$repo/test-bin/make" <<'EOF'
#!/bin/bash
printf '%s\n' "$PWD" >> "$TEST_MAKE_PWDS"
cat <<'OUTPUT'
BenchmarkCatCmd/concurrent-8 10 1000000 ns/op 1 B/op 1 allocs/op
BenchmarkMergeCmd/concurrent-8 10 2000000 ns/op 1 B/op 1 allocs/op
BenchmarkMetaCmd/default-8 10 3000000 ns/op 1 B/op 1 allocs/op
BenchmarkRowCountCmd/default-8 10 4000000 ns/op 1 B/op 1 allocs/op
BenchmarkSchemaCmd/default-8 10 5000000 ns/op 1 B/op 1 allocs/op
BenchmarkSizeCmd/default-8 10 6000000 ns/op 1 B/op 1 allocs/op
BenchmarkVersionCmd/default-8 10 7000000 ns/op 1 B/op 1 allocs/op
OUTPUT
EOF
    chmod +x "$repo/scripts/gen-bench.sh" "$repo/test-bin/make"

    git -C "$repo" init -q
    git -C "$repo" config user.email test@example.com
    git -C "$repo" config user.name "Test User"
    git -C "$repo" add benchmarks.md Makefile
    git -C "$repo" commit -qm "test: add fixture"
    git -C "$repo" tag v1.2.3
}

test_dirty_worktree_is_preserved() {
    local repo="$TEST_ROOT/dirty"
    local output="$TEST_ROOT/dirty-output"
    local make_pws="$TEST_ROOT/make-pwds"
    local original_branch

    create_test_repo "$repo"
    sed -i.bak '/| HEAD/i\
| local-change | 9 | 9 | 9.000 | 9.000 | 9.000 | 9.000 | 9.000 |' "$repo/benchmarks.md"
    rm "$repo/benchmarks.md.bak"
    original_branch=$(git -C "$repo" branch --show-current)

    (
        cd "$repo"
        PATH="$repo/test-bin:$PATH" TEST_MAKE_PWDS="$make_pws" \
            ./scripts/gen-bench.sh v1.2.3
    ) > "$output" 2>&1 || {
        cat "$output" >&2
        fail "gen-bench.sh should succeed with a dirty benchmarks.md"
    }

    [ "$(git -C "$repo" branch --show-current)" = "$original_branch" ] ||
        fail "current branch changed"
    assert_contains "| local-change |" "$repo/benchmarks.md"
    assert_contains "| v1.2.3 | 1 | 2 | 3.000 | 4.000 | 5.000 | 6.000 | 7.000 |" "$repo/benchmarks.md"
    [ "$(sort -u "$make_pws" | wc -l | tr -d ' ')" = "1" ] ||
        fail "benchmark runs used multiple directories"
    [ "$(head -1 "$make_pws")" != "$repo" ] ||
        fail "benchmark ran in the current worktree"
}

test_invalid_version_has_clear_error() {
    local repo="$TEST_ROOT/invalid"
    local output="$TEST_ROOT/invalid-output"

    create_test_repo "$repo"
    if (
        cd "$repo"
        PATH="$repo/test-bin:$PATH" TEST_MAKE_PWDS="$TEST_ROOT/unused" \
            ./scripts/gen-bench.sh missing
    ) > "$output" 2>&1; then
        fail "invalid version should fail"
    fi

    assert_contains "Error: Version 'missing' does not resolve to a commit" "$output"
}

test_dirty_worktree_is_preserved
test_invalid_version_has_clear_error
echo "PASS: gen-bench.sh"
