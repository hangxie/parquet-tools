# Engineering Skills & Workflow Standards (parquet-tools)

This document defines expectations for contributors
to the parquet-tools Go CLI project.

---

## Project Domain Skills

- Comfortable with Parquet file format, including metadata, schemas, and compression.
- Experienced with Go for CLI tools and streaming IO.
- Familiarity with common Parquet storage locations (local, S3, GCS, HTTP).

---

## Go Engineering Standards

- Follow idiomatic Go style; run `gofmt`/`goimports` on changes.
- Handle errors explicitly and provide useful CLI feedback.
- Use `context.Context` for cancellable operations.
- Avoid leaking goroutines or unnecessary memory allocations.
- Source files are preferred to be under 300 LOC and must never exceed 500 LOC; split larger files into cohesive units.

---

## Testing & TDD Expectations

- Each source file must have a one-to-one mapping with a corresponding test file (e.g., `foo.go` ↔ `foo_test.go`).
- Tests must cover both typical and edge-case scenarios.
- Use **table-driven tests** for core logic.
- Golden files (under `testdata/golden/`) must be updated thoughtfully.
- New test data should be generated in `testdata/gen/` (Go preferred) and update `scripts/update-golden.sh`.
- Follow **TDD principles**: design tests before implementation, and ensure tests fail before implementing the feature.

---

## CLI & Documentation Quality

- CLI flags and subcommands must have clear help text.
- Output formats (plain text, JSON) should be consistent and stable.
- Documentation (`README.md`) must reflect all behavioral changes.
- User examples should be runnable and tested.
- Compatibility notes should reflect supported platforms.

---

## Contribution Workflow Norms

- **Always commit after completing a task.** Once `make all` passes, create a commit immediately — do not wait to be asked.
- Make small, reviewable commits after each logical phase (plan, test, implement, document).
- Commit messages should follow **Conventional Commits**.
- **Do not** add the agent's name as a commit co-author (e.g., no `Co-Authored-By` trailers).
- Avoid breaking backward compatibility without clear migration notes.

---

## Quality Gates

- Code must pass formatting, linting, testing, and build checks (`make all`) before merging.
- Test coverage must be maintained or improved on significant logic changes.
- Refactoring and cleanup must preserve behavior and pass validation.

---

## Task Tracking Process

When working on improvements or fixes from a review:

1. **Prepare `TODO.md`** - Create or update `TODO.md` with numbered, categorized items. Use checkboxes (`- [ ]`) to track completion.
2. **Make changes** - Implement the fix or improvement for the selected item(s).
3. **Validate** - Ensure `make all` passes (see Quality Gates).
4. **Commit** - Commit only the source code changes. Do **not** commit `TODO.md`.
5. **Mark complete** - Update `TODO.md` to check off the completed item(s) (`- [x]`).
