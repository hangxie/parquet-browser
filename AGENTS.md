# Engineering Skills & Workflow Standards (parquet-browser)

This document defines expectations for contributors to the parquet-browser Go CLI, TUI, HTTP service, and Web UI project.

---

## Project Domain Skills

- Comfortable with the Parquet file format, including metadata, schemas, row groups, column chunks, pages, encodings, compression, statistics, and modular encryption keys.
- Experienced with Go for CLI tools, terminal user interfaces, HTTP services, streaming IO, and cancellable operations.
- Familiar with the project layers:
  - `cmd/` contains CLI commands, TUI screens, client logic, and version wiring.
  - `model/` contains Parquet reader, schema, formatting, and utility logic.
  - `service/` contains HTTP API and Web UI handlers.
  - `service/templates/` contains server-rendered Web UI templates.
  - `package/` contains release packaging scripts and metadata.
- Familiarity with common Parquet storage locations used by the upstream `parquet-tools` integration: local files, S3, GCS, Azure Blob Storage, HDFS, HTTP, and HTTPS.

---

## Go Engineering Standards

- Follow idiomatic Go style; run `make format` or `gofmt`/`goimports` on changes.
- Handle errors explicitly and provide useful CLI, TUI, HTTP, or Web UI feedback at the right boundary.
- Use `context.Context` for cancellable file loading, remote IO, service calls, and request-scoped operations.
- Avoid leaking goroutines, listeners, HTTP clients, response bodies, temporary files, or unnecessary memory allocations.
- Keep source files cohesive and prefer small files. When touching existing large files, avoid making them larger unless the change is tightly scoped; use a new focused file when that improves clarity.
- Preserve the separation between reader/model logic, command/TUI code, and service/Web UI handlers.

---

## Testing & TDD Expectations

- Each source file should have a corresponding test file where practical (e.g., `foo.go` <-> `foo_test.go`), especially for exported behavior, parsing, formatting, IO, and view-model logic.
- Tests must cover both typical and edge-case scenarios, including empty files, encrypted files, malformed inputs, remote path options, cancellation, HTTP status handling, and template rendering where relevant.
- Use table-driven tests for core logic.
- Keep tests deterministic. The Makefile downloads shared Parquet fixtures from the `parquet-tools` repository into `build/testdata`; do not commit generated files under `build/`.
- Prefer focused unit tests for `model/`, request/handler tests for `service/`, and CLI/TUI behavior tests for `cmd/`.
- Follow TDD principles for non-trivial behavior changes: write or update the failing test first, then implement the feature or fix.

---

## CLI, TUI, API & Documentation Quality

- CLI flags and subcommands must have clear help text and consistent behavior across `tui`, `serve`, and `web-ui` modes.
- TUI changes should preserve keyboard navigation, cancellation behavior, status-line clarity, and readable layouts on typical terminal sizes.
- HTTP API changes must keep JSON response shapes stable unless the README and `swagger.yaml` document a deliberate compatibility change.
- Web UI changes should keep server-rendered templates accessible, responsive, and usable without requiring custom client-side JavaScript beyond the existing progressive enhancement approach.
- Documentation (`README.md`) and examples must reflect behavioral, flag, API, schema, packaging, and workflow changes.
- Compatibility notes should reflect supported platforms and release artifacts.
- In Markdown prose, do not insert hard line breaks to wrap long lines; let the rendering engine handle wrapping. Hard breaks are only appropriate where the format genuinely requires them (e.g., list items, code blocks, tables).

---

## Contribution Workflow Norms

- Make small, reviewable commits after each logical phase when the maintainer asks for commits.
- Commit messages should follow Conventional Commits.
- Do not add the agent's name as a commit co-author (for example, no `Co-Authored-By` trailers).
- Avoid breaking backward compatibility without clear migration notes.
- Do not commit generated build output, downloaded test fixtures, coverage reports, or local scratch files.

---

## Quality Gates

- Code must pass formatting, linting, testing, and build checks before merging.
- Preferred full validation: `make all`.
- Focused validation while developing:
  - `go test ./...` for a quick test pass.
  - `make test` for the project test target, including fixture download, race detector, and coverage output.
  - `make format` before finalizing Go source changes.
  - `make lint` before finalizing shared or high-risk changes.
  - `make build` to verify the executable and linker metadata.
- Release and packaging changes should also run `make release-build` when practical.
- Test coverage must be maintained or improved on significant logic changes.
- Refactoring and cleanup must preserve behavior and pass validation.

---

## Task Tracking Process

When working on improvements or fixes from a review:

1. **Prepare `TODO.md`** - Create or update `TODO.md` with numbered, categorized items. Use checkboxes (`- [ ]`) to track completion.
2. **Make changes** - Implement the fix or improvement for the selected item(s).
3. **Validate** - Ensure the relevant quality gates pass.
4. **Commit when requested** - Commit only the source code changes. Do not commit `TODO.md`.
5. **Mark complete** - Update `TODO.md` to check off the completed item(s) (`- [x]`).
