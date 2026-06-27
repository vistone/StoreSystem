# Windows startup and HTTP smoke test design

## Context

The project must run on the current Windows development machine. The first startup attempt showed that Rust and MSVC Build Tools were missing from the environment, and the standalone process only exposed gRPC even though the log said the REST API was available.

## Goal

Make the existing standalone REST API actually reachable at runtime and add a regression test that proves the HTTP server accepts a basic write/read request.

## Scope

- Keep existing public APIs and config formats unchanged.
- Fix the Warp server startup calls by completing the bind-and-run sequence required by Warp 0.4.
- Add one focused Rust test for standalone HTTP write/read behavior.
- Make the existing Makefile and fault test runner work on Windows while keeping Linux commands available.
- Keep README and crate versions synchronized with the current crate version.

## Verification

- `cargo test http::tests::standalone_http_server_accepts_put_and_get`
- `cargo build --release`
- `cargo test`
- `cargo build --release --manifest-path client/Cargo.toml --bin store_client --bin fault_test`
- Manual standalone HTTP smoke check.
- `make test-fault`
- `make clean-data` when available, or equivalent PowerShell cleanup on Windows.
