# Contributing

Repobox uses stable Rust pinned by `rust-toolchain.toml`.

Before opening a pull request, run:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
scripts/audit-help.sh
scripts/generate-schemas.sh
git diff --exit-code -- docs/schemas
```

Public CLI JSON schemas are versioned contracts. Update their snapshots only
when the compatibility impact is intentional and documented in the changelog.

## CLI changes

- Keep noun/verb grammar and add a concrete example to every affected `--help` page.
- Preserve human/agent symmetry: a prompt or TUI path must have a `--no-input` alternative.
- Keep secrets out of argv. Use a hidden prompt, environment variable, or credential store.
- Immediate structured output is JSON; long-running output is JSONL through a final `result` event.
- Additive schema changes are preferred. Removal or semantic reuse requires a major version.
- Every mutation returns an undo command or an explicit reason no inverse exists.

Run the tier-3 rubric recorded in `docs/cli-audit.json` after changing the command surface.

## Provider changes

Provider code must remain behind the `DatabaseProvider` contract, preserve request IDs, retry only safe transient failures, handle pagination, and add a mocked wire test. Never run live provider tests from ordinary CI.

Before a release, a maintainer must execute [the live provider smoke](docs/live-smoke.md) in a disposable organization and prove cleanup. Missing credentials are a release blocker, not a reason to weaken the gate.

## Release

The release workflow expects:

- `CARGO_REGISTRY_TOKEN` for crates.io;
- `HOMEBREW_TAP_TOKEN` with access to `abhirupghosh/homebrew-tap`;
- a signed-off `v*` tag after the live smoke.

GitHub builds Linux/macOS x86_64/arm64 archives, checksums them, emits provenance attestations, publishes crates in dependency order, and updates the Homebrew formula. Do not create a tag if any publication credential or live-smoke evidence is missing.
