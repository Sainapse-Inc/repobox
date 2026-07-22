# Changelog

All notable changes to Repobox will be documented here.

## Unreleased

### Added

- Rust workspace with strict core contracts, a direct PlanetScale PostgreSQL provider, and an in-memory Docker Compose runtime adapter.
- `init`, `run`, `stop`, `pull`, status/logs, auth, environment, service, durable job, config, telemetry, update, doctor, completion, agent-context, and workflow help commands.
- Grok-inspired event-driven setup/dashboard TUIs with dirty/coalesced rendering, synchronized terminal updates, dedicated buffered output, and bounded log state.
- Deterministic branch-scoped environments, multi-database partial-success handling, append-only resumable jobs, staged forward-only refreshes, and explicit merged-branch pruning.
- Native PlanetScale browser login through OAuth device authorization, Bearer-token revocation on logout, service-token automation fallback, and backward-compatible credential migration.
- OS credential storage with an explicit permission-restricted fallback, secret redaction, and transient process-environment injection.
- Optional streaming local Postgres import with extension preflight/replay and no dump file.
- Versioned JSON/JSONL envelopes, schema snapshots, mutation undo metadata, stable exits, recursive agent-context manifest, and concrete help examples for every command.
- Linux/macOS x86_64/arm64 CI and release workflows, checksums, attestations, crates.io publication, and Homebrew tap automation.
- Architecture, configuration, agent-contract, feature-specification, security, and live-provider-smoke documentation.

### Changed

- Extended provider readiness budgets to tolerate observed PlanetScale
  transitions and reuse pending databases and branches during exact job resume.
- Hardened the manual release workflow with immutable action references,
  commit-bound manifests, publication preflight checks, crate checksum
  verification, and auditable OAuth-client authorization.

### Fixed

- Resume the explicitly requested create or pull job instead of selecting a
  newer operation for the same environment.
- Preserve canonical role identity across pull, avoid rotating roles after
  credential-read failures, and make credential deletion retry-safe across
  keyring and permission-restricted fallback storage.
- Require confirmation before resuming mutating jobs and retain successful
  bindings when another database service fails.
