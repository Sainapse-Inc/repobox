# Changelog

All notable changes to Repobox will be documented here.

## Unreleased

### Added

- Rust workspace with strict core contracts, a direct PlanetScale PostgreSQL provider, and an in-memory Docker Compose runtime adapter.
- `init`, `run`, `stop`, `pull`, status/logs, auth, environment, service, durable job, config, telemetry, update, doctor, completion, agent-context, and workflow help commands.
- Grok-inspired event-driven setup/dashboard TUIs with dirty/coalesced rendering, synchronized terminal updates, dedicated buffered output, and bounded log state.
- Deterministic branch-scoped environments, multi-database partial-success handling, append-only resumable jobs, staged forward-only refreshes, and explicit merged-branch pruning.
- OS credential storage with an explicit permission-restricted fallback, secret redaction, hidden token entry, and transient process-environment injection.
- Optional streaming local Postgres import with extension preflight/replay and no dump file.
- Versioned JSON/JSONL envelopes, schema snapshots, mutation undo metadata, stable exits, recursive agent-context manifest, and concrete help examples for every command.
- Linux/macOS x86_64/arm64 CI and release workflows, checksums, attestations, crates.io publication, and Homebrew tap automation.
- Architecture, configuration, agent-contract, feature-specification, security, and live-provider-smoke documentation.
