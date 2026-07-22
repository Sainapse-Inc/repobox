# Repobox

Repobox is an agent-native, terminal-only development environment platform. It keeps application processes on your machine while moving stateful dependencies into persistent, branch-scoped remote environments.

The v0.1 implementation starts with PostgreSQL on PlanetScale and Docker Compose:

- every Git branch receives isolated remote data, including `main`;
- every detected Compose Postgres service can map to its own PlanetScale database;
- non-database services keep running locally under a fast full-screen TUI;
- provider operations survive disconnects through an append-only job ledger;
- humans and LLMs use the same CLI, with stable JSON and JSONL output;
- credentials are injected into child processes and never written into the repository.

> [!CAUTION]
> PlanetScale branches and backups may cost money. `repobox pull` intentionally replaces environment-local database data and has no rollback after the old branch is deleted. Preview provider calls with `--dry-run --json`.

## Install

Before the first tagged release, build from source:

```sh
git clone https://github.com/abhirupghosh/repobox.git
cd repobox
cargo install --path crates/repobox --locked
```

Tagged releases are designed for:

```sh
cargo install repobox --locked
brew install abhirupghosh/tap/repobox
```

Release archives will cover Linux and macOS on x86_64 and arm64 with SHA-256 checksums and provenance attestations.

## Quick start

1. Authenticate with PlanetScale. Repobox opens a browser and shows a confirmation code; no token needs to be copied:

   ```sh
   repobox auth login
   ```

2. Enter a repository and run:

   ```sh
   cd your-repository
   repobox run
   ```

On the first interactive run, the setup TUI resolves Docker Compose, shows every detected PostgreSQL service, asks for the PlanetScale organization, writes `.repobox.yml`, and adds managed instructions to `CLAUDE.md` and `AGENTS.md`. Repobox provisions the current Git branch's data and starts remaining services locally.

To leave services running:

```sh
repobox run --detach
repobox status
repobox stop
```

To replace the current environment from the latest successful base-branch backups:

```sh
repobox pull --dry-run --json
repobox pull --yes
```

## Agent quick start

An LLM caller should discover the contract instead of scraping human help:

```sh
repobox agent-context --schemas --json
repobox config detect --json --no-input
repobox status --json --no-input
repobox run --detach --yes --json --no-input
```

For unattended automation, set `PLANETSCALE_SERVICE_TOKEN_ID` and `PLANETSCALE_SERVICE_TOKEN`; Repobox detects them and skips browser login. An agent coordinating with a human can instead run `repobox auth login --json --no-input`, surface the emitted approval URL and code, and wait for the final `result` event.

Immediate commands emit one JSON envelope. Provider create/pull/resume operations, `run`, and log streams emit JSONL through a final `result` event. Data goes to stdout; diagnostics and structured errors go to stderr. Mutations return `undo_command` or an explicit `undo_reason`.

See the [agent contract](docs/agent-contract.md) and `repobox help agents`.

## Existing and local data

Each service chooses one bootstrap mode:

- `attach`: use an existing PlanetScale database;
- `empty`: create it if missing and rely on base-branch backups for environments;
- `import`: opt in to streaming a local Compose Postgres database into the base branch once.

Import requires `data.allow_copy: true`, creates no dump file, and does no masking. Read the [configuration reference](docs/configuration.md) before using production-derived data.

## Project status

The offline v0.1 implementation and test suite are complete. A first public tag remains gated on the credentialed [live PlanetScale smoke](docs/live-smoke.md); no release should be published until every created provider resource is verified and cleaned up.

Hosted application execution on Modal, E2B, or similar providers is a planned runtime adapter, not a v0.1 claim. The initial architecture keeps that seam without adding a daemon or hosted control plane.

## Documentation

- [Feature specification](docs/features/branch-scoped-data.md)
- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Agent and automation contract](docs/agent-contract.md)
- [Live provider smoke](docs/live-smoke.md)
- [CLI design audit](docs/cli-audit.md)

## Development

Rust 1.97.1 is pinned by `rust-toolchain.toml`.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for schema and release checks.

## License

MIT
