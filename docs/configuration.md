# Configuration reference

Repobox discovers `.repobox.yml` from the requested repository upward. Version 1 is strict: unknown fields, duplicate environment-variable assignments, duplicate Compose service mappings, invalid names, multiple primary databases, empty Compose file lists, and empty native commands fail validation.

Print the exact machine-readable schema with:

```sh
repobox config schema --json
```

## Complete example

```yaml
version: 1

project:
  id: 018f6f4e-7040-7000-8000-000000000001
  name: example
  git:
    base_branch: main

runtime:
  driver: compose
  compose:
    files:
      - compose.yaml
    profiles: []

services:
  db:
    kind: postgres
    primary: true
    local:
      compose_service: db
    remote:
      provider: planetscale
      organization: acme
      database: example
      base_branch: main
      cluster_size: auto-smallest
    bootstrap:
      mode: empty
    env:
      pooled: DATABASE_URL
      direct: DIRECT_DATABASE_URL

data:
  allow_copy: false

agents:
  claude: true
  codex: true
```

## Project

- `project.id` is a non-nil UUID and the stable namespace for provider identities and local state. Do not regenerate it when moving or cloning a configured repository.
- `project.name` is the human label and defaults to the repository directory name during init.
- `project.git.base_branch` defaults to `main` and identifies the backup source and merged-branch comparison target.

## Runtime

Compose runtime:

```yaml
runtime:
  driver: compose
  compose:
    files: [compose.yaml, compose.dev.yaml]
    profiles: [dev]
```

Native runtime:

```yaml
runtime:
  driver: native
  native:
    command: [npm, run, dev]
    interactive: true
    working_directory: .
```

Native execution always receives the configured database variables. A human,
non-JSON run with `interactive: true` inherits stdin and remains in the terminal's
foreground process group, so terminal reads and job control work normally. JSON,
non-interactive, and `interactive: false` runs close stdin and use an isolated
managed process group; JSON mode captures redacted stdout and stderr as events.
Native `--detach` is unsupported in v0.1.

### Application-driver TLS

PlanetScale connection URLs use `sslmode=verify-full&sslrootcert=system`, the
provider's secure libpq form. Repobox preserves those parameters rather than
downgrading verification for a lowest-common-denominator driver.

Some non-libpq clients treat `system` as a literal filename. For example,
[`asyncpg`][2] requires the application adapter to pass `ssl=True` so Python
creates a hostname-verifying context from the trust store configured for that
runtime:

```python
pool = await asyncpg.create_pool(
    dsn=os.environ["DATABASE_URL"],
    ssl=True,
)
```

Keyword arguments override the corresponding DSN TLS settings. A native wrapper
may instead translate the sentinel to a verified CA-bundle path resolved inside
that exact runtime, but a host-specific path is not portable to Compose, macOS,
Windows, or a hosted environment. Do not remove certificate verification merely
to make the URL parse.

See PlanetScale's [connection parameters][1] and the driver's TLS documentation
before adapting an injected URL.

[1]: https://planetscale.com/docs/postgres/connecting/quickstart
[2]: https://magicstack.github.io/asyncpg/current/api/index.html?highlight=ssl

## Services

Every entry under `services` is one PostgreSQL data service.

- `kind` must be `postgres` in v0.1.
- `primary` may be true for at most one service. Init assigns `DATABASE_URL` and `DIRECT_DATABASE_URL` to the primary; additional services receive normalized service prefixes.
- `local.compose_service` names the service removed from transient Compose.
- `remote.provider` must be `planetscale`.
- `remote.organization`, `database`, and `base_branch` identify the source database.
- `remote.cluster_size` is a provider SKU or `auto-smallest`. Automatic selection considers eligible numeric SKUs and chooses the smallest. It minimizes initial cost; it does not estimate the memory required to import or query the configured data.
- `env.pooled` and `env.direct` are unique, valid process environment names.

Repobox applies `remote.cluster_size` when it creates a database or environment
branch. Changing the field does not resize an existing PlanetScale database.
Resize an existing target with PlanetScale first, wait for that operation to
finish, and then resume the Repobox job.

## Bootstrap modes

- `attach` — require an existing PlanetScale database; create only the environment branch and role.
- `empty` — create the database if missing, then restore environments from its base-branch backup.
- `import` — create the database if missing, inspect the local Compose Postgres service, replay supported extensions, and stream a logical dump into the base branch once.

Import also requires:

```yaml
data:
  allow_copy: true
```

Repobox never creates a local dump file. It streams one `pg_dump` process into
one `psql` process, does not mask or anonymize data, and keeps the source service
running if it was already running.

The source Compose service image must provide `sh`, `kill`, `pg_dump`, and
`pg_isready`. When Repobox starts a stopped source, it polls `pg_isready` with a
bounded, cancellation-aware timeout before inspecting extensions or launching
the dump. The shell then holds a control channel around `pg_dump`; closing that
channel on failure or cancellation terminates the container-side dump instead
of leaving it attached to the Docker daemon.

PlanetScale URLs use the libpq 16 `sslrootcert=system` profile. Repobox selects a
local `psql` only when its parsed major version is 16 or newer; an older,
unparseable, or missing client uses the labelled, auto-removed PostgreSQL 18
Docker fallback instead.

Import diagnostics retain the last 64 KiB of each process's stderr, drain both
streams concurrently, and redact the target password. A failed restore reports
the `psql` diagnostic even when the corresponding stream write ends with
`BrokenPipe`. Inspect and resume a failed job with:

```sh
repobox job view latest --json
repobox job resume EXACT_UUID --yes --json --no-input
```

`auto-smallest` can be too small for a logical restore containing wide JSON or
JSONB values. A target out-of-memory restart commonly appears to `psql` as an
unexpected EOF, invalid socket, or lost connection. Confirm the restart in
PlanetScale, resize the existing database, pin the larger SKU in
`remote.cluster_size`, wait for the resize to complete, and resume the exact
job. Do not loop retries at the same size; if the next controlled size also
restarts, stop and inspect the widest rows or contact the provider.

On SIGINT or SIGTERM, Repobox requests cooperative cancellation and gives the
active operation 30 seconds to finish its bounded cleanup. It terminates and
awaits managed dump/restore process groups, removes the temporary database role,
and stops a source Compose service only when Repobox started that service.
Successful cleanup returns `operation_interrupted` at the latest durable
checkpoint. `operation_interrupted_cleanup_incomplete` or
`operation_cleanup_failed` names resources that may remain and must be inspected
before resume.

Native runtimes receive a graceful interrupt and ten seconds to clean up their
own children. For an isolated JSON or non-interactive runtime, Repobox then
force-kills the managed process group. For a foreground interactive runtime,
the terminal delivers SIGINT to the shared foreground group; on timeout Repobox
can kill only the configured child without also killing itself. Either forced
path returns `native_runtime_cleanup_incomplete`, because descendants outside the
managed boundary cannot be proven absent. A clean `repobox run` interrupt emits
`runtime_stopped` in JSON mode and exits zero only after runtime cleanup completes.

The configured native command owns any child that deliberately creates a new
session or daemonizes. It must trap the graceful interrupt, stop those detached
children, and wait for them before exiting. Repobox can prove cleanup only for
the managed process group; arbitrary daemon ownership requires an operating-system
supervisor such as a cgroup or job object and is outside v0.1.

Host power loss, process `SIGKILL`, or an unavailable Docker daemon can still
interrupt outer cleanup. Resume the exact durable job after recovery and inspect
the named provider resources before removing anything manually.

Extensions that require a PlanetScale dashboard/restart action stop the job
with a specific error so the user can enable them and resume.

## Agent guides

`agents.claude` and `agents.codex` default to true. Init and config updates maintain a marker-delimited block in `CLAUDE.md` and `AGENTS.md`. Existing content outside the block is preserved, and rerunning the update is idempotent.

## Resolution and updates

Configuration resolution is:

```text
command flag > environment variable > .repobox.yml > user config > built-in default
```

Apply an RFC 7396 JSON Merge Patch atomically:

```sh
repobox config update \
  --patch '{"data":{"allow_copy":true}}' \
  --dry-run --json
```

After review, repeat without `--dry-run`. Use version control to restore a prior project configuration; Repobox does not retain an inverse patch.

For structured input, precedence is `--patch` > piped stdin > an interactive one-line prompt. `--no-input` disables only the prompt; an intentional pipe is still accepted.
