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

Native execution inherits the terminal and receives configured database variables. v0.1 does not detach or manage a native process after it exits.

## Services

Every entry under `services` is one PostgreSQL data service.

- `kind` must be `postgres` in v0.1.
- `primary` may be true for at most one service. Init assigns `DATABASE_URL` and `DIRECT_DATABASE_URL` to the primary; additional services receive normalized service prefixes.
- `local.compose_service` names the service removed from transient Compose.
- `remote.provider` must be `planetscale`.
- `remote.organization`, `database`, and `base_branch` identify the source database.
- `remote.cluster_size` is a provider SKU or `auto-smallest`. Automatic selection considers eligible numeric SKUs and chooses the smallest.
- `env.pooled` and `env.direct` are unique, valid process environment names.

## Bootstrap modes

- `attach` — require an existing PlanetScale database; create only the environment branch and role.
- `empty` — create the database if missing, then restore environments from its base-branch backup.
- `import` — create the database if missing, inspect the local Compose Postgres service, replay supported extensions, and stream a logical dump into the base branch once.

Import also requires:

```yaml
data:
  allow_copy: true
```

Repobox never creates a local dump file. It does not mask or anonymize data. Extensions that require a PlanetScale dashboard/restart action stop the job with a specific error so the user can enable them and resume.

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
