# Agent and automation contract

Repobox treats LLMs and shell automation as first-class callers. Every human operation has a non-interactive form; no agent should parse styled terminal output or send keystrokes to the TUI.

Start every unfamiliar session with:

```sh
repobox agent-context --json
```

Add `--schemas` when generating or validating a client. The manifest contains the complete recursive command/flag tree, output schema references, environment variables, current project context, and stable exit codes.

## Command grammar

```text
repobox
├── init
├── run
├── stop
├── pull
├── status
├── logs
├── auth
│   ├── login
│   ├── status
│   └── logout
├── env
│   ├── list
│   ├── create
│   ├── delete
│   └── prune
├── service
│   ├── list
│   ├── status
│   ├── restart
│   └── logs
├── job
│   ├── list
│   ├── view
│   ├── resume
│   └── cancel
├── config
│   ├── detect
│   ├── view
│   ├── schema
│   ├── validate
│   └── update
├── telemetry
│   ├── status
│   ├── enable
│   └── disable
├── update
├── doctor
├── completion
├── agent-context
└── help
```

All help pages include concrete examples. Long prose is available through `repobox help setup|agents|data|connections|environment|environments|formatting|config|exit-codes`.

## Global controls

- `--json` selects versioned JSON or JSONL.
- `--dry-run` plans mutations without provider calls or local writes.
- `--yes` approves confirmation gates.
- `--no-input` forbids prompts, browser launch, and TUI entry.
- `--repo PATH` selects the repository.
- `--color auto|always|never` controls human stderr styling; `NO_COLOR` always disables it.
- `--environment NAME` selects a data environment on relevant commands. It
  takes precedence over `REPOBOX_ENV`, which takes precedence over the current
  Git branch.

For non-interactive approved mutation, use all applicable controls:

```sh
repobox env create feature/demo --dry-run --json --no-input
repobox env create feature/demo --yes --json --no-input
repobox logs app --environment feature/demo --tail 50 --json --no-input
```

Structured input resolves as an explicit flag, then piped stdin, then an interactive prompt. For example:

```sh
printf '%s\n' '{"data":{"allow_copy":true}}' |
  repobox config update --dry-run --json --no-input
```

## Representative command: `run`

The implementation in `crates/repobox/src/app.rs` follows this annotated shape:

```rust
async fn run_project(cli: &Cli, output: &Output, repo: &Path, args: &RunArgs) -> Result<()> {
    // 1. Discover strict config; only an interactive TTY may auto-enter setup.
    let context = ProjectContext::load(repo)?;

    // 2. Resolve flag > REPOBOX_ENV > Git branch and inspect durable state.
    let environment = context.environment(args.environment.as_deref()).await?;

    // 3. Dry-run returns a provider-call plan using no credentials or network.
    if cli.dry_run { return output.data("run", &create_plan(environment)?); }

    // 4. Confirmation precedes any billable first provision; each step checkpoints.
    ensure_environment_if_needed(environment).await?;

    // 5. Resolve role URLs from the credential store and inject them in memory.
    let variables = environment_variables(environment)?;

    // 6. Human TTY: dashboard. JSON stream: JSONL + clean Ctrl-C.
    //    --detach: return immediately with a receipt whose inverse is `repobox stop`.
    run_selected_runtime(variables, output).await
}
```

The excerpt documents the control contract; the source remains authoritative for exact types and error propagation.

## Output

One-shot success is written to stdout:

```json
{"schema_version":1,"command":"status","data":{}}
```

Long-running mutation streams end with `event: result`; its data always contains both undo fields:

```json
{
  "schema_version": 1,
  "sequence": 7,
  "timestamp": "2026-07-22T00:00:02Z",
  "event": "result",
  "data": {
    "environment": {},
    "job": {},
    "resumed": false,
    "undo_command": null,
    "undo_reason": "the previous environment branch is deleted during the forward-only swap"
  }
}
```

Streams write one JSON object per line. `sequence` increases within the command invocation:

```json
{"schema_version":1,"sequence":1,"timestamp":"2026-07-22T00:00:00Z","event":"runtime_started","data":{}}
{"schema_version":1,"sequence":2,"timestamp":"2026-07-22T00:00:01Z","event":"log","data":{"service":"web","stream":"stdout","line":"ready"}}
```

Diagnostics, progress, warnings, update notices, and errors use stderr. JSON errors have this minimum shape:

```json
{"schema_version":1,"error":{"kind":"usage","code":"confirmation_required","message":"...","suggestion":"..."}}
```

No ANSI reaches a non-TTY stream. JSON mode never emits ANSI.

## Database connection profile

`agent-context --json` reports a non-secret `database_connection` object. The
PlanetScale profile is libpq 16 with `verify-full` TLS and system trust. Agents
must preserve those guarantees when adapting the URL to an application driver.
For `asyncpg`, pass `ssl=True` when constructing the connection or pool; do not
treat `sslrootcert=system` as a literal path and do not downgrade verification.
Run `repobox help connections --json` for the same guidance without loading a
project.

## Exit codes

| Exit | Kind | Agent behavior |
|---:|---|---|
| 0 | success | Continue. |
| 1 | runtime | Inspect error code and job state; retry only if suggested. |
| 2 | usage | Fix arguments or approval/input contract; do not retry unchanged. |
| 3 | not found | Discover resources/config first. |
| 4 | authentication | Run browser login or set both service-token variables. |
| 5 | conflict | Treat existing/in-progress/destructive-state conflict explicitly. |
| 6 | permission | Fix provider token grants; blind retries will not help. |

## Authentication

Never pass the service-token secret in argv. For CI or an agent process:

```sh
export PLANETSCALE_SERVICE_TOKEN_ID='...'
export PLANETSCALE_SERVICE_TOKEN='...'
repobox auth status --json --no-input
```

An interactive human runs `repobox auth login`; Repobox opens PlanetScale's device-approval page, prints the confirmation code, polls for approval, validates the resulting Bearer access token, and stores it in the normal credential layer. `repobox auth logout` revokes a stored browser token before removing it locally.

An agent that needs human approval can use:

```sh
repobox auth login --json --no-input
```

That command emits an `auth_pending` JSONL event containing `verification_url`, `user_code`, `browser_opened`, and `expires_in_seconds`, followed by a terminal `result` event after approval. It never reads stdin. If either service-token variable is present, service-token mode takes precedence and both variables must be non-empty. Database connection URLs and access tokens are never returned by status or agent-context.

## Jobs and recovery

Create and pull operations append checkpoints even though the initiating command normally waits. After disconnection:

```sh
repobox job view latest --json
repobox job view latest --exit-status --json
repobox job resume EXACT_UUID --yes --json --no-input
```

`latest` is accepted only on reads. Resume and cancel require an exact UUID. A failed pull leaves local services stopped until a successful resume.

For streamed PostgreSQL imports, inspect `data.steps[].message` on `job view`
and the final command error instead of inferring the cause from a closed pipe.
Repobox preserves these stable error codes:

| Error code | Meaning | Agent action |
|---|---|---|
| `planetscale_import_failed` | `psql` rejected the restore or lost the target connection. | Retain the redacted diagnostic, check provider state, and resume only after correcting the target failure. |
| `local_postgres_dump_failed` | The source-side `pg_dump` exited unsuccessfully. | Fix the local Compose service or dump compatibility before resuming. |
| `database_stream_interrupted` | The target exited successfully before consuming the complete dump. | Confirm no managed child remains, inspect the target state, then resume the exact UUID. |
| `operation_interrupted` | SIGINT or SIGTERM was observed and bounded cleanup completed at the latest durable checkpoint. | Follow the command-specific suggestion: create and pull resume an exact job UUID; delete and prune are rerun idempotently. |
| `operation_interrupted_cleanup_incomplete` | Cooperative cleanup failed or exceeded 30 seconds. | Inspect the named PlanetScale role, managed child, and Compose service before resuming. |
| `operation_cleanup_failed` | The operation reached a result, but a required role or source-service cleanup failed. | Resolve the named residual resource before retrying or resuming. |
| `native_runtime_cleanup_incomplete` | A native runtime missed its graceful shutdown window, or Repobox could not signal, stop, or reap it reliably. | Retain the cleanup diagnostic and inspect descendants and listening ports before another run. |
| `environment_recovery_required` | A create or pull lineage still owns the environment, including a terminal job with residual provider checkpoints. | Resume the exact UUID when permitted, or explicitly delete the environment before starting a different mutation or runtime. |
| `environment_mutation_lineage_conflict` | More than one unresolved create/pull lineage targets the environment. | Inspect every named UUID, then explicitly delete the environment to reconcile recorded provider resources. |
| `environment_not_ready_for_pull` | A new pull was requested for an environment whose previous mutation is incomplete. | Resume the exact earlier job or delete and recreate the environment; do not start a fresh pull. |
| `pull_resume_identity_missing` | A resumable pull lacks its durable provider identity checkpoint. | Make no provider mutation. Inspect the job and use explicit environment deletion to reconcile resources. |
| `pull_resume_identity_mismatch` | Current service configuration differs from the provider identity checkpointed by the pull. | Restore the matching configuration to resume, or explicitly delete the environment using its recorded resources. |
| `create_resume_identity_missing` | A legacy resumable create lacks a complete provider identity checkpoint. | Make no provider mutation. Inspect the job and provider manually; Repobox will not infer ownership from current configuration. |
| `create_resume_identity_mismatch` | Current service configuration differs from the provider identity checkpointed by the create. | Restore the matching configuration to resume, or explicitly delete the environment using its recorded resources. |
| `pull_staging_branch_missing` | A staged or credentialed pull checkpoint refers to a staging branch that no longer exists. | Do not recreate it implicitly or delete the canonical branch. Inspect the exact job and provider state, then reconcile by explicit deletion if needed. |
| `environment_binding_config_mismatch` | Stored local state points at a different provider database or branch than current configuration. | Repobox blocks runtime, create, and new pull before mutation. Restore the matching configuration or explicitly delete and recreate the environment. |
| `environment_delete_identity_missing` | Local state has no stored binding or create/pull checkpoint proving an exact delete target. | Inspect durable jobs and provider branches manually. Repobox will not delete a branch inferred only from current configuration. |
| `environment_provision_incomplete` / `environment_pull_incomplete` | All attempted calls returned, but durable steps or configured bindings are still incomplete. | Inspect the exact job and state; resume the lineage rather than treating the environment as ready. |

Process stderr is drained concurrently, limited to the last 64 KiB, and
credential-redacted. A `BrokenPipe` caused by a failed `psql` process does not
replace the provider diagnostic. If a pending backup is part of the recovery,
add `--wait`; it is not a generic retry flag.

Cancellation is checked between service operations and at safe pull phases. Once
an irreversible branch swap begins, Repobox finishes the current consistency
phase, checkpoints it, and stops before starting another service. Delete and
prune likewise stop between durable targets rather than continuing through the
remaining list; rerun the same delete or prune command to finish them.

`repobox env delete ENV --dry-run --yes --json --no-input` lists every exact
checkpointed `organization/database/branch` target. If Repobox cannot prove
ownership from stored bindings or create/pull checkpoints, dry-run and execution
both fail before provider access.

A native command that deliberately daemonizes or creates a new session owns that
detached child's cleanup. It must trap Repobox's graceful interrupt and wait for
the child before exiting; agents should verify expected listening ports after
stopping such a runtime.

## Compatibility

- `schema_version: 1` is the compatibility boundary for output and config.
- Additive fields and commands are allowed in v0.x; consumers must ignore unknown response fields.
- Field removal, semantic reuse, or exit-code reassignment requires a major version.
- Committed schemas under `docs/schemas/` are regenerated and diffed in CI.
- Update checks are at most daily, go to stderr, skip JSON/CI, and can be disabled with `REPOBOX_NO_UPDATE_CHECK=1`.
