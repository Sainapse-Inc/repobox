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

All help pages include concrete examples. Long prose is available through `repobox help setup|agents|data|environment|environments|formatting|config|exit-codes`.

## Global controls

- `--json` selects versioned JSON or JSONL.
- `--dry-run` plans mutations without provider calls or local writes.
- `--yes` approves confirmation gates.
- `--no-input` forbids prompts, browser launch, and TUI entry.
- `--repo PATH` selects the repository.
- `--color auto|always|never` controls human stderr styling; `NO_COLOR` always disables it.
- `--environment NAME` or `REPOBOX_ENV` selects a data environment on relevant commands.

For non-interactive approved mutation, use all applicable controls:

```sh
repobox env create feature/demo --dry-run --json --no-input
repobox env create feature/demo --yes --json --no-input
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

## Exit codes

| Exit | Kind | Agent behavior |
|---:|---|---|
| 0 | success | Continue. |
| 1 | runtime | Inspect error code and job state; retry only if suggested. |
| 2 | usage | Fix arguments or approval/input contract; do not retry unchanged. |
| 3 | not found | Discover resources/config first. |
| 4 | authentication | Set both provider credential variables or run login. |
| 5 | conflict | Treat existing/in-progress/destructive-state conflict explicitly. |
| 6 | permission | Fix provider token grants; blind retries will not help. |

## Authentication

Never pass the service-token secret in argv. For CI or an agent process:

```sh
export PLANETSCALE_SERVICE_TOKEN_ID='...'
export PLANETSCALE_SERVICE_TOKEN='...'
repobox auth status --json --no-input
```

An interactive human may run `repobox auth login`; Repobox opens the provider token page and reads the token with echo disabled. Both variables must be set together. Database connection URLs are never returned by status or agent-context.

## Jobs and recovery

Create and pull operations append checkpoints even though the initiating command normally waits. After disconnection:

```sh
repobox job view latest --json
repobox job view latest --exit-status --json
repobox job resume EXACT_UUID --yes --json --no-input
```

`latest` is accepted only on reads. Resume and cancel require an exact UUID. A failed pull leaves local services stopped until a successful resume.

## Compatibility

- `schema_version: 1` is the compatibility boundary for output and config.
- Additive fields and commands are allowed in v0.x; consumers must ignore unknown response fields.
- Field removal, semantic reuse, or exit-code reassignment requires a major version.
- Committed schemas under `docs/schemas/` are regenerated and diffed in CI.
- Update checks are at most daily, go to stderr, skip JSON/CI, and can be disabled with `REPOBOX_NO_UPDATE_CHECK=1`.
