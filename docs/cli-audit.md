# CLI design audit

Repobox declares tier **T3: platform CLI**. It has authentication, remote provider state, multiple resource nouns, durable long operations, a local dev loop, and agents as first-class callers.

- Name: `repobox`
- Audience: developers, shell automation, and LLM agents
- Nouns: auth, environments, services, jobs, config, telemetry
- Long-running operations: environment create/pull/resume, runtime, and logs
- Auth: browser-assisted PlanetScale service-token acquisition for humans; environment variables for CI

The canonical command tree and representative annotated `run` implementation are in [Agent and automation contract](agent-contract.md).

## Executed evidence

```text
$ scripts/audit-help.sh
All command help pages include concrete examples.

$ target/debug/repobox --json --version | jq -r '.command + " " + .data.version'
version 0.1.0

$ target/debug/repobox agent-context --schemas --json |
    jq '.data | {schema_version, command_groups: (.commands.subcommands | length), exit_codes: .contract.exit_codes}'
{
  "schema_version": 1,
  "command_groups": 17,
  "exit_codes": {
    "1": "runtime",
    "2": "usage",
    "3": "not_found",
    "4": "authentication",
    "5": "conflict",
    "6": "permission"
  }
}

$ cargo test --workspace --all-targets --locked
38 tests passed
```

The exact test count is expected to grow; CI treats any failure as blocking.

## Pattern audit

| Pattern | Status | Evidence |
|---|---|---|
| Command grammar | yes | `cli.rs` uses top-level workflow utilities and noun/verb groups with at most one subject positional. |
| Help and disclosure | yes | No args prints full help; every recursive page has examples; help topics and a recursive `agent-context` manifest ship. |
| Output modes | yes | Immediate JSON, long-operation JSONL, committed schemas, and schema-drift tests. |
| Stdout and stderr | yes | Data uses stdout; diagnostics/errors/update notices use stderr; JSON Compose operations suppress child chatter. |
| Exit codes | yes | One `ErrorKind` mapping defines stable exits 1–6 and the help topic documents them. |
| Auth | yes | Browser-assisted hidden entry for humans, environment credentials for CI, and unauthenticated status exits 4. |
| TTY and interactivity | yes | Prompt gates use `IsTerminal`; `--no-input`, `--yes`, and `NO_COLOR` are global. |
| Errors as UI | yes | Errors include kind, stable code, summary, suggestion, optional request ID/doc URL; mutations include undo metadata. |
| Config cascade | yes | Flag > env > project > user > default; structured input is flag > pipe > prompt. |
| Local dev loop | yes | Run/stop/status/logs, service filters, durable jobs, `latest --exit-status`, exact-ID mutation, and interrupt checkpoints. |
| Extensibility | n/a | v0.1 exposes internal traits but no third-party extension ecosystem. A later ecosystem is constrained to executable-on-PATH. |
| Versioning | yes | Plain and JSON version output; throttled/suppressible stderr update notices; no deprecated flags yet. |

## Tier-3 gate

The machine-auditable answers live in [cli-audit.json](cli-audit.json). They were validated with the `cli-design` rubric helper:

```json
{
  "skill": "cli-design",
  "tier": "T3",
  "violations": [],
  "warnings": [
    "Provider mutations are synchronous rather than background async commands; durable jobs still recover disconnects.",
    "v0.1 has no deprecated flags.",
    "v0.1 has no third-party extension ecosystem."
  ],
  "pass": true
}
```
