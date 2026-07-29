# Live PlanetScale smoke

This checklist is the release gate for provider behavior. It creates billable remote branches and may import data. Use only a dedicated PlanetScale organization and remove every resource afterward.

## Prerequisites

- Rust and Docker Compose as pinned by the repository.
- A disposable Git repository with at least one Compose PostgreSQL service and one local application service.
- A disposable OS user/profile for browser-login verification; login replaces any previously stored Repobox provider credential.
- A PlanetScale service token authorized to list organizations and cluster sizes and to create/delete databases, backups, branches, and roles in the disposable organization.
- These environment variables:

```sh
export PLANETSCALE_SERVICE_TOKEN_ID='...'
export PLANETSCALE_SERVICE_TOKEN='...'
export REPOBOX_LIVE_TEST_ORG='your-disposable-org'
```

Do not put values in shell history, fixtures, issue text, logs, or `.env` files committed to Git.

## Procedure

1. Build and verify offline.

   ```sh
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
   cargo test --workspace --all-targets --locked
   cargo build --release --locked
   ```

   Expected: every command exits zero.

2. Verify browser authentication, revocation, service-token automation, and discovery.

   ```sh
   env -u PLANETSCALE_SERVICE_TOKEN_ID -u PLANETSCALE_SERVICE_TOKEN \
     target/release/repobox auth login --no-browser
   env -u PLANETSCALE_SERVICE_TOKEN_ID -u PLANETSCALE_SERVICE_TOKEN \
     target/release/repobox auth status --json --no-input
   env -u PLANETSCALE_SERVICE_TOKEN_ID -u PLANETSCALE_SERVICE_TOKEN \
     target/release/repobox auth logout --yes --no-input --json
   target/release/repobox auth status --json --no-input
   target/release/repobox doctor --online --json --no-input
   ```

   Expected: browser login prints a PlanetScale approval URL/code, status reports `browser_oauth`, and logout reports `revoked: true`. The following status resolves the exported service token and reports `service_token`; the provider API check is true. `doctor` may still report missing project config until init.

3. Preview repository initialization.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture \
     init --organization "$REPOBOX_LIVE_TEST_ORG" \
     --dry-run --json --no-input
   ```

   Expected: no `.repobox.yml`, `CLAUDE.md`, or `AGENTS.md` is created; all detected Postgres services appear.

4. Initialize and inspect.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture \
     init --organization "$REPOBOX_LIVE_TEST_ORG" \
     --yes --json --no-input
   target/release/repobox --repo /absolute/path/to/fixture \
     config validate --json --no-input
   ```

   Expected: strict config is created, managed guide blocks preserve surrounding text, and no credential appears in repository files.

5. Preview and create the first environment.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture \
     env create smoke/main --create-backup --dry-run --json --no-input
   target/release/repobox --repo /absolute/path/to/fixture \
     env create smoke/main --create-backup --yes --json --no-input
   ```

   Expected: dry-run makes no API mutation. Create returns a ready environment, one binding per database, a succeeded job, and an undo command.

   PlanetScale database, backup, and branch transitions can each take several minutes. A
   healthy operation may remain pending for more than ten minutes; let Repobox's readiness
   budget expire or inspect the durable job before treating a quiet transition as failure.

6. Verify idempotence.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture \
     env create smoke/main --yes --json --no-input
   ```

   Expected: no duplicate branch or role is created; the same binding remains ready.

7. Run local services with structured logs.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture \
     run --environment smoke/main --yes --json --no-input
   ```

   While the runtime is active, use a second terminal with a conflicting
   environment variable to verify that the explicit logs selector wins:

   ```sh
   REPOBOX_ENV=smoke/not-running \
     target/release/repobox --repo /absolute/path/to/fixture \
     logs app --environment smoke/main --tail 50 --json --no-input
   ```

   Expected: stdout is JSONL only, remote Postgres services do not run locally,
   application services receive valid URLs, the logs command returns events
   from `smoke/main`, and Ctrl-C yields `runtime_stopped` after Compose
   stops.

8. Exercise forward-only refresh.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture \
     pull --environment smoke/main --dry-run --json --no-input
   target/release/repobox --repo /absolute/path/to/fixture \
     pull --environment smoke/main --yes --json --no-input
   ```

   Expected: staging is restored before old-branch deletion; the final branch has the canonical name; mutation output says no undo is available.

9. Exercise interruption and resume using a disposable second environment. Interrupt after a job checkpoint, then inspect and resume with the exact UUID.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture job view latest --json
   target/release/repobox --repo /absolute/path/to/fixture \
     job resume 018f6f4e-7040-7000-8000-000000000001 --yes --json --no-input
   ```

   Expected: replace the illustrative UUID with the returned ID; completed steps are not duplicated and the job reaches `succeeded`.

10. Exercise a permission or multi-service partial failure with a deliberately restricted disposable token or invalid second database mapping.

    Expected: the error kind is `permission` or the environment is `degraded`, successful service bindings remain recorded, the provider request ID is retained when supplied, and the suggested resume command contains an exact job UUID.

11. Delete environments explicitly.

    ```sh
    target/release/repobox --repo /absolute/path/to/fixture \
      env delete smoke/main --dry-run --json --no-input
    target/release/repobox --repo /absolute/path/to/fixture \
      env delete smoke/main --yes --json --no-input
    ```

    Expected: all environment branches and Repobox roles are absent. The base database remains unless it was created solely for this smoke and is removed manually in the next step.

## Cleanup

Use the PlanetScale dashboard/API to list the disposable organization and verify there are no `rbx-` staging or environment branches, Repobox roles, or unintended backups. Delete smoke-only databases manually after confirming no other user owns them. Repository-local state can then be removed by deleting the disposable fixture directory.

Never automate broad organization deletion from this runbook.

## Failure handling

- If failure occurs before `old_deleted`, inspect the job and safely resume forward.
- If failure occurs at or after `old_deleted`, do not recreate an old branch from memory; resume the staging rename and credential transfer.
- If credentials appear in output, stop immediately, revoke the OAuth access token, service token, and database role as applicable, preserve only redacted evidence, and block release.
- If cleanup cannot be proven, do not tag a release. Record the exact provider resource IDs and resolve ownership first.

### Import failure recovery

For `bootstrap.mode: import`, distinguish a provider failure from the local
stream symptom before retrying:

1. Inspect the exact job and retain its redacted error.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture \
     job view EXACT_UUID --json --no-input
   ```

2. If `planetscale_import_failed` contains an unexpected EOF, invalid socket,
   or lost connection, check PlanetScale's restart reason and cluster logs. A
   client-side broken pipe alone is not evidence of the root cause.
3. For a confirmed out-of-memory restart, resize the existing base database
   and wait until PlanetScale reports the resize complete. Also pin the chosen
   SKU in `remote.cluster_size`; changing the config alone does not resize an
   existing database.
4. Resume the same job once.

   ```sh
   target/release/repobox --repo /absolute/path/to/fixture \
     job resume EXACT_UUID --yes --json --no-input
   ```

5. If the controlled retry also restarts for out-of-memory, stop. Record the
   failing table or widest serialized row if known, then choose a larger
   cluster or contact PlanetScale support. Do not run an unbounded retry loop.

Expected: `pg_dump` and `psql` have exited after every failure or cancellation,
no `io.repobox.managed=psql` fallback container remains, the source Compose
service retains its original running state, and the resumed job either succeeds
or returns the provider diagnostic without exposing credentials.

Repeat once with the source service initially stopped and a local `psql` older
than version 16 (or absent). Expected: Repobox waits for `pg_isready`, uses the
managed PostgreSQL 18 client for the PlanetScale connection, and restores the
source service to stopped on success, failure, or cancellation.

When the application does not use libpq, also start it through the real
database adapter. Verify that the driver accepts PlanetScale's
`sslrootcert=system` semantics or securely maps them to its native system trust
store. A successful `psql` import alone does not prove application-driver TLS
compatibility.

## Release evidence

Store the Repobox commit, tool versions, timestamps, command exits, job IDs,
request IDs, created resource IDs, cleanup confirmation, and provider behavior
in an access-controlled release artifact. The public repository may retain a
sanitized outcome and approximate timings, but never organization names,
project or database names, job or request IDs, branch or backup IDs, private
application identifiers, table names, row counts, or data digests.

### Sanitized historical diagnostic

A pre-release disposable lifecycle run found that provider database, backup,
and branch transitions can exceed ten minutes. Exact resume recovered pending
resources, but the run also exposed role-identity and confirmation defects.
Cleanup removed the disposable provider and Docker resources. The result was a
failed release gate; the repository keeps only this sanitized summary while the
identifier-bearing evidence remains in the private release record.

### Private application import validation: 2026-07-23

This run validated the import and native-runtime paths against a real,
multi-gigabyte private application database. Exact application names,
provider resource IDs, table names, counts, and digests are intentionally
omitted from this public repository. The private durable job and provider
records retain the audit trail. This is application-pilot evidence, not a
replacement for the disposable lifecycle release gate above.

- Two smaller target sizes restarted for out-of-memory while restoring wide
  JSONB rows. After the existing database was resized to PS-20, resuming the
  same durable job completed the logical restore without another OOM restart.
- Source and target matched on schema shape, migrations, application reference
  data, stable per-table row counts, and a digest of those counts. Subsequent
  local drift was confined to tables receiving new source writes after the
  snapshot.
- The application's real async database adapter connected with certificate and
  hostname verification, compiled its database-backed registry, and completed
  a temporary write/read/delete transaction with zero persistent test rows.
- `repobox run` started both the backend and frontend against the remote branch.
  Their isolated health endpoints returned HTTP 200 while background workers,
  caching, scheduled sync, and synthetic probes were disabled for the pilot.
- Ctrl-C returned exit zero only after `runtime_stopped`. Both isolated ports
  were free afterward, no pilot Python, Node, frontend, or Repobox process
  remained, and the original local PostgreSQL container remained running with
  zero restarts.

The PS-20 database, environment branch, role, and import backup are intentionally
retained for continued private pilot testing and remain billable. Remove the
exact resources recorded by the durable job when the pilot ends; do not run broad
organization cleanup.
