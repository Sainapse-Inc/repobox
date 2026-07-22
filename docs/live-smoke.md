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

   Expected: stdout is JSONL only, remote Postgres services do not run locally, application services receive valid URLs, and Ctrl-C yields `runtime_stopped` after Compose stops.

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

## Release evidence

Record the Repobox commit, Rust version, Docker Compose version, provider organization (not credentials), timestamps, command exits, job IDs, request IDs, created resource IDs, cleanup confirmation, and any provider behavior that differed from the mocked wire tests.
