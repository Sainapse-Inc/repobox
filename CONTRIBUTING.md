# Contributing

Repobox uses stable Rust pinned by `rust-toolchain.toml`.

Before opening a pull request, run:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
scripts/audit-help.sh
scripts/generate-schemas.sh
git diff --exit-code -- docs/schemas
```

Public CLI JSON schemas are versioned contracts. Update their snapshots only
when the compatibility impact is intentional and documented in the changelog.

## CLI changes

- Keep noun/verb grammar and add a concrete example to every affected `--help` page.
- Preserve human/agent symmetry: a prompt or TUI path must have a `--no-input` alternative.
- Keep secrets out of argv. Use a hidden prompt, environment variable, or credential store.
- Immediate structured output is JSON; long-running output is JSONL through a final `result` event.
- Additive schema changes are preferred. Removal or semantic reuse requires a major version.
- Every mutation returns an undo command or an explicit reason no inverse exists.

Run the tier-3 rubric recorded in `docs/cli-audit.json` after changing the command surface.

## Provider changes

Provider code must remain behind the `DatabaseProvider` contract, preserve request IDs, retry only safe transient failures, handle pagination, and add a mocked wire test. Never run live provider tests from ordinary CI.

Before a release, a maintainer must execute [the live provider smoke](docs/live-smoke.md) in a disposable organization and prove cleanup. Missing credentials are a release blocker, not a reason to weaken the gate.

## Release

The release workflow expects:

- `CARGO_REGISTRY_TOKEN` for crates.io;
- `HOMEBREW_TAP_TOKEN` with access to `abhirupghosh/homebrew-tap`;
- a protected, signed-off stable tag named exactly `v<workspace-version>`
  after the live smoke;
- an explicit full commit SHA for every release, which the tag must resolve to;
- a dated `## [<workspace-version>] - YYYY-MM-DD` changelog heading;
- a tagged commit that is reachable from `origin/main`.

Release only through a manual dispatch of the reviewed workflow on `main`. Given an existing
release tag, use:

```sh
TAG=v0.1.0
COMMIT_SHA="$(git rev-parse "${TAG}^{commit}")"
gh workflow run release.yml --ref main -f tag="$TAG" -f commit="$COMMIT_SHA"
```

The repository variable `PLANETSCALE_OAUTH_RELEASE_MODE` is also a required, auditable release
decision. Set exactly one of:

- `dedicated_client`: the tagged code uses a separately registered, Repobox-owned OAuth client;
- `planetscale_approved`: PlanetScale has explicitly approved Repobox using the public
  PlanetScale CLI OAuth client for this release.

For example:

```sh
gh variable set PLANETSCALE_OAUTH_RELEASE_MODE --body dedicated_client
```

Do not set `planetscale_approved` without retaining PlanetScale's written approval in the release
evidence. Any unset or different value blocks the release before publication secrets are read.

Immediately before creating the tag, after the live smoke and OAuth release-mode decision are
complete, move the release notes out of `Unreleased` into the required dated version heading. The
workflow rejects a tag whose archived `CHANGELOG.md` has no matching heading.

Automatic tag-push releases are intentionally disabled. The workflow rejects a dispatch whose
`GITHUB_REF` is not `refs/heads/main`, then proves that the supplied tag resolves to the supplied
commit and that the commit is reachable from `origin/main` before it reads publication secrets.
This workflow accepts only plain `vX.Y.Z` tags; prereleases need a separate workflow that marks
the GitHub release as a prerelease and does not update the stable Homebrew formula.

Protect the `v*` tag namespace with a GitHub repository ruleset that blocks updates and deletion.
The workflow re-fetches and checks the tag before staging assets, immediately before crates.io,
and before final publication, but those checks do not replace server-side tag protection.

Every third-party `uses:` reference in the release workflow is pinned to a full commit SHA. The
trailing comment records the reviewed upstream version (or branch and pinned toolchain for actions
without immutable release tags). Update a pin only in a reviewed pull request: resolve lightweight
tags directly and annotated tags through their peeled `^{}` ref, verify that commit in the official
upstream repository, update the SHA and comment together, and rerun `actionlint` plus the release
shell simulations. Never replace a release pin with a mutable branch, major-version, or `master`
reference.

Missing publication credentials fail the release preflight; they never turn a publication into a
silent skip. GitHub pins every checkout to the validated tag commit and reruns the full Linux and
macOS quality, contract, dependency, and secret gates instead of trusting earlier branch CI. It
then builds and verifies the four Linux/macOS archives, emits checksums and provenance
attestations, and stages the exact asset set in a commit-bound GitHub draft. Only that
workflow-owned draft can be repaired on a rerun. A conflicting draft or incomplete published
release fails closed before crates.io. Cargo then packages all four exact `.crate` files with the
pinned toolchain and publishes them in dependency order. An existing name/version is skipped only
when crates.io reports it as non-yanked and its published checksum equals the local artifact's
SHA-256; any mismatch blocks the whole crate sequence. The same checksum check applies after a
failed or concurrent publish. After crates succeed, the validated draft becomes the public GitHub
release. A new Homebrew formula must pass the new-formula audit; an update must pass strict online
audit. Both paths must also pass `brew style`, install, and formula tests before the tap is pushed,
and neither preflight nor the final tap step permits a version downgrade.

Registry publication and GitHub/Homebrew publication cannot be one atomic transaction. The
workflow reduces partial-release risk by completing all builds and asset validation before the
first crate publish, serializing release runs, and making later steps safe to rerun. Every GitHub
release includes `RELEASE-MANIFEST.json`, binding its tag, commit, version, and archive checksums.
On a rerun, a release with the same tag, commit, and version is authoritative: its archives and
checksums are verified and reused by Homebrew rather than overwritten with a potentially
non-reproducible rebuild. A missing, invalid, or identity-mismatched manifest blocks the rerun;
investigate or explicitly remove an incomplete release only after verifying its provenance. Do not
create a tag if any publication credential or live-smoke evidence is missing.
