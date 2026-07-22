# Security policy

## Supported versions

Until Repobox has a stable release, security fixes are applied to the latest `main`. After v1, the current major release will be supported.

## Reporting a vulnerability

Please use GitHub private vulnerability reporting for this repository. Do not open a public issue containing credentials, connection URLs, provider request payloads, private data, or reproduction steps that create resources in an organization you do not own.

Include the affected version/commit, platform, impact, minimal redacted reproduction, and whether a credential or remote resource may already be exposed. Revoke exposed PlanetScale service tokens and database roles immediately; do not wait for triage.

## Security boundaries

- Service-token secrets are accepted through environment variables or a hidden prompt, never a CLI flag.
- Project configuration, state, jobs, generated agent guides, dry-run plans, and normal output must not contain passwords.
- Native credential storage is preferred. The explicit file fallback is local-only and permission-restricted, but users should prefer a working OS credential service.
- Local import performs no anonymization or policy enforcement. Only copy data you are authorized to place in the target provider organization.
- Provider mutations and Docker access execute with the current user's authority. Repobox is not a sandbox.

The [feature specification](docs/features/branch-scoped-data.md) contains the full security and privacy review.
