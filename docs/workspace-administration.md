# Workspace service access and policy

Workspace ownership belongs to trusted service authentication. A browser user
account, subscription name, and payment processing remain the calling app's
responsibility. Each backend workspace has independent work ownership and usage;
public source evidence remains shared.

Local installations can continue using `CHESS_CRAWL_API_TOKEN`, its secret-file
equivalent, or a static workspace-token map. These settings are loaded at API
startup. Hosted installations can select `CHESS_CRAWL_API_AUTH_MODE=database`.
This mode rejects simultaneous static credential configuration and resolves
each authenticated request against PostgreSQL. It requires migration `0017`.
API construction does not open or migrate the database. Liveness remains public;
readiness and product endpoints authenticate. A database outage returns 503.

Container readiness in database mode uses a separately provisioned credential
from `CHESS_CRAWL_HEALTHCHECK_TOKEN` or `CHESS_CRAWL_HEALTHCHECK_TOKEN_FILE`.
Provision its workspace and credential before starting API tasks, then deliver
the token through the deployment secret manager. The probe sends it only to the
local `/health/ready` endpoint. Static mode can continue falling back to the API
token; database mode never uses that fallback. An explicitly configured empty,
missing, or conflicting health credential fails the probe without logging it.

## Provision and inspect

Use `chess-crawl-admin workspaces` with trusted operator database access. No HTTP
administration endpoint grants credential or quota authority. The private SaaS
can call the same storage services from its trusted provisioning integration.
AWS runtime database credentials have read access to workspace credential
digests but cannot insert, update, or delete credentials. Provision and rotate
using operator/migration-owner access, and rerun the role bootstrap after this
migration to install that privilege boundary. Workspace budget tables retain
runtime write access for legacy admission and usage accounting.

Create a JSON policy file containing `BudgetPolicy` fields, for example:

```json
{
  "job_max_games": 10000,
  "workspace_max_games": 100000,
  "workspace_max_active_jobs": 2,
  "workspace_max_queued_jobs": 32
}
```

Omitted fields take the finite application defaults declared in `BudgetPolicy`;
the complete effective policy is stored. Unknown fields, non-integer values,
and values outside the positive PostgreSQL bigint range are rejected. This is
a full replacement policy, not a patch to the previous version.

```bash
chess-crawl-admin workspaces provision --workspace-id customer_42 --policy-file policy.json
chess-crawl-admin workspaces show --workspace-id customer_42
```

Provision creates the workspace, managed policy version 1, and its first service
credential in one transaction. It fails if the workspace already exists. `show`
returns current policy, monthly usage, and at most 100 recent credential records
with a truncation flag. It never returns a token or its digest.

Provision, issue, and rotate return a new bearer token **once**, after the
transaction commits. Deliver that output directly to the SaaS secret manager;
do not copy it into logs or browser configuration. PostgreSQL stores only a
SHA-256 digest of the randomly generated 256-bit token. Lost output cannot be
recovered; issue or rotate again.

## Issue, rotate, and revoke

```bash
chess-crawl-admin workspaces issue --workspace-id customer_42
chess-crawl-admin workspaces revoke --workspace-id customer_42 --credential-id CREDENTIAL_ID
chess-crawl-admin workspaces rotate --workspace-id customer_42
```

Issue adds an independently revocable credential. Revoke is idempotent and must
name the owning workspace. Rotate atomically revokes **all** existing workspace
credentials and issues one replacement. For overlapping rollout, issue a new
credential, distribute it, then revoke each obsolete credential.

Revocation applies to subsequent authentication on every API replica, without a
restart or credential cache. A request already authenticated can finish. These
operations preserve workspace identity, stored outputs, idempotency keys, usage,
and existing jobs; they do not cancel already admitted work.

## Change allowances

```bash
chess-crawl-admin workspaces set-policy --workspace-id customer_42 \
  --policy-file updated-policy.json --expected-version 1
```

Inspect the current version first. The expected-version check rejects competing
operator updates rather than overwriting a newer policy. A successful update
creates the next policy-history revision. Existing local/static workspaces can
also adopt managed policy through this command after their first submission has
installed a policy. If `show` returns `policy: null` for an existing workspace
(including a freshly migrated `local`), `--expected-version 0` explicitly
initializes managed policy version 1 without a product submission. Version 0
cannot overwrite an already installed policy.

Managed policy is authoritative across API replicas and worker stages. Startup
budget defaults cannot silently tighten it on the next request. New runs record
the effective policy and its workspace revision. Existing run lifetime ceilings
and consumed usage survive upgrades, downgrades, rotation, and retries. Shared
monthly quotas and admission concurrency use the current policy; a downgrade
below already consumed usage blocks further work until the quota allows it.
Monthly rollover does not reset lifetime run usage. Historical runs created
before migration have no invented admission revision.

Unmanaged local workspaces retain the existing environment-driven tightening
behavior. `chess-crawl-admin budgets resume` remains an explicit operator action
to extend an existing run and its shared allowances, preserving spent usage; any
shared-policy extension also records a revision. Merely raising workspace policy
does not automatically resume a run blocked by its lifetime budget.
