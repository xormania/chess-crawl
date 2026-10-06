# Hosted-service boundaries

Local installations and the hosted service use the same Chess-Crawl contracts.
Hosting changes configuration and service placement. Customer accounts, billing
and chess intelligence belong in the separate private SaaS repository.

| Component | Responsibility | Placement |
| --- | --- | --- |
| Chess-Crawl API | Workspace submissions, archive reads, working sets and stored results | Public backend; independently replicated |
| Acquisition workers | Provider requests, exact capture, source checkpoints | Public backend; separately sized |
| Processing workers | Normalization and local evidence processing | Public backend; separately sized |
| PostgreSQL | Durable scheduling, ownership, policies and source references | Shared database |
| Object storage | Exact compressed source evidence | Shared local volume or private S3 |
| Chess Dog | Symfony product interface using the API | Public local application and reusable UI |
| Hosted SaaS | Accounts, billing, onboarding and subscription policy mapping | Private product repository |
| Chess intelligence | Models, inference placement, evaluation and training | Private services behind product-owned contracts |

The private product maps Free, Basic, Pro and Advanced subscriptions to explicit
workspace policy values through trusted administration. Plan names and payment
records stay in the product database. Browser callers must not receive operator
database credentials or another workspace's backend credential.

Chess-Crawl supplies reproducible input selections and scoped result storage.
Private services decide which positions need inexpensive inference or expensive
evaluation, record model/evaluation versions and select training observations.
Changing those decisions should not require changing acquisition or publishing
model internals.

## Scale service roles independently

The API admits durable work and reads committed state. PostgreSQL scheduling is
authoritative; optional SQS messages are wake-up hints. Losing or duplicating a
hint does not transfer ownership. Workers retain dedicated database sessions for
advisory locks and verify fencing before writes and HTTP.

Acquisition replicas provide independent provider work and recovery capacity.
A provider's shared acquisition permit and persisted pacing still bound its
throughput; adding replicas does not authorize more requests to that provider.
Processing replicas work over captured evidence without provider permits.
API replicas handle request traffic without running acquisition in web processes.

Replicas need the same database, compatible code/schema, policy settings and
object-store namespace. Local Compose supplies shared volumes and an optional
API proxy; AWS supplies PostgreSQL, S3 and separately sized ECS roles. Use the
dedicated operator role for migrations and follow drain/restart instructions.
Do not assume mixed application versions are safe during schema changes.

## Keep configuration and product policy separate

[Shared configuration](configuration.md) validates each role offline. Environment
overrides optional TOML configuration; typed domain settings own defaults and
validation. Secrets use explicit secret sources.

Infrastructure configuration chooses storage, transport, queues, service sizing
and availability. Workspace policies choose admitted work and storage ceilings.
Changing a subscription does not reset usage or existing lifetime run budgets.
Trusted administrators can explicitly extend and resume retained runs. See
[workspace administration](workspace-administration.md) and
[work budgets](work-budgets.md).

Notifications are optional and independently retryable. Polling reads current
API state; Mercure adds updates. Neither delivery mode owns work scheduling.
Check eligible backlog, provider waiting reasons, database connections and
storage capacity before increasing task counts.

The public repositories remain useful through basic local Compose. The private
product can consume releases or reviewed upstream updates while keeping billing
and intelligence independently deployable.
