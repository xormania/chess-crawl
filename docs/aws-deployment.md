# AWS deployment and cost foundation

`deploy/aws/template.json` defines a private backend foundation using the same
PostgreSQL, API, worker, and source-storage boundaries as local Compose. It does
not provision resources merely by being checked in. Services default to zero
tasks; migrate, validate access, and pass the release gates before raising counts.

The template needs the compressed archive work (PR #26) and the staged SQS
worker/dispatcher work. Build an image containing all prerequisite changes; a
template alone does not make an older image support queue execution. The frontend
is independently deployed and is not changed by this work.

## Local archive deployment

Compose uses the named `archive_data` volume at
`/var/lib/chess-crawl/archive`. A one-shot initializer sets only the mount root
to mode 0700 and UID/GID 10001. It does not recursively rewrite objects.
Initializer capabilities are limited to CHOWN/FOWNER so reinitializing a volume
already owned by the application remains valid. The API gets a read-only mount;
acquisition workers get a writable mount. Containers
retain read-only root filesystems. Initializer success gates both services.
Compose explicitly sets local mode and this absolute directory; setting
`CHESS_CRAWL_ARCHIVE_BACKEND=s3` in `.env` does not change those container settings.
The AWS template explicitly sets S3 mode. A different Compose backend needs an
operator-reviewed override with the appropriate settings and storage access.

Existing inline PostgreSQL bodies stay readable. To move them, run the archive
relocation helper from a one-off worker container, which has the writable mount:

```bash
docker compose run --rm worker python -m chess_crawl.storage.archive_migration --batch-size 100
```

Back up both PostgreSQL and `archive_data`. Restoring only the database loses the
source objects it references. See [archive storage](archive-storage.md) for the
transaction contract, resume behavior, and recovery requirements. Other local
operational containers that access source bytes need the same archive mount.

## Moving a local dataset to AWS

Object references record their backend and location. Restoring a Compose database
into RDS does not make its local filesystem objects readable by Fargate. Preserve
both backups, pause acquisition/import writers, and run the transfer helper from
an operator process that can read every recorded source location. A one-off local
worker container has the existing Compose object mount. Configure its destination
as the stack's retained S3 bucket and supply short-lived operator role credentials
through the normal SDK credential chain; allow only target-prefix PUT/GET and
any needed source-bucket GET. Never copy credentials into archive records.

For a source-run operator process with the archive mounted at its recorded path:

```bash
export CHESS_CRAWL_ARCHIVE_BACKEND=s3
export CHESS_CRAWL_ARCHIVE_S3_BUCKET=your-stack-archive-bucket
python -m chess_crawl.storage.archive_transfer --batch-size 100 --after-object-id 0
```

Compose pins local mode, so a one-off container must explicitly override
`CHESS_CRAWL_ARCHIVE_BACKEND` and `CHESS_CRAWL_ARCHIVE_S3_BUCKET` with
`docker compose run -e ... worker`, and separately receive its temporary SDK
credentials securely. Its persistent source volume stays mounted. Fargate cannot
perform this initial local-filesystem transfer by itself.

The helper verifies original and encoded source checksums, copies the exact gzip
bytes, verifies destination readback, then atomically repoints current raw/import
references for that source object. Raw/import IDs, workspace ownership and source
provenance remain unchanged. Earlier commits survive a later failure; rerunning
with the previous cursor skips references already moved. It performs no provider
requests or source deletion. Retain the old object files and metadata as backups.

Save each returned `next_after_object_id` and pass it as `--after-object-id` while
`has_more` is true. Batches are bounded by distinct source objects, not bytes or
reference counts. No exact remaining count scans the entire archive. The cursor
is a checkpoint, not an exclusive lease or completion guarantee while writers
are active: references created during publication join the atomic cutover, but
later references to older objects can arrive behind the cursor. With writers
paused, make a final sweep from cursor zero until no objects move and `has_more`
is false. Run `archive_migration` with the same destination for historical inline
bodies; transfer intentionally handles only existing external objects.

Take a new PostgreSQL backup after the verified transfer and inline relocation,
restore it into RDS using the PostgreSQL operations guide, rerun cloud role grants,
and test historical raw/import reads before starting runtime tasks. Resume writers
with S3 configured for all new writes. Changing the backend setting alone affects
new writes and does not relocate historical references. Live source-to-S3 transfer,
restore, and scoped reads remain operator release checks.

## Defined AWS resources

| Responsibility | Resource and boundary |
| --- | --- |
| Archive database | Private encrypted RDS PostgreSQL 18, gp3 storage, configurable minor/class/size/backups/Multi-AZ, snapshots on removal/replacement, deletion protection by default. |
| Source evidence | Private S3 bucket, encryption, ownership enforcement, public-access block, TLS-only policy, versioning, retained on removal/replacement. No expiry policy deletes referenced evidence. |
| Dispatch | Encrypted standard SQS queue and retained DLQ; configurable visibility and retry count. PostgreSQL remains durable job truth. |
| API access | Internal ALB with ACM TLS listener, allowed only from the supplied frontend security group. ALB-to-container HTTP is confined to task security groups. |
| Runtime | Separate digest-pinned Fargate API, worker, dispatcher task definitions; desired counts default to zero. Task-scoped ephemeral `/tmp` is writable by UID 10001; root filesystems remain read-only. |
| Migration | One-shot task using RDS-managed master credentials; migrates and grants the separate restricted runtime role. Runtime tasks never receive master credentials. |
| Logs | CloudWatch log group with configurable retention; anonymous workload records enabled in cloud tasks. |

The template references an existing VPC, at least two private subnets in different
availability zones, frontend security group, ACM certificate, private ECR image,
and application secrets. It creates no public IPs, internet gateway, NAT gateway,
or VPC endpoints. Provide dependency connectivity through existing routes or
endpoints before starting tasks. Workers also need outbound HTTPS to chess
providers, which AWS-service endpoints alone do not provide.

The ALB DNS output is for a private Route53 alias matching `ApiHostname`; create
that record in your existing hosted zone. The frontend calls this hostname over
TLS from the allowed group and authenticates with the service API token. Browsers
must not receive the backend service token.

## Secrets, image, and database bootstrap

Supply ARNs, not secret values, in parameters:

- `ApplicationDatabaseSecretArn`: existing JSON secret with a `password` field
  containing at least 32 characters. `ApplicationDatabaseUser` defines its role.
- `ApiTokenSecretArn`: existing plaintext bearer service-token secret.
- `ApplicationSecretKmsKeyArn`: the optional customer-managed key for these
  application secrets; omit it for AWS-managed encryption.

Use one customer-managed key for both application secrets when supplying that
parameter, or extend the execution policy explicitly for additional keys.
The RDS-managed master secret is separate and accessible only to migration task
execution. IAM task roles grant object reads to API, object writes/queue receive
to workers, and queue send to dispatcher. The adapters use SDK role credentials;
no AWS access keys enter the template or archive references.

The Docker image includes the optional `s3` extra (Boto3 also supplies SQS),
archive initializer, and the official RDS global CA bundle. Its origin/date/hash
are pinned in `docker/rds-ca-bundle.json`. Update the bundle deliberately and
rebuild the image when trust roots rotate. Connections use `verify-full` through
the shared verified transport policy. Do not substitute disabled verification.

Build and push an immutable private ECR image through your existing publishing
workflow, then supply its digest reference in `ContainerImage` and the exact
repository ARN in `ContainerRepositoryArn`. The image architecture is x86-64.
`TaskCpu` and `TaskMemory` are configurable; a CloudFormation assertion rejects
unsupported Fargate combinations. Defaults are 0.5 vCPU / 1 GiB and zero tasks,
not a claim that those sizes fit every workload.

Fargate uses a task-scoped ephemeral bind volume for `/tmp`; it does not support
the ECS `tmpfs` setting. The image declares the same path as a Docker `VOLUME`
with UID/GID 10001 and mode 0700 so ECS copies writable application permissions.
Scratch data, including finite export spools and worker process bindings, is
removed with its task. The default Fargate ephemeral allocation also holds the
image; review concurrent export limits and scratch capacity before increasing
workloads. Local Compose uses a `/tmp` tmpfs with explicit UID/GID 10001 and mode
0700 because a mounted tmpfs hides the image directory's ownership.
The API probes authenticated readiness, and the worker probe checks its own
local process incarnation and database heartbeat. An unrelated live worker
cannot satisfy that task's health check.
The ALB idle timeout is explicitly 120 seconds, above the default 60-second
export preparation deadline before response headers. If operators raise
`CHESS_CRAWL_EXPORT_PREPARE_SECONDS`, update that timeout with additional
scheduling/serialization headroom in the same reviewed deployment change.

The migration task runs:

```bash
python -m chess_crawl.storage.cloud_bootstrap
```

ECS supplies master `CHESS_CRAWL_DATABASE_URL`/password and application
`CHESS_CRAWL_APPLICATION_DATABASE_USER`/password from role-specific secret
references. The helper reuses the schema initializer and atomically grants runtime
CONNECT, public-schema USAGE, table CRUD, and sequence usage. It excludes writes
to migration history and grants no role management, database creation, or schema
creation, including temporary tables. It revokes the database's default PUBLIC
TEMPORARY privilege because these workloads need no temporary DDL. It refuses to
reuse a role with administrative flags, memberships, ownership, or unexpected
explicit grants (including other schemas, functions, or grant options). The helper supports a non-superuser database owner with CREATEROLE,
matching the RDS master privilege model: unsafe runtime flags are refused before
rotation, and ALTER ROLE changes only LOGIN, inheritance and password settings.
Repeat this task after every schema upgrade to grant access to new tables. Failure logs never print passwords or database SQL error text.

The packaged migrations include work budgets (0012), worker-heartbeat retention
(0013), and admission/capture lookup indexes (0014). Bootstrap must apply these
before the API or workers start. Trusted quota settings use the same defaults
as local runs; configure shared API/worker environment overrides together when
changing policy. Operators inspect or extend retained budgets with
`chess-crawl-admin budgets show` and `budgets resume`; see
[work budgets](work-budgets.md). HTTP clients cannot extend those ceilings.

Use a dedicated application database and runtime role. PostgreSQL retains
standard PUBLIC privileges such as built-in function execution, language/type
usage, and database connection; these are not schema-management privileges. Do
not add PUBLIC-writable schemas or privileged SECURITY DEFINER functions to this
database, or reuse the role in other databases. Audit extensions and custom
PUBLIC grants before release; bootstrap is not a general privilege scrubber for
an existing shared cluster. See the official [PostgreSQL privilege model](https://www.postgresql.org/docs/18/ddl-priv.html).

## Validation and operator sequence

Offline checks require no AWS credentials:

```bash
uv tool run --from cfn-lint==1.57.1 cfn-lint deploy/aws/template.json
uv run python -m pytest -q tests/test_cloud_deployment_contract.py tests/test_cost_measurements.py
```

With your chosen AWS account/region, before provisioning:

1. Confirm PostgreSQL 18 minor-version and instance-class availability with
   `aws rds describe-db-engine-versions --engine postgres` and
   `aws rds describe-orderable-db-instance-options --engine postgres --engine-version 18.6`.
2. Review a CloudFormation change set with `CAPABILITY_IAM`; keep all desired
   counts zero. Review region prices, credits, existing network costs, and retained
   resources. RDS and the ALB incur charges even when ECS counts are zero.
3. Provision only after the operator approves that concrete change set. Run
   `MigrationTaskDefinition` using the output cluster/security group and supplied
   private subnets; require its container exit code zero.
4. Validate runtime-role DB access with verified TLS, object round trips and
   recovery, and actual SQS dispatch/receive/retry. Check logs without exporting
   credentials. Update desired counts to one API, one worker, one dispatcher.
5. Configure the private DNS alias and verify authenticated readiness from the
   frontend network. Confirm service health and durable queue/DB progress before
   increasing worker counts or opening access to customers.

The template is statically validated; it has not been deployed here. cfn-lint
does not prove account permissions, subnet routing, ACM/DNS matching, regional
engine/class availability, or successful ECS startup. Real RDS bootstrap, S3,
SQS, private TLS integration, restart/failure recovery, load tests, restore drills,
and credential rotation remain release gates. Customer authentication and workspace
authorization must be integrated from the corresponding application PRs before
exposing the service as SaaS. GPU/engine/model pools and automatic worker scaling
are later deployments, not resources secretly created by this template.

This template does not deploy the optional event publisher or Mercure hub.
The JSON API supports polling durable job state. Streaming updates require their
own private publisher/hub deployment and frontend integration before that
capability is advertised; database outbox retention must be monitored meanwhile.

For a deliberate teardown, first set both database and load-balancer deletion
protection parameters to `false` in a reviewed update. Stack deletion retains S3
objects/queues/logs and creates database snapshots. Those retained resources can
still incur charges and must be independently inventoried and removed only when
their evidence is no longer needed.

## Workload evidence and cost estimation

`CHESS_CRAWL_USAGE_LOG=true` emits `chess_crawl_usage` JSON records for archive
publication, reads, and deduplication. Records carry wall/process CPU seconds,
source/compressed byte counts, logical object operations, outcome, and format
version. They exclude identities, bodies, source URLs/keys, credentials, and
exception messages. Failed work remains visible. Missing imported-game counts
are null; the summary reports which counters were supplied.

Process CPU measures the Python process, not engine subprocesses, GPUs, or a
perfect attribution among concurrent threads. Object counters are logical
application operations, not exact billed requests including SDK retries. Archive
stage durations can overlap other stages, so summing them is not allocated task
time. Use ECS running task-seconds and configured vCPU/GiB for Fargate projections;
use S3/queue metrics or billing exports for real request quantities.
Deduplication counts evidence-reuse decisions, which may repeat within one
ingestion path; it does not count unique games or customer requests. ECS log
delivery is non-blocking with a bounded buffer so a logging outage does not stall
archive writes. Dropped logs can make this sample incomplete; compare it against
service-level usage metrics and billing evidence.

Export only those JSON records to a clean local file, then:

```bash
uv run python -m chess_crawl.costs usage.jsonl --billing-input billing-input.json
```

`billing-input.json` contains `units`, `rates`, `pricing_as_of`, optional
`currency`, and `applicable_credits`. Supported units include allocated Fargate
vCPU/GiB hours, S3 GiB-months/PUT/GET requests, SQS requests, RDS instance-hours and
storage, ALB hours/LCUs, log ingestion, NAT hours/bytes, endpoints, and transfer.
Rates must be prices per the named unit, after converting prices quoted per
1,000/1,000,000 requests. No service is assigned a made-up default price.

The estimator uses decimal arithmetic, reports known gross cost, missing rates,
and the result after explicitly supplied eligible credits. Unprovided services
remain outside the estimate. Credits are account-specific: verify applicability,
expiry, and remaining amounts; do not assume all services or all charges qualify.
Persist rate source/date/region with each projection and compare measured costs
per 1,000 imported games/positions and idle infrastructure before scaling.

## Primary references

- [RDS PostgreSQL 18.6 availability](https://aws.amazon.com/about-aws/whats-new/2026/08/amazon-rds-postgresql-18-6-17-11-16-15-15-19-14-24/)
- [RDS instance CloudFormation properties](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-rds-dbinstance.html)
- [RDS TLS trust roots](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html)
- [ECS Secrets Manager injection](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/specifying-sensitive-data.html)
- [Fargate private networking](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-networking.html)
- [Fargate task-definition constraints](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-tasks-services.html)
- [ECS ephemeral bind volumes and image permissions](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/bind-mounts.html)
- [ALB idle timeout](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-load-balancer-attributes.html)
- [SQS queue CloudFormation properties](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-sqs-queue.html)
- [AWS Pricing Calculator](https://calculator.aws/)
