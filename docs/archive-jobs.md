# Queued archive operations

Large working-set construction and export preparation can run as durable
processing jobs. The HTTP API validates a small request and returns 202; PostgreSQL
holds its identity, owner, budget and result references. Acquisition workers never
claim these operations, and processing does not contact chess providers.

Apply migration `0020` after draining older workers. Set
`CHESS_CRAWL_ARCHIVE_JOBS_ENABLED=true` on API processes to admit requests. The flag
only controls admission: existing jobs still run, and their status/downloads remain
available when it is false. Keep artifact storage configured and accessible while
draining jobs or serving retained downloads.

## API contract

All routes require the caller's workspace bearer credential. Use an
`Idempotency-Key` header on submissions; exact requests replay the same job even
when its retained artifact quota is full. Reusing that key for different options
or a different operation returns 409. Another workspace can independently use the
same key. Ownership comes from authentication, never request fields.

| Route | Request or result |
| --- | --- |
| `POST /v1/archive-jobs/working-sets` | The existing working-set body: `name`, `filters`, optional `settings` |
| `POST /v1/archive-jobs/exports` | `kind`: `games`, `users`, or `graph`; optional `provider` |
| `GET /v1/archive-jobs/{job_id}` | Contract version, operation, ordinary durable job status, and finalized working set or artifact descriptor |
| `GET /v1/archive-jobs/{job_id}/download` | The finalized export bytes, content hash and private cache headers |

Submission responses have `contract_version: 1`, `operation`, `run_id`, `job_ids`,
`replayed`, and `status_url`; `Location` identifies the status route. Clients can
also use ordinary `/v1/jobs/{id}` and `/v1/runs/{id}` polling and cancellation.
These internal runs/jobs have `provider: null`. Only the two explicit archive job
kinds allow a null provider; source-specific runs and handlers retain their
provider requirements.

Limits are server configuration. Requests cannot change worker stage, storage
location, retention, artifact sizes, or work budgets. Invalid options return 422;
workspace/backlog/artifact quota failures return 429. Admission disabled returns
409 with `archive_jobs_disabled`. Private status/downloads for another workspace
return 404 before object access.

## Selection and reproducibility

Selection happens when processing begins, rather than at submission time.
Working-set construction reuses the synchronous selector and freezes exact game
version IDs, settings, membership order, and the input signature. Retries reuse
an already finalized selection instead of selecting newer versions. Each job's
working-set idempotency identity is separate from caller-selected synchronous
working-set keys.

Exports reuse the same deterministic serializer and ownership filters as
immediate exports. Games/users exports contain public normalized provider data;
graph exports contain only the caller's traversal provenance. Each attempt takes
one read-only repeatable snapshot on the owned worker session, streams to a
private temporary file bounded by row, byte and time ceilings, and closes the
snapshot before uploading. This does not need a third worker database session
alongside the executor and heartbeat.

A finalized export manifest records renderer/contract versions, provider filter,
rows, logical bytes, processing snapshot timestamp, full content checksum, chunk
count and its own immutable signature. Result bytes stay fixed after publication.
An interrupted attempt can select a newer processing-time snapshot on retry until
its manifest is finalized; clients should only treat a ready result as reproducible.

Archive operations share ordinary fair scheduling, active/queued workspace caps
and normalization work budgets. They do not need unused import-game or remote
request/byte allowance. Each working-set member or serialized export row consumes
one normalization unit. Working-set membership is capped by remaining allowance
before construction and hashing. Export work is bounded by remaining allowance while the
snapshot streams; completed rows are charged after the snapshot closes, before
publication. Failed preparations also consume their completed rows. Concurrent
workspace work can consume allowance first and block publication; trusted budget
resume retains spent work. Occupied preparation slots use ordinary bounded
transient retries, rather than exhausting the work budget.

## Storage and resource bounds

| Setting | Default |
| --- | --- |
| `CHESS_CRAWL_ASYNC_MAX_WORKING_SET_MEMBERS` | 1,000,000 |
| `CHESS_CRAWL_ASYNC_EXPORT_MAX_ROWS` | 1,000,000 |
| `CHESS_CRAWL_ASYNC_EXPORT_MAX_BYTES` | 268,435,456 |
| `CHESS_CRAWL_ASYNC_EXPORT_PREPARE_SECONDS` | 600 |
| `CHESS_CRAWL_ARTIFACT_MAX_COUNT` | 32 per workspace |
| `CHESS_CRAWL_ARTIFACT_MAX_BYTES` | 1,073,741,824 per workspace |
| `CHESS_CRAWL_ARTIFACT_TTL_SECONDS` | 86,400 from admission |

Select `CHESS_CRAWL_ARTIFACT_BACKEND=local` or `s3`. Local storage uses an absolute
`CHESS_CRAWL_ARTIFACT_DIRECTORY` (default `/var/lib/chess-crawl/artifacts`) shared
by API/processing replicas. S3 uses `CHESS_CRAWL_ARTIFACT_S3_BUCKET`, falling back
to `CHESS_CRAWL_ARCHIVE_S3_BUCKET` when empty. Artifacts use
`artifacts/<workspace-sha256>/<job-id>/attempt-<generation>/sha256/...gz`; public raw evidence retains
its existing namespace. Processing needs artifact Get/Put/Delete access for
publication and retry cleanup; API needs Get access. The processing raw evidence
mount/permission stays read-only.

New exports atomically reserve their maximum allowed logical bytes and one
artifact slot before admission completes. Finalization reduces that reservation
to actual bytes (at least one byte for empty artifacts). Pending, interrupted,
ready and expired-but-unpruned reservations all count until trusted pruning
releases them. A workspace's new export cannot consume another workspace's
allowance. Work budgets can impose a lower effective bound than archive limits.

Uploads and downloads use individually verified chunks of at most 1 MiB. Downloads
validate the immutable manifest, private namespace, chunk sequence, compressed and
uncompressed sizes/checksums, and whole content hash. Each API replica reads shared
references and closes its database session before object I/O or network delivery.
Download memory shares the immediate export process/workspace capacity controls
and reserves 4 MiB per stream. Cross-replica download leases share the configured
export preparation slot count; the lease ends at the earlier of artifact expiry
and the synchronous export download lifetime. Disconnects and unconsumed responses
release local capacity; leases expire even if PostgreSQL is unavailable at close.

An object published immediately before a process crash can be left without a
committed chunk reference. The retry deletes committed partial chunks and never
source evidence. Per-attempt object namespaces prevent a stale in-flight delete
from removing a newer owner's publication. Unreferenced objects need a trusted storage inventory cleanup;
do not configure a short prefix lifecycle that can remove ready downloads before
their retained TTL. PostgreSQL result metadata and shared artifact storage belong
in the same backup/recovery plan.

## Retention

Expired artifacts are unavailable for new downloads. Run the trusted operator
command repeatedly until a workspace's due artifacts have been removed:

```bash
chess-crawl-admin prune-artifacts --workspace-id example --before 1791244800 --batch-size 10
```

`--before` is an inclusive expiration cutoff, clamped to the current time. One
call visits at most 100 artifacts and deletes at most 256 chunks per artifact;
the default visits 10. Interrupted deletions retain their references and quota
until a later call completes. Active jobs and live download leases prevent
pruning. Working sets, source evidence and their registered provenance are retained.
The command has direct trusted database/storage access and is unavailable through
the tenant API.
