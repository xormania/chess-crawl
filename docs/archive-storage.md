# Compressed source archives

PostgreSQL keeps source provenance, normalization state, and references. Original
responses and imported PGNs can be retained as immutable gzip objects locally or
in a private S3 bucket. A response already containing PGN does not need an extra
PGN object. The objects are source evidence, not generated exports.

The library default remains `CHESS_CRAWL_ARCHIVE_BACKEND=database`: existing
inline PostgreSQL bodies and consumers continue to work. Select `local` or `s3`
to send new bodies to object storage. The schema migration adds references without
moving any bytes or contacting AWS. Reads use the location saved with each object,
independently of the backend configured for new writes.

These settings currently configure source-run clients and operational helpers.
The checked-in standalone Compose stack continues using inline database storage:
it does not forward `CHESS_CRAWL_ARCHIVE_*` settings, provide a shared local
archive mount, or install the optional S3 SDK. Setting these variables in its
`.env` alone has no effect. An external backend in containers requires deployment
configuration that forwards the settings and provides either a durable archive
mount at the same path in every service or the S3 extra and AWS credentials.

## Local storage

```bash
export CHESS_CRAWL_ARCHIVE_BACKEND=local
export CHESS_CRAWL_ARCHIVE_DIRECTORY=/absolute/durable/path/archive
```

The directory must be persistent, privately writable by the service, and mounted
at the same absolute path for every reader and worker. Local objects use private
file permissions, filesystem synchronization, and atomic publication that refuses
to overwrite existing evidence. Object keys are content-addressed, validated,
and do not contain provider tokens, game names, or user-supplied paths.
The directory and its ancestors must not be writable by untrusted concurrent
filesystem users; the adapter cannot secure a directory controlled by an attacker.

## S3 storage

Install the optional SDK dependency, then configure a bucket:

```bash
uv sync --locked --group dev --extra api --extra s3
export CHESS_CRAWL_ARCHIVE_BACKEND=s3
export CHESS_CRAWL_ARCHIVE_S3_BUCKET=your-private-archive-bucket
```

Provisioning AWS resources is separate from archive initialization. Boto3 uses
its normal credential chain, including ECS/EC2 roles; archive references contain
the bucket and object key, never AWS credentials. Configure least-privilege
`s3:PutObject` and `s3:GetObject` permissions on this archive prefix. Keep the
bucket private, configure encryption, and retain objects for at least as long
as their database references. Versioning provides another recovery layer against
operator deletion or out-of-band replacement. This adapter does not delete
objects, change bucket policy, or create buckets.

Writes use `IfNoneMatch="*"` and a SHA-256 upload checksum. A precondition failure
is treated as a retry only after verifying the existing bytes. Conditional-write
conflicts and access errors fail without publishing database references.
See the official [PutObject SDK reference](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html)
and [credential provider documentation](https://docs.aws.amazon.com/boto3/latest/guide/credentials.html).

## Moving existing bodies

First take a database backup, preserve existing local/S3 objects, and configure
the destination. Run the operational helper against the selected archive:

```bash
uv run python -m chess_crawl.storage.archive_migration --batch-size 100
```

Each call prints `moved` and `remaining`. Repeat until `remaining` is zero. It
processes only inline bodies and commits each payload separately. A stopped or
failed run resumes by selecting bodies that still remain inline; no provider
requests occur. It is separate from SQL migration versions and must not be run
inside a caller-owned transaction. Metadata, payload IDs, source records, and
normalization state remain unchanged.

The helper checks original bytes, publishes and reads the destination object,
and only then replaces the inline bytes with a reference. Failed publication or
verification leaves the inline body intact. A later failed database commit may
leave an unreferenced object. Retrying reuses it. Never automatically delete that
object: another transaction may already reference the same immutable bytes.

Compression, object publication, and readback verification run before acquiring
database write locks. Only reference registration is transactional. Raw responses
are prepared before the transaction that also records their fetch evidence;
already stored responses skip object I/O. Code that owns an outer transaction
must call `prepare_archive_object()` beforehand and pass its verified
`prepared_object` to the storage helper. Unprepared external writes inside an
existing transaction are rejected, preserving rollback semantics without holding
database locks across storage calls.

## Integrity, recovery, and imports

Both encoded-object and original-body checksums and byte counts are recorded.
Reads verify the compressed bytes and bound decompression to the recorded original
size. Missing or corrupt evidence fails explicitly; there is no silent network
refetch. A compressor upgrade can create a new encoded object without replacing
earlier evidence.

`store_import_backup()` requires an explicit nonempty workspace and preserves an
import before parsing. `read_import_backup()` requires the matching workspace
before resolving its body. Repeated identical
bytes share an object, while import observations preserve their own source name
and capture date. These are internal storage helpers: applications must authorize
the requested workspace, never expose globally shared object IDs, and use the
scoped import reader when exposing imports.

Back up and restore PostgreSQL and its referenced object archive together.
Restoring PostgreSQL alone is insufficient after relocation. Restored local
objects must remain at the recorded absolute directory; moving directories or
changing buckets requires a separate verified relocation implementation. This
helper currently moves inline bodies only and intentionally performs no garbage
collection. Inspect missing/corrupt objects against the last independent backup.
When another source or import reuses the same object bytes, publication and
readback are verified again outside the database transaction. A registered row
alone cannot justify discarding an inline backup. Missing objects can be restored
from that backup; conflicting or corrupt objects fail without releasing it.
