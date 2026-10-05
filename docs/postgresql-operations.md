# PostgreSQL operations

[Backend guide](backend.md) · [Operations guide](cli.md) ·
[Contributing](../CONTRIBUTING.md)

This guide applies to the PostgreSQL-only backend. Run these commands from the
repository root with the same Compose project name and environment used to start
the application. They assume the supplied `postgres` service, database and role
`chess_crawl`, and PostgreSQL 18 clients in that container. The database port does
not need to be published. Commands use the image's local socket authentication;
no password is passed in command arguments or printed.

## Persistent data and credentials

The `postgres_data` volume is mounted at `/var/lib/postgresql`, containing
PostgreSQL 18's versioned data directory. `docker compose stop` and
`docker compose down` retain it. `docker compose down --volumes` deletes the
named database, archive, and Mercure volumes. The default Compose deployment
stores response bodies in `archive_data`, mounted at
`/var/lib/chess-crawl/archive`; PostgreSQL keeps their references, not their bytes.

Keep the generated `postgres_password` file with the database's operational
configuration. Bootstrap retains this file on reruns. The image initializes the
role password only for an empty data directory: generating a different file
against an existing volume does not change the database password. Database dumps
do not include cluster roles or their passwords, API credentials, Mercure JWTs,
or the hub's replay history. Store those credentials securely alongside the
backup and retain the configured topic prefix.

A full database restore preserves `event_archive_identity`, job/run revisions,
inline raw response bytes, external-object references, and outbox rows, including
delivery checkpoints. Restoring external responses and imported PGNs also requires
the matching object archive. Do not
rebuild an archive from JSONL exports or rewrite its identity to perform a
restore. Restored pending outbox rows retain their original event IDs. Delivery
remains at least once, including a possible duplicate when a hub accepted an
event before its database acknowledgement was backed up.

## Freeze a recovery point and verify its objects

`pg_dump` provides a consistent database snapshot while services run. For a
restore drill that compares the backup with current source data, first stop all
writers and keep them stopped through backup and verification:

```bash
docker compose stop api worker events
```

Also stop any separate worker, submission client, archive relocation/transfer,
import process, SQS dispatcher, or other application
instance using this database. Keeping only the worker stopped is insufficient:
API submissions and the event publisher also mutate database state. Leave
PostgreSQL and Mercure running. Keep writers stopped through the database dump,
object copy, and comparisons below. Online database dumps are valid snapshots,
but an independently timed object copy is not automatically a coherent backup.

Create this read-only verifier once. It records every object referenced by either
raw responses or imports and reads each through the production checksum and bounded
decompression checks. Retained, unreferenced objects are not required for this
recovery point. It does not print response bodies or credentials. Keep its manifest
private: it includes storage locations and content hashes.

```bash
umask 077
mkdir -p backups
chmod 700 backups
cat > backups/verify-archive.py <<'PY'
import json
from chess_crawl.storage.archives import read_archive_object
from chess_crawl.storage.db import database_url, open_database, transaction

with open_database(database_url()) as conn, transaction(conn, write=False):
    after = 0
    while True:
        rows = conn.execute('''
            SELECT a.* FROM archive_objects a WHERE a.id > %s AND (
                EXISTS (SELECT 1 FROM raw_payloads r WHERE r.archive_object_id = a.id)
                OR EXISTS (SELECT 1 FROM archive_imports i WHERE i.archive_object_id = a.id)
            ) ORDER BY a.id LIMIT 100
        ''', (after,)).fetchall()
        if not rows:
            break
        for row in rows:
            after = int(row['id'])
            try:
                read_archive_object(conn, after)
            except Exception:
                raise SystemExit(f'Archive object {after} failed verification') from None
            print(json.dumps(dict(row), sort_keys=True))
PY
```

## Back up the database and local archive together

The following Bash block applies to the default local archive path. It refuses
other referenced locations rather than silently omitting their bytes. For S3 or
mixed archives, use the operator-managed procedure below. A `.complete` marker is
written only after the database dump, verified object manifest, and archive copy
all succeed. An interrupted bundle without that marker is incomplete.

```bash
(
  set -eu
  umask 077
  mkdir -p backups
  chmod 700 backups
  backup_temp=$(mktemp "backups/chess-crawl-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX.part")
  object_temp="$backup_temp.objects"
  archive_temp="$backup_temp.archive"
  trap 'rm -f -- "$backup_temp" "$object_temp" "$archive_temp"' EXIT
  trap 'exit 1' HUP INT TERM
  backup_file="${backup_temp%.part}.dump"
  test ! -e "$backup_file"
  docker compose run --rm --no-deps -T api python - \
    < backups/verify-archive.py > "$object_temp"
  python3 - "$object_temp" <<'PY'
import json
import sys
with open(sys.argv[1]) as manifest:
    for line in manifest:
        row = json.loads(line)
        if (row['backend'], row['location']) != ('local', '/var/lib/chess-crawl/archive'):
            raise SystemExit('Use the operator-managed backup procedure for other archive locations')
PY
  docker compose exec -T postgres pg_dump --no-password \
    --username=chess_crawl --dbname=chess_crawl --format=custom > "$backup_temp"
  test -s "$backup_temp"
  docker compose exec -T postgres pg_restore --list < "$backup_temp" > /dev/null
  docker compose run --rm --no-deps -T api \
    tar --create --gzip --file=- --directory=/var/lib/chess-crawl/archive . > "$archive_temp"
  test -s "$archive_temp"
  mv -- "$object_temp" "$backup_file.objects.jsonl"
  mv -- "$archive_temp" "$backup_file.archive.tar.gz"
  mv -- "$backup_temp" "$backup_file"
  touch "$backup_file.complete"
  printf 'Backup bundle ready: %s\n' "$backup_file"
)
```

Review `pg_dump` warnings on stderr. `pg_restore --list` checks that the archive
header and contents list are readable; a successful restore is still required
to verify the table data. Copy the `.dump`, `.objects.jsonl`, `.archive.tar.gz`,
`.complete`, verifier, and protected operational configuration together outside
the database host. The restore and full object reads below verify the copied
bytes; a tar listing alone does not. This bundle is separate from a deployment's
retention policy or point-in-time recovery configuration.

### S3 and other operator-managed archives

Keep the same writer freeze. Produce the verified manifest and `pg_dump` from
that frozen database, then inventory every distinct backend/location in the
manifest. Preserve every listed object key and its exact compressed bytes using
the storage operator's backup facilities. For S3, record the bucket, key, backed-up
version ID (when versioning is enabled), and backup recovery point alongside the
database dump. Versioning in the live bucket alone is not an independent backup;
retain a protected copy under the deployment's recovery policy. Do not mark the
bundle complete until every listed object has a recoverable copy.

An S3 restore must make those bytes readable at the bucket/key recorded in the
restored database, with access granted to its application role. Verify the backup
by retrieving its copies and comparing compressed SHA-256 and byte length against
`stored_hash` and `stored_bytes` in the manifest. Check the decompressed SHA-256
and length against `body_hash` and `body_bytes` as well. Then run the production
verifier against the recovery deployment. A successful read from the original
live bucket proves its availability, not recoverability of a separate backup.
The operator must validate the chosen S3 backup/restore procedure and IAM access;
the local-volume commands below do not exercise AWS recovery.

For a new location, first recover objects at their recorded location and use the
[verified transfer helper](archive-storage.md#transferring-existing-external-objects)
to move the recovered archive. Changing `CHESS_CRAWL_ARCHIVE_BACKEND` only selects
new-write storage; it does not rewrite old references. Never overwrite an existing
object that disagrees with its recorded checksum during a drill.

## Restore to a new database

Set the backup path to the actual `.dump` file printed above. Keep the generated
restore database name for all subsequent commands in the same shell:

```bash
CHESS_CRAWL_BACKUP="backups/replace-with-your-backup.dump"
CHESS_CRAWL_RESTORE_DATABASE="chess_crawl_restore_$(date -u +%Y%m%dT%H%M%S)_$$"
export CHESS_CRAWL_BACKUP CHESS_CRAWL_RESTORE_DATABASE
(
  set -eu
  test -r "$CHESS_CRAWL_BACKUP"
  test -r "$CHESS_CRAWL_BACKUP.complete"
  test -r "$CHESS_CRAWL_BACKUP.objects.jsonl"
  case "$CHESS_CRAWL_RESTORE_DATABASE" in
    chess_crawl_restore_*) ;;
    *) printf 'Use a dedicated chess_crawl_restore_ database name\n' >&2; exit 1 ;;
  esac
  case "$CHESS_CRAWL_RESTORE_DATABASE" in
    *[!a-zA-Z0-9_]*|'') printf 'Invalid restore database name\n' >&2; exit 1 ;;
  esac
  docker compose exec -T postgres createdb --no-password \
    --username=chess_crawl --maintenance-db=postgres --template=template0 \
    --owner=chess_crawl "$CHESS_CRAWL_RESTORE_DATABASE"
  docker compose exec -T postgres pg_restore --no-password \
    --username=chess_crawl --dbname="$CHESS_CRAWL_RESTORE_DATABASE" \
    --no-owner --no-privileges --exit-on-error --single-transaction \
    < "$CHESS_CRAWL_BACKUP"
  printf 'Restored database: %s\n' "$CHESS_CRAWL_RESTORE_DATABASE"
)
```

`createdb` fails if the chosen name already exists. No command drops or cleans
an existing database, and the active `chess_crawl` database is never the restore
target. A failed restore leaves its dedicated target isolated; inspect it before
choosing whether to remove it. `--single-transaction` rolls back the restore's
changes on failure. `--no-owner --no-privileges` assigns restored objects to the
connecting role; provision deployment-specific grants separately.

Do not run `chess-crawl-admin migrate` on the empty restore target before `pg_restore`.
Restore the complete schema and data together. The dump contains migrations,
functions, trigger definitions, identity sequence state, and the archive identity.
Loading table data into an already initialized schema could execute event
triggers and create additional outbox rows instead of preserving the backup.

## Restore the local objects into an isolated volume

For the default local bundle, restore to a newly named volume. It is mounted at
the same absolute path saved in the database, while the production `archive_data`
volume remains separate. Use the application's backed-up image revision. The
initialization container only sets ownership on this new volume; extraction runs
as the normal application UID. These commands require a trusted backup archive.

```bash
CHESS_CRAWL_RESTORE_VOLUME="${CHESS_CRAWL_RESTORE_DATABASE}_archive"
export CHESS_CRAWL_RESTORE_VOLUME
(
  set -eu
  test -r "$CHESS_CRAWL_BACKUP.archive.tar.gz"
  if docker volume inspect "$CHESS_CRAWL_RESTORE_VOLUME" >/dev/null 2>&1; then
    printf 'Restore volume already exists; choose a new restore name\n' >&2
    exit 1
  fi
  docker volume create "$CHESS_CRAWL_RESTORE_VOLUME" >/dev/null
  docker run --rm --network=none --read-only --user=0:0 \
    --cap-drop=ALL --cap-add=CHOWN --cap-add=FOWNER \
    --mount "type=volume,source=$CHESS_CRAWL_RESTORE_VOLUME,target=/var/lib/chess-crawl/archive" \
    chess-crawl:local python /app/docker/archive_init.py
  docker run --rm --interactive --network=none --read-only --cap-drop=ALL \
    --mount "type=volume,source=$CHESS_CRAWL_RESTORE_VOLUME,target=/var/lib/chess-crawl/archive" \
    chess-crawl:local tar --extract --gzip --file=- --no-same-owner \
    --directory=/var/lib/chess-crawl/archive < "$CHESS_CRAWL_BACKUP.archive.tar.gz"
)
```

Retain a failed drill's isolated database and volume for inspection. Do not point
writers at them or replace the production volume to make a comparison pass.

## Compare source and restored data

For an exact drill, source writers must still be stopped and neither acquisition
nor event delivery may run against the restored database. Create a verification
query file under the private backup directory:

```bash
cat > backups/restore-verification.sql <<'SQL'
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT format(
  'SELECT %L AS section, %L AS name, COUNT(*)::text AS value FROM %I.%I;',
  'count', table_name, table_schema, table_name
)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY table_name
\gexec
SELECT 'migration', version, name, applied_at
FROM schema_migrations ORDER BY version;
SELECT 'identity', singleton, id
FROM event_archive_identity ORDER BY singleton;
SELECT 'raw', id, body_hash, body_bytes, body_compression, archive_object_id,
       octet_length(raw_body), encode(sha256(raw_body), 'hex')
FROM raw_payloads ORDER BY id;
SELECT 'object', id,
       encode(sha256(convert_to(row_to_json(archive_objects)::text, 'UTF8')), 'hex')
FROM archive_objects ORDER BY id;
SELECT 'import', id,
       encode(sha256(convert_to(row_to_json(archive_imports)::text, 'UTF8')), 'hex')
FROM archive_imports ORDER BY id;
SELECT 'outbox', id,
       encode(sha256(convert_to(row_to_json(event_outbox)::text, 'UTF8')), 'hex')
FROM event_outbox ORDER BY id;
SELECT 'sequence', sequencename, start_value, min_value, max_value,
       increment_by, cycle, cache_size, last_value
FROM pg_sequences WHERE schemaname = 'public' ORDER BY sequencename;
COMMIT;
SQL
(
  set -eu
  umask 077
  docker compose exec -T postgres psql --no-password --no-psqlrc \
    --username=chess_crawl --dbname=chess_crawl \
    --set=ON_ERROR_STOP=1 --tuples-only --no-align --quiet \
    < backups/restore-verification.sql > backups/source-verification.txt
  docker compose exec -T postgres psql --no-password --no-psqlrc \
    --username=chess_crawl --dbname="$CHESS_CRAWL_RESTORE_DATABASE" \
    --set=ON_ERROR_STOP=1 --tuples-only --no-align --quiet \
    < backups/restore-verification.sql > backups/restored-verification.txt
  diff -u backups/source-verification.txt backups/restored-verification.txt
)
```

A zero exit status from `diff` establishes equality for table counts, migration
history, archive identity, inline raw-body hashes and bytes, external references
and metadata, import ownership/provenance, complete outbox row hashes, and sequence
values. It does not read external object bytes. SHA-256 of the stored inline bytes
is compared separately from the archive's recorded `body_hash`, since a stored
response may be compressed. Outbox row hashes include its exact payload text,
revision, retry counters, error and delivery fields. This is a restore check,
not a proof that every source record was originally correct.

Read the restored external objects and compare their manifest with the verified
backup manifest. The volume override replaces the service's mount at this target,
so missing restored objects cannot be masked by the original live archive:

```bash
CHESS_CRAWL_RESTORE_URL="postgresql://chess_crawl@postgres:5432/$CHESS_CRAWL_RESTORE_DATABASE"
export CHESS_CRAWL_RESTORE_URL
(
  set -eu
  umask 077
  docker compose run --rm --no-deps -T \
    --env CHESS_CRAWL_DATABASE_URL="$CHESS_CRAWL_RESTORE_URL" \
    --volume "$CHESS_CRAWL_RESTORE_VOLUME:/var/lib/chess-crawl/archive:ro" \
    api python - < backups/verify-archive.py > backups/restored-objects.jsonl
  diff -u "$CHESS_CRAWL_BACKUP.objects.jsonl" backups/restored-objects.jsonl
)
```

Success requires every referenced response/import object to be present, with
matching compressed and decompressed checksums and lengths. A missing or corrupt
object exits unsuccessfully even if all database comparisons match. This checks
the recorded evidence, not the correctness of a provider's original response.

Check application schema support and the authenticated readiness route without
starting a worker or publisher on the restored copy:

```bash
docker compose run --rm --no-deps -T \
  --env CHESS_CRAWL_DATABASE_URL="$CHESS_CRAWL_RESTORE_URL" \
  --volume "$CHESS_CRAWL_RESTORE_VOLUME:/var/lib/chess-crawl/archive:ro" \
  api chess-crawl-admin info
docker compose run --rm --no-deps -T \
  --env CHESS_CRAWL_DATABASE_URL="$CHESS_CRAWL_RESTORE_URL" \
  --volume "$CHESS_CRAWL_RESTORE_VOLUME:/var/lib/chess-crawl/archive:ro" \
  api python - <<'PY'
import asyncio
import os
from pathlib import Path
import httpx
from chess_crawl.api import create_app
from chess_crawl.storage.migrations import SCHEMA_VERSION

async def verify():
    token = Path(os.environ['CHESS_CRAWL_API_TOKEN_FILE']).read_text().strip()
    transport = httpx.ASGITransport(app=create_app())
    async with httpx.AsyncClient(transport=transport, base_url='http://restore-drill') as client:
        response = await client.get('/health/ready', headers={'Authorization': f'Bearer {token}'})
    if response.status_code != 200 or response.json() != {'status': 'ready', 'schema_version': SCHEMA_VERSION}:
        raise SystemExit('Restored database failed application readiness')
    print('Restored database passes application readiness')

asyncio.run(verify())
PY
```

These one-off containers inherit the normal mounted credentials and execute
read-only checks. `--no-deps` prevents them from running the migration service or
other application services. No token value is printed. After a drill, resume
the original services with `docker compose start api worker events`; leave the
restored copy isolated or remove its explicitly named database after review.

For actual recovery, stop the original deployment's writers before switching
its connection URL and local archive volume to the verified targets. S3 deployments
must retain access to every recorded bucket/key. Keep only one active copy of an
archive identity. Two diverging copies publishing to the same topics can reuse
event IDs. Recovery to an older backup discards later writes and can replay
previously published events; coordinate client cache reset and API
resynchronization with that recovery point. PostgreSQL dumps do not restore
Mercure's independent history volume.

## Versions, upgrades and rollback

Compose pins the server to the PostgreSQL 18 major version. Container recreation
with that major version preserves its volume. Plan minor updates with a tested
backup and restore drill. For a major upgrade, provision a new volume/server,
make a logical dump, restore into an empty database on the target version, and
repeat data and application verification before switching services. Do not
retag the existing volume to another PostgreSQL major. Use a dump client whose
major version can read the source; an older `pg_dump` cannot dump a newer server.
Restoring a newer-version dump into an older major is not a supported rollback
assumption. `pg_upgrade` is an alternative with its own compatibility checks,
not an application CLI feature.

The SQLite removal is a fresh PostgreSQL cutover. Existing file archives are not
imported automatically and the backend has no SQLite fallback. To roll back an
application deployment, retain the previous image/revision and connection
settings, check that it supports the restored schema, and select a verified
PostgreSQL recovery database. A newer schema is rejected by older application
code; switching an image alone does not reverse migrations. Retain the old
PostgreSQL volume and credentials until the upgraded deployment is accepted.

## Separately managed PostgreSQL

Provision an existing database and a login role that owns the application
schema. For this implementation, use PostgreSQL 18 and allow schema migrations
and session advisory locks. Set `CHESS_CRAWL_DATABASE_URL` to a password-free
URL such as `postgresql://chess_crawl@database.example:5432/chess_crawl`.
TCP connections default to verified TLS (`sslmode=verify-full`), including
certificate-chain and hostname verification. For source-run clients, set
`CHESS_CRAWL_DATABASE_SSL_ROOT_CERT_FILE` to the trusted PEM CA file obtained
from the server operator. Weak URL or libpq environment options cannot downgrade
this policy. Unix sockets remain local and do not use TLS. Loopback TCP servers
without TLS require an explicit `CHESS_CRAWL_DATABASE_TRANSPORT=local`; that
exception is not a remote-server mode.

For source-run services that use password authentication, configure one of
`CHESS_CRAWL_DATABASE_PASSWORD_FILE` or `CHESS_CRAWL_DATABASE_PASSWORD`; setting
both is rejected.
The supplied Compose stack uses the secret file at
`${CHESS_CRAWL_SECRETS_DIR:-./data/dev-secrets}/postgres_password`; supply the
matching database password there. For an external server, use Compose 2.24.4 or
newer and the external overlay rather than changing only the bundled stack's
URL:

```bash
export CHESS_CRAWL_DATABASE_URL="postgresql://chess_crawl@database.example:5432/chess_crawl"
export CHESS_CRAWL_DATABASE_CA_FILE="/path/to/postgres-ca.pem"
docker compose -f compose.yaml -f compose.external.yaml up --build --detach --wait --wait-timeout 120
```

Make the CA PEM readable by container UID `10001`; for example, use mode `0444`
inside a protected parent directory. The overlay mounts it read-only in all
four Python services, enforces verified transport, and omits bundled Postgres
from the active services and startup dependencies. Application services still
wait for successful migrations against the selected server. Keep both Compose
files on subsequent commands. Source-run services require `chess-crawl-admin migrate`
before serving requests. The migration role must own existing objects when
applying later schema changes. See the [external database setup](backend.md#external-postgresql)
for configuration and failure diagnostics.

Use a direct server connection or a session-preserving pooler for worker and
publisher ownership. Transaction pooling cannot preserve their session advisory
locks. Provider network access belongs to the worker; readiness, report/export
reads and backup verification do not require live chess-provider requests.
For external servers, use their supported backup facilities or compatible
PostgreSQL clients with protected password files; the local-socket commands
above apply specifically to the bundled Compose service.

## References

- [PostgreSQL 18: pg_dump](https://www.postgresql.org/docs/18/app-pgdump.html)
- [PostgreSQL 18: pg_restore](https://www.postgresql.org/docs/18/app-pgrestore.html)
- [PostgreSQL 18: binary string functions](https://www.postgresql.org/docs/18/functions-binarystring.html)
- [PostgreSQL 18: upgrading a cluster](https://www.postgresql.org/docs/18/upgrading.html)
- [Official PostgreSQL image entrypoint](https://github.com/docker-library/postgres/blob/master/docker-entrypoint.sh)
