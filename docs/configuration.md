# Runtime configuration

Source-run processes and deployment services use the same setting names and
validated value objects. Provider clients, workers, request limits, work budgets,
exports, archive storage, database transport, dispatch maintenance, API
credentials, and Mercure settings read through one configuration source.
Deployment manifests still decide which settings to pass or mount into a
container. A host `.env` is not automatically loaded by Python.

## Sources and precedence

Configuration resolves in this order, from lowest to highest priority:

1. The owning value object's defaults.
2. An optional TOML file selected by `CHESS_CRAWL_CONFIG_FILE`.
3. Explicit environment settings, including empty strings.
4. Explicit numeric worker CLI flags and programmatic constructor arguments.

CLI overrides are applied before worker settings are validated. Other
administration commands retain their explicit `--database-url` override.
Programmatic value objects remain supported for tests and embedding.

The file contains only a `[chess_crawl]` table. Keys are the lowercase suffix of
an existing environment name: `CHESS_CRAWL_POLL_INTERVAL` becomes
`poll_interval`. Unknown file keys, nested tables, lists, and malformed TOML fail
when settings are loaded. Values may be TOML strings, integers, floats, or
booleans; the owning loader parses and validates the resulting value. File
loading does not change `os.environ`.

For example, a local source-run configuration can contain:

```toml
[chess_crawl]
database_url = "postgresql://chess_crawl@localhost:5432/chess_crawl"
database_transport = "local"
database_password_file = "/absolute/path/to/postgres_password"
archive_backend = "local"
archive_directory = "/absolute/path/to/shared-archive"
contact = "operator@example.com"
lichess_delay_s = 1.5
chesscom_delay_s = 1.0
provider_max_retries = 3
poll_interval = 1.0
heartbeat_interval = 5.0
job_max_retries = 3
retry_base = 30.0
retry_max = 3600.0
```

```bash
export CHESS_CRAWL_CONFIG_FILE=/absolute/path/to/runtime.toml
chess-crawl-admin config validate --role worker
chess-crawl-admin config show --role worker
python -m chess_crawl.jobs.worker --poll-interval 2
```

Mount a configuration file into each container that needs it and set
`CHESS_CRAWL_CONFIG_FILE` to its container path. Compose `.env` interpolation and
ECS task environment settings override file values. Do not place container-only
paths into host-run profiles. Configure independent instances with the same
provider settings, workspace allowances, and archive destination.

Settings are deployment configuration; restart services after changing them.
Shared workspace/run budget records retain their existing durable authority and
resume behavior. Updating configuration is not an instruction to reset spent
work or rewrite previously admitted budgets.

## Provider and worker settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `CHESS_CRAWL_CHESSCOM_DELAY_S` | `1.0` | Minimum Chess.com request delay; finite and nonnegative |
| `CHESS_CRAWL_LICHESS_DELAY_S` | `1.5` | Minimum Lichess request delay; finite and nonnegative |
| `CHESS_CRAWL_PROVIDER_MAX_RETRIES` | `3` | Immediate HTTP retries; nonnegative integer |
| `CHESS_CRAWL_POLL_INTERVAL` | `1.0` | Idle worker polling interval in seconds |
| `CHESS_CRAWL_HEARTBEAT_INTERVAL` | `5.0` | Liveness update interval in seconds |
| `CHESS_CRAWL_HEARTBEAT_MAX_AGE` | Derived | Default is `max(20, 4 × heartbeat interval)`; explicit values must be at least twice the interval |
| `CHESS_CRAWL_JOB_MAX_RETRIES` | `3` | Durable job retries after provider retries are exhausted |
| `CHESS_CRAWL_RETRY_BASE` | `30.0` | Initial durable retry delay in seconds |
| `CHESS_CRAWL_RETRY_MAX` | `3600.0` | Maximum exponential retry delay; must be at least the base |

Worker intervals must be finite and positive. Provider cooldowns and valid
server retry delays remain lower bounds. Changing worker count does not change
provider request admission or workspace budget authority.

Existing `--poll-interval`, `--heartbeat-interval`, `--max-retries`, `--retry-base`,
and `--retry-max` flags remain explicit overrides. Programmatic `WorkerSettings()`
retains its declared defaults; environment-aware execution uses
`WorkerSettings.from_env()`.

## Offline validation and inspection

`config validate` checks setting types, bounds, cross-field relationships,
archive destinations, and paired stage queues for combined routing. Acquisition
and processing roles accept their own isolated queue URL. `config show` runs the same
validation and returns effective values plus their `default`, `file`, or
`environment` origins. Credentials and the PostgreSQL connection string are
redacted. Other configured URLs are redacted when they contain credentials,
query parameters, or fragments. Provider configuration representations also omit OAuth tokens.
Neither command opens network connections, database sessions, or SDK clients.
Validation may read configured local configuration and secret files.

API validation checks the installed API extra and the shared authentication
configuration without constructing the HTTP application. It accepts static
credentials or `CHESS_CRAWL_API_AUTH_MODE=database`; database mode rejects static
credential settings and does not query issued credentials. Provision credentials
through [workspace administration](workspace-administration.md) before serving
authenticated requests.

Use `--role` to add deployment requirements:

| Role | Additional checks |
| --- | --- |
| `settings` (default) | Validate configuration without requiring a database; validate database policy if a URL was supplied |
| `admin`, `worker` | Require database settings, validate transport policy and password sources |
| `acquisition`, `processing` | Worker checks plus the corresponding stage queue when SQS is enabled |
| `dispatcher` | Require database settings and an SQS queue |
| `api` | Require database settings and validate API credentials using API startup; requires the API extra |
| `events` | Require database settings and validate Mercure URLs, JWT source, timeout, and retry intervals |

S3/SQS use requires the `s3` dependency extra. AWS credentials continue to come
from the SDK's standard credential provider chain, including ECS task roles;
configuration inspection does not query that chain.

Validation checks local configuration readiness. Actual database access,
provider authorization, S3 permissions, queue delivery, and deployment health
still require the relevant integration checks. See [AWS deployment](aws-deployment.md),
[archive storage](archive-storage.md), and [PostgreSQL operations](postgresql-operations.md).
