# Shared provider operating policy

Provider pacing and HTTP retry settings can be changed through trusted
administration while acquisition replicas continue running. Apply migration
`0021` before enabling this application version. These settings are independent
of workspace allowances, customer tiers, and deployment credentials.

Each provider has an optional current PostgreSQL policy and an append-only
version history. The policy contains only `min_delay_s` and `max_retries`.
Existing `ProviderSettings` validates both values. Until a policy is installed,
workers use the provider settings from their deployment configuration.

```bash
chess-crawl-admin operating-policy show --provider lichess
chess-crawl-admin operating-policy set --provider lichess \
  --expected-version 0 --min-delay-s 1.5 --max-retries 3
chess-crawl-admin operating-policy history --provider lichess
```

`show` returns the effective values, their database/deployment origin, and the
current version (`0` before installation). Subsequent changes require the exact
observed version. A concurrent or stale operator update fails without changing
the policy or history. `set` may change one or both fields; it retains the other
field's current effective value. History reads are paginated by version with
`--after-version` and `--limit`.

Use trusted operator database credentials for these commands. The HTTP API has
no provider-policy write endpoint. Cloud runtime roles can read the policy and
history but cannot insert, update, or delete them. Configuration and command
output do not include provider tokens or database passwords.

Acquisition clients resolve the current policy before each HTTP request,
including clients that were already cached in an existing worker session. The
retry allowance is captured for that request; a later request uses the latest
version. Active response completion also uses the current minimum delay when
persisting the next globally eligible request time.

Workers recheck the shared deadline and ownership after request-budget
reservation, including any wait for another budget transaction. Shutdown during
reservation or pacing returns unused reserved bytes without sending another
request.

Installing or changing policy extends global pacing to at least the update time
plus the new minimum delay. Existing cooldowns are never shortened. In
particular, Lichess's fixed 429 backoff and server retry floors remain effective.
The existing provider-wide acquisition ownership lock continues to serialize
provider access across replicas. Processing workers do not acquire that lock or
wait on those cooldowns.

## Process database session admission

`CHESS_CRAWL_DATABASE_MAX_CONNECTIONS` bounds concurrently admitted PostgreSQL
sessions in one Python process. It defaults to `32` and must be a positive
integer. `CHESS_CRAWL_DATABASE_ADMISSION_TIMEOUT_S` defaults to `5.0` and must be
finite and positive. A saturated process waits for a returning permit up to that
interval, then raises the existing database-availability error; HTTP requests
receive the existing 503 response and may retry.

Each admitted connection remains a dedicated PostgreSQL session. There is no
transaction pooling, session reassignment, or heartbeat-based transfer of job
ownership. Closing a connection returns its permit once; failed connection
setup also returns admission. Workers require capacity for at least two
connections so the execution and heartbeat sessions remain independent.

The process initializes one admission controller on its first connection.
Changing environment or TOML settings does not create a second controller or
increase an already running process's capacity. Restart the service to change
this deployment setting. Forked services initialize their own controller;
never share a live PostgreSQL ownership session across processes.

Configure each role's cap and replica count against the database's overall
connection budget. The controllers do not reserve idle database connections.
An API process, acquisition worker, processing worker, dispatcher, and event
publisher each have independent process limits, so their aggregate potential
capacity grows with replicas. `config validate --role worker` checks the
execution/heartbeat minimum, and `config show` includes the deployment session
settings and their origins.
