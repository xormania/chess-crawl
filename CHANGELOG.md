# Changelog

Notable changes are recorded here. Entries under `Unreleased` have not been
assigned a release version. See [CONTRIBUTING.md](CONTRIBUTING.md#changelog-requirement) for the update policy.

## Unreleased

### Added

- Server-owned lifetime run budgets, monthly workspace quotas, bounded streamed
  provider responses, and fair workspace claims. Budget exhaustion retains
  incomplete checkpoints for explicit operator extension and resume. Descendant
  backlog admission and original-response replay preserve captured work without
  historical re-fetch after a quota pause or interrupted coverage commit.
  Selective lookup indexes keep quota and replay checks independent of terminal
  shared archive history.

- Bounded worker status with an exact active-worker count and batched 24-hour
  retention for expired stopped/failed heartbeats. Migration `0013` supports
  status and cleanup without changing execution ownership.
- Container health probes bind to the worker's own UUID and live local process,
  so another healthy executor cannot hide an expired container heartbeat.
- Workspace-owned submissions, jobs, Mercure topics, immutable player working sets,
  and exact-input analysis result reuse, with server-bound service credentials.
- Rich archive APIs for profile/resource history, game evidence, clock precision,
  coverage, offline upgrades, resource collection, and normalized PGN export.
- Full/incremental/backfill import request contracts with optional date bounds and
  bounded execution batches. Synchronous working sets enforce a configurable
  10,000-member default with atomic overflow rejection.
- Authenticated archive lookup, reports, scoped cached-payload/job/run catalogs,
  streamed public game/user JSONL and owned graph CSV exports, and one-job
  profile/statistics refresh submissions. Streamed archive exports close their
  snapshot and connection when an HTTP download is interrupted.

- Concurrent durable workers with per-job session ownership and fencing,
  independent provider acquisition and local processing stages, persistent
  request/retry pacing, and aggregate worker liveness. Migration `0009` must
  precede starting the new workers; stop old serial executors before upgrading.
- Durable job dispatch outbox with optional SQS publication and consumption.
  Duplicate deliveries reuse database job state; local polling remains available.
- Checkpointed offline normalization upgrades with fixed source high-water marks
  and workspace-scoped source selection; upgrades do not fetch provider data.
- Resumable full-history, incremental, and explicit backfill acquisition with
  durable resource coverage. Closed Chess.com monthly sources are reused and
  parser upgrades replay their archived bodies locally. Lichess pagination
  preserves millisecond boundaries and timestamp ties, with individual
  follow-ups for unfinished games that complete after an incremental watermark.
  Completed history watermarks remain monotonic; later full jobs reuse stored
  history locally and request only newer ranges. Sealed monthly collection
  adopts newer archived sources without downloading them again.
  Proven finite and partially acquired Lichess intervals are subtracted from
  later collection requests; missing gaps and oldest timestamp ties remain
  explicit. Reused broad pages respect exact creation-time run selection.

- Immutable gzip source archives with local filesystem and optional S3 adapters,
  verified original/encoded checksums, import evidence references, and resumable
  offline relocation of existing PostgreSQL payload bodies. Inline storage stays
  compatible by default; object-backed archives require backing up their objects
  alongside PostgreSQL. See [archive storage](docs/archive-storage.md).
- Immutable game evidence revisions, queryable PGN move/variation trees,
  headers, comments, NAGs, provider metadata and lexical tokens. Precise clock
  and reported elapsed observations retain conflicting sources, malformed
  values, and decimal resolution. Migration 0006 preserves existing archives;
  replay fills the new records locally. Standard chess supports legal replay;
  Chess960/other variants retain notation and clocks with explicit unsupported
  board interpretation. PGN exports can be reconstructed from the database.
  Recurring source bodies refresh the current revision and game metadata even
  when the same run already acquired the game and has exhausted its allowance.
- Complete queryable provider profile/statistics facts, typed rating records,
  alias evidence, and observation history retaining recurring values and
  conditional refreshes. Migration `0008_player_resources` recovers retained
  fetch occurrences; local parser replay populates newly normalized fields.
- A registered supplementary player-resource catalog and collectors for
  Chess.com clubs, team matches, tournaments, and online status, plus Lichess
  rating history, performance statistics, activity, and teams. Complete native
  JSON, rating-history source points, and explicit coverage/failure evidence
  remain available without treating unknown or unavailable data as empty.
- Ownership for raw player resources and authenticated team observations,
  OAuth credential/workspace binding, and preservation with quarantine of
  unowned legacy Lichess profile relationship facts. Public profiles request
  all documented public extensions independently of OAuth relationship data.
  See [player data](docs/player-data.md) for migration and ownership details.
- Current Chess.com statistics alongside profile facts in the rich player read;
  statistics replay preserves account identity and display names after renames.
- Migration-time quarantine of historical Lichess profile captures until safe
  source replay, provider-native verification/streaming flags, and queryable
  highest/lowest tactics and lessons ratings.
- Fresh statistics/resources follow the account currently holding a username
  even when raw-body deduplication reuses older bytes; acquisition attempts stay
  attached to stable account identity across renames and username reuse.
- Public resource requests omit configured OAuth credentials; private resource
  collection does not publish its timestamps in shared alias history.

- A locked Devbox development environment with Python 3.13, uv, Git, and
  PostgreSQL 18 tools, plus setup, source-check, and test commands. uv owns the
  project's `.venv`; Docker Compose continues to own the application services.
- Pinned Bandit security checks for application, deployment, and CI Python,
  with reviewed, rule-specific exceptions, complete-scan validation, and stage
  timings in the existing required offline check. Compose smoke now rejects
  optimized Python so its assertions cannot silently disappear.
- Shared application services for bounded imports and opponent crawls, with
  idempotent submissions and consistent archive snapshots. ([#7](https://github.com/xormania/chess-crawl/pull/7))
- An authenticated JSON API for asynchronous submissions, job/run state,
  worker health, and paginated archive reads. ([#9](https://github.com/xormania/chess-crawl/pull/9))
- A serial worker with exclusive archive ownership, restart recovery, bounded
  retries, and persistent provider cooldowns. ([#8](https://github.com/xormania/chess-crawl/pull/8))
- Transactional job/run events, a durable outbox, and private Mercure delivery.
  ([#10](https://github.com/xormania/chess-crawl/pull/10))
- A standalone Docker Compose backend with its own Mercure hub, development
  credentials, health checks, and persistent PostgreSQL archive. API and event
  integration remain independent of an external web application's deployment.
  ([#11](https://github.com/xormania/chess-crawl/pull/11), [#12](https://github.com/xormania/chess-crawl/pull/12))
- Contributor setup, PR guidelines and a PR template requiring evidence of the
  need and of the result; a changelog update requirement for all PRs beyond
  CI/test-only changes.

- A PostgreSQL operations guide with full-database custom backups, isolated
  restoration, binary payload and event identity verification, authenticated
  readiness checks, and major-version upgrade and recovery instructions.
  External-server setup uses the optional Compose overlay and a mounted CA
  certificate for verified TLS, without starting an unused bundled database.
  See [PostgreSQL operations](docs/postgresql-operations.md).

### Changed

- Game normalization prepares PGN evidence outside write transactions, commits
  each game's evidence and run attribution with its source checkpoint, and
  resumes remaining items after interruption. Account reconciliation takes
  exclusive provider ownership only for confirmed renames or placeholder merges.

- Full CI and Devbox smoke now run after pushes to `master` and support manual
  runs. Push/manual checks validate the selected commit without PR merge-history
  assumptions; PR scope, promotion policy, and required check names are retained.
  Concurrency separates event types and branches so manual checks cannot cancel
  post-merge validation.
- External PostgreSQL connections require certificate-chain and hostname
  verification by default. A separate Compose overlay omits the bundled
  database and mounts its operator-supplied CA certificate, retaining migration
  startup gates. Local plaintext connections require an explicit confined
  transport policy; changing the bundled URL alone cannot weaken remote TLS.
- Database password-file read and decoding failures return sanitized archive
  unavailability responses (503 with retry guidance) instead of internal errors.
- PostgreSQL 18 is now the only supported database. Compose provisions it with
  persistent storage, password-file secrets, readiness checks, and migrations
  before application startup. CLI and services use `CHESS_CRAWL_DATABASE_URL`
  or `--database-url`; the SQLite backend, shared archive volume, and `--db`
  option are removed. Existing SQLite files are not automatically imported.
- Database coordination uses PostgreSQL session advisory locks, preserving
  one acquisition executor and one publisher per archive. Database integration
  tests run against real disposable PostgreSQL instances in both Python CI jobs.

- CI isolates deployment-only checks, overlaps image build/pull and polls
  startup health sooner without weakening readiness. It records stage timings,
  test durations and machine-readable performance evidence, with optional
  repeatable regression comparisons. Changelog policy now inspects the pinned
  merge diff and has executable behavior tests for rename and content rules.
- CI scopes application checks to the PR merge diff while preserving required
  check names and full promotion validation. Documentation-only changes skip
  application work, test-only changes skip Compose, and unrecognized paths run
  all checks. Python analysis reuses caches and Docker separates dependencies
  from application packaging; the offline suite and API/Mercure smoke still
  execute when selected.
- Lichess game requests include available clocks, evaluations, and accuracy
  data by default, with individual capture switches in source and Compose runs.
- Reports expose separate `no_result` and `in_progress` counts. The API retains
  `unfinished` as a compatibility alias for `no_result`; CLI labels now state
  the actual metric.
- Tests ignore inherited runtime configuration and require `--run-live` for
  marked provider/network tests; unmarked tests stay offline and isolated.
- Promotion checks require this repository's `dev` branch, and CI reruns on PR
  edits so changing the target cannot reuse stale validation.
- Reworked agent guidance into a short orientation with local documentation
  links; consolidated branch, PR, and changelog rules in CONTRIBUTING and
  added PROJECT as a basic top-level directory guide. Removed inherited product
  restrictions and clarified cheating detection and analysis as intended goals.
- Consolidated SQLite connections and transactions, durable job/run state,
  and report/export queries under explicit shared owners.
  ([#5](https://github.com/xormania/chess-crawl/pull/5))
- Reorganized the README around CLI and backend setup, with dedicated usage
  and contribution guides describing the current implementation.

### Fixed

- Public archive freshness uses successful fetches linked to public source
  evidence, excluding private payloads and fetches with no captured source.

- Standalone Chess.com conditional game observations refresh the current
  revision, clocks, outcome, and participant facts when a previously retained
  source becomes latest again, while preserving immutable evidence history.

- Public Lichess game exports and history collection omit account OAuth tokens,
  preventing token-only private game evidence from entering the public archive.
  Explicit owned resources retain workspace-scoped OAuth access.

- Cloud bootstrap rotates runtime credentials using attributes allowed to a
  non-superuser RDS master, preserving restrictive creation defaults and unsafe
  role refusal. Raw response registration retains a per-source transaction lock
  while unrelated archival writes proceed concurrently.

- CloudFormation rejects the unsupported Fargate combination of 0.5 vCPU and
  8 GiB before creating runtime task definitions.

- Cloud bootstrap denies temporary-table creation as well as schema/role DDL,
  and refuses existing runtime roles carrying unexpected explicit grants or
  ownership. Dedicated-database PUBLIC privilege assumptions are documented.
- Development Mercure credentials authorize the workspace-scoped local job and
  run topics, restoring private event delivery in Compose without granting
  access to other workspaces or rotating existing master credentials.
- Server-owned run ceilings and monthly workspace quotas bound history acquisition
  and deferred processing across continuations, retries, and new submission keys.
  API admission rejects excess workspace backlog/quota atomically with scoped
  HTTP 429 diagnostics; stored budget reads preserve lifetime policy and usage.
- HTTP exports prepare a finite temporary spool before sending response bytes,
  releasing database snapshots before slow client delivery. Operator row, byte,
  preparation and download limits bound each export, and per-workspace database
  admission works across API replicas. Failed or interrupted streams close the
  private temporary file explicitly.

- Archive relocation selects pending payloads through a partial ordered index
  and checks continuation without counting the entire remaining queue per batch.
  Its JSON result adds `has_more`; `remaining` is null while work remains unless
  `--count-remaining` explicitly requests an exact reporting scan.

- Configured local archival requires an explicit absolute directory and rejects
  missing, blank, or relative locations, preventing source-run processes from
  silently selecting different archives based on their working directory.

- Object reuse rechecks publication and readback before relocation releases an
  inline backup, restoring missing objects and retaining inline evidence on
  corruption. Archive settings currently apply to source-run clients and
  operational helpers; the existing Compose stack continues inline storage.


- Object publication and verification run before database write transactions,
  keeping API/job mutations responsive during slow archive storage. Raw-response
  deduplication skips object I/O; callers owning outer transactions must prepare
  external objects beforehand so source references still roll back atomically.
- Preserve unused native move data alongside PGN-derived tokens, including
  conflicting notation and nontext native values. Evidence parser v2 repairs
  previous omissions through local replay into a new immutable revision.

- Read game revisions and reconstruct PGN from one complete database snapshot,
  including when a concurrent writer replaces and removes an old unreferenced
  version. Read/export helpers remain coherent inside READ COMMITTED callers
  and preserve exact clock and derived-timing decimal strings.
- Restore explicitly null current game times, opening facts, and participant
  results when a previously observed body becomes current again. Sparse omitted
  facts remain preserved; known live observations cannot retain a stale completed
  end time. The v5 game normalizer can repair existing facts by offline replay.
- SQS workers poll durable database jobs after missing or unclaimable delivery
  hints, preventing expired or dead-lettered messages from stranding runnable work.

- Rejected offline upgrade identities preserve another job's progress; failure
  reporting updates only the requesting job's provider and ownership scope.
- Player resource/statistics parsing preserves the account and time bound to
  its acquisition across intervening renames, conditional responses, and local
  replay of bytes reused by multiple holders. Stable identity reconciliation
  moves fetch bindings with the other evidence. Private collection preserves
  public identity facts; private-only placeholders have null public dates until
  a public observation supplies them. Replay uses user parser v7 and resource
  parser v2 without downloading stored sources again.
- Deferred normalization queues preserve the captured fetch identity through
  worker resumes. New observations schedule a successor when older processing
  is still running, so a source rejected as out of date cannot remain pending
  without work. Private placeholder creation and multi-account replay retain
  scoped identity locks while unrelated account writes proceed concurrently.
  Internal delayed replay can target its exact successful fetch occurrence;
  the existing worker retains the same captured identity through parsing.
  Private-only profile lookups remain invisible outside the owning scope;
  public user lookups, listings, exports, and user totals also exclude those
  identities until public evidence exists, while authorized scoped reads retain them.
  Public identities with supplied zero dates or game observations remain visible.

- Failed Chess.com statistics requests retain request evidence without creating
  or refreshing a player account. Local Lichess profile replay also populates
  legacy first/last names while preserving explicit current names and native JSON.

- Reconcile renamed accounts with username-only placeholders while preserving
  their game, snapshot, source, and discovery history. Conflicting stable
  provider account IDs produce an explicit error instead of overwriting identity.
- Preserve known zero statistics and keep unavailable rated-game totals
  unknown. Profile/stat replay refreshes previously normalized snapshot values.
- Stop treating missing Chess.com archive results as evidence of ongoing play.
  Parser upgrades repair stored values when data is replayed or reacquired;
  existing archives are not automatically reprocessed in full.
- Retain Lichess flag, disabled, and verification observations in normalized
  profile snapshots without treating a flag as a country claim.
- Build Lichess acquisition metadata before sending requests and support the
  accepted exclusive date boundary without losing fetched response evidence.
- Count shared discovery edges independently for each crawl. Schema migration 5
  preserves original recorded edge provenance; historical memberships that
  were never stored cannot be reconstructed.
- Resume local opponent discovery when a crash occurs after acquisition fills
  the run's game budget, without fetching additional games.
- Clear removed profile status/title fields on authoritative profile refreshes
  while preserving them during partial game/stat observations; replaying older
  profiles does not replace newer profile metadata.
- Parser upgrades refresh previously normalized games without double-counting
  run attribution; opponent discovery uses the selected run's games, and
  provider pacing persists across acquisition calls. ([#7](https://github.com/xormania/chess-crawl/pull/7))
- Worker heartbeats tolerate transient SQLite writer contention, and CLI
  acquisition obtains exclusive ownership before opening a writable archive.
  ([#8](https://github.com/xormania/chess-crawl/pull/8))
- Claimed job snapshots include the persisted revision after state-change
  triggers, matching API and event reads. ([#10](https://github.com/xormania/chess-crawl/pull/10))

### Removed

- Dormant internal provider iteration/protocol abstractions that bypassed the
  application's bounded acquisition path. Integrations use the documented CLI
  and HTTP services; active provider endpoint clients remain available.
- Obsolete `proj/` planning and audit records.

## 0.1.0 — 2026-07-03

- Initial alpha prerelease. See the [published release](https://github.com/xormania/chess-crawl/releases/tag/v0.1.0).
