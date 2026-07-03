# chess-crawl — Project Plan

*A provider-neutral, raw-first, local-first chess data archive.*

---

## 1. Summary

chess-crawl is a provider-neutral, raw-first, local chess **data archive** operated by a single person from the command line. It fetches data from public chess-provider APIs — Chess.com PubAPI and the Lichess API in v1 — and preserves a durable local record on disk: the exact RAW provider response for every successful fetch, alongside normalized query tables derived from those raw bodies. The tool is not organized around one seed player; a player is merely one possible entrypoint into discovery. Retrieval can be selective and bounded (a date window, a specific game id, a crawl depth), but once a payload is fetched it is preserved verbatim, and every normalized row can be traced back to the raw body it came from.

Two ideas hold the design together. First, **raw-first**: normalization is always re-runnable from stored raw payloads without re-hitting any API, so a parser bug or a schema change is a local reprocessing task, not a re-crawl. Second, **provider abstraction over shared normalized storage**: a single `ProviderClient` interface with provider-specific fetchers, parsers, and rate-limit/cache behavior feeds one shared SQLite schema, while identity and semantics stay provider-scoped. Acquisition is expressed as durable, job-based work (`discovery_jobs`) so the system is restartable, idempotent, and resumable at any point — a crawl interrupted mid-flight resumes exactly where it stopped. Opponent crawling is only one discovery strategy among several, never the core architecture.

Ethically, chess-crawl is public-API-only, serial, and polite: no scraping, no rate-limit evasion, and no automated cheating accusations of any kind. Provider account-status labels (Chess.com `closed:fair_play_violations`, Lichess `tosViolation`) are recorded as the provider's own neutral facts, never as a determination this tool makes. A Chess.com username and a Lichess username are never assumed to refer to the same human — that boundary is enforced both technically (provider-scoped identity) and as a stated ethical rule.

---

## 2. Scope

### In scope for the MVP (both providers)

- **Fetch user profile** (`fetch_user_profile`): Chess.com `/pub/player/{username}`, Lichess `/api/user/{username}`. Stores raw body, normalizes into `provider_users` + `user_snapshots`.
- **Fetch user stats** (`fetch_user_stats`): Chess.com `/pub/player/{username}/stats`, Lichess `perfs` block. Rating/record snapshots preserved raw and normalized.
- **Fetch user games with bounds** (`fetch_user_games` / `fetch_monthly_archive`): Chess.com month-by-month immutable archives with ETag/304; Lichess `--since`/`--until` NDJSON date-range stream, fanned out into per-calendar-month stream-chunk jobs. Bounded by date window and/or max count.
- **Fetch a single game by id** (`fetch_game_by_id`): Lichess `/api/game/{gameId}`; **Chess.com resolves the game from its owning monthly archive** (there is no single-game-by-id endpoint — see below).
- **Fetch games by an explicit id list** (`fetch_games_by_ids`): Lichess `POST /api/games/export/_ids` (batches up to ~300); Chess.com resolves each id through its owning monthly archive.
- **Import a provider export / dump** (`import_export_dump`): ingest an already-downloaded body (PGN dump, Lichess NDJSON export) into `raw_payloads` and normalize, no network required.
- **Opponent crawl to depth X** (`crawl_opponents`): from a seed user, enqueue child fetch jobs for opponents discovered in games, bounded by `--depth N`. One discovery strategy, not the spine.
- **Jobs status / resume** (`jobs status`, `jobs resume`): inspect and restart durable work.
- **Raw-first storage + normalized tables**: every fetch lands in `raw_payloads` before/with normalization; normalized rows link back via `source_records`.
- **Reports** (`query user`, `query game`) and **exports** (`export games`, `export graph`): read-side over normalized tables.
- **Optional Lichess personal OAuth token**: opt-in, raises the operator's own rate limits, **never required**, never logged (see §12, §15).

### Chess.com single-game fetch resolves through the monthly archive

The Chess.com PubAPI has **no single-game-by-id HTTP endpoint** — a game is only reachable through the monthly archive that owns it. Therefore `fetch_game_by_id` / `fetch_games_by_ids` for Chess.com **derive the owning archive** (username + `YYYY/MM`) from the game's canonical `url`, fetch `/pub/player/{username}/games/{YYYY}/{MM}` (honoring ETag/304), and extract the game by `uuid`. Only Lichess offers a genuine by-id / by-ids HTTP fetch. This is reflected consistently in the provider abstraction (§5) and the job model (§11).

### Explicitly resolved v1 boundaries (formerly open)

- **Bughouse and other >2-player games**: The 2-color participant model (`game_participants` with `UNIQUE(game_id,color)`) is a firm v1 invariant. Bughouse games (four players, `bughousepartnerlose` result code) are **stored raw-first but NOT normalized** into `games`/`game_participants`; their `raw_payloads` row is marked `normalization_status='skipped'` with a reason. Nothing is lost — a future partnership/board extension can normalize them from stored raw. Ingest that encounters bughouse never errors and never forces a bughouse game into a two-color shape.
- **Non-terminal / mutable games** (Chess.com daily in progress, Lichess correspondence in progress, current-month games): these **are** archived raw-first. In the normalized `games` table they carry `outcome = NULL` (no decided result) and `is_live = 1` (refetch-eligible). Refetch until terminal is driven by the mutable-resource path (§10, §12).
- **Event containers** (tournament/match/swiss): v1 keeps only the opaque reference on `games.tournament_ref`. No canonical event table exists in v1 (deliberate non-goal); cross-provider or partially-crawled events are unmodeled. See Open Questions (§18) for the forward path.

### Explicitly out of scope (v1)

- **Cross-provider identity resolution** — never linking a Chess.com human to a Lichess human (see §18; wanted but a deliberate non-goal).
- **Engine analysis, cheat detection, or any fair-play verdict.**
- **Authenticated private-data collection / export** — v1 reads only public data. The optional Lichess token only *raises rate limits*; it never unlocks private scopes. An authenticated *personal-data export* (a user pulling their own private data) is a future, opt-in extension.
- **Live board / event streaming** (no TV, no game-stream following).
- **Web UI** — CLI only.
- **Distributed or parallel crawling** — acquisition is serial and single-process by design.

### Public-data-only boundary

Only endpoints that are public and unauthenticated (or, for Lichess, that return the same public data with a token merely raising limits) are ever called. No scraping of HTML, no undocumented endpoints, no rate-limit evasion.

### No-accusation stance

The archive records neutral facts only. Provider-assigned status strings are stored and reported verbatim as the provider's own labels. Any anomaly signal is explicitly non-conclusive, human-review-only, and carries a non-suppressible disclaimer. chess-crawl never labels a player a cheater.

---

## 3. Provider-Neutral Design Principles

### Raw-first preservation

Every successful fetch is written to `raw_payloads` (raw body + provenance: URL, params, status, ETag/Last-Modified, fetched-at) before or alongside normalization, and normalized rows point back through `source_records`. **Why it matters:** an archive's most durable asset is the untransformed source. Parsers evolve and have bugs; provider schemas drift. Because normalization is a pure function of stored raw bodies, we can re-derive every table offline, without spending a single API request or risking data that is no longer fetchable (deleted accounts, closed games). Raw-first is what makes the archive trustworthy over years.

### Provider abstraction

A single `ProviderClient` interface (fetch + parse + rate-limit/cache policy) sits behind a `registry`, with concrete `chesscom` and `lichess` implementations. **Why it matters:** Chess.com and Lichess differ in archive unit (immutable months vs. date-range streams), caching (strong ETag/304 vs. content-hash/immutability), timestamps (seconds vs. milliseconds), and 429 behavior (Lichess mandates a 60s pause). Isolating those differences behind one interface keeps the jobs runner, storage, and reports provider-agnostic, and makes adding a third provider a matter of implementing the interface — not touching the core.

### Provider-scoped identity

Users and games are keyed by surrogate integer ids and scoped to a provider: `provider_users` enforces `UNIQUE(provider, provider_user_id)` (partial, when present) plus `UNIQUE(provider, username_normalized)`; games enforce `UNIQUE(provider, provider_game_id)`. **Why it matters:** Chess.com has a stable numeric `player_id` and mutable usernames, while a Lichess id equals the lowercased (immutable) username. Conflating these would corrupt the archive and, worse, assert that two accounts are the same human — a claim we refuse to make. Provider-scoping is simultaneously the correct data model and the ethical firewall between providers.

### Job-based acquisition

All work is durable rows in `discovery_jobs` with an exact `kind`, its own `state`, and enough parameters to re-execute. **Why it matters:** archival crawls are long, interruptible, and rate-limited. Modeling work as persisted jobs makes the system restartable, idempotent, and resumable at any point — a killed process, a 429, or a machine reboot never loses progress, and re-running a completed job is a no-op rather than a duplicate fetch.

### Shared normalized storage

Both providers write into one normalized schema (`games`, `game_participants`, `ratings_at_game`, `time_controls`, `variants`, `user_snapshots`, …) using shared taxonomies — a common outcome `{white_win, black_win, draw}` (plus `NULL` for undecided games), a shared variant/time-class vocabulary — while retaining provider-native strings and raw bodies. **Why it matters:** the value of the archive is cross-provider queryability. Normalizing timestamps to epoch seconds, result codes to a shared outcome, and speeds/variants to a shared taxonomy lets one SQL query span providers, without ever discarding the provider's own representation.

---

## 4. Architecture

### Layered view

```
+---------------------------------------------------------------+
|  CLI  (chess_crawl/cli.py, Typer)                             |
|  init | provider list | fetch user|games | crawl opponents    |
|  jobs status|resume | query user|game | export games|graph    |
+-------------------------------+-------------------------------+
                                | enqueues / inspects jobs
+-------------------------------v-------------------------------+
|  Jobs layer  (jobs/runner.py, jobs/discovery.py, models.py)  |
|  pops durable discovery_jobs serially; discovery may enqueue  |
|  child jobs (opponent crawl, per-archive fetch, id batches)   |
+-----------------+-----------------------------+---------------+
                  | fetch via                    | read/write
+-----------------v-----------+   +--------------v--------------+
|  Provider layer             |   |  Storage layer              |
|  providers/registry.py ->   |   |  storage/raw.py (raw-first) |
|  base.ProviderClient        |   |  storage/repository.py      |
|   chesscom/{client,parser,  |   |  storage/{db,schema.sql,    |
|   endpoints}                |   |   migrations}               |
|   lichess/{client,parser,   |   |  normalize/{users,games,    |
|   endpoints}                |   |   codes}                    |
+-----------------+-----------+   +--------------+--------------+
                  |                              |
                  +--------------+---------------+
                                 v
                    +-------------------------+
                    |  SQLite  (single file)  |
                    |  raw_payloads, games,   |
                    |  provider_users, ...    |
                    +-------------------------+
                                 ^
                    +------------+------------+
                    |  Reports & Export       |
                    |  reports/{queries,      |
                    |  render}, export/{games,|
                    |  graph}  (read-side)    |
                    +-------------------------+
```

### Data-flow diagram (one job, raw-first)

```
CLI command
   |  (fetch games lichess USER --since --until)
   v
enqueue discovery_jobs row  (kind=fetch_user_games, state=pending, params)
   |
   v
runner pops job  ---------------------------------------------+
   |  marks state=in_progress; writes crawl_runs / fetch_logs |
   v                                                          |
ProviderClient.fetch()  (serial + polite delay;              |
   |  Chess.com: send If-None-Match; Lichess: stream NDJSON) |
   |                                                          |
   |-- 304 Not Modified --> reuse cached raw_payload --+      |
   |-- 429 -------------> honor Retry-After /           |     |
   |                       Lichess: wait 60s, requeue --+     |
   v                                                    |     |
[RAW-FIRST]  storage/raw.py                             |     |
   insert raw_payloads (body [gzip if large],           |     |
   provenance, etag, fetched_at)  --> COMMIT  <---------+     |
   |                                                          |
   v  (only after raw is durably committed)                   |
parser (chesscom/lichess parser.py) + normalize/*             |
   |  ms->s, result codes -> outcome, variant/speed taxonomy  |
   v                                                          |
upsert normalized rows  (provider_users, user_snapshots,      |
   games, game_participants, ratings_at_game, time_controls,  |
   variants)  + source_records link back to raw_payloads      |
   |                                                          |
   v                                                          |
discovery.py inspects normalized result:                      |
   may enqueue CHILD discovery_jobs -------------------------+
   (e.g. crawl_opponents depth-1: fetch_user_* per opponent;
    fetch_user_games -> per-month archive/stream-chunk jobs)
   |
   v
runner marks job state=done (idempotent; re-run = no-op)
   |
   errors -> errors table; job -> error/blocked with backoff
```

### One-line responsibilities

- **CLI** — parse the exact commands, translate each into one or more enqueued `discovery_jobs`, or run a read-side query/export; no network or DB logic of its own.
- **Jobs runner** — pop pending jobs serially, drive one job to a terminal state, own retry/backoff and `crawl_runs`/`fetch_logs`/`errors` bookkeeping.
- **Discovery** — given a normalized result, decide which child jobs (if any) to enqueue; encapsulates each strategy (opponent crawl, archive fan-out, id batching).
- **Provider registry** — resolve a provider key (`chess.com` / `lichess`) to its `ProviderClient`.
- **ProviderClient** — perform the actual HTTP fetch with provider-specific endpoints, headers, caching, and rate-limit reaction; return a raw `RawRecord` (bytes + provenance).
- **Parser + normalize** — turn a stored raw body into shared normalized rows without any network access.
- **Storage/raw** — persist the raw payload durably (raw-first) before normalization runs.
- **Storage/repository + normalize** — upsert normalized entities idempotently and write `source_records` links.
- **SQLite** — the single-file durable archive.
- **Reports/Export** — read-only projections over normalized tables (`query user|game`, `export games|graph`).

### Key design decisions

- **Raw is committed before normalization.** The `raw_payloads` insert commits first; normalization is a separate step that can fail, be retried, or be re-run in bulk from stored raw bodies. No fetch is ever "lost" to a parser bug.
- **Serial, single-process, polite by construction.** No concurrency anywhere in the acquisition path. Rate-limit reactions live in the ProviderClient policy: Chess.com honors `Retry-After`; Lichess stops and waits a full 60s on 429.
- **Idempotency via natural keys + surrogate ids.** `UNIQUE(provider, provider_game_id)`, `UNIQUE(canonical_url)`, and `UNIQUE(content_hash)` make re-running a job a safe no-op; jobs are keyed (`dedup_key`) so re-enqueue is deduplicated.
- **Caching is provider-specific but invisible above the client.** Chess.com uses ETag/Last-Modified/304 and treats past months as permanently cacheable; Lichess relies on game immutability + `content_hash` dedup. The runner just asks the client to fetch.
- **Content-hash dedup as the cross-provider safety net.** `content_hash` over a deterministic canonical subset of the raw game body catches duplicates even where conditional caching is weak (Lichess).
- **Discovery is pluggable and bounded.** Opponent crawl is one `discovery.py` strategy behind a depth bound; archive fan-out and id-batching are others. The runner does not hardcode any single crawl shape.
- **Compression at the storage boundary.** Large raw bodies (NDJSON streams, PGN dumps) are gzipped (optionally zstd) in `raw_payloads`; the choice is centralized in `storage/raw.py` and self-described per row.
- **Timestamps normalized on the way in.** Lichess milliseconds convert to epoch seconds during normalization; raw ms values stay verbatim in `raw_payloads`.

---

## 5. Provider Abstraction

The core architectural bet of chess-crawl is a **thin provider seam over a fat shared core**. Everything that differs between Chess.com and Lichess (HTTP shapes, pagination model, timestamp units, caching mechanics, 429 etiquette, result vocabulary) is confined to a small number of files per provider: a `client.py` (fetch, raw-first), a `parser.py` (raw → normalized DTO), an `endpoints.py`, and a `FetchPolicy`. Everything downstream — storage schema, jobs, reports, exports, dedup — is provider-agnostic and never branches on `provider == "chess.com"`.

```
                     provider-SPECIFIC                 provider-AGNOSTIC (shared)
                 ┌───────────────────────┐         ┌──────────────────────────────┐
  discovery_job  │  ProviderClient impl   │  Raw    │  raw.py  → raw_payloads       │
  (kind, target) │  chesscom / lichess    │ Record  │           source_records      │
        │        │  ── fetch, raw-first ──┼────────▶│  repository.py                │
        ▼        │                        │         │                              │
  registry.get() │  Parser impl           │ Norm    │  normalize/* → provider_users │
  → client       │  parse_user/parse_game │ DTO     │   games, game_participants … │
                 └───────────────────────┘         └──────────────────────────────┘
                  (only place providers differ)      (one schema for all providers)
```

Two hard rules make this seam trustworthy:

1. **Every client method returns RAW + provenance first.** Parsing is a *separate*, re-runnable step that reads from `raw_payloads`, never from the network. A schema/parser fix is replayed offline against stored raw bodies.
2. **The parser is the only component allowed to interpret provider vocabulary.** It maps native strings (`"checkmated"`, `"outoftime"`, `chess_blitz`, `perf: bullet`) into the shared taxonomy while *retaining* the native string in a `*_raw` column.

### The RawRecord and fetch metadata

Every fetch — success or documented failure — yields a `RawRecord`: the in-flight fetch object handed to `storage/raw.py` for raw-first storage, later persisted as a `raw_payloads` row and consumed by the parser. It bundles the verbatim body with enough provenance to (a) dedup, (b) support conditional re-fetch, and (c) reconstruct the fetch in `fetch_logs`.

```python
# providers/base.py  (DTO sketch — not final code)

EndpointType = Literal[
    "user_profile", "user_stats", "archives_index", "monthly_archive",
    "user_games_stream", "game", "games_by_ids", "import_dump",
]

@dataclass(frozen=True)
class RawRecord:
    provider: str                 # "chess.com" | "lichess"
    endpoint_type: EndpointType   # what this body is (shared enum, §8)
    request_url: str              # exact URL fetched
    request_params: dict          # since/until/max/perfType/etc (for provenance)
    http_status: int              # 200, 304, 404, 410, 429 …
    fetched_at: int               # unix epoch SECONDS (our clock, UTC)
    body: bytes | None            # verbatim payload; None on 304/404
    media_type: str               # application/json | x-ndjson | x-chess-pgn
    # conditional-cache provenance (Chess.com-strong, Lichess-weak):
    etag: str | None
    last_modified: str | None
    # whole-body fingerprint for RAW dedup (NOT the per-game identity hash):
    body_hash: str | None         # 'sha256:<hex>' over decompressed body bytes
    # soft-target identity, so raw rows link even before parse:
    target_username: str | None
    target_game_id: str | None
    archive_unit: str | None      # "2024/07" or "since=..&until=.." chunk id
```

Note the rename: `RawRecord.body_hash` fingerprints the **whole payload** for raw-dedup; the per-game identity hash is `content_hash` over a canonical field subset (§10) and is computed later, at normalization, per game. One raw body → many `content_hash`es.

A streamed endpoint (Lichess NDJSON) yields **one page-level `RawRecord` whose `body` is the whole NDJSON blob**, written through as lines arrive; individual games are split out during normalization. The invariant is: *the raw bytes are durably stored before any game is normalized.* For large bodies `storage/raw.py` compresses transparently; `RawRecord` carries the uncompressed bytes and lets storage decide the codec.

### The ProviderClient interface

`ProviderClient` is an ABC in `providers/base.py`. Every method is **raw-returning** and **serial** (no method fans out concurrent requests). Iterators are lazy so a resumable job can checkpoint after each yielded record.

```python
# providers/base.py

class ProviderClient(ABC):
    # ---- identity / metadata ----
    @abstractmethod
    def key(self) -> str: ...                    # "chess.com" | "lichess"
    @abstractmethod
    def display_name(self) -> str: ...           # "Chess.com" | "Lichess"
    @abstractmethod
    def user_agent(self) -> str: ...             # descriptive UA (required by both)

    # ---- single-shot user fetches ----
    @abstractmethod
    def get_user_profile(self, username: str) -> RawRecord: ...
    @abstractmethod
    def get_user_stats(self, username: str) -> RawRecord: ...

    # ---- archive planning ----
    @abstractmethod
    def list_archive_units(self, username: str, since: int | None,
                           until: int | None) -> list["ArchiveUnit"]: ...
        # chess.com: one ArchiveUnit per calendar month (from /games/archives)
        # lichess:   per-calendar-month date-range chunks synthesized over
        #            [max(createdAt, since), until]; NO server-side month concept

    # ---- bulk game fetches (the streaming seam) ----
    @abstractmethod
    def iter_user_games(
        self, username: str, since: int | None, until: int | None,
        filters: "GameFilters",
    ) -> Iterator[RawRecord]: ...
        # chess.com: iterate archive months in range, GET each /YYYY/MM (with ETag),
        #            yield one RawRecord per month page.
        # lichess:   GET /api/games/user/{u} with Accept: x-ndjson, stream lines,
        #            yield one page RawRecord per chunk. since/until in MS at the wire.

    # ---- game-level fetches ----
    @abstractmethod
    def get_game(self, game_ref: str) -> RawRecord: ...
        # lichess:   GET /api/game/{id}  (true by-id endpoint)
        # chess.com: derive owning archive (username + YYYY/MM) from the game url,
        #            GET /pub/player/{u}/{YYYY}/{MM} (ETag/304), extract by uuid.
        #            There is NO single-game-by-id Chess.com endpoint.
    @abstractmethod
    def get_games_by_ids(self, refs: Sequence[str]) -> Iterator[RawRecord]: ...
        # lichess:   POST /api/games/export/_ids, ≤~300 ids/batch, stream NDJSON.
        # chess.com: group refs by owning (username, YYYY/MM), fetch each archive
        #            once, extract the requested uuids — archive-mediated, not by-id.

    # ---- policy hook (rate-limit + conditional cache) ----
    @abstractmethod
    def policy(self) -> "FetchPolicy": ...
```

Supporting value objects, also in `base.py`:

```python
@dataclass(frozen=True)
class ArchiveUnit:
    provider: str
    username: str
    unit_id: str          # "2024/07" (chess.com) | "2024-07" range chunk (lichess)
    url: str | None       # chess.com month URL; None for lichess synthetic chunks
    since: int | None     # epoch s, inclusive  (lichess chunk bounds)
    until: int | None     # epoch s, exclusive
    immutable: bool       # chess.com past month = True; current month/lichess = False

@dataclass(frozen=True)
class GameFilters:
    rated: bool | None = None
    perf_types: tuple[str, ...] = ()   # shared taxonomy keys; parser maps to native
    color: str | None = None
    include_moves: bool = True
    include_clocks: bool = False
    include_evals: bool = False
    include_opening: bool = True
    max_games: int | None = None
```

### FetchPolicy: the rate-limit / cache hook

`policy()` returns a per-provider `FetchPolicy`. The **job runner** owns the actual sleeping and retry loop; the policy just declares the numbers and reactions, so politeness rules are data, not scattered `sleep()` calls.

```python
@dataclass(frozen=True)
class FetchPolicy:
    min_delay_s: float                 # polite serial spacing between requests
    supports_conditional: bool         # True → send If-None-Match / If-Modified-Since
    honor_retry_after: bool            # chess.com: honor Retry-After header on 429
    fixed_429_backoff_s: float | None  # lichess: HARD 60s stop on 429
    max_retries: int
    def next_delay(self, status: int, retry_after: float | None) -> float:
        # chess.com: base=min_delay; on 429 → max(retry_after, exp backoff)
        # lichess:   on 429 → return fixed_429_backoff_s (60), non-negotiable
        ...
```

| | chess.com policy | lichess policy |
|---|---|---|
| `min_delay_s` | small polite delay (config) | small polite delay (config) |
| `supports_conditional` | **True** (ETag / Last-Modified / 304) | False (weak; rely on content_hash) |
| `honor_retry_after` | True | n/a |
| `fixed_429_backoff_s` | None (backoff schedule) | **60.0 (hard rule)** |

The runner loop is uniform across providers:

```
for each planned request in a job:
    sleep(policy.min_delay_s since last request)
    attach conditional headers if policy.supports_conditional and we have etag/last_modified
    send serially with client.user_agent()
    on 200/206 → build RawRecord(body), store raw, log fetch_log(200)
    on 304     → RawRecord(status=304, body=None); reuse prior raw; log 304
    on 404/410 → RawRecord(no body); record errors row (neutral); advance job cursor
    on 429     → log; sleep(policy.next_delay(429, retry_after)); retry (≤ max_retries)
    checkpoint job cursor (archive unit / stream watermark) → resumable
```

### The Parser and shared normalized DTOs

Parsing is a distinct class per provider (`providers/chesscom/parser.py`, `providers/lichess/parser.py`) implementing a small `Parser` protocol. Parsers are **pure**: `RawRecord (or raw_payloads body) → DTO`, no network, no DB writes. `normalize/users.py` and `normalize/games.py` call the parser, then upsert via `repository.py`.

```python
class Parser(Protocol):
    def parse_user(self, raw: RawRecord) -> "NormalizedUser": ...
    def parse_game(self, raw_body_one_game: bytes) -> "NormalizedGame": ...
    # a games-page RawRecord (chess.com month / lichess NDJSON blob) is split
    # into per-game raw bodies upstream, so parse_game always sees ONE game.
```

The DTOs are the **contract** between the provider seam and the shared store. Provider quirks are already resolved (timestamps in seconds, outcome collapsed or `None`, native strings preserved in `*_raw`).

```python
@dataclass(frozen=True)
class NormalizedUser:
    provider: str
    provider_user_id: str | None     # chess.com numeric player_id; lichess id (==lc username); None until profile resolves it
    username_normalized: str         # lowercased, lookup key
    display_username: str            # original casing
    title: str | None
    account_status_raw: str | None   # "closed:fair_play_violations" | "tosViolation" | …
    created_at: int | None           # epoch SECONDS (converted from ms for lichess)
    last_seen_at: int | None         # epoch SECONDS
    country: str | None
    is_verified: bool | None
    # perfs/stats normalized separately into user_snapshots

@dataclass(frozen=True)
class NormalizedParticipant:
    color: Literal["white", "black"]
    provider_user_id: str | None
    username_normalized: str | None
    display_username: str | None
    rating: int | None
    rating_diff: int | None
    rd: int | None
    result_raw: str | None           # "checkmated" | "outoftime" | "resign" | derived
    is_ai: bool = False              # lichess aiLevel present

@dataclass(frozen=True)
class NormalizedGame:
    provider: str
    provider_game_id: str | None     # chess.com uuid; lichess 8-char id
    canonical_url: str | None
    content_hash: str                # over deterministic canonical subset of raw body (§10)
    rated: bool | None
    variant_key: str                 # shared taxonomy; e.g. "standard","chess960","threecheck"
    variant_raw: str                 # native: chess.com rules / lichess variant
    time_class: str                  # shared: bullet|blitz|rapid|classical|correspondence
    time_control_raw: str | None     # native TC string, e.g. "180+2","1/259200"
    outcome: Literal["white_win", "black_win", "draw"] | None  # None = not decided
    is_live: bool                    # not-yet-terminal (in-progress) → refetch-eligible
    status_raw: str | None           # native status/termination verbatim
    end_time: int | None             # epoch SECONDS
    start_time: int | None           # epoch SECONDS (chess.com daily; lichess createdAt)
    white: NormalizedParticipant
    black: NormalizedParticipant
    eco: str | None
    opening_name: str | None
    opening_ply: int | None
    pgn: str | None
```

Result collapse lives entirely in the parser (full rules in §10):

- **Chess.com** — read `white.result` / `black.result`; exactly one is `"win"` → that color wins; both a draw-family code → `draw`.
- **Lichess** — `winner` present → `{white_win|black_win}` regardless of status term; `winner` **absent AND a terminal status** (`draw`,`stalemate`,`outoftime`,`mate`,…) → `draw`; `winner` **absent AND a non-terminal status** (`aborted`,`unknownfinish`,`noStart`) → `outcome = None`, `is_live` per whether it may still progress. The `status` string is always retained in `status_raw`, never re-judged.

Provider account-status labels flow through as **neutral facts** in `account_status_raw`; the parser never sets a guilt flag or derives a cheating determination.

### registry.py — provider key to client instance

`registry.py` is the single lookup that turns a `providers.key` string into a live, config-injected `ProviderClient`. It is the only module that imports the concrete client classes.

```python
# providers/registry.py  (sketch)

_FACTORIES: dict[str, Callable[[ProviderSettings, HttpSession], ProviderClient]] = {
    "chess.com": ChessComClient,
    "lichess":   LichessClient,
}

def known_keys() -> list[str]:            # backs `chess-crawl provider list`
    return sorted(_FACTORIES)

def get_client(key: str, cfg: Config, session: HttpSession) -> ProviderClient:
    if key not in _FACTORIES:
        raise UnknownProvider(key)        # CLI validates PROVIDER against this
    return _FACTORIES[key](cfg.provider(key), session)

def get_parser(key: str) -> Parser:
    return {"chess.com": ChessComParser, "lichess": LichessParser}[key]()
```

The `PROVIDER` CLI argument, `discovery_jobs` execution, and the `providers` table row all key off the same lowercase strings `chess.com` / `lichess`, so registry membership is the single source of "which providers exist."

### config.py — per-provider settings injection

`config.py` loads a layered config (defaults → config file → env → CLI flags) and exposes a per-provider slice. Clients never read global config directly; the registry injects a `ProviderSettings`.

```python
@dataclass(frozen=True)
class ProviderSettings:
    key: str
    min_delay_s: float          # politeness spacing
    user_agent: str             # descriptive UA (contact/app info)
    oauth_token: str | None     # lichess OPTIONAL; raises rate limits; never required; never logged
    max_retries: int
    default_page_max: int | None

class Config:
    def provider(self, key: str) -> ProviderSettings: ...
```

- **user-agent** — both providers want a descriptive UA; `client.user_agent()` returns `settings.user_agent`.
- **delay** — feeds `FetchPolicy.min_delay_s`; tunable per provider.
- **token** — only meaningful for Lichess (`Authorization: Bearer`), **supported in v1** but strictly optional, personal-use, and never a precondition for reads; Chess.com ignores it (PubAPI is unauthenticated). The token is never written to any raw payload, log, or provenance record (§12, §15).

### Why the storage model stays shared

The payoff: **`storage/schema.sql` has no provider-specific tables or columns.** `provider_users`, `games`, `game_participants`, `raw_payloads`, `source_records`, `discovery_jobs` all carry a `provider` discriminator, not a provider-shaped schema. Provider divergence is absorbed in exactly three places — the client (fetch shape), the policy (etiquette), the parser (vocabulary) — and each divergence is *resolved into the shared DTO* before it reaches storage:

- ms vs s → parser converts to seconds; store is uniform.
- monthly-archive vs NDJSON-stream → both funnel into `iter_user_games` yielding `RawRecord`s and `source_records` rows; `games` doesn't know which happened.
- numeric player_id vs id-equals-username → both land in `provider_user_id`; the `UNIQUE(provider, provider_user_id)` partial index and `UNIQUE(provider, username_normalized)` index cover both.
- ETag/304 vs content-hash dedup → both reduce to "do we already have this raw body?"; `raw_payloads` + `body_hash` + `UNIQUE(content_hash)` on games is the shared dedup, with conditional caching a chess.com-only optimization on top.

Adding a third provider later means writing one `client.py`, one `parser.py`, one `FetchPolicy`, and one registry entry — touching nothing in storage, jobs, reports, or export.

### Capability comparison

| Capability | chess.com | lichess |
|---|---|---|
| **Identity model** | Stable numeric `player_id`; username can change | `id` == lowercased username; no separate numeric id |
| **Archive unit** | Immutable calendar-month archive (`/games/YYYY/MM`) | Date-range NDJSON stream; no month concept (client synthesizes per-month range chunks) |
| **Single game by id** | **No by-id endpoint** — resolve via owning monthly archive (username + `YYYY/MM` from url, extract by `uuid`) | True by-id: `GET /api/game/{id}`; batch `POST /api/games/export/_ids` |
| **Timestamp unit** | Epoch **seconds** (stored as-is) | Epoch **milliseconds** (parser ÷1000 → seconds) |
| **Conditional caching** | Strong: ETag + Last-Modified + 304; past months permanently cacheable | Weak; rely on game immutability + `content_hash` dedup |
| **Streaming** | Per-month JSON pages (no stream needed) | NDJSON stream required (`Accept: x-ndjson`), one game per line; must stream |
| **429 reaction** | Serial + honor `Retry-After` / backoff | Serial + **hard 60s stop** before resuming |
| **Result semantics** | Per-color result codes (`win`/`checkmated`/`timeout`/`agreed`/…) | `winner` (white/black/absent) + `status` (`mate`/`outoftime`/`draw`/…) |
| **Variant/speed taxonomy** | `rules` (chess/chess960/kingofthehill/…) + `time_class` (daily/rapid/blitz/bullet) | `variant` (standard/chess960/threeCheck/…) + `speed`/`perf` (ultraBullet…correspondence) |
| **Username mutability** | Mutable (rely on `player_id` for identity continuity) | Immutable (no rename feature) |
| **Auth** | None (public, unauthenticated) | Optional personal bearer token → higher limits (never required) |

Both providers normalize into the same `{white_win, black_win, draw}` outcome (plus `NULL` for undecided games) and the same shared variant/speed keys, with native strings preserved in `variant_raw` / `time_control_raw` / `status_raw` / `result_raw`.

---

## 6. Project Structure

```
chess-crawl/                          # repo root (distribution: chess-crawl)
├── pyproject.toml                    # build metadata, deps, console_scripts entrypoint, tool config
├── README.md                         # quickstart, ethics/ToS note, CLI usage, provider matrix
├── LICENSE
├── .gitignore
│
├── chess_crawl/                      # importable package (chess_crawl)
│   ├── __init__.py                   # package version, public re-exports
│   ├── __main__.py                   # `python -m chess_crawl` -> cli.app()
│   ├── cli.py                        # Typer app: init/provider/fetch/crawl/jobs/query/export command wiring
│   ├── config.py                     # resolve DB path, User-Agent, polite-delay, optional Lichess token, data dir
│   │
│   ├── providers/                    # provider abstraction layer
│   │   ├── __init__.py
│   │   ├── base.py                   # ProviderClient interface + shared DTOs (RawRecord, NormalizedUser/Game, FetchPolicy)
│   │   ├── registry.py               # map provider key ("chess.com","lichess") -> ProviderClient instance/factory
│   │   ├── chesscom/
│   │   │   ├── __init__.py
│   │   │   ├── client.py             # HTTP fetchers: profile/stats/archives-list/monthly-archive; by-id via archive; ETag/304 + polite delay
│   │   │   ├── parser.py             # parse Chess.com JSON bodies -> shared DTOs (result codes, seconds timestamps)
│   │   │   └── endpoints.py          # URL builders + constants for api.chess.com/pub/*
│   │   └── lichess/
│   │       ├── __init__.py
│   │       ├── client.py             # HTTP fetchers incl. httpx.stream NDJSON; 429->60s hard pause; optional token
│   │       ├── parser.py             # parse Lichess NDJSON/JSON -> shared DTOs (winner+status, ms->s)
│   │       └── endpoints.py          # URL builders + constants for lichess.org/api/*
│   │
│   ├── storage/                      # persistence layer (single-file SQLite archive)
│   │   ├── __init__.py
│   │   ├── db.py                     # connection factory, PRAGMAs (WAL, foreign_keys), transaction helpers
│   │   ├── schema.sql                # canonical DDL for all tables + indexes (source of truth)
│   │   ├── migrations.py             # tiny migration runner driven by schema_migrations
│   │   ├── raw.py                    # write/read raw_payloads (gzip/zstd, body_hash, source_records link, fetch_logs)
│   │   └── repository.py             # typed upserts/queries over providers/provider_users/games/... (idempotent)
│   │
│   ├── jobs/                         # durable, resumable acquisition work
│   │   ├── __init__.py
│   │   ├── models.py                 # DiscoveryJob dataclasses + job-kind enum, state machine, discovery_edges
│   │   ├── runner.py                 # serial job executor: claim -> fetch -> persist -> enqueue; restart-safe
│   │   └── discovery.py              # discovery strategies (opponent expansion, archive fan-out, id batching)
│   │
│   ├── normalize/                    # raw payload -> normalized rows (re-runnable, no refetch)
│   │   ├── __init__.py
│   │   ├── users.py                  # provider_users/user_snapshots from raw profile+stats bodies
│   │   ├── games.py                  # games/game_participants/ratings_at_game/time_controls/variants
│   │   └── codes.py                  # result/status/variant/speed code maps -> shared outcome + taxonomy
│   │
│   ├── reports/                      # read-side query + rendering (no fetching)
│   │   ├── __init__.py
│   │   ├── queries.py                # parameterized SQL for `query user` / `query game` and summaries
│   │   └── render.py                 # format query results as tables/plain text; carries neutral disclaimers
│   │
│   └── export/                       # user-controlled local exports
│       ├── __init__.py
│       ├── games.py                  # export normalized games (CSV/NDJSON/PGN) from local archive
│       └── graph.py                  # export discovery graph (edges/nodes) for GEXF/GraphML/edge-list
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                   # tmp DB fixture, FakeClock, seeded providers, fake-transport / no_network guard
│   ├── fixtures/                     # sample provider responses (recorded, redacted, deterministic)
│   │   ├── chesscom/
│   │   │   ├── player.json           # /pub/player/{username}
│   │   │   ├── stats.json            # /pub/player/{username}/stats
│   │   │   ├── archives.json         # /pub/player/{username}/games/archives
│   │   │   └── archive_2024_01.json  # /pub/player/{username}/games/2024/01
│   │   ├── lichess/
│   │   │   ├── user.json             # /api/user/{username}
│   │   │   ├── games.ndjson          # /api/games/user/{username} (multi-line stream sample)
│   │   │   └── game.json             # /api/game/{gameId}
│   │   └── graphs/
│   │       ├── chesscom_graph.json   # tiny known crawl graph
│   │       └── lichess_graph.json
│   ├── test_providers_chesscom.py    # fetch+parse chess.com against fixtures
│   ├── test_providers_lichess.py     # fetch+parse lichess incl. streaming NDJSON mock
│   ├── test_storage.py               # migrations, raw round-trip, upsert idempotency, unique constraints
│   ├── test_normalize.py             # DTO->rows, ms->s conversion, outcome/variant mapping, content_hash
│   ├── test_jobs.py                  # job state machine, resume/restart, opponent discovery
│   └── test_cli.py                   # Typer CliRunner smoke tests for every command
│
└── docs/
    ├── architecture.md               # layer diagram, raw-first flow, job lifecycle
    ├── data-model.md                 # table-by-table schema reference + identity rules
    ├── providers.md                  # Chess.com vs Lichess differences honored by the code
    └── ethics.md                     # public-API-only, neutral-labels, ToS/rate-limit policy
```

### Module responsibilities (one line each)

- `cli.py` — Typer command tree; enqueues jobs or runs read-side queries. No network/DB logic.
- `config.py` — layered config resolution + per-provider `ProviderSettings`; owns token secrecy.
- `providers/base.py` — `ProviderClient`, `Parser`, `FetchPolicy`, and shared DTOs (`RawRecord`, `NormalizedUser`, `NormalizedGame`, `NormalizedParticipant`).
- `providers/registry.py` — provider-key → client/parser factory; single source of "which providers exist."
- `providers/{chesscom,lichess}/{client,parser,endpoints}.py` — the only provider-specific code.
- `storage/db.py` — connections + PRAGMAs + transactions.
- `storage/schema.sql` / `migrations.py` — canonical DDL + idempotent forward migrations.
- `storage/raw.py` — raw-first writes (`raw_payloads`, `source_records`, `fetch_logs`), `body_hash` dedup, compression.
- `storage/repository.py` — idempotent natural-key upserts over normalized tables.
- `jobs/{models,runner,discovery}.py` — durable job engine, serial runner, discovery strategies.
- `normalize/{users,games,codes}.py` — pure raw→normalized transforms and shared taxonomies.
- `reports/{queries,render}.py` — read-only projections with non-suppressible disclaimers.
- `export/{games,graph}.py` — user-controlled local exports.

### Console-script entrypoint wiring

```toml
# pyproject.toml (excerpt)
[project.scripts]
chess-crawl = "chess_crawl.cli:app"
```

```python
# chess_crawl/cli.py (structure only)
import typer

app = typer.Typer(no_args_is_help=True, add_completion=True)
provider_app = typer.Typer(help="Inspect configured providers")
fetch_app    = typer.Typer(help="Fetch users / games")
crawl_app    = typer.Typer(help="Discovery crawls")
jobs_app     = typer.Typer(help="Job control")
query_app    = typer.Typer(help="Query the local archive")
export_app   = typer.Typer(help="Export from the local archive")

app.add_typer(provider_app, name="provider")   # provider list
app.add_typer(fetch_app,    name="fetch")       # fetch user | fetch games
app.add_typer(crawl_app,    name="crawl")       # crawl opponents
app.add_typer(jobs_app,     name="jobs")        # jobs status | jobs resume
app.add_typer(query_app,    name="query")       # query user | query game
app.add_typer(export_app,   name="export")      # export games | export graph

@app.command()                                   # chess-crawl init
def init(): ...
```

`chess_crawl/__main__.py` calls `from chess_crawl.cli import app; app()` so `python -m chess_crawl` and the installed `chess-crawl` script share one entrypoint.

---

## 7. Dependencies

The guiding constraint is **local-first, single-binary-feel, few dependencies**. Every dependency earns its place against a concrete requirement (streaming NDJSON, nested subcommands, durable local storage, raw retention, testable HTTP mocks). Anything not clearly justified is left to the standard library.

### HTTP client — httpx (recommended) over requests

| Concern | httpx | requests |
| --- | --- | --- |
| Streaming large bodies | First-class `with client.stream("GET", url) as r: r.iter_lines()` | `stream=True` + `iter_lines()` works but is more fragile, weaker typing, easy to leak connections |
| Lichess NDJSON | Ideal: stream line-by-line, never buffer a huge export | Possible but pushes toward `.content`/`.text` materialization |
| Per-request timeouts | Granular `Timeout(connect/read/write/pool)` | Single `(connect, read)` tuple, coarser |
| Connection reuse | `httpx.Client` keep-alive pool across serial requests | `Session` pooling, comparable |
| Conditional GET (ETag/304) | Explicit header control, clean 304 handling | Works, comparable |

**Recommendation: httpx.** The decisive factor is Lichess: `/api/games/user/{username}` returns a potentially huge NDJSON stream that *must* be consumed incrementally, and `httpx.stream(...)` is the clean, well-typed way to do it (parse-and-persist each line, apply the 60s-on-429 rule mid-stream, honor content_hash dedup as lines arrive). Pin `httpx>=0.27,<1.0`.

### CLI framework — typer (recommended), argparse as zero-dep fallback

The CLI is genuinely nested: `fetch user|games`, `crawl opponents`, `jobs status|resume`, `query user|game`, `export games|graph`, each with a positional `PROVIDER` arg. Typer models these as sub-`Typer` apps with type-annotated parameters, auto-generated `--help`, and shell completion essentially for free. Because Typer's surface is confined to `cli.py`, dropping to stdlib `argparse` later is a contained refactor. Pin `typer>=0.12,<1.0`.

### Storage — stdlib sqlite3 + tiny migration runner (recommended) over SQLAlchemy/SQLModel

The archive is a **single local SQLite file** with a hand-authored, canonical `schema.sql` and a stable set of tables and partial unique indexes (`UNIQUE(provider, provider_user_id) WHERE …`, `UNIQUE(content_hash)`, etc.). Stdlib `sqlite3` gives full control over PRAGMAs (WAL, `foreign_keys=ON`), partial indexes, `INSERT … ON CONFLICT` upserts, and explicit transactions — exactly the primitives the raw-first + idempotent-job design leans on. A ~50-line migration runner keyed on `schema_migrations` covers evolution. SQLAlchemy/SQLModel would add an ORM over a schema we already fully own. No dependency.

### Raw-body compression — stdlib gzip (default), zstandard (optional extra)

Every successful fetch is stored in `raw_payloads`, compressed when large. Stdlib `gzip` is the **guaranteed floor** (zero deps, universally readable). `zstandard` behind an optional extra gives materially better ratio/speed on large Lichess exports. The codec is recorded per row (`body_compression`) so payloads are self-describing; never make a base install unable to *read* the archive.

### PGN parsing — raw always stored; python-chess as an optional extra

Raw PGN/TCN is always preserved verbatim in `raw_payloads`, so no parser is needed for the core archive or for normalization (which reads structured JSON/NDJSON fields, not the move text). Deep move-level analysis (SAN/FEN walking, per-move features) is a value-add offered as an optional `analysis` extra (`python-chess>=1.999,<2.0`). If absent, ingest still succeeds.

### NDJSON handling — stdlib json, no extra dependency

NDJSON is one JSON object per line. Combined with httpx's `iter_lines()`, stdlib `json.loads` per non-empty line is complete, streaming, and dependency-free.

### Test stack — pytest + respx (or pytest-httpx), optional hypothesis

pytest is the baseline. The critical requirement is mocking httpx **including streaming responses**: `respx` (preferred) or `pytest-httpx` can register routes returning NDJSON stream bodies, exercising the line-by-line path against fixtures without network access. `hypothesis` is optional, useful for property-testing normalization invariants (ms→s conversion, content_hash determinism, outcome mapping).

### Recommended minimal dependency set

**Runtime (baseline install):**
```toml
dependencies = [
  "httpx>=0.27,<1.0",   # HTTP + streaming NDJSON (Lichess), conn reuse, per-request timeouts
  "typer>=0.12,<1.0",   # nested provider subcommands, help, completion
]
# storage: stdlib sqlite3 | compression: stdlib gzip | NDJSON: stdlib json  (no deps)
```

**Optional extras:**
```toml
[project.optional-dependencies]
zstd     = ["zstandard>=0.22,<1.0"]      # better raw-payload compression
analysis = ["python-chess>=1.999,<2.0"]  # deep PGN/move analysis (raw always stored regardless)
```

**Dev / test:**
```toml
[dependency-groups]
dev = [
  "pytest>=8,<9",
  "respx>=0.21,<1.0",        # httpx mocking incl. streaming responses (or: pytest-httpx>=0.30,<1.0)
  "hypothesis>=6,<7",        # optional: property tests for normalization invariants
]
```

**Version-range guidance:** pin `>=x.y,<X+1.0` for pre-1.0 libraries (httpx, typer, zstandard, respx) whose minors can carry breaking changes; `>=major,<major+1` for stable ones (pytest, hypothesis). Keep the baseline runtime deliberately at **two packages** — every addition is weighed against "can stdlib do this acceptably?"

---

## 8. Raw-First Storage

Raw-first storage is the invariant the whole archive rests on: **no normalized row exists that cannot be regenerated from a durably stored raw body.** Every successful fetch lands as an immutable `raw_payloads` row *before* any parser touches it, and normalization is a separate, versioned, re-runnable pass. If the parser is buggy today, we bump `parser_version` tomorrow and re-normalize the entire archive with zero network calls. This section owns the canonical DDL for `raw_payloads`, `source_records`, and `fetch_logs`; §9 references them.

### The shared endpoint/body-kind enum

One taxonomy names *what a body is* / *which endpoint produced it*, reused under the single column name `endpoint_type` across `raw_payloads`, `source_records`, `fetch_logs`, and `errors`:

```
user_profile | user_stats | archives_index | monthly_archive |
user_games_stream | game | games_by_ids | import_dump
```

It is **distinct from, but mapped to,** `discovery_jobs.kind`:

| discovery_jobs.kind | endpoint_type of the body it produces |
|---|---|
| `fetch_user_profile` | `user_profile` |
| `fetch_user_stats` | `user_stats` |
| `fetch_user_games` | `archives_index` (chess.com fan-out) / `user_games_stream` (lichess chunk) |
| `fetch_monthly_archive` | `monthly_archive` |
| `fetch_game_by_id` | `game` (lichess) / `monthly_archive` (chess.com, archive-mediated) |
| `fetch_games_by_ids` | `games_by_ids` (lichess) / `monthly_archive` (chess.com) |
| `import_export_dump` | `import_dump` |
| `crawl_opponents`, `resume` | *(no direct body)* |

### The `raw_payloads` Table

One row per **successfully-retrieved body worth keeping** — the source of truth. Append-mostly: rows are inserted on fetch, their `normalization_status` block is later updated in place; `raw_body` and provenance are never mutated after commit.

```sql
CREATE TABLE raw_payloads (
    id                   INTEGER PRIMARY KEY,               -- surrogate
    provider             TEXT    NOT NULL,                  -- 'chess.com' | 'lichess' (FK providers.key)
    endpoint_type        TEXT    NOT NULL,                  -- shared enum above
    provider_url         TEXT,                              -- concrete fetched URL when one exists
    canonical_source_key TEXT    NOT NULL,                  -- stable logical key (see below)
    request_params       TEXT,                              -- JSON: query params / POST body id-set
    response_status      INTEGER NOT NULL,                  -- HTTP status of the kept body (200/206)
    response_headers     TEXT,                              -- JSON: cache-relevant subset only
    content_type         TEXT,                              -- application/json | x-ndjson | x-chess-pgn
    fetched_at           INTEGER NOT NULL,                  -- epoch SECONDS UTC (wall clock of fetch)
    body_hash            TEXT    NOT NULL,                  -- 'sha256:<hex>' over DECOMPRESSED bytes
    body_compression     TEXT    NOT NULL DEFAULT 'none',   -- 'none' | 'gzip' | 'zstd'
    raw_body             BLOB    NOT NULL,                  -- bytes AS STORED (compressed if != none)
    body_bytes           INTEGER NOT NULL,                  -- length of the DECOMPRESSED body
    parser_version       TEXT,                              -- parser that last (attempted) normalization
    normalization_status TEXT    NOT NULL DEFAULT 'pending',-- pending|parsed|failed|skipped|stale
    normalized_at        INTEGER,                           -- epoch s of last normalization attempt
    error_ref            INTEGER,                           -- FK errors.id when status='failed'
    FOREIGN KEY (provider)  REFERENCES providers(key),
    FOREIGN KEY (error_ref) REFERENCES errors(id)
);
```

**`canonical_source_key`** is the logical identity of the resource, independent of transport details, so re-fetches of the same logical thing collide on a predictable key even if the concrete URL varies:

| endpoint_type | canonical_source_key example |
|---|---|
| `user_profile` | `chess.com/player/erik/profile` |
| `user_stats` | `chess.com/player/erik/stats` |
| `archives_index` | `chess.com/player/erik/games/archives` |
| `monthly_archive` | `chess.com/player/erik/games/2024/06` |
| `game` (lichess) | `lichess/game/<gameId>` |
| `user_games_stream` (lichess) | `lichess/games/user/thibault?since=..&until=..&rated=..` (params sorted) |
| `games_by_ids` (lichess) | `lichess/games/export/_ids#<sorted,joined ids hash>` |
| `import_dump` | `import/<sha256-of-file>` |

`canonical_source_key` is **not unique**: the same immutable resource can be fetched twice over months (two rows, equal `body_hash`). Uniqueness/dedup is a *policy* enforced by `body_hash`, not a table constraint — we deliberately keep the ability to record "we fetched X again on date Y and it was byte-identical," valuable provenance for mutable resources.

**`response_headers`** stores only the cache-relevant subset as JSON:

```json
{"etag": "\"a1b2...\"", "last_modified": "Wed, 03 Jul 2024 …", "content-type": "application/json", "content-length": "48213"}
```

#### Indexes

```sql
CREATE INDEX ix_raw_provider_endpoint ON raw_payloads(provider, endpoint_type);
CREATE INDEX ix_raw_body_hash         ON raw_payloads(body_hash);
CREATE INDEX ix_raw_norm_status       ON raw_payloads(normalization_status)
    WHERE normalization_status IN ('pending','failed','stale');
CREATE INDEX ix_raw_canonical_key     ON raw_payloads(canonical_source_key, fetched_at);
```

The partial index keeps the normalization work-queue scan cheap even with millions of already-`parsed` rows.

### Write-Order Guarantee

> **A normalized row is only ever written after (or in the same transaction that has already committed) the `raw_payloads` row it derives from. The raw body is durable at or before the moment any derived data becomes durable. Normalization never precedes raw persistence, and normalization failure never rolls back the raw body.**

```
Phase 1 — PERSIST RAW  (transaction A, must fully commit first)
  BEGIN
    compute body_hash over decompressed bytes
    IF exists raw_payloads WHERE body_hash = ? AND endpoint_type = ?:
        -> DEDUP short-circuit (see Idempotency); do NOT insert a second body
    ELSE:
        choose compression; INSERT raw_payloads(... normalization_status='pending')
  COMMIT                          <-- raw body is now durable, no matter what happens next

Phase 2 — NORMALIZE  (transaction B, independent, re-runnable)
  BEGIN
    read raw_payloads.raw_body (decompress in memory)
    parse -> DTOs (users, games, participants, ratings, ...)
    UPSERT normalized rows (provider_users, games, game_participants, ...)
    INSERT source_records linking each entity -> this raw_payload_id
    UPDATE raw_payloads SET normalization_status='parsed', parser_version=?, normalized_at=?
  COMMIT
  -- on parse error: transaction B ROLLS BACK its normalized writes,
  --   then a tiny separate txn sets status='failed', error_ref=<errors.id>.
  --   Phase 1's raw body is untouched.
```

Decoupling the two transactions is what makes **re-parse without refetch** natural: Phase 2 is a pure function of `(raw_body, parser_version)` and can be replayed at any time. Crash safety holds — if the process dies after Phase 1 and before Phase 2, the row sits at `pending` and the next `jobs resume` picks it up.

**Re-parse operation** — a parser upgrade is bulk maintenance, not a re-crawl:

```sql
UPDATE raw_payloads
   SET normalization_status='stale'
 WHERE endpoint_type IN (...affected...)
   AND (parser_version IS NULL OR parser_version < :new_version);
```

The normalization pass then drains `stale`/`pending` exactly as on first ingest. Normalized upserts are idempotent (keyed by `content_hash` / provider ids), so re-normalization converges. **No fetch, no rate-limit budget, no provider contact is ever required to re-normalize.**

### `normalization_status` Lifecycle

```
                 (raw committed)
                      │
                      ▼
     ┌────────────► pending ──────────┐
     │                │               │ parser says "nothing to
     │                │ parse OK      │ normalize here" (empty
     │                ▼               │ month, [] games, bughouse
     │             parsed             ▼  → skipped)
 re-parse            │             skipped
 requested           │ parser bumped   │
 (bulk UPDATE)       ▼   / bug fix     │ re-parse
     │             stale ◄─────────────┘ requested
     │                │
     │                ▼  (re-run Phase 2)
     └──── failed ◄── parse error ──► errors row (error_ref)
              │
              └── retry after parser fix ──► pending/stale
```

| status | meaning | terminal? |
|---|---|---|
| `pending` | raw stored, not yet normalized | no |
| `parsed` | normalized successfully at `parser_version` | until re-parse |
| `failed` | parse attempted, raised; `error_ref` set | no (retriable) |
| `skipped` | intentionally not normalized (empty body; bughouse/>2-player body; byte-identical to an earlier `parsed` payload) | no |
| `stale` | previously terminal, a newer `parser_version` should revisit | no |

`failed` never blocks the archive: the raw body is safe, the error is captured in `errors`, a later parser fix flips it back into the queue. `skipped` means "correctly decided not to emit normalized rows" — e.g. a bughouse game (§2), an empty `games` array, or a dedup match.

### `source_records` — Provenance (Many-to-Many)

Normalized entities and raw payloads are **many-to-many**; `source_records` records *which raw body justified which normalized fact*.

```sql
CREATE TABLE source_records (
    id             INTEGER PRIMARY KEY,
    entity_type    TEXT    NOT NULL,   -- 'user' | 'user_snapshot' | 'game' | 'game_participant' | ...
    entity_id      INTEGER NOT NULL,   -- surrogate id in the corresponding normalized table
    provider       TEXT    NOT NULL,   -- denormalized for scoped queries (FK providers.key)
    endpoint_type  TEXT    NOT NULL,   -- copied from the raw_payload (shared enum) for filtering
    source_key     TEXT,               -- e.g. the monthly-archive key, or game uuid within a dump
    json_pointer   TEXT,               -- optional: pointer into the raw body (e.g. /games/17)
    raw_payload_id INTEGER NOT NULL,   -- FK raw_payloads.id
    first_seen_at  INTEGER NOT NULL,   -- epoch s: when THIS entity was first linked to THIS payload
    FOREIGN KEY (raw_payload_id) REFERENCES raw_payloads(id),
    FOREIGN KEY (provider)       REFERENCES providers(key),
    UNIQUE (entity_type, entity_id, raw_payload_id)
);

CREATE INDEX ix_srcrec_entity  ON source_records(entity_type, entity_id);
CREATE INDEX ix_srcrec_payload ON source_records(raw_payload_id);
```

`UNIQUE(entity_type, entity_id, raw_payload_id)` makes provenance links **idempotent** — re-normalizing a payload re-asserts the same links via `INSERT … ON CONFLICT DO NOTHING`, preserving the original `first_seen_at`.

Two shapes:

- **One payload → many entities.** A single `monthly_archive` body is the source for dozens of `game` rows and their participants; each gets its own `source_records` row.
- **One entity → many payloads.** A single game legitimately appears in both players' monthly archives, plus a by-id fetch, plus an import dump. The `game` row is deduped to one normalized identity (by `content_hash` / `provider_game_id`) but accumulates several `source_records` rows — one per witnessing payload.

```
        raw_payloads                         normalized
   ┌────────────────────┐               ┌───────────────────┐
   │ #101 monthly_arch  │──┐         ┌──│ game #5001        │
   │  erik/2024/06      │  │  source │  │  (dedup identity) │
   └────────────────────┘  ├─records─┤  └───────────────────┘
   ┌────────────────────┐  │  (M:N)  │  ┌───────────────────┐
   │ #102 monthly_arch  │──┤         ├──│ game #5002        │
   │  hikaru/2024/06    │  │         │  └───────────────────┘
   └────────────────────┘  │         │
   ┌────────────────────┐  │         │   (game #5001 linked from
   │ #240 game (lichess)│──┘         │    payloads #101, #102, #240:
   │  <id of 5001>      │            │    same game, three witnesses)
   └────────────────────┘            │
```

### Compression Policy

Chosen per-payload at Phase 1 based on the **decompressed** size, recorded in `body_compression`.

| decompressed size | `body_compression` | rationale |
|---|---|---|
| `< 4 KiB` | `none` | overhead not worth it; JSON profiles stay inspectable |
| `4 KiB – threshold` | `gzip` | stdlib, always available, good ratio on JSON/PGN |
| large / bulk NDJSON streams | `zstd` **if available**, else `gzip` | Lichess streams / large exports compress far better with zstd |

- **`gzip` is the guaranteed floor** (stdlib). `zstd` is an optional dependency; the fetcher probes for it and silently downgrades to `gzip` if absent. A zstd row on a machine without zstd surfaces a clear error ("row #N is zstd-compressed; install `chess-crawl[zstd]`") rather than corrupting output; a maintenance command can bulk-recompress zstd→gzip for portability.
- `body_hash` and `body_bytes` are **always over the decompressed bytes**, invariant to compression.
- `raw_body` holds bytes *as stored*; every read path decompresses per `body_compression` before parse/export.

### `body_hash` Usage

`body_hash = 'sha256:' || hex(sha256(decompressed_body_bytes))` — a faithful fingerprint of the provider's response. It serves three jobs:

1. **Dedup of identical bodies.** Before inserting in Phase 1, look up `body_hash` (+ `endpoint_type`). A hit against an existing `parsed` row means byte-identical content — no second copy, no re-normalize; optionally record a lightweight re-fetch observation in `fetch_logs`.
2. **Change detection on mutable resources.** The Chess.com *current* month and any recent Lichess stream are mutable. A **different** `body_hash` for the same `canonical_source_key` proves the resource changed → store the new body as a fresh row (keeping history) and normalize it; upserts reconcile changed games into existing normalized identities.
3. **Corruption / integrity check.** On export or re-parse, re-hashing `raw_body` detects silent bit-rot or a mis-set `body_compression`.

`body_hash` (raw fingerprint) is distinct from a game's `content_hash` (normalized identity over a canonical subset). One raw body → many `content_hash`es.

### Idempotency

- **Immutable resource, re-fetched, body matches.** Chess.com conditional `If-None-Match` → `304` (recorded only in `fetch_logs`, no new payload). If a full body *is* returned, Phase 1's `body_hash` lookup matches → short-circuit; `normalization_status` stays `parsed`. Only side effect: an optional `fetch_logs` note.
- **First time / body changed.** New `body_hash` → new row → normalize once.
- **Normalization replay.** Re-parsing re-emits DTOs that upsert onto existing identities and re-assert `source_records` under its UNIQUE guard. The archive converges.
- **Resumability.** A job interrupted between Phase 1 and Phase 2 leaves `pending` rows; `jobs resume` drains them. Interrupted mid-normalization rolls back Phase 2, so a row is either fully `parsed` or still `pending`/`failed` — never half-written.

### `fetch_logs` vs `raw_payloads`

| | `fetch_logs` | `raw_payloads` |
|---|---|---|
| **Grain** | one row per **HTTP attempt** | one row per **kept body** |
| **Includes** | successes, `304`, `404`, `410`, `429`, timeouts, retries | only bodies worth preserving (`200`/`206`) |
| **Holds the body?** | no (or a truncated error snippet) | yes — the full `raw_body` blob |
| **Purpose** | operational log: rate-limit accounting, conditional-request outcomes, retry history, politeness auditing | the durable archive: source of truth for normalization |
| **Retention** | prunable telemetry | never pruned casually; it *is* the dataset |

Canonical merged DDL (referenced verbatim by §9 and §14):

```sql
CREATE TABLE fetch_logs (
    id             INTEGER PRIMARY KEY,
    provider       TEXT    NOT NULL REFERENCES providers(key),
    job_id         INTEGER REFERENCES discovery_jobs(id),   -- which job drove this attempt
    crawl_run_id   INTEGER REFERENCES crawl_runs(id),
    url            TEXT    NOT NULL,
    endpoint_type  TEXT    NOT NULL,                         -- shared enum
    method         TEXT    NOT NULL DEFAULT 'GET',
    status_code    INTEGER,                                  -- 200/206/304/404/410/429/5xx/NULL on transport error
    from_cache     INTEGER NOT NULL DEFAULT 0 CHECK(from_cache IN (0,1)),  -- 304 / immutable-month skip
    etag           TEXT,                                     -- validator seen/sent
    last_modified  TEXT,
    retry_after    INTEGER,                                  -- honored Retry-After seconds (429/503)
    bytes          INTEGER,
    duration_ms    INTEGER,
    attempt        INTEGER NOT NULL DEFAULT 1,
    attempted_at   INTEGER NOT NULL,                         -- epoch s
    raw_payload_id INTEGER REFERENCES raw_payloads(id),      -- the payload this attempt PRODUCED, if any
    error_ref      INTEGER REFERENCES errors(id)
);
CREATE INDEX ix_fetchlog_time   ON fetch_logs(provider, attempted_at);
CREATE INDEX ix_fetchlog_status ON fetch_logs(status_code);
CREATE INDEX ix_fetchlog_url    ON fetch_logs(url);
```

**Every kept body has a producing `fetch_logs` row** (`raw_payload_id` set), but **most `fetch_logs` rows have no payload** — a `304` proves our cached body is current, a `404`/`410` records absence/gone, a `429` records a throttle-and-backoff (Lichess: the 60s pause logged with `retry_after`), a timeout records a retriable failed attempt. This lets us audit rate-limit politeness and conditional-cache effectiveness without polluting the archive.

---

## 9. Normalized Schema

### Global SQLite Decisions

Every connection is opened with the same pragmas:

```sql
PRAGMA journal_mode = WAL;        -- concurrent readers during a serial writer; crash-safe resume
PRAGMA foreign_keys = ON;         -- enforce every FK below (must be re-issued per connection)
PRAGMA synchronous = NORMAL;      -- WAL-safe, fast enough for a serial/polite crawler
PRAGMA busy_timeout = 5000;       -- tolerate the reader/writer overlap WAL allows
PRAGMA foreign_keys;              -- asserted == 1 at startup; migrations refuse to run otherwise
```

Cross-cutting conventions:

- **Surrogate integer PKs.** Every entity table uses `id INTEGER PRIMARY KEY`. Natural keys are `UNIQUE`/partial-unique indexes. Exceptions: `providers` (`key` PK), `schema_migrations` (`version` PK), `ratings_at_game` (`PK(game_id,color)`).
- **Epoch-second integer timestamps, UTC.** Chess.com stored as-is; Lichess ms integer-divided by 1000 during normalization (`floor(ms/1000)`). Raw ms survive verbatim in `raw_payloads`.
- **Type affinity discipline.** `INTEGER` for ids/timestamps/counts/booleans, `TEXT` for strings/urls/enums, `TEXT`/`BLOB` for JSON. Booleans `INTEGER NOT NULL CHECK(col IN (0,1))`. Enums `TEXT … CHECK(col IN (...))`.
- **Raw/blob provenance via `source_records`.** Every normalized row that was materialized from a fetch links to the raw body through `source_records` (§8). `user_snapshots` additionally carries a direct `raw_payload_id` for one-hop lookup.
- **Partial unique indexes for nullable natural keys.** `CREATE UNIQUE INDEX … WHERE col IS NOT NULL`.
- **Upsert idiom.** `INSERT … ON CONFLICT(<natural key>) DO UPDATE …` (or `DO NOTHING`).

`raw_payloads`, `source_records`, and `fetch_logs` are defined canonically in §8; below are the remaining fourteen tables.

### providers

```sql
CREATE TABLE providers (
  key       TEXT PRIMARY KEY CHECK(key IN ('chess.com','lichess')),
  name      TEXT NOT NULL,                 -- 'Chess.com', 'Lichess'
  base_url  TEXT NOT NULL,                 -- https://api.chess.com/pub/ , https://lichess.org/api
  docs_url  TEXT,
  added_at  INTEGER NOT NULL               -- epoch s
);
```
Seeded at `init` via `INSERT … ON CONFLICT(key) DO NOTHING`.

### provider_users

A provider-scoped identity for one account on one provider. The anchor of the whole graph; a Chess.com `erik` and a Lichess `erik` are two rows, never merged.

```sql
CREATE TABLE provider_users (
  id                  INTEGER PRIMARY KEY,
  provider            TEXT NOT NULL REFERENCES providers(key),
  provider_user_id    TEXT,               -- chess.com numeric player_id (as text); lichess id; NULL until known
  username_normalized TEXT NOT NULL,      -- lowercased username, the lookup key
  display_username    TEXT NOT NULL,      -- original casing preserved
  account_status      TEXT,               -- provider-native label, neutral fact (see §15)
  title               TEXT,               -- GM/IM/... or NULL
  first_seen_at       INTEGER NOT NULL,   -- epoch s, when this crawl first learned of the user
  updated_at          INTEGER NOT NULL    -- epoch s, last time any column was refreshed
);

CREATE UNIQUE INDEX ux_pu_provider_pid
  ON provider_users(provider, provider_user_id) WHERE provider_user_id IS NOT NULL;
CREATE UNIQUE INDEX ux_pu_provider_uname
  ON provider_users(provider, username_normalized);
```

Upsert: discovery inserts by username; a later profile fetch backfills the id.

```sql
INSERT INTO provider_users(provider, provider_user_id, username_normalized,
                           display_username, account_status, title, first_seen_at, updated_at)
VALUES (:p, :pid, :uname, :disp, :status, :title, :now, :now)
ON CONFLICT(provider, username_normalized) DO UPDATE SET
   provider_user_id = COALESCE(excluded.provider_user_id, provider_users.provider_user_id),
   display_username = excluded.display_username,
   account_status   = excluded.account_status,
   title            = excluded.title,
   updated_at       = excluded.updated_at;
```

`first_seen_at` is pinned to first discovery. `provider_user_id` is only ever filled, never nulled back (`COALESCE`). A Chess.com **rename** is handled by matching the stable `provider_user_id` via `ux_pu_provider_pid` and updating `username_normalized`/`display_username`.

### user_snapshots

A point-in-time capture of a user's mutable public state (status, follower/game counts, per-category rating/perf blob, and the username as observed — which preserves Chess.com rename history). One row per *materially different* fetch; identical consecutive fetches collapse via `content_hash`.

```sql
CREATE TABLE user_snapshots (
  id               INTEGER PRIMARY KEY,
  provider_user_id INTEGER NOT NULL REFERENCES provider_users(id),
  captured_at      INTEGER NOT NULL,       -- epoch s, when we fetched
  observed_username TEXT NOT NULL,         -- display-cased username as seen (rename history)
  status           TEXT,                   -- provider-native account-status label at capture, neutral fact
  title            TEXT,
  country          TEXT,
  followers        INTEGER,                -- chess.com followers; NULL on lichess
  patron           INTEGER,                -- lichess patron flag (0/1); NULL on chess.com
  count_all        INTEGER,                -- total games (lichess count.all / derived)
  count_rated      INTEGER,
  count_win        INTEGER,
  count_loss       INTEGER,
  count_draw       INTEGER,
  perfs_or_stats   TEXT,                   -- normalized JSON of stats/perfs (canonicalized)
  content_hash     TEXT NOT NULL,          -- hash over the canonical subset of this snapshot
  raw_payload_id   INTEGER NOT NULL REFERENCES raw_payloads(id),
  UNIQUE(provider_user_id, content_hash)
);

CREATE INDEX ix_snap_user_time ON user_snapshots(provider_user_id, captured_at);
```

`content_hash` is computed over the canonical, order-stable subset that defines "state" (status + counts + perf/stats ratings + observed_username), *excluding* volatile fields such as `last_online`/`seenAt`, so a mere re-login does not spuriously create a snapshot.

```sql
INSERT INTO user_snapshots(...) VALUES (...)
ON CONFLICT(provider_user_id, content_hash) DO UPDATE
   SET captured_at = excluded.captured_at;   -- refresh "last confirmed" time, keep one row
```

### games

One normalized row per distinct game, provider-scoped, deduplicated three ways. Provider-native strings are retained alongside the shared taxonomy references.

```sql
CREATE TABLE games (
  id                INTEGER PRIMARY KEY,
  provider          TEXT NOT NULL REFERENCES providers(key),
  provider_game_id  TEXT,               -- chess.com uuid; lichess 8-char id; NULL if truly absent
  canonical_url     TEXT,               -- game.url / https://lichess.org/{id}
  content_hash      TEXT NOT NULL,      -- hash over deterministic canonical subset of raw game body (§10)
  variant_id        INTEGER NOT NULL REFERENCES variants(id),
  time_control_id   INTEGER NOT NULL REFERENCES time_controls(id),
  rated             INTEGER NOT NULL CHECK(rated IN (0,1)),
  outcome           TEXT CHECK(outcome IN ('white_win','black_win','draw')),  -- NULL = not decided
  is_live           INTEGER NOT NULL DEFAULT 0 CHECK(is_live IN (0,1)),       -- 1 = not yet terminal, refetch-eligible
  status_raw        TEXT,               -- chess.com per-color codes summary / lichess status verbatim
  created_at        INTEGER,            -- epoch s: start_time / createdAt(ms->s)
  ended_at          INTEGER,            -- epoch s: end_time / lastMoveAt(ms->s)
  ply_count         INTEGER,
  eco               TEXT,
  opening_name      TEXT,               -- lichess opening.name; NULL on chess.com
  opening_ply       INTEGER,            -- lichess opening.ply; NULL on chess.com
  tournament_ref    TEXT,               -- tournament/match/swiss url or id, if any (opaque in v1)
  first_seen_at     INTEGER NOT NULL    -- epoch s
);

CREATE UNIQUE INDEX ux_games_provider_gid
  ON games(provider, provider_game_id) WHERE provider_game_id IS NOT NULL;
CREATE UNIQUE INDEX ux_games_url
  ON games(canonical_url)             WHERE canonical_url IS NOT NULL;
CREATE UNIQUE INDEX ux_games_content_hash
  ON games(content_hash);             -- total: content_hash is NOT NULL, the backstop dedup

CREATE INDEX ix_games_ended ON games(ended_at);
CREATE INDEX ix_games_provider_time ON games(provider, ended_at);
CREATE INDEX ix_games_live ON games(provider, is_live) WHERE is_live = 1;  -- refetch candidates
```

- **`outcome`** is the shared 3-value normalization, **nullable**: `NULL` for a game with no decided result — Lichess `aborted`/`unknownfinish`/`noStart`, or any in-progress game (`is_live=1`). Terminal decided games always carry `white_win`/`black_win`/`draw`. This deliberately avoids baking a Chess.com "every game is decided" assumption into the shared model.
- **`is_live`** marks a not-yet-terminal game as refetch-eligible; `status_raw` carries the specifics.
- **Upsert precedence.** Immutable finished games conflict primarily on `content_hash`:

  ```sql
  INSERT INTO games(...) VALUES (...)
  ON CONFLICT(content_hash) DO UPDATE SET
     provider_game_id = COALESCE(games.provider_game_id, excluded.provider_game_id),
     canonical_url    = COALESCE(games.canonical_url, excluded.canonical_url);
  ```

  For a **live** game (`is_live=1`), whose `content_hash` legitimately changes as it progresses, normalization first matches the existing row by `(provider, provider_game_id)` and **updates in place** (new `content_hash`, possibly new `outcome`, `is_live` flipped to 0 on termination) — the mutable-resource change-detection path (§10). A byte-identical re-fetch short-circuits before any write.

### game_participants

The two sides of a (normalized) game. Bughouse / >2-player games are never normalized here (§2) — this table is strictly two rows per game.

```sql
CREATE TABLE game_participants (
  id                  INTEGER PRIMARY KEY,
  game_id             INTEGER NOT NULL REFERENCES games(id),
  color               TEXT NOT NULL CHECK(color IN ('white','black')),
  provider_user_id    INTEGER REFERENCES provider_users(id),  -- NULL if opponent not yet materialized
  username_normalized TEXT,                                   -- from the game body; NULL if only anonymized
  result_raw          TEXT,                                   -- per-color code / derived from winner+status
  is_winner           INTEGER CHECK(is_winner IN (0,1)),      -- NULL on a draw OR an undecided game
  is_ai               INTEGER NOT NULL DEFAULT 0 CHECK(is_ai IN (0,1)),
  UNIQUE(game_id, color)
);

CREATE INDEX ix_gp_user  ON game_participants(provider_user_id);
CREATE INDEX ix_gp_uname ON game_participants(username_normalized);
```

`is_winner` is `NULL` on a draw and on an undecided (`outcome IS NULL`) game. Upsert on `(game_id, color)` with `provider_user_id = COALESCE(...)`, so a participant first stored username-only is later linked without duplication.

### ratings_at_game

The rating each side held **as of that game** — historical, not the current stats in `user_snapshots`. A pure per-side fact keyed by `(game_id, color)`; **no user column** — the side's identity is resolved through `game_participants(game_id, color)`.

```sql
CREATE TABLE ratings_at_game (
  game_id     INTEGER NOT NULL REFERENCES games(id),
  color       TEXT NOT NULL CHECK(color IN ('white','black')),
  rating      INTEGER,                 -- white.rating / players.white.rating
  rating_diff INTEGER,                 -- lichess ratingDiff; NULL on chess.com
  rd          INTEGER,                 -- glicko deviation / provisional signal if provided; else NULL
  PRIMARY KEY(game_id, color)
);
```

`ON CONFLICT(game_id, color) DO UPDATE …`. Immutable in practice; re-normalization is a safe rewrite.

### time_controls

A deduplicated dictionary of distinct clock configurations, referenced by `games.time_control_id`. This is the **single home** for the shared time-class.

```sql
CREATE TABLE time_controls (
  id                INTEGER PRIMARY KEY,
  kind              TEXT NOT NULL CHECK(kind IN ('clock','correspondence')),
  initial_seconds   INTEGER,           -- base clock; NULL for correspondence
  increment_seconds INTEGER,           -- per-move increment; NULL for correspondence
  days              INTEGER,           -- correspondence days-per-move; NULL for clock
  time_class        TEXT NOT NULL      -- shared class: bullet|blitz|rapid|classical|correspondence
                      CHECK(time_class IN ('bullet','blitz','rapid','classical','correspondence')),
  raw_label         TEXT NOT NULL      -- provider-native string, verbatim: "600","180+2","1/259200","blitz"
);

CREATE UNIQUE INDEX ux_tc_tuple
  ON time_controls(kind,
                   COALESCE(initial_seconds,-1),
                   COALESCE(increment_seconds,-1),
                   COALESCE(days,-1),
                   time_class,
                   raw_label);
```

The shared `time_class` set is exactly `{bullet, blitz, rapid, classical, correspondence}`. Chess.com `daily` and Lichess `correspondence` both fold to `correspondence`; Lichess `ultraBullet` folds to `bullet`. The provider-native speed/`time_class` string (`daily`, `ultraBullet`, …) is preserved in `raw_payloads` and re-derivable; `raw_label` preserves the native clock descriptor. Get-or-create: `INSERT … ON CONFLICT DO NOTHING` then `SELECT id`.

### variants

The shared variant taxonomy.

```sql
CREATE TABLE variants (
  id                   INTEGER PRIMARY KEY,
  canonical_name       TEXT NOT NULL,     -- standard|chess960|crazyhouse|antichess|atomic|horde|
                                          --   kingofthehill|racingkings|threecheck|bughouse|fromposition
  provider             TEXT NOT NULL REFERENCES providers(key),
  provider_native_name TEXT NOT NULL,     -- chess.com rules value / lichess variant value, verbatim
  mapped               INTEGER NOT NULL DEFAULT 1 CHECK(mapped IN (0,1)),  -- 0 = unrecognized native string, needs review
  UNIQUE(provider, provider_native_name)
);
```

| provider | provider_native_name | canonical_name |
|----------|----------------------|----------------|
| chess.com | `chess` | `standard` |
| chess.com | `chess960` | `chess960` |
| chess.com | `kingofthehill` | `kingofthehill` |
| chess.com | `threecheck` | `threecheck` |
| chess.com | `bughouse` | `bughouse` |
| lichess | `standard` | `standard` |
| lichess | `kingOfTheHill` | `kingofthehill` |
| lichess | `threeCheck` | `threecheck` |
| lichess | `racingKings` | `racingkings` |
| lichess | `fromPosition` | `fromposition` |

Unknown native strings insert with `canonical_name = lower(native)`, `mapped = 0` (for later review); the game is never dropped.

### discovery_jobs

The durable, restartable unit of work (owned operationally by §11; DDL here). This is the canonical column and state vocabulary that CLI (§13) and reports (§14) conform to.

```sql
CREATE TABLE discovery_jobs (
    id             INTEGER PRIMARY KEY,
    crawl_run_id   INTEGER REFERENCES crawl_runs(id),   -- NULL for one-off/ad-hoc jobs
    parent_job_id  INTEGER REFERENCES discovery_jobs(id),
    provider       TEXT    NOT NULL REFERENCES providers(key),
    kind           TEXT    NOT NULL,                    -- exact spine kinds
    target         TEXT    NOT NULL,                    -- username | game_ref | id-list | import path
    params_json    TEXT    NOT NULL DEFAULT '{}',       -- since/until/filters/caps/depth/cursor
    state          TEXT    NOT NULL DEFAULT 'pending'
                     CHECK(state IN ('pending','in_progress','done','error','skipped','blocked')),
    priority       INTEGER NOT NULL DEFAULT 100,        -- LOWER value = popped sooner
    depth          INTEGER NOT NULL DEFAULT 0,
    attempts       INTEGER NOT NULL DEFAULT 0,
    dedup_key      TEXT    NOT NULL,                    -- canonical (provider,kind,target,params)
    enqueued_at    INTEGER NOT NULL,
    started_at     INTEGER,
    done_at        INTEGER,
    reason         TEXT                                 -- error/skip/block explanation, last Retry-After, etc.
);

CREATE INDEX ix_jobs_runnable  ON discovery_jobs(state, priority, enqueued_at);
CREATE INDEX ix_jobs_run_state ON discovery_jobs(crawl_run_id, state);
CREATE UNIQUE INDEX ux_jobs_dedup_live
    ON discovery_jobs(dedup_key) WHERE state IN ('pending','in_progress');
```

`kind ∈ {fetch_user_profile, fetch_user_stats, fetch_user_games, fetch_monthly_archive, fetch_game_by_id, fetch_games_by_ids, import_export_dump, crawl_opponents, resume}`.

### discovery_edges

The opponent graph: a directed edge "from_user played to_user", aggregated with a game count and pinned at its minimum discovery depth. One discovery strategy among several.

```sql
CREATE TABLE discovery_edges (
  id            INTEGER PRIMARY KEY,
  crawl_run_id  INTEGER REFERENCES crawl_runs(id),
  provider      TEXT NOT NULL REFERENCES providers(key),
  from_user_id  INTEGER NOT NULL REFERENCES provider_users(id),
  to_user_id    INTEGER NOT NULL REFERENCES provider_users(id),
  via_game_id   INTEGER REFERENCES games(id),   -- an exemplar game; NULL for aggregate-only edges
  game_count    INTEGER NOT NULL DEFAULT 1,     -- weight; incremented per genuinely-new participant pairing
  depth         INTEGER NOT NULL,               -- BFS depth at which this edge was first recorded (min)
  edge_kind     TEXT NOT NULL DEFAULT 'opponent',
  first_seen_at INTEGER NOT NULL,
  UNIQUE(provider, from_user_id, to_user_id)
);

CREATE INDEX ix_edge_from ON discovery_edges(from_user_id);
CREATE INDEX ix_edge_to   ON discovery_edges(to_user_id);
```

`UNIQUE(provider, from_user_id, to_user_id)` — one aggregated edge per ordered pair; `provider` is redundant (both user FKs are provider-scoped) but kept explicit to hard-enforce that **an edge never spans providers**.

```sql
INSERT INTO discovery_edges(crawl_run_id, provider, from_user_id, to_user_id, via_game_id,
                            game_count, depth, edge_kind, first_seen_at)
VALUES (:run, :p, :from, :to, :g, 1, :depth, 'opponent', :now)
ON CONFLICT(provider, from_user_id, to_user_id) DO UPDATE SET
   game_count  = discovery_edges.game_count + 1,
   depth       = MIN(discovery_edges.depth, excluded.depth),
   via_game_id = COALESCE(discovery_edges.via_game_id, excluded.via_game_id);
```

Weight increments are keyed off newly-inserted `game_participants`, so a replayed archive that produces no new games produces no increment.

### crawl_runs

The umbrella for one crawling invocation, grouping its jobs, fetches, edges, and counters.

```sql
CREATE TABLE crawl_runs (
  id           INTEGER PRIMARY KEY,
  seed_spec    TEXT NOT NULL,     -- e.g. 'chess.com/erik depth=2' -- human-readable entrypoint
  provider     TEXT NOT NULL REFERENCES providers(key),
  params_json  TEXT,              -- full resolved parameters (since/until, depth, filters, caps)
  status       TEXT NOT NULL CHECK(status IN ('running','paused','done','failed','cancelled')),
  counters     TEXT,              -- JSON: {jobs_total, jobs_done, games_new, users_new, bytes, ...}
  started_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  finished_at  INTEGER            -- NULL until terminal
);

CREATE INDEX ix_runs_status ON crawl_runs(status);
```

`crawl_runs.status` (run-level umbrella) is deliberately distinct from `discovery_jobs.state` (job-level). `jobs resume` reattaches to an existing `running`/`paused` row rather than forking a run.

### errors

Structured record of failed fetches/parses for triage, retry backoff, and dead-lettering.

```sql
CREATE TABLE errors (
  id            INTEGER PRIMARY KEY,
  provider      TEXT REFERENCES providers(key),
  url           TEXT,
  endpoint_type TEXT,                      -- shared enum
  error_kind    TEXT NOT NULL
                  CHECK(error_kind IN ('http_404','http_410','http_429','timeout','parse','stream','other')),
  status_code   INTEGER,
  message       TEXT,
  occurred_at   INTEGER NOT NULL,
  retry_count   INTEGER NOT NULL DEFAULT 0,
  is_dead       INTEGER NOT NULL DEFAULT 0 CHECK(is_dead IN (0,1))
);

CREATE INDEX ix_errors_url  ON errors(url);
CREATE INDEX ix_errors_live ON errors(is_dead, occurred_at);
```

`http_404`/`http_410` are typically terminal (`is_dead=1`); `http_429`/`timeout`/`stream` are retriable — a 429 forces the provider's mandatory pause (Lichess: full 60s) before resume. Recording is neutral: an error is an operational fact, never a judgment about an account.

### schema_migrations

```sql
CREATE TABLE schema_migrations (
  version    INTEGER PRIMARY KEY,   -- monotonically increasing
  name       TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);
```

Each migration runs in a transaction ending with `INSERT INTO schema_migrations`; an already-present version is skipped. Re-running `init` is a no-op.

### Game identity

Provider-scoped, deduplicated with a defense-in-depth trio:

- `UNIQUE(provider, provider_game_id)` **partial** — Chess.com `uuid`, Lichess 8-char base62 id.
- `UNIQUE(canonical_url)` **partial** — `game.url` / `https://lichess.org/{id}`.
- `UNIQUE(content_hash)` **total** — the always-present backstop over a deterministic canonical subset (§10) with a scheme version.

A single physical game collapses to one `games` row regardless of discovery path. `provider` is part of identity: an identical position on Chess.com and Lichess are two different games. Finished games are immutable (upsert only backfills id/url); live games mutate in place via the `(provider, provider_game_id)` match.

### User identity

Provider-scoped and **never merged across providers** — a first-class technical and ethical rule. Two unique indexes:

- `UNIQUE(provider, provider_user_id)` **partial** — Chess.com stable numeric `player_id`; Lichess id (== lowercased username). Partial because opponents are often discovered by handle before a profile fetch reveals the id (Chess.com), or trivially known (Lichess).
- `UNIQUE(provider, username_normalized)` **total** — the everyday lookup key; `display_username` preserves casing.

A **Chess.com rename** keeps the same `provider_user_id` row and rewrites the username; **Lichess** has no rename. `provider_user_id` is only ever filled, never nulled back.

### Foreign-Key / ER Relationship Summary

```
providers (key)
   │  1
   ├──────< provider_users (provider)
   ├──────< games (provider)
   ├──────< variants (provider)
   ├──────< fetch_logs (provider)
   ├──────< errors (provider)
   ├──────< crawl_runs (provider)
   └──────< discovery_edges (provider)

provider_users (id)
   │  1
   ├──────< user_snapshots (provider_user_id) ──1──> raw_payloads (raw_payload_id)
   ├──────< game_participants (provider_user_id, NULLABLE)
   ├──────< discovery_edges (from_user_id)
   └──────< discovery_edges (to_user_id)

games (id)
   │  1
   ├──────2 game_participants (game_id)              -- exactly two, UNIQUE(game_id,color)
   ├──────2 ratings_at_game (game_id)                -- PK(game_id,color); side via game_participants
   ├──────< discovery_edges (via_game_id, NULLABLE)
   ├───N:1─> variants (variant_id)
   └───N:1─> time_controls (time_control_id)

raw_payloads (id) ──1──< source_records (raw_payload_id) ~~> (entity_type,entity_id) soft-ref
                                                             → games / provider_users / user_snapshots / ...
raw_payloads (id) ──1──< fetch_logs (raw_payload_id, NULLABLE)

crawl_runs (id)
   │  1
   ├──────< discovery_jobs (crawl_run_id)
   ├──────< fetch_logs (crawl_run_id, NULLABLE)
   └──────< discovery_edges (crawl_run_id, NULLABLE)

schema_migrations (version)   -- standalone bookkeeping
```

### Idempotency guarantees

- **Raw-first, then derive.** Raw persisted before/with normalization; a crash between fetch and normalize loses nothing.
- **`providers` / `schema_migrations`** — `ON CONFLICT DO NOTHING` / version-guarded.
- **`provider_users`** — upsert on `(provider, username_normalized)`; `first_seen_at` pinned, id backfilled only.
- **`user_snapshots`** — upsert on `(provider_user_id, content_hash)`; identical re-polls bump `captured_at`.
- **`games`** — upsert on `content_hash` (immutable path) or `(provider, provider_game_id)` (live path); one row per physical game.
- **`game_participants` / `ratings_at_game`** — upsert on `(game_id, color)`.
- **`variants` / `time_controls`** — get-or-create on defining tuples.
- **`discovery_edges`** — upsert on `(provider, from_user_id, to_user_id)`; weight increments only for new participants; depth pinned to the minimum.
- **`crawl_runs`** — created once, mutated in place; resume reattaches.
- **`discovery_jobs`** — idempotent on `dedup_key` (live-partial unique); cursor blob makes each job resumable.
- **`fetch_logs` / `errors`** — append-only event logs (errors may upsert to bump `retry_count`/`is_dead`). Neither ever encodes a judgment about an account.

Net guarantee: killing the process at any point and re-running the same command converges to the identical database state — no duplicate users/games/edges/snapshots/dictionary rows, zero unnecessary refetches.

---

## 10. Normalization Rules

Normalization is a **pure, re-runnable transform** from a stored `raw_payloads` row into normalized rows. It never triggers network I/O: every rule reads only the raw body plus its `source_records` provenance. Re-running over an unchanged raw payload is idempotent (same surrogate ids reused via natural-key upserts).

```
raw_payloads.body ──▶ parser (provider-specific) ──▶ shared DTOs ──▶ normalize/* ──▶ rows
      (verbatim)         chesscom/ | lichess/         base.py         users,games,…
```

### Provider-Scoped Identity

Identity is **always** `(provider, provider_user_id)` — never a bare username, never cross-provider. A Chess.com `erik` and a Lichess `erik` are two unrelated `provider_users` rows and MUST NOT be merged, linked, or heuristically matched.

| Field | Chess.com | Lichess |
|---|---|---|
| `provider` | `chess.com` | `lichess` |
| `provider_user_id` | numeric `player_id` from `GET /pub/player/{username}` **only** | `id` (== lowercased username; no separate numeric id exists) |
| `username_normalized` | `lower(username)` | `lower(username)` (== `id`) |
| `display_username` | provider's original-cased `username` | provider's original-cased `username` |

Rules:

- `username_normalized` is the **lookup key**; `display_username` is presentation only.
- **Chess.com identity is resolved ONLY from the profile endpoint.** Opponents discovered inside a game body yield only a **username** (and possibly a per-player `uuid`, which is a separate player GUID — *not* the numeric `player_id`). The trailing segment of a member `@id` URL (`.../pub/player/{username}`) is the **username**, not the id. Therefore `provider_user_id` stays **NULL** for such users until a `fetch_user_profile` resolves the numeric `player_id`, after which it is backfilled. Never derive `player_id` from `@id` or from a game-body `uuid`.
- Uniqueness is enforced by two indexes so a NULL-id row still can't duplicate a username:
  - `UNIQUE(provider, provider_user_id) WHERE provider_user_id IS NOT NULL` (partial)
  - `UNIQUE(provider, username_normalized)`
- Upsert order in `normalize/users.py`: match on `(provider, provider_user_id)` first (authoritative); if id is NULL, fall back to `(provider, username_normalized)`; when a later profile fetch supplies the numeric id, **attach it to the existing username row**.

```
find_user(provider, id=None, uname):
    if id is not None:
        row = by (provider, id)          # authoritative
        if row: reconcile_username(row, uname); return row
    row = by (provider, lower(uname))     # username fallback
    if row and id is not None: row.provider_user_id = id  # backfill
    return row or insert(...)
```

### Changed Usernames and Account-State Edge Cases

**Chess.com (rename allowed, `player_id` stable):** identity is the numeric id. When a fetch's `player_id` matches an existing row but the username differs:
1. Write a **new** `user_snapshots` row capturing `observed_username` + status + `captured_at` (rename history preserved).
2. Update the live `provider_users` row's `username_normalized` + `display_username` in place (id unchanged, all game FKs stay valid).
3. If the *new* username collides with a different existing row (a recycled handle), keep them distinct by id and let the partial-unique index arbitrate.

**Lichess (no rename):** `id` is effectively immutable. Edge cases, all as **neutral facts, never merges**:
- Closed / disabled / `tosViolation` accounts: still a valid `provider_users` row; record status in the snapshot, keep games. Do not delete.
- Games referencing a since-closed opponent may surface the user anonymized. Store what the game body gives; leave `provider_user_id` (and `username_normalized`) NULL if only an anonymized placeholder is present — do **not** invent an id.

### Timestamps

All normalized time columns are **INTEGER unix epoch SECONDS, UTC**. Raw values are preserved verbatim in `raw_payloads`.

| Normalized target | Chess.com source | Lichess source | Conversion |
|---|---|---|---|
| `games.created_at` | `start_time` (daily) else NULL | `createdAt` (ms) | Lichess: `// 1000` |
| `games.ended_at` | `end_time` (s) | `lastMoveAt` (ms) | Lichess: `// 1000` |
| `provider_users` created (`created_at` DTO) | `joined` (s) | `createdAt` (ms) | Lichess `// 1000` |
| `provider_users` last-seen (`last_seen_at` DTO) | `last_online` (s) | `seenAt` (ms) | Lichess `// 1000` |
| `user_snapshots.captured_at` | fetch wall-clock (s) | fetch wall-clock (s) | — |

Rules: Chess.com passes through (never multiply). Lichess floor-divides by 1000 (`ms // 1000`); never round, never apply timezone math (epoch is already UTC). Missing timestamp → NULL (Chess.com non-daily games have no `start_time` → `created_at` NULL, never copy `end_time`). The parser must not mutate raw.

### Ratings at Game Time

`ratings_at_game` captures the rating each side carried into/out of this specific game — distinct from current stats. The side is `(game_id, color)`; whose side it is comes from `game_participants(game_id, color)` — **no user column here.**

| Column | Chess.com | Lichess |
|---|---|---|
| `game_id`, `color` | white/black seat | white/black seat |
| `rating` | `white.rating` / `black.rating` | `players.{color}.rating` |
| `rating_diff` | NULL (not provided per game) | `players.{color}.ratingDiff` (may be negative; NULL for provisional/casual) |
| `rd` | NULL/unknown | glicko deviation / provisional signal if present, else NULL |

`rating` is NULL when the body omits it (unrated casual, bot `aiLevel` seat) — never default to 0 or to a current snapshot. This table is **never** used to back-derive "current" rating.

### Result / Status Normalization

Shared outcome enum on `games.outcome`: `white_win | black_win | draw`, or **NULL** for an undecided game. Raw is always retained: `games.status_raw` (provider status/term) and `game_participants.result_raw` (per-seat code).

**(a) Chess.com — per-color `result` code → outcome.** The color whose code is `win` won; draw codes appear on both colors.

| Chess.com per-color `result_raw` | Outcome contribution |
|---|---|
| `win` | that color wins |
| `checkmated` / `resigned` / `timeout` / `abandoned` / `lose` | other color wins |
| `kingofthehill` / `threecheck` / `bughousepartnerlose` (variant loss) | other color wins |
| `agreed` / `repetition` / `stalemate` / `insufficient` / `50move` / `timevsinsufficient` | `draw` |

Resolution: `white_win` if `white.result=='win'`; `black_win` if `black.result=='win'`; `draw` if either code ∈ draw-set. Store exact per-color strings in `game_participants.result_raw`; a representative term (or PGN `Termination`) in `games.status_raw`. (Bughouse games are not normalized at all — §2.)

**(b) Lichess — `winner` + `status` → outcome.**

| Lichess `winner` | Lichess `status` (examples) | Outcome | is_live |
|---|---|---|---|
| `white` | `mate`,`resign`,`outoftime`,`timeout`,`variantEnd`,`cheat`,`nostart` | `white_win` | 0 |
| `black` | `mate`,`resign`,`outoftime`,`timeout`,`variantEnd`,`cheat`,`nostart` | `black_win` | 0 |
| absent | `draw`,`stalemate`,`outoftime` (both flagged/insufficient),`variantEnd` (drawn) | `draw` | 0 |
| absent | `started`,`created` (in progress) | **NULL** | **1** |
| absent | `aborted`,`unknownfinish`,`noStart` (voided, will not resume) | **NULL** | 0 |

Rules:
- `winner` present → that color wins regardless of status term.
- `winner` absent **AND terminal** status → `draw`.
- `winner` absent **AND non-terminal** status → `outcome = NULL`; the game is still stored with `status_raw`. `is_live = 1` only when the game may still progress (`started`/`created`); `aborted`/`noStart`/`unknownfinish` are void (`is_live = 0`, `outcome = NULL`).
- **Ethics:** `cheat` (Lichess) and `closed:fair_play_violations` (Chess.com) are the **provider's own labels**, copied verbatim into `status_raw`/status fields as neutral facts. This tool never derives, infers, or asserts a cheating determination.
- `game_participants.result_raw` for Lichess: synthesize per-seat (`win`/`loss`/`draw`/`unfinished`) from `winner`+`is_live`, but keep the authoritative `status` in `games.status_raw`.

### Variants / Rules Taxonomy

Canonical `variants.canonical_name` set (lowercase): `standard, chess960, crazyhouse, antichess, atomic, horde, kingofthehill, racingkings, threecheck, bughouse, fromposition`.

| canonical_name | Chess.com `rules` | Lichess `variant` |
|---|---|---|
| `standard` | `chess` | `standard` |
| `chess960` | `chess960` | `chess960` |
| `crazyhouse` | `crazyhouse` | `crazyhouse` |
| `antichess` | *(n/a)* | `antichess` |
| `atomic` | *(n/a)* | `atomic` |
| `horde` | *(n/a)* | `horde` |
| `kingofthehill` | `kingofthehill` | `kingOfTheHill` |
| `racingkings` | *(n/a)* | `racingKings` |
| `threecheck` | `threecheck` | `threeCheck` |
| `bughouse` | `bughouse` | *(n/a; game stored raw-only)* |
| `fromposition` | derive from `initial_setup`/non-standard FEN | `fromPosition` |

Match case-insensitively; store `provider_native_name` with the provider's original casing. Unknown native string → insert with `canonical_name = lower(native)`, `mapped = 0`; never drop the game. Chess.com "from position" is inferred from `initial_setup`/`SetUp`+`FEN`.

### Time Controls / Clocks

`time_controls` normalizes both providers into `(kind, initial_seconds, increment_seconds, days)` plus the shared `time_class`, retaining the native `raw_label`.

**Chess.com `time_control` string:**
| Raw form | Parse | kind | time_class source |
|---|---|---|---|
| `"600"` | `initial_seconds=600, increment=0` | `clock` | provider `time_class` |
| `"180+2"` | `initial_seconds=180, increment_seconds=2` | `clock` | provider `time_class` |
| `"1/259200"` | daily: `days = 259200/86400 = 3` | `correspondence` | `daily → correspondence` |

**Lichess `clock{initial,increment,totalTime}` + `speed`:**
- `initial_seconds = clock.initial`, `increment_seconds = clock.increment` (already seconds).
- No `clock` object → `kind='correspondence'`, `days` from daysPerTurn if available else NULL.
- `speed` maps to shared class: `ultraBullet,bullet → bullet`; `blitz → blitz`; `rapid → rapid`; `classical → classical`; `correspondence → correspondence`.

**Shared time-class taxonomy:** `{bullet, blitz, rapid, classical, correspondence}` — the exact set enforced by `time_controls.time_class CHECK`. Chess.com `daily` folds to `correspondence`; Lichess `ultraBullet` folds to `bullet`. Prefer the provider's declared class; only estimate from `initial + 40*increment` when the provider omits it. `raw_label` preserves the native descriptor (`"180+2"`, `"1/259200"`, `"blitz"`). The deterministic UNIQUE tuple dedups identical controls.

### Missing Optional Fields

Every optional provider field maps to a **nullable** column: absent → NULL. Never fabricate, never use a sentinel that could be mistaken for data (no `0` ratings, no epoch-0 timestamps, no `"unknown"` strings). Because `raw_payloads` holds the complete body, a field missing today can be normalized later if a column is added — no refetch.

### PGN Storage vs Parsed Headers

- **Always** store raw PGN verbatim in `raw_payloads`.
- **Eagerly** parse only a small, cheap header set into `games` columns for querying:

| Column | Chess.com source | Lichess source |
|---|---|---|
| `eco` | PGN `ECO` / `eco` URL tail | `opening.eco` / PGN `ECO` |
| `opening_name` | PGN `Opening` / `ECOUrl` tail | `opening.name` |
| `opening_ply` | *(n/a → NULL)* | `opening.ply` |

- **`utc_date`** (PGN `UTCDate`+`UTCTime`) and **`termination`** (PGN `Termination`) are **not** separate columns: the date is cross-checked against `ended_at`/`created_at` (which are the queryable timestamps), and termination is folded into `status_raw`. Both remain fully recoverable from stored raw.
- **Deep** move-by-move parsing (SAN/FEN sequences, clock stamps) is **lazy and optional**, gated behind the `analysis` extra (`python-chess`); ingest succeeds without it.
- Header parsing tolerates missing/garbled tags → NULL, never raises fatally.

### Game `canonical_url` + `content_hash`

**canonical_url:** Chess.com game `url`; Lichess `https://lichess.org/{gameId}`.

**content_hash** provides provider-agnostic dedup and change detection, computed over a **deterministic, versioned canonical subset**, so the same game normalized twice (or fetched via two paths) collapses to one row, while a genuine change (mutable current-month game) is detectable.

```
canonical = {
  "hash_version": 1,                          # scheme version — bump forks a fresh, isolated dedup space
  "provider": provider,                       # "chess.com" | "lichess"  → dedup is provider-scoped
  "provider_game_id": uuid | gameId,          # omitted only if truly absent
  "participants": sorted([                    # order-independent
      (color, provider_user_id_or_username_normalized, rating)
  ], key=lambda t: t[0]),                     # by color
  "outcome": outcome,                         # white_win|black_win|draw|None
  "moves": normalized_movetext                # SEE BELOW
}
content_hash = "sha256:" + sha256(
    json.dumps(canonical, sort_keys=True, separators=(",",":"), ensure_ascii=False).encode("utf-8")
).hexdigest()
```

**Field-subset policy (pinned):** the canonical subset is exactly the keys above. **Optional/volatile fields are deliberately EXCLUDED** — openings, clocks, evals, tags, annotations, and any field that is present-or-absent depending on request flags — because they are not identity-defining and their presence must not fork the dedup space. Absent optional fields are simply not in `canonical`. `hash_version` is embedded so a future change to the subset or movetext rule bumps the version and triggers a full, explicit re-hash migration rather than silently splitting identities.

**`normalized_movetext` rule** (deterministic, in priority):
1. If SAN movetext is cheaply extractable from PGN → strip clock/eval annotations, comments, and the result tag; collapse whitespace; use the bare SAN sequence.
2. Else use the provider move string (Lichess `moves`, or Chess.com `tcn` verbatim as a stable proxy).
3. Serialize the whole `canonical` with `sort_keys=True, separators=(",",":")` before hashing so the hash is reproducible across runs and machines.

**Enforcement / behavior:**
- `UNIQUE(content_hash)` on `games`. On ingest: compute hash → if identical hash exists, **skip** and just link a new `source_records` row to the existing game (records another witness).
- **Live/mutable games:** a later fetch producing a *different* hash for the same `(provider, provider_game_id)`/`canonical_url` signals change → **update the existing row in place** (new hash/outcome, `is_live` maybe flipped to 0), append `source_records`/`raw_payloads` as an audit trail. Past-month + all finished games are immutable, so a matching id should always yield a matching hash (a mismatch is a bug/corruption worth logging to `errors`).

### Bounding recent-game refetch (Lichess, no month immutability)

Chess.com gives immutable past months + ETag/304, so refetch is cheap and well-defined. Lichess has no month concept. The resolved v1 strategy:
- Maintain a **per-user `lastMoveAt` high-water mark** = `MAX(games.ended_at)` for `(provider='lichess', user)`. Incremental runs stream from `since = high_water_mark − look_back`.
- Apply a **rolling look-back window** (configurable, default 14 days) re-streamed on each incremental run — enough to catch games that appeared retroactively or that finished after a correspondence delay. Games already stored dedup on `content_hash`; changed/late games update in place.
- Coverage for Lichess is therefore expressed as **"date ranges requested"** (the union of streamed `[since,until)` windows), not "guaranteed-complete months" — reports say so explicitly (§14).

### Raw + Normalized Rationale

- **Re-normalization without refetch:** every rule is a deterministic function of the stored raw body. A new result code, a mapping fix, or a new column is a re-run over `raw_payloads`, never a re-crawl.
- **Audit:** `source_records` ties every normalized row to the exact raw payload + provenance (endpoint, fetched-at, ETag/Last-Modified) that produced it.
- **Forward-compatibility:** fields the current schema ignores are preserved verbatim, so future features can be normalized retroactively from history already on disk.

---

## 11. Data Acquisition Jobs & Discovery Model

The acquisition layer is a **general-purpose durable job engine**. Crawling is not baked into it; opponent discovery (`crawl_opponents`) is merely one strategy that runs on top of the same queue every other acquisition uses. The engine knows only how to pop a job, run it through a `ProviderClient`, persist raw-first, normalize, and enqueue whatever child jobs the handler returns. Any discovery policy is expressed purely as "which jobs get enqueued." The `discovery_jobs` table DDL is canonical in §9; this section owns its runtime behavior.

### State semantics (canonical enum)

`discovery_jobs.state ∈ {pending, in_progress, done, error, skipped, blocked}` — the single vocabulary that CLI (§13) and reports (§14) render:

| state | meaning | CLI human label (§13 legend) |
|---|---|---|
| `pending` | enqueued, not yet claimed | queued |
| `in_progress` | claimed by the runner (`started_at` set) | running |
| `done` | succeeded (idempotent no-op on re-run) | done |
| `error` | retries exhausted / dead-lettered (`reason` holds last failure) | failed |
| `skipped` | deliberately not run (cap exceeded, filtered, closed account with no data, bughouse) | skipped |
| `blocked` | cannot run yet but not failed (429 cooldown, backoff, unmet dependency); flipped back to `pending` when unblocked | waiting |

**`dedup_key`** is a deterministic hash over `(provider, kind, normalized_target, canonical_params)`. Enqueue is `INSERT … ON CONFLICT(dedup_key) WHERE state IN ('pending','in_progress') DO NOTHING` (via `ux_jobs_dedup_live`), so repeated enqueues collapse to one live job while a completed/errored row does not block a legitimate re-enqueue.

**`params_json`** carries everything the handler and resume need: `since`/`until` (epoch seconds), filters, caps snapshot, `depth` budget, and a `cursor` for resumable streaming/pagination (last archive URL processed, last Lichess `until` watermark, last committed stream offset).

### Job Kinds

Every `kind` maps to one handler that does exactly: fetch → store raw → normalize → return child jobs. Children are enqueued transactionally with the parent's `done` commit.

| kind | fetches (ProviderClient call) | normalizes into | typically enqueues |
|---|---|---|---|
| `fetch_user_profile` | Chess.com `GET /pub/player/{u}`; Lichess `GET /api/user/{u}` | `provider_users`, `user_snapshots` | optionally `fetch_user_stats`, `fetch_user_games` |
| `fetch_user_stats` | Chess.com `GET /pub/player/{u}/stats`; Lichess perfs from profile body | `user_snapshots` (rating facts) | (leaf) |
| `fetch_user_games` | **fan-out only** — resolves the archive units for a user | (nothing directly) | Chess.com: one `fetch_monthly_archive` per month from `/games/archives`; Lichess: one `fetch_user_games` stream-chunk job **per calendar-month window** over `[since,until]` |
| `fetch_monthly_archive` | Chess.com `GET /pub/player/{u}/{YYYY}/{MM}` (ETag/304 aware) | `games`, `game_participants`, `ratings_at_game`, `time_controls`, `variants` | `crawl_opponents` for new opponents (if strategy active) |
| `fetch_game_by_id` | Lichess `GET /api/game/{id}`; **Chess.com resolves via owning monthly archive** (username + `YYYY/MM` from url, extract by uuid) | one `games` row + participants | (leaf) |
| `fetch_games_by_ids` | Lichess `POST /api/games/export/_ids` (≤~300); **Chess.com groups ids by owning archive, fetches each month once** | many `games` rows | (leaf, or `crawl_opponents`) |
| `import_export_dump` | reads a local NDJSON/PGN dump path (no network) | `games` + participants, raw-first from file bytes | `crawl_opponents` per discovered user |
| `crawl_opponents` | **strategy job**, no direct fetch | (nothing directly) | `fetch_user_games` for the user, and depth+1 `crawl_opponents` for new opponents |
| `resume` | control job — reconciles a run's stale state | (nothing) | flips stale `in_progress` → `pending` |

**`fetch_user_games` fan-out** honors the providers' archive-unit difference, using **per-calendar-month chunks on both sides** for symmetric, resumable units:

```
fetch_user_games(provider, user, since, until, filters)
├─ chess.com:  GET /games/archives  →  ["…/2024/11", "…/2024/12", …]
│               for each month URL within [since,until]:
│                   enqueue fetch_monthly_archive(user, YYYY, MM)   # immutable past months cache via ETag
└─ lichess:    split [since,until] into per-CALENDAR-MONTH windows
                for each month-window:
                    enqueue fetch_user_games(cursor=window)         # one NDJSON stream chunk / month
                    # handler streams /api/games/user/{u}?since&until&perfType&rated…,
                    # writing the `until` watermark after each fully-persisted game
```

Lichess has no monthly-archive concept, so its "archive unit" is a bounded **per-month date-range stream chunk**. Each chunk streams NDJSON serially and advances a `cursor` (the `until` watermark of the last committed game), so an interrupted chunk resumes without refetching committed games and without dropping the tail. Per-month chunking gives resumable, bounded units that mirror Chess.com months.

### The Runner Loop

A single serial worker (politeness + rate-limit compliance forbid concurrency per provider). It commits **once per job** so a crash never loses more than the in-flight job.

```
RUNNER:
  loop:
    job := claim_next_runnable()          # atomic: pop + mark in_progress
    if job is None:
        if any blocked jobs due soon: sleep_until_unblock(); continue
        else: break                        # queue drained

    try:
        client := registry.get_client(job.provider)
        polite_delay(client)               # serial + per-provider min interval

        result := HANDLERS[job.kind](client, job)   # -> RawRecord(s) + child job specs

        with db.transaction():             # atomic per-job commit
            raw_ids := store_raw(result.raw)            # RAW-FIRST: body + provenance
            record_source(job, raw_ids)                 # source_records
            normalize(job.kind, result)                 # idempotent upserts
            for child in result.children:
                enqueue(child)                          # ON CONFLICT dedup_key DO NOTHING
            mark(job, 'done', done_at=now())
        log_fetch(job, ok=True)            # fetch_logs

    except RateLimited as e:               # HTTP 429
        cooldown := max(e.retry_after or 0, provider_default_cooldown(job.provider))
        # Lichess HARD RULE: cooldown >= 60s on 429
        mark(job, 'blocked', reason=f"429; wait {cooldown}s", requeue_after=now()+cooldown)
        sleep(cooldown)

    except NotFound:  mark(job, 'skipped', reason='404 not found')
    except Gone:      mark(job, 'skipped', reason='410 gone')

    except TransientError as e:            # timeouts, 5xx, connection reset
        job.attempts += 1
        if job.attempts < MAX_ATTEMPTS:
            backoff := base * 2^job.attempts + jitter
            mark(job, 'blocked', reason=str(e), requeue_after=now()+backoff)
        else:
            mark(job, 'error', reason=f"dead after {job.attempts}: {e}")   # dead-letter
            record_error(job, e)           # errors table
```

`claim_next_runnable()` runs a single atomic statement so claiming is safe even if a future version runs multiple workers:

```sql
UPDATE discovery_jobs SET state='in_progress', started_at=:now
WHERE id = (SELECT id FROM discovery_jobs
            WHERE state='pending' AND (crawl_run_id=:run OR :run IS NULL)
            ORDER BY priority ASC, enqueued_at ASC LIMIT 1)
RETURNING *;
```

**Raw-first invariant:** `store_raw` writes the verbatim provider body into `raw_payloads` (compressed when large) with provenance in `source_records`/`fetch_logs` **before** any normalized row is written, inside the same transaction. If normalization later changes, every normalized table is rebuildable from `raw_payloads` with zero refetching.

### Discovery Strategies & Inputs

A "strategy" is a recipe for seeding the queue; the engine is strategy-agnostic. Entry inputs (seeds):
- **Explicit user(s):** `fetch user PROVIDER USERNAME` → `fetch_user_profile` (+ `fetch_user_stats`, `fetch_user_games`).
- **Explicit game(s):** `fetch_game_by_id` / `fetch_games_by_ids` (Chess.com archive-mediated).
- **Date-bounded games:** `fetch games PROVIDER USERNAME --since --until` → `fetch_user_games`.
- **Local dump:** `import_export_dump` over an NDJSON/PGN path (offline seed).
- **Opponent crawl:** `crawl opponents PROVIDER USERNAME --depth N` → a `crawl_runs` row + a root `crawl_opponents` job at `depth=0`.

**Caps** (carried in `params_json`, enforced at enqueue and claim time, snapshotted onto `crawl_runs`):

| cap | meaning | enforced by |
|---|---|---|
| `max_jobs` | total jobs a run may create | enqueue guard: `skipped` once exceeded |
| `max_games` | total normalized games this run may ingest | handler halts ingest / stops enqueuing archive jobs |
| `max_users` | distinct `provider_users` this run may touch | `crawl_opponents` stops expanding |
| `max_depth` | crawl radius | `crawl_opponents` will not enqueue `depth > max_depth` |

**Per-provider filters** (also in `params_json`): `time_class`/`perfType`, `rules`/`variant`, `rated`. Chess.com filters map to `time_class`+`rules` (applied client-side during normalization; the archive endpoint is not filterable); Lichess maps to `perfType`+`variant`+`rated` query params so filtering happens server-side on the stream.

### Opponent Crawl — One Strategy Among Many

`crawl_opponents` performs **no fetch itself**; it schedules `fetch_user_games`, then reacts to the participants those fetches normalize, recording the graph in `discovery_edges` and expanding outward while caps allow.

```
HANDLER crawl_opponents(client, job):
    run   := job.crawl_run_id
    user  := job.target
    depth := job.depth

    # 1. Ensure we have this user's games (raw-first ingest in child jobs)
    enqueue fetch_user_games(provider=job.provider, target=user,
                             since=job.params.since, until=job.params.until,
                             filters=job.params.filters,
                             crawl_run_id=run, parent=job.id, depth=depth)

    # 2. From games already ingested for `user`, find opponents (game_participants)
    for opp in opponents_of(user, run, filters=job.params.filters):
        if not caps_allow(run, kind='user'):          # max_users / max_jobs
            mark_skipped_and_continue()

        record_edge(run, from=user, to=opp.user,
                    via_game=opp.game_id, depth=depth+1)   # discovery_edges (idempotent, min-depth)

        seen_depth := min_known_depth(run, opp.user)       # NULL if never seen
        if seen_depth is None or (depth+1) < seen_depth:
            set_min_depth(run, opp.user, depth+1)
            if (depth+1) <= run.caps.max_depth and caps_allow(run, kind='job'):
                enqueue crawl_opponents(provider=job.provider, target=opp.user,
                                        crawl_run_id=run, parent=job.id,
                                        depth=depth+1, params=job.params)
        # else: already reached at <= depth -> record edge only, do NOT re-expand

    return no_raw, children_enqueued_above
```

Key discipline:
- **Provider-scoped dedup:** opponents are `provider_users` keyed by `(provider, provider_user_id | username_normalized)`. A Chess.com opponent and a same-named Lichess user are never merged. A `chess.com` crawl only ever enqueues `chess.com` jobs.
- **Min-depth tracking:** the first (shortest) path determines a user's crawl depth; a later, longer path records the edge but never re-expands — preventing loops.
- **`discovery_edges`** records `(crawl_run_id, from_user_id, to_user_id, via_game_id, depth)` as neutral graph facts, exportable via `export graph`.

`opponents_of` reads only already-normalized `game_participants`, so the strategy is a pure function of stored data and re-runs cleanly. Because enqueue is dedup-keyed, two users pointing at the same new opponent produce **one** live `crawl_opponents` job; both edges are still recorded.

### Resume & Idempotency

1. **Stale reset** (startup / `resume` job / `jobs resume`): any job left `in_progress` for a run is reset to `pending`:
   `UPDATE discovery_jobs SET state='pending', started_at=NULL WHERE state='in_progress' AND crawl_run_id = :run;`
   Because each job commits atomically, a reset job re-runs from its `params.cursor` watermark.
2. **Idempotent handlers:**
   - Raw dedup: `raw_payloads.body_hash`; an identical body is not re-stored.
   - Game dedup: `games` `UNIQUE(content_hash)` (+ `UNIQUE(provider, provider_game_id)` / `UNIQUE(canonical_url)`).
   - User dedup: `provider_users` uniqueness on `(provider, provider_user_id)` / `(provider, username_normalized)`.
   - Chess.com sends `If-None-Match`/`If-Modified-Since`; a `304` short-circuits to a no-op `done`. Lichess relies on immutability + `content_hash`.
3. **Cursor-based resume** for streams/fan-out: `fetch_user_games` chunks and `fetch_monthly_archive` iteration persist a `cursor` after each committed unit, so an interrupted stream resumes at the exact watermark.

Kill the process at any moment and re-launch; `resume` reclaims stale jobs, dedup guarantees no double-writes, cursors guarantee no lost or repeated work — identically whether the queue was seeded by `fetch user`, `fetch games`, `import_export_dump`, or `crawl opponents`.

---

## 12. HTTP, Rate-Limiting & Caching (per provider)

### Shared client contract

Every provider exposes ONE serial HTTP client instance, owned by the job runner. There is **no parallelism, ever** — not across providers, not within a provider, not across jobs. Requests are issued one at a time; the inter-request delay is enforced *before* each network call so a burst of queued jobs cannot collapse the spacing. Concurrency is a non-goal and an anti-goal: it risks 429s, violates the providers' "serial + polite" guidance, and makes `fetch_logs` ordering ambiguous.

All shared behavior lives in a `BaseHttpClient` (in `providers/base.py`), parameterized by a per-provider `FetchPolicy`:

- **Configurable inter-request delay** (`policy.min_delay_s`), from `config.py`, applied via `sleep(delay)` before each request.
- **Exponential backoff with jitter** on retryable failures (`429`, `5xx`, connect/read timeouts): `base_backoff * 2**attempt`, capped at `max_backoff`, plus full jitter. A `Retry-After` header, when present, **overrides** (use the larger).
- **Retry limit** (`policy.max_retries`). On exhaustion the client raises a terminal error; the runner writes one `errors` row and marks the job `error`. Every individual attempt is logged to `fetch_logs`.
- **Timeouts:** separate `connect_timeout` and `read_timeout` (httpx `Timeout`). Lichess `read_timeout` is generous because game exports stream for a long time; a slow stream is not a stall as long as bytes keep arriving.
- **gzip:** always send `Accept-Encoding: gzip`, let httpx auto-decompress; store the *decompressed* body in `raw_payloads` (compressing ourselves per §8). Never rely on transport gzip for at-rest compression.
- **Structured logging into `fetch_logs`** (§8 DDL): one row per attempt with `{provider, job_id, crawl_run_id, url, endpoint_type, method, status_code, from_cache, etag, last_modified, retry_after, bytes, duration_ms, attempt, attempted_at, raw_payload_id, error_ref}`. `raw_payloads` is written only on a body-bearing `200`/`206`.

#### User-Agent (required, both providers)

```
chess-crawl/{version} (+contact: {contact}; {provider}-user: {username})
```
- `{version}` — the `chess_crawl` package version (never blank).
- `{contact}` — a configured contact (email/URL); ships as an obvious placeholder (`set-me@example.invalid`) so an unconfigured deployment is visibly identifiable, but always present.
- `{provider}` — `chess.com` or `lichess`.
- `{username}` — the configured provider username if set; when unset, the trailing clause is omitted entirely.

Example: `chess-crawl/0.4.1 (+contact: xorman@example.org; lichess-user: myhandle)`.

### Provider policy table

| Policy dimension | chess.com | lichess |
|---|---|---|
| Default inter-req delay | ~1.0 s (serial, polite), configurable | ~1.5 s (serial, polite), configurable |
| 429 reaction | Backoff w/ jitter; honor `Retry-After` | **HARD RULE: STOP and wait a full 60 s before resuming**, then continue |
| 5xx / timeout | Exponential backoff + jitter, up to `max_retries` | Exponential backoff + jitter, up to `max_retries` |
| Conditional caching | Strong: `ETag`/`If-None-Match` + `Last-Modified`/`If-Modified-Since` → 304 | Weak/absent: rely on immutability + `content_hash` dedup |
| Permanent cache | Yes — finished (past-month) archives immutable → skip refetch | No archive-level immutability; individual *finished games* are immutable |
| Streaming | No (JSON read whole) | **Yes — NDJSON streamed line-by-line** |
| Auth | None (public, unauthenticated) | **Optional** personal bearer token → higher limits (never required) |
| Timestamp unit | Epoch **seconds** (store as-is) | **Milliseconds** → ÷1000 during normalization |

### Chess.com specifics

- **Conditional requests.** For any endpoint that previously returned an `ETag`/`Last-Modified` (stored in `raw_payloads.response_headers`), attach `If-None-Match`/`If-Modified-Since`. A `304` means "reuse the stored raw body": log `from_cache=1`, do NOT rewrite `raw_payloads`, hand the existing body to normalization if needed.
- **Immutable past-month archives.** `.../games/YYYY/MM` for any month strictly before the current UTC month is permanently immutable. If we already hold a stored `200`, **skip the request entirely** (log `from_cache=1`, status NULL). The primary request-saver for backfills.
- **Current month is mutable.** Always refetched, but *with* conditional headers, so an unchanged current month cheaply returns `304`. Games that became terminal since last fetch update in place (`is_live` flips to 0).
- **Retry-After** honored on `429`/`503` as the wait floor.
- **404 vs 410.** `404` = never existed → non-retryable "absent"; `410` = gone (removed/closed) → distinct non-retryable, logged separately.

### Lichess specifics

- **Streaming NDJSON.** Game endpoints are consumed via httpx streaming (`client.stream(...)`), iterating response lines. The **full raw body** (concatenated NDJSON, or PGN) is persisted to `raw_payloads` as the source-of-truth blob (compressed if large), written **through as lines arrive** so a mid-stream failure still preserves a partial raw body and its resumable position. Individual game lines are split out during normalization, each linked to the enclosing payload via `source_records`. Set `Accept: application/x-ndjson` (or `application/x-chess-pgn`).
- **429 → hard 60 s stop.** On `429`, do **not** apply generic backoff. STOP, wait a full **60 seconds**, resume. Encoded as a policy branch, not a tuning knob. It counts against `max_retries` for runaway protection; every 429 attempt is logged.
- **Optional bearer token (supported in v1).** If config/env supplies a personal OAuth token, send `Authorization: Bearer {token}` to raise the operator's own limits (personal use). **Never required**; absence just means lower limits. The token is **never logged** — excluded from the `fetch_logs` header subset, never written to `raw_payloads`/`response_headers`/`errors`, masked in `-vv` debug output.
- **Weak conditional caching.** Do not depend on `ETag`/`304`. Dedup relies on **game immutability + `content_hash`**: re-fetched lines that hash to an existing `games.content_hash` are recognized as duplicates and skipped at normalization. Overlapping date ranges (from the look-back window, §10) are expected and de-duped by hash.
- **Millisecond timestamps.** `createdAt`, `lastMoveAt`, `seenAt` are ms; convert to seconds during normalization. Raw ms preserved verbatim.

### Token secret handling (resolved)

The optional Lichess token is read, in precedence order, from `--lichess-token`, then env `CHESS_CRAWL_LICHESS_TOKEN` (preferred — stays out of shell history), then a config-file field. It lives only in the in-memory `ProviderSettings.oauth_token`. It is **never** persisted to `raw_payloads`, `response_headers`, `fetch_logs`, `errors`, `source_records`, or any export, and is masked in verbose logs. Personal-use only; a raised personal limit is not a license to crawl harder or bypass the serial/polite posture.

### Request lifecycle (branches by provider policy)

```text
function perform(provider, request_spec, job):
    policy = registry.get_client(provider).policy()
    ua = build_user_agent(version, config.contact, provider, config.username_for(provider))

    # ---- CACHE / IMMUTABILITY GATE (pre-network) ----
    if provider == "chess.com" and request_spec.is_monthly_archive:
        if request_spec.month < current_utc_month and have_stored_200(request_spec.url):
            log_fetch(provider, job, request_spec, status=None, from_cache=1)
            return stored_raw_body(request_spec.url)          # skip network entirely

    cond = load_conditional_validators(request_spec.url)      # {etag, last_modified} or {}
    headers = { "User-Agent": ua, "Accept-Encoding": "gzip" }
    headers += accept_header_for(request_spec)                # json | ndjson | pgn
    if provider == "lichess" and config.lichess_token:
        headers["Authorization"] = "Bearer " + config.lichess_token   # NEVER logged

    if policy.supports_conditional and cond:                  # chess.com only, effectively
        if cond.etag:          headers["If-None-Match"]     = cond.etag
        if cond.last_modified: headers["If-Modified-Since"] = cond.last_modified

    attempt = 0
    while attempt <= policy.max_retries:
        sleep(policy.min_delay_s)                             # serial spacing, ALWAYS
        started = now()
        try:
            resp = (http.stream if request_spec.streaming else http.request)(
                       request_spec.method, request_spec.url, headers,
                       connect_timeout=policy.connect_timeout, read_timeout=policy.read_timeout)
        except (ConnectTimeout, ReadTimeout, TransportError) as e:
            log_fetch(..., status=None, outcome="timeout|transport_error", attempt=attempt)
            attempt += 1; sleep(backoff_with_jitter(policy, attempt)); continue

        if resp.status == 200:
            if request_spec.streaming:                        # lichess NDJSON / PGN
                raw_id = open_raw_payload(provider, request_spec)       # write-through
                for line in resp.iter_lines():
                    append_raw(raw_id, line)
                finalize_raw_payload(raw_id)                            # compress if large
            else:
                body = resp.read()                                      # gzip auto-decompressed
                raw_id = write_raw_payload(provider, request_spec, body)
                store_conditional_validators(request_spec.url, resp.etag, resp.last_modified)
            log_fetch(..., status=200, bytes=..., duration=now()-started); return raw_id

        elif resp.status == 304:                    # chess.com conditional hit
            log_fetch(..., status=304, from_cache=1); return stored_raw_body(request_spec.url)

        elif resp.status == 429:
            log_fetch(..., status=429, retry_after=resp.retry_after)
            if provider == "lichess": sleep(60)               # HARD RULE
            else: sleep(max(resp.retry_after or 0, backoff_with_jitter(policy, attempt)))
            attempt += 1; continue

        elif resp.status in 500..599:
            log_fetch(..., status=resp.status)
            attempt += 1; sleep(max(resp.retry_after or 0, backoff_with_jitter(policy, attempt))); continue

        elif resp.status == 404:
            log_fetch(..., status=404); record_absent(errors, provider, job, request_spec, kind="not_found")
            return ABSENT                            # non-retryable

        elif resp.status == 410:
            log_fetch(..., status=410); record_absent(errors, provider, job, request_spec, kind="gone")
            return GONE                              # non-retryable, distinct from 404

        else:
            log_fetch(..., status=resp.status); attempt += 1; sleep(backoff_with_jitter(policy, attempt)); continue

    write_error(errors, provider, job, request_spec, final="retries_exhausted", attempts=attempt)
    raise TerminalFetchError
```

### Invariants

- **No parallelism, anywhere.** One serial client per provider; the pre-request `sleep(delay)` is unconditional. Scale-up is longer runs, never concurrency.
- **Raw-first.** A body-bearing `200` writes `raw_payloads` before (or write-through, for streams) any normalization. `304` and immutable-month skips reuse the existing raw body and never rewrite it.
- **Every attempt is logged** to `fetch_logs`; only *terminal* failures escalate to `errors`.
- **Provider policy is data.** The branches are driven by `FetchPolicy` resolved from the registry, keeping the lifecycle single-sourced while honoring each provider's rules.

---

## 13. CLI Design

`chess-crawl` is a single Typer application (`chess_crawl/cli.py`). Every command resolves to one of two shapes:

- **Acquisition commands** (`fetch user`, `fetch games`, `crawl opponents`, `jobs resume`) never talk to a provider directly. They **enqueue `discovery_jobs` rows and then drive the runner**. Because the job table is the source of truth, they are **crash-safe, resumable, and idempotent**: re-running re-enqueues the same logical jobs, and the runner deduplicates against existing live jobs (`dedup_key`).
- **Read/inspection commands** (`init`, `provider list`, `jobs status`, `query …`, `export …`) never enqueue work; they only read local SQLite (or, for `init`, create it).

A user can `Ctrl-C` any acquisition command and later `chess-crawl jobs resume` (or re-run the original command) to continue exactly where the runner left off.

### Job-state legend (canonical → CLI label)

The CLI displays the canonical `discovery_jobs.state` enum (§11) with friendly labels:

```
pending → queued    in_progress → running    blocked → waiting
done    → done       error       → failed      skipped → skipped
```

### Global Options

Attached to the root callback, inherited by every subcommand. Precedence: **CLI flag > env var > config file > default**.

| Option | Env var | Default | Meaning |
|---|---|---|---|
| `--db PATH` | `CHESS_CRAWL_DB` | `./chess-crawl.db` | SQLite archive file. Created lazily only by `init`; other commands error if missing. |
| `--config PATH` | `CHESS_CRAWL_CONFIG` | `./chess-crawl.toml` if present | TOML config supplying defaults. |
| `--user-agent TEXT` | `CHESS_CRAWL_USER_AGENT` | `chess-crawl/<version> (+<contact>)` | Sent on every request. Mandatory for Lichess, polite for Chess.com. |
| `--contact TEXT` | `CHESS_CRAWL_CONTACT` | none (warns if unset) | Contact email/URL folded into the User-Agent. |
| `--delay FLOAT` | `CHESS_CRAWL_DELAY` | `1.0` | Minimum seconds between successive requests to the same provider host. |
| `--lichess-token TEXT` | `CHESS_CRAWL_LICHESS_TOKEN` | none | **Optional** Lichess personal OAuth token; only raises the operator's own limits. Never required; never logged. Env preferred so it stays out of shell history. |
| `--verbose`, `-v` | `CHESS_CRAWL_VERBOSE` | off (repeatable) | `-v` = info; `-vv` = debug (URLs, ETag/304, Retry-After; token masked). Logs → stderr. |
| `--json` | — | off | Machine-readable JSON on stdout instead of human tables. |
| `--version` | — | — | Print version, exit 0. |

`PROVIDER` is a **positional** argument on data commands with exactly two accepted values: `chess.com` | `lichess`. Any other value is a usage error (exit 2) listing valid keys.

### Exit Codes (shared)

| Code | Name | Meaning |
|---|---|---|
| `0` | OK | Success. Acquisition: all driven jobs reached `done`. |
| `1` | ERROR | Unexpected/internal error. |
| `2` | USAGE | Bad arguments (unknown provider, malformed date, exclusive flags, missing arg). |
| `3` | NOT_FOUND | Provider 404/410 for the requested user/game, or a `query` target absent locally. |
| `4` | RATE_LIMITED | Aborted after exhausting 429 backoff (Lichess: waited 60s and still 429). Jobs left `pending`/`blocked` for later `jobs resume`. |
| `5` | PARTIAL | Run completed but ≥1 job ended in `error`. Archive still valid; see `errors` and `jobs status`. |
| `130` | INTERRUPTED | `SIGINT`/`Ctrl-C`. Current job checkpointed; state resumable. |

`--json` mode also includes `"ok": bool` / `"exit_code": N`.

### `chess-crawl init`

```
chess-crawl init [--db PATH] [--force]
```
Creates the SQLite file if absent, applies `storage/schema.sql`, records migrations in `schema_migrations`, seeds `providers`. Prints resolved DB path and schema version. `--force` re-runs migrations to latest rather than erroring on an existing DB. Fully idempotent.

```
$ chess-crawl init --db ./archive.db
Created ./archive.db
Applied migrations: 0001_init … 0009_discovery_edges_depth  (schema_version=9)
Seeded providers: chess.com, lichess
```
**Exit:** `0` created/current; `1` cannot write / migration failure; `2` bad `--db`.

### `chess-crawl provider list`

```
chess-crawl provider list [--json]
```
Prints a static capability matrix (no network).

```
$ chess-crawl provider list
PROVIDER    ID MODEL          ARCHIVE UNIT           TS UNIT  CACHING        429 POLICY   AUTH
chess.com   numeric player_id monthly (immutable)    seconds  ETag/304       Retry-After  none
lichess     id == username    date-range NDJSON      millis   content_hash   wait 60s     optional token
```
```
$ chess-crawl provider list --json
[
  {"key":"chess.com","id_model":"numeric_player_id","username_mutable":true,
   "archive_unit":"month","immutable_past":true,"ts_unit":"seconds",
   "caching":["etag","last_modified","304"],"rate_limit":{"serial":true,"on_429":"retry_after"},
   "auth":"none","single_game_by_id":false},
  {"key":"lichess","id_model":"username_as_id","username_mutable":false,
   "archive_unit":"date_range_stream","immutable_past":false,"ts_unit":"milliseconds",
   "caching":["content_hash","game_immutability"],"rate_limit":{"serial":true,"on_429":"wait_60s"},
   "auth":"optional_oauth_token","single_game_by_id":true}
]
```

### `chess-crawl fetch user`

```
chess-crawl fetch user PROVIDER USERNAME [--with-stats]
```
Enqueues `fetch_user_profile` (+ `fetch_user_stats` when `--with-stats`; Lichess perfs are already embedded, so stats just records the normalized per-perf ratings), then drives the runner.

```
$ chess-crawl fetch user chess.com hikaru --with-stats
[chess.com] GET /pub/player/hikaru … 200 (raw #1041, provider_user_id=15448422)
[chess.com] GET /pub/player/hikaru/stats … 200 (raw #1042)
Upserted provider_user id=7 (hikaru) | user_snapshots +1 | ratings: rapid 2790, blitz 3298, bullet 3312
Jobs: 2 done, 0 failed. OK.

$ chess-crawl fetch user lichess DrNykterstein --json
{"ok":true,"provider":"lichess","provider_user_id":"drnykterstein",
 "display_username":"DrNykterstein","user_pk":8,"snapshot_id":552,
 "perfs":{"bullet":3221,"blitz":3067,"rapid":2903},"jobs":{"done":1,"failed":0},"exit_code":0}
```
The raw profile JSON is stored verbatim; `provider_users` upserted; a new `user_snapshots` row captures point-in-time fields (followers, status/`tosViolation`, ratings). Provider account-status strings are stored as **neutral facts**. Chess.com profile fetches send conditional headers; a 304 is recorded in `fetch_logs` without duplicating a payload.
**Exit:** `0`; `3` 404/410; `4` rate-limited; `2` bad provider/username; `5` `--with-stats` where profile succeeded but stats failed.

### `chess-crawl fetch games`

```
chess-crawl fetch games PROVIDER USERNAME
    [--since TEXT] [--until TEXT]
    [--time-class TEXT ... | --perf TEXT ...]
    [--rules TEXT ... | --variant TEXT ...]
    [--rated / --unrated]
    [--max-games INT]
```
Bound semantics are provider-specific (the central difference this command hides):
- **chess.com** — `--since`/`--until` accept `YYYY-MM` month bounds (a `YYYY-MM-DD` is floored/ceiled to its month). The command lists `/games/archives`, intersects the window, and enqueues one `fetch_monthly_archive` per month.
- **lichess** — accept `YYYY-MM-DD`/ISO date bounds, and enqueue **one `fetch_user_games` stream-chunk job per calendar-month window** over the range (each with its own resumable cursor).

Both first ensure the owning `fetch_user_profile` exists (auto-enqueued if unknown, so identity is resolved before games are attributed).

`--time-class` (chess.com: `daily|rapid|blitz|bullet`) / `--perf` (lichess CSV) are aliases enforced per provider; the wrong one for a provider is a usage error with a hint. `--rules`/`--variant` filter (chess.com client-side; lichess `perfType`/`variant` server-side). `--rated/--unrated` defaults to both. `--max-games` caps normalized games (lichess `max`; chess.com stops after N in reverse-chron order).

```
# Chess.com — month bounds → monthly-archive jobs
$ chess-crawl fetch games chess.com hikaru --since 2024-01 --until 2024-03 --time-class blitz --rated
[chess.com] archives: 39 total, 3 in window (2024/01,2024/02,2024/03)
[chess.com] GET …/games/2024/01 … 200 (raw #1101, 812 games; 640 blitz rated kept)
[chess.com] GET …/games/2024/02 … 304 Not Modified (cache hit; 0 refetched)
[chess.com] GET …/games/2024/03 … 200 (raw #1102, 774 games; 601 kept)
Normalized: 1241 games, 2482 participants, 1241 ratings_at_game. New: 1180, dup(content_hash): 61.
Jobs: 3 done. OK.

# Lichess — date bounds → one stream-chunk job per calendar month
$ chess-crawl fetch games lichess DrNykterstein --since 2024-01-01 --until 2024-04-01 \
      --perf bullet,blitz --rated --max-games 5000
[lichess] fan-out: 3 month-chunk jobs (2024-01, 2024-02, 2024-03)
[lichess] STREAM …/games/user/DrNykterstein?since=…&until=…&perfType=bullet,blitz&rated=true (2024-01)
[lichess]  … 200 x-ndjson  1000 … (raw #1103, gz)   2024-02 (raw #1104)   2024-03 (raw #1105)
Stored 3 raw NDJSON chunks. Normalized: 3120 games, ms→s converted. New: 3120, dup: 0. Jobs: 3 done. OK.
```
**Idempotency:** chess.com past months are immutable → repeat runs yield 304s; the current month is mutable but games dedupe on `content_hash`/`provider_game_id`. lichess re-streams a date range but finished games dedupe on `content_hash`; the look-back window (§10) catches late/retroactive games.
**Exit:** `0`; `2` wrong bound granularity / wrong filter alias / bad date; `3` user 404; `4` rate-limited (partial chunks/months left resumable); `5` some jobs failed.

### `chess-crawl crawl opponents`

```
chess-crawl crawl opponents PROVIDER USERNAME --depth N
    [--since TEXT] [--until TEXT]
    [--time-class TEXT ... | --perf TEXT ...]
    [--rules TEXT ... | --variant TEXT ...]
    [--rated / --unrated]
    [--max-users INT] [--max-games INT] [--max-jobs INT]
```
Breadth-first discovery, **one strategy, not the core**. A `crawl_runs` row records seed/provider/depth/bounds/caps. The seed is enqueued at depth 0; as games normalize, distinct opponents become `provider_users` and a `crawl_opponents` job is enqueued for each at `depth+1` only while `depth+1 ≤ N` and caps allow; each hop writes a `discovery_edges` row. **Crawling never crosses providers** — a `chess.com` crawl only enqueues `chess.com` jobs (the identity boundary enforced structurally).

```
$ chess-crawl crawl opponents chess.com hikaru --depth 1 \
      --since 2024-01 --until 2024-01 --time-class blitz --max-users 200 --max-jobs 500
crawl_run #12 (chess.com, seed=hikaru, depth=1) started
depth0: hikaru → 1 archive job … 640 blitz games, 187 distinct opponents
depth1: enqueued 187 users (capped at max-users=200) → 187 archive jobs
[chess.com] driving 188 jobs (serial, delay=1.0s) … ####################  done
discovery_edges: +187. provider_users: +187. games: +38k (dup 4.1k).
Jobs: 188 done, 0 failed. crawl_run #12 COMPLETE. OK.
```
Fully resumable — `Ctrl-C` then `chess-crawl jobs resume --run 12` continues the BFS. Re-issuing the command attaches to the same frontier; `discovery_edges` insert with the idempotent upsert on `(provider, from_user_id, to_user_id)`.
**Exit:** `0`; `2` bad depth/bounds/caps; `3` seed 404; `4` rate-limited (frontier preserved); `5` some failed; `130` interrupted.

### `chess-crawl jobs status`

```
chess-crawl jobs status [--run ID] [--json]
```
Read-only aggregation over `discovery_jobs` (by `kind` × `state`) and `crawl_runs` (by depth), plus recent `errors`.

```
$ chess-crawl jobs status --run 12
crawl_run #12  chess.com  seed=hikaru  depth=1  started 2026-07-02 14:02Z
STATE        COUNT   KIND                    (legend: waiting=blocked, running=in_progress)
done           181   fetch_monthly_archive
done             6   fetch_user_profile
waiting          1   fetch_monthly_archive   (429; requeue 14:31Z)
BY DEPTH:  depth0: 1/1 done   depth1: 186/188 done (1 waiting, 1 running)
Recent errors: none.  Overall: 98.9% complete.
```
```
$ chess-crawl jobs status --json
{"runs":[{"crawl_run_id":12,"provider":"chess.com","seed":"hikaru","depth":1,
  "jobs":{"pending":0,"in_progress":0,"blocked":1,"done":187,"error":0,"skipped":0}}],
 "global":{"pending":0,"in_progress":0,"blocked":1,"done":193,"error":0,"skipped":0},"exit_code":0}
```
**Exit:** `0` when readable (a reported failure is not itself a failure); `2` unknown `--run ID`; `1` unreadable DB.

### `chess-crawl jobs resume`

```
chess-crawl jobs resume [--run ID] [--max-jobs INT]
```
Re-drives the runner over not-yet-terminal jobs (`pending` + `blocked` due, plus re-queuing crash-orphaned `in_progress` rows). Deterministic order (depth, then enqueue order); respects backoff and the serial `--delay`.

```
$ chess-crawl jobs resume --run 12
Resuming crawl_run #12: 1 blocked→due, 0 pending, 1 orphaned→pending.
[chess.com] GET …/games/2024/01 (retry after 429) … 200 (raw #1290, 2 new games)
Jobs: 2 done, 0 failed. crawl_run #12 COMPLETE. OK.
```
**Exit:** `0` all runnable done (or none pending); `4` re-hit rate limit; `5` some failed; `2` unknown `--run ID`; `130` interrupted.

### `chess-crawl query user`

```
chess-crawl query user PROVIDER USERNAME [--json]
```
Read-only — never fetches. Looks up `provider_users` by `(provider, username_normalized)`, joins the latest `user_snapshots`, per-perf ratings, and archive coverage. Not-in-archive → exit `3` with a hint to `fetch user`.

```
$ chess-crawl query user chess.com hikaru
chess.com / hikaru   (provider_user_id=15448422, user_pk=7)
display: Hikaru   status: premium   joined: 2014-05-01   last_online: 2026-07-01
ratings (latest snapshot 2026-07-02): rapid 2790  blitz 3298  bullet 3312
local games: 3,241   coverage: 2024-01 … 2024-03 (blitz), 2025-11 … 2026-06 (all)
snapshots on file: 4
```
**Exit:** `0` found; `3` not in local archive; `2` bad provider.

### `chess-crawl query game`

```
chess-crawl query game PROVIDER GAME_ID [--json]
```
Read-only. Resolves via `UNIQUE(provider, provider_game_id)` / `canonical_url`; prints normalized fields plus provenance (`raw_payloads` row, `content_hash`). Outcome shown in the shared taxonomy with the raw code alongside; an undecided game shows `outcome: (unfinished)`.

```
$ chess-crawl query game lichess q7ZvsdUF
lichess / q7ZvsdUF   https://lichess.org/q7ZvsdUF   (game_pk=90412)
rated blitz (standard)   created 2024-01-14 20:11:04Z   ply 63
white: DrNykterstein 3061 (+6)   black: penguingim1 2998 (-6)
outcome: white_win   [raw: winner=white status=resign]
opening: B23 Sicilian Defense   provenance: raw #1103 (NDJSON line 402), content_hash=sha256:8f3a…c1
```
**Exit:** `0` found; `3` not in local archive; `2` bad provider/id.

### `chess-crawl export games`

```
chess-crawl export games
    [--provider PROVIDER] [--user USERNAME]
    [--since TEXT] [--until TEXT]
    [--format csv|jsonl|pgn] (default csv)
    [--out PATH] (default: stdout)
```
Read-only. `--user` requires `--provider` (identity is provider-scoped; usernames are not global). `--since/--until` filter normalized `ended_at` (epoch seconds), accepting `YYYY-MM-DD`. `pgn` reconstructs from stored raw; games lacking PGN are skipped with a stderr note.

```
$ chess-crawl export games --provider chess.com --user hikaru \
      --since 2024-01-01 --until 2024-04-01 --format pgn --out hikaru_q1.pgn
Exported 1,241 games → hikaru_q1.pgn (pgn). 0 skipped.
```
Deterministic for a fixed archive (stable sort by `ended_at`, then `provider_game_id`); `--out` overwrites atomically (temp + rename).
**Exit:** `0` (0 matches → empty output + stderr note); `2` `--user` without `--provider` / bad format/date; `1` write failure.

### `chess-crawl export graph`

```
chess-crawl export graph
    [--provider PROVIDER]
    [--format gexf|graphml|edgelist|json] (default gexf)
    [--out PATH] (default: stdout)
    [--min-games INT] (default 1)
```
Nodes = `provider_users` (scoped to `--provider` if given). **A graph is always single-provider** — nodes are never merged across providers. If `--provider` is omitted, output is a disjoint union with provider-prefixed node ids and no cross-provider edges. Edges = opponent relationships weighted by game count (from `game_participants`), optionally overlaid with `discovery_edges` metadata (run, depth). `--min-games` drops edges below the threshold.

```
$ chess-crawl export graph --provider chess.com --format graphml --min-games 3 --out hikaru_net.graphml
Graph: 188 nodes, 642 edges (min-games=3). Provider=chess.com. → hikaru_net.graphml
```
**Exit:** `0` (empty graph allowed); `2` bad format; `1` write failure.

### End-to-End Example Session

```bash
# 0. One-time setup: create the archive and set polite identity via env.
export CHESS_CRAWL_DB=./archive.db
export CHESS_CRAWL_CONTACT="xorman@gmail.com"
export CHESS_CRAWL_LICHESS_TOKEN="lip_…"          # optional; raises YOUR Lichess limits
chess-crawl init                                   # exit 0, schema seeded
chess-crawl provider list                          # confirm keys & capabilities

# 1. Seed identities on each provider (provider-scoped; NOT assumed to be the same human).
chess-crawl fetch user chess.com hikaru --with-stats
chess-crawl fetch user lichess DrNykterstein

# 2. Bounded game pulls — note the provider-specific bound granularity.
chess-crawl fetch games chess.com hikaru --since 2024-01 --until 2024-03 --time-class blitz --rated
chess-crawl fetch games lichess DrNykterstein --since 2024-01-01 --until 2024-04-01 \
    --perf bullet,blitz --rated --max-games 5000

# 3. One-hop opponent discovery on Chess.com. Ctrl-C midway is safe…
chess-crawl crawl opponents chess.com hikaru --depth 1 \
    --since 2024-01 --until 2024-01 --time-class blitz --max-users 200 --max-jobs 500
# ^C   (interrupted → exit 130, frontier checkpointed)

# 4. Inspect and resume — the whole system is job-driven, so this just continues.
chess-crawl jobs status --run 12
chess-crawl jobs resume --run 12                   # drives remaining pending/blocked jobs → exit 0

# 5. Query the local archive (no network).
chess-crawl query user chess.com hikaru
chess-crawl query game lichess q7ZvsdUF

# 6. Export for downstream tools.
chess-crawl export games --provider chess.com --user hikaru \
    --since 2024-01-01 --until 2024-04-01 --format pgn --out hikaru_q1.pgn
chess-crawl export graph --provider chess.com --min-games 3 --format graphml --out hikaru_net.graphml

# 7. Re-run anything — idempotent. Immutable chess.com months return 304s,
#    lichess finished games dedupe on content_hash; no duplicates, no wasted refetch.
chess-crawl fetch games chess.com hikaru --since 2024-01 --until 2024-03 --time-class blitz --rated
```

Every acquisition step merely wrote `discovery_jobs` rows and turned the runner; the read/inspect/export steps only touched local SQLite. That uniformity is what makes the CLI resumable after any interruption and safe to re-run verbatim.

---

## 14. Reports & Analytics

All reports read only from normalized query tables and never refetch. Every report is **provider-scoped by default**: a `provider` filter participates in every query, and the surrogate `provider_users.id` is always resolved *within* one provider.

**Cross-provider rule.** No report JOINs a Chess.com user to a Lichess user on username, rating, or any field. When a caller wants both providers, results stack **side-by-side** with the `provider` column retained and an explicit caveat above the table:

> These two panels describe accounts on different providers. chess-crawl does **not** assume the `chess.com` account and the `lichess` account belong to the same person.

The only combining operator allowed is a `UNION ALL` that keeps the `provider` discriminator and never correlates identities:

```sql
SELECT 'chess.com' AS provider, /* metrics */ FROM ... WHERE provider='chess.com'
UNION ALL
SELECT 'lichess'   AS provider, /* metrics */ FROM ... WHERE provider='lichess';
```

**Canonical column references** (all aligned to §9): `games(id, provider, provider_game_id, canonical_url, content_hash, outcome ∈ {white_win,black_win,draw}|NULL, is_live, status_raw, rated 0/1, ended_at, time_control_id, variant_id, eco, opening_name)`; `game_participants(game_id, color, provider_user_id, username_normalized, result_raw, is_winner)`; `ratings_at_game(game_id, color, rating, rating_diff, rd)` — **no user column; side resolved via `game_participants(game_id,color)`**; `user_snapshots(provider_user_id, captured_at, observed_username, status, followers, count_*, ...)`; `time_controls(id, kind, initial_seconds, increment_seconds, days, time_class, raw_label)`; `variants(id, canonical_name, provider, provider_native_name, mapped)`; `discovery_edges(crawl_run_id, provider, from_user_id, to_user_id, via_game_id, depth, edge_kind)`; `discovery_jobs(crawl_run_id, provider, kind, state, depth, target)`.

Seed resolution, reused as `:uid`:
```sql
SELECT id FROM provider_users WHERE provider = :provider AND username_normalized = :username;
```

Shared per-color scoring expression (a `game_participants` row `gp` for the user + `games.outcome`), **NULL-aware**:
```sql
CASE
  WHEN g.outcome IS NULL                              THEN 'unfinished'
  WHEN (gp.color='white' AND g.outcome='white_win')
    OR (gp.color='black' AND g.outcome='black_win')   THEN 'win'
  WHEN g.outcome='draw'                               THEN 'draw'
  ELSE 'loss'
END
```
Undecided games (`outcome IS NULL`) are counted in `games` totals but never contribute to W/L/D, so a Chess.com "every game is decided" assumption never leaks into aggregates.

### User game summary / counts

- **Inputs:** `:provider`, `:uid`, optional `:since`/`:until` (epoch seconds).
- **Output:** `games, rated_games, unrated_games, wins, losses, draws, unfinished, first_game_ts, last_game_ts, distinct_opponents`.

```sql
WITH mine AS (
  SELECT g.id, gp.color, g.outcome, g.rated, g.ended_at
  FROM games g
  JOIN game_participants gp ON gp.game_id = g.id AND gp.provider_user_id = :uid
  WHERE g.provider = :provider
    AND (:since IS NULL OR g.ended_at >= :since)
    AND (:until IS NULL OR g.ended_at <  :until)
)
SELECT
  COUNT(*)                                             AS games,
  SUM(rated)                                           AS rated_games,
  SUM(1 - rated)                                       AS unrated_games,
  SUM((color='white' AND outcome='white_win')
   OR (color='black' AND outcome='black_win'))         AS wins,
  SUM(outcome='draw')                                  AS draws,
  SUM((color='white' AND outcome='black_win')
   OR (color='black' AND outcome='white_win'))         AS losses,
  SUM(outcome IS NULL)                                 AS unfinished,
  MIN(ended_at) AS first_game_ts,
  MAX(ended_at) AS last_game_ts
FROM mine;
-- distinct_opponents via the opponent CTE below (COUNT(DISTINCT opp_id)).
```

### Opponent list

- **Inputs:** `:provider`, `:uid`, optional window.
- **Output:** `opponent_username, opponent_user_id, games, my_wins, draws, my_losses`.

```sql
WITH opp AS (
  SELECT gp_o.provider_user_id AS opp_id, gp_m.color AS my_color, g.outcome
  FROM games g
  JOIN game_participants gp_m ON gp_m.game_id = g.id AND gp_m.provider_user_id = :uid
  JOIN game_participants gp_o ON gp_o.game_id = g.id AND gp_o.color <> gp_m.color
  WHERE g.provider = :provider AND gp_o.provider_user_id IS NOT NULL
)
SELECT pu.display_username, opp.opp_id,
  COUNT(*)                                                       AS games,
  SUM((my_color='white' AND outcome='white_win')
   OR (my_color='black' AND outcome='black_win'))                AS my_wins,
  SUM(outcome='draw')                                            AS draws,
  SUM((my_color='white' AND outcome='black_win')
   OR (my_color='black' AND outcome='white_win'))                AS my_losses
FROM opp
JOIN provider_users pu ON pu.id = opp.opp_id
GROUP BY opp.opp_id
ORDER BY games DESC, pu.username_normalized;
```
Opponents present only by username (not yet materialized to a `provider_users` row) are naturally excluded; resolve them by running `fetch user` / `crawl opponents`.

### Repeated opponents (played ≥ N times)

Identical to the opponent list with a trailing `HAVING COUNT(*) >= :n` and `ORDER BY games DESC`. Also the natural feed for `crawl opponents` prioritization.

### Rating-band distribution

- **Inputs:** `:provider`, `:uid`, optional band width (default 100).
- **Output:** `band_floor, band_label, games`.

The opponent's side is resolved via `game_participants(game_id,color)` (since `ratings_at_game` has no user column):
```sql
WITH opp_r AS (
  SELECT rag.rating
  FROM games g
  JOIN game_participants me  ON me.game_id = g.id AND me.provider_user_id = :uid
  JOIN game_participants opp ON opp.game_id = g.id AND opp.color <> me.color
  JOIN ratings_at_game rag   ON rag.game_id = g.id AND rag.color = opp.color
  WHERE g.provider = :provider AND rag.rating IS NOT NULL
)
SELECT (rating/100)*100 AS band_floor,
       printf('%d–%d', (rating/100)*100, (rating/100)*100+99) AS band_label,
       COUNT(*) AS games
FROM opp_r
GROUP BY band_floor
ORDER BY band_floor;
```
For fixed bands (`<1000, 1000–1199, …, 2400+`) swap the `GROUP BY` for a `CASE`. Ratings are provider-native and never compared across providers.

### Time-control / speed distribution

- **Inputs:** `:provider`, `:uid`.
- **Output:** `time_class, raw_label, initial_seconds, increment_seconds, games`.

```sql
SELECT tc.time_class, tc.raw_label, tc.initial_seconds, tc.increment_seconds,
       COUNT(*) AS games
FROM games g
JOIN game_participants gp ON gp.game_id = g.id AND gp.provider_user_id = :uid
JOIN time_controls tc     ON tc.id = g.time_control_id
WHERE g.provider = :provider
GROUP BY tc.id
ORDER BY games DESC;
```
`time_class` is the shared taxonomy (bullet/blitz/rapid/classical/correspondence; Chess.com `daily` and Lichess `ultraBullet` folded per §10); `raw_label` preserves the native clock descriptor.

### Variant distribution

- **Inputs:** `:provider`, `:uid`.
- **Output:** `canonical_name, provider_native_name, games, pct`.

```sql
SELECT v.canonical_name, v.provider_native_name,
       COUNT(*) AS games,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (), 1) AS pct
FROM games g
JOIN game_participants gp ON gp.game_id = g.id AND gp.provider_user_id = :uid
JOIN variants v           ON v.id = g.variant_id
WHERE g.provider = :provider
GROUP BY v.id
ORDER BY games DESC;
```

### Win / loss / draw summaries

- **Inputs:** `:provider`, `:uid`, optional filters.
- **Output:** (a) per-color pivot; (b) termination breakdown.

```sql
-- (a) by color, from games.outcome + game_participants.color
SELECT gp.color,
  COUNT(*)                                                   AS games,
  SUM((gp.color='white' AND g.outcome='white_win')
   OR (gp.color='black' AND g.outcome='black_win'))          AS wins,
  SUM(g.outcome='draw')                                      AS draws,
  SUM((gp.color='white' AND g.outcome='black_win')
   OR (gp.color='black' AND g.outcome='white_win'))          AS losses,
  SUM(g.outcome IS NULL)                                     AS unfinished,
  ROUND(100.0*(SUM((gp.color='white' AND g.outcome='white_win')
   OR (gp.color='black' AND g.outcome='black_win'))
   + 0.5*SUM(g.outcome='draw'))
   / NULLIF(SUM(g.outcome IS NOT NULL),0), 1)                AS score_pct
FROM games g
JOIN game_participants gp ON gp.game_id = g.id AND gp.provider_user_id = :uid
WHERE g.provider = :provider
GROUP BY gp.color;

-- (b) how games ended (native codes retained)
SELECT g.status_raw, COUNT(*) AS games
FROM games g
JOIN game_participants gp ON gp.game_id = g.id AND gp.provider_user_id = :uid
WHERE g.provider = :provider
GROUP BY g.status_raw ORDER BY games DESC;
```
`score_pct` divides by decided games only. Raw per-color codes stay in `game_participants.result_raw`, the native end status in `games.status_raw`.

### Account-status summary

- **Inputs:** `:provider`, a population selector (`:crawl_run_id`, or opponents-of-`:uid`).
- **Output:** `status, status_category, users, pct`.

`status_category` is a query-time bucketing over the verbatim `user_snapshots.status`:
```sql
WITH latest AS (
  SELECT us.*, ROW_NUMBER() OVER (PARTITION BY us.provider_user_id
                                  ORDER BY us.captured_at DESC) AS rn
  FROM user_snapshots us
)
SELECT ls.status,
       CASE
         WHEN ls.status IN ('closed:fair_play_violations','tosViolation') THEN 'provider_restriction'
         WHEN ls.status IN ('closed','closed:abuse','disabled')           THEN 'closed'
         ELSE 'active'
       END AS status_category,
       COUNT(*) AS users,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (), 1) AS pct
FROM latest ls
JOIN provider_users pu ON pu.id = ls.provider_user_id AND pu.provider = :provider
WHERE ls.rn = 1
  AND ls.provider_user_id IN ( /* population: opponents-of-:uid or crawl_runs membership */ )
GROUP BY ls.status
ORDER BY users DESC;
```

**Mandatory caveat rendered with this report:**

> `status`/`status_category` are the **provider's own** account labels captured at snapshot time — Chess.com `basic`/`premium`/`staff`/`closed`/`closed:fair_play_violations`, or Lichess `disabled`/`tosViolation`/`closed`. chess-crawl reports them as neutral, provider-supplied facts. They are **not** a determination made by this tool, and their presence in a report is not an accusation.

### Games by period

- **Inputs:** `:provider`, `:uid`, `:granularity` (`%Y`, `%Y-%m`, `%Y-%m-%d`, `%Y-W%W`).

```sql
SELECT strftime(:granularity, g.ended_at, 'unixepoch') AS period,
       COUNT(*) AS games,
       SUM((gp.color='white' AND g.outcome='white_win')
        OR (gp.color='black' AND g.outcome='black_win')) AS wins,
       SUM(g.outcome='draw') AS draws
FROM games g
JOIN game_participants gp ON gp.game_id = g.id AND gp.provider_user_id = :uid
WHERE g.provider = :provider
GROUP BY period ORDER BY period;
```
`ended_at` is normalized epoch seconds (Chess.com native; Lichess `lastMoveAt` converted), so `strftime(... 'unixepoch')` is uniform across providers.

### Connected components over discovery_edges

- **Inputs:** `:provider` (required — components are per provider), optional `:crawl_run_id`, `:seed_id`.

Provider-scoped single-component reachability (undirected BFS in pure SQL):
```sql
WITH RECURSIVE reach(user_id) AS (
  SELECT :seed_id
  UNION
  SELECT CASE WHEN e.from_user_id = r.user_id
              THEN e.to_user_id ELSE e.from_user_id END
  FROM discovery_edges e
  JOIN reach r ON (e.from_user_id = r.user_id OR e.to_user_id = r.user_id)
  WHERE e.provider = :provider
    AND (:crawl_run_id IS NULL OR e.crawl_run_id = :crawl_run_id)
)
SELECT COUNT(*) AS component_size FROM reach;
```
Full enumeration + centrality via networkx (optional, in `export/graph.py`):
```python
import networkx as nx
G = nx.Graph()
# rows: SELECT provider, from_user_id, to_user_id FROM discovery_edges [WHERE crawl_run_id=?]
for prov, a, b in rows:
    G.add_edge((prov, a), (prov, b))        # provider is PART OF the node key →
    # a chess.com node and a lichess node can never share an edge/component
components = sorted(nx.connected_components(G), key=len, reverse=True)
```
Baking `provider` into the node key is the structural guarantee that the "same username across providers is not the same person" boundary cannot be violated by the graph layer.

### Job & crawl status + depth-frontier summary

- **Inputs:** `:crawl_run_id` (or all), `:provider`.

```sql
-- overall job health for a run (canonical state enum)
SELECT state, COUNT(*) AS jobs
FROM discovery_jobs WHERE crawl_run_id = :crawl_run_id
GROUP BY state;                       -- pending|in_progress|done|error|skipped|blocked

-- depth-frontier: how much work sits at each BFS depth
SELECT depth,
  SUM(state='pending')     AS pending,
  SUM(state='in_progress') AS running,
  SUM(state='done')        AS done,
  SUM(state='error')       AS failed
FROM discovery_jobs WHERE crawl_run_id = :crawl_run_id
GROUP BY depth ORDER BY depth;
```
The **frontier** is the set of `pending` jobs at the current maximum reached depth; `MAX(depth) WHERE state='done'` vs `crawl_runs.params_json.max_depth` shows remaining depth budget. Exactly what `jobs status` renders and `jobs resume` consumes.

### Archive coverage (fetched vs missing)

The two providers differ fundamentally; the report **must not** pretend they share a unit. Coverage is derived **entirely from existing data** — no helper tables outside the 16-table spine.

**Chess.com (discrete, enumerable).** The expected set is the stored `/games/archives` index (a `raw_payloads` row with `endpoint_type='archives_index'`, parsed to month keys), or generated month-by-month between the user's join month and the current month. Fetched units are read from `fetch_logs` where `endpoint_type='monthly_archive'` and `status_code IN (200,304)`, with `YYYY/MM` **parsed from `fetch_logs.url`** and the user attributed by URL (or via `source_records → provider_users`):

```sql
-- expected_months(:uid): CTE materialized from the archives_index raw body for this user
-- (parse the ".../games/YYYY/MM" list), else generated over [join_month, current_month].
WITH fetched AS (
  SELECT DISTINCT
         substr(fl.url, instr(fl.url,'/games/')+7, 7) AS ym   -- 'YYYY/MM' from the url
  FROM fetch_logs fl
  WHERE fl.provider = 'chess.com'
    AND fl.endpoint_type = 'monthly_archive'
    AND fl.status_code IN (200, 304)
    AND fl.url LIKE '%/player/' || :username_normalized || '/games/%'
)
SELECT em.ym AS archive_unit,
       CASE WHEN f.ym IS NULL THEN 'MISSING' ELSE 'fetched' END AS status
FROM expected_months em
LEFT JOIN fetched f ON f.ym = em.ym
ORDER BY em.ym;
```
The current month is flagged mutable (may grow); past months are immutable — a single successful fetch marks them permanently complete.

**Lichess (continuous, no month concept).** There is no archive unit to enumerate. Coverage is the union of `[since,until)` windows streamed, reconstructed from `fetch_logs` rows with `endpoint_type='user_games_stream'` for that user (parse `since`/`until` from `url`), with gaps derived by interval subtraction over `[user.createdAt … now]`:

```
covered:  [2025-01-01 .. 2025-06-30]  [2025-09-01 .. 2026-07-02]
GAP:                      [2025-07-01 .. 2025-08-31]   <- not yet streamed
```
Because Lichess months are not immutable and dedup relies on `content_hash` + game immutability (plus the look-back window, §10), "coverage" here means *date ranges requested*, not *guaranteed-complete months* — and the report says so explicitly.

### Anomaly candidates (human review only)

> **Disclaimer — read first.** Everything in this subsection consists of **neutral, non-conclusive statistical signals** surfaced only to help a **human analyst** decide where to look. **They are not evidence of wrongdoing of any kind.** chess-crawl makes **no accusation, assigns no guilt, and reaches no conclusion** about any account. Ordinary, legitimate players routinely trigger these signals (improvement, a strong tournament, a preferred opponent pool, a new account created by an already-skilled player). Any provider account-status label that appears (Chess.com `closed:fair_play_violations`, Lichess `tosViolation`, etc.) is the **provider's own label**, reported as a neutral fact — never a determination by this tool. These signals must never be exported or presented as a "cheating score," a ranking of suspicion, or an automated flag. They are inputs to human judgment only.

Each signal is a tunable, threshold-parameterized query over already-stored data, provider-scoped:

- **High score versus higher-rated opponents.** Score against opponents rated ≥ `:gap` higher (opponent rating resolved via `game_participants`+`ratings_at_game` color join). Small `n` explicitly labeled low-confidence.
- **Steep rating gain over a short span.** Largest rise within any `:days`-day window over the user's own `ratings_at_game` sequence in one time-class. Improvement is normal; this only bookmarks a period.
- **Dense neighborhood of already provider-flagged accounts.** Among the user's `discovery_edges` neighbors, the fraction whose latest `user_snapshots.status` carries a provider restriction label. **Reports counts of the provider's labels only; proximity is not participation** and is stated as such.
- **Very new account with an already-high rating.** Accounts whose join/create time is within `:days` yet whose rating ≥ `:rating`. Returning players and imports are common explanations.
- **Unusual draw / timeout / termination ratios.** Per-user `status_raw`/`result_raw` histogram vs the cohort baseline for the same time-class. Time-control habits and connection issues produce these naturally.

**Sample neutral report wording (safe for a human to read):**

> **Review note — not a finding.** For `provider = chess.com`, user `example_user` scored 76% (0.5 per draw) across 33 rated blitz games versus opponents rated 150+ points higher, during 2026-01 to 2026-03 (sample size: 33). This is a statistical observation surfaced for optional human review. It is **not** an indication of any rule violation and implies no judgment by chess-crawl. Any account-status label shown elsewhere is the provider's own label. Please treat this as a starting point for human analysis, not a conclusion.

> **Neighborhood note — not a finding.** Of `example_user`'s 41 discovered opponents on `lichess`, 6 currently carry the provider's own `tosViolation` label as recorded at last snapshot. This is a neutral count of provider-supplied labels among graph neighbors. It is **not** evidence about `example_user`, and chess-crawl draws no inference from it. Proximity in the discovery graph is not participation.

All anomaly outputs carry the disclaimer inline (emitted non-suppressibly by `reports/render.py`), are gated behind an explicit `--include-review-signals` flag on any report/export command, and are omitted from default `query`/`export` output so they can never be mistaken for a factual determination.

---

## 15. Safety, Ethics & Publication Constraints

`chess-crawl` archives data about **real, named people**. Every design decision here treats that fact as the primary constraint. Where a rule can be enforced mechanically, it is; where it cannot, it is stated plainly in the code, the docs, and the exports.

### Acquisition Boundaries

| Boundary | v1 stance | Enforcement point |
|---|---|---|
| Data source | **Public provider APIs only** (`api.chess.com/pub/`, `lichess.org/api`) | `endpoints.py` names only public hosts/paths |
| HTML scraping | **Never** — no profile HTML, no DOM parsing, no headless browser | Only JSON/NDJSON/PGN `Accept` types; no HTML parser dependency |
| Concurrency | **Serial only**, one in-flight request per provider, with a polite delay | `runner.py` single-flight; delay is a floor |
| Rate-limit reaction | **Respect provider guidance; never evade** | `client.py`/`FetchPolicy` honor `Retry-After` / mandatory pause |
| Authentication | **Public reads only.** The optional Lichess personal token raises limits but unlocks no private scope | Only an opt-in bearer token is ever sent, and only to Lichess; no login/cookies |
| Private / third-party data | **Never fetched, ever** | No endpoint returning non-public data is reachable |

The tool retrieves only what any anonymous member of the public could retrieve. It does not access data gated behind login and does not reconstruct private information.

### No Rate-Limit Evasion

"Polite and serial" is a correctness requirement, not just etiquette. The client honors each provider's stated guidance and treats throttling as instructions to obey.

- **Chess.com:** serial with a deliberate delay. On `429`, stop and honor `Retry-After`. Strong `ETag`/`Last-Modified` are used to send conditional requests and accept `304`, which *reduces* load — the correct direction of optimization.
- **Lichess:** serial with a descriptive `User-Agent`. On `429`, a **mandatory full 60-second pause before any resume** (hard rule), regardless of any other signal. This pause is non-configurable downward.
- Back-off is never circumvented by rotating IPs, spoofing user agents, distributing requests across hosts, splitting work across processes to defeat a shared limiter, or ignoring `Retry-After`. There is no "fast mode."

```
on response:
  if status == 429:
      pause = provider.mandatory_pause_seconds        # lichess -> 60 (floor), chesscom -> Retry-After
      pause = max(pause, retry_after_header or 0)
      log_fetch(status=429, retry_after=pause)
      sleep(pause)                                     # obey; do NOT retry faster, do NOT switch identity
      resume_serially()
```

#### The Optional Lichess Token Is Present-But-Optional in v1

Lichess offers an **optional** personal OAuth token that raises rate limits. In chess-crawl:

- **Supported in v1, opt-in, off by default.** v1 works fully unauthenticated. If the operator supplies **their own** token (env `CHESS_CRAWL_LICHESS_TOKEN`, config, or `--lichess-token`), it is sent as `Authorization: Bearer` **to Lichess only** to raise **their own** limits, exactly as Lichess intends.
- **Never required, never bundled, never shared.** Absence just means lower limits. The token is **never** written to `raw_payloads`, `fetch_logs`, `response_headers`, `errors`, provenance, or any export, and is masked in verbose logs.
- A raised personal limit is **not** a license to crawl harder against third parties or to bypass the serial/polite posture. The 60s-on-429 rule still applies.

#### Authenticated Personal-Data Export Is a FUTURE Extension Only

A future authenticated *export* capability (a user pulling **their own** full account data from an authenticated personal-data endpoint) is:
- opt-in, disabled by default, clearly labeled personal-data-only;
- usable **only** to fetch **the authenticating user's own** data — never third-party private data;
- **out of scope for v1.** v1 archives public data only. (This is the only auth-related future item; the rate-limit token above is a present v1 feature.)

### No Cheating Accusations, No Guilt Labels

This tool **never** makes, implies, computes, ranks, or emits a determination that a person cheated. Absolute, across every command, report, export, log line, and column name.

- No output field asserts guilt. There is no `is_cheater`, no `cheat_score`, no `suspected` — no boolean or numeric a reader could interpret as an accusation.
- **Anomaly signals**, if surfaced at all, are **neutral, non-conclusive, statistical observations for human review only**, never phrased as conclusions, always travelling with a disclaimer. Acceptable framing: "rating gain over interval," "win rate vs. rating band" — descriptive statistics, not verdicts.
- Every report/export that includes any anomaly-adjacent signal carries a fixed disclaimer emitted by `reports/render.py` / `export/*` as a non-optional preamble; it cannot be suppressed.

```
ANOMALY_DISCLAIMER = (
  "These figures are descriptive statistics derived from public data. "
  "They are NOT evidence of cheating, and this tool makes no such determination. "
  "Any interpretation requires qualified human judgment. Do not use these figures "
  "to accuse, label, or make decisions about any individual."
)
```

#### Provider Account-Status Labels Are the Provider's Facts, Not Ours

| Provider value | Source | How chess-crawl treats it |
|---|---|---|
| `closed:fair_play_violations` (Chess.com `status`) | Chess.com's own moderation | Stored raw; displayed as "Chess.com reports account status: `closed:fair_play_violations`" |
| `closed`, `closed:abuse`, `basic`, `premium`, `staff` (Chess.com) | Chess.com | Stored/displayed verbatim, attributed to Chess.com |
| `tosViolation` (Lichess) | Lichess's own moderation | Stored raw; displayed as "Lichess reports: `tosViolation`" |
| `disabled`, `closed` (Lichess) | Lichess | Stored/displayed verbatim, attributed to Lichess |

- Always **attributed to the provider** in human-facing output ("Chess.com reports…", "Lichess reports…"), never a chess-crawl finding.
- chess-crawl does **not** re-derive, infer, corroborate, second-guess, or editorialize them, nor translate `fair_play_violations` into "cheater."
- They are one more field of the archived public record — nothing added, nothing concluded.

### Provider-Scoped Identity: A Technical AND Ethical Rule

A Chess.com username and a Lichess username that look alike, or match exactly, **must not be assumed to be the same human.**

- **Technically:** identity is provider-scoped throughout. `provider_users` is keyed by a surrogate id with `UNIQUE(provider, provider_user_id)` and `UNIQUE(provider, username_normalized)`. There is no cross-provider identity key, no "same person" table, no automatic linking, no join collapsing two providers into one human. Chess.com `provider_user_id` = numeric `player_id`; Lichess `provider_user_id` = lowercased username id — different namespaces, never unified.
- **Ethically:** asserting two accounts are the same person is a claim about a real individual this tool has no basis to make and that could cause real harm if wrong. chess-crawl refuses. Any future account-linking feature would require the affected person's own explicit assertion, be opt-in, and be clearly marked as a user-supplied claim — never a tool inference. Out of scope for v1.

### Local-First Storage; The User Controls All Exports

- **Local-first:** all data lives in the user's local SQLite DB and raw-payload store. No phone-home, no upload, no sync to any chess-crawl-operated service, no telemetry. The only network traffic is the polite, serial fetches the user requested.
- **User owns the data and every export:** nothing leaves the machine unless the user runs an explicit `export` command with an explicit destination. There is no background or implicit export.
- Because raw payloads are preserved, the user can inspect exactly what a provider returned, re-normalize without refetching, and delete anything they choose.

### Respecting Each Provider's Terms of Service

chess-crawl is built to operate inside both providers' documented expectations: public endpoints only, serial and polite access, honoring throttling, honoring immutability/caching to minimize load, no scraping or evasion. Users remain responsible for their own use. Docs direct users to read and follow the current Chess.com and Lichess Terms of Service and API guidance, and to stop if a provider asks them to.

### Responsibly Publishing or Sharing Derived Data About Real, Named People

- **Aggregate and anonymize by default when publishing.** Prefer distributions and rating-band summaries over per-named-person dossiers.
- **Do not publish anomaly signals against named individuals.** They are for private human review only; next to a real name they read as an accusation regardless of any disclaimer.
- **Never publish or imply a cheating conclusion.** Reposting a provider's `fair_play_violations`/`tosViolation` label republishes *that provider's* moderation action; attribute it precisely, add nothing, and consider whether naming the person serves any legitimate purpose.
- **Keep the provider-scoped boundary in anything you share.**
- **Respect provider ToS and licensing on redistribution.** Prefer linking to canonical provider URLs over rehosting bulk dumps.
- **Honor removal and context.** Data can be stale; accounts get compromised; closures get reversed — publish with humility and a date.
- **Carry the disclaimer** on any shared artifact that includes derived statistics.

Exports that include any per-named-individual derived signal emit a machine- and human-readable disclaimer header, so the constraint travels with the file.

### README "Responsible Use" Blockquote (Paste-Ready)

> **Responsible Use**
>
> `chess-crawl` archives **public** data about **real people** from the Chess.com and Lichess public APIs. With that comes responsibility.
>
> - **Public API only.** Documented public endpoints; **no** HTML scraping, **no** login, **no** private or third-party non-public data. An optional Lichess personal token (your own) may raise **your** rate limits, but unlocks no private data; authenticated personal-data export is a future, opt-in feature for **your own** data only.
> - **Polite and serial. No evasion.** Requests go out one at a time with a courteous delay. On HTTP `429`, the tool stops and waits — **a mandatory 60 seconds for Lichess** — and honors `Retry-After`. It never rotates identities, spoofs, or works around rate limits.
> - **No cheating accusations. Ever.** No determination that anyone cheated, no guilt label. Any anomaly statistic is a neutral, non-conclusive figure for qualified human review only, and always ships with a disclaimer.
> - **Provider labels are the provider's, not ours.** Values like Chess.com `closed:fair_play_violations` or Lichess `tosViolation` are those providers' own determinations, shown as neutral facts, attributed to the provider, with nothing inferred.
> - **Different sites, different people.** A Chess.com username and a Lichess username are **never** assumed to be the same human — technically and ethically.
> - **Your machine, your data, your call.** Everything is stored locally. No telemetry, no upload. Nothing is exported unless you run an export command yourself.
> - **Follow the rules of the road.** Respect the current Chess.com and Lichess Terms of Service and API guidance. If a provider asks you to stop, stop.
> - **Publish with care.** When sharing derived data about named individuals, aggregate and anonymize, never imply guilt, keep provider identities separate, and carry the disclaimer.
>
> You are responsible for how you use this tool and for anything you choose to publish.

---

## 16. Testing Plan

### Guiding Principles And Default-Suite Guarantee

The default `pytest` run MUST be **fully offline, deterministic, and fast** (target < 10s). It never opens a socket, never sleeps in real time, never depends on wall-clock `now()`:

- A session-autouse `no_network` fixture monkeypatches the HTTP transport and raises on any real socket connect.
- All time is injected: runner, rate limiter, and backoff depend on a `Clock` protocol (`now()`, `sleep(seconds)`), never `time.*`. Tests pass a `FakeClock` recording requested sleeps and advancing virtual time instantly.
- All randomness (jitter, ids) is seeded or injected.
- Live tests are quarantined behind `@pytest.mark.live`, skipped unless `--run-live` is passed.

**DTO/entity naming (standardized across the suite):** `RawRecord` = in-flight fetch object returned by a client and persisted into `raw_payloads`; `NormalizedUser` / `NormalizedGame` / `NormalizedParticipant` = normalized DTOs. There is no separate `RawPayload`/`UserProfileDTO`/`GameDTO` type. Raw dedup uses `RawRecord.body_hash` (whole-body fingerprint); per-game identity uses `content_hash` (canonical subset) — the two are never conflated.

```
tests/
  conftest.py                 # temp db, FakeClock, fake transport, no_network guard
  fixtures/ (chesscom/ lichess/ graphs/ …)
  contract/test_provider_contract.py
  unit/{test_endpoints_*, test_parser_*, test_content_hash, test_timestamps}.py
  storage/{test_migrations, test_raw_ingestion, test_repository_upserts}.py
  http/{test_chesscom_http, test_lichess_http}.py
  jobs/{test_state_machine, test_runner_resume, test_caps_and_depth}.py
  e2e/test_two_provider_crawl.py
  live/test_live_smoke.py            # @pytest.mark.live
```

### Provider Contract Tests

One parametrized suite proves **both** clients are substitutable behind `ProviderClient`. Parametrized over a `client_case` fixture yielding `(client, fixture_bundle)` per provider.

- `isinstance`/structural match for `ProviderClient` (all methods, declared signatures via `runtime_checkable` Protocol + `inspect.signature`).
- `client.key()` returns exactly `"chess.com"`/`"lichess"` and matches a `providers.key` row.
- `get_user_profile(username)` returns a `NormalizedUser` with the same field set regardless of provider; provider extras live in the raw body, never as top-level DTO fields.
- `iter_user_games(...)` yields a homogeneous stream of `RawRecord`; after normalization every `NormalizedGame` carries `provider`, an `outcome ∈ {white_win, black_win, draw}` **or `None`** (with `status_raw` retained), `end_time` as **int epoch seconds**, and a `content_hash`.
- Every fetch method returns a `RawRecord` (body bytes + provenance) so raw-first is structurally guaranteed by the interface.
- The Chess.com by-id path is exercised through the **owning monthly archive** (fixture-served), never a direct by-id GET.

### Fixtures For Both Providers

Curated real-shape payloads, checked in, never fetched at test time. Includes edge cases: a `closed:fair_play_violations` Chess.com account, a Lichess `tosViolation`/`disabled` account, a draw with `winner` absent, a Lichess **`aborted`** game (asserts `outcome IS NULL`, `is_live=0`) and a `started` in-progress game (`outcome NULL`, `is_live=1`), a Chess.com `agreed`/`agreed` draw, a Lichess `status:"cheat"` game (asserts label stored verbatim, no derived guilt), a chess960 game on each side, and a Chess.com **bughouse** game (asserts raw stored, `normalization_status='skipped'`, no `games` row).

### URL Construction Unit Tests (endpoints.py)

Pure, no I/O. Chess.com: `player_profile("Hikaru") == ".../pub/player/hikaru"` (lowercased path); `monthly_archive("hikaru",2021,1)` ends `/games/2021/01` (zero-padded; `2021/1` never produced); out-of-range month raises. Lichess: `user_profile("Thibault") == ".../api/user/thibault"`; `user_games(...)` exact query string (`perfType` CSV, booleans `true`/`false`, ms passthrough); `export_by_ids([...])` respects the ~300-id batch boundary (over-limit raises before any request).

### Raw-First Ingestion Idempotency

- **Body dedup:** ingesting the same body twice yields exactly **one** `raw_payloads` row (deduped by `body_hash`); the second ingest returns the existing id (a new `fetch_logs` row MAY record the second fetch, but no duplicate body).
- **Compression transparency:** large bodies round-trip byte-identical; `body_hash` over the **decompressed** bytes is invariant to codec.
- **Re-parse without refetch:** with raw rows present, normalize at `parser_version = N+1`. Assert **zero HTTP calls**; normalized rows updated in place (same surrogate ids via natural-key upsert); **`raw_payloads.parser_version` advances** and **`normalization_status` transitions `parsed → stale → parsed`**; a parser change that extracts a previously-missing field (e.g. `eco`) backfills it purely from stored raw.
- **Raw preservation:** raw ms timestamps and provider-native strings remain verbatim even though normalized columns hold converted seconds.

### Normalized Schema And Migration Tests

Against `:memory:` and a temp file:
- Apply all migrations from empty; assert every canonical table exists: `providers, provider_users, user_snapshots, games, game_participants, ratings_at_game, time_controls, variants, source_records, raw_payloads, fetch_logs, discovery_jobs, discovery_edges, crawl_runs, errors, schema_migrations`.
- `schema_migrations` records each version; re-running `migrate()` is idempotent.
- **Partial-unique indexes** (introspect via `PRAGMA index_list`/`index_info`): `provider_users` (two NULL-id rows with different usernames succeed; same `(provider, username_normalized)` fails); `games` (`content_hash` unique; provider-id/url partial-unique).
- **Foreign keys** enforced (`game_participants.game_id`/`.provider_user_id`, `ratings_at_game`, `source_records.raw_payload_id`, `discovery_edges` endpoints) — orphan insert raises.
- **Provider scoping:** the same `username_normalized` under `chess.com` and `lichess` coexists as two distinct rows.
- **Outcome nullability:** a game with `outcome NULL` inserts; the `CHECK(outcome IN (...))` still rejects a bad non-null value.
- **discovery_edges.depth** column present; edge upsert keeps the minimum depth.

### Per-Provider HTTP Behavior Tests

Injected fake transport mapping `(method, url) → queued (status, headers, body)`; records call order, headers (`User-Agent`, `If-None-Match`, `If-Modified-Since`, and that `Authorization` is **absent** unless a token is configured and **never** appears in `fetch_logs`), and per-call `FakeClock` sleeps.

Chess.com: 200 archive → games + raw stored, ETag captured; 304 on matching `If-None-Match` → cached result, no new raw body, `from_cache=1`, skip re-normalization; 404 → `NotFoundError` + `errors` row (terminal); 410 → `GoneError` (terminal); 429 with `Retry-After: n` → sleeps exactly `n` virtual seconds then retries, requests remain serial; current month mutable path vs past-month conditional path.

Lichess: NDJSON streaming parse across arbitrary byte boundaries (one `NormalizedGame` per line, tolerates trailing/blank line, partial-line-at-boundary, never one big JSON parse); **429 → exactly one `sleep(60)`** (not `Retry-After`, not backoff math), no request during the wait, a second consecutive 429 waits another full 60s; descriptive `User-Agent` on every request; streaming mid-fault durability (K parsed games + raw persisted, resumable from the last committed watermark); `Accept` negotiation (ndjson vs pgn).

### Content-Hash Dedup And Change Detection

`content_hash` over a **deterministic, versioned canonical subset** (stable field order, whitespace-normalized, optional fields excluded): two byte-different-but-semantically-identical bodies hash equal → one `games` row; two genuinely different games hash differently; `provider` in the canonical subset means dedup is provider-scoped; **change detection** — re-fetching a mutable current-month archive where a live game finished yields a new hash → the existing `(provider, provider_game_id)` row updates in place (`is_live` flips to 0), a new `source_records`/raw body appears, no duplicate; an unchanged game short-circuits; Lichess overlapping date-range restreams insert zero new rows.

### Job Runner, Resume, State Machine, Caps, Depth

- **State machine:** transitions through `{pending → in_progress → done|error|blocked|skipped}` with retryable (429/network → blocked) vs terminal (404/410 → skipped, exhausted → error) distinguished; illegal transitions rejected.
- **Kinds coverage:** every `discovery_jobs.kind` exercised at least once.
- **Idempotent/restartable:** kill mid-job (after N committed units), restart → no duplicate raw/normalized rows, job continues from persisted cursor; a completed job re-enqueued is a no-op (blocked by `ux_jobs_dedup_live`).
- **Resume:** `jobs resume` picks up `pending`/`blocked`-due/orphaned-`in_progress` jobs deterministically and drives them to `done`.
- **Caps:** `--depth`, max-games, max-users, max-jobs enforced; the runner stops enqueuing once a cap is hit and records why, leaving the frontier `pending` (resumable), not silently dropped.
- **Depth:** `crawl_opponents --depth N` enqueues opponents only up to depth N; a `discovery_edges` row with its depth is written per discovery; depth-0 is the seed only.

### Two-Provider Fake-Graph End-to-End Crawl

The flagship integration test. Two tiny fully-known synthetic graphs (one per provider) in `fixtures/graphs/*.json` served through the fake transport (profiles, archives/streams, games all resolve within the fixtures → closed, finite crawl).

```
chess.com:            lichess:
   A                     P
  / \                   / \
 B   C                 Q   R
     |                     |
     D                     S     (D, S at depth 2)
```

Run `crawl opponents PROVIDER <seed> --depth 2` through the real runner + real storage (temp db) + real parsers, fake transport only. Golden assertions:
- `provider_users`: exactly A/B/C/D under `chess.com` and P/Q/R/S under `lichess` as **separate** rows; no cross-provider merge even if a username string collides.
- `games`: exact identities (by `content_hash`/`provider_game_id`); shared games stored once with two `game_participants` rows.
- `ratings_at_game`: correct rating per `(game_id,color)`; `game_participants` correct color and normalized `outcome` (including a `NULL`-outcome fixture game excluded from W/L/D but present).
- `discovery_edges`: exact edge set with correct `depth` (A→B, A→C depth 1; C→D depth 2), no depth-3 edges.
- `discovery_jobs`: all reach `done`; a second full run is a no-op (zero new rows, zero HTTP calls).
- Timestamps: Lichess `end_time` == fixture `lastMoveAt // 1000`; Chess.com pass through.

Validates provider neutrality, provider-scoped identity, dedup, depth capping, edge+depth recording, and end-to-end idempotency in one test.

### Optional Live Smoke Tests

`tests/live/test_live_smoke.py`, `@pytest.mark.live`, skipped unless `--run-live`. Serial, polite, tiny bounded requests (one public profile per provider, one 1-game window), honoring the same rate-limit rules (Lichess 60s-on-429), asserting only coarse shape (200, DTO parses, epoch conversion sane) and never exact values. They catch upstream schema drift, run on demand / nightly, and are never part of the default gate.

---

## 17. Milestones

Chess.com is implemented before Lichess **deliberately** (immutable monthly archives + strong ETag/304 make raw-first + dedup easiest to prove first), but the `ProviderClient` interface and shared storage are provider-neutral from Phase 1 so Lichess slots in without reshaping the core.

### Phase 0 — Research And Plan
**Goal:** Lock contracts and de-risk provider differences before code.
**Deliverables:** confirmed API facts (`docs/providers.md`); frozen naming spine + shared DTO shapes (`RawRecord`, `NormalizedUser`, `NormalizedGame`); ADRs for raw-first + `parser_version` re-normalization, versioned `content_hash` canonicalization, provider-scoped identity, injected `Clock`, outcome-nullable + `is_live`, Chess.com by-id-via-archive; captured real-shape fixtures for both providers.
**Exit:** DTO field lists and the full 16-table list agreed and written; fixtures checked in; no application code yet.

### Phase 1 — Storage Foundation + Provider-Neutral Core
**Goal:** A durable, migratable, raw-first store and the abstraction seams — zero provider network logic yet.
**Deliverables:** `storage/schema.sql` + `migrations.py` + `db.py` (all canonical tables, partial-unique indexes, FKs, `:memory:` support); `storage/raw.py` (`RawRecord` write/read, `body_hash` dedup, compression, `source_records`, `fetch_logs`); `storage/repository.py` (natural-key upserts); `providers/base.py` (`ProviderClient` + DTOs), `registry.py`; `config.py`, `Clock`; CLI skeleton (`init`, `provider list`); migration + raw-idempotency + contract-scaffold tests green (against a stub client).
**Exit:** `chess-crawl init` builds a fully-migrated db; `provider list` prints both keys; migration/raw/idempotency tests pass; the contract harness runs against a stub. No network code merged.

### Phase 2 — Chess.com Client + Raw-First Ingestion + Normalization
**Goal:** End-to-end Chess.com acquisition proving the raw-first + normalize + dedup loop.
**Deliverables:** `providers/chesscom/{endpoints,client,parser}.py` (serial + polite delay, conditional GET, 200/304/404/410/429+Retry-After, by-id via owning archive; per-color result codes → shared outcome); `normalize/{users,games,codes}.py` (shared taxonomies, epoch-seconds pass-through, versioned `content_hash`); CLI `fetch user/games chess.com`, `query user/game chess.com`; Chess.com HTTP/parser/content_hash/re-parse tests green.
**Exit:** fetching a Chess.com user persists raw + normalized rows; re-fetch of an immutable past month → 304, no new raw; re-normalization at a bumped `parser_version` updates rows without any HTTP; `content_hash` dedups identical games; a live game finishing updates in place.

### Phase 3 — Lichess Client Reusing Shared Storage
**Goal:** Add the structurally different provider (streaming NDJSON, ms timestamps, 60s-on-429, optional token) with **no** change to storage or DTOs.
**Deliverables:** `providers/lichess/{endpoints,client,parser}.py` (descriptive UA, mandatory 60s-on-429, streaming NDJSON reader, optional bearer token never logged, id==lowercased username, ms→s, winner+status → shared outcome incl. `NULL` for aborted/in-progress, variant/speed taxonomy); Lichess by-id / by-ids / import wired; CLI `fetch user/games lichess`, `query user/game lichess`; Lichess HTTP tests (NDJSON chunk boundaries, exactly-one `sleep(60)`, partial-stream durability), timestamp tests; the parametrized contract suite green for **both** providers.
**Exit:** contract suite passes for both; Lichess games stream and persist incrementally; ms land as seconds while raw ms preserved; a 429 triggers exactly one `sleep(60)`; the diff touches only `providers/lichess/*` + registry wiring.

### Phase 4 — Jobs, Discovery, Opponent Crawl, Resume
**Goal:** Turn point fetches into durable, resumable, bounded discovery.
**Deliverables:** `jobs/{models,runner,discovery}.py` (job + state machine, serial execution, retry classification, caps/budgets, idempotent restart; `crawl_opponents` writes `discovery_edges` with depth); `crawl_runs` bookkeeping; CLI `crawl opponents --depth N`, `jobs status`, `jobs resume`; state-machine, resume/crash-idempotency, caps/depth tests, and the two-provider fake-graph depth-2 e2e test green.
**Exit:** a depth-2 crawl on each provider reproduces the exact expected rows/states; killing and restarting mid-crawl produces zero duplicates and finishes; a re-run is a no-op; caps stop expansion while leaving the frontier resumable.

### Phase 5 — Reports And Exports
**Goal:** Read-side value + user-controlled egress.
**Deliverables:** `reports/{queries,render}.py` (per-user summaries, game queries, aggregates — all NULL-outcome-aware, all carrying the neutral human-review-only disclaimer where any anomaly/status signal appears; anomaly signals gated behind `--include-review-signals`); `export/{games,graph}.py`; CLI `export games`, `export graph`; report/query tests over a seeded db; export round-trip tests.
**Exit:** `query user/game` return correct normalized data; `export games/graph` produce well-formed, re-importable output; account-status labels appear only as neutral provider-reported facts.

### Phase 6 — Hardening, Docs, Packaging
**Goal:** Ship-ready.
**Deliverables:** robustness pass (error taxonomy, backoff/jitter, compression thresholds, DB integrity checks, `--run-live` nightly lane); docs (`docs/` usage, provider notes, ethics, ADR index; `README`); packaging (`pyproject.toml`: `chess-crawl` distribution + console script + `chess_crawl` package; version pinning; CI matrix).
**Exit:** `pip install .` exposes the `chess-crawl` console script; default suite is offline/deterministic/fast and green; live lane runs green on demand; docs cover install, each CLI command, and the ethics/identity boundaries.

---

## 18. Open Questions

The critic's coverage gaps are **resolved as decisions** in the body (bughouse → raw-only, §2/§10; non-terminal games → `outcome NULL` + `is_live`, §2/§9/§10; `content_hash` canonicalization → versioned pinned subset, §10; Lichess recent-game refetch → high-water mark + rolling look-back, §10; optional-token secret handling → §12/§15). The genuinely-forward-looking items that remain open:

### Cross-provider operator-asserted alias (deliberate non-goal in v1)
Provider-scoped identity forbids asserting a Chess.com account and a Lichess account are the same human, yet operators may want to view "one player" across both. **Open:** do we ever offer an *operator-asserted*, clearly-labeled, non-authoritative local alias table (never inferred, never exported as fact, never used in any join that produces a "person")? Leaning: if built, a separate local-only annotation store, opt-in, with UX that says "your manual note, not a claim by the tool," and stripped from all exports by default. Out of scope for v1.

### Event containers (tournament/match/swiss)
v1 keeps only `games.tournament_ref` (opaque url/id). **Open:** do we later add canonical `tournaments`/`matches`/`swiss` tables? How do we model an event that spans providers (kept separate per the identity boundary) or that we have only partially crawled? Leaning: a provider-scoped `events` table plus a `game_events` link, added as a non-breaking migration when a concrete reporting need appears.

### Lichess id immutability — defensive path
We treat the Lichess id (== lowercased username) as immutable `provider_user_id`. **Open:** if a Lichess account is ever renamed-by-support or otherwise breaks this assumption, do we need a defensive migration, or is the guarantee strong enough to hard-code? Leaning: keep the assumption; add a diagnostic that flags any observed `id`≠`lower(username)` to `errors` for human review rather than auto-migrating.

### Raw-body storage growth, codec, and compaction
Raw-first plus large NDJSON/PGN bodies grows fast. Codec (`gzip` floor / `zstd` extra) and per-row self-description are decided (§8). **Open:** the exact size threshold that triggers compression, whether to ever offer a `vacuum`/`recompress` maintenance step, and how compaction stays consistent with "once fetched, preserved" (leaning: recompression only changes codec, never content; `body_hash` over decompressed bytes stays invariant).

### Bulk import format commitment
`import_export_dump` must ingest already-downloaded data. **Open:** which formats we commit to in v1 (Chess.com monthly JSON, Lichess NDJSON exports, generic multi-game PGN), and how we attribute provider/provenance for a raw PGN whose origin is ambiguous. Leaning: require an explicit `--provider` on import and reject dumps we cannot confidently attribute, rather than guessing.

---

## 19. Implementation Checklist (PR-sized steps)

### Phase 0 — Research And Plan
- [ ] Write `docs/providers.md` (endpoints, rate-limit reactions, timestamp units, Chess.com by-id-via-archive, no bughouse normalization).
- [ ] Write ADRs: raw-first + `parser_version` re-normalization; versioned `content_hash` canonicalization; provider-scoped identity; injected `Clock`; outcome-nullable + `is_live`; optional Lichess token (present, opt-in, never logged).
- [ ] Freeze shared DTO field lists (`RawRecord`, `NormalizedUser`, `NormalizedGame`, `NormalizedParticipant`) in `docs/`.
- [ ] Capture and check in real-shape fixtures for both providers (incl. edge cases: closed/tosViolation, draws, aborted/in-progress, chess960, AI, cheat status, bughouse).

### Phase 1 — Storage Foundation + Provider-Neutral Core
- [ ] Author `storage/schema.sql` with all 16 canonical tables (raw_payloads/source_records/fetch_logs per §8; the rest per §9).
- [ ] Add partial-unique indexes (`provider_users`, `games`) and FK constraints; `discovery_edges.depth`; `time_controls.time_class`; nullable `games.outcome` + `games.is_live`.
- [ ] Implement `storage/db.py` (`:memory:` + file, `PRAGMA foreign_keys=ON`).
- [ ] Implement `storage/migrations.py` with `schema_migrations` tracking and idempotent apply.
- [ ] Write migration tests (tables, partial-unique indexes, FKs, idempotent re-run, outcome nullability).
- [ ] Implement `storage/raw.py` (`RawRecord` write/read, `body_hash` dedup, compression, `source_records`, `fetch_logs`).
- [ ] Write raw-ingestion idempotency tests (same body → one row; re-parse at new `parser_version` → no HTTP, rows updated, `normalization_status parsed→stale→parsed`).
- [ ] Implement `storage/repository.py` natural-key upserts + lookups.
- [ ] Define `providers/base.py` `ProviderClient` + shared DTOs + `FetchPolicy`.
- [ ] Implement `providers/registry.py` registering `chess.com` and `lichess`.
- [ ] Implement `config.py` and the `Clock` protocol (real + `FakeClock`); wire optional-token secrecy.
- [ ] Build CLI skeleton (`init`, `provider list`) with the job-state legend.
- [ ] Add `conftest.py`: temp db, `FakeClock`, fake transport, `no_network` guard.
- [ ] Write the parametrized contract-test harness (stub client for now).

### Phase 2 — Chess.com Client + Ingestion + Normalization
- [ ] Implement `providers/chesscom/endpoints.py` (profile, stats, archives-list, zero-padded monthly archive) + URL unit tests.
- [ ] Implement `providers/chesscom/parser.py` (profile, stats, archive list, monthly archive → DTOs; player_id only from profile — never from `@id`/uuid) + parser tests.
- [ ] Implement `normalize/codes.py` shared taxonomies (variants, time-class fold, per-color result → outcome).
- [ ] Implement `normalize/users.py` + `normalize/games.py` (epoch-seconds pass-through, versioned `content_hash`, bughouse → skipped, live-game in-place update).
- [ ] Write content_hash dedup + change-detection tests.
- [ ] Implement `providers/chesscom/client.py` (serial + polite delay; conditional GET; 200/304/404/410/429+Retry-After; by-id via owning archive).
- [ ] Write Chess.com HTTP tests (200/304/404/410/429) using fake transport + `FakeClock`.
- [ ] Wire ingestion (raw-first then normalize) for profile/stats/monthly-archive/user-games.
- [ ] Add CLI `fetch user/games chess.com`, `query user/game chess.com`.
- [ ] Write the re-parse-at-bumped-parser_version test (no refetch, rows updated).

### Phase 3 — Lichess Client
- [ ] Implement `providers/lichess/endpoints.py` (user, games-by-user with all params, game-by-id, export-by-ids ≤~300) + URL unit tests (CSV perfType, ms passthrough, batch-limit guard).
- [ ] Implement streaming NDJSON reader + `providers/lichess/parser.py` (id==lowercased username, ms→s, winner+status→outcome incl. `NULL`/`is_live`, variant/speed mapping) + parser/timestamp tests.
- [ ] Implement `providers/lichess/client.py` (serial, descriptive UA, mandatory 60s-on-429, streaming, optional bearer token never logged/persisted).
- [ ] Write Lichess HTTP tests (NDJSON chunk-boundary parse, exactly-one `sleep(60)`, partial-stream durability, token-absent-from-logs).
- [ ] Wire Lichess `fetch_game_by_id` / `fetch_games_by_ids` / `import_export_dump` reusing shared storage.
- [ ] Add CLI `fetch user/games lichess`, `query user/game lichess`.
- [ ] Turn the parametrized contract suite green for both providers.

### Phase 4 — Jobs, Discovery, Crawl, Resume
- [ ] Implement `jobs/models.py` job model + state machine (canonical `{pending,in_progress,done,error,skipped,blocked}`; retryable vs terminal).
- [ ] Write state-machine tests (legal/illegal transitions, all kinds exercised).
- [ ] Implement `jobs/runner.py` (serial execution, retry classification, idempotent restart, caps/budgets, Lichess-month-chunk fan-out).
- [ ] Write resume/crash-idempotency tests (kill mid-job, restart, zero duplicates).
- [ ] Implement `jobs/discovery.py` `crawl_opponents` (opponent expansion + `discovery_edges` with min-depth) + `crawl_runs` bookkeeping.
- [ ] Write caps + depth tests.
- [ ] Add CLI `crawl opponents --depth N`, `jobs status` (with legend), `jobs resume`.
- [ ] Build the two fake graphs (`fixtures/graphs/*.json`) and write the two-provider depth-2 e2e golden test (asserts edge depths, NULL-outcome handling, provider-scoped identity).

### Phase 5 — Reports And Exports
- [ ] Implement `reports/queries.py` (per-user summary, opponent list, rating-band via `game_participants` color join, time-control via `time_controls.time_class`, variant, W/L/D NULL-aware, account-status with query-time `status_category`, archive coverage derived from `fetch_logs.url`, discovery components).
- [ ] Implement `reports/render.py` with the non-suppressible neutral disclaimer on any status/anomaly signal; gate anomaly signals behind `--include-review-signals`.
- [ ] Implement `export/games.py` (PGN/NDJSON/CSV) + round-trip test.
- [ ] Implement `export/graph.py` (edge list / GraphML, single-provider node keys) + round-trip test.
- [ ] Add CLI `export games` and `export graph`; write report/query tests over a seeded db (assert column names match `schema.sql`).

### Phase 6 — Hardening, Docs, Packaging
- [ ] Harden error taxonomy in `errors`, backoff/jitter, compression thresholds, DB integrity checks.
- [ ] Add `@pytest.mark.live` smoke tests behind `--run-live` + a nightly/on-demand CI lane.
- [ ] Write `docs/` usage + per-command reference, provider notes, ethics + identity-boundary statement, ADR index, `README` (paste-ready Responsible Use blockquote).
- [ ] Author `pyproject.toml` (`chess-crawl` distribution, `chess-crawl` console script, `chess_crawl` package, `zstd`/`analysis` extras) and configure the CI matrix.
- [ ] Verify `pip install .` exposes the console script and the default suite runs fully offline, deterministic, and fast.