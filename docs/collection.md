# Incremental and full-history collection

Chess Dog submits `POST /v1/imports` with a provider, username, positive
`max_games`, and optional collection settings. Set `collection_mode` to `full`,
`incremental`, or `backfill` for resumable history collection; the default
`bounded` mode requires explicit dates and a total game cap. The backend queues
`fetch_user_games` internally. Callers do not select arbitrary job kinds or URLs.
Opening a profile or reading archived games never starts collection.

The HTTP `batch_size` defaults to 1 and allows 1–12. Each execution handles at
most that many monthly archives, preserved source pages, or unfinished-game
follow-ups, or one Lichess stream page. Internal executor callers have a separate
default of 4 and ceiling of 100. The same job returns to pending until its
durable checkpoint is complete. Provider
failure does not advance that checkpoint. Raw source acquisition and queued
normalization are separate; `collection_coverage.state = complete` means the
source unit was acquired, not that interpretation or analysis has finished.
Check the referenced raw payload's normalization status and parser version for
that separate result. Collection settings are recorded in the original job;
checkpoints cannot be reused with different providers, players, or selection
filters. Capacity settings may be increased while resuming.

## Chess.com

The collector fetches the archives index once per job, inventories its monthly
units, and records pending coverage. Subsequent executions resume that inventory
instead of repeating the index download. Provider URLs are validated and
requests are built from the known endpoint rather than following arbitrary
URLs supplied in a response.

Monthly archives successfully observed after the UTC month ends are sealed.
Full and incremental imports reuse their preserved responses without requesting
those months again. Existing archive bodies from before this feature are
adopted automatically. A source observed while its month was open must be
refreshed after closure so games completed later in that month are not missed.
The current month remains refreshable. Existing conditional validators are
used for index and monthly requests, so unchanged responses can return 304.
Unchanged bodies continue to deduplicate in the archive.

Optional `since`/`until` Unix-second bounds (or explicit `since_ms`/`until_ms`)
select the months overlapping that window. The full body of every selected
month is preserved and normalized. A later working-set selection can filter
individual games more precisely without requesting the month again.

An old parser version or incomplete normalization causes local replay (or a
normalization job when acquisition is running separately). No historical
network fetch is needed to extract information already present in a preserved
source. A `backfill` explicitly refreshes even sealed months. Internally scheduled
backfills can additionally select `months` as a list of `YYYY/MM` resources or
record an `upgrade_id`; those fields are not exposed by the current HTTP import
body. Absent requested months are recorded as missing. Network acquisition is
not performed inside a SQL schema migration. A listed monthly source returning
404/410 is recorded as
missing and does not prevent later available months from being acquired.

## Lichess

The export endpoint uses native millisecond creation timestamps. Its lower
bound is inclusive and upper bound exclusive. This follows the implementation
in Lichess's
[GameApiV2](https://github.com/lichess-org/lila/blob/master/modules/api/src/main/GameApiV2.scala),
[game Query](https://github.com/lichess-org/lila/blob/master/modules/game/src/main/Query.scala),
and [database DSL](https://github.com/lichess-org/lila/blob/master/modules/db/src/main/dsl.scala).
The [official export specification](https://github.com/lichess-org/api/blob/master/doc/specs/tags/games/api-games-user-username.yaml)
describes the supported parameters, returned evidence, and export rate limits.

Full history starts with an open lower bound and a fixed upper timestamp taken
when the job starts. Later full and incremental collections after a completed
open-lower-bound history scan request only the gap from that scan's upper bound.
A full job reuses its preserved source pages in checkpointed local batches,
queuing interpretation and new-run associations separately when processing
workers are enabled. An already covered past upper bound needs no stream
request. Without a complete baseline it imports full
history through the missing ranges. Proven finite intervals and newer portions
of paused saturated pages are reused too. A saturated page proves only the
range strictly newer than its oldest timestamp, leaving that oldest millisecond
open for the safe tie overlap. Disjoint intervals never certify an unknown
older prefix; only their contiguous union from the beginning certifies history.
Each request recomputes missing intervals so another job's committed acquisition
can be reused between checkpoints. Internal `since_ms` and `until_ms` restrict a window; HTTP `since` and
`until` Unix-second bounds are converted without rounding. A partial window
does not certify an uncollected full history.
Completed history watermarks only increase. An older backfill or a delayed
overlapping completion still records its own window without moving the next
incremental starting point backwards.
Reusing a broader raw page retains all its source evidence but associates only
games whose exact native creation timestamp is in the requested half-open
window. This applies equally to local replay and deferred processing.

Pages request `sort=dateDesc`, `pgnInJson=true`, `opening=true`, `division=true`,
`ongoing=true`, and `finished=true`. Clock, evaluation, and accuracy flags retain
the configured provider evidence settings. HTTP `max_games` is the initial page
budget, subject to the configured server ceiling, and is not a total full-history
game cap. Internal executor parameters also support `page_size` (default 1000,
range 1–10000) and `max_page_size`; these are not fields in the current HTTP body.
The next upper bound overlaps the oldest
timestamp by one millisecond. The limit grows to include already observed
boundary games plus new games. This avoids losing games sharing the same
timestamp. It can temporarily exceed `page_size`, up to `max_page_size`
(default 10000, maximum 100000). If a tied boundary exceeds that budget, the job
reports an error and retains its checkpoint and raw data; it never claims the
history is complete. Resolving that error requires an operator-managed retry
with a larger internal boundary budget.

Ongoing games are tracked individually. Every later collection refreshes those
game IDs before scanning the newer creation-time window. This prevents an old
correspondence game finishing later from disappearing behind the watermark.
Known terminal statuses remove the follow-up; unfamiliar statuses remain under
observation. A missing or failed follow-up is recorded and does not silently
discard the unfinished game. Missing (404/410) game IDs stay in the follow-up
table for a later import, while the current import continues to newer available
games. Transient errors retain the current cursor for retry. Follow-up refreshes are intentionally separate
from historical enrichment/backfill of already completed games.

Page budgets bound game count per execution. Budgeted HTTP responses are streamed
under an operator-configured byte limit (16 MiB by default), bounded read
headroom, and a whole-response deadline. The worker
reserves response capacity before the request and charges the actual bytes read;
an interrupted attempt retains its conservative reservation. Durable run budgets
also bound remote requests, total bytes, games, and normalization work, while
workspace quotas limit monthly usage and unfinished jobs. These limits apply to
undated full, incremental, and backfill imports without silently shortening their
history. See [work budgets](work-budgets.md) for the configured defaults.

Exhaustion leaves the run incomplete with its checkpoint and captured sources
intact. An operator can raise the configured policy and resume the run; this
preserves its lifetime counters. A new UTC month refreshes monthly workspace
capacity but does not reset a run's budget or automatically resume it. Coverage
describes provider-visible games at the scan time, not deleted, private, or
otherwise unavailable games.

## Data upgrades and recovery

Schema versions remain SQL-only. Parser upgrades reprocess local sources.
Provider evidence upgrades use explicit backfill jobs. Each has its own identity
and progress. A job interruption after storing a response but before updating
its checkpoint is safe: the next execution reuses that job's retained successful
response and fetch observation, then repeats the uncheckpointed normalization
without another provider request. Source coverage and cursor advancement commit
together after validation. Analysis results remain independently versioned.
