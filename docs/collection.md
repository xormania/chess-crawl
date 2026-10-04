# Incremental and full-history collection

The collector runs asynchronous `fetch_user_games` jobs. Set their stored `collection_mode`
to `full`, `incremental`, or `backfill` to use resumable collection rather than
the legacy bounded fetch. Opening a profile or reading archived games never
starts a collection request.

Each execution handles at most `batch_size` monthly archives (default 4, maximum
100), one Lichess stream page, or `batch_size` preserved source pages or
unfinished-game follow-ups. The
same job returns to pending until its durable checkpoint is complete. Provider
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
source. A `backfill` explicitly refreshes even sealed months. Optional `months`
selects a list of `YYYY/MM` resources; absent requested months are recorded as
missing. Include an `upgrade_id` in backfill parameters to connect the request
to a provider-data upgrade. Network acquisition is not performed inside a SQL
schema migration. A listed monthly source returning 404/410 is recorded as
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
can be reused between checkpoints. Explicit `since_ms` and `until_ms` restrict a window; `since` and
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
the configured provider evidence settings. `page_size` defaults to 1000 and
allows 1–10000. A supplied `max_games` is used as the initial
page budget when `page_size` is absent, not as a total full-history game cap.
The next upper bound overlaps the oldest
timestamp by one millisecond. The limit grows to include already observed
boundary games plus new games. This avoids losing games sharing the same
timestamp. It can temporarily exceed `page_size`, up to `max_page_size`
(default 10000, maximum 100000). If a tied boundary exceeds that budget, the job
reports an error and retains its checkpoint and raw data; it never claims the
history is complete. Increase the budget and resume to resolve the boundary.

Ongoing games are tracked individually. Every later collection refreshes those
game IDs before scanning the newer creation-time window. This prevents an old
correspondence game finishing later from disappearing behind the watermark.
Known terminal statuses remove the follow-up; unfamiliar statuses remain under
observation. A missing or failed follow-up is recorded and does not silently
discard the unfinished game. Missing (404/410) game IDs stay in the follow-up
table for a later import, while the current import continues to newer available
games. Transient errors retain the current cursor for retry. Follow-up refreshes are intentionally separate
from historical enrichment/backfill of already completed games.

The bounded responses are currently read into memory by the existing HTTP
client. The page budget bounds game count; it is not a byte-size limit. Very
large annotated games still require a future streamed/byte-budget acquisition
path. Coverage describes provider-visible games at the scan time, not deleted,
private, or otherwise unavailable games.

## Data upgrades and recovery

Schema versions remain SQL-only. Parser upgrades reprocess local sources.
Provider evidence upgrades use explicit backfill jobs. Each has its own identity
and progress. A job interruption after storing a response but before updating
its checkpoint is safe: the next execution reuses/deduplicates evidence and
repeats the uncheckpointed unit. Source coverage and cursor advancement commit
together after validation. Analysis results remain independently versioned.
