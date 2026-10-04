# Game and timing evidence

Migration `0006_game_evidence` adds immutable normalized revisions. Existing
games and source bodies remain intact. Replay existing game payloads locally to
populate evidence; no provider download is required. SQL migration and replay
are separate operations so migrating an archive never depends on a provider.

## Stored representations

`game_versions` retains native JSON metadata, every PGN header, ordered header
items (including duplicate tags), starting position, time-control periods,
normalization version, interpretation status, and issues. The PGN text itself
is represented by ordered lexical records in `game_pgn_tokens`, rather than
another standalone PGN copy. Native `moves` strings are converted to the same
records; `move_text_origin` distinguishes their origin from supplied PGN without
overwriting any native metadata key.

`game_move_nodes` preserves a tree of occurrences. Node zero is the starting
position. Parent and variation indices distinguish alternatives from the
played line; comments, variation preambles, NAGs and arbitrary bracket
annotations remain queryable. Provider analysis entries retain their payload
and source pointer alongside PGN annotations, without equating their meanings.

`game_clock_observations` distinguishes remaining-clock readings from explicitly
reported elapsed-move time. PGN `[%clk]` and `[%emt]` retain their complete
annotation and decimal resolution; Lichess JSON `clocks` uses centiseconds, as
documented in its [export specification](https://github.com/lichess-org/api/blob/master/doc/specs/tags/games/api-games-user-username.yaml).
Values use PostgreSQL NUMERIC and Python Decimal; read helpers return decimal
strings so JSON encoding cannot silently lose precision. Missing observations
produce no row; reported zero produces a row with zero; malformed annotations
produce an invalid observation; an extra array element has unmapped status.

PGN and API readings are separate observations even when they disagree.
`game_derived_timings` is reserved for explicitly versioned calculations with
their input observations and assumptions. This foundation does not infer
thinking time or claim that source clocks reveal latency, premoves, or whether
an increment has already been applied.

`game_version_sources` associates a normalized revision with all observed source
bodies and positions inside those bodies. An identical revision is reused on
replay. Changed native metadata, clocks, or notation creates another revision.
The current revision follows fetch evidence, including a previously seen body
returning later. Old source replay cannot overwrite more recent game metadata.
An already acquired game still refreshes when that previously stored body is
observed again as current; this does not consume another run/game allowance.
Current time and opening facts replace prior values when their native fields
are explicitly supplied, including reported nulls. Omitted fields remain sparse
and preserve known values; malformed time/opening values remain in source
evidence without being treated as explicit nulls. A known live state clears a
stale completion time when no end is supplied; supplied result evidence clears stale
participant wins/losses. The `games-normalizer-v5` identity permits an offline
upgrade to repair these current facts without changing immutable evidence
versions or fetching source bodies again.
Database triggers reject updates to normalized revisions and their move, token,
or observed clock contents. Individual evidence deletion is also rejected.
Their initial evidence must be inserted in the version creation transaction;
later inserts cannot silently change a committed version's contents.
An explicitly removed, unreferenced version can cascade to its contents;
downstream working-set references are responsible for protecting selected
versions from removal. Additional source associations and derived results use
separate records.

## Parser and interpretation boundaries

The pinned [pgn-read 2.7.3](https://github.com/RogerMarsh/pgn-read) dependency is
BSD-3-Clause and provides PGN grammar and standard-chess legal replay. Crawl
does not contain a custom rules engine. Standard chess and standard-chess
custom FEN positions have interpreted SAN/UCI and before/after FEN when legal.

Chess960 and other variants preserve their move tree, clocks, headers,
annotations, starting FEN, and lexical evidence. Their interpretation status
is `unsupported`; UCI, derived board positions, and position hashes remain
NULL. A future variant-capable rules adapter can fill a new normalized version
by replaying existing sources. This is an explicit limitation of the initial
permissively licensed parser, not an assertion that variant games lack data.

Malformed and unknown syntax is retained with source character offsets and
issues; it is never certified as fully interpreted. A complete raw payload
can be marked normalized while its individual game reports partial or
unsupported interpretation: these statuses describe different operations.

`fen-state-v1` position keys hash variant plus the first four FEN fields.
Counters remain in full FEN and history remains in the tree. This conservative
key retains the supplied en-passant square even if a capture is unavailable,
so it may split otherwise equivalent positions. It does not claim complete
transposition equivalence. Hashes are index aids; exact matching also compares
the recorded canonical state.

## Reads and export

`storage.game_evidence.read_game_version(conn, game_id, version_id=None)` returns
the requested game revision, nodes, exact clock values, derived timing records,
and provenance. Omitting a version selects the current revision. A version from
another game is never returned.

`export_game_version_pgn` reconstructs notation from database tokens without
reading the compressed source. Headers, comments, variations and unfamiliar
elements survive; whitespace is normalized. Partial/unsupported export requires
explicit `allow_partial=True`, so a caller can display its interpretation
status. PGN synthesized from JSON `moves` uses a generated unknown result marker.
The original provider result remains in native metadata and normalized game
records; the generated marker must not be treated as source-reported evidence.

## Workload measurements

Run `uv run python scripts/benchmark_game_evidence.py --iterations 100` for
an offline parser sample. Use a representative PGN with `--pgn FILE`; the
small checked-in evidence fixture exercises variations and clock annotations.

Add `--database --games 1000` with the dedicated test administrator settings
from CONTRIBUTING.md and the appropriate application transport/password
settings. It creates and drops only a randomly named benchmark database and
measures import, idempotent replay, revision reads, position lookup, and total
table/index bytes. The input is repeated under distinct game IDs, so those
numbers characterize this fixture, not the full diversity of real archives.
Compare the same data, interpreter, PostgreSQL version, and machine. This
change establishes measurements; it does not claim an unmeasured speedup.
