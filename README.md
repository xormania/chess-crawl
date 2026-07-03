# chess-crawl

Provider-neutral, raw-first, local-first chess data archive.

Status: Phase 3 local archive workflow. The package can initialize and inspect
a local SQLite archive, fetch bounded slices from Chess.com and Lichess public
APIs, store raw provider responses before normalization, run serial durable
jobs, perform bounded opponent discovery, report on normalized local data, and
export non-raw JSONL/CSV views.

## Principles

- Provider-neutral: Chess.com and Lichess share one archive schema, but account
  identity is always provider-scoped. Matching usernames across providers are
  never treated as the same person.
- Raw-first: later fetches/imports should store provider response bytes in
  `raw_payloads` before normalization. Normalized rows are traceable back to raw
  bodies through `source_records`.
- Local-first: data stays in the local SQLite file the operator chooses.
- Public API only: no scraping, no private data access, no undocumented
  endpoints.
- No cheating accusations: provider status labels are stored as provider-supplied
  neutral facts only; this tool does not infer or assert misconduct.

## Implemented Commands

From an installed environment:

```bash
chess-crawl init --db ./data/chess-crawl.sqlite
chess-crawl provider list
chess-crawl db info --db ./data/chess-crawl.sqlite

chess-crawl fetch user chess.com SameName --db ./data/chess-crawl.sqlite
chess-crawl fetch user lichess SameName --db ./data/chess-crawl.sqlite
chess-crawl fetch stats chess.com SameName --db ./data/chess-crawl.sqlite
chess-crawl fetch archives chess.com SameName --db ./data/chess-crawl.sqlite
chess-crawl fetch games chess.com SameName --month 2024-01 --db ./data/chess-crawl.sqlite
chess-crawl fetch games lichess SameName --since 2024-01-01 --until 2024-02-01 --limit 50 --db ./data/chess-crawl.sqlite

chess-crawl query user chess.com SameName --db ./data/chess-crawl.sqlite
chess-crawl query game lichess lichgame1 --db ./data/chess-crawl.sqlite
chess-crawl query raw --provider chess.com --limit 10 --db ./data/chess-crawl.sqlite

chess-crawl crawl opponents chess.com SameName \
  --depth 1 --max-users 25 --max-games 200 --max-jobs 100 \
  --since 2024-01 --until 2024-02 \
  --db ./data/chess-crawl.sqlite

chess-crawl jobs status --db ./data/chess-crawl.sqlite
chess-crawl jobs list --db ./data/chess-crawl.sqlite
chess-crawl jobs show 1 --db ./data/chess-crawl.sqlite
chess-crawl jobs resume --db ./data/chess-crawl.sqlite

chess-crawl report summary --db ./data/chess-crawl.sqlite
chess-crawl report user chess.com SameName --db ./data/chess-crawl.sqlite
chess-crawl report opponents chess.com SameName --db ./data/chess-crawl.sqlite
chess-crawl report games-by-month --provider chess.com --db ./data/chess-crawl.sqlite

chess-crawl export games --format jsonl --output games.jsonl --db ./data/chess-crawl.sqlite
chess-crawl export users --format jsonl --output users.jsonl --db ./data/chess-crawl.sqlite
chess-crawl export graph --format csv --output graph.csv --db ./data/chess-crawl.sqlite
```

From the source tree, use:

```bash
PYTHONPATH=src python3 -m chess_crawl.cli init --db ./data/chess-crawl.sqlite
PYTHONPATH=src python3 -m chess_crawl.cli provider list
PYTHONPATH=src python3 -m chess_crawl.cli db info --db ./data/chess-crawl.sqlite
PYTHONPATH=src python3 -m chess_crawl.cli fetch games chess.com SameName --month 2024-01 --db ./data/chess-crawl.sqlite
PYTHONPATH=src python3 -m chess_crawl.cli jobs status --db ./data/chess-crawl.sqlite
```

`python -m chess_crawl` is wired to the same CLI when the package is importable.

## End-to-End Local Workflow

```bash
chess-crawl init --db ./data/chess-crawl.sqlite
chess-crawl provider list

# Direct bounded acquisition.
chess-crawl fetch user chess.com SameName --db ./data/chess-crawl.sqlite
chess-crawl fetch games chess.com SameName --month 2024-01 --db ./data/chess-crawl.sqlite

# Bounded opponent discovery. All caps are required.
chess-crawl crawl opponents lichess SameName \
  --depth 1 --max-users 20 --max-games 100 --max-jobs 80 \
  --since 2024-01-01 --until 2024-02-01 \
  --db ./data/chess-crawl.sqlite

# Resume after interruption or inspect durable work.
chess-crawl jobs status --db ./data/chess-crawl.sqlite
chess-crawl jobs resume --db ./data/chess-crawl.sqlite

# Read and export only local normalized data.
chess-crawl report user lichess SameName --db ./data/chess-crawl.sqlite
chess-crawl export games --format jsonl --output games.jsonl --db ./data/chess-crawl.sqlite
```

## Raw-First Fetch Behavior

Successful `200` provider responses are written to `raw_payloads` before
normalization runs. Fetch attempts, including `304`, `404`, `410`, `429`, and
retry attempts, are recorded in `fetch_logs`; body-less statuses do not create
raw payload rows. Normalization is re-runnable from stored raw bytes.

Chess.com conditional requests reuse stored ETag/Last-Modified validators when
available. Lichess game export is requested as NDJSON and is bounded by the
required date window plus `--limit`.

## Provider Boundaries

User identity is provider-scoped. A Chess.com username and a Lichess username
with the same spelling are stored as different accounts and are never assumed to
be the same human.

Only documented public APIs are called. There is no scraping, no undocumented
endpoint use, no rate-limit evasion, and no automated cheating accusation
logic. Provider account-status labels are stored only as provider-supplied
neutral facts.

Lichess can use an optional bearer token from `CHESS_CRAWL_LICHESS_TOKEN`; it is
not required and request headers are not written to raw payloads or fetch logs.

## Durable Jobs And Bounded Crawls

Durable work is stored in `discovery_jobs` and grouped by `crawl_runs` for
crawls. Job states are `pending`, `in_progress`, `done`, `error`, `skipped`, and
`blocked`. `jobs resume` resets stale `in_progress` jobs and unblocks `blocked`
jobs before driving the same serial runner.

Opponent crawl is one discovery strategy. It is provider-scoped and bounded by
explicit `--depth`, `--max-users`, `--max-games`, `--max-jobs`, `--since`, and
`--until` arguments. Depth `0` fetches the seed/source user. Depth `1` discovers
and fetches opponents found from the seed's normalized games. The strategy reads
opponents from `game_participants`, writes `discovery_edges`, and never crosses
providers.

## Reports And Exports

Reports are factual read-side summaries over normalized tables. They handle
`outcome IS NULL` as unfinished games and keep provider identity in every query.
Provider account-status labels, when shown, are rendered as provider-supplied
facts only.

Exports are bounded local views. Games and users export JSONL; graph edges export
CSV. Raw payload bodies are not exported by default. Provider is preserved in
every row, and graph exports do not merge accounts across providers.

## Known Limitations

- Crawl jobs use a pragmatic serial path: each `crawl_opponents` job fetches the
  bounded games for that user and then expands from normalized participants.
  This keeps the current implementation small; the plan's fuller fan-out model
  remains future work.
- Lichess NDJSON responses are still buffered for each bounded request before
  normalization. Incremental per-line checkpointing is not implemented.
- Chess.com monthly archives are the public API's natural game unit. A crawl can
  stop scheduling more work at `--max-games`, but a fetched month may contain
  more games than the remaining normalized-game budget.
- Game normalization captures core identity, participants, ratings,
  variant/time class, timestamps, outcome, status, and PGN where present. It
  does not implement engine analysis, cheating detection, or event containers.
- Chess.com single-game-by-id is intentionally not implemented because the
  public API has no such endpoint; fetch the owning monthly archive instead.
- Exports currently implement JSONL for games/users and CSV for graph edges.
- Default tests remain offline and fixture-based.
