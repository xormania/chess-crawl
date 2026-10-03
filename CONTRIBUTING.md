# Contributing to chess-crawl

Start with the [README](README.md) for what the application does, the
[CLI guide](docs/cli.md) for archive operations, and the
[backend guide](docs/backend.md) for the API, worker, Compose, and Mercure
contracts. The [PR guidelines](docs/pull-requests.md) define the required
problem evidence, explanation of the change, proof of the result, and changelog
policy. Automated contributors must also follow [AGENTS.md](AGENTS.md).

## Development setup

Use Python 3.11 or newer, Git, and [uv](https://docs.astral.sh/uv/).
CI exercises Python 3.11 and 3.13. Docker with the Compose plugin is needed
only for the container stack and its smoke check.

```bash
git clone https://github.com/xormania/chess-crawl.git
cd chess-crawl
git switch dev
git switch -c docs/my-change
uv sync --locked --group dev
uv run chess-crawl --help
```

Choose a descriptive branch name for the actual work. Install `--extra api`
as well when running the HTTP server outside Docker:

```bash
uv sync --locked --group dev --extra api
```

The development group includes the dependencies needed by the offline API
tests. The API extra also installs the server runtime. CLI commands do not
automatically load `.env`; export settings as described in the guides.

## Keep implementation ownership clear

All paths below are relative to `src/chess_crawl/`.

| Area | Owner and responsibility |
| --- | --- |
| CLI and HTTP adapters | `cli.py` and `api/` translate input/output and call shared application services. |
| Shared operations | `application/` validates submissions, applies bounds and idempotency, and returns read snapshots. |
| SQLite access | `storage/db.py` owns connections, read/write access, transactions, and savepoints. |
| Archive storage | `storage/` owns SQL for raw payloads, normalized records, provenance, acquisition, and events. `storage/queries.py` supplies report/export reads. |
| Durable job/run state | `jobs/state.py` owns creation, claims, transitions, checkpoints, recovery, and counters. |
| Acquisition execution | `jobs/runner.py` executes jobs; `jobs/discovery.py` applies crawl bounds; `jobs/worker.py` manages the serial worker. |
| Provider behavior | `providers/` contains provider-specific endpoints, parsing, capabilities, and request policies. |
| Event delivery | `events/` publishes committed outbox events to Mercure. |

Use `open_database(path)` for read-only access and
`open_database(path, writable=True)` for writes. Use the shared `transaction`
context manager and `atomic` decorator; nested mutations use savepoints. Keep
SQL inside `storage/` or `jobs/state.py`, and keep network requests outside
database transactions.

Preserve raw responses and fetch logs before normalization. Keep users and
games provider-scoped, and keep acquisition bounded and serial. Use documented
public APIs; respect provider cooldowns. Account-status data remains a neutral
provider observation, not an accusation. Do not infer that matching usernames
across providers identify the same person.

## Validate the reason for the change

Choose a check that directly exercises the problem or requested capability.
For a bug fix, show the failing behavior before the fix and the corrected
behavior afterward when reproducible. For a feature, demonstrate the requested
acceptance behavior. For documentation, verify the changed commands, links,
and claims against the implementation. Record commands, results, and any
limits in the PR; passing unrelated tests is not proof of the stated fix.

Run these checks for code changes:

```bash
uv run ruff check .
uv run mypy .
uv run python -m pytest -q
uv run chess-crawl --help
```

Tests are offline by default and block outbound sockets. Shared database
fixtures live in `tests/conftest.py`; reusable builders live in
`tests/support.py`. Organize new cases by behavior, reuse these helpers, and
use fake clocks for retry and pacing tests. Do not make the default suite
depend on live providers. Live tests must be explicitly marked and skipped by
default.

CI runs the offline suite with `-m "not live and not slow"`. A focused CLI
workflow run and a coverage run are available when useful:

```bash
uv run python -m pytest -q -m "workflow and not live and not slow"
uv run python -m pytest -q --cov=chess_crawl --cov-report=term-missing
```

Documentation-only changes do not require the entire application test suite.
Check executable examples and affected claims, and state which checks ran and
which were skipped. Follow the [backend smoke-check instructions](docs/backend.md#offline-compose-smoke-check)
for container, worker, or event integration changes; the smoke check writes
synthetic data and belongs on a disposable archive.

## Submit a pull request

Work branches target `dev`. `master` receives promotion PRs from `dev`.
Keep changes focused; if work is split into internal PRs, integrate it before
presenting the final draft PR into `dev`. The maintainer decides when delivery
and promotion PRs are ready to merge. Do not push directly to `dev` or `master`,
or rewrite published history.

Use the [PR template](.github/pull_request_template.md) and follow the
[PR guidelines](docs/pull-requests.md). Every PR must explain why it is needed,
what it changes, and how the evidence demonstrates that the change addresses
the reason. Update [CHANGELOG.md](CHANGELOG.md) unless the entire PR is confined
to CI and/or tests. Documentation changes and mixed PRs require an entry.

Keep credentials, local archives, provider dumps, generated exports, and
temporary validation output out of commits. Use concise, project-focused
commit messages such as `fix: preserve parser replay progress` or
`docs: clarify archive setup`. Contributions use the project's
[Apache-2.0 license](LICENSE).
