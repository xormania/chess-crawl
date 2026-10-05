# Contributing to chess-crawl

Start with the [README](README.md) for the project overview and
[PROJECT.md](PROJECT.md) for the top-level directory organization.

## Development setup

Use [Devbox](https://www.jetify.com/docs/devbox/installing-devbox) for the pinned
development toolchain. Install Devbox as your normal user; it also requires Nix
and offers to install it if missing. On Windows, install and run it inside WSL2,
with the checkout in the Linux filesystem (for example, `~/src/chess-crawl`).
Use Docker Desktop's WSL integration or a working Linux Docker engine with the
Compose v2 plugin. Docker is a separate host prerequisite, not a Devbox service.
The lock includes Linux x86-64/ARM64 and Apple Silicon macOS packages. Use the
[direct uv setup](#without-devbox) on Intel macOS.

Devbox provides Python 3.13, uv, Git, and PostgreSQL 18 tools such as `psql`,
`pg_dump`, and `pg_restore`. `devbox.lock` pins the tool packages; `uv.lock` pins
the Python dependencies. uv creates and manages the project's ignored `.venv`.
Devbox's Python and PostgreSQL plugins are disabled, so entering the environment
does not create another virtual environment or start a database. Docker Compose
provides the application services; tests require a separate disposable database.
uv is directed to Devbox's exact Python executable with interpreter downloads
disabled. Setup replaces an existing `.venv` if it uses a different interpreter.

Use your host Git for the initial checkout, then enter the pinned environment:

```bash
git clone https://github.com/xormania/chess-crawl.git
cd chess-crawl
git switch dev
git switch -c docs/my-change
devbox shell
devbox run setup
uv run chess-crawl-admin --help
```

Choose a descriptive branch name for the actual work. `devbox run setup` installs
the development group and API extra with the existing lock file. Run it after
pulling dependency changes. Shell entry does not install Python dependencies or
start services. Use `uv run ...` inside the shell; no manual `.venv` activation
is needed. You can also use `devbox run` commands from outside the shell:

```bash
devbox run setup
devbox run check
# After configuring the disposable test database below:
devbox run test -q
devbox run test -q -k provider
```

`check` runs Ruff, Mypy, Bandit, and administration-command help, stopping at the
first failure.
`test` forwards its arguments to pytest. The PostgreSQL tools use explicit
connection settings; they do not automatically connect to Compose's unpublished
database port. Follow the [operations guide](docs/postgresql-operations.md) for
container-based backup and restore commands.

Keep `devbox.json`, `devbox.lock`, and `uv.lock` in Git; keep `.devbox/` and `.venv/`
local. To upgrade a pinned tool, change its version in `devbox.json`, run
`devbox install`, and review both Devbox files together. Use uv for Python
dependency upgrades. CI still exercises Python 3.11 and 3.13 against real
PostgreSQL 18, and the package continues to support Python 3.11 or newer.
An additional Linux CI smoke check replaces an existing non-Devbox Python 3.13
virtual environment, runs setup and source checks,
exercises pytest argument forwarding without a database, and verifies that setup
preserves both lock files. It runs when the toolchain, dependency files, check
script, or its workflow changes, and on every push to `master` or manual Devbox
run. The existing CI jobs retain the full database
suite and Compose integration checks.

### Without Devbox

Devbox is the recommended contributor setup, but direct uv use remains
supported. Install Python 3.11+, Git, uv, and the PostgreSQL tools yourself, then:

```bash
uv sync --locked --group dev --extra api
```

The development group includes the dependencies needed by the offline API
tests. The API extra also installs the server runtime. Administration commands do not
automatically load `.env`; export settings as described in the guides.

## Shared implementation

Database connections and transactions are consolidated in
`src/chess_crawl/storage/db.py`; durable job and crawl-run state is managed in
`src/chess_crawl/jobs/state.py`. Shared public archive reads use
`src/chess_crawl/storage/queries.py`; paginated reports, owned export reads, and
export preparation admission use `src/chess_crawl/storage/api_views.py`. Extend
these shared implementations when working in those areas.

Use the shared transaction helper for mutations. Outermost writes explicitly
use READ COMMITTED before acquiring the shared migration gate; an externally owned
writable transaction at stronger isolation is rejected before mutation. Read
views use REPEATABLE READ and remain read-only. Executor writes also verify
session locks and job fencing tokens at the transaction boundary. Migrations
take the gate exclusively. Conflicting logical resources use `operation_lock`;
independent writes do not take an archive-wide exclusive lock. See
[execution and upgrades](docs/execution.md) for ownership and recovery.

SQL statements live in `storage/` or `jobs/state.py`; the storage-boundary test
checks this consolidation.

## Validate changes

Run database tests against a dedicated disposable PostgreSQL 18 server. The
fixture's administrator must be able to create and drop databases. Never point
these settings at an application database: tests deliberately create and remove
isolated databases. One local setup is:

```bash
export POSTGRES_PASSWORD="disposable-test-password"
docker run --name chess-crawl-test-postgres --detach \
  --publish 127.0.0.1:55432:5432 --env POSTGRES_PASSWORD postgres:18
docker exec chess-crawl-test-postgres pg_isready -U postgres
export CHESS_CRAWL_TEST_DATABASE_URL="postgresql://postgres@127.0.0.1:55432/postgres"
export CHESS_CRAWL_TEST_DATABASE_PASSWORD="$POSTGRES_PASSWORD"
```

Wait until `pg_isready` succeeds before testing. Remove the disposable server
with `docker rm --force --volumes chess-crawl-test-postgres` afterward.

Run these checks for code changes:

```bash
uv run ruff check .
uv run mypy .
uv run python .github/scripts/check_bandit.py
uv run python -m pytest -q
uv run chess-crawl-admin --help
```

Tests are offline by default, block outbound connections except their dedicated
PostgreSQL endpoint, and ignore inherited `CHESS_CRAWL_*` application settings.
The explicit `CHESS_CRAWL_TEST_DATABASE_URL` and separate test password configure
only the disposable database fixtures. Tests that exercise configuration set their own values
with `monkeypatch`. Shared database
fixtures live in `tests/conftest.py`; reusable builders live in
`tests/support.py`. Organize new cases by behavior, reuse these helpers, and
use fake clocks for retry and pacing tests. Do not make the default suite
depend on live providers. Live tests must use `@pytest.mark.live` and are skipped
unless explicitly enabled with `--run-live`. Only opted-in live tests may use
the caller's provider configuration and network access; unmarked tests remain
isolated even when that option is present. To run intentionally selected live
cases, use `uv run python -m pytest --run-live -m live` with appropriate provider
credentials and acquisition bounds.

CI runs the offline suite with `-m "not live and not slow"` on Python 3.11 and
3.13. Both versions run Mypy; Ruff and Bandit run once, on Python 3.11. CI
classifies the actual PR merge result against its base parent, including both
sides of renames:

| Changed files | Offline checks | Compose smoke |
| --- | --- | --- |
| Only Markdown under `docs/`, `AGENTS.md`, `PROJECT.md`, `CONTRIBUTING.md`, `CHANGELOG.md`, or the PR template | Omitted | Omitted |
| Only tests, the changelog workflow/checker, and the documentation above | Run | Omitted |
| Only `Dockerfile`, `compose.yaml`, `.dockerignore`, `.env.example`, `docker/mercure-entrypoint.sh`, or the CI Compose overlay | Omitted | Run |
| Application, dependencies, Python deployment helpers, shared CI, `README.md`, `LICENSE`, or any unrecognized path | Run | Run |
| Any promotion to `master`, or an empty merge diff | Run | Run |

Every push to `master` also runs the full offline suite, analysis, security
checks, Compose integration smoke, and Devbox smoke against the resulting
commit. This records checks on the actual branch commit after a merge; it works
with squash, fast-forward, and merge commits. Pushes to other branches rely on
their PR checks and do not start duplicate push runs.

For an on-demand check, open **Actions → CI → Run workflow**, select the branch,
and run it. Manual CI always runs the full offline and Compose checks, including
for documentation-only revisions. **Actions → Devbox → Run workflow** runs the
development-environment check independently. These controls become available
once the workflow changes reach the default branch. Changelog and promotion
source policies apply to PRs, where the source and target branches are known.

Concurrency separates PRs, master pushes, and manual runs by event and PR/ref.
A newer run replaces an older run in the same group, while a manual check cannot
cancel a master-push check or a different branch's manual check.

Mixed changes take the union of the applicable checks. Required checks retain
their names and report their scope even when no application work is needed. A scope-classification error fails those required
checks. `README.md` and `LICENSE` are packaging inputs, so they need full checks.
Tests remain the full offline suite; CI does not guess which individual tests
are affected by a source edit.

CI revalidates PR edits as well as new commits so retargeting cannot reuse an
obsolete promotion check. Title and description edits also rerun scoped checks;
skipping the required jobs on those events could hide a previous failure.
Mypy caches are separated by Python version; Mypy still executes and validates
its incremental data. These caches primarily help subsequent commits and reruns
within a PR because GitHub isolates PR cache entries. uv's remote dependency
cache is disabled: its pruned cache retained metadata while the prebuilt wheels
were downloaded again, with no measured install benefit. The pinned, locked
install still runs each time.

Docker keeps dependency layers separate from application sources and builds the
shared Compose image once, while pulling PostgreSQL and Mercure concurrently. CI's Compose
overlay checks startup readiness every second, preserving the normal health
interval, probes, failure budgets and service dependencies. The smoke job
compares the complete rendered base/CI configurations and rejects any other
change introduced by that overlay. Remote Docker cache export is deliberately
omitted: measured setup and transfer overhead exceeded the benefit for this
small image. Caches accelerate work; they never stand in for a successful check.

### Behavior and performance evidence

Normal CI exercises real temporary Git merges for scope and changelog policy,
executes the workflow's shell guards (including both failures in concurrent
build/pull), and tests the Compose overlay contract. It also retains the full
application suite and real API/worker/private-Mercure smoke whenever selected.

Bandit is pinned in the development dependency group and scans every Python
file under `src/`, `scripts/`, `docker/`, and `.github/scripts/`, including its
own wrapper. The wrapper rejects findings at every severity/confidence level,
reported scan errors, missing target directories, and incomplete file coverage.
It uses the checked-in `pyproject.toml` configuration and the existing required
offline check; no additional required-check setting is needed. Ruff's optional
`S` rules are not also enabled.

Review scanner findings before suppressing them. Use a rule-specific comment
with its reason (for example, `# nosec B608 # Only literal columns; values are bound`).
Existing exceptions document fixed SQL fragments/columns, trusted workflow or
operator subprocess arguments, and operator-selected disposable smoke endpoints.
Do not use blanket suppressions or a baseline to conceal unreviewed findings.
Pytest fixtures under `tests/` are outside the scan. The executable Compose
smoke test remains scanned, with only its assertion rule exempted; it refuses
to run with `-O` or `PYTHONOPTIMIZE` because those options remove its checks.
Behavior tests execute the actual timed CI command against clean, vulnerable,
unparseable, and incomplete fixture trees, checking that failures stay failures.

Each selected job writes stage timings to its job summary and uploads a
`ci-performance-*` artifact retained for 14 days. JSON samples include the
revision, run attempt, Python version, runner and check variant. Offline jobs
also retain JUnit results and print the 15 slowest test durations. Failures keep
the command's exit status and are recorded as failures, never faster successes.

Use `.github/scripts/ci_performance.py` to repeat the same command into separate
baseline/candidate directories, then compare their medians. For example:

```bash
python .github/scripts/ci_performance.py measure --label mypy --output-dir /tmp/ci-baseline -- uv run --no-sync mypy . .github/scripts
python .github/scripts/ci_performance.py measure --label mypy --output-dir /tmp/ci-candidate -- uv run --no-sync mypy . .github/scripts
python .github/scripts/ci_performance.py compare --baseline /tmp/ci-baseline --candidate /tmp/ci-candidate
```

Collect several samples with the same interpreter/runner and cache condition.
Hosted wall times are informational by default because shared-runner variance
is material. An explicit `--fail-on-regression` comparison can enforce a budget
using both `--max-regression-percent` and `--min-regression-seconds`; keep generous
limits and separate cold and warm measurements. Missing or incompatible samples
must not be treated as proof of a speedup.

A focused integration/workflow run and a coverage run are available when useful:

```bash
uv run python -m pytest -q -m "workflow and not live and not slow"
uv run python -m pytest -q --cov=chess_crawl --cov-report=term-missing
```

Documentation-only changes do not require the entire application test suite.
Check executable examples and affected claims, and state which checks ran and
which were skipped. Follow the [backend smoke-check instructions](docs/backend.md#offline-compose-smoke-check)
for container, worker, or event integration changes; the smoke check writes
synthetic data and belongs on a disposable database.

## Branches and pull requests

Work branches target `dev`. Promotion PRs use this repository's `dev` branch
as their source and `master` as their target when the maintainer requests a
promotion. If work is split into internal PRs, integrate it before presenting
the final draft PR into `dev`. Leave delivery and promotion PRs unmerged until
the maintainer requests the merge. Publish through work branches rather than
pushing directly to `dev` or `master`.

Use the [PR template](.github/pull_request_template.md) to explain the reason,
the change, and the evidence described below.

## Why is this PR needed?

Describe the concrete problem, missing capability, or documentation gap and
who or what it affects. State the desired behavior. Support the reason with
reviewable evidence: a minimal reproduction, failing test output, an issue or
requirement, a measured result, a log excerpt, or a precise source/document
reference. Identify the baseline commit or version when behavior depends on it.

A feature request can establish a need without pretending an existing feature
is broken. A cleanup should identify the duplication, inconsistency, or
maintenance cost it removes. A documentation PR should identify the inaccurate
or missing guidance. An issue link needs a short explanation of the relevant
claim; it is not a substitute for one.

## What does the PR change?

Explain the mechanism and resulting behavior in terms a reviewer can compare
with the problem. Describe affected interfaces, configuration, storage, or
operator steps when applicable. Call out migrations, compatibility changes,
and material limitations. Keep unrelated cleanup in a separate PR.

## What proves it addresses the reason?

Tie each material claim to a result that exercises it. Include the commands or
reproduction steps, relevant output, and links to CI runs or artifacts where
available. Identify the tested revision and distinguish local checks from CI.
Summarize long logs and link to the complete evidence; redact secrets.

| Change | Evidence of the need | Evidence of the result |
| --- | --- | --- |
| Bug fix | Failing regression or reproducible incorrect behavior on the baseline. | The same scenario passes with the fix; relevant regression checks pass. |
| Feature | Requested capability and explicit acceptance conditions. | An example, integration check, or test demonstrates those conditions. |
| Refactor | Precise examples of duplication, inconsistent ownership, or another concrete maintenance problem. | Inspection shows the new ownership; relevant behavior checks demonstrate preservation. |
| Documentation | Missing/inaccurate instructions with source references. | Commands, links, and changed claims checked against the implementation. |
| CI or tests | A gap, failure, unreliable check, or missing coverage. | A representative run or scenario shows that the changed check detects or resolves it. |
| Promotion | Included work and its readiness for `master`. | Linked delivery PR evidence and checks of the promotion merge result. |

Before/after results are preferred where the baseline can be reproduced.
When it cannot, say why and provide the strongest available evidence without
claiming an unobserved result. State untested areas or environment limitations.
Do not claim a performance gain without measurements or a fixed failure based
only on an unrelated green suite. Reviewers must be able to trace the chain
from reason, through change, to demonstrated result.

## Changelog requirement

Every PR that changes anything beyond CI and/or tests must add or update a
substantive entry in [CHANGELOG.md](CHANGELOG.md). This includes application
code, dependency/packaging changes, configuration, deployment, documentation,
and contribution policy. A PR mixing those changes with CI/tests still needs
an entry. CI/test-only PRs may omit the entry, but must explain the exemption
in the PR's Changelog section.

Add the entry under `Unreleased`, using `Added`, `Changed`, `Fixed`, `Removed`,
or `Security` as appropriate. Describe the observable behavior or contributor
impact, not a list of commits. Include compatibility or migration instructions
when needed, and link a PR or issue when available. Do not invent release
versions or dates. When a release is actually prepared, move its entries under
the chosen version and date and retain an `Unreleased` section for later work.

The `Changelog policy` CI check classifies a PR as CI/test-only only when
**every changed path** is in one of these locations:

- `tests/`
- `.github/workflows/`
- `.github/actions/`
- `scripts/compose_smoke.py`

Both the old and new path must qualify for a rename. Shared files such as
`pyproject.toml` and `uv.lock` are not exempt just because a change helps tests.
New CI/test support paths need a reviewed policy/check update or a changelog
entry. A removed changelog or a rename without added content does not satisfy
the check. The check inspects the pinned PR merge and its base parent, without
paginated or mutable GitHub file-list reads. It verifies the presence of a content update;
reviewers verify that the entry accurately describes the change.

A `dev` to `master` promotion carries the changelog entries already accumulated
on `dev`; do not add a duplicate entry merely to promote them. Review the full
PR diff against its target. If the promotion has no changes outside CI/tests,
the same exemption applies.

## Review and merge

Before accepting a PR, verify that the reason is supported, the change follows
from that reason, the result is demonstrated, relevant checks pass, and the
changelog requirement is satisfied. Evidence quality is a review requirement;
a template heading or green CI badge alone cannot establish it.

The changelog workflow reports its result on PRs. Making a failure block
merging requires selecting `Changelog policy` as a required status check in
the applicable GitHub branch rules; adding the workflow does not change those
settings. Resolve review findings with supporting evidence.

## Commit content

Keep credentials, local archives, provider dumps, generated exports, and
temporary validation output out of commits. Use concise, project-focused
commit messages such as `fix: preserve parser replay progress` or
`docs: clarify archive setup`. Contributions use the project's
[Apache-2.0 license](LICENSE).
