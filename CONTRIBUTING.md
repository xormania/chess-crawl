# Contributing to chess-crawl

Start with the [README](README.md) for the project overview and
[PROJECT.md](PROJECT.md) for the top-level directory organization.

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

## Shared implementation

Database connections and transactions are consolidated in
`src/chess_crawl/storage/db.py`; durable job and crawl-run state is managed in
`src/chess_crawl/jobs/state.py`. Report and export reads use
`src/chess_crawl/storage/queries.py`. Extend these shared implementations when
working in those areas.

SQL statements live in `storage/` or `jobs/state.py`; the storage-boundary test
checks this consolidation.

## Validate changes

Run these checks for code changes:

```bash
uv run ruff check .
uv run mypy .
uv run python -m pytest -q
uv run chess-crawl --help
```

Tests are offline by default, block outbound connections, and ignore inherited
`CHESS_CRAWL_*` settings. Tests that exercise configuration set their own values
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

CI runs the offline suite with `-m "not live and not slow"`. It revalidates
PR edits as well as new commits so retargeting cannot reuse an obsolete
promotion check; title and description edits also rerun CI. A focused CLI
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
the check. The check verifies the presence of a content update; reviewers
verify that the entry accurately describes the change.

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
