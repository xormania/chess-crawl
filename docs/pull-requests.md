# Pull request guidelines

A PR must establish a reason for changing the repository and demonstrate that
the proposed change addresses that reason. A list of edited files or a green
CI badge alone does not establish either claim. Use the
[PR template](../.github/pull_request_template.md) for every PR, including
CI/test-only work and `dev` to `master` promotions.

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
substantive entry in [CHANGELOG.md](../CHANGELOG.md). This includes application
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

Work PRs target `dev`; promotion PRs target `master` from `dev`. Internal work
PRs must be integrated before handing the maintainer a final draft into `dev`.
See [CONTRIBUTING.md](../CONTRIBUTING.md) for setup and validation commands.

Before accepting a PR, verify that the reason is supported, the change follows
from that reason, the result is demonstrated, relevant checks pass, and the
changelog requirement is satisfied. Evidence quality is a review requirement,
not something a template heading or automated keyword check can establish.

The changelog workflow reports its result on PRs. Repository maintainers must
select `Changelog policy` as a required status check in the applicable branch
rules to make a failure block merging; adding a workflow does not itself alter
GitHub branch protection. Keep drafts open until the maintainer decides they
are ready, and resolve findings with supporting evidence.
