# Changelog

Notable changes are recorded here. Entries under `Unreleased` have not been
assigned a release version. See [CONTRIBUTING.md](CONTRIBUTING.md#changelog-requirement) for the update policy.

## Unreleased

### Added

- Shared application services for bounded imports and opponent crawls, with
  idempotent submissions and consistent archive snapshots. ([#7](https://github.com/xormania/chess-crawl/pull/7))
- An authenticated JSON API for asynchronous submissions, job/run state,
  worker health, and paginated archive reads. ([#9](https://github.com/xormania/chess-crawl/pull/9))
- A serial worker with exclusive archive ownership, restart recovery, bounded
  retries, and persistent provider cooldowns. ([#8](https://github.com/xormania/chess-crawl/pull/8))
- Transactional job/run events, a durable outbox, and private Mercure delivery.
  ([#10](https://github.com/xormania/chess-crawl/pull/10))
- A standalone Docker Compose backend with its own Mercure hub, development
  credentials, health checks, and persistent SQLite archive. API and event
  integration remain independent of an external web application's deployment.
  ([#11](https://github.com/xormania/chess-crawl/pull/11), [#12](https://github.com/xormania/chess-crawl/pull/12))
- Contributor setup, PR guidelines and a PR template requiring evidence of the
  need and of the result; a changelog update requirement for all PRs beyond
  CI/test-only changes.

### Changed

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

- Parser upgrades refresh previously normalized games without double-counting
  run attribution; opponent discovery uses the selected run's games, and
  provider pacing persists across acquisition calls. ([#7](https://github.com/xormania/chess-crawl/pull/7))
- Worker heartbeats tolerate transient SQLite writer contention, and CLI
  acquisition obtains exclusive ownership before opening a writable archive.
  ([#8](https://github.com/xormania/chess-crawl/pull/8))
- Claimed job snapshots include the persisted revision after state-change
  triggers, matching API and event reads. ([#10](https://github.com/xormania/chess-crawl/pull/10))

### Removed

- Obsolete `proj/` planning and audit records.

## 0.1.0 — 2026-07-03

- Initial alpha prerelease. See the [published release](https://github.com/xormania/chess-crawl/releases/tag/v0.1.0).
