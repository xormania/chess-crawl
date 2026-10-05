# Agent orientation

chess-crawl is a Python project for collecting, preserving, and analyzing
chess data. Product operations use an HTTP API, with background workers and
Mercure updates. Dedicated administration commands maintain the archive.

This file provides orientation and navigation. The maintainer's current
request sets the task.

## Start here

- [README.md](README.md): project overview, current capabilities, and setup.
- [CONTRIBUTING.md](CONTRIBUTING.md): development setup, shared implementation,
  validation, branches, pull requests, and changelog requirements.
- [Operations guide](docs/cli.md): migrations, readiness, and archive maintenance.
- [Backend guide](docs/backend.md): API, workers, Mercure, and deployment.

## Find the implementation

See [PROJECT.md](PROJECT.md) for the top-level directories and what belongs
in them.

## Working guidance

Read the documentation and implementation relevant to the current task.
Existing architecture describes the starting point for a change.

When documentation and implementation disagree, identify the discrepancy.
Distinguish observed behavior from assumptions when explaining findings.
