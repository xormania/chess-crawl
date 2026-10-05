#!/bin/sh
# Run source checks from any working directory. Database tests run separately.
set -eu
cd "$(dirname "$0")/.."

uv run --locked ruff check . .github/scripts
uv run --locked mypy . .github/scripts
uv run --locked python .github/scripts/check_bandit.py
uv run --locked chess-crawl-admin --help
