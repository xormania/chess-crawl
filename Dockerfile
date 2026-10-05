# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.12.19 AS uv

FROM python:3.13-slim AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock ./
# Reuse third-party dependencies when only application source changes.
RUN uv sync --locked --no-dev --extra api --no-install-project
COPY README.md LICENSE ./
COPY src ./src
RUN uv sync --locked --no-dev --extra api --no-editable

FROM python:3.13-slim AS runtime
RUN groupadd --gid 10001 chess-crawl \
    && useradd --uid 10001 --gid 10001 --create-home chess-crawl
COPY --from=builder /app/.venv /app/.venv
COPY docker/healthcheck.py /app/docker/healthcheck.py
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CHESS_CRAWL_WORKER_IDENTITY_FILE=/tmp/chess-crawl-worker.json
WORKDIR /app
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "chess_crawl.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
