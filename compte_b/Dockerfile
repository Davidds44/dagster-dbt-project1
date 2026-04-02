# Build from repo root of the compte_b package (directory containing pyproject.toml).
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Dependency layer (cached when lockfile unchanged)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Application source
COPY src ./src
COPY analysis ./analysis
COPY streamlit ./streamlit
RUN mkdir -p data/input data/raw

# NOTE: `./data:/app/data` is mounted via docker-compose, so we don't bake
# `data/input` into the image (keeps the image smaller).

RUN uv sync --frozen --no-dev

# Reduce image size by removing package manager caches.
RUN rm -rf /root/.cache/uv /root/.cache/pip /tmp/*

ENV COMPTE_B_DUCKDB_PATH=/app/data/compte_b.duckdb \
    COMPTE_B_SQLITE_PATH=/app/data/partage.sqlite \
    DOWNLOADS_DIR=/downloads

CMD ["bash"]
