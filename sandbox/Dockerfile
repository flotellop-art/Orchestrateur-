FROM python:3.12.10-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    NPM_CONFIG_UPDATE_NOTIFIER=false \
    NPM_CONFIG_FUND=false \
    HOME=/tmp/orchestrator-home

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates nodejs npm \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir pytest==9.1.1 Flask==3.1.3 \
    && groupadd --gid 65532 orchestrator \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin orchestrator

WORKDIR /workspace
USER 65532:65532

ENTRYPOINT ["/bin/sh", "-lc"]
