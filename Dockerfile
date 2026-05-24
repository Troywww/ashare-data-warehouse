# ============================================================================
# A股数据仓库 — Docker 镜像
# ============================================================================

FROM python:3.12-slim

WORKDIR /app

# Copy local wheels for opentdx (not on PyPI)
COPY wheels/ /tmp/wheels/

# Copy dependency files and install
COPY requirements.txt ./
# Install opentdx from local wheel first
RUN pip install --no-cache-dir --find-links /tmp/wheels opentdx \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config.yaml ./

# Install the package itself
COPY pyproject.toml ./
# Install the package; opentdx is already installed above
RUN pip install --no-cache-dir --find-links /tmp/wheels . \
    && rm -rf /tmp/wheels

# Create data directory
RUN mkdir -p /app/data/ingestion

# Default command: run scheduler (blocking)
CMD ["ingestion", "schedule"]

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from src.ingestion.db import IngestionDB; IngestionDB().get_db_size()" || exit 1
