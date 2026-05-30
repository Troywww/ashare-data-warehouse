# ============================================================================
# A股数据仓库 — Docker 镜像
# ============================================================================

FROM python:3.12-slim

WORKDIR /app

# Copy dependency files and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config.yaml ./

# Install the package itself
COPY pyproject.toml ./
RUN pip install --no-cache-dir .

# Create data directory and persist easy_tdx cache (avoids 15min refetch on restart)
RUN mkdir -p /app/data/ingestion /app/data/.easy_tdx/cache \
    && ln -sf /app/data/.easy_tdx /root/.easy_tdx

# Entrypoint: scheduler + MCP server
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Healthcheck is defined in docker-compose.yml (TCP check on MCP port)
