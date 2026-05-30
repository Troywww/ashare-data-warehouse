#!/bin/bash
# ============================================================================
# A股数据仓库 — Docker 入口
#
# 三种模式:
#   1. 无参数 → 启动 MCP Server + scheduler 线程（生产模式）
#   2. 参数是 CLI 命令 → python -m src.ingestion <cmd> (init/backfill/status/...)
#   3. 参数是 python  → 直接执行原始 python 命令
#
# Usage:
#   docker compose up -d                           # 生产模式
#   docker compose run --rm ingestion init         # 初始化数据库
#   docker compose run --rm ingestion python -c "..."  # 原始 python
# ============================================================================
set -e

if [ $# -eq 0 ]; then
    echo "[entrypoint] Starting MCP server (scheduler runs as daemon thread)..."
    exec python -m src.ingestion.mcp_server
elif [ "$1" = "python" ]; then
    shift
    echo "[entrypoint] Running raw python: $*"
    exec python "$@"
else
    echo "[entrypoint] Running command: $*"
    exec python -m src.ingestion "$@"
fi
