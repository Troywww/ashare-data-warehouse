"""MCP Server (HTTP/SSE) — A 股数据查询接口.

部署后其他 Agent 可通过 MCP 协议远程查询 DuckDB 中的 19 张表。
只读操作，不做数据更新（更新请通过 web 面板或 CLI）。

Agent 配置:
  "mcpServers": {
    "ashare": {
      "url": "http://host:8000/sse"
    }
  }
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd
from mcp.server.fastmcp import FastMCP

from src.ingestion.config import load_config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import FETCHER_REGISTRY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_config_path: str = os.getenv("INGESTION_CONFIG", "config.yaml")
mcp = FastMCP("ashare-data-warehouse", log_level="ERROR")


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _to_json(df: pd.DataFrame) -> str:
    """DataFrame → JSON 字符串, 处理 NaN/NaT."""
    df = df.copy().astype(object).where(pd.notnull(df), None)
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None if isinstance(x, float) and (x == float("inf") or x == float("-inf")) else x
        )
    return df.to_json(orient="records", date_format="iso", force_ascii=False)


def _get_db() -> IngestionDB:
    cfg = load_config(_config_path)
    return IngestionDB(cfg.db_path)


# ---------------------------------------------------------------------------
# 工具: 行情数据
# ---------------------------------------------------------------------------


@mcp.tool(description="查询个股日K线（前复权），symbol=股票代码，days=天数")
async def query_kline(symbol: str, days: int = 60) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT date, open, high, low, close, volume, amount, pct_chg, turnover_rate
        FROM daily_ohlcv WHERE symbol = ?
        ORDER BY date DESC LIMIT ?
    """, [symbol, days]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询个股估值 (PE/PB/总市值/流通市值)，symbol=股票代码，days=天数")
async def query_valuation(symbol: str, days: int = 60) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT date, pe_ttm, pb, ps_ttm, pcf_ncf_ttm, total_mv, circ_mv
        FROM daily_valuation WHERE symbol = ?
        ORDER BY date DESC LIMIT ?
    """, [symbol, days]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询外围指数行情 (美股/港股/黄金/原油)，留空=所有指数最新价")
async def query_global_markets(symbol: str = "") -> str:
    db = _get_db()
    if symbol:
        df = db.conn.execute("""
            SELECT date, open, high, low, close, volume
            FROM global_markets WHERE symbol = ?
            ORDER BY date DESC LIMIT 60
        """, [symbol]).fetchdf()
    else:
        df = db.conn.execute("""
            SELECT symbol, date, close FROM global_markets
            WHERE date = (SELECT MAX(date) FROM global_markets) ORDER BY symbol
        """).fetchdf()
    db.close()
    return _to_json(df)


# ---------------------------------------------------------------------------
# 工具: 资金
# ---------------------------------------------------------------------------


@mcp.tool(description="查询个股资金流向 (主力净流入/大单/中单/小单)，symbol=股票代码")
async def query_capital_flow(symbol: str, days: int = 60) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT date, net_main, net_super_5d, net_large_5d, net_medium_5d, net_small_5d
        FROM capital_flow WHERE symbol = ?
        ORDER BY date DESC LIMIT ?
    """, [symbol, days]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询北向资金每日净买入（沪股通/深股通），days=最近天数")
async def query_northbound_flow(days: int = 30) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT trade_date, market, net_buy FROM northbound_flow
        ORDER BY trade_date DESC LIMIT ?
    """, [days]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询融资融券数据，symbol=股票代码(留空=全市场汇总)")
async def query_margin_trading(symbol: str = "") -> str:
    db = _get_db()
    if symbol:
        df = db.conn.execute("""
            SELECT date, rzye, rzye_buy, rqyl, rzrqye
            FROM margin_trading WHERE symbol = ?
            ORDER BY date DESC LIMIT 60
        """, [symbol]).fetchdf()
    else:
        df = db.conn.execute("""
            SELECT date, SUM(rzye) as total_融资余额, SUM(rqyl) as total_融券余量
            FROM margin_trading GROUP BY date
            ORDER BY date DESC LIMIT 30
        """).fetchdf()
    db.close()
    return _to_json(df)


# ---------------------------------------------------------------------------
# 工具: 信号
# ---------------------------------------------------------------------------


@mcp.tool(description="查询龙虎榜，date_str=日期(YYYY-MM-DD，留空=最近5天)")
async def query_dragon_tiger(date_str: str = "") -> str:
    db = _get_db()
    if date_str:
        df = db.conn.execute("""
            SELECT symbol, date, reason, change_pct, net_buy, buy_amount,
                   sell_amount, net_buy_ratio, perf_1d, perf_2d, perf_5d
            FROM dragon_tiger WHERE date = ? ORDER BY net_buy DESC
        """, [date_str]).fetchdf()
    else:
        df = db.conn.execute("""
            SELECT symbol, date, reason, change_pct, net_buy, buy_amount,
                   sell_amount, net_buy_ratio, perf_1d, perf_2d, perf_5d
            FROM dragon_tiger ORDER BY date DESC, net_buy DESC LIMIT 100
        """).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询板块涨跌排名，date_str留空=最新一天，top_n=返回条数")
async def query_board_daily(date_str: str = "", top_n: int = 20) -> str:
    db = _get_db()
    if date_str:
        df = db.conn.execute("""
            SELECT date, board_name, board_type, change_pct, rank, leader_name
            FROM board_daily WHERE date = ? ORDER BY rank LIMIT ?
        """, [date_str, top_n]).fetchdf()
    else:
        df = db.conn.execute("""
            SELECT date, board_name, board_type, change_pct, rank, leader_name
            FROM board_daily WHERE date = (SELECT MAX(date) FROM board_daily)
            ORDER BY rank LIMIT ?
        """, [top_n]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询雪球关注热度排名，top_n=返回条数")
async def query_hot_stocks(top_n: int = 30) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT date, rank, symbol, stock_name, follow_count, price
        FROM hot_stocks WHERE date = (SELECT MAX(date) FROM hot_stocks)
        ORDER BY rank LIMIT ?
    """, [top_n]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询大宗交易，symbol=股票代码(留空=全市场)，days=最近天数")
async def query_block_trades(symbol: str = "", days: int = 30) -> str:
    db = _get_db()
    if symbol:
        df = db.conn.execute("""
            SELECT stock_code, trade_date, price, volume, amount, premium_ratio
            FROM block_trades WHERE stock_code = ?
            ORDER BY trade_date DESC LIMIT ?
        """, [symbol, days]).fetchdf()
    else:
        df = db.conn.execute("""
            SELECT stock_code, trade_date, price, amount, premium_ratio
            FROM block_trades ORDER BY trade_date DESC LIMIT 50
        """).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询限售解禁日历，days_ahead=未来多少天")
async def query_lockup_calendar(days_ahead: int = 30) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT stock_code, unlock_date, unlock_vol, unlock_ratio
        FROM lockup_calendar WHERE unlock_date BETWEEN CURRENT_DATE AND CURRENT_DATE + ?
        ORDER BY unlock_date
    """, [days_ahead]).fetchdf()
    db.close()
    return _to_json(df)


# ---------------------------------------------------------------------------
# 工具: 基本面
# ---------------------------------------------------------------------------


@mcp.tool(description="查询个股季度财务数据 (EPS/ROE/营收/利润/毛利率)")
async def query_fundamentals(symbol: str) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT end_date, eps, roe, revenue, profit, revenue_yoy, profit_yoy,
               bvps, operating_cashflow, gross_margin
        FROM fundamentals WHERE symbol = ? ORDER BY end_date DESC LIMIT 20
    """, [symbol]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询股东户数（筹码集中度），symbol=股票代码")
async def query_holder_count(symbol: str) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT end_date, holder_count, change_qoq
        FROM holder_count WHERE stock_code = ? ORDER BY end_date DESC LIMIT 20
    """, [symbol]).fetchdf()
    db.close()
    return _to_json(df)


# ---------------------------------------------------------------------------
# 工具: 股票搜索
# ---------------------------------------------------------------------------


@mcp.tool(description="按股票代码或名称模糊搜索，keyword=代码或名称关键词")
async def search_stocks(keyword: str) -> str:
    db = _get_db()
    pattern = f"%{keyword}%"
    df = db.conn.execute("""
        SELECT symbol, name, market FROM stock_universe
        WHERE symbol LIKE ? OR name LIKE ? LIMIT 50
    """, [pattern, pattern]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询某行业下的所有股票，industry_name=行业名(如'半导体')")
async def query_industry_stocks(industry_name: str) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT u.symbol, u.name FROM stock_universe u
        JOIN stock_classification c ON u.symbol = c.symbol
        WHERE c.industry = ? ORDER BY u.symbol
    """, [industry_name]).fetchdf()
    db.close()
    return _to_json(df)


@mcp.tool(description="查询某概念板块下的所有股票，concept_name=概念名(如'人工智能')")
async def query_concept_stocks(concept_name: str) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT u.symbol, u.name FROM stock_universe u
        JOIN concept_blocks cb ON u.symbol = cb.symbol
        WHERE cb.concept_name = ? ORDER BY u.symbol
    """, [concept_name]).fetchdf()
    db.close()
    return _to_json(df)


# ---------------------------------------------------------------------------
# 工具: 数据概览
# ---------------------------------------------------------------------------


@mcp.tool(description="获取市场概览：各表行数、最新日期、数据库大小")
async def get_market_overview() -> str:
    cfg = load_config(_config_path)
    db = IngestionDB(cfg.db_path)
    stats = db.table_stats()
    db_size = db.get_db_size()

    overview = []
    for name, count in sorted(stats.items()):
        max_date = db.get_max_date(name)
        overview.append({
            "table": name, "rows": count,
            "max_date": max_date.isoformat() if max_date else None,
        })
    db.close()
    return json.dumps({
        "tables": overview,
        "total_rows": sum(stats.values()),
        "db_size_bytes": db_size,
        "db_size_human": _human_size(db_size),
    }, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 工具: 自定义查询
# ---------------------------------------------------------------------------


@mcp.tool(description="执行自定义只读SQL查询（仅SELECT/WITH，返回JSON）")
async def run_sql(sql: str) -> str:
    if not sql.upper().strip().startswith(("SELECT", "WITH")):
        return json.dumps({"error": "仅支持 SELECT 查询"})
    db = _get_db()
    df = db.conn.execute(sql).fetchdf()
    db.close()
    return _to_json(df)


# ---------------------------------------------------------------------------
# 启动
# ---------------------------------------------------------------------------


def _human_size(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f}{unit}"
        b /= 1024
    return f"{b:.1f}TB"


def main():
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    logger.info("MCP Server starting on http://%s:%d/sse", host, port)
    import uvicorn
    app = mcp.sse_app()
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
