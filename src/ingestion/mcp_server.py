"""MCP Server (HTTP/SSE) — A 股数据查询接口.

部署后其他 Agent 可通过 MCP 协议远程查询 DuckDB 中的 27 张表。
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
from src.ingestion.service import DataService, POLICIES

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
    """Open DuckDB — same process as scheduler thread, so default mode is safe.

    Both the MCP query handlers and the scheduler thread use the default
    (read_write) connection mode, which DuckDB allows within a single process.
    """
    cfg = load_config(_config_path)
    return IngestionDB(cfg.db_path)


def _get_service() -> DataService:
    cfg = load_config(_config_path)
    return DataService(cfg.db_path)


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
    svc = _get_service()
    result = await svc.fetch("global_markets", {"symbol": symbol})
    return json.dumps(result, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# 工具: 资金
# ---------------------------------------------------------------------------


@mcp.tool(description="查询个股资金流向 (主力净流入/大单/中单/小单)，symbol=股票代码")
async def query_capital_flow(symbol: str, days: int = 60) -> str:
    db = _get_db()
    df = db.conn.execute("""
        SELECT date, net_main, net_super, net_large, net_medium, net_small
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


@mcp.tool(description="查询融资融券数据，symbol=股票代码(留空=全市场当天)")
async def query_margin_trading(symbol: str = "") -> str:
    svc = _get_service()
    result = await svc.fetch("margin_trading", {"symbol": symbol})
    return json.dumps(result, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# 工具: 信号
# ---------------------------------------------------------------------------


@mcp.tool(description="查询龙虎榜，date_str=日期(YYYY-MM-DD，留空=最近7天)")
async def query_dragon_tiger(date_str: str = "") -> str:
    svc = _get_service()
    result = await svc.fetch("dragon_tiger", {"date_str": date_str})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="查询龙虎榜席位明细（买入/卖出营业部详情），symbol=股票代码，date_str=日期(YYYY-MM-DD)")
async def query_dragon_tiger_seats(symbol: str, date_str: str = "") -> str:
    svc = _get_service()
    result = await svc.fetch("dragon_tiger_seats", {"symbol": symbol, "date_str": date_str})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="查询板块涨跌排名，date_str留空=最新一天，top_n=返回条数")
async def query_board_daily(date_str: str = "", top_n: int = 20) -> str:
    svc = _get_service()
    result = await svc.fetch("board_daily", {})
    # Filter top_n client-side
    if isinstance(result, list) and len(result) > top_n:
        result = result[:top_n]
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="查询雪球关注热度排名，top_n=返回条数")
async def query_hot_stocks(top_n: int = 30) -> str:
    svc = _get_service()
    result = await svc.fetch("hot_stocks", {})
    if isinstance(result, list) and len(result) > top_n:
        result = result[:top_n]
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="查询同花顺热点题材归因，返回当天热点股票及涨停原因")
async def query_hot_reasons() -> str:
    svc = _get_service()
    result = await svc.fetch("hot_reasons", {})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="查询大宗交易，symbol=股票代码(留空=全市场)，days=最近天数")
async def query_block_trades(symbol: str = "", days: int = 30) -> str:
    svc = _get_service()
    result = await svc.fetch("block_trades", {"symbol": symbol})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="查询限售解禁日历，days_ahead=未来多少天")
async def query_lockup_calendar(days_ahead: int = 30) -> str:
    svc = _get_service()
    result = await svc.fetch("lockup_calendar", {})
    return json.dumps(result, ensure_ascii=False, default=str)


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
    db = _get_db()
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
# 工具: 实时数据（走 DataService + Cache）
# ---------------------------------------------------------------------------


@mcp.tool(description="获取个股实时行情（盘中3秒刷新），symbol=股票代码(如000001)")
async def get_realtime_quote(symbol: str) -> str:
    svc = _get_service()
    result = await svc.fetch("realtime_quote", {"symbol": symbol})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="批量获取实时行情，symbols=多个股票代码用逗号分隔(如000001,600000)")
async def get_realtime_quotes(symbols: str) -> str:
    svc = _get_service()
    code_list = [s.strip() for s in symbols.split(",")]
    result = await svc.fetch("realtime_quotes", {"symbols": code_list})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="获取盘中分钟K线，symbol=股票代码，period=周期(1min/5min/15min)，count=条数")
async def get_intraday_kline(symbol: str, period: str = "1min", count: int = 240) -> str:
    data_type = f"intraday_kline_{period}" if period in ("1min", "5min") else "intraday_kline_1min"
    svc = _get_service()
    result = await svc.fetch(data_type, {"symbol": symbol, "count": count})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="获取今日涨停梯队（连板排名）")
async def get_limit_up_ladder() -> str:
    svc = _get_service()
    result = await svc.fetch("limit_up_ladder")
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="获取最新市场滚动新闻（新浪财经），count=条数")
async def get_latest_news(count: int = 50) -> str:
    svc = _get_service()
    result = await svc.fetch("cls_telegram", {"count": count})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="获取个股新闻，symbol=股票代码")
async def get_stock_news(symbol: str) -> str:
    svc = _get_service()
    result = await svc.fetch("stock_news", {"symbol": symbol})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="获取个股公告，symbol=股票代码，days=回溯天数")
async def get_announcements(symbol: str, days: int = 30) -> str:
    svc = _get_service()
    result = await svc.fetch("announcements", {"symbol": symbol, "days": days})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="获取个股研报（东财），symbol=股票代码")
async def get_research_reports(symbol: str) -> str:
    svc = _get_service()
    result = await svc.fetch("research_reports", {"symbol": symbol})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="获取机构一致预期EPS，symbol=股票代码")
async def get_eps_consensus(symbol: str) -> str:
    svc = _get_service()
    result = await svc.fetch("eps_consensus", {"symbol": symbol})
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="查询大股东增减持记录，symbol=股票代码（按需拉取并缓存）")
async def query_shareholder_changes(symbol: str = "") -> str:
    if not symbol:
        db = _get_db()
        df = db.conn.execute("""
            SELECT * FROM shareholder_changes
            ORDER BY announce_date DESC LIMIT 50
        """).fetchdf()
        db.close()
        return _to_json(df)
    svc = _get_service()
    result = await svc.fetch("shareholder_changes", {"symbol": symbol})
    return json.dumps(result, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# 工具: 计算层
# ---------------------------------------------------------------------------


@mcp.tool(description="查找出现技术信号的股票，indicator=macd/kdj/rsi/boll，signal=golden_cross/dead_cross/oversold/overbought/upper_break/lower_break，period=daily/weekly")
async def find_signal_stocks(indicator: str = "macd", signal: str = "golden_cross",
                              period: str = "daily", lookback: int = 120) -> str:
    svc = _get_service()
    result = await svc.compute("signal_scan", {
        "indicator": indicator,
        "signal": signal,
        "period": period,
        "lookback": lookback,
    })
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool(description="计算个股技术指标，symbol=股票代码，indicator=macd/kdj/rsi/boll，可指定参数如fast/slow/signal/period")
async def compute_indicator(symbol: str, indicator: str = "macd",
                             fast: int = 12, slow: int = 26, signal_period: int = 9,
                             lookback: int = 120) -> str:
    svc = _get_service()
    result = await svc.compute(indicator, {
        "symbol": symbol,
        "fast": fast,
        "slow": slow,
        "signal": signal_period,
        "lookback": lookback,
    })
    return json.dumps(result, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# 工具: 系统管理
# ---------------------------------------------------------------------------


@mcp.tool(description="清除缓存（开盘前调用，或指定type只清某一类）")
async def clear_cache(data_type: str = "") -> str:
    svc = _get_service()
    if data_type:
        cleared = svc.invalidate_by_type(data_type)
        return json.dumps({"cleared": cleared, "type": data_type}, ensure_ascii=False)
    else:
        cleared = svc.invalidate_all()
        return json.dumps({"cleared": cleared, "type": "all"}, ensure_ascii=False)


@mcp.tool(description="查看缓存统计")
async def cache_stats() -> str:
    svc = _get_service()
    stats = svc.cache_stats()
    return json.dumps(stats, ensure_ascii=False)


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
    # Start scheduler as a daemon thread BEFORE uvicorn — this ensures both
    # the scheduler and MCP handlers share the same Python process, so DuckDB
    # connections (all default read_write mode) can coexist.
    from src.ingestion.scheduler import start_scheduler_thread
    cfg = load_config(_config_path)
    start_scheduler_thread(cfg)

    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    logger.info("MCP Server starting on http://%s:%d/sse", host, port)
    import uvicorn
    app = mcp.sse_app()
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
