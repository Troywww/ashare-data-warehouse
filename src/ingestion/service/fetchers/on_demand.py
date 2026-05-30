"""On-demand fetchers — lightweight wrappers for tables moved from pipeline.

Each function mirrors the pipeline fetcher logic but takes minimal params
(date_str / symbol) and returns a list of dicts for DataService to persist.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# dragon_tiger — akshare
# ---------------------------------------------------------------------------


_AKSHARE_FIELD_MAP = {
    "symbol": "代码",
    "date": "上榜日",
    "reason": "解读",
    "close": "收盘价",
    "change_pct": "涨跌幅",
    "net_buy": "龙虎榜净买额",
    "buy_amount": "龙虎榜买入额",
    "sell_amount": "龙虎榜卖出额",
    "total_amount": "龙虎榜成交额",
    "market_total_amount": "市场总成交额",
    "net_buy_ratio": "净买额占总成交比",
    "amount_ratio": "成交额占总成交比",
    "turnover_rate": "换手率",
    "float_mv": "流通市值",
    "perf_1d": "上榜后1日",
    "perf_2d": "上榜后2日",
    "perf_5d": "上榜后5日",
    "perf_10d": "上榜后10日",
    "comment": "解读",
}


async def fetch_dragon_tiger(date_str: str = "") -> list[dict[str, Any]]:
    """Fetch dragon tiger data for a date (default: latest trading day)."""
    import akshare as ak

    if not date_str:
        date_str = date.today().isoformat()
    start = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

    try:
        df = ak.stock_lhb_detail_em(
            start_date=start.replace("-", ""),
            end_date=date_str.replace("-", ""),
        )
    except Exception as e:
        logger.warning("dragon_tiger on-demand fetch failed: %s", e)
        return []

    if df is None or df.empty:
        return []

    reverse_map = {v: k for k, v in _AKSHARE_FIELD_MAP.items() if v in df.columns}
    df = df.rename(columns=reverse_map)
    known = list(_AKSHARE_FIELD_MAP.keys())
    cols = [c for c in known if c in df.columns]
    df = df[cols]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)

    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# dragon_tiger_seats — akshare (席位明细)
# ---------------------------------------------------------------------------


async def fetch_dragon_tiger_seats(symbol: str, date_str: str = "") -> list[dict[str, Any]]:
    """Fetch dragon tiger seat details from akshare for a stock on a date.

    Calls stock_lhb_stock_detail_em(symbol, date, flag) for both 买入 and 卖出 sides.
    Returns per-seat breakdown with buy/sell amounts and ratios.

    akshare columns:
        序号, 交易营业部名称, 买入金额, 买入金额-占总成交比例,
        卖出金额, 卖出金额-占总成交比例, 净额, 类型
    """
    import akshare as ak

    if not date_str:
        date_str = date.today().isoformat()

    if not symbol:
        return []

    all_rows = []
    for side in ["买入", "卖出"]:
        try:
            df = ak.stock_lhb_stock_detail_em(
                symbol=symbol,
                date=date_str.replace("-", ""),
                flag=side,
            )
        except Exception as e:
            logger.debug("dragon_tiger_seats %s %s %s: %s", symbol, date_str, side, e)
            continue

        if df is None or df.empty:
            continue

        for _, row in df.iterrows():
            seat_name = str(row.get("交易营业部名称", ""))
            if not seat_name or seat_name == "nan":
                continue

            buy_amt = _parse_amount_str(row.get("买入金额"))
            sell_amt = _parse_amount_str(row.get("卖出金额"))
            net_amt = _parse_amount_str(row.get("净额"))
            reason = str(row.get("类型", ""))

            # Parse ratio strings like "6.53%" → 6.53
            buy_ratio_raw = row.get("买入金额-占总成交比例")
            sell_ratio_raw = row.get("卖出金额-占总成交比例")
            buy_ratio = _extract_ratio(buy_ratio_raw)
            sell_ratio = _extract_ratio(sell_ratio_raw)

            all_rows.append({
                "symbol": symbol,
                "date": date_str,
                "seat_name": seat_name,
                "buy_amount": buy_amt,
                "sell_amount": sell_amt,
                "net_amount": net_amt,
                "buy_ratio": buy_ratio,
                "sell_ratio": sell_ratio,
                "reason": reason,
                "side": side,
            })

    return all_rows


# ---------------------------------------------------------------------------
# board_daily — easy_tdx
# ---------------------------------------------------------------------------


async def fetch_board_daily() -> list[dict[str, Any]]:
    """Fetch board rankings from easy_tdx MacClient (industry + concept)."""
    from easy_tdx.mac.client import MacClient
    from easy_tdx.mac.enums import BoardType

    today = date.today()
    rows = []

    try:
        with MacClient("121.36.248.138", timeout=15) as client:
            for board_type, type_name in [(BoardType.HY, "industry"), (BoardType.GN, "concept")]:
                boards_df = client.get_board_list(board_type)
                if boards_df.empty:
                    continue
                for i, (_, board) in enumerate(boards_df.iterrows()):
                    price = board.get("price", 0) or 0
                    pre_close = board.get("pre_close", 0) or 0
                    change_pct = round((price - pre_close) / pre_close * 100, 2) if pre_close else 0
                    rows.append({
                        "date": today,
                        "board_name": board.get("name", ""),
                        "board_type": type_name,
                        "change_pct": change_pct,
                        "rank": i + 1,
                        "total_mv": None,
                        "turnover_rate": None,
                        "up_count": None,
                        "down_count": None,
                        "leader_name": board.get("symbol_name", ""),
                        "leader_pct": None,
                    })
    except Exception as e:
        logger.warning("board_daily on-demand fetch failed: %s", e)
        return []

    if rows:
        df = pd.DataFrame(rows)
        df = df.sort_values("change_pct", ascending=False).reset_index(drop=True)
        df["rank"] = df.index + 1
        return df.to_dict(orient="records")
    return []


# ---------------------------------------------------------------------------
# hot_stocks — akshare (雪球)
# ---------------------------------------------------------------------------


async def fetch_hot_stocks() -> list[dict[str, Any]]:
    """Fetch hot stocks ranking from akshare (雪球关注度)."""
    import akshare as ak

    try:
        df = ak.stock_hot_follow_xq()
    except Exception as e:
        logger.warning("hot_stocks on-demand fetch failed: %s", e)
        return []

    if df is None or df.empty:
        return []

    field_map = {
        "symbol": "股票代码",
        "stock_name": "股票简称",
        "follow_count": "关注",
        "price": "最新价",
    }
    rev = {v: k for k, v in field_map.items() if v in df.columns}
    df = df.rename(columns=rev)
    known = list(field_map.keys())
    cols = [c for c in known if c in df.columns]
    df = df[cols]

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].str.replace(r"^(SH|SZ|BJ)", "", regex=True)

    today = date.today()
    df["date"] = today
    df["rank"] = range(1, len(df) + 1)

    return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# hot_reasons — akshare (同花顺)
# ---------------------------------------------------------------------------


async def fetch_hot_reasons() -> list[dict[str, Any]]:
    """Fetch hot stock reasons from akshare (东方财富热榜).

    Note: stock_hot_rank_ths (同花顺) was removed from akshare.
    Now using stock_hot_rank_em which provides rank/price/change but no reason_tags.
    Retries up to 2 times on transient network errors.
    """
    import asyncio
    import akshare as ak

    last_error = None
    for attempt in range(5):
        try:
            df = ak.stock_hot_rank_em()
            break
        except Exception as e:
            last_error = e
            if attempt < 4:
                delay = 1.5 * (2 ** attempt)  # 1.5, 3, 6, 12
                logger.debug("hot_reasons attempt %d failed: %s, retrying in %.1fs...", attempt + 1, e, delay)
                await asyncio.sleep(delay)
    else:
        logger.warning("hot_reasons on-demand fetch failed after 5 attempts: %s", last_error)
        return []

    if df is None or df.empty:
        return []

    # stock_hot_rank_em returns: 当前排名, 代码, 股票名称, 最新价, 涨跌额, 涨跌幅
    field_map = {
        "rank": "当前排名",
        "symbol": "代码",
        "stock_name": "股票名称",
        "close": "最新价",
        "change_amt": "涨跌额",
        "change_pct": "涨跌幅",
    }
    rev = {v: k for k, v in field_map.items() if v in df.columns}
    df = df.rename(columns=rev)
    known = list(field_map.keys())
    cols = [c for c in known if c in df.columns]
    df = df[cols]

    today = date.today()
    results = []
    for _, row in df.iterrows():
        symbol = str(row.get("symbol", "")).replace("SZ", "").replace("SH", "").replace("BJ", "").zfill(6)
        results.append({
            "date": today,
            "symbol": symbol,
            "stock_name": str(row.get("stock_name", "")),
            "reason_tags": "",  # not available from stock_hot_rank_em
            "close": _parse_float(row.get("close")),
            "change_amt": _parse_float(row.get("change_amt")),
            "change_pct": _parse_float(row.get("change_pct")),
            "turnover_rate": None,
            "amount": None,
            "volume": None,
        })

    return results


# ---------------------------------------------------------------------------
# margin_trading — akshare
# ---------------------------------------------------------------------------


async def fetch_margin_trading(date_str: str = "") -> list[dict[str, Any]]:
    """Fetch margin trading data from akshare (上交所+深交所)."""
    import akshare as ak

    if not date_str:
        date_str = date.today().isoformat()

    all_rows = []

    # SSE
    try:
        df_sse = ak.stock_margin_detail_sse(date=date_str.replace("-", ""))
        if df_sse is not None and not df_sse.empty:
            field_map_sse = {
                "symbol": "标的证券代码",
                "date": "交易日",
                "rzye": "融资余额",
                "rzye_buy": "融资买入额",
                "rzye_repay": "融资偿还额",
                "rqyl": "融券余量",
                "rqyl_sell": "融券卖出量",
                "rqyl_repay": "融券偿还量",
            }
            rev = {v: k for k, v in field_map_sse.items() if v in df_sse.columns}
            df_sse = df_sse.rename(columns=rev)
            known = list(field_map_sse.keys())
            cols = [c for c in known if c in df_sse.columns]
            if cols:
                df_sse = df_sse[cols]
                if "date" in df_sse.columns:
                    df_sse["date"] = pd.to_datetime(df_sse["date"]).dt.date
                all_rows.extend(df_sse.to_dict(orient="records"))
    except Exception as e:
        logger.debug("margin_trading SSE fetch failed: %s", e)

    # SZSE
    try:
        df_szse = ak.stock_margin_detail_szse(date=date_str.replace("-", ""))
        if df_szse is not None and not df_szse.empty:
            field_map_szse = {
                "symbol": "证券代码",
                "date": "信用交易日期",
                "rzye": "融资余额",
                "rqyl": "融券余量",
                "rqyl_amt": "融券余额",
                "rzrqye": "融资融券余额",
            }
            rev = {v: k for k, v in field_map_szse.items() if v in df_szse.columns}
            df_szse = df_szse.rename(columns=rev)
            known = list(field_map_szse.keys())
            cols = [c for c in known if c in df_szse.columns]
            if cols:
                df_szse = df_szse[cols]
                if "date" in df_szse.columns:
                    df_szse["date"] = pd.to_datetime(df_szse["date"]).dt.date
                all_rows.extend(df_szse.to_dict(orient="records"))
    except Exception as e:
        logger.debug("margin_trading SZSE fetch failed: %s", e)

    return all_rows


# ---------------------------------------------------------------------------
# block_trades — akshare (东财)
# ---------------------------------------------------------------------------


async def fetch_block_trades(date_str: str = "") -> list[dict[str, Any]]:
    """Fetch block trades from akshare (东财大宗交易).

    Column mapping (akshare returns 12 columns):
      序号, 交易日期, 证券代码, 证券简称, 涨跌幅, 收盘价, 成交价,
      折溢率, 成交笔数, 成交总量, 成交总额, 成交总额/流通市值
    """
    import akshare as ak

    if not date_str:
        date_str = date.today().isoformat()

    # Try requested date first, then fall back to yesterday
    dates_to_try = [date_str]
    if date_str == date.today().isoformat():
        dates_to_try.append((date.today() - timedelta(days=1)).isoformat())

    df = None
    last_error = None
    for d in dates_to_try:
        try:
            df = ak.stock_dzjy_mrtj(start_date=d.replace("-", ""), end_date=d.replace("-", ""))
            if df is not None and not df.empty:
                break
        except Exception as e:
            last_error = e
            continue

    if df is None or df.empty:
        if last_error:
            logger.warning("block_trades on-demand fetch failed: %s", last_error)
        return []

    # Column name mapping (akshare Chinese → our English)
    field_map = {
        "stock_code": "证券代码",
        "trade_date": "交易日期",
        "price": "成交价",
        "volume": "成交总量",
        "amount": "成交总额",
        "premium_ratio": "折溢率",
    }
    rev = {v: k for k, v in field_map.items() if v in df.columns}
    df = df.rename(columns=rev)

    results = []
    for _, row in df.iterrows():
        results.append({
            "stock_code": str(row.get("stock_code", "")).zfill(6),
            "trade_date": _parse_date(row.get("trade_date")),
            "price": _parse_float(row.get("price")),
            "volume": _parse_int(row.get("volume")),
            "amount": _parse_float(row.get("amount")),
            "premium_ratio": _parse_float(row.get("premium_ratio")),
            "buyer_broker": "",
            "seller_broker": "",
        })

    return results


# ---------------------------------------------------------------------------
# lockup_calendar — akshare (东财)
# ---------------------------------------------------------------------------


async def fetch_lockup_calendar() -> list[dict[str, Any]]:
    """Fetch lockup calendar from akshare (东财限售解禁).

    Uses stock_restricted_release_detail_em which returns:
      序号, 股票代码, 股票简称, 解禁时间, 限售股类型, 解禁数量,
      实际解禁数量, 实际解禁市值, 占解禁前流通市值比例, ...
    """
    import akshare as ak

    today = date.today()
    start = (today - timedelta(days=30)).strftime("%Y%m%d")
    end = (today + timedelta(days=90)).strftime("%Y%m%d")

    try:
        df = ak.stock_restricted_release_detail_em(start_date=start, end_date=end)
    except Exception as e:
        logger.warning("lockup_calendar on-demand fetch failed: %s", e)
        return []

    if df is None or df.empty:
        return []

    field_map = {
        "stock_code": "股票代码",
        "unlock_date": "解禁时间",
        "unlock_vol": "解禁数量",
        "unlock_ratio": "占解禁前流通市值比例",
        "status": "限售股类型",
    }
    rev = {v: k for k, v in field_map.items() if v in df.columns}
    df = df.rename(columns=rev)

    results = []
    for _, row in df.iterrows():
        results.append({
            "stock_code": str(row.get("stock_code", "")).zfill(6),
            "unlock_date": _parse_date(row.get("unlock_date")),
            "unlock_vol": _parse_int(row.get("unlock_vol")),
            "unlock_ratio": _parse_float(row.get("unlock_ratio")),
            "status": str(row.get("status", "")),
        })

    return results


# ---------------------------------------------------------------------------
# global_markets — easy_tdx
# ---------------------------------------------------------------------------


_GLOBAL_SYMBOLS = [
    (74, "TSLA", "TSLA", "特斯拉"),       # ExMarket.US_STOCK
    (74, "AAPL", "AAPL", "苹果"),
    (74, "MSFT", "MSFT", "微软"),
    (74, "QQQ", "QQQ", "纳斯达克100ETF"),
    (74, "SPY", "SPY", "标普500ETF"),
    (31, "00700", "HK00700", "腾讯控股"),  # ExMarket.HK_MAIN_BOARD
    (31, "00001", "HK00001", "长和"),
    (31, "00005", "HK00005", "汇丰控股"),
    (31, "09988", "HK09988", "阿里巴巴"),
    (31, "03690", "HK03690", "美团"),
]


async def fetch_global_markets() -> list[dict[str, Any]]:
    """Fetch global market klines from easy_tdx MacExClient."""
    from easy_tdx.ex.mac_client import MacExClient

    _MARKETS = [
        (74, "TSLA", "TSLA", "特斯拉"),       # ExMarket.US_STOCK
        (74, "AAPL", "AAPL", "苹果"),
        (74, "MSFT", "MSFT", "微软"),
        (74, "QQQ", "QQQ", "纳斯达克100ETF"),
        (74, "SPY", "SPY", "标普500ETF"),
        (31, "00700", "HK00700", "腾讯控股"),  # ExMarket.HK_MAIN_BOARD
        (31, "00001", "HK00001", "长和"),
        (31, "00005", "HK00005", "汇丰控股"),
        (31, "09988", "HK09988", "阿里巴巴"),
        (31, "03690", "HK03690", "美团"),
    ]

    all_rows = []
    try:
        with MacExClient.from_best_host() as client:
            for ex_market, sym, our_sym, name in _MARKETS:
                try:
                    df = client.goods_kline(ex_market, sym, count=180)
                    if df.empty:
                        continue
                    for _, row in df.iterrows():
                        dt = row.get("datetime")
                        if dt is None:
                            continue
                        dt_str = str(dt)[:10] if hasattr(dt, "strftime") else str(dt)[:10]
                        all_rows.append({
                            "symbol": our_sym,
                            "date": dt_str,
                            "open": float(row.get("open", 0)),
                            "high": float(row.get("high", 0)),
                            "low": float(row.get("low", 0)),
                            "close": float(row.get("close", 0)),
                            "volume": float(row.get("vol", 0)),
                        })
                except Exception as e:
                    logger.debug("global %s: %s", our_sym, e)
    except Exception as e:
        logger.warning("global_markets on-demand fetch failed: %s", e)
        return []

    if all_rows:
        df = pd.DataFrame(all_rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.drop_duplicates(subset=["symbol", "date"])
        return df.to_dict(orient="records")
    return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_amount_str(val) -> float | None:
    """Parse Chinese-format amount like '4.78亿', '3000万', or plain float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s or s == "nan":
        return None
    # Remove prefix like "买入"/"卖出"
    s = s.lstrip("买入卖出入净额")
    if not s:
        return None
    try:
        # Try direct float first
        return float(s)
    except ValueError:
        pass
    try:
        if "亿" in s:
            return float(s.replace("亿", "")) * 1e8
        elif "万" in s:
            return float(s.replace("万", "")) * 1e4
    except ValueError:
        pass
    return None


def _extract_ratio(val) -> float | None:
    """Extract percentage from strings like '6.53%' or '(6.53%)'."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s or s == "nan":
        return None
    # Remove parentheses and %
    s = s.replace("(", "").replace(")", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()[:10]
    return s if s else None
