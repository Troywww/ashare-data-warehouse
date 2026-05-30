"""一致预期EPS + 研报 on-demand fetcher.

数据来源：
- EPS一致预期: akshare.stock_profit_forecast_ths（同花顺）
- 研报: 东方财富 reportapi.eastmoney.com（绕过 akshare 的 infoCode bug）
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

_ak = None


def _get_ak():
    global _ak
    if _ak is None:
        try:
            import akshare as ak
            _ak = ak
        except ImportError:
            logger.warning("akshare not installed, consensus/reports unavailable")
            return None
    return _ak


async def fetch_eps_consensus(symbol: str) -> list[dict[str, Any]]:
    """Fetch analyst consensus EPS forecast via akshare（东财）.

    Returns list of {symbol, year, analyst_count, eps_min, eps_avg, eps_max}.
    """
    ak = _get_ak()
    if ak is None:
        return []

    try:
        df = ak.stock_profit_forecast_ths(symbol)
    except Exception as e:
        logger.debug("consensus fetch failed for %s: %s", symbol, e)
        return []

    if df is None or df.empty:
        return []

    # Positional: 0=year, 1=analysts, 2=min, 3=avg, 4=max
    results = []
    for _, row in df.iterrows():
        results.append({
            "symbol": symbol,
            "year": _parse_year(row.iloc[0] if len(row) > 0 else None),
            "analyst_count": _parse_int(row.iloc[1] if len(row) > 1 else None),
            "eps_min": _parse_float(row.iloc[2] if len(row) > 2 else None),
            "eps_avg": _parse_float(row.iloc[3] if len(row) > 3 else None),
            "eps_max": _parse_float(row.iloc[4] if len(row) > 4 else None),
        })

    return [r for r in results if r["year"] is not None]


async def fetch_research_reports(symbol: str, max_pages: int = 3) -> list[dict[str, Any]]:
    """Fetch research reports from Eastmoney API directly.

    Bypasses akshare's stock_research_report_em which has a bug:
    when a stock has no reports, API returns empty data[], causing
    KeyError('infoCode') because the DataFrame has no columns.

    Returns list of {id, symbol, title, org_name, publish_date, rating,
                     target_price, eps_this_yr, eps_next_yr, eps_next2_yr, url}.
    """
    import requests

    url = "https://reportapi.eastmoney.com/report/list"
    params = {
        "industryCode": "*",
        "pageSize": "5000",
        "industry": "*",
        "rating": "*",
        "ratingChange": "*",
        "beginTime": "2000-01-01",
        "endTime": f"{datetime.now().year + 1}-01-01",
        "pageNo": "1",
        "fields": "",
        "qType": "0",
        "orgCode": "",
        "code": symbol,
        "rcode": "",
        "p": "1",
        "pageNum": "1",
        "pageNumber": "1",
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        data_json = r.json()
    except Exception as e:
        logger.warning("research_reports API request failed for %s: %s", symbol, e)
        return []

    total_page = data_json.get("TotalPage", 0)
    if total_page == 0:
        return []

    # Collect all pages
    all_data = list(data_json.get("data", []) or [])
    for page in range(2, min(total_page, max_pages) + 1):
        try:
            params.update({"pageNo": page, "p": page, "pageNum": page, "pageNumber": page})
            r = requests.get(url, params=params, timeout=15)
            page_json = r.json()
            page_data = page_json.get("data", []) or []
            all_data.extend(page_data)
        except Exception as e:
            logger.debug("research_reports page %d failed: %s", page, e)
            break

    if not all_data:
        return []

    # Build results with column name mapping
    this_eps_key = "predictThisYearEps"
    next_eps_key = "predictNextYearEps"
    next2_eps_key = "predictNextTwoYearEps"

    results = []
    for i, item in enumerate(all_data):
        info_code = item.get("infoCode", "")
        pdf_url = f"https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf" if info_code else ""

        publish_date = item.get("publishDate", "")
        if publish_date and "T" in str(publish_date):
            publish_date = str(publish_date)[:10]

        results.append({
            "id": str(i + 1),
            "symbol": symbol,
            "title": str(item.get("title", "")),
            "org_name": str(item.get("orgSName", "")),
            "publish_date": publish_date,
            "rating": str(item.get("emRatingName", "")),
            "target_price": _parse_float(item.get("indvAimPriceT")),
            "eps_this_yr": _parse_float(item.get(this_eps_key)),
            "eps_next_yr": _parse_float(item.get(next_eps_key)),
            "eps_next2_yr": _parse_float(item.get(next2_eps_key)),
            "url": pdf_url,
        })

    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_year(val) -> int | None:
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def _parse_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_float(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def _parse_date_str(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()[:10]
    return s if s else None
