"""Technical indicator calculations — pure numpy/pandas, no I/O.

All functions are vectorized for performance.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# MACD
# ---------------------------------------------------------------------------


def compute_macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> dict[str, Any]:
    """Compute MACD indicator.

    Parameters
    ----------
    close : pd.Series
        Close price series, ordered by date ascending.
    fast, slow, signal : int
        MACD parameters.

    Returns
    -------
    dict with keys: dif, dea, macd_bar, latest, golden_cross, dead_cross.
    """
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_bar = 2 * (dif - dea)

    # Detect cross
    dif_prev = dif.shift(1)
    golden_cross = (dif_prev < dea.shift(1)) & (dif >= dea)
    dead_cross = (dif_prev > dea.shift(1)) & (dif <= dea)

    return {
        "dif": dif.values.tolist(),
        "dea": dea.values.tolist(),
        "macd_bar": macd_bar.values.tolist(),
        "latest": {
            "dif": float(dif.iloc[-1]) if len(dif) > 0 else None,
            "dea": float(dea.iloc[-1]) if len(dea) > 0 else None,
            "macd_bar": float(macd_bar.iloc[-1]) if len(macd_bar) > 0 else None,
        },
        "golden_cross": golden_cross.iloc[-1] if len(golden_cross) > 0 else False,
        "dead_cross": dead_cross.iloc[-1] if len(dead_cross) > 0 else False,
        "params": {"fast": fast, "slow": slow, "signal": signal},
    }


# ---------------------------------------------------------------------------
# KDJ
# ---------------------------------------------------------------------------


def compute_kdj(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    n: int = 9,
    k_weight: int = 3,
    d_weight: int = 3,
) -> dict[str, Any]:
    """Compute KDJ (Stochastic) indicator.

    Returns dict with keys: k, d, j, latest, golden_cross, dead_cross.
    """
    low_n = low.rolling(window=n).min()
    high_n = high.rolling(window=n).max()
    rsv = (close - low_n) / (high_n - low_n) * 100

    k = pd.Series(50.0, index=close.index)
    for i in range(len(k)):
        if i == 0:
            k.iloc[i] = 50
        else:
            k.iloc[i] = (k_weight - 1) / k_weight * k.iloc[i - 1] + 1 / k_weight * rsv.iloc[i]

    d = k.ewm(span=d_weight, adjust=False).mean()
    j = 3 * k - 2 * d

    golden_cross = (k.shift(1) < d.shift(1)) & (k >= d)
    dead_cross = (k.shift(1) > d.shift(1)) & (k <= d)

    return {
        "k": k.values.tolist(),
        "d": d.values.tolist(),
        "j": j.values.tolist(),
        "latest": {
            "k": float(k.iloc[-1]) if len(k) > 0 else None,
            "d": float(d.iloc[-1]) if len(d) > 0 else None,
            "j": float(j.iloc[-1]) if len(j) > 0 else None,
        },
        "golden_cross": bool(golden_cross.iloc[-1]) if len(golden_cross) > 0 else False,
        "dead_cross": bool(dead_cross.iloc[-1]) if len(dead_cross) > 0 else False,
        "params": {"n": n, "k_weight": k_weight, "d_weight": d_weight},
    }


# ---------------------------------------------------------------------------
# RSI
# ---------------------------------------------------------------------------


def compute_rsi(close: pd.Series, period: int = 14) -> dict[str, Any]:
    """Compute RSI indicator."""
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    return {
        "rsi": rsi.values.tolist(),
        "latest": float(rsi.iloc[-1]) if len(rsi) > 0 else None,
        "oversold": bool(rsi.iloc[-1] < 30) if len(rsi) > 0 else False,
        "overbought": bool(rsi.iloc[-1] > 70) if len(rsi) > 0 else False,
        "params": {"period": period},
    }


# ---------------------------------------------------------------------------
# BOLL (Bollinger Bands)
# ---------------------------------------------------------------------------


def compute_boll(
    close: pd.Series,
    period: int = 20,
    multiplier: float = 2.0,
) -> dict[str, Any]:
    """Compute Bollinger Bands.

    Parameters
    ----------
    close : pd.Series
        Close price series, ordered by date ascending.
    period : int
        Moving average period (default 20).
    multiplier : float
        Standard deviation multiplier (default 2.0).

    Returns
    -------
    dict with keys: upper, middle, lower, width, latest, upper_break, lower_break.
    """
    middle = close.rolling(window=period).mean()
    std = close.rolling(window=period).std()
    upper = middle + multiplier * std
    lower = middle - multiplier * std
    width = (upper - lower) / middle

    # Signal detection on latest bar
    has_data = len(close) >= period
    is_upper_break = bool(close.iloc[-1] > upper.iloc[-1]) if has_data else False
    is_lower_break = bool(close.iloc[-1] < lower.iloc[-1]) if has_data else False

    return {
        "upper": upper.values.tolist(),
        "middle": middle.values.tolist(),
        "lower": lower.values.tolist(),
        "width": width.values.tolist(),
        "latest": {
            "upper": float(upper.iloc[-1]) if has_data and pd.notna(upper.iloc[-1]) else None,
            "middle": float(middle.iloc[-1]) if has_data and pd.notna(middle.iloc[-1]) else None,
            "lower": float(lower.iloc[-1]) if has_data and pd.notna(lower.iloc[-1]) else None,
            "width": float(width.iloc[-1]) if has_data and pd.notna(width.iloc[-1]) else None,
        },
        "upper_break": is_upper_break,
        "lower_break": is_lower_break,
        "params": {"period": period, "multiplier": multiplier},
    }


# ---------------------------------------------------------------------------
# Signal scan — find stocks matching conditions
# ---------------------------------------------------------------------------


def signal_scan(
    kline_data: pd.DataFrame,
    indicator: str = "macd",
    signal: str = "golden_cross",
    period: str = "daily",
    params: dict | None = None,
) -> list[dict[str, Any]]:
    """Scan multiple stocks for a technical signal.

    Parameters
    ----------
    kline_data : pd.DataFrame
        Must have columns: symbol, date, open, high, low, close.
        Sorted by (symbol, date) ascending.
    indicator : str
        One of 'macd', 'kdj', 'rsi'.
    signal : str
        One of 'golden_cross', 'dead_cross', 'oversold', 'overbought'.
    period : str
        'daily', 'weekly' (used for metadata only).
    params : dict | None
        Indicator parameters.

    Returns
    -------
    list of {symbol, date, signal_type, value, extra}
    """
    if params is None:
        params = {}

    results = []
    for symbol, group in kline_data.groupby("symbol"):
        group = group.sort_values("date")
        try:
            if indicator == "macd":
                macd_params = {"fast": params.get("fast", 12), "slow": params.get("slow", 26), "signal": params.get("signal", 9)}
                comp = compute_macd(group["close"], **macd_params)

                if signal == "golden_cross" and comp["golden_cross"]:
                    results.append({
                        "symbol": symbol,
                        "date": str(group["date"].iloc[-1].date()) if hasattr(group["date"].iloc[-1], "date") else str(group["date"].iloc[-1]),
                        "signal": "golden_cross",
                        "value": comp["latest"]["dif"],
                        "extra": {"indicator": indicator, "period": period, **macd_params},
                    })
                elif signal == "dead_cross" and comp["dead_cross"]:
                    results.append({
                        "symbol": symbol,
                        "date": str(group["date"].iloc[-1].date()) if hasattr(group["date"].iloc[-1], "date") else str(group["date"].iloc[-1]),
                        "signal": "dead_cross",
                        "value": comp["latest"]["dif"],
                        "extra": {"indicator": indicator, "period": period, **macd_params},
                    })
            elif indicator == "kdj":
                kdj_params = {"n": params.get("n", 9), "k_weight": params.get("k_weight", 3), "d_weight": params.get("d_weight", 3)}
                comp = compute_kdj(group["high"], group["low"], group["close"], **kdj_params)

                if signal == "golden_cross" and comp["golden_cross"]:
                    results.append({
                        "symbol": symbol,
                        "date": str(group["date"].iloc[-1].date()) if hasattr(group["date"].iloc[-1], "date") else str(group["date"].iloc[-1]),
                        "signal": "golden_cross",
                        "value": comp["latest"]["k"],
                        "extra": {"indicator": indicator, "period": period, **kdj_params},
                    })
            elif indicator == "rsi":
                rsi_params = {"period": params.get("period", 14)}
                comp = compute_rsi(group["close"], **rsi_params)

                if signal == "oversold" and comp["oversold"]:
                    results.append({
                        "symbol": symbol,
                        "date": str(group["date"].iloc[-1].date()) if hasattr(group["date"].iloc[-1], "date") else str(group["date"].iloc[-1]),
                        "signal": "oversold",
                        "value": comp["latest"],
                        "extra": {"indicator": indicator, "period": period, **rsi_params},
                    })
                elif signal == "overbought" and comp["overbought"]:
                    results.append({
                        "symbol": symbol,
                        "date": str(group["date"].iloc[-1].date()) if hasattr(group["date"].iloc[-1], "date") else str(group["date"].iloc[-1]),
                        "signal": "overbought",
                        "value": comp["latest"],
                        "extra": {"indicator": indicator, "period": period, **rsi_params},
                    })
            elif indicator == "boll":
                boll_params = {"period": params.get("period", 20), "multiplier": params.get("multiplier", 2.0)}
                comp = compute_boll(group["close"], **boll_params)

                if signal == "upper_break" and comp["upper_break"]:
                    results.append({
                        "symbol": symbol,
                        "date": str(group["date"].iloc[-1].date()) if hasattr(group["date"].iloc[-1], "date") else str(group["date"].iloc[-1]),
                        "signal": "upper_break",
                        "value": comp["latest"]["width"],
                        "extra": {"indicator": indicator, "period": period, **boll_params},
                    })
                elif signal == "lower_break" and comp["lower_break"]:
                    results.append({
                        "symbol": symbol,
                        "date": str(group["date"].iloc[-1].date()) if hasattr(group["date"].iloc[-1], "date") else str(group["date"].iloc[-1]),
                        "signal": "lower_break",
                        "value": comp["latest"]["width"],
                        "extra": {"indicator": indicator, "period": period, **boll_params},
                    })
        except Exception:
            continue

    return results


def compute_indicators_row(close: pd.Series, high: pd.Series, low: pd.Series) -> dict:
    """Compute all indicator latest values for one stock.

    Returns a dict suitable for INSERT into indicator_values table.
    All indicators use default parameters (MACD 12/26/9, KDJ 9/3/3, RSI 14, BOLL 20/2).
    """
    macd = compute_macd(close)
    kdj = compute_kdj(high, low, close)
    rsi = compute_rsi(close)
    boll = compute_boll(close)

    return {
        "macd_dif": macd["latest"]["dif"],
        "macd_dea": macd["latest"]["dea"],
        "macd_bar": macd["latest"]["macd_bar"],
        "kdj_k": kdj["latest"]["k"],
        "kdj_d": kdj["latest"]["d"],
        "kdj_j": kdj["latest"]["j"],
        "rsi_14": rsi["latest"],
        "boll_upper": boll["latest"]["upper"],
        "boll_middle": boll["latest"]["middle"],
        "boll_lower": boll["latest"]["lower"],
        "boll_width": boll["latest"]["width"],
    }


def params_hash(params: dict) -> str:
    """Generate a deterministic hash for indicator parameters."""
    raw = json.dumps(params, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()[:8]
