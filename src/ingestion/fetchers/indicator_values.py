"""Fetcher: indicator_values — 30 项技术指标 (MyTT 2D batch engine).

从 daily_ohlcv 读取 K 线，pivot 成 (时间 × 股票) 矩阵，一次性计算全部指标。
全量模式计算所有股票，增量模式只计算当天有 K 线更新的股票。

性能：跨股票矢量化，5500 只股票 ~35 秒（相比逐股计算 ~329 秒，9.4x 加速）。
"""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from src.ingestion.config import Config
from src.ingestion.db import IngestionDB
from src.ingestion.fetchers import register_fetcher

logger = logging.getLogger(__name__)

MIN_BARS = 120        # minimum OHLCV bars required to compute (daily)
MIN_BARS_W = 24        # weekly: ~6 months of weekly bars
MIN_BARS_M = 6          # monthly: ~6 months of monthly bars
LOOKBACK_DAYS = 250   # how far back to load OHLCV (daily)
BATCH = 800           # symbols per DB read / pivot batch


# ============================================================
# 2D helper functions — 对标 MyTT，操作 (time, stocks) 矩阵
# ============================================================

def _REF(S, N=1):
    return pd.DataFrame(S).shift(N).values

def _DIFF(S, N=1):
    return pd.DataFrame(S).diff(N).values

def _STD(S, N):
    return pd.DataFrame(S).rolling(N).std(ddof=0).values

def _SUM(S, N):
    if N > 0:
        return pd.DataFrame(S).rolling(N).sum().values
    return pd.DataFrame(S).cumsum().values

def _HHV(S, N):
    return pd.DataFrame(S).rolling(N).max().values

def _LLV(S, N):
    return pd.DataFrame(S).rolling(N).min().values

def _MA(S, N):
    return pd.DataFrame(S).rolling(N).mean().values

def _EMA(S, N):
    return pd.DataFrame(S).ewm(span=N, adjust=False).mean().values

def _SMA(S, N, M=1):
    return pd.DataFrame(S).ewm(alpha=M/N, adjust=False).mean().values

def _AVEDEV(S, N):
    from numpy.lib.stride_tricks import sliding_window_view
    result = np.full_like(S, np.nan)
    windows = sliding_window_view(S, N, axis=0)     # (T-N+1, stocks, N)
    means = windows.mean(axis=2)                      # (T-N+1, stocks)
    abs_devs = np.abs(windows - means[:, :, np.newaxis])  # (T-N+1, stocks, N)
    result[N-1:] = abs_devs.mean(axis=2)              # (T-N+1, stocks)
    return result

def _R(N, D=3):
    return np.round(N, D)

def _COUNT(S, N):
    return _SUM(S, N)

def _MAX(S1, S2):
    return np.maximum(S1, S2)

def _MIN(S1, S2):
    return np.minimum(S1, S2)

def _IF(S, A, B):
    return np.where(S, A, B)

def _ABS(S):
    return np.abs(S)


# ============================================================
# 2D indicator functions — 逻辑与 MyTT 完全一致，输入输出为 (time, stocks) 矩阵
# ============================================================

def _MACD_2D(CLOSE, SHORT=12, LONG=26, M=9):
    DIF = _EMA(CLOSE, SHORT) - _EMA(CLOSE, LONG)
    DEA = _EMA(DIF, M)
    MACD = (DIF - DEA) * 2
    return _R(DIF), _R(DEA), _R(MACD)

def _KDJ_2D(CLOSE, HIGH, LOW, N=9, M1=3, M2=3):
    low_n = _LLV(LOW, N)
    high_n = _HHV(HIGH, N)
    hl_diff = high_n - low_n
    with np.errstate(divide='ignore', invalid='ignore'):
        rsv = (CLOSE - low_n) / hl_diff * 100
    rsv = np.where(hl_diff == 0, 50, rsv)
    K = _EMA(rsv, (M1 * 2 - 1))
    D = _EMA(K, (M2 * 2 - 1))
    J = K * 3 - D * 2
    return K, D, J

def _RSI_2D(CLOSE, N=24):
    DIF = CLOSE - _REF(CLOSE, 1)
    abs_dif_sma = _SMA(_ABS(DIF), N)
    with np.errstate(divide='ignore', invalid='ignore'):
        rsi_value = _SMA(_MAX(DIF, 0), N) / abs_dif_sma * 100
    rsi_value = np.where(abs_dif_sma == 0, 50, rsi_value)
    return _R(rsi_value),

def _BOLL_2D(CLOSE, N=20, P=2):
    MID = _MA(CLOSE, N)
    UPPER = MID + _STD(CLOSE, N) * P
    LOWER = MID - _STD(CLOSE, N) * P
    return _R(UPPER), _R(MID), _R(LOWER)

def _BIAS_2D(CLOSE, L1=6, L2=12, L3=24):
    BIAS1 = (CLOSE - _MA(CLOSE, L1)) / _MA(CLOSE, L1) * 100
    BIAS2 = (CLOSE - _MA(CLOSE, L2)) / _MA(CLOSE, L2) * 100
    BIAS3 = (CLOSE - _MA(CLOSE, L3)) / _MA(CLOSE, L3) * 100
    return _R(BIAS1), _R(BIAS2), _R(BIAS3)

def _PSY_2D(CLOSE, N=12, M=6):
    PSY = _COUNT(CLOSE > _REF(CLOSE, 1), N) / N * 100
    PSYMA = _MA(PSY, M)
    return _R(PSY), _R(PSYMA)

def _TRIX_2D(CLOSE, M1=12, M2=20):
    TR = _EMA(_EMA(_EMA(CLOSE, M1), M1), M1)
    TRIX = (TR - _REF(TR, 1)) / _REF(TR, 1) * 100
    TRMA = _MA(TRIX, M2)
    return TRIX, TRMA

def _DPO_2D(CLOSE, M1=20, M2=10, M3=6):
    DPO = CLOSE - _REF(_MA(CLOSE, M1), M2)
    MADPO = _MA(DPO, M3)
    return DPO, MADPO

def _MTM_2D(CLOSE, N=12, M=6):
    MTM = CLOSE - _REF(CLOSE, N)
    MTMMA = _MA(MTM, M)
    return MTM, MTMMA

def _ROC_2D(CLOSE, N=12, M=6):
    ROC = 100 * (CLOSE - _REF(CLOSE, N)) / _REF(CLOSE, N)
    MAROC = _MA(ROC, M)
    return ROC, MAROC

def _EXPMA_2D(CLOSE, N1=12, N2=50):
    return _EMA(CLOSE, N1), _EMA(CLOSE, N2)

def _BBI_2D(CLOSE, M1=3, M2=6, M3=12, M4=20):
    return (_MA(CLOSE, M1) + _MA(CLOSE, M2) + _MA(CLOSE, M3) + _MA(CLOSE, M4)) / 4,

def _DFMA_2D(CLOSE, N1=10, N2=50, M=10):
    DIF = _MA(CLOSE, N1) - _MA(CLOSE, N2)
    DIFMA = _MA(DIF, M)
    return DIF, DIFMA

def _DMI_2D(CLOSE, HIGH, LOW, M1=14, M2=6):
    TR = _SUM(_MAX(_MAX(HIGH - LOW, _ABS(HIGH - _REF(CLOSE, 1))), _ABS(LOW - _REF(CLOSE, 1))), M1)
    HD = HIGH - _REF(HIGH, 1)
    LD = _REF(LOW, 1) - LOW
    DMP = _SUM(_IF((HD > 0) & (HD > LD), HD, 0), M1)
    DMM = _SUM(_IF((LD > 0) & (LD > HD), LD, 0), M1)
    with np.errstate(divide='ignore', invalid='ignore'):
        PDI = DMP * 100 / TR
        MDI = DMM * 100 / TR
    ADX = _MA(_ABS(MDI - PDI) / (PDI + MDI) * 100, M2)
    ADXR = (ADX + _REF(ADX, M2)) / 2
    return PDI, MDI, ADX, ADXR

def _ATR_2D(CLOSE, HIGH, LOW, N=20):
    TR = _MAX(_MAX((HIGH - LOW), _ABS(_REF(CLOSE, 1) - HIGH)), _ABS(_REF(CLOSE, 1) - LOW))
    return _MA(TR, N),

def _WR_2D(CLOSE, HIGH, LOW, N=10, N1=6):
    high_n = _HHV(HIGH, N)
    low_n = _LLV(LOW, N)
    hl_diff = high_n - low_n
    with np.errstate(divide='ignore', invalid='ignore'):
        wr = (high_n - CLOSE) / hl_diff * 100
    wr = np.where(hl_diff == 0, 50, wr)
    high_n1 = _HHV(HIGH, N1)
    low_n1 = _LLV(LOW, N1)
    hl_diff1 = high_n1 - low_n1
    with np.errstate(divide='ignore', invalid='ignore'):
        wr1 = (high_n1 - CLOSE) / hl_diff1 * 100
    wr1 = np.where(hl_diff1 == 0, 50, wr1)
    return _R(wr), _R(wr1)

def _CCI_2D(CLOSE, HIGH, LOW, N=14):
    TP = (HIGH + LOW + CLOSE) / 3
    return (TP - _MA(TP, N)) / (0.015 * _AVEDEV(TP, N)),

def _CR_2D(CLOSE, HIGH, LOW, N=20):
    MID = _REF(HIGH + LOW + CLOSE, 1) / 3
    return _SUM(_MAX(0, HIGH - MID), N) / _SUM(_MAX(0, MID - LOW), N) * 100,

def _KTN_2D(CLOSE, HIGH, LOW, N=20, M=10):
    MID = _EMA((HIGH + LOW + CLOSE) / 3, N)
    ATRN = _ATR_2D(CLOSE, HIGH, LOW, M)[0]
    UPPER = MID + 2 * ATRN
    LOWER = MID - 2 * ATRN
    return UPPER, MID, LOWER

def _XSII_2D(CLOSE, HIGH, LOW, N=102, M=7):
    AA = _MA((2 * CLOSE + HIGH + LOW) / 4, 5)
    TD1 = AA * N / 100
    TD2 = AA * (200 - N) / 100
    CC = _ABS((2 * CLOSE + HIGH + LOW) / 4 - _MA(CLOSE, 20)) / _MA(CLOSE, 20)
    # DMA with array input — 2D version using numpy broadcast
    cc_arr = np.where(np.isnan(CC), 1.0, CC)
    DD = np.zeros_like(CLOSE)
    DD[0] = CLOSE[0]
    for i in range(1, len(CLOSE)):
        DD[i] = cc_arr[i] * CLOSE[i] + (1 - cc_arr[i]) * DD[i - 1]
    TD3 = (1 + M / 100) * DD
    TD4 = (1 - M / 100) * DD
    return TD1, TD2, TD3, TD4

def _OBV_2D(CLOSE, VOL):
    return _SUM(_IF(CLOSE > _REF(CLOSE, 1), VOL,
                    _IF(CLOSE < _REF(CLOSE, 1), -VOL, 0)), 0) / 10000,

def _VR_2D(CLOSE, VOL, M1=26):
    LC = _REF(CLOSE, 1)
    return _SUM(_IF(CLOSE > LC, VOL, 0), M1) / _SUM(_IF(CLOSE <= LC, VOL, 0), M1) * 100,

def _EMV_2D(HIGH, LOW, VOL, N=14, M=9):
    VOLUME = _MA(VOL, N) / VOL
    MID = 100 * (HIGH + LOW - _REF(HIGH + LOW, 1)) / (HIGH + LOW)
    EMV = _MA(MID * VOLUME * (HIGH - LOW) / _MA(HIGH - LOW, N), N)
    MAEMV = _MA(EMV, M)
    return EMV, MAEMV

def _MASS_2D(HIGH, LOW, N1=9, N2=25, M=6):
    MASS = _SUM(_MA(HIGH - LOW, N1) / _MA(_MA(HIGH - LOW, N1), N1), N2)
    MA_MASS = _MA(MASS, M)
    return MASS, MA_MASS

def _MFI_2D(CLOSE, HIGH, LOW, VOL, N=14):
    TYP = (HIGH + LOW + CLOSE) / 3
    V1 = _SUM(_IF(TYP > _REF(TYP, 1), TYP * VOL, 0), N) / _SUM(_IF(TYP < _REF(TYP, 1), TYP * VOL, 0), N)
    return 100 - (100 / (1 + V1)),

def _BRAR_2D(OPEN, CLOSE, HIGH, LOW, M1=26):
    AR = _SUM(HIGH - OPEN, M1) / _SUM(OPEN - LOW, M1) * 100
    BR = _SUM(_MAX(0, HIGH - _REF(CLOSE, 1)), M1) / _SUM(_MAX(0, _REF(CLOSE, 1) - LOW), M1) * 100
    return AR, BR

def _ASI_2D(OPEN, CLOSE, HIGH, LOW, M1=26, M2=10):
    LC = _REF(CLOSE, 1)
    AA = _ABS(HIGH - LC)
    BB = _ABS(LOW - LC)
    CC = _ABS(HIGH - _REF(LOW, 1))
    DD = _ABS(LC - _REF(OPEN, 1))
    R = _IF((AA > BB) & (AA > CC), AA + BB / 2 + DD / 4,
            _IF((BB > CC) & (BB > AA), BB + AA / 2 + DD / 4, CC + DD / 4))
    X = (CLOSE - LC + (CLOSE - OPEN) / 2 + LC - _REF(OPEN, 1))
    SI = 16 * X / R * _MAX(AA, BB)
    ASI = _SUM(SI, M1)
    ASIT = _MA(ASI, M2)
    return ASI, ASIT

def _ZHUOYAO_2D(CLOSE, N1=120, N2=60, N3=20, M=10):
    LONG1 = (CLOSE / _REF(CLOSE, N1) - 1) * 100
    LONG = _EMA(LONG1, M)
    MID = (CLOSE / _REF(CLOSE, N2) - 1) * 100
    SHORT = (CLOSE / _REF(CLOSE, N3) - 1) * 100
    TREND = _EMA(MID, M)
    return _R(LONG), _R(MID), _R(SHORT), _R(TREND)

def _BIAS_SIGNAL_2D(CLOSE, P=10, M=30):
    X = (CLOSE - _MA(CLOSE, M)) / _MA(CLOSE, M) * 100
    S_SMA = _MA(X, P)
    X_LMA = _MA(X, M)
    return _R(X), _R(S_SMA), _R(X_LMA)

def _TAQ_2D(HIGH, LOW, N=20):
    UP = _HHV(HIGH, N)
    DOWN = _LLV(LOW, N)
    MID = (UP + DOWN) / 2
    return UP, MID, DOWN


# ============================================================
# Indicator registry
# ============================================================

_BATCH_SPECS: list[tuple[str, object, tuple[str, ...], tuple[str, ...], dict]] = [
    # (name, func, input_cols, output_cols, params)
    ("MACD", _MACD_2D, ("close",), ("MACD_DIF", "MACD_DEA", "MACD_HIST"), {}),
    ("KDJ", _KDJ_2D, ("close", "high", "low"), ("KDJ_K", "KDJ_D", "KDJ_J"), {}),
    ("RSI", _RSI_2D, ("close",), ("RSI",), {}),
    ("BOLL", _BOLL_2D, ("close",), ("BOLL_UPPER", "BOLL_MID", "BOLL_LOWER"), {}),
    ("BIAS", _BIAS_2D, ("close",), ("BIAS1", "BIAS2", "BIAS3"), {}),
    ("PSY", _PSY_2D, ("close",), ("PSY", "PSY_MA"), {}),
    ("TRIX", _TRIX_2D, ("close",), ("TRIX", "TRIX_MA"), {}),
    ("DPO", _DPO_2D, ("close",), ("DPO", "DPO_MA"), {}),
    ("MTM", _MTM_2D, ("close",), ("MTM", "MTM_MA"), {}),
    ("ROC", _ROC_2D, ("close",), ("ROC", "ROC_MA"), {}),
    ("EXPMA", _EXPMA_2D, ("close",), ("EXPMA_12", "EXPMA_50"), {}),
    ("BBI", _BBI_2D, ("close",), ("BBI",), {}),
    ("DFMA", _DFMA_2D, ("close",), ("DFMA_DIF", "DFMA_DMA"), {}),
    ("DMI", _DMI_2D, ("close", "high", "low"), ("DMI_PDI", "DMI_MDI", "DMI_ADX", "DMI_ADXR"), {}),
    ("ATR", _ATR_2D, ("close", "high", "low"), ("ATR",), {}),
    ("WR", _WR_2D, ("close", "high", "low"), ("WR1", "WR2"), {}),
    ("CCI", _CCI_2D, ("close", "high", "low"), ("CCI",), {}),
    ("CR", _CR_2D, ("close", "high", "low"), ("CR",), {}),
    ("KTN", _KTN_2D, ("close", "high", "low"), ("KTN_UPPER", "KTN_MID", "KTN_LOWER"), {}),
    ("XSII", _XSII_2D, ("close", "high", "low"), ("XSII_TD1", "XSII_TD2", "XSII_TD3", "XSII_TD4"), {}),
    ("OBV", _OBV_2D, ("close", "volume"), ("OBV",), {}),
    ("VR", _VR_2D, ("close", "volume"), ("VR",), {}),
    ("EMV", _EMV_2D, ("high", "low", "volume"), ("EMV", "EMV_MA"), {}),
    ("MASS", _MASS_2D, ("high", "low"), ("MASS", "MASS_MA"), {}),
    ("MFI", _MFI_2D, ("close", "high", "low", "volume"), ("MFI",), {}),
    ("BRAR", _BRAR_2D, ("open", "close", "high", "low"), ("AR", "BR"), {}),
    ("ASI", _ASI_2D, ("open", "close", "high", "low"), ("ASI", "ASI_MA"), {}),
    ("ZHUOYAO", _ZHUOYAO_2D, ("close",), ("ZY_LONG", "ZY_MID", "ZY_SHORT", "ZY_TREND"), {}),
    ("BIAS_SIGNAL", _BIAS_SIGNAL_2D, ("close",), ("BS_X", "BS_SMA", "BS_LMA"), {}),
    ("TAQ", _TAQ_2D, ("high", "low"), ("TAQ_UP", "TAQ_MID", "TAQ_DOWN"), {}),
]


def _resample_ohlcv(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Resample daily OHLCV to weekly or monthly bars.

    Args:
        df: [symbol, date, open, high, low, close, volume] — daily bars
        freq: 'D' (passthrough), 'W' (Fri close), 'M' (month-end close)

    Returns:
        Resampled DataFrame with same columns.
    """
    if freq == 'D':
        return df

    rule = 'W-FRI' if freq == 'W' else 'ME'
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])

    result = (
        df.set_index('date')
        .groupby('symbol')
        .resample(rule)
        .agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        })
        .reset_index()
    )
    # Forward-fill NaN gaps (holidays, suspensions) per symbol.
    # Without this, a single NaN row poisons rolling(N) windows.
    # Price (OHLC) carries forward — the "last known price" is still valid.
    # Volume resets to 0 for holiday weeks (no trading = no volume).
    ohlc_cols = ['open', 'high', 'low', 'close']
    nan_mask = result['close'].isna()                     # remember holiday rows
    result[ohlc_cols] = result.groupby('symbol')[ohlc_cols].ffill()
    result['volume'] = result.groupby('symbol')['volume'].ffill()
    result.loc[nan_mask, 'volume'] = 0.0                   # holidays → 0 volume
    # Drop rows still NaN after ffill (before first data point)
    result = result.dropna(subset=['close'])
    result['date'] = result['date'].dt.date
    return result[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]


def compute_indicators_batch(df_all: pd.DataFrame, trade_date: date, freq: str = 'D') -> list[dict]:
    """Compute all 30 indicators for all stocks via 2D matrix operations.

    Core insight: instead of N per-stock calls to MyTT functions (each on a
    250-row 1D array), pivot to a (250, N) matrix and compute each indicator
    once across all columns.  This turns ~165,000 small pandas rolling/ewm
    calls into ~40 big ones, reducing Python overhead and keeping pandas in
    its C extension (which releases the GIL) for longer stretches.

    Args:
        df_all: DataFrame with columns [symbol, date, open, high, low, close, volume]
        trade_date: date to stamp results with

    Returns:
        list of dicts with keys [symbol, date, INDICATOR_COLUMNS...]
    """
    # Normalize column names
    if "vol" in df_all.columns and "volume" not in df_all.columns:
        df_all = df_all.rename(columns={"vol": "volume"})

    # Pivot all needed columns into (time, stocks) matrices
    # Keep the pivot DataFrames so we can extract symbol names from columns
    pivots: dict[str, np.ndarray] = {}
    _pivot_close = df_all.pivot(index="date", columns="symbol", values="close")
    _pivot_close = _pivot_close.sort_index()
    symbols = list(_pivot_close.columns)  # stock codes, preserved from pivot

    pivots["close"] = _pivot_close.values.astype(float)
    pivots["open"]   = df_all.pivot(index="date", columns="symbol", values="open").sort_index().values.astype(float)
    pivots["high"]   = df_all.pivot(index="date", columns="symbol", values="high").sort_index().values.astype(float)
    pivots["low"]    = df_all.pivot(index="date", columns="symbol", values="low").sort_index().values.astype(float)
    pivots["volume"] = df_all.pivot(index="date", columns="symbol", values="volume").sort_index().values.astype(float)

    # Compute each indicator → extract latest row per stock.
    # Also keep second-to-last row for cross-detection (MACD_DIF, MACD_DEA).
    latest_arrays: dict[str, np.ndarray] = {}
    prev_dif: np.ndarray | None = None
    prev_dea: np.ndarray | None = None

    for name, func, inputs, outputs, params in _BATCH_SPECS:
        args = [pivots[c] for c in inputs]
        try:
            raw = func(*args, **params)
        except Exception:
            logger.debug("Batch indicator %s failed", name, exc_info=True)
            continue
        if not isinstance(raw, tuple):
            raw = (raw,)
        for out_name, arr in zip(outputs, raw):
            if arr.ndim == 2:
                latest_arrays[out_name] = arr[-1]  # (stocks,) — latest time step
                # Save second-to-last for MACD cross detection
                if out_name == "MACD_DIF":
                    prev_dif = arr[-2] if arr.shape[0] >= 2 else None
                elif out_name == "MACD_DEA":
                    prev_dea = arr[-2] if arr.shape[0] >= 2 else None
            else:
                latest_arrays[out_name] = arr

    # Build result rows — compute MACD cross flag from full history
    rows: list[dict] = []
    for i, sym in enumerate(symbols):
        row: dict = {"symbol": sym, "date": trade_date, "freq": freq}
        has_data = False
        for out_name, latest in latest_arrays.items():
            val = latest[i]
            if not np.isnan(val):
                row[out_name] = float(val)
                has_data = True

        # MACD cross detection: golden=1, dead=-1, none=0
        dif_cur = row.get("MACD_DIF")
        dea_cur = row.get("MACD_DEA")
        if dif_cur is not None and dea_cur is not None \
           and prev_dif is not None and prev_dea is not None:
            pd_val = prev_dif[i]
            pdv_val = prev_dea[i]
            if not np.isnan(pd_val) and not np.isnan(pdv_val):
                if dif_cur > dea_cur and pd_val <= pdv_val:
                    row["MACD_CROSS"] = 1   # golden cross
                elif dif_cur < dea_cur and pd_val >= pdv_val:
                    row["MACD_CROSS"] = -1  # dead cross
                else:
                    row["MACD_CROSS"] = 0
                has_data = True

        if has_data:
            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Main fetch
# ---------------------------------------------------------------------------


@register_fetcher(
    "indicator_values",
    depends_on=["daily_ohlcv"],
    group="signals",
    description="30项技术指标 (MyTT 2D batch) — 依赖 daily_ohlcv",
)
def fetch(db: IngestionDB, config: Config, trade_date: date) -> int:
    """Compute indicators for all eligible stocks and store to indicator_values.

    - Backfill (config._backfill=True): all stocks with >= 120 OHLCV bars.
    - Incremental: only stocks with OHLCV rows for trade_date.
    - D/W/M frequencies run sequentially (ThreadPoolExecutor was tested but
      slower — GIL contention across pivot/groupby/ffill in 3× pandas workloads).
    """
    if getattr(config, "_backfill", False):
        df = db.conn.execute(
            "SELECT symbol FROM daily_ohlcv "
            "GROUP BY symbol HAVING COUNT(*) >= ?",
            [MIN_BARS],
        ).fetchdf()
        symbols = df["symbol"].tolist()
        logger.info("indicator_values: backfill — %d eligible stocks", len(symbols))
    else:
        df = db.conn.execute(
            "SELECT DISTINCT symbol FROM daily_ohlcv WHERE date = ?",
            [trade_date],
        ).fetchdf()
        symbols = df["symbol"].tolist()
        if symbols:
            placeholders = ",".join(["?"] * len(symbols))
            df = db.conn.execute(
                f"SELECT symbol FROM daily_ohlcv "
                f"WHERE symbol IN ({placeholders}) "
                f"GROUP BY symbol HAVING COUNT(*) >= ?",
                symbols + [MIN_BARS],
            ).fetchdf()
            symbols = df["symbol"].tolist()
        logger.info(
            "indicator_values: incremental — %d stocks updated today "
            "(with >= %d bars)", len(symbols), MIN_BARS,
        )

    if not symbols:
        return 0

    total = 0

    for freq in ('D', 'W', 'M'):
        freq_total = 0

        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i + BATCH]
            placeholders = ",".join(["?"] * len(batch))
            lookback = 500 if freq == 'M' else LOOKBACK_DAYS
            start_date = trade_date - pd.Timedelta(days=lookback)

            df = db.conn.execute(f"""
                SELECT symbol, date, open, high, low, close, volume
                FROM daily_ohlcv
                WHERE symbol IN ({placeholders}) AND date >= ?
                ORDER BY symbol, date
            """, batch + [start_date]).fetchdf()

            if df.empty:
                continue

            # Resample daily → weekly/monthly if needed
            df_freq = _resample_ohlcv(df, freq)

            # Filter to stocks with enough bars after resample
            bar_counts = df_freq.groupby('symbol').size()
            min_bars = MIN_BARS_W if freq == 'W' else MIN_BARS_M
            valid_symbols = bar_counts[bar_counts >= min_bars].index
            if len(valid_symbols) == 0:
                continue
            df_freq = df_freq[df_freq['symbol'].isin(valid_symbols)]

            # Batch compute via 2D matrix operations
            rows = compute_indicators_batch(df_freq, trade_date, freq=freq)

            if rows:
                df_result = pd.DataFrame(rows)
                keep_cols = ["symbol", "date", "freq"] + [
                    c for c in df_result.columns if c not in ("symbol", "date", "freq")
                ]
                df_result = df_result[keep_cols]
                db.upsert_dataframe("indicator_values", df_result)
                freq_total += len(rows)

        total += freq_total
        logger.info("indicator_values[%s]: %d stocks", freq, freq_total)

    return total
