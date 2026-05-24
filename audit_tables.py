"""A股数据仓库 — 逐表数据质量审核

用法: python audit_tables.py
输出: 每张表的行数、抽样、异常检测
"""
import logging
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

# 确保能找到 src
sys.path.insert(0, str(Path(__file__).parent / "."))

logging.basicConfig(level=logging.WARNING)
from src.ingestion.config import load_config
from src.ingestion.db import IngestionDB

cfg = load_config("./config.yaml")
DB = os.path.join(tempfile.gettempdir(), f"audit_full_{os.urandom(4).hex()}.duckdb")
db = IngestionDB(DB)
td = date.today()

def q(sql):
    return db.conn.execute(sql).fetchdf()

def run_fetcher(module_path, table_name):
    """Run a fetcher and return (rows_written, total_count, error)."""
    try:
        mod = __import__(module_path, fromlist=["fetch"])
        n = mod.fetch(db, cfg, td)
        c = db.count(table_name)
        return n, c, None
    except Exception as e:
        try:
            c = db.count(table_name)
        except:
            c = 0
        return 0, c, str(e)[:80]

def audit(table, module=None, sample_cols="*"):
    """Audit a single table."""
    n, total, err = run_fetcher(module or f"src.ingestion.fetchers.{table}", table)
    status = "✅" if err is None else "❌"
    print(f"\n{status} {table}")
    print(f"      写入: {n}, 总计: {total}")
    if err:
        print(f"      错误: {err}")
    if total > 0:
        # 列信息
        try:
            cols = q(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' AND table_schema='main'")["column_name"].tolist()
            print(f"      列: {cols}")
        except:
            pass
        # 抽样
        try:
            df = q(f"SELECT {sample_cols} FROM \"{table}\" LIMIT 3")
            print(f"      样本:")
            for _, row in df.iterrows():
                print(f"        {dict(row)}")
        except Exception as e:
            print(f"      抽样错误: {e}")
    return status, total

# ===== 开始逐表审核 =====
print(f"{'='*60}")
print(f"A 股数据仓库 — 数据质量审核")
print(f"DB: {DB}")
print(f"日期: {td}")
print(f"{'='*60}")

results = []

# === P0: 核心行情 ===
results.append(audit("trade_calendar", sample_cols="date, is_trading"))
results.append(audit("stock_universe", sample_cols="symbol, name, market"))
results.append(audit("stock_classification", sample_cols="symbol, industry, region"))
results.append(audit("concept_blocks", sample_cols="symbol, concept_name"))

# === P0: OHLCV ===
print("\n--- daily_ohlcv (分批, 只测5只) ---")
from src.ingestion.fetchers.daily_ohlcv import _fetch_single_opentdx
for sym in ["600519", "000001", "300750", "920000"]:
    rows = _fetch_single_opentdx(sym, 5)
    print(f"  {sym}: {len(rows)} days")
    if rows:
        print(f"    {rows[0]['date']}~{rows[-1]['date']}, close: {rows[-1]['close']}, vol: {rows[-1]['volume']}")

# === P1: 估值/资金 ===
results.append(audit("daily_valuation"))
results.append(audit("northbound_flow"))
results.append(audit("holder_count", sample_cols="stock_code, end_date, holder_count"))

# === P2: 信号 ===
results.append(audit("xdxr_events", sample_cols="stock_code, ex_date, bonus_ratio"))
results.append(audit("block_trades", sample_cols="stock_code, trade_date, price, volume"))
results.append(audit("lockup_calendar", sample_cols="stock_code, unlock_date, unlock_ratio"))
results.append(audit("hot_reasons"))
results.append(audit("hot_stocks"))

# === akshare 类需要单独处理 ===
print("\n--- akshare 表 (独立测试) ---")
import akshare as ak

# dragon_tiger
try:
    start = (td - timedelta(days=7)).isoformat()
    df = ak.stock_lhb_detail_em(start_date=start, end_date=td.isoformat())
    if df is not None and not df.empty:
        print(f"  dragon_tiger: {len(df)} rows (akshare)")
        print(f"    cols: {list(df.columns)[:8]}")
except Exception as e:
    print(f"  dragon_tiger: {e}")

# margin SSE
try:
    df = ak.stock_margin_detail_sse()
    if df is not None and not df.empty:
        print(f"  margin SSE: {len(df)} rows (akshare)")
        print(f"    cols: {list(df.columns)}")
except Exception as e:
    print(f"  margin SSE: {e}")

# margin SZSE
try:
    df = ak.stock_margin_detail_szse()
    if df is not None and not df.empty:
        print(f"  margin SZSE: {len(df)} rows (akshare)")
        print(f"    cols: {list(df.columns)}")
except Exception as e:
    print(f"  margin SZSE: {e}")

# fundamentals
try:
    from src.ingestion.fetchers.fundamentals import _quarter_end
    qe = _quarter_end(td)
    df = ak.stock_yjbb_em(date=qe.replace("-", ""))
    if df is not None and not df.empty:
        print(f"  fundamentals: {len(df)} rows (akshare, quarter={qe})")
        print(f"    cols: {list(df.columns)[:10]}")
except Exception as e:
    print(f"  fundamentals: {e}")

# board_daily
try:
    df_ind = ak.stock_board_industry_name_em()
    df_con = ak.stock_board_concept_name_em()
    if df_ind is not None:
        print(f"  board_daily industry: {len(df_ind)} rows")
    if df_con is not None:
        print(f"  board_daily concept: {len(df_con)} rows")
except Exception as e:
    print(f"  board_daily: {e}")

# capital_flow (单只验证)
try:
    from src.ingestion.fetchers.capital_flow import _fetch_single_flow
    result = _fetch_single_flow("000001", 0)
    print(f"  capital_flow (000001): {result is not None}")
    if result:
        print(f"    net_main={result.get('net_main')}, net_super={result.get('net_super')}")
except Exception as e:
    print(f"  capital_flow: {e}")

# === global_markets ===
print("\n--- global_markets ---")
try:
    from src.ingestion.fetchers.global_markets import _fetch_global_markets
    df = _fetch_global_markets()
    if not df.empty:
        print(f"  global_markets: {len(df)} rows, {df['symbol'].nunique()} symbols")
        print(f"    symbols: {df['symbol'].unique().tolist()}")
except Exception as e:
    print(f"  global_markets: {e}")

# === 汇总 ===
print(f"\n{'='*60}")
print(f"📊 审核汇总")
print(f"{'='*60}")
print(f"DB 路径: {DB}")
ok = sum(1 for s, t in results if s == "✅")
fail = sum(1 for s, t in results if s == "❌")
print(f"通过: {ok}/{ok+fail}")
print(f"失败: {fail}")
print(f"DB 大小: {os.path.getsize(DB)/1024:.0f} KB")

# 总行数
stats = db.table_stats()
total_rows = sum(stats.values())
print(f"总数据行: {total_rows:,}")
for name, cnt in sorted(stats.items()):
    if cnt > 0:
        print(f"  {name:<25} {cnt:>10,} 行")

db.close()
print(f"\n💾 保留 DB 文件供后续查看: {DB}")
