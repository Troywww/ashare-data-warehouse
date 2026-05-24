"""Web 控制面板 — API 路由与页面路由."""

import json
import logging
import threading
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from flask import Blueprint, Flask, Response, jsonify, render_template, request

from src.ingestion.config import load_config
from src.ingestion.db import IngestionDB
from src.ingestion.engine import DailyUpdateEngine
from src.ingestion.fetchers import FETCHER_REGISTRY

logger = logging.getLogger(__name__)

bp = Blueprint("web", __name__, url_prefix="/")

# 全局状态
_config_path: str = "config.yaml"
_running_tasks: dict[str, dict] = {}
_task_counter: int = 0
_lock = threading.Lock()

# 表分类与顺序（按业务层级排列）
TABLE_CATEGORIES = [
    {"name": "基础数据", "icon": "📁", "tables": ["trade_calendar", "stock_universe", "stock_classification", "concept_blocks"]},
    {"name": "行情数据", "icon": "📈", "tables": ["daily_ohlcv", "daily_valuation", "xdxr_events"]},
    {"name": "资金流向", "icon": "💰", "tables": ["capital_flow", "northbound_flow", "margin_trading"]},
    {"name": "市场信号", "icon": "🔔", "tables": ["dragon_tiger", "board_daily", "hot_stocks", "hot_reasons", "block_trades", "lockup_calendar"]},
    {"name": "财务数据", "icon": "📄", "tables": ["fundamentals", "holder_count"]},
    {"name": "外围市场", "icon": "🌍", "tables": ["global_markets"]},
]

# 哪些表支持历史回补
BACKFILL_TABLES = {"daily_ohlcv", "daily_valuation"}


def init_app(app: Flask) -> None:
    global _config_path
    _config_path = app.config.get("CONFIG_PATH", "config.yaml")


# ---------------------------------------------------------------------------
# 页面路由
# ---------------------------------------------------------------------------


@bp.route("/")
def dashboard():
    return render_template("dashboard.html")


@bp.route("/update")
def update_page():
    # 将 TABLE_CATEGORIES 转为包含 supports_backfill 的完整结构
    categories = []
    for cat in TABLE_CATEGORIES:
        tables = []
        for name in cat["tables"]:
            entry = FETCHER_REGISTRY.get(name)
            tables.append({
                "name": name,
                "supports_backfill": name in BACKFILL_TABLES,
                "description": entry.description if entry else "",
            })
        categories.append({
            "name": cat["name"],
            "icon": cat["icon"],
            "tables": tables,
        })
    return render_template("update.html", categories=categories)


@bp.route("/search")
def search_page():
    tables = sorted(FETCHER_REGISTRY.keys())
    return render_template("search.html", tables=tables)


@bp.route("/schedule")
def schedule_page():
    return render_template("schedule.html")


# ---------------------------------------------------------------------------
# API — 数据统计
# ---------------------------------------------------------------------------


@bp.route("/api/stats")
def api_stats():
    try:
        cfg = load_config(_config_path)
        engine = DailyUpdateEngine(cfg)
        stats = engine.status()
        db = IngestionDB(cfg.db_path)
        db_size = db.get_db_size()
        db.close()

        # 各表最新日期
        max_dates = {}
        for name in stats:
            try:
                d = db.get_max_date(name)
                max_dates[name] = d.isoformat() if d else None
            except Exception:
                max_dates[name] = None

        # 进度文件
        progress = {}
        progress_path = Path(cfg.data_dir) / ".progress.json"
        if progress_path.exists():
            try:
                progress = json.loads(progress_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        return jsonify({
            "rows": stats,
            "total_rows": sum(stats.values()),
            "db_size": db_size,
            "db_size_human": _human_size(db_size),
            "max_dates": max_dates,
            "progress": progress,
            "table_count": len(stats),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/tables")
def api_tables():
    try:
        cfg = load_config(_config_path)
        db = IngestionDB(cfg.db_path)

        # 读取进度文件
        progress_data = {}
        progress_path = Path(cfg.data_dir) / ".progress.json"
        if progress_path.exists():
            try:
                progress_data = json.loads(progress_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        table_progress = progress_data.get("tables", {})

        # 按分类顺序组织数据
        categories = []
        for cat in TABLE_CATEGORIES:
            items = []
            for name in cat["tables"]:
                entry = FETCHER_REGISTRY.get(name)
                try:
                    count = db.count(name)
                    max_date = db.get_max_date(name)
                except Exception:
                    count = 0
                    max_date = None

                prog = table_progress.get(name, {})
                items.append({
                    "name": name,
                    "rows": count,
                    "max_date": max_date.isoformat() if max_date else None,
                    "description": entry.description if entry else "",
                    "supports_backfill": name in BACKFILL_TABLES,
                    "last_update": prog.get("last_update"),
                    "last_rows": prog.get("rows"),
                    "last_elapsed": prog.get("elapsed_sec"),
                    "last_error": prog.get("error"),
                })
            categories.append({
                "name": cat["name"],
                "icon": cat["icon"],
                "tables": items,
            })

        db.close()
        return jsonify({"categories": categories})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# API — 触发更新
# ---------------------------------------------------------------------------


def _run_update(tables: list[str] | None, backfill: bool, task_id: str):
    """在后台线程中运行更新任务."""
    try:
        cfg = load_config(_config_path)
        engine = DailyUpdateEngine(cfg)

        with _lock:
            _running_tasks[task_id]["status"] = "running"
            _running_tasks[task_id]["progress"] = "initializing"

        if backfill:
            results = engine.run_backfill(tables=tables)
        else:
            results = engine.run_daily_update(tables=tables)

        ok = sum(1 for r in results if r.error is None and not r.skipped)
        failed = [r for r in results if r.error]
        total_rows = sum(r.rows for r in results)

        with _lock:
            _running_tasks[task_id]["status"] = "done"
            _running_tasks[task_id]["progress"] = "completed"
            _running_tasks[task_id]["rows"] = total_rows
            _running_tasks[task_id]["results"] = [
                {"name": r.name, "rows": r.rows, "elapsed": round(r.elapsed, 1),
                 "error": r.error, "skipped": r.skipped}
                for r in results
            ]
            _running_tasks[task_id]["ok"] = ok
            _running_tasks[task_id]["failed"] = len(failed)
            if failed:
                _running_tasks[task_id]["error"] = failed[0].error
    except Exception as e:
        with _lock:
            _running_tasks[task_id]["status"] = "error"
            _running_tasks[task_id]["error"] = str(e)


@bp.route("/api/update", methods=["POST"])
def api_update():
    global _task_counter
    data = request.get_json(silent=True) or {}
    tables = data.get("tables")  # None = all, or list of names
    backfill = data.get("backfill", False)

    with _lock:
        _task_counter += 1
        task_id = f"task_{_task_counter}"
        _running_tasks[task_id] = {
            "id": task_id,
            "name": "backfill" if backfill else "daily-update",
            "status": "pending",
            "tables": tables or "all",
            "backfill": backfill,
            "progress": "",
            "rows": 0,
            "error": None,
            "results": None,
            "ok": 0,
            "failed": 0,
        }

    thread = threading.Thread(target=_run_update, args=(tables, backfill, task_id), daemon=True)
    thread.start()

    return jsonify({"task_id": task_id})


@bp.route("/api/task/<task_id>")
def api_task_status(task_id: str):
    with _lock:
        task = _running_tasks.get(task_id)
    if task is None:
        return jsonify({"error": "task not found"}), 404
    return jsonify(task)


@bp.route("/api/tasks")
def api_tasks():
    with _lock:
        tasks = list(_running_tasks.values())
    return jsonify(sorted(tasks, key=lambda t: t["id"], reverse=True)[:50])


# ---------------------------------------------------------------------------
# API — 数据查询
# ---------------------------------------------------------------------------


@bp.route("/api/query", methods=["POST"])
def api_query():
    try:
        data = request.get_json(silent=True) or {}
        sql = data.get("sql", "").strip()
        if not sql:
            return jsonify({"error": "SQL is required"}), 400

        sql_upper = sql.upper().strip()
        if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
            return jsonify({"error": "Only SELECT queries are allowed"}), 403

        cfg = load_config(_config_path)
        db = IngestionDB(cfg.db_path)
        t0 = time.perf_counter()
        df = db.conn.execute(sql).fetchdf()
        elapsed = time.perf_counter() - t0
        db.close()

        return jsonify({
            "columns": list(df.columns),
            "rows": _df_to_json(df.head(1000)),
            "total": len(df),
            "elapsed": round(elapsed, 3),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/quick-search", methods=["POST"])
def api_quick_search():
    """按股票代码 + 表名快速搜索."""
    try:
        data = request.get_json(silent=True) or {}
        table = data.get("table", "daily_ohlcv")
        symbol = data.get("symbol", "").strip()
        limit = min(int(data.get("limit", 100)), 1000)

        where = ""
        params = []
        if symbol:
            where = " WHERE symbol = ?"
            params.append(symbol)

        cfg = load_config(_config_path)
        db = IngestionDB(cfg.db_path)

        # Verify table exists and get correct date column
        if table not in db.TABLES:
            db.close()
            return jsonify({"error": f"Unknown table: {table}"}), 400

        date_col = db.TABLE_DATE_COLUMNS.get(table)
        if date_col:
            sql = f"SELECT * FROM {table}{where} ORDER BY {date_col} DESC LIMIT {limit}"
        else:
            sql = f"SELECT * FROM {table}{where} LIMIT {limit}"
        df = db.conn.execute(sql, params).fetchdf()
        db.close()

        return jsonify({
            "columns": list(df.columns),
            "rows": _df_to_json(df),
            "total": len(df),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# API — 调度与配置
# ---------------------------------------------------------------------------


@bp.route("/api/schedule")
def api_schedule():
    try:
        cfg = load_config(_config_path)
        groups = dict(cfg.schedule_groups)
        schedule = dict(cfg.schedule.data) if hasattr(cfg.schedule, "data") else {}

        # 自动归类：找出哪些表没在组里
        all_tables = set(FETCHER_REGISTRY.keys())
        grouped = set()
        for members in groups.values():
            grouped.update(members)
        ungrouped = sorted(all_tables - grouped)

        return jsonify({
            "schedule": {k: v for k, v in sorted(schedule.items())},
            "groups": {k: v for k, v in groups.items()},
            "ungrouped": ungrouped,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/source-toggle", methods=["POST"])
def api_source_toggle():
    """切换数据源启用/禁用（仅在当前会话生效）. """
    try:
        data = request.get_json(silent=True) or {}
        source = data.get("source", "")
        enabled = data.get("enabled", True)

        cfg = load_config(_config_path)
        if hasattr(cfg.sources, source):
            setattr(cfg.sources, source, enabled)
            return jsonify({"ok": True, "source": source, "enabled": enabled})
        return jsonify({"error": f"Unknown source: {source}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# API — 调度配置修改
# ---------------------------------------------------------------------------


@bp.route("/api/schedule/update", methods=["POST"])
def api_schedule_update():
    """更新 config.yaml 中的调度配置."""
    try:
        data = request.get_json(silent=True) or {}
        entry_name = data.get("name", "").strip()
        new_time = data.get("time", "").strip()

        if not entry_name:
            return jsonify({"error": "name is required"}), 400

        # 读取当前配置
        import yaml
        cfg_path = Path(_config_path)
        if not cfg_path.exists():
            return jsonify({"error": "config.yaml not found"}), 500

        with open(cfg_path, encoding="utf-8") as f:
            config_data = yaml.safe_load(f)

        if "schedule" not in config_data:
            config_data["schedule"] = {}

        if new_time:
            config_data["schedule"][entry_name] = new_time
        else:
            config_data["schedule"].pop(entry_name, None)

        with open(cfg_path, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f, allow_unicode=True, default_flow_style=False)

        return jsonify({"ok": True, "name": entry_name, "time": new_time or None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _df_to_json(df: pd.DataFrame) -> list[dict]:
    """DataFrame 转 JSON，处理 NaN/Inf 等非法 JSON 值."""
    """DataFrame 转 JSON，处理 NaN/NaT/Inf 等非法 JSON 值。"""
    df = df.copy()
    # object 化后替换所有 NA 值（包括 NaN / NaT / None）
    df = df.astype(object).where(pd.notnull(df), None)
    # 替换 inf
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None if isinstance(x, float) and (x == float("inf") or x == float("-inf")) else x
        )
    return df.to_dict(orient="records")


def _human_size(bytes_: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if bytes_ < 1024:
            return f"{bytes_:.1f} {unit}"
        bytes_ /= 1024
    return f"{bytes_:.1f} TB"
