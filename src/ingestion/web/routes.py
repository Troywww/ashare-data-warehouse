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
from src.ingestion.service import DataService

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
    {"name": "市场信号", "icon": "🔔", "tables": ["dragon_tiger", "dragon_tiger_seats", "board_daily", "hot_stocks", "hot_reasons", "block_trades", "lockup_calendar"]},
    {"name": "财务数据", "icon": "📄", "tables": ["fundamentals", "holder_count", "eps_consensus", "research_reports"]},
    {"name": "股东信息", "icon": "👥", "tables": ["shareholder_changes", "announcements"]},
    {"name": "资讯快讯", "icon": "📰", "tables": ["cls_telegram", "stock_news"]},
    {"name": "技术指标", "icon": "📐", "tables": ["indicator_values"]},
    {"name": "外围市场", "icon": "🌍", "tables": ["global_markets"]},
]

# 哪些表支持历史回补
BACKFILL_TABLES = {"daily_ohlcv", "daily_valuation"}

# 初始化判定：哪些表有数据才算"已初始化"
INIT_TABLES = [
    "stock_universe", "trade_calendar", "daily_ohlcv", "daily_valuation",
]

# 哪些表是自动增量更新的（在 scheduler 中有调度配置）
AUTO_UPDATE_TABLES = {
    "core": ["stock_universe", "stock_classification", "concept_blocks",
             "daily_ohlcv", "daily_valuation", "capital_flow",
             "northbound_flow", "board_daily", "xdxr_events"],
    "signals": ["dragon_tiger", "hot_stocks", "hot_reasons", "margin_trading",
                "block_trades", "lockup_calendar", "indicator_values"],
    "global_markets": ["global_markets"],
    "trade_calendar": ["trade_calendar"],
    "holder_count": ["holder_count"],
    "fundamentals": ["fundamentals"],
}

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
    """在后台线程中运行更新任务，逐表上报进度."""
    try:
        cfg = load_config(_config_path)
        engine = DailyUpdateEngine(cfg)

        with _lock:
            _running_tasks[task_id]["status"] = "running"
            _running_tasks[task_id]["progress"] = "启动中..."
            _running_tasks[task_id]["results"] = []

        # Progress callback — called from worker threads, must be thread-safe
        def _on_progress(r):
            with _lock:
                task = _running_tasks.get(task_id)
                if task is None:
                    return
                entry = {
                    "name": r.name, "rows": r.rows,
                    "elapsed": round(r.elapsed, 1),
                    "error": r.error, "skipped": r.skipped,
                }
                task["results"].append(entry)
                task["rows"] = sum(item["rows"] for item in task["results"])
                done = len(task["results"])
                total = task.get("total_tables", done)
                if r.error:
                    task["progress"] = f"{done}/{total} ✗ {r.name}"
                elif r.skipped:
                    task["progress"] = f"{done}/{total} — {r.name} (跳过)"
                else:
                    task["progress"] = f"{done}/{total} ✓ {r.name} {r.rows}行"

        if backfill:
            results = engine.run_backfill(tables=tables, progress_callback=_on_progress)
        else:
            results = engine.run_daily_update(tables=tables, progress_callback=_on_progress)

        ok = sum(1 for r in results if r.error is None and not r.skipped)
        failed = [r for r in results if r.error]

        with _lock:
            _running_tasks[task_id]["status"] = "done"
            _running_tasks[task_id]["progress"] = f"完成 — {ok} 成功"
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
        # Calculate expected table count for progress display
        if tables:
            total_tables = len(tables)
        else:
            from src.ingestion.engine import _FETCHER_ORDER
            total_tables = len(_FETCHER_ORDER)

        _running_tasks[task_id] = {
            "id": task_id,
            "name": "backfill" if backfill else "daily-update",
            "status": "pending",
            "tables": tables or "all",
            "backfill": backfill,
            "progress": "",
            "rows": 0,
            "total_tables": total_tables,
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


@bp.route("/api/cache-stats")
def api_cache_stats():
    """查看 DataService 缓存统计."""
    try:
        cfg = load_config(_config_path)
        svc = DataService(cfg.db_path)
        stats = svc.cache_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e), "cache_enabled": False})


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

        # 解析调度时间，附加频率/策略信息
        parsed_schedule = {}
        for name, time_str in schedule.items():
            parsed = _parse_schedule_time(time_str)
            entry = {
                "raw": time_str,
                "time": parsed["time"],
                "frequency": parsed["frequency"],
                "frequency_label": parsed["frequency_label"],
                "strategy": parsed["strategy"],
                "is_daily": parsed["is_daily"],
            }
            # 附加上下文说明
            if name in groups:
                entry["type"] = "group"
                entry["tables"] = groups[name]
                entry["table_count"] = len(groups[name])
            else:
                entry["type"] = "single"
                entry["tables"] = [name]
                entry["table_count"] = 1
            parsed_schedule[name] = entry

        # 构建冲突检测信息：每个表属于哪些调度项
        table_to_schedules: dict[str, list[str]] = {}
        for name, entry in parsed_schedule.items():
            for t in entry["tables"]:
                table_to_schedules.setdefault(t, []).append(name)

        return jsonify({
            "schedule": parsed_schedule,
            "groups": {k: v for k, v in groups.items()},
            "ungrouped": ungrouped,
            "table_schedules": table_to_schedules,  # 冲突检测用
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _parse_schedule_time(time_str: str) -> dict:
    """解析调度时间字符串，返回频率、策略等结构化信息."""
    time_str = time_str.strip()
    parts = time_str.split()
    frequency = "daily"
    frequency_label = "每天"
    time_part = time_str
    strategy = "每天收盘后自动增量更新"

    if len(parts) == 2:
        freq_word = parts[0].lower()
        time_part = parts[1]
        if freq_word == "weekly":
            frequency = "weekly"
            frequency_label = "每周"
            strategy = "每周定期自动更新（低频数据）"
        elif freq_word == "monthly":
            frequency = "monthly"
            frequency_label = "每月"
            strategy = "每月定期自动更新（低频数据）"
        elif freq_word == "quarterly":
            frequency = "quarterly"
            frequency_label = "每季度"
            strategy = "每季度定期自动更新（财报相关数据）"
        elif freq_word == "yearly":
            frequency = "yearly"
            frequency_label = "每年"
            strategy = "每年定期自动更新（年度静态数据）"

    # 按频率细化策略说明
    if time_str.startswith("yearly"):
        strategy = "每年自动更新一次（年度静态数据，如交易日历）"
    elif time_str.startswith("monthly"):
        strategy = "每月自动更新一次（月度快照数据，如股东户数）"
    elif time_str.startswith("quarterly"):
        strategy = "每季度自动更新一次（季度财报数据）"
    elif time_str.startswith("weekly"):
        strategy = "每周自动更新一次（周度分类数据，如行业/概念板块）"
    elif frequency == "daily":
        strategy = "每天收盘后自动增量更新（核心行情/资金数据）"

    # 提取 HH:MM 用于时间线
    time_match = time_part if ":" in time_part else ""
    is_daily = frequency == "daily"

    return {
        "time": time_part,
        "frequency": frequency,
        "frequency_label": frequency_label,
        "strategy": strategy,
        "is_daily": is_daily,
        "time_match": time_match,
    }


# ---------------------------------------------------------------------------
# API — 系统状态（初始化/回补判定）
# ---------------------------------------------------------------------------


@bp.route("/api/system-status")
def api_system_status():
    """返回系统初始化状态、回补状态、自动更新配置."""
    try:
        cfg = load_config(_config_path)
        db = IngestionDB(cfg.db_path)

        # 各表行数
        row_counts = {}
        for name in INIT_TABLES + ["dragon_tiger", "capital_flow", "northbound_flow",
                                     "holder_count", "fundamentals", "global_markets"]:
            try:
                row_counts[name] = db.count(name)
            except Exception:
                row_counts[name] = 0

        # 初始化判定
        init_status = {}
        for name in INIT_TABLES:
            count = row_counts.get(name, 0)
            init_status[name] = {
                "initialized": count > 0,
                "rows": count,
            }

        all_init = all(v["initialized"] for v in init_status.values())

        # 回补判定（daily_ohlcv > 100万行 或 stock_universe 都有数据即可认为已回补）
        ohlcv_rows = row_counts.get("daily_ohlcv", 0)
        ohlcv_stocks = db.conn.execute(
            "SELECT COUNT(DISTINCT symbol) FROM daily_ohlcv"
        ).fetchone()[0] if ohlcv_rows > 0 else 0
        backfill_status = {
            "done": ohlcv_rows > 1_000_000,
            "daily_ohlcv_rows": ohlcv_rows,
            "stocks_with_data": ohlcv_stocks,
        }

        # 自动更新调度
        schedule_info = {}
        for group_name, tables in AUTO_UPDATE_TABLES.items():
            time_str = cfg.schedule.data.get(group_name, "")
            schedule_info[group_name] = {
                "tables": tables,
                "time": time_str,
                "enabled": bool(time_str),
            }

        # 按需获取的表（不在 AUTO_UPDATE_TABLES 中，但有 DataService policy）
        from src.ingestion.service import POLICIES
        on_demand_tables = {}
        for data_type, policy in POLICIES.items():
            if policy.persist and policy.check_db_first:
                db_table = policy.db_table or data_type
                try:
                    db_count = db.count(db_table)
                except Exception:
                    db_count = 0
                on_demand_tables[data_type] = {
                    "db_table": db_table,
                    "rows": db_count,
                    "source": policy.source,
                    "trading_ttl": policy.trading_ttl,
                    "closed_ttl": policy.closed_ttl,
                }

        db.close()

        return jsonify({
            "init": init_status,
            "all_initialized": all_init,
            "backfill": backfill_status,
            "schedule": schedule_info,
            "on_demand": on_demand_tables,
            "total_tables_in_db": len(db.TABLES),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# API — MCP 工具列表
# ---------------------------------------------------------------------------


@bp.route("/api/mcp-tools")
def api_mcp_tools():
    """返回 MCP Server 中所有工具的列表（名称、描述、参数）."""
    try:
        # 从 mcp_server 模块内省工具列表
        from src.ingestion.mcp_server import mcp

        tools = []
        # FastMCP 通过 _tool_manager 管理工具
        tm = getattr(mcp, "_tool_manager", None)
        if tm is None:
            tm = getattr(mcp, "_mcp_server", None)
            if tm:
                tm = getattr(tm, "_tool_manager", None)

        if tm:
            registered = getattr(tm, "_tools", {}) or getattr(tm, "tools", {})
            for name, tool in registered.items():
                desc = getattr(tool, "description", "") or ""
                params = []
                # 尝试获取参数签名
                fn = getattr(tool, "fn", None) or getattr(tool, "__wrapped__", None)
                if fn:
                    import inspect
                    try:
                        sig = inspect.signature(fn)
                        for pname, param in sig.parameters.items():
                            if pname in ("self", "cls"):
                                continue
                            params.append({
                                "name": pname,
                                "type": str(param.annotation.__name__) if param.annotation is not param.empty else "str",
                                "default": str(param.default) if param.default is not param.empty else None,
                            })
                    except Exception:
                        pass

                tools.append({
                    "name": name,
                    "description": desc,
                    "parameters": params,
                })

        # 按名称排序
        tools.sort(key=lambda t: t["name"])

        return jsonify({
            "tools": tools,
            "total": len(tools),
            "mcp_url": "http://host:8000/sse",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# MCP 工具参考页
# ---------------------------------------------------------------------------


@bp.route("/mcp")
def mcp_page():
    return render_template("mcp.html")


# ---------------------------------------------------------------------------
# 数据与缓存页
# ---------------------------------------------------------------------------


@bp.route("/data")
def data_page():
    return render_template("data.html")


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
