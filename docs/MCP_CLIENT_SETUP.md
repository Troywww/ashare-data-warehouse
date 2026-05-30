# MCP Client 接入指南

ashare-data-warehouse 通过 MCP 协议暴露 27+ 个数据工具，
支持所有标准 MCP 客户端接入。

## 服务地址

| 环境 | MCP 端点 |
|------|---------|
| 本地 Docker | `http://localhost:8000/sse` |
| 远程服务器 | `http://<你的IP>:8000/sse` |

---

## 1. Claude Desktop

在 `claude_desktop_config.json` 中添加：

### 1.1 SSE 直连（推荐）

```json
{
  "mcpServers": {
    "ashare": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

> Claude Desktop 的 MCP SSE 支持从 **2026 年 5 月版本**开始可用。
> 如果版本较旧或遇到"url 不是已知字段"错误，请使用下面的 stdio 版。

### 1.2 stdio 包装（SSE → stdio 桥接）

如果 Claude Desktop 不支持 SSE，创建一个桥接脚本 `mcp_bridge.sh`：

```bash
#!/bin/bash
# 在 Claude Desktop 里用这个包装脚本替代 SSE 直连
# 通过 websocat 将 SSE 转为 stdio
exec websocat ws://localhost:8000/sse
```

`claude_desktop_config.json` 配置：

```json
{
  "mcpServers": {
    "ashare": {
      "command": "bash",
      "args": ["/path/to/mcp_bridge.sh"]
    }
  }
}
```

---

## 2. Codex CLI / OpenClaw

### 2.1 Codex CLI

Codex CLI 在项目根目录的 `.claude/settings.json` 中配置：

```json
{
  "mcpServers": {
    "ashare": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

或者在 Codex CLI 中添加自定义 MCP：

```bash
# Codex CLI 命令行添加 MCP 连接
codex mcp add "ashare" --url "http://localhost:8000/sse"
```

### 2.2 OpenClaw

OpenClaw 的 `config.yaml` 或项目 `.claw.yaml` 中添加：

```yaml
mcpServers:
  ashare:
    url: "http://localhost:8000/sse"
```

---

## 3. Hermes

Hermes 的 `hermes_config.yaml` 中添加：

```yaml
mcp_servers:
  ashare:
    transport: sse
    url: "http://localhost:8000/sse"
    # 如果是远程服务器：
    # url: "http://your-server-ip:8000/sse"
```

Hermes 也支持在任务中按需调用：

```yaml
task: "分析000001"
tools:
  - mcp: ashare
    tool: query_kline
    args:
      symbol: "000001"
      days: 60
```

---

## 4. Cursor

Cursor 的 `.cursor/mcp.json` 中添加：

```json
{
  "mcpServers": {
    "ashare": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

然后重启 Cursor，在聊天框中 Agent 会自动发现可用的 MCP 工具，
输入"分析 000001"即可触发数据查询。

---

## 5. 通用 MCP 客户端

大部分通用 MCP 客户端（如 mcp-cli, mcp-proxy 等）都支持 SSE 传输：

```bash
# mcp-cli
mcp-cli connect http://localhost:8000/sse

# mcp-proxy
mcp-proxy --sse-port 8000 --target http://localhost:8000/sse
```

---

## 6. Python 代码直接调用（无客户端时）

如果只是想从 Python 脚本中调用，不依赖客户端：

```python
import asyncio
import json
from mcp.client.sse import sse_client

async def query_ashare(tool: str, args: dict):
    """直接调用 ashare MCP 工具"""
    async with sse_client("http://localhost:8000/sse") as (read, write):
        # 初始化
        await write.send(json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "script"}}
        }))
        await read()

        # 调用工具
        await write.send(json.dumps({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": tool, "arguments": args}
        }))
        resp = await read()
        return json.loads(resp.lstrip("data: "))

# 使用示例
result = asyncio.run(query_ashare("query_kline", {"symbol": "000001", "days": 60}))
print(result)
```

---

## 7. 可用工具速查

连接成功后，agent 会自动通过 `tools/list` 发现所有工具。
以下是常用工具列表供参考：

### 历史查询（直查 DuckDB，毫秒级）

| 工具 | 参数 | 说明 |
|------|------|------|
| `query_kline` | symbol, days | 日K线（前复权） |
| `query_valuation` | symbol, days | PE/PB/市值 |
| `query_capital_flow` | symbol, days | 主力资金流向 |
| `query_dragon_tiger` | date_str | 龙虎榜 |
| `query_board_daily` | date_str, top_n | 板块涨跌排名 |
| `query_fundamentals` | symbol | 财务数据 |
| `query_northbound_flow` | days | 北向资金 |
| `query_margin_trading` | symbol | 融资融券 |
| `query_lockup_calendar` | days_ahead | 解禁日历 |
| `query_block_trades` | symbol, days | 大宗交易 |
| `query_shareholder_changes` | symbol | 增减持 |
| `query_global_markets` | symbol | 外围市场 |
| `query_hot_stocks` | top_n | 雪球热度 |
| `query_industry_stocks` | industry_name | 行业成分股 |
| `query_concept_stocks` | concept_name | 概念成分股 |
| `search_stocks` | keyword | 股票搜索 |
| `run_sql` | sql | 自定义SQL |
| `get_market_overview` | - | 市场概览 |

### 实时数据（走 DataService 缓存）

| 工具 | 参数 | 缓存 |
|------|------|------|
| `get_realtime_quote` | symbol | 3s/1h |
| `get_realtime_quotes` | symbols | 3s/1h |
| `get_intraday_kline` | symbol, period, count | 30s/永久 |
| `get_limit_up_ladder` | - | 30s/永久 |
| `get_latest_news` | count | 5min+写库 |
| `get_stock_news` | symbol | 5min+写库 |
| `get_announcements` | symbol, days | 30min+写库 |
| `get_research_reports` | symbol | 1h+写库 |
| `get_eps_consensus` | symbol | 1h+写库 |

### 计算工具

| 工具 | 参数 | 说明 |
|------|------|------|
| `compute_indicator` | symbol, indicator, fast/slow/signal | MACD/KDJ 即时计算 |
| `find_signal_stocks` | indicator, signal, period | 全市场信号扫描 |

### 系统管理

| 工具 | 参数 | 说明 |
|------|------|------|
| `clear_cache` | data_type | 清缓存 |
| `cache_stats` | - | 缓存统计 |

---

## 8. 连接验证

连接后，可以发给 agent 这段话测试：

```
从 ashare MCP 获取 000001 平安银行最近 10 天的日K线，
同时获取它的估值数据和最新财联社快讯。
```

正常返回说明连接成功。
