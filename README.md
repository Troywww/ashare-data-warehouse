# stock-ingestion

**A 股数据仓库** — 每日自动采集，Docker 一键部署，为量化分析和 AI 应用提供结构化数据底座。

覆盖 A 股全市场（沪/深/北）**19 张表**，行情、估值、资金、信号、财务、外围六层持久化数据，从 2010 年至今累计约 **1500 万行**。同时封装了研报、新闻、公告、快讯等在线数据接口，供应用层按需即时获取。

---

## 项目定位

做 A 股数据分析，最大的障碍不是分析本身，而是**数据基础设施**：

- 数据源分散在各处（东财、通达信、腾讯、同花顺……），接口协议各异（HTTP、TCP Socket）
- 原始接口返回的数据杂乱，缺少清洗、去重、格式统一
- 历史数据无法回溯，增量更新需要自己维护进度
- 多表之间缺乏关联设计，分析时需要反复 join 不同源的碎片数据

本项目解决了这些问题。它把 6 个数据源的原始接口统一封装成 19 张关系表，每天自动增量更新，开箱即用。你可以直接在这套数据之上构建：

- **量化策略回测系统**
- **股票分析看板（Dashboard）**
- **AI 投资助手（LLM + 数据）**
- **自定义选股/预警规则**

---

## 数据分层与业务价值

数据按业务语义分为 5 层，每一层解决不同的问题：

### 基础层 — 全量品种索引

| 表 | 内容 | 业务价值 |
|----|------|---------|
| `stock_universe` | 沪/深/北全市场 ~6000 只股票代码与名称 | 所有分析的前提，知道"有哪些股票" |
| `trade_calendar` | A 股交易日历 | 判断某日是否开市，避免空数据查询 |
| `stock_classification` | 申万行业 + 地域归属 | 行业分类筛选、板块轮动分析 |
| `concept_blocks` | 通达信概念板块（N:N 映射） | 概念股筛选，一票多概念归因 |

> **分析场景：** 筛选某个行业或概念下的全部股票 → 为后续行情分析圈定标的池。

### 行情层 — 价格与估值

| 表 | 内容 | 业务价值 |
|----|------|---------|
| `daily_ohlcv` | **核心表** — 日 K 线（前复权 QFQ），含涨跌幅、换手率 | 技术分析、回测、价格趋势计算 |
| `daily_valuation` | PE(TTM) / PB / PS / PCF / 总市值 / 流通市值 | 估值分位数、低估值选股、市值风格判断 |
| `xdxr_events` | 除权除息事件记录 | 复权校正、分红跟踪 |

> **分析场景：** 计算某股票当前 PE 处于历史 % 分位；筛选低估值高增长股票；按市值分层分析板块表现。

### 资金面 — 钱往哪流

| 表 | 内容 | 业务价值 |
|----|------|---------|
| `capital_flow` | 主力净流入 + 超大单/大单/中单/小单 5 日累计 | 资金动向判断，主力意图追踪 |
| `northbound_flow` | 北向资金（沪股通 + 深股通）每日净买入 | 外资风向标，全球资金配置视角 |
| `margin_trading` | 融资余额 / 融券余量 | 杠杆资金情绪，市场热度指标 |

> **分析场景：** 识别主力连续净流入的个股；观察北向资金大幅流入/流出时市场的后续走势。

### 信号层 — 事件与情绪

| 表 | 内容 | 业务价值 |
|----|------|---------|
| `dragon_tiger` | 龙虎榜（上榜原因、买卖金额、1/2/5/10 日表现） | 短线游资追踪、龙虎榜效应分析 |
| `board_daily` | 行业 + 概念板块每日涨跌排名、领涨股 | 热点板块识别，轮动节奏把握 |
| `hot_stocks` | 雪球关注热度排名 | 散户情绪指标，人气-价格背离检测 |
| `hot_reasons` | 同花顺题材归因标签（人工运营） | 涨停原因归因，题材关联分析 |
| `block_trades` | 大宗交易（成交价、折溢价率、买卖营业部） | 大资金动向、折溢价套利信号 |
| `lockup_calendar` | 限售解禁日历（含未来 90 天） | 解禁压力预判，减持风险预警 |

> **分析场景：** 龙虎榜上榜后 5 日胜率统计；解禁前 30 天的股价异常波动检测。

### 财务层 — 基本面

| 表 | 内容 | 业务价值 |
|----|------|---------|
| `fundamentals` | 季度财报（EPS / ROE / 营收 / 利润 / 毛利率 / 经营现金流） | 基本面筛选、财报超预期检测 |
| `holder_count` | 股东户数（季度环比） | 筹码集中度变化，主力吸筹/派发判断 |

> **分析场景：** ROE > 15% 且股东户数连续下降（筹码集中）的选股策略。

### 外围层 — 跨市场联动

| 表 | 内容 | 业务价值 |
|----|------|---------|
| `global_markets` | 美股/港股/黄金/原油/外汇日 K 线 | 跨市场联动分析，隔夜风险预判 |

> **分析场景：** 美股中概股下跌 → A 股对应板块开盘压力评估。

### 深度研究层 — 即时拉取（不落库）

以下数据**不存储在 DuckDB 中**（实时性强、按个股维度查询、不适合批量入库），但可以通过各自的原生 API 或 akshare 直接获取，适合个股深度研究和盘面解读：

| 数据 | 来源 | 说明 | 获取方式 |
|------|------|------|---------|
| `research_reports` | 东方财富 | 个股研报列表（标题、机构、评级、EPS 预测） | 东财 `reportapi.eastmoney.com` |
| `stock_news` | 东方财富 | 个股新闻（标题、来源、摘要、原文链接） | `akshare.stock_news_em(code)` |
| `financial_reports` | 新浪财经 | 三张财报（利润表、资产负债表、现金流量表） | `quotes.sina.cn` |
| `cninfo_filings` | 巨潮资讯 | 上市公司公告全文 | `cninfo.com.cn` |
| `cls_news` | 财联社 | 全市场实时电报 | `cls.cn` |
| `global_news` | 东方财富 | 全球财经快讯 | 东财 `np-weblist` |

> 这些接口**尚未封装在项目代码中**，应用层需要直接调用各自的 SDK 或 HTTP 接口。项目通过 `em_auth.py` 提供了东财域名的 NID 认证支持，为调用东财系接口扫除了前置障碍。

---

## 数据源总览

| 数据源 | 协议 | 覆盖表 | 特性 | 替代方案 |
|--------|------|--------|------|---------|
| **opentdx**（通达信） | TCP Socket | 5 张核心表（stock_universe, classification, concept_blocks, daily_ohlcv, capital_flow, global_markets） | 盘中实时，数据最全，含北交所；TCP 有限流，回补并发 ≤ 3 | baostock（缺北交所）、akshare |
| **akshare** | HTTP | 8 张表（dragon_tiger, margin_trading, fundamentals, hot_stocks 等） | 社区活跃，接口丰富；依赖上游数据源稳定性 | 无直接替代 |
| **东方财富 API** | HTTP | 5 张表（northbound_flow, block_trades, lockup_calendar, holder_count, xdxr_events） | 覆盖独有数据（大宗、解禁、股东户数）；部分字段官方已停更 | 同花顺 |
| **腾讯 API** | HTTP | daily_valuation（增量） | 简单可靠，盘中实时 PE/PB | baostock（历史回补） |
| **同花顺** | HTTP | hot_reasons | 人工运营题材标签，数据质量高 | 无替代 |
| **baostock** | TCP | trade_calendar, daily_valuation（历史） | 免费稳定，但仅沪深，无北交所 | opentdx |

---

## 前端 + LLM 应用场景

这套数据天然适合作为 LLM 应用的"知识库"——LLM 不擅长计算数值，但擅长理解关系、生成自然语言解读。以下是一些典型的应用方式：

**智能选股助手**

用户问："帮我找找最近主力在买、估值又不贵的股票"
→ LLM 生成 SQL 查询 `capital_flow` + `daily_valuation` + `daily_ohlcv`
→ 返回结果：`"贵州茅台（600519），近 5 日主力净流入 12.5 亿，PE(TTM) 25.3，处于近 5 年 35% 分位……"`

**每日复盘简报**

LLM 读取当日 `board_daily`（板块排名）、`dragon_tiger`（龙虎榜）、`northbound_flow`（北向资金），自动生成：
> "今日北向资金净买入 85 亿，连续第 3 日净流入；龙虎榜显示游资集中在 AI 算力方向；板块方面，半导体 +3.2% 领涨……"

**异常检测与预警**

基于历史数据计算统计量（如 PE 分位、换手率 z-score、北向资金突变），当某指标超出阈值时推送给用户。

---

## Web 控制面板

提供图形化管理界面，Docker 部署后自动启动：

```
http://localhost:5000
```

| 页面 | 功能 |
|------|------|
| **仪表盘** | 各表行数、数据库大小、更新状态、错误记录 |
| **数据管理** | 手动触发全量/单表更新和回补，跟踪任务进度 |
| **数据搜索** | 按股票代码快速搜索、自由 SQL 查询、结果导出 CSV |
| **调度配置** | 查看和编辑各表/组的调度时间 |

通过 `ingestion serve` 命令启动（Docker 默认随 compose 启动）。

---

## MCP Server（HTTP/SSE）

提供 **18 个只读数据工具**，其他 Agent 可通过 MCP 协议远程查询数据。

Docker 部署后自动启动，端口 `8000`。其他 Agent 配置：

```json
{
  "mcpServers": {
    "ashare": {
      "url": "http://你的服务器IP:8000/sse"
    }
  }
}
```

| 工具 | 说明 |
|------|------|
| `query_kline` | 日K线（前复权） |
| `query_valuation` | PE/PB/估值数据 |
| `query_fundamentals` | 季度财务 |
| `query_capital_flow` | 资金流向 |
| `query_dragon_tiger` | 龙虎榜 |
| `query_northbound_flow` | 北向资金 |
| `query_board_daily` | 板块涨跌排名 |
| `query_margin_trading` | 融资融券 |
| `query_hot_stocks` | 热度排名 |
| `query_block_trades` | 大宗交易 |
| `query_lockup_calendar` | 限售解禁 |
| `query_global_markets` | 外围指数 |
| `query_holder_count` | 股东户数 |
| `search_stocks` | 股票搜索 |
| `query_industry_stocks` | 行业成分股 |
| `query_concept_stocks` | 概念板块 |
| `get_market_overview` | 市场概览 |
| `run_sql` | 自定义只读SQL |

> MCP Server 只提供**稳定读接口**，与在线/实验性功能分离。在线数据获取和自定义分析通过 `.claude/skills/ashare-data-warehouse.md` Skill 实现。

---

## 快速开始

```bash
# Docker 部署（推荐）
cd ashare-data-warehouse
docker compose up -d

# 手动触发增量更新
docker compose exec ingestion daily-update

# 查看各表数据量
docker compose exec ingestion status

# 本地开发
pip install -r requirements.txt
pip install -e .
ingestion daily-update
```

详细命令说明见 [docs/DATA_WAREHOUSE.md](docs/DATA_WAREHOUSE.md)。

---

## 架构概要

```
6 个数据源 ──→ 19 个 fetcher ──→ 3 个 Wave ──→ DuckDB（19 张表）
                      (并发调度)         (分层执行)
```

- **Fetcher 层**：每个数据源对应一个独立 fetcher，错误隔离，单表失败不影响其余
- **Engine 层**：Wave 0（并行）→ Wave 1（串行，有依赖）→ Wave 2（并行）
- **存储层**：DuckDB 嵌入式 OLAP，INSERT OR REPLACE 幂等写入，天然支持重跑

---

## 部署与运维

```bash
docker compose up -d
docker compose logs -f
```

`config.yaml`、`data/`、`logs/` 通过 volume 挂载到宿主机，升级镜像不丢数据。

数据备份：
```bash
cp data/ingestion/stock_research.duckdb backup/$(date +%Y%m%d).duckdb
```

---

## 致谢

本项目受 [simonlin1212/a-stock-data](https://github.com/simonlin1212/a-stock-data) 启发，在其数据采集思路上重构为 DuckDB 持久化 + 并发调度 + Docker 部署的生产级数据管道。

依赖的开源项目：opentdx、baostock、akshare、DuckDB、pandas、pyarrow。

## 许可证

MIT
