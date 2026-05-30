# 数据目录

---

## 一、DuckDB 持久化表（26 张）

### 1.1 基础数据层

| 表名 | 行数参考 | 内容 | 来源 | 更新频率 | 更新方式 |
|------|---------|------|------|---------|---------|
| `stock_universe` | ~5500 | 股票代码+名称+市场 | opentdx | 日频 | 盘后 Pipeline Wave 0 |
| `trade_calendar` | ~2500/年 | A股交易日历 | baostock | 年频 | Pipeline（一次性拉取）|
| `stock_classification` | ~5000 | 行业+地域分类 | opentdx | 日频 | Pipeline Wave 1 |
| `concept_blocks` | ~50000 | 概念板块映射（N:N） | opentdx | 日频 | Pipeline Wave 1 |
| `xdxr_events` | ~50000 | 除权除息事件 | opentdx | 日频 | Pipeline Wave 1 |

### 1.2 行情数据层

| 表名 | 行数参考 | 内容 | 来源 | 更新频率 | 更新方式 |
|------|---------|------|------|---------|---------|
| `daily_ohlcv` | ~5M | 前复权日K线 + 涨跌幅 + 换手率 | opentdx | 日频 15:30 | Pipeline Wave 1 |
| `daily_valuation` | ~5M | PE(TTM)/PB/PS/PCF/市值 | 腾讯/baostock | 日频 15:30 | Pipeline Wave 1 |
| `global_markets` | ~500K | 美股/港股/黄金/原油日线 | opentdx | 日频 09:00 | Pipeline Wave 2 |

### 1.3 资金流向层

| 表名 | 行数参考 | 内容 | 来源 | 更新频率 | 更新方式 |
|------|---------|------|------|---------|---------|
| `capital_flow` | ~5M | 个股资金流向（主力/大单/中单/小单） | opentdx | 日频 15:30 | Pipeline Wave 1 |
| `northbound_flow` | ~5K | 北向资金每日净买入（沪/深） | eastmoney | 日频 18:00 | Pipeline Wave 0 |
| `margin_trading` | ~800K | 融资融券余额明细 | akshare | 日频 17:00 | Pipeline Wave 2 |

### 1.4 信号事件层

| 表名 | 行数参考 | 内容 | 来源 | 更新频率 | 更新方式 |
|------|---------|------|------|---------|---------|
| `dragon_tiger` | ~50K | 龙虎榜（含后续表现1~10日） | akshare | 日频 17:00 | Pipeline Wave 2 |
| `board_daily` | ~80K | 板块涨跌排名+领涨股 | opentdx | 日频 15:30 | Pipeline Wave 2 |
| `hot_stocks` | ~5K | 雪球关注热度排名 | akshare | 日频 | Pipeline Wave 2 |
| `hot_reasons` | ~50K | 同花顺热点题材归因 | akshare | 日频 15:30 | Pipeline Wave 2 |
| `block_trades` | ~200K | 大宗交易 | eastmoney | 日频 | Pipeline Wave 2 |
| `lockup_calendar` | ~50K | 限售解禁日历（含未来90天） | eastmoney | 日频 | Pipeline Wave 2 |
| `indicator_values` | ~16K/日 | 30项技术指标（D/W/M三种频率） | 自算 (MyTT 2D batch) | 日频 17:00 | Pipeline Wave 2 |
| `shareholder_changes` | ~可变 | 大股东增减持 | akshare | 日频 | Pipeline Wave 2 |

### 1.5 基本面层

| 表名 | 行数参考 | 内容 | 来源 | 更新频率 | 更新方式 |
|------|---------|------|------|---------|---------|
| `fundamentals` | ~150K | 季报财务数据（EPS/ROE/营收/利润/毛利率/现金流） | akshare | 季频 | Pipeline Wave 2 |
| `holder_count` | ~150K | 股东户数变化（环比比） | eastmoney | 月频 | Pipeline Wave 2 |
| `eps_consensus` | ~可变 | 机构一致预期EPS（当年+未来2年） | akshare(同花顺) | 按需 | DataService 按需 📥 |
| `research_reports` | ~可变 | 东财研报（评级+目标价+EPS预测） | akshare(东财) | 按需 | DataService 按需 📥 |

### 1.6 舆情层

| 表名 | 行数参考 | 内容 | 来源 | 更新频率 | 更新方式 |
|------|---------|------|------|---------|---------|
| `cls_telegram` | ~可变 | 财联社快讯 | CLS 直连 | 按需 + 持久化 | DataService 按需 📥 |
| `stock_news` | ~可变 | 个股新闻 | eastmoney | 按需 + 持久化 | DataService 按需 📥 |
| `announcements` | ~可变 | 巨潮公告 | akshare(cninfo) | 按需 + 持久化 | DataService 按需 📥 |

### 1.7 系统支持层

指标信号的衍生于 `indicator_values` 表在 DataService 层实时完成（`_scan_indicator_values`），无需独立持久化。

---

## 二、数据源全表

| 数据源 | 协议 | 获取内容 | 稳定性 |
|--------|------|---------|--------|
| **opentdx** | TCP | K线/资金流/板块/概念/复权因子 | ⭐⭐⭐⭐⭐ (最稳定) |
| **akshare** | HTTP | 龙虎榜/财务/热点/研报/公告/EPS预期 | ⭐⭐⭐⭐ |
| **eastmoney 直连** | HTTP | 北向/大宗/解禁/股东/增减持 | ⭐⭐⭐⭐ |
| **腾讯 API** | HTTP | PE/PB/市值 | ⭐⭐⭐⭐⭐ |
| **baostock** | TCP | 交易日历 | ⭐⭐⭐⭐⭐ |
| **CLS 直连** | HTTP | 财联社快讯 | ⭐⭐⭐ |

---

## 三、缓存策略（DataService TTLCache）

### 盘中（09:30~15:00）

| 数据类型 | TTL | 写入DuckDB | 说明 |
|---------|-----|-----------|------|
| 实时行情 5档 | **3 秒** | ❌ | 盘中高频刷新 |
| 分时图 | **30 秒** | ❌ | |
| 1分钟K线 | **30 秒** | ❌ | |
| 5分钟K线 | **2 分钟** | ❌ | |
| 涨停梯队 | **30 秒** | ❌ | |
| 资金流（分钟级） | **60 秒** | ❌ | |
| 北向（分钟级） | **60 秒** | ❌ | |
| 财联社快讯 | **5 分钟** | ✅ 追加 | 最新快讯自动存库 |
| 个股新闻 | **5 分钟** | ✅ 追加 | |
| 巨潮公告 | **30 分钟** | ✅ 追加 | |
| 研报 | **1 小时** | ✅ UPSERT | |
| 一致预期EPS | **1 小时** | ✅ UPSERT | |
| 增减持 | **1 小时** | ✅ UPSERT | |
| MACD/KDJ信号扫描 | **1 小时** | ✅ UPSERT | |

### 盘后

| 数据类型 | TTL |
|---------|-----|
| 行情/分时/K线 | **永不失效**（盘后不会变） |
| 快讯/新闻 | **30 分钟**（闭市后也可能出消息）|
| 公告/研报 | **2~4 小时** |
| 技术指标 | **永不失效** |

**缓存清除时机**：Pipeline 收盘后跑完 Wave 2 → 自动清空所有缓存 → 次日开盘重新拉取。

---

## 四、MCP 工具清单（27 个）

### 4.1 历史查询（直查 DuckDB，毫秒级）

| 工具 | 参数 | 对应表 |
|------|------|--------|
| `query_kline` | symbol, days | daily_ohlcv |
| `query_valuation` | symbol, days | daily_valuation |
| `query_fundamentals` | symbol | fundamentals |
| `query_capital_flow` | symbol, days | capital_flow |
| `query_dragon_tiger` | date_str | dragon_tiger |
| `query_northbound_flow` | days | northbound_flow |
| `query_board_daily` | date_str, top_n | board_daily |
| `query_margin_trading` | symbol | margin_trading |
| `query_hot_stocks` | top_n | hot_stocks |
| `query_block_trades` | symbol, days | block_trades |
| `query_lockup_calendar` | days_ahead | lockup_calendar |
| `query_global_markets` | symbol | global_markets |
| `query_holder_count` | symbol | holder_count |
| `query_shareholder_changes` | symbol | shareholder_changes |
| `search_stocks` | keyword | stock_universe |
| `query_industry_stocks` | industry_name | 关联查询 |
| `query_concept_stocks` | concept_name | concept_blocks |
| `get_market_overview` | — | 聚合统计 |
| `run_sql` | sql | 自定义SQL |

### 4.2 实时查询（走 DataService + Cache）

| 工具 | 参数 | 缓存 TTL |
|------|------|---------|
| `get_realtime_quote` | symbol | 3s / 1h |
| `get_realtime_quotes` | symbols(逗号分隔) | 3s / 1h |
| `get_intraday_kline` | symbol, period, count | 30s / 永久 |
| `get_limit_up_ladder` | — | 30s / 永久 |

### 4.3 舆情查询（按需拉取 + 持久化）

| 工具 | 参数 | 缓存 | DB |
|------|------|------|----|
| `get_latest_news` | count (默认50) | 5min | cls_telegram |
| `get_stock_news` | symbol | 5min | stock_news |
| `get_announcements` | symbol, days | 30min | announcements |
| `get_research_reports` | symbol | 1h | research_reports |
| `get_eps_consensus` | symbol | 1h | eps_consensus |

### 4.4 计算工具

| 工具 | 参数 | 说明 |
|------|------|------|
| `compute_indicator` | symbol, indicator, fast/slow/signal | 单只股票技术指标 |
| `find_signal_stocks` | indicator, signal, period, lookback | 全市场信号扫描 |

### 4.5 系统管理

| 工具 | 说明 |
|------|------|
| `clear_cache` | 清缓存（开盘前调用） |
| `cache_stats` | 查看缓存状态 |

---

## 五、Pipeline 执行顺序

```
09:00  global_markets（美股/期货收盘更新）
       └→ Wave 0 并行

16:00  Wave 0 → trade_calendar, stock_universe, northbound_flow, ...（并行）
         Wave 1 → xdxr_events → daily_ohlcv（串行）
         Wave 2 → daily_valuation, capital_flow, stock_classification,
                   concept_blocks, indicator_values（并行）
17:00  signals group → dragon_tiger, hot_stocks, hot_reasons,
         margin_trading, block_trades, lockup_calendar, indicator_values
```

---

## 六、数据就绪时间线

```
15:00  收盘
15:30  K线/估值/资金流/板块 → 可用
16:00  龙虎榜（交易所发布）→ 可用
17:00  融资融券/大宗交易 → 可用
18:00  北向资金（港交所结算）→ 可用
次日09:00  开盘前 Pipeline 清缓存
09:30  开盘 → 实时行情走 DataService 缓存
```

---

## 七、缺失但仍需补的能力

| 功能 | 原因 | 优先级 |
|------|------|--------|
| 大股东增减持 | `shareholder_changes` fetcher 已实现（200 只/次限制）| ✅ 已完成 |
| PEG 自动计算 | 需要 EPS增速 + PE 联动 | 🟢 低（agent可自算）|
| 行业毛利率/ROE中位数计算 | 需稳定取到完整财报数据 | 🟢 低 |
