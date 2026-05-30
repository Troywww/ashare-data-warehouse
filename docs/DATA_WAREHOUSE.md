# 📦 数据仓库完整说明文档

## 数据总览

| 项目 | 说明 |
|------|------|
| 存储引擎 | DuckDB |
| 表数量 | 27 张 |
| 核心数据源 | OpenTDX(opentdx) / baostock / 腾讯API / 东财datacenter / akshare / 同花顺 |
| 更新方式 | 每日增量 daily-update，一次性 backfill |

---

## 表分类总览

| 层级 | 表名 | 用途 | 更新 |
|------|------|------|------|
| **主轴** | `trade_calendar` | 交易日历，所有任务的前置检查 | 每年一次 |
| | `stock_universe` | 全品种索引（A股/ETF/指数/债券），所有表的数据范围以此为准 | 每交易日 16:00 |
| **分类** | `stock_classification` | 通达信行业（HY）+ 地域（DQ）归属 | 每交易日 16:00 |
| | `concept_blocks` | 通达信概念板块（GN）归属，N:N 关联 | 每交易日 16:00 |
| **行情** | `daily_ohlcv` | 日K线（前复权），全品种，主行情表 | 每交易日 16:00 |
| | `daily_valuation` | 估值数据（PE/PB/市值/流通市值） | 每交易日 16:00 |
| **事件** | `xdxr_events` | 除权除息事件，触发 OHLCV 回补 | 每交易日 16:00 |
| **信号** | `dragon_tiger` | 龙虎榜（19字段全量） | 每交易日 17:00 |
| | `capital_flow` | 个股资金流向（主力当值+5日分单累计） | 每交易日 16:00 |
| | `board_daily` | 行业+概念板块涨跌排名 | 每交易日 17:00 |
| | `hot_stocks` | 雪球关注热度排名 | 每交易日 17:00 |
| | `hot_reasons` | 同花顺题材归因（人工运营标签） | 每交易日 17:00 |
| | `northbound_flow` | 北向资金（沪股通/深股通净买入） | 每交易日 16:00 |
| **资金** | `margin_trading` | 融资融券（融资余额/融券余量） | 每交易日 17:00 |
| | `block_trades` | 大宗交易（成交价/折溢价/营业部） | 每交易日 17:00 |
| | `holder_count` | 股东户数（筹码集中度） | 每月月初 |
| | `lockup_calendar` | 限售解禁（30天历史+90天未来） | 每交易日 17:00 |
| **技术指标** | `indicator_values` | 30 项技术指标（MACD/KDJ/RSI/BOLL 等）D/W/M 三频 | 每日 17:00 |
| **财务** | `fundamentals` | 季度财务（EPS/ROE/营收/净利等） | 每季度末 |
| **外围** | `global_markets` | 美股/港股/黄金/原油/外汇K线 | 每交易日 **09:00**（美股前日收盘+港股当日开盘前更新） |

> 完整表清单（27 张）及详细说明见 [DATA_CATALOG.md](DATA_CATALOG.md)。

| **按需获取（DataService 自动缓存+落库）** | 接口 | 内容 | 更新 |
|------|------|------|------|
| `research_reports` | 东财 `reportapi.eastmoney.com` | 研报列表（标题/机构/评级/三年EPS预测） | 应用层按需，自动缓存到DB |
| `stock_news` | akshare `stock_news_em(code)` | 个股新闻（标题/来源/摘要/链接） | 应用层按需，自动缓存到DB |
| `eps_consensus` | akshare(同花顺) | 机构一致预期EPS | 应用层按需，自动缓存到DB |
| `cls_telegram` | 财联社 `cls.cn` | 全市场实时电报 | 应用层按需，自动缓存到DB |
| `shareholder_changes` | akshare | 大股东增减持 | 应用层按需，自动缓存到DB |
| `announcements` | 巨潮 `cninfo.com.cn` | 上市公司公告全文 | 应用层按需，自动缓存到DB |
| `dragon_tiger_seats` | 东财 datacenter | 龙虎榜营业部席位明细 | 应用层按需，自动缓存到DB |
| `financial_reports` | 新浪 `quotes.sina.cn` | 三张财报（利润表/资产负债表/现金流量表） | 应用层按需，仅自选股 |
| `cninfo_filings` | 巨潮 `cninfo.com.cn` | 上市公司公告全文 | 应用层按需，仅自选股 |

---

### 1. `trade_calendar` — 交易日历

> **主源：baostock `query_trade_dates()`**（TCP，每年 12 月底拉取下一年全年交易日历）
> **备源：** 无（沪深交易所每年 12 月发布次年交易日安排）
> **更新：** 每年 12 月最后一个交易日全量覆盖。每日任务启动前检查本表，非交易日跳过所有数据刷新。

| 字段 | 类型 | 说明 |
|------|------|------|
| `date` | DATE PK | 日期 |
| `is_trading` | BOOLEAN | `1`=交易日, `0`=休市 |

---

### 2. `stock_universe` — 主表（**所有品种索引**）

> **主源：opentdx `stock_list(SH)` + `stock_list(SZ)` + `stock_list(BJ)`**（TCP，盘中实时）
> **更新：** 每交易日 16:00，全量 INSERT OR REPLACE

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | VARCHAR PK | 品种代码 |
| `name` | VARCHAR | 品种简称 |
| `market` | VARCHAR | `sh` / `sz` / `bj` |

**覆盖范围：**

| 市场 | 数量 | 代码前缀 |
|------|------|---------|
| 沪市 A 股 | ~2,515 | 60xxxx + 68xxxx |
| 深市 A 股 | ~3,158 | 00xxxx + 30xxxx |
| 北交所 | ~317 | 92xxxx（opentdx `MARKET.BJ`） |
| **总计** | **~5,990** | |

**过滤规则：** 代码前缀白名单 `60/68`（沪）、`00/30`（深）、`92`（北交所），排除债券/转债前缀 `81-89`。

**退市：** 品种不在 stock_list 中时下次覆盖自动移除，历史数据不受影响。

**更新时序：**

```
每日 ── OpenTDX stock_list（sh/sz/bj）  ~0.1s
```

**其他说明：**

- 本表是 INSERT OR REPLACE 全量覆盖，非增量追加

### 3. `daily_ohlcv` — 日K线（主行情表）

> **主源：opentdx `stock_kline(adjust=QFQ)`**（TCP，前复权，8 线程并发）
> **备源：baostock `query_history_k_data_plus(adjustflag=2)`**（TCP，仅 sh/sz）
> **增量：** 每交易日 16:00，拉取近 5 天
> **回补：** count=800，过滤 2010-01-01 前数据

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | VARCHAR PK | 品种代码 |
| `date` | DATE PK | 交易日 |
| `open` | DOUBLE | 开盘价（前复权） |
| `high` | DOUBLE | 最高价（前复权） |
| `low` | DOUBLE | 最低价（前复权） |
| `close` | DOUBLE | 收盘价（前复权） |
| `volume` | BIGINT | 成交量（股） |
| `amount` | DOUBLE | 成交额（元） |
| `pct_chg` | DOUBLE | 涨跌幅（%） |
| `turnover_rate` | DOUBLE | 换手率（%） |

**覆盖：** A 股 + ETF + 指数等（跟随 `stock_universe`），2010-01-01 起，前复权。ETF/指数无需复权（QFQ 对其无影响）。

---

### 4. `stock_classification` — 行业/地域索引

> **主源：opentdx `stock_board_list(HY/DQ)` + `stock_board_members`**（TCP，行业 HY 127个 + 地域 DQ 32个）
> **更新：** 每交易日 16:00，全量覆盖

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | VARCHAR PK | 品种代码 |
| `industry` | VARCHAR | 通达信行业一级（HY） |
| `region` | VARCHAR | 通达信地域（DQ） |

---

### 5. `concept_blocks` — 概念板块索引

> **主源：opentdx `stock_board_list(GN)` + `stock_board_members`**（TCP，269个概念板块）
> **更新：** 每交易日 16:00，全量覆盖

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | VARCHAR PK | 品种代码 |
| `concept_name` | VARCHAR PK | 概念板块名称 |
| `board_code` | VARCHAR | 板块代码（如 880547） |

---

### 6. `xdxr_events` — 除权除息事件

> **主源：东财 datacenter `RPT_SHAREBONUS_DET`**（HTTP）
> **注意：** 当前 API 仅返回 `SECURITY_CODE, EX_DIVIDEND_DATE, BONUS_RATIO, NOTICE_DATE` 四个字段。
> `CASH_DIVIDEND` / `TRANSFER_RATIO` / `CATEGORY` 已不可用，对应字段置 NULL。
> **更新：** 每交易日 16:00，仅当天数据

| 字段 | 类型 | 说明 | 来源 | 更新频率 |
|------|------|------|------|---------|
| `stock_code` | VARCHAR PK | 6位股票代码 | 东财 datacenter `RPT_SHAREBONUS_DET` | 每日 |
| `ex_date` | DATE PK | 除权除息日 | 同上 | 每日 |
| `cash_dividend` | DOUBLE | 每股派息（元） | 同上 | 每日 |
| `bonus_ratio` | DOUBLE | 送股（每10股） | 同上 | 每日 |
| `transfer_ratio` | DOUBLE | 转增（每10股） | 同上 | 每日 |
| `category` | VARCHAR | 事件类型：除权除息 / 股本变化 | 同上 | 每日 |

**接口方案：**

| 操作 | 接口 | 协议 | 方式 | 耗时 |
|------|------|------|------|------|
| 每日检测 | **东财 datacenter `RPT_SHAREBONUS_DET`** | HTTP | 过滤 `EX_DIVIDEND_DATE = today` | ~1s |
| 当日写入 | 检测到的结果直接写入 `xdxr_events` | — | 仅当天数据，不补历史 | — |

**工作流程：**

```
每日更新:
  ① 调用东财 datacenter → 查当天 EX_DIVIDEND_DATE 的股票
  ② 有 → 记录到 xdxr_events + 通知 daily_ohlcv 回补
  ③ 无 → 跳过

daily_ohlcv 回补:
  对 xdxr_events 中 ex_date = today 的每只股票
  → baostock 拉全量历史（adjustflag=2）
  → INSERT OR REPLACE 覆盖该股票全部旧数据
  → 前复权价格更新完成

分析工具使用:
  SELECT * FROM xdxr_events WHERE stock_code = '600519' ORDER BY ex_date
  → 可查看某只股票历史上每次除权除息的详细记录
```

**不补历史的原因：**

历史除权信息已体现在 baostock 返回的前复权价格中，不需要再单独记录。`xdxr_events` 只存每天增量的事件，供回补和查询使用。

---

### 7. `daily_valuation` — 估值数据

> **增量：腾讯API `qt.gtimg.cn`**（HTTP，每日 16:00，pe/pb/市值/流通市值）
> **历史：baostock `query_history_k_data_plus`**（TCP，首次部署时回补 2015~至今）
> **注意：** baostock 的 `adjustflag` 参数需传字符串 `"2"`（传 int 2 会触发内部 bug）
> **更新：** 增量每日 16:00；历史全量首次部署跑一次

| 字段 | 类型 | 说明 | 来源 |
|------|------|------|------|
| `symbol` | VARCHAR PK | 品种代码 | 腾讯API / baostock |
| `date` | DATE PK | 交易日 | 腾讯API / baostock |
| `pe_ttm` | DOUBLE | 市盈率 TTM | 腾讯API + baostock |
| `pb` | DOUBLE | 市净率 MRQ | 腾讯API + baostock |
| `ps_ttm` | DOUBLE | 市销率 TTM | baostock（历史），增量 NULL |
| `pcf_ncf_ttm` | DOUBLE | 每股现金流 TTM | baostock（历史），增量 NULL |
| `total_mv` | DOUBLE | 总市值（亿） | 腾讯API（增量），历史 NULL |
| `circ_mv` | DOUBLE | 流通市值（亿） | 腾讯API（增量），历史 NULL |

---

### 8. `dragon_tiger` — 龙虎榜

> **主源：akshare `stock_lhb_detail_em()`**（HTTP，盘后数据，交易所发布后东财更新，含北交所）
> **备源：** 无
> **更新：** 每交易日 17:00，滑动窗口拉最近 7 天，~5s

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | VARCHAR PK | 品种代码 |
| `date` | DATE PK | 上榜日 |
| `reason` | VARCHAR PK | 上榜原因 |
| `close` | DOUBLE | 收盘价 |
| `change_pct` | DOUBLE | 涨跌幅（%） |
| `net_buy` | DOUBLE | 净买额 |
| `buy_amount` | DOUBLE | 买入额 |
| `sell_amount` | DOUBLE | 卖出额 |
| `total_amount` | DOUBLE | 成交额 |
| `market_total_amount` | DOUBLE | 市场总成交额 |
| `net_buy_ratio` | DOUBLE | 净买额/总成交（%） |
| `amount_ratio` | DOUBLE | 成交额/总成交（%） |
| `turnover_rate` | DOUBLE | 换手率（%） |
| `float_mv` | DOUBLE | 流通市值 |
| `perf_1d` | DOUBLE | 上榜后 1 日涨跌幅 |
| `perf_2d` | DOUBLE | 上榜后 2 日涨跌幅 |
| `perf_5d` | DOUBLE | 上榜后 5 日涨跌幅 |
| `perf_10d` | DOUBLE | 上榜后 10 日涨跌幅 |
| `comment` | VARCHAR | 解读（机构买卖信息） |

---

### 9. `capital_flow` — 资金流向

> **主源：OpenTDX `stock_capital_flow`**（TCP，通达信，复用连接，~2min）
> **接口说明：** 返回 `今日主力净流入` + `5日超大单/大单/中单/小单净额`（5日累计值）
> **更新：** 每交易日 16:00，逐只查询写入（复用 TCP 连接）

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | VARCHAR PK | 品种代码 |
| `date` | DATE PK | 交易日 |
| `net_main` | DOUBLE | 今日主力净流入（当日值） |
| `net_super_5d` | DOUBLE | 5日超大单净额（5日累计） |
| `net_large_5d` | DOUBLE | 5日大单净额（5日累计） |
| `net_medium_5d` | DOUBLE | 5日中单净额（5日累计） |
| `net_small_5d` | DOUBLE | 5日小单净额（5日累计） |

**来源说明：**

| 字段 | 类型 | 说明 | 来源 |
|------|------|------|------|
| `symbol` | VARCHAR PK | 6位股票代码 | OpenTDX |
| `date` | DATE PK | 交易日 | OpenTDX |
| `net_main` | DOUBLE | 今日主力净流入 | OpenTDX `今日主力净流入` |
| `net_super_5d` | DOUBLE | 5日超大单净额（累计） | OpenTDX `5日超大单净额` |
| `net_large_5d` | DOUBLE | 5日大单净额（累计） | OpenTDX `5日大单净额` |
| `net_medium_5d` | DOUBLE | 5日中单净额（累计） | OpenTDX `5日中单净额` |
| `net_small_5d` | DOUBLE | 5日小单净额（累计） | OpenTDX `5日小单净额` |

**接口方案：**

| 操作 | 接口 | 协议 | 耗时 |
|------|------|------|------|
| 每日增量 | **OpenTDX `stock_capital_flow`** | TCP | ~2min（5991只，复用连接） |

**更新时序：**

```
每日 16:00 后:
  复用 TdxClient 逐只查询（断连自动重建）
  写入 daily_ohlcv → INSERT OR REPLACE
```

**注意：** `turnover_rate`、`amount` 不在此表存储——需要时 JOIN `daily_ohlcv` 获取。

---

### 10. `board_daily` — 板块涨跌排名

> **主源：OpenTDX `stock_board_members`**（TCP，通达信盘中实时，遍历板块成分股计算汇总）
> **行业板块：** 全部 127 个，~3 秒
> **概念板块：** 前 50 个，~1 秒
> **汇总方式：** 每板块取成分股行情 → 算涨跌幅均值、上涨/下跌家数、领涨股
> **更新：** 每交易日 16:00

| 字段 | 类型 | 说明 |
|------|------|------|
| `date` | DATE PK | 交易日 |
| `board_name` | VARCHAR PK | 板块名称 |
| `board_type` | VARCHAR | `industry` / `concept` |
| `change_pct` | DOUBLE | 涨跌幅（%） |
| `rank` | INTEGER | 排名 |
| `total_mv` | DOUBLE | 总市值 |
| `turnover_rate` | DOUBLE | 换手率（%） |
| `up_count` | INTEGER | 上涨家数 |
| `down_count` | INTEGER | 下跌家数 |
| `leader_name` | VARCHAR | 领涨股名称 |
| `leader_pct` | DOUBLE | 领涨股涨跌幅 |

---

### 11. `hot_stocks` — 雪球关注热度

> **主源：雪球 `stock_hot_follow_xq()`**（HTTP，盘中实时，全市场关注热度含北交所）
> **备源：** 无
> **更新：** 每交易日 17:00，~10s

| 字段 | 类型 | 说明 |
|------|------|------|
| `date` | DATE PK | 日期 |
| `rank` | INTEGER PK | 热度排名 |
| `symbol` | VARCHAR | 6位股票代码 |
| `stock_name` | VARCHAR | 股票简称 |
| `follow_count` | DOUBLE | 雪球关注热度值 |
| `price` | DOUBLE | 最新价 |

**接口方案：**

| 操作 | 接口 | 耗时 |
|------|------|------|
| 每日增量 | **akshare `stock_hot_follow_xq()`** | ~10s |

**北交所：** ⚠️ 数据源全量返回，含北交所股票。

---

### 12. `hot_reasons` — 同花顺题材归因

> **主源：同花顺 `zx.10jqka.com.cn`**（HTTP，盘中实时，编辑部人工运营题材标签，含北交所）
> **备源：** 无
> **更新：** 每交易日 17:00，~1s

| 字段 | 类型 | 说明 |
|------|------|------|
| `date` | DATE PK | 日期 |
| `symbol` | VARCHAR PK | 6位股票代码 |
| `stock_name` | VARCHAR | 股票简称 |
| `reason_tags` | VARCHAR | 题材标签（如"芯片+AI+半导体"） |
| `close` | DOUBLE | 收盘价 |
| `change_amt` | DOUBLE | 涨跌额 |
| `change_pct` | DOUBLE | 涨跌幅（%） |
| `turnover_rate` | DOUBLE | 换手率（%） |
| `amount` | DOUBLE | 成交额（万） |
| `volume` | DOUBLE | 成交量 |

---

### 13. `northbound_flow` — 北向资金

> **主源：东财 push2 `kamt.kline/get`**（HTTP，盘中实时分钟级，盘后返回全日汇总）
> **备源：同花顺北向**（HTTP，262 个分钟点实时）
> **更新：** 每交易日 16:00，~1s。北向资金不涉及北交所。

| 字段 | 类型 | 说明 |
|------|------|------|
| `trade_date` | DATE PK | 交易日 |
| `market` | VARCHAR PK | `sh`（沪股通）/ `sz`（深股通） |
| `net_buy` | DOUBLE | 净买入额 |

| `trade_date` | DATE PK | 交易日 |
| `market` | VARCHAR PK | `sh`（沪股通）/ `sz`（深股通） |
| `net_buy` | DOUBLE | 净买入额 |

---

### 14. `margin_trading` — 融资融券

> **主源：akshare SSE/SZSE margin 双接口**（HTTP，盘后~16:00交易所发布后）。全量覆盖，无备用源。北交所股票非两融标的。全市场两融标的，每日全量覆盖。

| 字段 | 类型 | 说明 | 来源 |
|------|------|------|------|
| `symbol` | VARCHAR PK | 6位代码 | SSE/SZSE |
| `date` | DATE PK | 交易日 | SSE/SZSE |
| `rzye` | DOUBLE | 融资余额 | SSE + SZSE |
| `rzye_buy` | DOUBLE | 融资买入额 | SSE |
| `rzye_repay` | DOUBLE | 融资偿还额 | SSE |
| `rqyl` | DOUBLE | 融券余量 | SSE + SZSE |
| `rqyl_sell` | DOUBLE | 融券卖出量 | SSE |
| `rqyl_repay` | DOUBLE | 融券偿还量 | SSE |
| `rqyl_amt` | DOUBLE | 融券余额（元） | SZSE |
| `rzrqye` | DOUBLE | 融资融券余额 | SZSE |

**接口方案：**

| 操作 | 接口 | 耗时 |
|------|------|------|
| 每日增量 | **akshare `stock_margin_detail_sse(date=今天)` + `stock_margin_detail_szse()`** | ~5s |

**更新时序：** 每日 17:00 后，两交易所全量拉取。

**北交所：** ❌ 不支持——北交所股票不是融资融券标的。

---

### 15. `fundamentals` — 季度财务数据

> **主源：akshare `stock_yjbb_em()`**（HTTP，季度数据，每季度末后上市公司出财报后可用）。全市场含北交所，无备用源。

| 字段 | 类型 | 说明 | 来源 |
|------|------|------|------|
| `symbol` | VARCHAR PK | 6位股票代码 | `股票代码` |
| `end_date` | DATE PK | 季度末日期 | 由传入季度参数计算 |
| `publ_date` | DATE | 最新公告发布日期 | `最新公告日期` |
| `eps` | DOUBLE | 基本每股收益 | `每股收益` |
| `roe` | DOUBLE | 净资产收益率 | `净资产收益率` |
| `revenue` | DOUBLE | 营业总收入 | `营业总收入-营业总收入` |
| `profit` | DOUBLE | 净利润 | `净利润-净利润` |
| `revenue_yoy` | DOUBLE | 营收同比增长（%） | `营业总收入-同比增长` |
| `profit_yoy` | DOUBLE | 净利润同比增长（%） | `净利润-同比增长` |
| `bvps` | DOUBLE | 每股净资产 | `每股净资产` |
| `operating_cashflow` | DOUBLE | 每股经营现金流量 | `每股经营现金流量` |
| `gross_margin` | DOUBLE | 销售毛利率（%） | `销售毛利率` |
| `industry` | VARCHAR | 申万二级行业（同步到stock_universe） | `所处行业` |

**接口方案：**

| 操作 | 接口 | 协议 | 耗时 | 覆盖 |
|------|------|------|------|------|
| 季度更新 | **`akshare.stock_yjbb_em(date)`** | HTTP | ~20s | 全市场 5,700+只（含北交所） |

**更新时序：**

```
每季度末后（月/日/年判定）:
  判断当前月份 → 计算上个季度末日期
  akshare yjbb 全市场一次拉取     ~20s
  INSERT OR REPLACE 写入 fundamentals
  _update_stock_industry() 回填 stock_universe.industry
```

**不再使用：**
- `net_margin` — yjbb 不返回，去掉
- `mootdx.finance()` 逐只补充 — 效率低，仅覆盖200只，去掉

---

### 16. `lockup_calendar` — 限售解禁

> **主源：东财 datacenter `RPT_LIFT_STAGE`**（HTTP）
> **更新：** 每交易日 17:00，30 天历史 + 90 天未来

**列名说明：** 当前 API 列名为 `SECURITY_CODE, FREE_DATE, FREE_RATIO`。
`UNLOCK_DATE` / `UNLOCK_VOL` / `STATUS` 已不可用，对应字段置 NULL。

| 字段 | 类型 | 说明 |
|------|------|------|
| `stock_code` | VARCHAR PK | 品种代码 |
| `unlock_date` | DATE PK | 解禁日 |
| `unlock_vol` | BIGINT | 解禁数量（股） |
| `unlock_ratio` | DOUBLE | 解禁市值（万元） |
| `status` | VARCHAR | `待解禁` / `已解禁` |

---

### 17. `block_trades` — 大宗交易

> **主源：东财 datacenter `RPT_DATA_BLOCKTRADE`**（HTTP，报表名已修正 `RPT_DATA_OCCURTRADE→RPT_DATA_BLOCKTRADE`）
> **更新：** 每交易日 17:00，滑动窗口拉最近 30 天

**列名说明：** 当前 API 列名为 `SECURITY_CODE, TRADE_DATE, DEAL_PRICE, DEAL_VOLUME, DEAL_AMT, PREMIUM_RATIO, BUYER_NAME, SELLER_NAME`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `stock_code` | VARCHAR PK | 品种代码 |
| `trade_date` | DATE PK | 交易日 |
| `price` | DOUBLE PK | 成交价 |
| `volume` | BIGINT | 成交量（股） |
| `amount` | DOUBLE | 成交额（元） |
| `premium_ratio` | DOUBLE | 折溢价率（%）（负=折价） |
| `buyer_broker` | VARCHAR | 买方营业部 |
| `seller_broker` | VARCHAR | 卖方营业部 |

---

### 18. `holder_count` — 股东户数

> **主源：东财 datacenter `RPT_HOLDERNUMLATEST`**（HTTP，分页循环）
> **更新：** 每月月初全量拉取

**列名说明：** 当前 API 列名为 `SECURITY_CODE, END_DATE, HOLDER_NUM, AVG_MARKET_CAP`。
`CHANGE_QOQ` / `AVG_SHARES` 已不可用，对应字段置 NULL。

| 字段 | 类型 | 说明 |
|------|------|------|
| `stock_code` | VARCHAR PK | 品种代码 |
| `end_date` | DATE PK | 数据截止日 |
| `holder_count` | BIGINT | 股东户数 |
| `change_qoq` | DOUBLE | 股东户数环比变化（%） |
| `avg_shares` | DOUBLE | 户均持股数 |

---

### 不存库（应用层获取）

以下接口**未封装在项目代码中**，应用层按需直接调用。项目已通过 `em_auth.py` 为东财域名提供 NID 认证支持。

---

#### 1. `research_reports` — 个股研报列表

获取个股的历史研究报告列表（标题、机构、评级、预测数据）。

**接口：** `https://reportapi.eastmoney.com/report/list`

**请求方式：** GET

**参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `code` | str | 是 | 股票代码（纯数字，如 `600519`） |
| `pageNo` | int | 否 | 页码，默认 1 |
| `pageSize` | int | 否 | 每页条数，默认 20 |
| `beginTime` | str | 否 | 开始日期 `YYYY-MM-DD` |
| `endTime` | str | 否 | 结束日期 `YYYY-MM-DD` |
| `qType` | int | 否 | 查询类型，默认 0 |

**返回格式：** JSONP（需剥离回调包裹）

**Python 示例：**

```python
import requests, json

url = "https://reportapi.eastmoney.com/report/list"
params = {"code": "600519", "pageNo": 1, "pageSize": 20, "qType": 0}
resp = requests.get(url, params=params)
text = resp.text[resp.text.index("(") + 1 : resp.text.rindex(")")]
data = json.loads(text)
for r in data["data"]:
    print(r["publishDate"], r["orgSName"], r["emRatingName"], r["title"])
```

**关键返回字段：** `title`（标题）、`orgSName`（机构）、`publishDate`（日期）、`emRatingName`（评级）、`infoCode`（研报唯一 ID）

---

#### 2. `stock_news` — 个股新闻

获取东方财富个股相关的最新新闻。

**方式：** `akshare.stock_news_em(stock="600519")`

**参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `stock` | str | 是 | 股票代码，如 `"300059"` |

**返回列：** `code`、`title`、`content`、`public_time`、`url`

**注意：** 仅返回当日最新 20 条新闻，不支持翻页。

```python
import akshare as ak
df = ak.stock_news_em(stock="600519")
print(df[["public_time", "title", "url"]])
```

---

#### 3. `financial_reports` — 三张财报

获取个股的利润表、资产负债表、现金流量表。

**方式：** `akshare.stock_financial_report_sina()`

**参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `stock` | str | 是 | 格式 `"sh600519"` 或 `"sz000001"` |
| `symbol` | str | 是 | `"资产负债表"` / `"利润表"` / `"现金流量表"` |

```python
import akshare as ak

# 利润表
df = ak.stock_financial_report_sina(stock="sh600519", symbol="利润表")
print(df.head(40))  # 近 10 年季度数据
```

---

#### 4. `cninfo_filings` — 巨潮上市公司公告

查询上市公司公告全文（年报、季报、临时公告等）。

**接口：** `http://www.cninfo.com.cn/new/hisAnnouncement/query`

**请求方式：** POST（form-data）

**参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `pageNum` | int | 否 | 页码，默认 1 |
| `pageSize` | int | 否 | 每页条数，默认 30 |
| `stock` | str | 否 | 股票代码（空=全市场） |
| `searchkey` | str | 否 | 关键词搜索 |
| `category` | str | 否 | 公告分类，如 `category_ndbg_szsh`（年报） |
| `seDate` | str | 否 | 日期范围 `YYYY-MM-DD~YYYY-MM-DD` |
| `column` | str | 否 | 板块，如 `szse` |

**公告分类：**

| category 值 | 含义 |
|---|---|
| `category_ndbg_szsh` | 年度报告 |
| `category_bndbg_szsh` | 半年度报告 |
| `category_yjdbg_szsh` | 季度报告 |
| `category_rcjy_szsh` | 日常经营 |
| `category_rzrq_szsh` | 融资/债券 |

```python
import requests

url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
data = {
    "pageNum": "1", "pageSize": "30", "column": "szse",
    "stock": "000001", "category": "category_ndbg_szsh",
    "seDate": "2024-01-01~2024-12-31",
    "sortName": "pubdate", "sortType": "desc",
}
headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded"}
resp = requests.post(url, data=data, headers=headers)
for item in resp.json()["announcements"]:
    print(item["secCode"], item["announcementTitle"],
          f"http://static.cninfo.com.cn/{item['adjunctUrl']}")
```

---

#### 5. `cls_news` — 财联社实时电报

获取财联社 7×24 小时实时财经快讯。

**接口：** `https://www.cls.cn/v1/roll/get_roll_list`

**请求方式：** POST（JSON）

**参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `app` | str | 是 | 固定 `"CailianpressWeb"` |
| `os` | str | 是 | 固定 `"web"` |
| `rn` | int | 否 | 返回条数，默认 100 |
| `last_time` | int | 否 | 时间戳锚点，首次=当前时间，后续取最后一条的 `ctime` |
| `sign` | str | 是 | 签名（需将参数排序后 SHA → MD5） |

**注意：** 接口有签名验证，需计算 `sign` 参数。如使用 akshare 可通过 `ak.stock_info_global_cls()` 获取（部分版本支持）。

**Python 示例（含签名）：**

```python
import requests, time, hashlib

url = "https://www.cls.cn/v1/roll/get_roll_list"
ts = int(time.time())
payload = {
    "app": "CailianpressWeb", "os": "web",
    "rn": 50, "last_time": ts, "sv": "8.4.3",
}
# 签名计算（简化示例，实际需按 cls.cn 最新签名规则）
raw = f"app={payload['app']}&last_time={payload['last_time']}&os={payload['os']}&rn={payload['rn']}"
payload["sign"] = hashlib.md5(hashlib.sha256(raw.encode()).hexdigest().encode()).hexdigest()

resp = requests.post(url, json=payload, headers={"User-Agent": "Mozilla/5.0"})
for item in resp.json().get("data", {}).get("roll_data", [])[:10]:
    print(item["title"], item["ctime"])
```

---

#### 6. `global_news` — 全球财经快讯

获取东方财富全球指数行情快讯。

**接口：** `https://push2.eastmoney.com/api/qt/ulist.np/get`

**请求方式：** GET

**参数：**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `secids` | str | 是 | 逗号分隔的标的 ID（见下表） |
| `fields` | str | 否 | 返回字段，`f2,f3,f4,f12,f14` |
| `fltt` | int | 否 | 2 |

**secids 标的值：**

| secid | 标的 |
|-------|------|
| `1.000001` | 上证指数 |
| `0.399001` | 深证成指 |
| `100.DJIA` | 道琼斯 |
| `100.NDX` | 纳斯达克 |
| `100.HSI` | 恒生指数 |
| `100.N225` | 日经 225 |
| `101.GC00Y` | COMEX 黄金 |
| `102.CL00Y` | NYMEX 原油 |
| `133.USDCNH` | 美元/离岸人民币 |

**返回字段：** `f2`（最新价）、`f3`（涨跌幅%）、`f4`（涨跌额）、`f12`（代码）、`f14`（名称）

```python
import requests, json, re

url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
params = {
    "secids": "1.000001,100.DJIA,100.NDX,100.HSI,100.N225,101.GC00Y,102.CL00Y",
    "fields": "f2,f3,f4,f12,f14", "fltt": 2,
}
resp = requests.get(url, params=params)
data = json.loads(re.search(r"\{.*\}", resp.text).group())
for item in data["data"]["diff"]:
    print(f"{item['f14']}: {item['f2']} ({item['f3']}%)")
```

---

### 19. `global_markets` — 外围行情

> **主源：opentdx `goods_kline()`**（TCP，使用 `EX_MARKET` 枚举）
> **更新：** 每交易日 09:00，自动删除 6 个月前旧数据

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | VARCHAR PK | 品种代码 |
| `date` | DATE PK | 交易日 | 同上 |
| `open` | DOUBLE | 开盘价 | 同上 |
| `high` | DOUBLE | 最高价 | 同上 |
| `low` | DOUBLE | 最低价 | 同上 |
| `close` | DOUBLE | 收盘价 | 同上 |
| `volume` | DOUBLE | 成交量 | 同上 |

**覆盖品种：** 美股（TSLA/AAPL/MSFT/QQQ/SPY）、港股（00700）、黄金、原油、外汇等。

**清理：** 每日增量，自动删除 6 个月前旧数据。

---

## 各源引用接口一览

### OpenTDX (opentdx) — 主数据源

| 接口 | 数据 | 说明 |
|------|------|------|
| `stock_list(SH/SZ/BJ)` | 全品种索引 | TCP，盘中实时 |
| `stock_board_list(HY/DQ/GN)` | 板块列表 + 板块指数价格/领涨股 | TCP，单次调用全量 |
| `stock_board_members()` | 板块成分股行情 | TCP |
| `stock_kline(adjust=QFQ)` | 日K线前复权 | TCP，多线程并发 |
| `stock_capital_flow()` | 个股资金流向 | TCP，主力净流入+5日分单 |
| `goods_kline()` | 外围行情（美股/港股/期货） | TCP，扩展市场 |

### 腾讯API

| 接口 | 数据 | 说明 |
|------|------|------|
| `qt.gtimg.cn/q=sh600519,sz000001` | 实时行情（PE/PB/市值/换手率/涨跌停价） | HTTP，盘中实时，批量 80 只/次 |

### 同花顺

| 接口 | 数据 | 说明 |
|------|------|------|
| `zx.10jqka.com.cn/event/api/getharden` | 当日强势股 + 题材归因（人工标签） | HTTP，需 User-Agent |

### akshare

| 接口 | 数据 | 说明 |
|------|------|------|
| `stock_lhb_detail_em()` | 龙虎榜 | HTTP，日期需 YYYYMMDD 格式 |
| `stock_margin_detail_sse/szse()` | 融资融券 | HTTP |
| `stock_yjbb_em()` | 季度财务 | HTTP |
| `stock_hsgt_fund_flow_summary_em()` | 北向资金每日汇总 | HTTP |
| `stock_hot_follow_xq()` | 雪球关注热度 | HTTP |
| `stock_board_industry/concept_name_em()` | 板块涨跌排名（备源） | HTTP，依赖 push2 |
| `stock_info_a_code_name()` | A 股代码简称全量 | 日频，15:00 后可获取，~7s |
| `stock_yjbb_em(date)` | 季度财务全市场批量 | 季度数据，~20s |
| `stock_lhb_detail_em(start, end)` | 龙虎榜（19字段） | 盘后，~5s |
| `stock_hot_follow_xq()` | 雪球关注热度排名 | 盘中实时，~10s |
| `stock_margin_detail_sse(date)` | 上交所融资融券全量 | 盘后 ~16:00 |
| `stock_margin_detail_szse()` | 深交所融资融券全量 | 盘后 |
| `stock_board_industry_name_em()` | 行业板块涨跌排名 | 盘后 |
| `stock_board_concept_name_em()` | 概念板块涨跌排名 | 盘后 |
| `stock_news_em(code)` | 个股新闻 | 盘中实时 |

### 东方财富 API

| 接口 | 数据 | 说明 |
|------|------|------|
| `reportapi.eastmoney.com/report/list` | 个股研报列表（`research_reports`） | HTTP，JSONP |
| `push2.eastmoney.com/api/qt/ulist.np/get` | 全球指数行情（`global_news`） | HTTP，JSONP |
| `datacenter-web.eastmoney.com` | 大宗交易、解禁、股东户数、除权除息 | HTTP |
| `np-weblist.eastmoney.com` | 全球财经快讯 | HTTP |

### baostock

| 接口 | 数据 | 说明 |
|------|------|------|
| `query_trade_dates()` | 交易日历 | TCP |
| `query_history_k_data_plus(adjustflag=2)` | OHLCV 历史（备源）、估值历史回补 | TCP，仅 sh/sz |

### 巨潮资讯

| 接口 | 数据 | 说明 |
|------|------|------|
| `cninfo.com.cn/new/hisAnnouncement/query` | 上市公司公告全文（`cninfo_filings`） | HTTP POST |
| `static.cninfo.com.cn/{adjunctUrl}` | 公告 PDF 下载 | HTTP GET |

### 新浪财经

| 接口 | 数据 | 说明 |
|------|------|------|
| `stock_financial_report_sina(stock, symbol)` | 三张财报（`financial_reports`） | akshare 封装，全量历史数据 |

### 财联社

| 接口 | 数据 | 说明 |
|------|------|------|
| `cls.cn/v1/roll/get_roll_list` | 实时电报（`cls_news`） | HTTP POST，需签名 |

---

---

## 调度系统设计

### 配置文件（`config.yaml`）

```yaml
schedule:
  core: "16:00"               # 组名: 时间（HH:MM）
  signals: "17:00"
  global_markets: "09:00"     # 单表独立时间
  weekly: "weekly 10:00"      # 每周（行业/概念分类）
  fundamentals: "monthly 10:00"     # 月度（每月 1 号）
  holder_count: "monthly 10:00"     # 月度（每月 1 号）
  trade_calendar: "yearly 10:00"    # 年度

schedule_groups:
  core:
    - stock_universe
    - xdxr_events
    - daily_ohlcv
    - daily_valuation
    - capital_flow
    - northbound_flow
    - board_daily
  signals:
    - dragon_tiger
    - hot_stocks
    - hot_reasons
    - margin_trading
    - block_trades
    - lockup_calendar
    - indicator_values
  weekly:
    - stock_classification
    - concept_blocks
```

### 执行模式

engine.py 支持两种执行模式：

**全量模式**（`ingestion daily-update`，无 `--tables`）— 按 Wave 分层并行：

| Wave | 并行策略 | 表 |
|------|---------|---|
| Wave 0 | 并行 | stock_universe, trade_calendar, global_markets, northbound_flow, board_daily, dragon_tiger, hot_stocks, hot_reasons, margin_trading, block_trades, lockup_calendar, holder_count, fundamentals |
| Wave 1 | 串行（有依赖） | xdxr_events → daily_ohlcv |
| Wave 2 | 并行 | daily_valuation, capital_flow, stock_classification, concept_blocks, indicator_values |

**定向模式**（指定 `tables` 参数）— 直接并行跑目标表，跳过 Wave。scheduler 每次触发均使用此模式。

### 调度规则

| 规则 | 说明 |
|------|------|
| **组调度** | `core: "16:00"` → 定向模式并行跑组内所有表 |
| **单表调度** | `global_markets: "09:00"` → 只跑这一个表 |
| **两者同时** | 表同时在组里和单独调度 → 两个时间都会跑 |
| **超时处理** | 前一次没跑完 → 下一次自动跳过，不排队不并发 |
| **依赖处理** | 定向模式假定依赖数据已在 DB 中（来自之前的全量/组更新），不重复跑依赖表 |
| **并行安全** | 每个 worker 独立 DB 连接写入，DuckDB 内部串行化写操作，无冲突 |

### 应用层控制

```python
from src.ingestion.config import load_config

cfg = load_config("config.yaml")

# 改组内容
cfg.schedule_groups["core"].append("hot_reasons")

# 改调度时间
cfg.schedule.data["core"] = "15:00"
cfg.schedule.data["daily_ohlcv"] = "15:30"

# 新增自定义组
cfg.schedule.data["my_group"] = "14:00"
cfg.schedule_groups["my_group"] = ["daily_ohlcv", "daily_valuation"]

# 环境变量覆盖（Docker 部署用）
# INGESTION_SCHEDULE_DAILY_OHLCV="15:30"
```

### 默认时序

| 时间 | 触发 | 内容 |
|------|------|------|
| 09:00 | `global_markets` | 外围行情 |
| 16:00 | `core` | 日线 + 估值 + 资金流 + 北向 + 板块 + 除权（并行） |
| 17:00 | `signals` | 龙虎榜 + 两融 + 题材归因 + 热度 + 大宗 + 解禁 + 技术指标（并行） |
| 每周一 10:00 | `weekly` | 行业分类 + 概念板块（低频） |
| 每月 1 日 10:00 | `holder_count` | 股东户数 |
| 每月 10:00 | `fundamentals` | 季度财务 |
| 每年 12/31 10:00 | `trade_calendar` | 交易日历 |

### 命令

```bash
# 启动调度（Docker 默认）
ingestion schedule

# 手动触发全量更新
ingestion daily-update

# 选择性回补
ingestion backfill --tables daily_ohlcv
ingestion backfill --tables daily_valuation
```

## 数据源认证与可用性

| 数据源 | 协议 | 可用性 | 数据类型 | 源更新时间 | 调度时间 | 备用源 | 备源差异 | 降级调整 |
|--------|------|--------|---------|-----------|---------|--------|---------|---------|
| **OpenTDX** | TCP | ✅ **通** | 全品种OHLCV(QFQ)、股票列表、板块归属、美股/港股/外汇K线 | 盘中实时（3-5s延迟） | 16:00后 | baostock (sh/sz only) | 无北交所、需adjustflag=2、有pctChg/turn字段 | 过滤bj代码 |
| **baostock** | TCP | ✅ **通** | OHLCV(备)、估值历史(peTTM/pbMRQ/psTTM)、交易日历 | 收盘后 **~17:00** | 仅全量回补 | OpenTDX | OpenTDX无pe/pb直接返回需计算 | 无 |
| **腾讯API qt.gtimg.cn** | HTTP | ✅ **通** | PE_TTM/PB/总市值/流通市值/换手率/涨跌停价 | **盘中实时** | 16:00后 | — | — | — |
| **东财 push2** | HTTP | ✅ **通** | 个股分钟级资金流向(主力/超大单/大单/中单/小单)、北向资金 | **盘中实时** | 16:00后 | OpenTDX stock_capital_flow | 只有今日主力净流入，无分单明细 | 仅可补net_main |
| **同花顺 10jqka** | HTTP | ✅ **通** | 当日强势股题材归因（人工运营tags） | **盘中实时** | 17:00后 | — | — | — |
| **东财 datacenter** | HTTP | ✅ **通** | 龙虎榜/解禁/大宗/股东/分红/除权；研报列表 | 盘后更新 | 17:00后 | — | — | — |
| **akshare stock_yjbb_em** | HTTP | ✅ **通** | 季度财务全市场批量(含EPS/ROE/营收/净利/毛利率) | 季度数据 | 每季度末 | mootdx finance | 逐只TCP，37字段更全但仅自选股 | 仅自选股 |
| **雪球 stock_hot_follow_xq** | HTTP | ✅ **通** | 全市场关注热度排名（含代码/名称/热度值/最新价） | **盘中实时** | 17:00后 | — | — | — |
| **百度K线** | HTTP | ✅ **通** | 日K线 + MA5/MA10/MA20 均价直出 | **盘中实时** | 应用层按需 | — | — | — |
| **akshare SSE/SZSE margin** | HTTP | ✅ **通** | 融资融券全量（含融资余额/买入/偿还、融券余量/卖出/偿还） | 盘后 **~16:00** | 17:00后 | — | — | — |

---

## 常用命令

```bash
# 启动定时调度（Docker 默认）
ingestion schedule

# 手动触发每日更新
ingestion daily-update

# 选择性回补历史数据
ingestion backfill --tables daily_ohlcv
ingestion backfill --tables daily_valuation

# 查看各表数据行数
ingestion status

# 启动调度器（阻塞，适合 systemd）
ingestion schedule
```


---



## 已知限制

| 表 | 限制 | 原因 |
|---|------|------|
| `xdxr_events` | `cash_dividend`/`transfer_ratio`/`category` 始终 NULL | 东财 API 已不再返回这些字段 |
| `board_daily` | `total_mv`/`turnover_rate`/`up_count`/`down_count`/`leader_pct` 全 None | opentdx 板块 API 不提供这些字段 |
| `lockup_calendar` | `unlock_vol`/`status` NULL | 东财 API 已不再提供这些字段 |
| `capital_flow` | `net_super_5d` 等字段为 5 日累计而非当值 | opentdx API 限制 |
| `holder_count` | `change_qoq`/`avg_shares` 始终 NULL | 东财 API 已不再提供这些字段 |

