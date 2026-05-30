---
name: ashare-data-warehouse
description: A 股数据 — 查询本地 DuckDB 数据库 / 通过 akshare 和 HTTP 在线获取 / 管理项目部署
metadata:
  type: skill
  project: ashare-data-warehouse
---

# A 股数据仓库 — Agent 使用指南

提供两种工作模式，Agent 可按需选择：

| 模式 | 说明 | 适合场景 |
|------|------|---------|
| **🌐 纯在线** | 通过 akshare + HTTP API 在线获取所有数据 | 快速问答、单次分析，无需部署 |
| **💾 本地部署** | 部署项目到本地，查询 DuckDB（性能更好，支持历史回溯） | 高频分析、回测、大数据量查询 |

---

## 一、🌐 纯在线模式

所有数据通过 akshare 或 HTTP API 在线获取，不需要部署项目。

### 行情 K 线

```python
import akshare as ak

# A 股日 K 线（前复权），默认返回近一年
df = ak.stock_zh_a_hist(symbol="600519", period="daily",
                         start_date="20250101", end_date="20260524",
                         adjust="qfq")
# 列: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
```

### 估值数据

```python
import requests

# 腾讯行情接口，批量最多 80 只
resp = requests.get("https://qt.gtimg.cn/q=sh600519,sz000001,sz300750")
# 返回 PE_TTM, PB, 总市值, 流通市值, 换手率等
for line in resp.text.strip().split(";"):
    if line:
        fields = line.split("~")
        name, code, pe, pb, total_mv = fields[1], fields[2], fields[39], fields[46], fields[45]
```

### 股票列表

```python
df = ak.stock_info_a_code_name()  # 全市场 ~6000 只代码+名称
```

### 行业分类

```python
# 行业板块成分股
df = ak.stock_board_industry_cons_em(symbol="半导体")
```

### 概念板块

```python
df = ak.stock_board_concept_cons_em(symbol="人工智能")
```

### 资金流向

```python
# 个股资金流向（最近 100 个交易日）
df = ak.stock_individual_fund_flow(stock="600519", market="sh")
# 列: 日期, 主力净流入, 小单净流入, 中单净流入, 大单净流入, 超大单净流入
```

### 北向资金

```python
df = ak.stock_hsgt_fund_flow_summary_em(symbol="北上")
# 或按日查询
df = ak.stock_hsgt_north_net_flow_in_em(symbol="沪股通")
```

### 融资融券

```python
df = ak.stock_margin_detail_sse(date="20260522")
df = ak.stock_margin_detail_szse()
```

### 龙虎榜

```python
df = ak.stock_lhb_detail_em(start_date="20260501", end_date="20260524")
```

### 板块涨跌排名

```python
df_industry = ak.stock_board_industry_name_em()    # 行业板块排名
df_concept = ak.stock_board_concept_name_em()      # 概念板块排名
```

### 季度财务

```python
df = ak.stock_yjbb_em(date="2025-12-31")  # 全市场季度财报
# 列: 股票代码, 营业收入, 净利润, 每股收益, ROE, 毛利率, 营收同比, 净利同比
```

### 市场热度

```python
df = ak.stock_hot_follow_xq()  # 雪球关注热度排名
```

### 交易日历

```python
df = ak.tool_trade_date_hist_sina()  # 全市场交易日历
```

### 外围指数

```python
import requests, json, re

# 东方财富全球指数行情
url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
params = {"secids": "1.000001,100.DJIA,100.NDX,100.HSI,100.N225,101.GC00Y,102.CL00Y",
          "fields": "f2,f3,f4,f12,f14", "fltt": 2}
resp = requests.get(url, params=params)
data = json.loads(re.search(r"\{.*\}", resp.text).group())
for item in data["data"]["diff"]:
    print(f"{item['f14']}: {item['f2']} ({item['f3']}%)")
```

---

## 二、💾 本地部署模式

### 连接数据库

```python
from src.ingestion.db import IngestionDB
db = IngestionDB()
df = db.conn.execute("SELECT COUNT(*) FROM daily_ohlcv").fetchdf()
```

### 初始化 / 更新数据

```bash
# 检查状态
python -m src.ingestion status

# 首次全量建库
ingestion backfill

# 每日增量更新
ingestion daily-update

# 指定表更新
ingestion daily-update -t daily_ohlcv,global_markets
```

### 27 张表与常用查询

| 层级 | 表名 | 用途 |
|------|------|------|
| **基础** | `trade_calendar` | 交易日历 |
| | `stock_universe` | 全品种索引（含上市日期） |
| | `stock_classification` | 行业 + 地域分类 |
| | `concept_blocks` | 概念板块 N:N 映射 |
| **行情** | `daily_ohlcv` | **核心** — 日 K 线（前复权）含涨跌幅/换手率 |
| | `daily_valuation` | PE(TTM)/PB/PS/PCF/总市值/流通市值 |
| | `xdxr_events` | 除权除息事件 |
| | `global_markets` | 外围指数日 K 线（美股/港股/黄金/原油） |
| **资金** | `capital_flow` | 主力/超大单/大单/中单/小单净流入 |
| | `northbound_flow` | 北向资金（沪股通/深股通） |
| | `margin_trading` | 融资融券余额明细 |
| **信号** | `dragon_tiger` | 龙虎榜（含上榜后 1/2/5/10 日表现） |
| | `dragon_tiger_seats` | 龙虎榜营业部席位明细 |
| | `board_daily` | 板块涨跌排名 + 领涨股 |
| | `hot_stocks` | 雪球关注热度排名 |
| | `hot_reasons` | 同花顺题材归因标签 |
| | `block_trades` | 大宗交易（含折溢价率/营业部） |
| | `lockup_calendar` | 限售解禁日历（未来 90 天） |
| **技术指标** | `indicator_values` | 30 项技术指标（MACD/KDJ/RSI/BOLL 等）D/W/M 三频 |
| **财务** | `fundamentals` | 季度财报（EPS/ROE/营收/利润/毛利率/现金流） |
| | `holder_count` | 股东户数（环比变化） |
| | `eps_consensus` | 机构一致预期 EPS（当年+未来 2 年） |
| | `research_reports` | 个股研报（标题/机构/评级/EPS 预测） |
| **股东** | `shareholder_changes` | 大股东增减持 |
| | `announcements` | 巨潮公告 |
| **资讯** | `cls_telegram` | 财联社实时快讯 |
| | `stock_news` | 个股新闻 |

```sql
-- 个股行情 + 估值
SELECT a.date, a.close, a.pct_chg, b.pe_ttm, b.pb, b.total_mv
FROM daily_ohlcv a
LEFT JOIN daily_valuation b ON a.symbol = b.symbol AND a.date = b.date
WHERE a.symbol = '600519'
ORDER BY a.date DESC LIMIT 60;

-- 北向资金趋势
SELECT trade_date, market, net_buy
FROM northbound_flow
ORDER BY trade_date DESC LIMIT 30;

-- 龙虎榜上榜后表现
SELECT symbol, reason, net_buy, total_amount, perf_1d, perf_2d, perf_5d
FROM dragon_tiger
WHERE date = '2026-05-22'
ORDER BY net_buy DESC;

-- 概念板块成分股
SELECT u.symbol, u.name, cb.concept_name
FROM stock_universe u
JOIN concept_blocks cb ON u.symbol = cb.symbol
WHERE cb.concept_name LIKE '%人工智能%';

-- 低估值 + 资金流入筛选
SELECT v.symbol, u.name, v.pe_ttm, v.pb, c.net_main, f.roe
FROM daily_valuation v
JOIN stock_universe u ON v.symbol = u.symbol
LEFT JOIN capital_flow c ON v.symbol = c.symbol AND v.date = c.date
LEFT JOIN fundamentals f ON v.symbol = f.symbol
WHERE v.date = (SELECT MAX(date) FROM daily_valuation)
  AND v.pe_ttm BETWEEN 10 AND 30
  AND v.pb < 3
  AND c.net_main > 0
ORDER BY c.net_main DESC LIMIT 20;
```

---

## 三、🌐 在线实时数据（仅在线获取，不落库）

### 个股研报

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

### 个股新闻

```python
df = ak.stock_news_em(stock="600519")  # 当日最新 20 条
```

### 三张财报

```python
df_profit = ak.stock_financial_report_sina(stock="sh600519", symbol="利润表")
df_balance = ak.stock_financial_report_sina(stock="sh600519", symbol="资产负债表")
df_cash = ak.stock_financial_report_sina(stock="sh600519", symbol="现金流量表")
```

### 上市公司公告

```python
import requests
url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
data = {"pageNum": "1", "pageSize": "30", "column": "szse",
        "stock": "000001", "category": "category_ndbg_szsh",
        "seDate": "2025-01-01~2025-12-31"}
resp = requests.post(url, json=data, headers={"User-Agent": "Mozilla/5.0"})
for item in resp.json()["announcements"]:
    print(item["secCode"], item["announcementTitle"],
          f"http://static.cninfo.com.cn/{item['adjunctUrl']}")
```

### 财联社实时电报

```python
import requests, time, hashlib
url = "https://www.cls.cn/v1/roll/get_roll_list"
payload = {"app": "CailianpressWeb", "os": "web", "rn": 50, "last_time": int(time.time())}
raw = f"app={payload['app']}&last_time={payload['last_time']}&os={payload['os']}&rn={payload['rn']}"
payload["sign"] = hashlib.md5(hashlib.sha256(raw.encode()).hexdigest().encode()).hexdigest()
resp = requests.post(url, json=payload, headers={"User-Agent": "Mozilla/5.0"})
for item in resp.json().get("data", {}).get("roll_data", [])[:10]:
    print(item["title"], item["ctime"])
```

---

## 四、典型分析场景

### 智能选股助手

```python
# 在线方式：akshare 筛选
df = ak.stock_yjbb_em(date="2025-12-31")
# ROE > 15%, 营收增长 > 20%, 毛利率 > 30%
candidates = df[(df['净资产收益率'] > 15) & (df['营收同比增长'] > 20) & (df['销售毛利率'] > 30)]
```

### 每日复盘简报

1. 查 `ak.stock_board_industry_name_em()` — 板块排名
2. 查 `ak.stock_lhb_detail_em()` — 龙虎榜
3. 查 `ak.stock_hsgt_fund_flow_summary_em()` — 北向资金
4. 查财联社电报 — 当日重大快讯

### 个股全面分析

1. `stock_zh_a_hist` — K 线走势
2. `stock_financial_report_sina` — 三张财报
3. `stock_individual_fund_flow` — 资金流向
4. `stock_news_em` — 最新新闻
5. `reportapi.eastmoney.com` — 机构研报
6. `cninfo` — 最新公告

---

## 五、注意事项

- **在线接口频率**：akshare 和东方财富 API 有频率限制，避免高频循环请求
- **数据延迟**：行情数据需等 A 股收盘后（约 16:00）才完整
- **本地部署优势**：适合大量股票的历史数据回测，在线模式适合单只股票的快速查询
- **opentdx**：如果部署本地项目，wheel 在 `wheels/` 目录中（GitHub 源已删）
