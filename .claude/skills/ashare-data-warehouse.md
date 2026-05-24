---
name: ashare-data-warehouse
description: A 股数据仓库 — 查询本地 DuckDB 数据库、管理数据更新、在线获取新闻研报等实时数据
metadata:
  type: skill
  project: ashare-data-warehouse
---

# A 股数据仓库 — Agent 使用指南

本项目提供 A 股全市场（沪/深/北）结构化数据，包含 **19 张表**（行情、估值、资金、信号、财务、外围），以及在线实时数据接口（新闻、研报、公告、快讯）。

---

## 1. 数据库查询

### 连接

```python
from src.ingestion.db import IngestionDB
db = IngestionDB()  # 默认路径: ./data/ingestion/stock_research.duckdb
df = db.conn.execute("SELECT COUNT(*) FROM daily_ohlcv").fetchdf()
```

### 19 张表概览

| 层级 | 表名 | 用途 |
|------|------|------|
| **基础** | `trade_calendar` | 交易日历 |
| | `stock_universe` | 全品种索引（~6000 只） |
| | `stock_classification` | 行业 + 地域分类 |
| | `concept_blocks` | 概念板块映射 |
| **行情** | `daily_ohlcv` | **核心** — 日 K 线（前复权 QFQ） |
| | `daily_valuation` | 每日估值（PE/PB/PS/PCF/市值） |
| | `xdxr_events` | 除权除息事件 |
| **资金** | `capital_flow` | 主力净流入、分单流向 |
| | `northbound_flow` | 北向资金每日汇总 |
| | `margin_trading` | 融资融券余额 |
| **信号** | `dragon_tiger` | 龙虎榜明细（含上榜后 1/2/5/10 日表现） |
| | `board_daily` | 板块每日涨跌排名、领涨股 |
| | `hot_stocks` | 雪球关注热度排名 |
| | `hot_reasons` | 同花顺题材归因标签 |
| | `block_trades` | 大宗交易（含买卖营业部） |
| | `lockup_calendar` | 限售解禁日历（含未来 90 天） |
| **财务** | `fundamentals` | 季度财报（EPS/ROE/营收/利润/毛利率） |
| | `holder_count` | 股东户数（季度环比） |
| **外围** | `global_markets` | 美股/港股/黄金/原油/外汇日 K 线 |

### 常用查询模板

#### 个股行情

```sql
-- 个股日 K 线 + 涨跌幅 + 换手率
SELECT date, open, high, low, close, volume, amount, pct_chg, turnover_rate
FROM daily_ohlcv
WHERE symbol = '600519'
ORDER BY date DESC LIMIT 120;
```

#### 行情 + 估值关联

```sql
-- 个股行情 + 估值
SELECT a.date, a.close, a.pct_chg, b.pe_ttm, b.pb, b.total_mv
FROM daily_ohlcv a
LEFT JOIN daily_valuation b ON a.symbol = b.symbol AND a.date = b.date
WHERE a.symbol = '600519'
ORDER BY a.date DESC LIMIT 60;
```

#### 行业板块成分股

```sql
-- 查询某行业的所有股票
SELECT u.symbol, u.name, c.industry, c.region
FROM stock_universe u
JOIN stock_classification c ON u.symbol = c.symbol
WHERE c.industry LIKE '%半导体%';
```

#### 北向资金

```sql
-- 北向资金最近 N 日趋势
SELECT trade_date, market, net_buy
FROM northbound_flow
ORDER BY trade_date DESC LIMIT 30;
```

#### 龙虎榜 + 上榜后表现

```sql
-- 查询某日龙虎榜 + 上榜后表现
SELECT symbol, reason, net_buy, total_amount, perf_1d, perf_2d, perf_5d
FROM dragon_tiger
WHERE date = '2026-05-22'
ORDER BY net_buy DESC;
```

#### 概念板块关联

```sql
-- 查询某个概念下的所有股票
SELECT u.symbol, u.name, cb.concept_name
FROM stock_universe u
JOIN concept_blocks cb ON u.symbol = cb.symbol
WHERE cb.concept_name LIKE '%人工智能%';
```

#### 异常检测查询

```sql
-- 估值异常: 当前 PE 处于近 5 年高分位
WITH pe_stats AS (
  SELECT symbol,
    PERCENT_RANK() OVER (PARTITION BY symbol ORDER BY pe_ttm) as pe_percentile
  FROM daily_valuation
  WHERE date >= (SELECT MAX(date) - INTERVAL '5 years' FROM daily_valuation)
)
SELECT DISTINCT v.symbol, u.name, v.pe_ttm, v.total_mv, s.pe_percentile
FROM daily_valuation v
JOIN pe_stats s ON v.symbol = s.symbol
JOIN stock_universe u ON v.symbol = u.symbol
WHERE v.date = (SELECT MAX(date) FROM daily_valuation)
  AND v.pe_ttm > 0
  AND s.pe_percentile > 0.9
ORDER BY s.pe_percentile DESC LIMIT 20;
```

---

## 2. 数据初始化与更新

如果数据库不存在或需要更新，按以下步骤操作：

### 检查状态

```bash
# 查看各表数据行数 / 检查是否已初始化
cd /path/to/ashare-data-warehouse
python -m src.ingestion status
```

### 首次部署（初始化全量数据）

```bash
docker compose up -d                              # 启动调度器
docker compose exec ingestion backfill            # 拉取全量历史数据（耗时较长）
```

或本地运行：

```bash
cd /path/to/ashare-data-warehouse
pip install -r requirements.txt
pip install -e .
ingestion backfill                                 # 全量回补
```

### 每日增量更新

```bash
ingestion daily-update                             # 全量增量
ingestion daily-update -t daily_ohlcv,global_markets  # 单表增量
```

### 依赖与注意事项

- **opentdx** — 主数据源，wheel 已在 `wheels/` 目录中（GitHub 源已删）
- **网络要求** — 需要访问通达信 TCP 服务器（7709 端口）、东方财富 HTTP API 等
- **首次全量回补** — 根据网络情况可能需要 30 分钟到数小时
- **磁盘空间** — DuckDB 约 1.2 GB，随数据增长会缓慢增加

---

## 3. 在线数据接口（应用层按需拉取）

以下数据**不存储在 DuckDB 中**，应用层按需通过 HTTP 或 akshare 获取。

### 研报列表 (`research_reports`)

获取个股研究报告。

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

### 个股新闻 (`stock_news`)

```python
import akshare as ak
df = ak.stock_news_em(stock="600519")
# 返回列: code, title, content, public_time, url
```

### 三张财报 (`financial_reports`)

```python
import akshare as ak
df = ak.stock_financial_report_sina(stock="sh600519", symbol="利润表")
# symbol 可选: "资产负债表" / "利润表" / "现金流量表"
```

### 上市公司公告 (`cninfo_filings`)

```python
import requests
url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
data = {
    "pageNum": "1", "pageSize": "30", "column": "szse",
    "stock": "000001", "category": "category_ndbg_szsh",
    "seDate": "2025-01-01~2025-12-31",
}
headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/x-www-form-urlencoded"}
resp = requests.post(url, data=data, headers=headers)
for item in resp.json()["announcements"]:
    print(item["secCode"], item["announcementTitle"],
          f"http://static.cninfo.com.cn/{item['adjunctUrl']}")
```

### 财联社实时电报 (`cls_news`)

```python
import requests, time, hashlib
url = "https://www.cls.cn/v1/roll/get_roll_list"
ts = int(time.time())
payload = {"app": "CailianpressWeb", "os": "web", "rn": 50, "last_time": ts}
# 签名: 按参数排序拼接 → SHA256 → MD5
raw = f"app={payload['app']}&last_time={payload['last_time']}&os={payload['os']}&rn={payload['rn']}"
payload["sign"] = hashlib.md5(hashlib.sha256(raw.encode()).hexdigest().encode()).hexdigest()
resp = requests.post(url, json=payload, headers={"User-Agent": "Mozilla/5.0"})
for item in resp.json().get("data", {}).get("roll_data", [])[:10]:
    print(item["title"])
```

### 全球指数行情 (`global_news`)

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

## 4. 典型分析场景

### 场景 A：回答用户关于某只股票的问题

1. 先检查本地 DB 是否可用：看 `data/ingestion/stock_research.duckdb` 是否存在
2. 如果存在，用 SQL 查 `daily_ohlcv` + `daily_valuation` + `capital_flow` 等表
3. 如果需要最新新闻/研报，调用在线接口补充
4. 如果需要财务数据（非基本面摘要），调用 `stock_financial_report_sina`

### 场景 B：生成每日复盘

1. 确保 DB 有最新数据（如果数据较旧，先跑 `ingestion daily-update`）
2. 查询 `board_daily`（板块排名）、`dragon_tiger`（龙虎榜）、`northbound_flow`（北向资金）
3. 结合在线 `cls_news`（当日快讯）做综合解读

### 场景 C：选股筛选

1. 用 SQL 做多表 JOIN 筛选：
   - `daily_valuation` → 低 PE、低 PB
   - `capital_flow` → 主力净流入
   - `holder_count` → 筹码集中
   - `fundamentals` → ROE > 15%
2. 在线查 `research_reports` 验证机构覆盖情况
3. 在线查 `stock_news` 看近期是否有负面新闻

---

## 5. 注意事项

- **DB 查询只读**：Agent 只应执行 SELECT 查询，不要写/改表结构
- **数据延迟**：行情数据通常 16:00 后更新（A 股收盘后），外围数据 09:00 前更新
- **在线接口频率**：财联社、巨潮资讯有反爬机制，不要高频请求
- **opentdx 限流**：回补 K 线时并发 ≤ 3，首次全量建库可能需要等待
