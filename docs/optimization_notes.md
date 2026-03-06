# Snowflake Optimizations

## Overview
This section documents the performance optimizations that can be applied to the
stock analytics pipeline and their measured impact.

---

## 1. Incremental Models

### What Changed
Converted two mart models from `TABLE` to `INCREMENTAL` materialization
using the `merge` strategy.

| Model | Before | After | Strategy |
|---|---|---|---|
| `fct_stock_prices` | `table` (full refresh) | `incremental` (merge) | merge on `stock_price_id` |
| `dim_companies` | `table` (full refresh) | `incremental` (merge) | merge on `company_id` |

### How Incremental Works in This Project

```
First run (full load):
  All records from int_stock_moving_averages → fct_stock_prices

Subsequent runs (incremental):
  Only records where ingested_at > max(ingested_at) in fct_stock_prices
  → merge into fct_stock_prices (insert new, update changed)
```

### Why This Matters
- **Before:** Every dbt run reprocessed all ~32,000 rows from scratch
- **After:** Only new rows since the last run are processed
- For daily ingestion of 25 tickers = only ~25 new rows processed per run
- Reduces compute time and Snowflake credit consumption significantly

---

## 2. Clustering Keys

### What Changed
Added clustering keys to both mart tables to improve query pruning performance.

| Model | Clustering Keys | Why These Columns |
|---|---|---|
| `fct_stock_prices` | `(price_date, ticker)` | Most queries filter by date range and/or specific ticker |
| `dim_companies` | `(sector, ticker)` | Most queries filter by sector or join on ticker |

### How Clustering Helps
Without clustering, Snowflake scans all micro-partitions in a table
even for highly filtered queries. With clustering on `price_date`:

```sql
-- This query without clustering: scans ALL partitions
-- This query WITH clustering on price_date: scans only relevant partitions
SELECT * FROM fct_stock_prices
WHERE price_date >= '2024-01-01'
AND ticker = 'AAPL'
```

### Clustering Effectiveness
Run this to verify clustering is working:
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'STOCK_ANALYTICS.MARTS.FCT_STOCK_PRICES',
    '(PRICE_DATE, TICKER)'
);
```

A healthy output shows:
- `average_depth` close to 1.0 (lower is better)
- `average_overlaps` close to 0 (lower is better)

---

## 3. Performance Metrics
### The metrics has been left blank intentionally as there won't be impactable measure with data size of around 45000 rows
### The optimization pattern still holds true when this data grows at scale of millions or billions of rows.

### Before Optimization (Full Refresh TABLE)
_(Fill in after running QUERY 1 in performance_analysis.sql)_

| Metric | Value |
|---|---|
| fct_stock_prices full refresh time | ___ seconds |
| MB scanned | ___ MB |
| Partitions scanned / total | ___ / ___ |
| Partition pruning % | __% |

### After Optimization (Incremental + Clustering)
_(Fill in after first incremental run)_

| Metric | Value |
|---|---|
| fct_stock_prices incremental run time | ___ seconds |
| MB scanned | ___ MB |
| Partitions scanned / total | ___ / ___ |
| Partition pruning % | __% |
| Improvement | __% faster, __% less data scanned |

---

## 4. Key Learnings

### When to Use Incremental vs Table
| Use `table` when | Use `incremental` when |
|---|---|
| Dataset is small (<1M rows) | Dataset grows continuously over time |
| Full recalculation needed every run | New rows can be processed independently |
| window functions span entire history | Lookback window is bounded |
| During development/debugging | Pipeline is stable and in production |

### Incremental Caveats in This Project
Window functions like `LAG()` and moving averages in intermediate models
are calculated across the **full history** of each ticker. This means:
- Intermediate models remain as `view` — they always recalculate fully
- Only the final `fct_stock_prices` and `dim_companies` are incremental
- This is the correct pattern — incremental at the serving layer,
  full recalc at the transformation layer

### Clustering Key Selection Principles
1. Choose columns that appear most frequently in `WHERE` and `JOIN` clauses
2. Put the highest cardinality column first (date before ticker)
3. Avoid clustering on boolean or low-cardinality columns
4. Monitor with `SYSTEM$CLUSTERING_INFORMATION()` regularly

---

## 5. Commands to Run Optimized Pipeline

```bash
# Full refresh (use when reloading all data)
dbt build --full-refresh --select tag:marts

# Incremental run (normal daily operation)
dbt build --select tag:marts

# Check only fct_stock_prices incrementally
dbt run --select fct_stock_prices
```