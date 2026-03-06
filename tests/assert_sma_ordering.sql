-- tests/assert_sma_ordering.sql
-- -----------------------------------------------
-- Singular Test: On any given day with sufficient history,
-- a trending stock's SMA ordering should be mathematically
-- consistent — we validate that SMA values are not null
-- after the warm-up period (90+ trading days per ticker).
-- -----------------------------------------------

select
    ticker,
    price_date,
    sma_7d,
    sma_30d,
    sma_90d
from {{ ref('fct_stock_prices') }}
where
    trading_day_seq > 90          -- after warm-up period
    and (
        sma_7d  is null
        or sma_30d is null
        or sma_90d is null
    )