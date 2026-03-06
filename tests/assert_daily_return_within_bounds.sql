-- tests/assert_daily_return_within_bounds.sql
-- -----------------------------------------------
-- Singular Test: Daily return should never exceed ±75%
-- A return beyond this range almost certainly indicates
-- bad data (e.g. stock split not adjusted, ingestion error).
-- -----------------------------------------------

select
    ticker,
    price_date,
    daily_return_pct
from {{ ref('fct_stock_prices') }}
where
    daily_return_pct is not null
    and (
        daily_return_pct > 75
        or daily_return_pct < -75
    )