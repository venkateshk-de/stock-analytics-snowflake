-- tests/assert_high_price_gte_low_price.sql
-- -----------------------------------------------
-- Singular Test: High price must always be >= low price.
-- A violation means corrupted OHLCV data from the source.
-- -----------------------------------------------

select
    ticker,
    price_date,
    high_price,
    low_price
from {{ ref('fct_stock_prices') }}
where
    high_price < low_price