-- tests/assert_positive_prices.sql
-- -----------------------------------------------
-- Singular Test: All OHLCV price values must be positive.
-- Zero or negative prices are invalid for equities.
-- -----------------------------------------------

select
    ticker,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
from {{ ref('fct_stock_prices') }}
where
    open_price  <= 0
    or high_price  <= 0
    or low_price   <= 0
    or close_price <= 0
    or volume      < 0