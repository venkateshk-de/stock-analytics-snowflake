-- tests/assert_no_future_dates.sql
-- -----------------------------------------------
-- Singular Test: No price records should have a future date.
-- Future dates indicate a data ingestion or timezone issue.
-- -----------------------------------------------

select
    ticker,
    price_date
from {{ ref('fct_stock_prices') }}
where
    price_date > current_date()