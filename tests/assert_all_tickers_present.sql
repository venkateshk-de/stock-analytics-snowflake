-- tests/assert_all_tickers_present.sql
-- -----------------------------------------------
-- Singular Test: All 25 expected tickers must be present
-- in fct_stock_prices. A missing ticker means the ingestion
-- script failed silently for that stock.
-- -----------------------------------------------

with expected_tickers as (
    select column1 as ticker
    from (values
        ('AAPL'), ('MSFT'), ('NVDA'), ('GOOGL'), ('META'),
        ('JPM'),  ('BAC'),  ('GS'),   ('V'),     ('MA'),
        ('JNJ'),  ('PFE'),  ('UNH'),  ('ABBV'),  ('MRK'),
        ('XOM'),  ('CVX'),  ('COP'),  ('SLB'),   ('EOG'),
        ('AMZN'), ('TSLA'), ('HD'),   ('MCD'),   ('NKE')
    )
),

actual_tickers as (
    select distinct ticker
    from {{ ref('fct_stock_prices') }}
)

-- Returns any expected ticker that is missing from the fact table
select ticker
from expected_tickers
where ticker not in (select ticker from actual_tickers)