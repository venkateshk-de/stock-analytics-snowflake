-- tests/assert_fct_dim_referential_integrity.sql
-- -----------------------------------------------
-- Singular Test: Every ticker in fct_stock_prices
-- must have a matching record in dim_companies.
-- An orphaned ticker means the metadata ingestion missed
-- a company that has price data.
-- -----------------------------------------------

select distinct
    f.ticker
from {{ ref('fct_stock_prices') }} f
left join {{ ref('dim_companies') }} d
    on f.ticker = d.ticker
where
    d.ticker is null