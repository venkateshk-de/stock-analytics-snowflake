{{
  config(
    materialized = 'table',
    schema       = 'MARTS',
    tags         = ['marts', 'stock_prices']
  )
}}

/*
  fct_stock_prices
  ----------------
  Final fact table combining all stock price data with returns,
  moving averages, and company attributes.

  This is the primary table for BI tools and analysis.
  Materialised as TABLE for query performance.
*/

with

base as (

    select * from {{ ref('int_stock_moving_averages') }}

),

final as (

    select
        -- Surrogate key
        stock_price_id,

        -- Dimensions
        ticker,
        company_name,
        sector,
        industry,
        country,
        exchange,
        currency,
        market_cap_category,

        -- Date
        price_date,
        date_trunc('week',  price_date)::date           as price_week,
        date_trunc('month', price_date)::date           as price_month,
        date_trunc('year',  price_date)::date           as price_year,
        dayofweek(price_date)                           as day_of_week,
        trading_day_seq,

        -- Prices
        open_price,
        high_price,
        low_price,
        close_price,
        prev_close_price,
        volume,

        -- Returns
        price_change,
        pct_change_open_to_close,
        daily_return_pct,
        intraday_range,
        intraday_range_pct,
        cumulative_avg_return_pct,

        -- Moving averages
        sma_7d,
        sma_30d,
        sma_90d,
        vol_ma_7d,
        vol_ma_30d,
        price_vs_sma_30d,

        -- Company size
        market_cap_usd,
        full_time_employees,

        -- Audit
        ingested_at,
        dbt_updated_at

    from base

)

select * from final