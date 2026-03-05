{{
  config(
    materialized = 'view',
    schema       = 'INTERMEDIATE',
    tags         = ['intermediate', 'stock_prices']
  )
}}

/*
  int_stock_prices_joined
  -----------------------
  Joins cleaned stock prices with company metadata.
  This is the foundation for all downstream calculations.
*/

with

stock_prices as (

    select * from {{ ref('stg_stock_prices') }}

),

company_metadata as (

    select * from {{ ref('stg_company_metadata') }}

),

final as (

    select
        -- Keys
        sp.stock_price_id,
        sp.ticker,
        sp.price_date,

        -- Price data
        sp.open_price,
        sp.high_price,
        sp.low_price,
        sp.close_price,
        sp.volume,
        sp.price_change,
        sp.pct_change_open_to_close,

        -- Company attributes
        cm.company_id,
        cm.company_name,
        cm.sector,
        cm.industry,
        cm.country,
        cm.exchange,
        cm.currency,
        cm.market_cap_usd,
        cm.market_cap_category,
        cm.full_time_employees,

        -- Audit
        sp.ingested_at,
        sp.dbt_updated_at

    from stock_prices sp
    left join company_metadata cm
        on sp.ticker = cm.ticker

)

select * from final