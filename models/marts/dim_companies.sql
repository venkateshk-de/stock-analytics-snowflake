{{
  config(
    materialized = 'table',
    schema       = 'MARTS',
    tags         = ['marts', 'companies']
  )
}}

/*
  dim_companies
  -------------
  Dimension table for company reference data.
  Used to slice and filter fct_stock_prices in BI tools.
*/

with

base as (

    select * from {{ ref('stg_company_metadata') }}

),

final as (

    select
        company_id,
        ticker,
        company_name,
        sector,
        industry,
        country,
        exchange,
        currency,
        market_cap_usd,
        market_cap_category,
        full_time_employees,
        website,
        ingested_at,
        dbt_updated_at

    from base

)

select * from final