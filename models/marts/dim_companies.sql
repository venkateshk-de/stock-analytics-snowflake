{{
  config(
    materialized        = 'incremental',
    schema              = 'MARTS',
    unique_key          = 'company_id',
    incremental_strategy = 'merge',
    cluster_by          = ['sector', 'ticker'],
    tags                = ['marts', 'companies']
  )
}}

/*
  dim_companies
  -------------
  Dimension table for company reference data.
  One row per company/ticker.

  Optimizations:
    - Converted from TABLE to INCREMENTAL (merge strategy)
    - Added clustering keys on sector and ticker
    - Incremental filter: only processes new/updated company
      records based on ingested_at timestamp
    - unique_key = company_id ensures clean upsert behaviour

  Note: dim_companies is a small table (25 rows) so incremental
  provides minimal performance benefit here. However it is
  included for learning purposes and to demonstrate the pattern
  for slowly changing reference data.
*/

with

base as (

    select * from {{ ref('stg_company_metadata') }}

    -- Incremental filter — only process new/updated company records
    {% if is_incremental() %}
        where ingested_at > (
            select max(ingested_at)
            from {{ this }}
        )
    {% endif %}

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