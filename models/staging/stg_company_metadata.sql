{{
  config(
    materialized = 'view',
    tags         = ['staging', 'company_metadata']
  )
}}

/*
  stg_company_metadata
  --------------------
  Staging model for company / security reference data from Cybersyn.

  Transformations applied:
    1. Rename columns to snake_case convention
    2. Standardise string casing (exchange, sector, industry → UPPER/INITCAP)
    3. Cast market_cap to FLOAT
    4. Filter out records with NULL ticker or company_name
    5. Deduplicate on ticker (keep latest record if dupes exist)
*/

with

source as (

    select * from {{ source('cybersyn', 'company_metadata') }}

),

renamed_and_cast as (

    select
        -- Identifiers
        upper(trim(ticker))                                     as ticker,
        trim(company_name)                                      as company_name,

        -- Exchange & classification — normalise to UPPER
        upper(trim(exchange))                                   as exchange,
        initcap(trim(sector))                                   as sector,
        initcap(trim(industry))                                 as industry,

        -- Geography
        upper(trim(country))                                    as country,

        -- Market data
        try_cast(market_cap as float)                           as market_cap_usd,

        -- External identifiers — keep as strings
        nullif(trim(cik),  '')                                  as sec_cik,
        nullif(trim(isin), '')                                  as isin,

        -- Audit
        current_timestamp()                                     as dbt_loaded_at

    from source

),

validated as (

    select *
    from renamed_and_cast
    where
        ticker          is not null
        and company_name is not null
        and trim(ticker) != ''

),

-- Deduplicate: if a ticker appears more than once, keep the row with the
-- highest market_cap (most complete / recent snapshot)
deduplicated as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by ticker
                order by market_cap_usd desc nulls last, dbt_loaded_at desc
            ) as rn
        from validated
    )
    where rn = 1

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['ticker']) }}      as company_id,

        ticker,
        company_name,
        exchange,
        sector,
        industry,
        country,
        market_cap_usd,

        -- Convenience flag
        case
            when market_cap_usd >= 200e9  then 'Mega Cap'
            when market_cap_usd >= 10e9   then 'Large Cap'
            when market_cap_usd >= 2e9    then 'Mid Cap'
            when market_cap_usd >= 300e6  then 'Small Cap'
            when market_cap_usd >  0      then 'Micro Cap'
            else 'Unknown'
        end                                                     as market_cap_category,

        sec_cik,
        isin,
        dbt_loaded_at

    from deduplicated

)

select * from final