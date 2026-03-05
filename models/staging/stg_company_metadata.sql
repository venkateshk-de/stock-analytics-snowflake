{{
  config(
    materialized = 'view',
    schema       = 'STAGING',
    tags         = ['staging', 'company_metadata']
  )
}}

/*
  stg_company_metadata
  --------------------
  Cleans and standardises raw company reference data.

  Transformations:
    - Rename columns to snake_case
    - Normalise casing (ticker/exchange/country → UPPER, sector/industry → INITCAP)
    - Cast market_cap → FLOAT, full_time_employees → INTEGER
    - Nullify empty strings
    - Deduplicate on ticker (keep highest market_cap)
    - Derive market_cap_category
*/

with

source as (

    select * from {{ source('raw', 'raw_company_metadata') }}

),

renamed as (

    select
        upper(trim(ticker))                             as ticker,
        trim(company_name)                              as company_name,
        initcap(trim(sector))                           as sector,
        initcap(trim(industry))                         as industry,
        upper(trim(country))                            as country,
        upper(trim(exchange))                           as exchange,
        upper(trim(currency))                           as currency,
        market_cap::float                               as market_cap_usd,
        full_time_employees::integer                    as full_time_employees,
        nullif(trim(website), '')                       as website,
        ingested_at                                     as ingested_at

    from source

),

validated as (

    select *
    from renamed
    where
        ticker          is not null
        and trim(ticker) != ''
        and company_name is not null

),

-- Keep one row per ticker — highest market cap wins
deduplicated as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by ticker
                order by market_cap_usd desc nulls last, ingested_at desc
            ) as rn
        from validated
    )
    where rn = 1

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['ticker']) }}  as company_id,
        ticker,
        company_name,
        sector,
        industry,
        country,
        exchange,
        currency,
        market_cap_usd,

        case
            when market_cap_usd >= 200e9  then 'Mega Cap'
            when market_cap_usd >= 10e9   then 'Large Cap'
            when market_cap_usd >= 2e9    then 'Mid Cap'
            when market_cap_usd >= 300e6  then 'Small Cap'
            when market_cap_usd >  0      then 'Micro Cap'
            else                               'Unknown'
        end                                             as market_cap_category,

        full_time_employees,
        website,
        ingested_at,
        current_timestamp()                             as dbt_updated_at

    from deduplicated

)

select * from final