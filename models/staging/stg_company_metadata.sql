with source as (
    select 
        TICKER 
        ,COMPANY_NAME
        ,SECTOR
        ,INDUSTRY
        ,COUNTRY
        ,EXCHANGE
        ,CURRENCY
        ,MARKET_CAP
        ,FULL_TIME_EMPLOYEES
        ,WEBSITE
        ,INGESTED_AT
    from {{ source('raw', 'raw_company_metadata') }}

),
renamed_and_cast as (
    select
        -- Identifiers
        upper(trim(TICKER))                                     as TICKER,
        trim(COMPANY_NAME)                                      as COMPANY_NAME,

        -- Exchange & classification — normalise to UPPER
        initcap(trim(SECTOR))                                   as SECTOR,
        initcap(trim(INDUSTRY))                                 as INDUSTRY,
        upper(trim(EXCHANGE))                                   as EXCHANGE,

        -- Geography
        upper(trim(country))                                    as COUNTRY,

        -- Market data
        CAST(MARKET_CAP as float)                               as MARKET_CAP_USD,
        trim(CURRENCY)                                          as CURRENCY,
        FULL_TIME_EMPLOYEES                                     as FULL_TIME_EMPLOYEES,
        trim(WEBSITE)                                           as WEBSITE,       

        -- Audit
        current_timestamp()                                     as dbt_loaded_at
    from source

),
validated as (
    select TICKER, COMPANY_NAME, SECTOR, INDUSTRY, EXCHANGE, COUNTRY, MARKET_CAP_USD, CURRENCY, FULL_TIME_EMPLOYEES, WEBSITE, dbt_loaded_at
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
        select TICKER, COMPANY_NAME, SECTOR, INDUSTRY, EXCHANGE, COUNTRY, MARKET_CAP_USD, CURRENCY, FULL_TIME_EMPLOYEES, WEBSITE, dbt_loaded_at
            ,row_number() over (
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
        {{ dbt_utils.generate_surrogate_key(['ticker']) }} as company_id,
        TICKER,
        COMPANY_NAME,
        EXCHANGE,
        SECTOR,
        INDUSTRY,
        COUNTRY,
        MARKET_CAP_USD,
        -- Convenience flag
        case
            when market_cap_usd >= 200e9  then 'Mega Cap'
            when market_cap_usd >= 10e9   then 'Large Cap'
            when market_cap_usd >= 2e9    then 'Mid Cap'
            when market_cap_usd >= 300e6  then 'Small Cap'
            when market_cap_usd >  0      then 'Micro Cap'
            else 'Unknown'
        end as market_cap_category,
        FULL_TIME_EMPLOYEES, 
        WEBSITE
        dbt_loaded_at
    from deduplicated
)
select * from final