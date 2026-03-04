{{
  config(
    materialized = 'view',
    tags         = ['staging', 'stock_prices']
  )
}}

/*
  stg_stock_prices
  ----------------
  Staging model for daily OHLCV stock price data sourced from Cybersyn.

  Transformations applied:
    1. Rename columns to snake_case convention
    2. Cast all numeric fields to consistent FLOAT / DATE types
    3. Filter out records with NULL ticker, date, or close price
    4. Filter out records before the configured start_date
    5. Derive a surrogate key: ticker + date
*/

with source as (
    select 
        DATE
        ,OPEN
        ,HIGH
        ,LOW
        ,CLOSE
        ,VOLUME
        ,TICKER
        ,INGESTED_AT
    from {{ source('raw', 'raw_stock_prices') }}
),
renamed_and_cast as (
    select
        -- Identifiers
        upper(trim(TICKER))                             as TICKER,
        try_cast(DATE as date)                          as PRICE_DATE,
        -- OHLCV prices — cast to FLOAT, coerce invalid strings to NULL
        try_cast(OPEN       as float)                   as OPEN_PRICE,
        try_cast(HIGH       as float)                   as HIGH_PRICE,
        try_cast(LOW        as float)                   as LOW_PRICE,
        try_cast(CLOSE      as float)                   as CLOSE_PRICE,
        -- Volume
        try_cast(VOLUME     as bigint)                  as VOLUME,
        -- Audit columns
        INGESTED_AT                                     as INGESTED_AT,
        current_timestamp()                             as dbt_loaded_at
    from source
),
validated as (
    select *
    from renamed_and_cast
    where
        -- Drop rows missing essential fields
        TICKER          is not null
        and PRICE_DATE  is not null
        and CLOSE_PRICE is not null

        -- Drop nonsensical prices
        and CLOSE_PRICE  > 0
        and OPEN_PRICE   > 0
        and HIGH_PRICE   > 0
        and LOW_PRICE    > 0

        -- High must be >= Low
        and HIGH_PRICE  >= LOW_PRICE

        -- Respect configured start date
        and PRICE_DATE  >= '{{ var("start_date") }}'::date

),
final as (
    select
        -- Surrogate key
       {{ dbt_utils.generate_surrogate_key(['TICKER', 'PRICE_DATE']) }} as STOCK_PRICE_ID,
        TICKER,
        PRICE_DATE,
        OPEN_PRICE,
        HIGH_PRICE,
        LOW_PRICE,
        CLOSE_PRICE,
        VOLUME,
        -- Derived helpers
        CLOSE_PRICE - OPEN_PRICE                        as PRICE_CHANGE,
        --round(safe_divide(CLOSE_PRICE - OPEN_PRICE, OPEN_PRICE) * 100, 4) as pct_change_open_to_close,
        dbt_loaded_at
    from validated
)
select * from final