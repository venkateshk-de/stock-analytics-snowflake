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

with

source as (

    select * from {{ source('cybersyn', 'stock_price_timeseries') }}

),

renamed_and_cast as (

    select
        -- Identifiers
        upper(trim(ticker))                             as ticker,
        try_cast(date as date)                          as price_date,

        -- OHLCV prices — cast to FLOAT, coerce invalid strings to NULL
        try_cast(open       as float)                   as open_price,
        try_cast(high       as float)                   as high_price,
        try_cast(low        as float)                   as low_price,
        try_cast(close      as float)                   as close_price,
        try_cast(adj_close  as float)                   as adj_close_price,

        -- Volume
        try_cast(volume     as bigint)                  as volume,

        -- Audit columns
        current_timestamp()                             as dbt_loaded_at

    from source

),

validated as (

    select *
    from renamed_and_cast
    where
        -- Drop rows missing essential fields
        ticker          is not null
        and price_date  is not null
        and close_price is not null

        -- Drop nonsensical prices
        and close_price  > 0
        and open_price   > 0
        and high_price   > 0
        and low_price    > 0

        -- High must be >= Low
        and high_price  >= low_price

        -- Respect configured start date
        and price_date  >= '{{ var("start_date") }}'::date

),

final as (

    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['ticker', 'price_date']) }}
                                                        as stock_price_id,
        ticker,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        adj_close_price,
        volume,

        -- Derived helpers
        close_price - open_price                        as price_change,
        round(
            safe_divide(close_price - open_price, open_price) * 100
        , 4)                                            as pct_change_open_to_close,

        dbt_loaded_at

    from validated

)

select * from final