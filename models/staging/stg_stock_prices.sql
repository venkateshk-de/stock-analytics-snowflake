{{
  config(
    materialized = 'view',
    schema       = 'STAGING',
    tags         = ['staging', 'stock_prices']
  )
}}

/*
  stg_stock_prices
  ----------------
  Cleans and standardises raw daily OHLCV stock price data.

  Transformations:
    - Rename columns to snake_case
    - Cast DATE → DATE, prices → FLOAT, volume → BIGINT
    - Uppercase and trim ticker
    - Filter nulls and invalid prices
    - Derive price_change and pct_change_open_to_close
*/

with

source as (

    select * from {{ source('raw', 'raw_stock_prices') }}

),

renamed as (

    select
        upper(trim(ticker))             as ticker,
        try_cast(date   as date)        as price_date,
        try_cast(open   as float)       as open_price,
        try_cast(high   as float)       as high_price,
        try_cast(low    as float)       as low_price,
        try_cast(close  as float)       as close_price,
        try_cast(volume as bigint)      as volume,
        ingested_at                     as ingested_at

    from source

),

validated as (

    select *
    from renamed
    where
        ticker          is not null
        and price_date  is not null
        and close_price is not null
        and open_price  is not null
        and high_price  is not null
        and low_price   is not null
        and close_price > 0
        and open_price  > 0
        and high_price  > 0
        and low_price   > 0
        and high_price  >= low_price

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['ticker', 'price_date']) }}
                                                        as stock_price_id,
        ticker,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,

        -- Derived
        round(close_price - open_price, 4)              as price_change,
        round(
            {{ safe_divide('(close_price - open_price)', 'open_price') }}
            * 100
        , 4)                                            as pct_change_open_to_close,

        ingested_at,
        current_timestamp()                             as dbt_updated_at

    from validated

)

select * from final