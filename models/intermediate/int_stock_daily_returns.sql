{{
  config(
    materialized = 'view',
    schema       = 'INTERMEDIATE',
    tags         = ['intermediate', 'returns']
  )
}}

/*
  int_stock_daily_returns
  -----------------------
  Calculates day-over-day return metrics per ticker.
  Uses LAG window function partitioned by ticker ordered by date.
*/

with

base as (

    select * from {{ ref('int_stock_prices_joined') }}

),

with_returns as (

    select
        *,

        -- Previous day close
        lag(close_price) over (
            partition by ticker
            order by price_date
        )                                                       as prev_close_price,

        -- Daily return % based on previous close
        round(
            {{ safe_divide(
                '(close_price - lag(close_price) over (partition by ticker order by price_date))',
                'lag(close_price) over (partition by ticker order by price_date)'
            ) }} * 100
        , 4)                                                    as daily_return_pct,

        -- Intraday range
        round(high_price - low_price, 4)                        as intraday_range,

        -- Intraday range as % of close
        round(
            {{ safe_divide('(high_price - low_price)', 'close_price') }} * 100
        , 4)                                                    as intraday_range_pct,

        -- Row number per ticker for identifying first trading day
        row_number() over (
            partition by ticker
            order by price_date
        )                                                       as trading_day_seq

    from base

),

final as (

    select
        stock_price_id,
        ticker,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        prev_close_price,
        volume,
        price_change,
        pct_change_open_to_close,
        daily_return_pct,
        intraday_range,
        intraday_range_pct,
        trading_day_seq,

        -- Company
        company_id,
        company_name,
        sector,
        industry,
        country,
        exchange,
        currency,
        market_cap_usd,
        market_cap_category,
        full_time_employees,

        -- Audit
        ingested_at,
        dbt_updated_at

    from with_returns

)

select * from final