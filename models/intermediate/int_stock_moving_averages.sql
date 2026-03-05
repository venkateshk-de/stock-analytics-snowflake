{{
  config(
    materialized = 'view',
    schema       = 'INTERMEDIATE',
    tags         = ['intermediate', 'moving_averages']
  )
}}

/*
  int_stock_moving_averages
  -------------------------
  Adds 7, 30, and 90-day simple moving averages (SMA) and
  volume moving averages on top of daily returns data.

  Note: Early rows will have partial windows — this is expected
  behaviour for SMA calculations at the start of a ticker's history.
*/

with

base as (

    select * from {{ ref('int_stock_daily_returns') }}

),

with_moving_averages as (

    select
        *,

        -- Price moving averages (close)
        round(avg(close_price) over (
            partition by ticker
            order by price_date
            rows between 6 preceding and current row
        ), 4)                                           as sma_7d,

        round(avg(close_price) over (
            partition by ticker
            order by price_date
            rows between 29 preceding and current row
        ), 4)                                           as sma_30d,

        round(avg(close_price) over (
            partition by ticker
            order by price_date
            rows between 89 preceding and current row
        ), 4)                                           as sma_90d,

        -- Volume moving averages
        round(avg(volume) over (
            partition by ticker
            order by price_date
            rows between 6 preceding and current row
        ), 0)                                           as vol_ma_7d,

        round(avg(volume) over (
            partition by ticker
            order by price_date
            rows between 29 preceding and current row
        ), 0)                                           as vol_ma_30d,

        -- Cumulative average return per ticker
        round(avg(daily_return_pct) over (
            partition by ticker
            order by price_date
            rows between unbounded preceding and current row
        ), 4)                                           as cumulative_avg_return_pct

    from base

),

final as (

    select
        -- Keys & dates
        stock_price_id,
        ticker,
        price_date,
        trading_day_seq,

        -- Prices
        open_price,
        high_price,
        low_price,
        close_price,
        prev_close_price,
        volume,

        -- Returns
        price_change,
        pct_change_open_to_close,
        daily_return_pct,
        intraday_range,
        intraday_range_pct,
        cumulative_avg_return_pct,

        -- Moving averages
        sma_7d,
        sma_30d,
        sma_90d,
        vol_ma_7d,
        vol_ma_30d,

        -- Signals derived from MAs
        case
            when close_price > sma_30d then 'Above 30d MA'
            when close_price < sma_30d then 'Below 30d MA'
            else 'At 30d MA'
        end                                             as price_vs_sma_30d,

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

    from with_moving_averages

)

select * from final