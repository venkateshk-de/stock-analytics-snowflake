{{
    config(
        materialized='view',
        description='Daily percentage return per stock, calculated using LAG() window function'
    )
}}

/*
    int_daily_returns.sql
    ─────────────────────
    Calculates the day-over-day percentage return for each stock.

    Formula:
        daily_return % = ((close_price - previous_close) / previous_close) * 100

    Window concept:
        LAG(close_price, 1) OVER (PARTITION BY ticker ORDER BY date)
        ↑ "For each ticker, look back 1 row by date to get yesterday's close"

    Why PARTITION BY ticker?
        Without it, LAG() would bleed across tickers — AAPL's Monday close
        would be used as GOOG's Friday previous price. Partitioning resets
        the window per ticker.

    NULL handling:
        The very first trading day per ticker has no previous close,
        so LAG() returns NULL → daily_return_pct will be NULL for that row.
        This is correct behaviour; we don't fabricate a baseline return.
*/

with source as (

    select * from {{ ref('stg_stock_prices') }}

),

lagged as (

    select
        ticker,
        PRICE_DATE as date,
        open_price,
        high_price,
        low_price,
        close_price,
        --adj_close_price,
        volume,

        -- LAG: pull the previous trading day's close for this ticker
        lag(close_price, 1) over (
            partition by ticker
            order by date
        ) as prev_close_price,

        -- LAG on adjusted close for split/dividend-adjusted returns
        /*
        lag(adj_close_price, 1) over (
            partition by ticker
            order by date
        ) as prev_adj_close_price
        */
    from source

),

returns as (

    select
        ticker,
        date,
        close_price,
        --adj_close_price,
        prev_close_price,
        --prev_adj_close_price,
        volume,

        -- Raw return: based on unadjusted close
        round(
            ((close_price - prev_close_price) / nullif(prev_close_price, 0)) * 100,
            4
        ) as daily_return_pct,

        -- Adjusted return: accounts for stock splits & dividends (preferred for analysis)
        /*
        round(
            ((adj_close_price - prev_adj_close_price) / nullif(prev_adj_close_price, 0)) * 100,
            4
        ) as daily_adj_return_pct,
        */
        -- Absolute dollar change
        round(close_price - prev_close_price, 4) as daily_price_change,

        -- Direction flag — useful for win-rate calculations downstream
        case
            when close_price > prev_close_price then 'UP'
            when close_price < prev_close_price then 'DOWN'
            when close_price = prev_close_price then 'FLAT'
            else null  -- first trading day, no prior close
        end as return_direction

    from lagged

)

select * from returns
order by ticker, date