{{
    config(
        materialized='view',
        description='7-day, 30-day, and 90-day simple moving averages (SMA) per stock using window functions'
    )
}}

/*
    int_moving_averages.sql
    ───────────────────────
    Calculates Simple Moving Averages (SMA) across three lookback windows.

    What is an SMA?
        The average closing price over the last N trading days.
        Traders use crossovers (e.g. price crossing above the 30-day SMA)
        as buy/sell signals.

    Window function anatomy:
        avg(close_price) OVER (
            PARTITION BY ticker          ← reset per stock
            ORDER BY date                ← walk forward in time
            ROWS BETWEEN 6 PRECEDING     ← include today + 6 prior rows = 7 days
                     AND CURRENT ROW
        )

    ROWS vs RANGE:
        We use ROWS (physical row count) rather than RANGE (logical value range)
        because market data has gaps (weekends, holidays). ROWS ensures we always
        average exactly N trading sessions, not N calendar days.

    min_periods behaviour:
        For the first N-1 rows per ticker, we don't yet have a full window.
        We still calculate the average over available rows (i.e. a 3-day average
        for the 3rd row of a 7-day window). This matches pandas default behaviour.
        If you want NULL until the full window is available, wrap in a CASE WHEN
        count(*) OVER (...) = N THEN avg(...) ELSE NULL END.
*/

with source as (

    select * from {{ ref('stg_stock_prices') }}

),

moving_averages as (

    select
        ticker,
        PRICE_DATE as date,
        close_price,
        --adj_close_price,
        volume,

        -- ── 7-Day SMA ───────────────────────────────────────────────────────
        round(
            avg(close_price) over (
                partition by ticker
                order by date
                rows between 6 preceding and current row
            ), 4
        ) as sma_7d,

        -- ── 30-Day SMA ──────────────────────────────────────────────────────
        round(
            avg(close_price) over (
                partition by ticker
                order by date
                rows between 29 preceding and current row
            ), 4
        ) as sma_30d,

        -- ── 90-Day SMA ──────────────────────────────────────────────────────
        round(
            avg(close_price) over (
                partition by ticker
                order by date
                rows between 89 preceding and current row
            ), 4
        ) as sma_90d,

        -- ── Adjusted-close SMAs (preferred for backtesting) ─────────────────
        /*
        round(
            avg(adj_close_price) over (
                partition by ticker
                order by date
                rows between 6 preceding and current row
            ), 4
        ) as sma_adj_7d,
        

        round(
            avg(adj_close_price) over (
                partition by ticker
                order by date
                rows between 29 preceding and current row
            ), 4
        ) as sma_adj_30d,

        round(
            avg(adj_close_price) over (
                partition by ticker
                order by date
                rows between 89 preceding and current row
            ), 4
        ) as sma_adj_90d,
        */
        -- ── Volume SMA (useful for spotting volume spikes) ───────────────────
        round(
            avg(volume) over (
                partition by ticker
                order by date
                rows between 29 preceding and current row
            ), 0
        ) as volume_sma_30d,

        -- ── Row count within window (for min_periods auditing) ───────────────
        count(close_price) over (
            partition by ticker
            order by date
            rows between 89 preceding and current row
        ) as rolling_90d_row_count

    from source

),

signals as (

    select
        *,

        -- Golden/Death cross signal: short SMA crossing long SMA
        case
            when sma_7d > sma_30d  then 'BULLISH'   -- short above long
            when sma_7d < sma_30d  then 'BEARISH'   -- short below long
            else 'NEUTRAL'
        end as sma_7_30_signal,

        -- Price vs 90-day trend
        case
            when close_price > sma_90d then 'ABOVE_TREND'
            when close_price < sma_90d then 'BELOW_TREND'
            else 'AT_TREND'
        end as price_vs_90d_trend,

        -- Volume spike: today's volume > 2x the 30-day average
        case
            when volume > volume_sma_30d * 2 then true
            else false
        end as is_volume_spike

    from moving_averages

)

select * from signals
order by ticker, date