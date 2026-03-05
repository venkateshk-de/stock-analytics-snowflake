{{
    config(
        materialized='view',
        description='Rolling volatility (std dev of daily returns) over 7, 30, and 90-day windows — a core risk metric'
    )
}}

/*
    int_volatility.sql
    ──────────────────
    Measures how "risky" or "noisy" a stock is by computing the rolling
    standard deviation of daily returns.

    Why std dev = volatility?
        A stock that moves ±0.2% per day is low volatility (boring, stable).
        A stock that moves ±3% per day is high volatility (exciting, risky).
        Std dev quantifies that spread numerically.

    Annualised volatility (σ_annual):
        The industry-standard way to express volatility is annualised:
            σ_annual = σ_daily × √252
        where 252 = typical number of US trading days per year.
        This lets you compare volatility across assets (e.g., "AAPL has 28% annual vol").

    We build on int_daily_returns rather than raw prices because volatility
    is a property of *returns*, not price levels. A $500 stock and a $5 stock
    can have identical volatility if their % moves are the same.

    Sharpe ratio preview (bonus):
        sharpe = (avg_daily_return / daily_std_dev) × √252
        We compute this here so it's available for mart models downstream.
        Assumes risk-free rate ≈ 0 for simplicity.
*/

with daily_returns as (

    select * from {{ ref('int_daily_returns') }}

) select * from daily_returns

/*
,

rolling_stats as (

    select
        ticker,
        date,
        close_price,
        daily_return_pct
        --daily_adj_return_pct,

        -- ── 7-Day Rolling Volatility ─────────────────────────────────────────
        
        round(
            stddev_pop(daily_adj_return_pct) over (
                partition by ticker
                order by date
                rows between 6 preceding and current row
            ), 6
        ) as volatility_7d,

        -- ── 30-Day Rolling Volatility (most commonly cited) ──────────────────
        round(
            stddev_pop(daily_adj_return_pct) over (
                partition by ticker
                order by date
                rows between 29 preceding and current row
            ), 6
        ) as volatility_30d,

        -- ── 90-Day Rolling Volatility ────────────────────────────────────────
        round(
            stddev_pop(daily_adj_return_pct) over (
                partition by ticker
                order by date
                rows between 89 preceding and current row
            ), 6
        ) as volatility_90d,

        -- ── Rolling average return (for Sharpe ratio) ────────────────────────
        round(
            avg(daily_adj_return_pct) over (
                partition by ticker
                order by date
                rows between 29 preceding and current row
            ), 6
        ) as avg_return_30d,

        -- ── Rolling row counts (audit: how many trading days in each window) ──
        count(daily_adj_return_pct) over (
            partition by ticker
            order by date
            rows between 6 preceding and current row
        ) as window_rows_7d,

        count(daily_adj_return_pct) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ) as window_rows_30d
    
    from daily_returns
    --where daily_adj_return_pct is not null  -- exclude the first row per ticker

),

annualised as (

    select
        ticker,
        date,
        close_price,
        daily_return_pct,
       -- daily_adj_return_pct,

        -- Raw daily volatilities
        volatility_7d,
        volatility_30d,
        volatility_90d,

        -- Annualised volatility (× √252) — industry standard
        round(volatility_7d  * sqrt(252), 4) as annualised_vol_7d,
        round(volatility_30d * sqrt(252), 4) as annualised_vol_30d,
        round(volatility_90d * sqrt(252), 4) as annualised_vol_90d,

        -- 30-day Sharpe ratio (risk-free rate assumed = 0)
        -- Positive = return earned per unit of risk taken
        round(
            case
                when volatility_30d = 0 or volatility_30d is null then null
                else (avg_return_30d / volatility_30d) * sqrt(252)
            end,
            4
        ) as sharpe_ratio_30d,

        -- Volatility regime classification
        case
            when volatility_30d is null                then null
            when volatility_30d * sqrt(252) < 15       then 'LOW'         -- < 15% annual vol
            when volatility_30d * sqrt(252) between 15 and 30 then 'MEDIUM'  -- 15–30%
            when volatility_30d * sqrt(252) between 30 and 50 then 'HIGH'    -- 30–50%
            else                                            'EXTREME'    -- > 50%
        end as volatility_regime,

        window_rows_7d,
        window_rows_30d

    from rolling_stats

)

select * from annualised
order by ticker, date
*/