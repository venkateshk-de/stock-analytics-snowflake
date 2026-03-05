import yfinance as yf
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
TICKERS = [
    # Technology
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META',
    # Finance
    'JPM', 'BAC', 'GS', 'V', 'MA',
    # Healthcare
    'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK',
    # Energy
    'XOM', 'CVX', 'COP', 'SLB', 'EOG',
    # Consumer
    'AMZN', 'TSLA', 'HD', 'MCD', 'NKE'
]

START_DATE = '2019-01-01'
END_DATE   = datetime.today().strftime('%Y-%m-%d')

# ─────────────────────────────────────────
# STEP 1: PULL DATA FROM YAHOO FINANCE
# ─────────────────────────────────────────
def fetch_stock_prices(tickers, start, end):
    print(f"Fetching stock prices for {len(tickers)} tickers...")
    all_data = []

    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start, end=end, auto_adjust=True, progress=False)
            
            # Fix for newer yfinance versions — flatten MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]
            
            # Now safely lowercase
            df.columns = [col.lower() for col in df.columns]
            
            df['ticker']      = ticker
            df['ingested_at'] = datetime.utcnow()
            df = df.reset_index()
            df.rename(columns={'date': 'date', 'Date': 'date'}, inplace=True)
            all_data.append(df)
            print(f"{ticker}: {len(df)} rows")
        except Exception as e:
            print(f"{ticker}: Failed — {e}")

    if not all_data:
        raise ValueError("No data fetched for any ticker. Check your internet connection or ticker symbols.")

    combined = pd.concat(all_data, ignore_index=True)
    combined.columns = [col.upper() for col in combined.columns]
    print(f"\nTotal rows fetched: {len(combined)}")
    return combined


# ─────────────────────────────────────────
# STEP 2: SAVE TO CSV
# ─────────────────────────────────────────
def save_to_csv(df, filepath):
    df.to_csv(filepath, index=False)
    print(f"Saved to {filepath}")


# ─────────────────────────────────────────
# STEP 3: CONNECT TO SNOWFLAKE
# ─────────────────────────────────────────
def get_snowflake_connection():
    return snowflake.connector.connect(
        account   = os.getenv('SNOWFLAKE_ACCOUNT'),
        user      = os.getenv('SNOWFLAKE_USER'),
        password  = os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
        database  = os.getenv('SNOWFLAKE_DATABASE'),
        schema    = os.getenv('SNOWFLAKE_SCHEMA'),
        role      = os.getenv('SNOWFLAKE_ROLE')
    )


# ─────────────────────────────────────────
# STEP 4: CREATE RAW TABLE IN SNOWFLAKE
# ─────────────────────────────────────────
def create_raw_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS STOCK_ANALYTICS.RAW.RAW_STOCK_PRICES (
            DATE            DATE,
            OPEN            FLOAT,
            HIGH            FLOAT,
            LOW             FLOAT,
            CLOSE           FLOAT,
            VOLUME          BIGINT,
            TICKER          VARCHAR(20),
            INGESTED_AT     TIMESTAMP_NTZ
        )
    """)
    print("Raw table ready")


# ─────────────────────────────────────────
# STEP 5: STAGE & LOAD INTO SNOWFLAKE
# ─────────────────────────────────────────
def load_to_snowflake(cursor, filepath):
    # Create an internal stage
    cursor.execute("CREATE STAGE IF NOT EXISTS STOCK_ANALYTICS.RAW.STOCK_STAGE")

    # Upload CSV to stage
    cursor.execute(f"PUT file://{filepath} @STOCK_ANALYTICS.RAW.STOCK_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    print("File uploaded to Snowflake stage")

    # Truncate and reload (full refresh for now)
    cursor.execute("TRUNCATE TABLE IF EXISTS STOCK_ANALYTICS.RAW.RAW_STOCK_PRICES")

    # COPY INTO raw table
    cursor.execute("""
        COPY INTO STOCK_ANALYTICS.RAW.RAW_STOCK_PRICES
        FROM @STOCK_ANALYTICS.RAW.STOCK_STAGE
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'null', '')
        )
        ON_ERROR = 'CONTINUE'
    """)
    print("Data loaded into RAW_STOCK_PRICES")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    # 1. Fetch
    df = fetch_stock_prices(TICKERS, START_DATE, END_DATE)

    # 2. Save locally
    csv_path = os.path.abspath("ingestion/stock_prices_raw.csv")
    save_to_csv(df, csv_path)

    # 3. Load to Snowflake
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        create_raw_table(cursor)
        load_to_snowflake(cursor, csv_path)
        print("\n Ingestion complete!")
    finally:
        cursor.close()
        conn.close()