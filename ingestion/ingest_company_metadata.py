import yfinance as yf
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

TICKERS = [
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META',
    'JPM', 'BAC', 'GS', 'V', 'MA',
    'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK',
    'XOM', 'CVX', 'COP', 'SLB', 'EOG',
    'AMZN', 'TSLA', 'HD', 'MCD', 'NKE'
]

# ─────────────────────────────────────────
# STEP 1: PULL METADATA FROM YAHOO FINANCE
# ─────────────────────────────────────────
def fetch_company_metadata(tickers):
    print("Fetching company metadata...")
    records = []

    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            records.append({
                'TICKER'            : ticker,
                'COMPANY_NAME'      : info.get('longName'),
                'SECTOR'            : info.get('sector'),
                'INDUSTRY'          : info.get('industry'),
                'COUNTRY'           : info.get('country'),
                'EXCHANGE'          : info.get('exchange'),
                'CURRENCY'          : info.get('currency'),
                'MARKET_CAP'        : info.get('marketCap'),
                'FULL_TIME_EMPLOYEES': info.get('fullTimeEmployees'),
                'WEBSITE'           : info.get('website'),
                'INGESTED_AT'       : datetime.utcnow()
            })
            print(f"{ticker}: {info.get('longName')}")
        except Exception as e:
            print(f"{ticker}: Failed — {e}")

    return pd.DataFrame(records)


# ─────────────────────────────────────────
# STEP 2: CREATE RAW TABLE
# ─────────────────────────────────────────
def create_raw_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS STOCK_ANALYTICS.RAW.RAW_COMPANY_METADATA (
            TICKER                  VARCHAR(20),
            COMPANY_NAME            VARCHAR(255),
            SECTOR                  VARCHAR(100),
            INDUSTRY                VARCHAR(100),
            COUNTRY                 VARCHAR(100),
            EXCHANGE                VARCHAR(50),
            CURRENCY                VARCHAR(10),
            MARKET_CAP              BIGINT,
            FULL_TIME_EMPLOYEES     BIGINT,
            WEBSITE                 VARCHAR(255),
            INGESTED_AT             TIMESTAMP_NTZ
        )
    """)
    print("Metadata table ready")


# ─────────────────────────────────────────
# STEP 3: LOAD TO SNOWFLAKE
# ─────────────────────────────────────────
def load_to_snowflake(cursor, df):
    cursor.execute("CREATE STAGE IF NOT EXISTS STOCK_ANALYTICS.RAW.STOCK_STAGE")

    csv_path = os.path.abspath("ingestion/company_metadata_raw.csv")
    df.to_csv(csv_path, index=False)

    cursor.execute(f"PUT file://{csv_path} @STOCK_ANALYTICS.RAW.STOCK_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cursor.execute("TRUNCATE TABLE IF EXISTS STOCK_ANALYTICS.RAW.RAW_COMPANY_METADATA")
    cursor.execute("""
        COPY INTO STOCK_ANALYTICS.RAW.RAW_COMPANY_METADATA
        FROM @STOCK_ANALYTICS.RAW.STOCK_STAGE/company_metadata_raw.csv.gz
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'null', '')
        )
        ON_ERROR = 'CONTINUE'
    """)
    print("Metadata loaded into Snowflake")


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    df = fetch_company_metadata(TICKERS)

    conn = snowflake.connector.connect(
        account   = os.getenv('SNOWFLAKE_ACCOUNT'),
        user      = os.getenv('SNOWFLAKE_USER'),
        password  = os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
        database  = os.getenv('SNOWFLAKE_DATABASE'),
        schema    = os.getenv('SNOWFLAKE_SCHEMA'),
        role      = os.getenv('SNOWFLAKE_ROLE')
    )
    cursor = conn.cursor()

    try:
        create_raw_table(cursor)
        load_to_snowflake(cursor, df)
        print("\n Metadata ingestion complete!")
    finally:
        cursor.close()
        conn.close()