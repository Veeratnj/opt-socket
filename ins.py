import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, TIMESTAMP
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Database URL from .env
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@" \
         f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

# SQLAlchemy setup
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ORM Model
class BankNiftyOHLC(Base):
    __tablename__ = 'bank_nifty_ohlc_data'

    id = Column(Integer, primary_key=True, index=True)
    token = Column(String(50), nullable=False)
    start_time = Column(TIMESTAMP, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    interval = Column(String(10))
    created_at = Column(TIMESTAMP)

def insert_ohlc_rows(session, df, token="BANKNIFTY"):
    for _, row in df.iterrows():
        ohlc = BankNiftyOHLC(
            token=token,
            start_time=row['timestamp'],
            open=row['open'],
            high=row['high'],
            low=row['low'],
            close=row['close'],
            interval=row.get('interval', '30s')
        )
        session.add(ohlc)
    session.commit()
    print(f"{len(df)} rows inserted into bank_nifty_ohlc_data.")

def main(csv_file):
    # Read CSV
    df = pd.read_csv(csv_file)

    # Check if 'interval' exists; if not, set default
    if 'interval' not in df.columns:
        df['interval'] = '30s'

    # Database session
    session = SessionLocal()
    try:
        insert_ohlc_rows(session, df)
    finally:
        session.close()

if __name__ == "__main__":
    # Replace with your CSV file name
    csv_file = '30sec_candles_manual.csv'
    main(csv_file)
