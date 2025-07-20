from datetime import datetime
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime,Float ,text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError


# Load env vars
load_dotenv()

# --- PostgreSQL Connection Setup ---
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# --- SQLAlchemy Model ---
class StockDetails(Base):
    __tablename__ = 'stock_details'
    id = Column(Integer, primary_key=True, index=True)
    stock_name = Column(String, nullable=False)
    token = Column(String, unique=True, nullable=False)
    ltp = Column(Float, nullable=False)
    last_update = Column(DateTime, nullable=False)

class BankNiftyOhlcData(Base):
    __tablename__ = 'bank_nifty_ohlc_data'
    
    id = Column(Integer, primary_key=True, index=True)
    token = Column(String(50), nullable=False)
    start_time = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    interval = Column(String(10), nullable=True)
    created_at = Column(DateTime, server_default=text('CURRENT_TIMESTAMP'), nullable=True)

def insert_ohlc_data(token, start_time, open_, high, low, close, interval=None):
    session = SessionLocal()
    try:
        ohlc_entry = BankNiftyOhlcData(
            token=token,
            start_time=start_time,
            open=float(open_),
            high=float(high),
            low=float(low),
            close=float(close),
            interval=interval
        )
        session.add(ohlc_entry)
        session.commit()
    except SQLAlchemyError as e:
        print(f"Insert OHLC Error: {e}")
        session.rollback()
    finally:
        session.close()



def insert_data(token, stock_name, ltp):
    session = SessionLocal()
    try:
        stock = session.query(StockDetails).filter_by(token=token).first()
        now = datetime.now()
        if stock:
            stock.ltp = float(ltp)
            stock.stock_name = stock_name
            stock.last_update = now
        else:
            stock = StockDetails(
                token=token,
                stock_name=stock_name,
                ltp=float(ltp),
                last_update=now
            )
            session.add(stock)
        session.commit()
    except Exception as e:
        print(f"DB Error: {e}")
        session.rollback()
    finally:
        session.close()



