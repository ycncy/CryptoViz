from sqlalchemy import Column, String, Float, TIMESTAMP
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class CryptoCurrencyRealtimeHistory(Base):
    __tablename__ = "raw_crypto_data"

    currency = Column(String(10), primary_key=True)
    timestamp = Column(TIMESTAMP, primary_key=True)
    source = Column(String(50))
    name = Column(String(255))
    circulating_supply = Column(Float)
    total_supply = Column(Float)
    max_supply = Column(Float)
    price = Column(Float)
    volume_24 = Column(Float)
    market_cap = Column(Float)
    percent_change_1h = Column(Float)
    percent_change_24h = Column(Float)
    percent_change_7d = Column(Float)
    percent_change_30d = Column(Float)
    percent_change_60d = Column(Float)
    percent_change_90d = Column(Float)
    percent_change_1y = Column(Float)
