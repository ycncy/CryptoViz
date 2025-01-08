from sqlalchemy import Column, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CryptoCurrencyOHLCModel(Base):
    __tablename__ = "raw_ohlc_data"

    id = Column(String, primary_key=True, unique=True, nullable=False)
    name = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    record_datetime = Column(DateTime, nullable=False)

    def __repr__(self):
        return f"<CryptoCurrencyOHLCModel(name={self.name}, symbol={self.symbol}, record_datetime={self.record_datetime})>"
