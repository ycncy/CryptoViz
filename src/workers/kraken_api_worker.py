import os

from config.logging import logger
from src.database.models.ohlc_history import CryptoCurrencyOHLCModel
from src.database.postgres import PostgresDB
from src.workers.kafka_worker import KafkaWorker


class KrakenApiWorker(KafkaWorker):
    def __init__(self):
        super().__init__(topic_name="raw_data.ohlc.kraken.api")
        self.postgres_db = PostgresDB(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
            dbname=os.environ.get("POSTGRES_DB", "postgres"),
        )

    def process_data(self, raw_data):
        logger.info("Processing data from processed_data.crypto_currency.ohlc")

        rows_to_insert = [
            {
                "record_datetime": data["record_datetime"],
                "symbol": data["symbol"],
                "name": data["name"],
                "open": data["open"],
                "close": data["close"],
                "high": data["high"],
                "low": data["low"]
            }
            for data in raw_data["data"]
        ]

        try:
            self.postgres_db.base_insert(rows_to_insert, CryptoCurrencyOHLCModel)
            logger.info(
                f"Inserted OHLC data for currencies: {[row['symbol'] for row in rows_to_insert]}"
            )
        except Exception as e:
            logger.error(
                f"Failed to insert OHLC data for currencies: {[row['symbol'] for row in rows_to_insert]}, error: {str(e)}"
            )
            self.postgres_db.session.rollback()
