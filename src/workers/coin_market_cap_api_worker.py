import os

from config.logging import logger
from src.database.postgres import PostgresDB
from src.database.models.crypto_currency_history import CryptoCurrencyRealtimeHistory
from src.workers.kafka_worker import KafkaWorker


class CoinMarketCapAPIWorker(KafkaWorker):
    def __init__(self):
        super().__init__(topic_name="raw_data.coin_market_cap.api")
        self.postgres_db = PostgresDB(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
            dbname=os.environ.get("POSTGRES_DB", "postgres"),
        )

    def process_data(self, raw_data):
        logger.info("Processing data from raw_data.coin_market_cap.api")

        rows_to_insert = [
            {
                "timestamp": raw_data["timestamp"],
                "currency": data["symbol"],
                "source": "COIN_MARKET_CAP_API",
                "name": data["name"],
                "circulating_supply": data["circulating_supply"],
                "total_supply": data["total_supply"],
                "max_supply": data["max_supply"],
                "price": data["quotes"][0]["price"],
                "volume_24": data["quotes"][0]["volume_24h"],
                "market_cap": data["quotes"][0]["market_cap"],
                "percent_change_1h": data["quotes"][0]["percent_change_1h"],
                "percent_change_24h": data["quotes"][0]["percent_change_24h"],
                "percent_change_7d": data["quotes"][0]["percent_change_7d"],
                "percent_change_30d": data["quotes"][0]["percent_change_30d"],
                "percent_change_60d": data["quotes"][0]["percent_change_60d"],
                "percent_change_90d": data["quotes"][0]["percent_change_90d"],
                "percent_change_1y": data["quotes"][0]["percent_change_1y"],
            }
            for data in raw_data["data"]
        ]
        try:
            self.postgres_db.base_insert(rows_to_insert, CryptoCurrencyRealtimeHistory)
            logger.info(
                f"Inserted data from raw_data.coin_market_cap.api for currency: {[coin_data['currency'] for coin_data in rows_to_insert]}"
            )
        except Exception as e:
            logger.error(
                f"Failed to insert data for currency: {[coin_data['currency'] for coin_data in rows_to_insert]}, error: {str(e)}"
            )
            self.postgres_db.session.rollback()
