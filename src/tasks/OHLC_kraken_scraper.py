import os
from typing import List
import requests

from config.logging import logger
from src.database.models.crypto_currency_history import CryptoCurrencyRealtimeHistory
from src.database.postgres import PostgresDB
from src.kafka.producer import Producer
from src.models.kraken_api import CryptoCurrencyOHLC
from src.models.source import Source
from src.tasks.base_task import BaseTask


class OHLCKrakenScraper(BaseTask):
    limit: int = 25

    def __init__(self, source_url: str, kafka_producer: Producer, kafka_topic: str):
        super().__init__(source_url=source_url, kafka_producer=kafka_producer, kafka_topic=kafka_topic)
        self.postgres_db = PostgresDB(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
            dbname=os.environ.get("POSTGRES_DB", "postgres"),
        )

    def scrap_data(
            self,
    ) -> List[CryptoCurrencyOHLC]:
        logger.info("Scraping coinmarketcap data")
        with self.postgres_db.session as session:
            cryptocurrencies = session.query(
                CryptoCurrencyRealtimeHistory.name,
                CryptoCurrencyRealtimeHistory.currency,
            ).distinct(CryptoCurrencyRealtimeHistory.currency).all()

            cryptocurrencies_data = []

            for cryptocurrency in cryptocurrencies:
                params = {
                    "pair": f"{cryptocurrency.currency}USD"
                }
                response = requests.get(url=self.source_url, params=params)
                if response.status_code != 200:
                    raise Exception("Erreur")
                else:
                    raw_data = response.json()["result"]
                    if raw_data:
                        cryptocurrencies_data.append(
                            CryptoCurrencyOHLC.from_json(raw_data, name=cryptocurrency.name, symbol=cryptocurrency.currency)
                        )

            return cryptocurrencies_data

    def run_task(
            self,
    ) -> None:
        scraped_data = self.scrap_data()

        self.send_data_to_kafka_topic(
            source=Source.KRAKEN_API,
            data=[raw_data.to_dict() for raw_data in scraped_data],
        )

        logger.info("Data forwarded successfully to Kafka topic")
