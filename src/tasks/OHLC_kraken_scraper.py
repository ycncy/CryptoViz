import os
from typing import List
import requests
from prometheus_client import Summary, Counter, Histogram, Gauge

from config.logging import logger
from src.database.models.crypto_currency_history import CryptoCurrencyRealtimeHistory
from src.database.postgres import PostgresDB
from src.kafka.producer import Producer
from src.models.kraken_api import CryptoCurrencyOHLC
from src.models.source import Source
from src.tasks.base_task import BaseTask

REQUEST_COUNT = Counter("kraken_requests_total", "Total number of requests to Kraken API")
FAILED_REQUESTS = Counter("kraken_failed_requests_total", "Total number of failed requests to Kraken API")
SCRAPING_DURATION = Summary("kraken_scraping_duration_seconds", "Time taken to scrape data from Kraken API")
PROCESSING_DURATION = Histogram("kraken_processing_duration_seconds", "Time taken to process and send data to Kafka")
SENT_DATA_SIZE_COUNTER = Counter("kraken_sent_data_size_bytes", "Size of data sent to Kafka during the last execution")
SCRAPED_DATA_SIZE_COUNTER = Counter("kraken_scraped_items_count", "Number of items scraped during the last execution")

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

    @SCRAPING_DURATION.time()
    def scrap_data(self) -> List[CryptoCurrencyOHLC]:
        logger.info("Scraping Kraken data")
        REQUEST_COUNT.inc()
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
                    FAILED_REQUESTS.inc()
                    logger.error(f"Failed to fetch data for {cryptocurrency.currency}")
                    continue
                else:
                    raw_data = response.json()
                    if "result" in raw_data:
                        cryptocurrencies_data.append(
                            CryptoCurrencyOHLC.from_json(raw_data["result"], name=cryptocurrency.name, symbol=cryptocurrency.currency)
                        )

            SCRAPED_DATA_SIZE_COUNTER.inc(len(cryptocurrencies_data))
            return cryptocurrencies_data

    @PROCESSING_DURATION.time()
    def run_task(self) -> None:
        scraped_data = self.scrap_data()
        serialized_data = [raw_data.to_dict() for raw_data in scraped_data]

        data_size = sum(len(str(data)) for data in serialized_data)
        SENT_DATA_SIZE_COUNTER.inc(data_size)

        self.send_data_to_kafka_topic(
            source=Source.KRAKEN_API,
            data=serialized_data,
        )

        logger.info(f"Data forwarded successfully to Kafka topic. Size: {data_size} bytes")
