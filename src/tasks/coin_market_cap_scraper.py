import time
from typing import List
import requests
from prometheus_client import Summary, Counter, Histogram

from config.logging import logger
from src.models.coin_market_cap_api import CryptoCurrencyData
from src.models.source import Source
from src.tasks.base_task import BaseTask

REQUEST_COUNT = Counter("coinmarketcap_requests_total", "Total number of requests to CoinMarketCap API")
SUCCESSFUL_REQUESTS = Counter("coinmarketcap_successful_requests_total", "Total number of successful requests")
FAILED_REQUESTS = Counter("coinmarketcap_failed_requests_total", "Total number of failed requests")
SCRAPING_DURATION = Summary("coinmarketcap_scraping_duration_seconds", "Time taken to scrape data from CoinMarketCap API")
PROCESSING_DURATION = Summary("coinmarketcap_processing_duration_seconds", "Time taken to process and send data to Kafka")
KAFKA_MESSAGES_SENT = Counter("kafka_messages_sent_total", "Total number of messages sent to Kafka")
SCRAPED_ITEMS_COUNT = Histogram("coinmarketcap_scraped_items_count", "Number of items scraped per task")

class CoinMarketCapScraper(BaseTask):
    limit: int = 25

    @SCRAPING_DURATION.time()
    def scrap_data(
        self,
    ) -> List[CryptoCurrencyData]:
        logger.info("Scraping coinmarketcap data")
        REQUEST_COUNT.inc()
        params = {"limit": self.limit, "start": 1}
        route = f"{self.source_url}/data-api/v3/cryptocurrency/listing"
        response = requests.get(url=route, params=params)
        if response.status_code != 200:
            FAILED_REQUESTS.inc()
            raise Exception("Erreur")
        else:
            SUCCESSFUL_REQUESTS.inc()
            raw_data = response.json()["data"]["cryptoCurrencyList"]
            SCRAPED_ITEMS_COUNT.observe(len(raw_data))
            return [
                CryptoCurrencyData.from_json(crypto_currency_raw_data)
                for crypto_currency_raw_data in raw_data
            ]

    @PROCESSING_DURATION.time()
    def run_task(
        self,
    ) -> None:
        scraped_data = self.scrap_data()

        start_time = time.time()

        self.send_data_to_kafka_topic(
            source=Source.COIN_MARKET_CAP_API,
            data=[raw_data.to_dict() for raw_data in scraped_data],
        )

        end_time = time.time()

        KAFKA_MESSAGES_SENT.inc(len(scraped_data))

        logger.info("Data forwarded successfully to Kafka topic")
