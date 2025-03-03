from typing import List
import requests
from prometheus_client import Summary, Counter, Histogram, Gauge

from config.logging import logger
from src.models.coin_market_cap_api import CryptoCurrencyData
from src.models.source import Source
from src.tasks.base_task import BaseTask

REQUEST_COUNT = Counter("coinmarketcap_requests_total", "Total number of requests to CoinMarketCap API")
FAILED_REQUESTS = Counter("coinmarketcap_failed_requests_total", "Total number of failed requests")
SCRAPING_DURATION = Summary("coinmarketcap_scraping_duration_seconds", "Time taken to scrape data from CoinMarketCap API")
PROCESSING_DURATION = Histogram("coinmarketcap_processing_duration_seconds", "Time taken to process and send data to Kafka")
SENT_DATA_SIZE_COUNTER = Counter("coinmarketcap_sent_data_size_bytes_total", "Total size of data sent to Kafka over time")
SCRAPED_DATA_SIZE_COUNTER = Counter("coinmarketcap_scraped_data_size_bytes_total", "Total size of data scraped over time")

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
            raw_data = response.json()["data"]["cryptoCurrencyList"]
            SCRAPED_DATA_SIZE_COUNTER.inc(len(raw_data))
            return [
                CryptoCurrencyData.from_json(crypto_currency_raw_data)
                for crypto_currency_raw_data in raw_data
            ]

    @PROCESSING_DURATION.time()
    def run_task(self) -> None:
        scraped_data = self.scrap_data()
        serialized_data = [raw_data.to_dict() for raw_data in scraped_data]

        data_size = sum(len(str(data)) for data in serialized_data)

        SENT_DATA_SIZE_COUNTER.inc(data_size)

        self.send_data_to_kafka_topic(
            source=Source.COIN_MARKET_CAP_API,
            data=serialized_data,
        )

        logger.info(f"Data forwarded successfully to Kafka topic. Size: {data_size} bytes")


