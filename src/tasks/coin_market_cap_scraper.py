from typing import List
import requests

from config.logging import logger
from src.models.coin_market_cap_api import CryptoCurrencyData
from src.models.source import Source
from src.tasks.base_task import BaseTask


class CoinMarketCapScraper(BaseTask):
    limit: int = 25

    def scrap_data(
        self,
    ) -> List[CryptoCurrencyData]:
        logger.info("Scraping coinmarketcap data")
        params = {"limit": self.limit, "start": 1}
        route = f"{self.source_url}/data-api/v3/cryptocurrency/listing"
        response = requests.get(url=route, params=params)
        if response.status_code != 200:
            raise Exception("Erreur")
        else:
            raw_data = response.json()["data"]["cryptoCurrencyList"]
            return [
                CryptoCurrencyData.from_json(crypto_currency_raw_data)
                for crypto_currency_raw_data in raw_data
            ]

    def run_task(
        self,
    ) -> None:
        scraped_data = self.scrap_data()

        self.send_data_to_kafka_topic(
            source=Source.COIN_MARKET_CAP_API,
            data=[raw_data.to_dict() for raw_data in scraped_data],
        )

        logger.info("Data forwarded successfully to Kafka topic")
