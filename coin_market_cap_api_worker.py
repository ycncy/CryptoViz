from dotenv import load_dotenv

from config.logging import logger
from src.workers.coin_market_cap_api_worker import CoinMarketCapAPIWorker

load_dotenv()

if __name__ == "__main__":
    logger.info("Starting CoinMarketCap API Worker")
    worker = CoinMarketCapAPIWorker()
    worker.read_from_kafka()
