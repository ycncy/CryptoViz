from dotenv import load_dotenv

from config.logging import logger
from src.workers.coin_market_cap_api_worker import CoinMarketCapAPIWorker
from src.workers.kraken_api_worker import KrakenApiWorker

load_dotenv()

if __name__ == "__main__":
    logger.info("Starting Kraken API Worker")
    worker = KrakenApiWorker()
    worker.read_from_kafka()
