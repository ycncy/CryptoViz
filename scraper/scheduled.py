import os

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.kafka_wrapper.producer import Producer
from src.tasks.coin_market_cap_scraper import CoinMarketCapScraper


def init_kafka_producer() -> Producer:
    host = os.environ.get("KAFKA_HOST", "localhost:9092")
    client_id = os.environ.get("KAFKA_CLIENT_ID", "tester")
    return Producer(
        host=[host],
        client_id=client_id,
    )


if __name__ == "__main__":
    kafka_producer = init_kafka_producer()
    kafka_producer.connect()

    coin_market_cap_task = CoinMarketCapScraper(
        source_url="https://api.coinmarketcap.com/",
        kafka_producer=kafka_producer,
        kafka_topic="raw_data.coin_market_cap.api",
    )

    scheduler = BlockingScheduler()

    scheduler.add_job(
        coin_market_cap_task.run_task, trigger=IntervalTrigger(seconds=10)
    )

    scheduler.start()
