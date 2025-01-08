import os

import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

from config.logging import logger
from src.database.postgres import PostgresDB
from src.tasks.OHLC_kraken_scraper import OHLCKrakenScraper
from src.tasks.aggregate_and_delete_old_data import delete_old_data, aggregate_old_data
from src.kafka.producer import Producer
from src.tasks.coin_market_cap_scraper import CoinMarketCapScraper

load_dotenv()


def init_postgres_connection() -> PostgresDB:
    logger.info("Connecting to postgres database...")
    return PostgresDB(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
    )


def init_kafka_producer() -> Producer:
    logger.info("Initializing kafka producer...")
    host = os.environ.get("KAFKA_HOST", "kafka:9092")
    client_id = os.environ.get("KAFKA_CLIENT_ID", "scheduler")
    return Producer(
        host=[host],
        client_id=client_id,
    )


if __name__ == "__main__":
    logger.info("Initializing scheduler...")

    kafka_producer = init_kafka_producer()
    kafka_producer.connect()
    logger.info("Successfully connected to Kafka.")

    postgres_connection = init_postgres_connection()

    coin_market_cap_task = CoinMarketCapScraper(
        source_url="https://api.coinmarketcap.com/",
        kafka_producer=kafka_producer,
        kafka_topic="raw_data.coin_market_cap.api",
    )

    kraken_task = OHLCKrakenScraper(
        source_url="https://api.kraken.com/0/public/OHLC",
        kafka_producer=kafka_producer,
        kafka_topic="raw_data.ohlc.kraken.api",
    )

    scheduler = BlockingScheduler()

    scheduler.add_job(
        coin_market_cap_task.run_task,
        trigger=IntervalTrigger(minutes=2, timezone=pytz.UTC),
    )

    scheduler.add_job(
        kraken_task.run_task,
        trigger=IntervalTrigger(minutes=0, timezone=pytz.UTC),
    )

    scheduler.add_job(
        aggregate_old_data,
        args=[postgres_connection.session],
        trigger=CronTrigger(hour=0, minute=5, timezone=pytz.UTC),
    )

    scheduler.add_job(
        delete_old_data,
        args=[postgres_connection.session],
        trigger=CronTrigger(hour=0, minute=5, timezone=pytz.UTC),
    )

    logger.info("Starting the scheduler...")
    scheduler.start()
