import json
import os
from abc import ABC, abstractmethod

from dotenv import load_dotenv
from kafka import KafkaConsumer

from config.logging import logger

load_dotenv()


class KafkaWorker(ABC):
    def __init__(self, topic_name):
        self.topic_name = topic_name
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=os.environ.get("KAFKA_HOST", "kafka:9092"),
            client_id=os.environ.get("KAFKA_CLIENT_ID", "kafka:9092"),
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

    def read_from_kafka(self):
        logger.info(f"Start processing kafka messages from topic {self.topic_name}")
        for message in self.consumer:
            logger.info("Processing message")
            self.process_data(message.value)

    @abstractmethod
    def process_data(self, message):
        pass
