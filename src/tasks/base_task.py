from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict

from src.kafka.message import KafkaMessage
from src.kafka.producer import Producer
from src.models.source import Source


@dataclass
class BaseTask(ABC):
    source_url: str
    kafka_producer: Producer
    kafka_topic: str

    @abstractmethod
    def scrap_data(self):
        raise NotImplementedError

    def send_data_to_kafka_topic(self, data: List[Dict], source: Source) -> None:
        if len(data) == 0:
            raise Exception("No data to publish")

        kafka_message = KafkaMessage.build_kafka_message(
            source=source, data=[raw_data for raw_data in data]
        )

        self.kafka_producer.send_message(topic=self.kafka_topic, message=kafka_message)

    @abstractmethod
    def run_task(cls) -> None:
        raise NotImplementedError
