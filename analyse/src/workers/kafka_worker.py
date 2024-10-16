import json
import os
from abc import ABC, abstractmethod

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

print(os.getenv("KAFKA_HOST"))


class KafkaWorker(ABC):
    def __init__(self, topic_name):
        self.topic_name = topic_name
        self.consumer = KafkaConsumer(self.topic_name, bootstrap_servers=os.getenv("KAFKA_HOST"),
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                                      auto_offset_reset='earliest')

    def read_from_kafka(self):
        print(f"Start processing kafka messages from topic {self.topic_name}")
        for message in self.consumer:
            print("Processing message")
            self.process_data(message.value)

    @abstractmethod
    def process_data(self, message):
        pass
