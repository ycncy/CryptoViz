import json
from dataclasses import dataclass
from typing import List, Optional, Any

from kafka import KafkaProducer

from src.kafka.message import KafkaMessage


@dataclass
class Producer:
    host: List[str]
    client_id: str
    connexion: Optional[KafkaProducer] = None

    def connect(self) -> None:
        self.connexion = KafkaProducer(
            bootstrap_servers=self.host,
            client_id=self.client_id,
            value_serializer=lambda v: json.dumps(v).encode("ascii"),
        )

    def send_message(
        self,
        topic: str,
        message: KafkaMessage,
        key: Any = None,
        partition: Optional[int] = None,
    ) -> None:
        if not self.connexion:
            raise ConnectionError("Producer not connected, use connect() first")
        self.connexion.send(
            topic, value=message.to_dict(), key=key, partition=partition
        )
        self.connexion.flush()

    def close(self) -> None:
        if self.connexion:
            self.connexion.close()
