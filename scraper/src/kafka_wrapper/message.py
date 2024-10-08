from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from src.models.source import Source


@dataclass
class KafkaMessage:
    source: str
    data: List[Dict]
    timestamp: str = datetime.now().isoformat()

    def to_dict(self):
        return {
            "source": self.source,
            "timestamp": self.timestamp,
            "data": [coin for coin in self.data],
        }

    @classmethod
    def build_kafka_message(cls, source: Source, data: List[Dict]) -> "KafkaMessage":
        return cls(
            source=source.value, data=data, timestamp=datetime.utcnow().isoformat()
        )
