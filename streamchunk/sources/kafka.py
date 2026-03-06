from typing import Any, Dict, List, Optional

from .base import BaseSource


class KafkaSource(BaseSource):
    """
    Reads messages from a Kafka topic.
    Requires: pip install streamchunk[kafka]
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "streamchunk",
        value_deserializer=None,
        **consumer_kwargs
    ):
        try:
            from kafka import KafkaConsumer
        except ImportError:
            raise ImportError(
                "kafka-python is required. Install with: pip install streamchunk[kafka]"
            )

        self._exhausted = False
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=value_deserializer,
            **consumer_kwargs
        )

    def pull(self, n: int) -> List[Any]:
        rows = []
        for msg in self._consumer:
            rows.append(msg.value)
            if len(rows) >= n:
                break
        return rows

    def is_exhausted(self) -> bool:
        # Kafka topics are unbounded by default; exhaustion must be signalled externally
        return self._exhausted

    def stop(self):
        """Signal this source to stop iteration."""
        self._exhausted = True

    def close(self):
        self._consumer.close()
