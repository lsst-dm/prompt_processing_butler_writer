from __future__ import annotations

import logging

from confluent_kafka import Consumer

_LOG = logging.getLogger(__name__)


class KafkaReader:
    def __init__(self, cluster: str, topic: str) -> None:
        self._consumer = Consumer({"bootstrap.servers": cluster})
        self._consumer.subscribe(topic)

    def read_message(self) -> str:
        poll_timeout = 30  # seconds
        while True:
            msg = self._consumer.poll(poll_timeout)
            if msg is None:
                _LOG.debug(f"Still waiting for Kafka message after {poll_timeout} seconds")
            elif error := msg.error():
                raise RuntimeError(f"Kafka returned an error: {error}")
            else:
                return msg.value().decode("utf-8")
