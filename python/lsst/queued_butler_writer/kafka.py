from __future__ import annotations

import logging

from confluent_kafka import Consumer

_LOG = logging.getLogger(__name__)


class KafkaReader:
    def __init__(self, cluster: str, topic: str, username: str | None, password: str | None) -> None:
        config = {"bootstrap.servers": cluster, "group.id": "butler-writer"}
        if username is not None:
            config["security.protocol"] = "sasl_plaintext"
            config["sasl.mechanism"] = "SCRAM-SHA-512"
            config["sasl.username"] = username
            config["sasl.password"] = password
        self._consumer = Consumer(config)
        self._consumer.subscribe([topic])

    def read_messages(self, batch_size: int = 500, timeout_seconds: int = 30) -> list[str]:
        while True:
            messages = self._consumer.consume(batch_size, timeout_seconds)
            if not messages:
                _LOG.debug(f"Still waiting for Kafka message after {timeout_seconds} seconds")
            else:
                valid_messages = []
                errors = []
                for msg in messages:
                    if (error := msg.error()) is None:
                        valid_messages.append(msg)
                    else:
                        error_message = error.str()
                        _LOG.error(f"Kafka error: {error_message}")
                        errors.append(error_message)
                if valid_messages:
                    return [msg.value().decode("utf-8") for msg in valid_messages]
                elif errors:
                    raise RuntimeError(f"Error while reading Kafka messages: {errors}")
