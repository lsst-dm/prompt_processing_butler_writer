# This file is part of prompt_processing_butler_writer.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

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
