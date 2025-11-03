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
from collections.abc import Iterator
from contextlib import contextmanager

from confluent_kafka import Consumer, Message

_LOG = logging.getLogger(__name__)


class KafkaReader:
    _BATCH_SIZE = 500
    _TIMEOUT_SECONDS = 30

    def __init__(self, cluster: str, topic: str, username: str | None, password: str | None) -> None:
        config = {
            "bootstrap.servers": cluster,
            "group.id": "butler-writer",
            # Configure manual commit of read offsets to guarantee
            # at-least-once processing of messages.
            "enable.auto.commit": "false",  # allow use of Consumer.commit()
            "auto.offset.reset": "earliest",  # read from the beginning if there is no stored offset
        }
        if username is not None:
            assert password is not None, "If username is provided, password must also be set."
            config["security.protocol"] = "sasl_plaintext"
            config["sasl.mechanism"] = "SCRAM-SHA-512"
            config["sasl.username"] = username
            config["sasl.password"] = password
        self._consumer = Consumer(config)
        self._consumer.subscribe([topic])
        self._pending_messages: list[Message] = []

    def close(self) -> None:
        self._consumer.close()

    @contextmanager
    def read_messages(self) -> Iterator[list[str]]:
        """Context manager returning a batch of messages read from Kafka.  If
        the context manager is exited cleanly without the caller's code
        throwing an exception, the messages are consumed and the read
        position is committed to the Kafka broker.  If an exception is
        thrown by the caller inside the context manager, the read position is
        not committed and the same messages will be returned the next time
        read_messages() is called.

        Returns
        -------
        messages
            The list of messages read from Kafka, decoded as UTF-8 strings.
        """
        # If we have some messages that we read out of Kafka but that the
        # caller failed to process, return them again.  This helps us guarantee
        # at-least-once processing without needing to manually seek() the Kafka
        # read offset on failure.
        if not self._pending_messages:
            self._pending_messages = self._fetch_next_message_batch()

        yield [msg.value().decode("utf-8") for msg in self._pending_messages]

        # Permanently store our read position at the broker.
        self._consumer.commit(asynchronous=False)
        self._pending_messages = []

    def _fetch_next_message_batch(self) -> list[Message]:
        while True:
            messages = self._consumer.consume(self._BATCH_SIZE, self._TIMEOUT_SECONDS)
            if not messages:
                _LOG.debug(f"Still waiting for Kafka message after {self._TIMEOUT_SECONDS} seconds")
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
                    return valid_messages
                elif errors:
                    raise RuntimeError(f"Error while reading Kafka messages: {errors}")
