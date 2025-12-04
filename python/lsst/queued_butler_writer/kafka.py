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
from typing import Callable, TypeAlias

from confluent_kafka import Consumer, Message, Producer

_LOG = logging.getLogger(__name__)

MessageHandler: TypeAlias = Callable[[list[str]], list[str]]
"""Callback function that takes a list of messages from Kafka as inputs, and
returns a list of messages to send to Kafka as outputs."""


class KafkaConnection:
    _BATCH_SIZE = 500
    _TIMEOUT_SECONDS = 30

    def __init__(
        self, cluster: str, input_topic: str, output_topic: str, username: str | None, password: str | None
    ) -> None:
        common_config = {
            "bootstrap.servers": cluster,
        }
        if username is not None or password is not None:
            assert username is not None, "If password is provided, username must also be set."
            assert password is not None, "If username is provided, password must also be set."
            common_config["security.protocol"] = "sasl_plaintext"
            common_config["sasl.mechanism"] = "SCRAM-SHA-512"
            common_config["sasl.username"] = username
            common_config["sasl.password"] = password

        consumer_config = common_config | {
            "group.id": "butler-writer",
            # Configure manual commit of read offsets to guarantee
            # at-least-once processing of messages.
            "enable.auto.commit": "false",  # allow use of Consumer.commit()
            "auto.offset.reset": "earliest",  # read from the beginning if there is no stored offset
        }
        self._consumer = Consumer(consumer_config)
        self._consumer.subscribe([input_topic])
        self._pending_messages: list[Message] = []

        producer_config = common_config | {"transactional.id": "butler-writer-output"}
        self._output_topic = output_topic
        self._producer = Producer(producer_config)
        self._producer.init_transactions()

    def close(self) -> None:
        self._consumer.close()

    def read_and_write_messages(self, callback: MessageHandler) -> None:
        """Read a batch of messages from the Kafka consumer, write a batch of
        messages to the Kafka producer, and store the consumer's read offsets
        at the broker.  The entire sequence is atomic -- either BOTH the read
        offsets and output messages are committed, or NEITHER is committed.
        This guarantees at-least-once processing of the messages.

        If this function completes successfully, it is guaranteed that both the
        output messages and the read position have been committed
        to the broker.

        If an exception is thrown, it is unknown whether the transaction
        completed successfully or failed.  In this case, the same messages will
        be read the next time this function is called, and as a result it is
        possible the messages may be processed more than once.

        Parameters
        ----------
        callback : `Callable` [ [`list` [`str`] ], `list` [ `str` ] ]
            Function that accepts a list of messages read from Kafka as input,
            and returns a list of messages to be sent to Kafka.
        """
        # If we have some messages that we read out of Kafka but that the
        # caller failed to process, return them again.  This helps us guarantee
        # at-least-once processing without needing to manually seek() the Kafka
        # read offset on failure.
        if not self._pending_messages:
            self._pending_messages = self._fetch_next_message_batch()

        input_messages = [msg.value().decode("utf-8") for msg in self._pending_messages]
        output_messages = callback(input_messages)
        try:
            self._producer.begin_transaction()
            output_batch = [{"value": msg} for msg in output_messages]
            self._producer.produce_batch(self._output_topic, output_batch)
            self._producer.send_offsets_to_transaction(
                self._consumer.position(self._consumer.assignment()), self._consumer.consumer_group_metadata()
            )
            self._producer.commit_transaction()
            self._pending_messages = []
        except Exception:
            self._producer.abort_transaction()
            raise

    def _fetch_next_message_batch(self) -> list[Message]:
        while True:
            messages = self._consumer.consume(self._BATCH_SIZE, self._TIMEOUT_SECONDS)
            if not messages:
                _LOG.info(f"Still waiting for Kafka message after {self._TIMEOUT_SECONDS} seconds")
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


class MockKafkaConnection:
    def __init__(self) -> None:
        self.last_output: list[str] = []
        self._pending_messages: list[str] = []

    def add_messages(self, messages: list[str]) -> None:
        self._pending_messages.extend(messages)

    def has_pending_messages(self) -> bool:
        return len(self._pending_messages) > 0

    def read_and_write_messages(self, callback: MessageHandler) -> None:
        output = callback(self._pending_messages)
        self._pending_messages = []
        self.last_output = output
