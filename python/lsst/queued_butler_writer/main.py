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

import datetime
import logging
import os
from typing import Literal
from uuid import uuid4

import backoff
import pydantic
from lsst.daf.butler import Butler, DatasetId
from lsst.resources import ResourcePath

from .butler import handle_prompt_processing_completion
from .kafka import KafkaConnection
from .messages import (
    BatchIngestedEvent,
    DatasetBatch,
    PromptProcessingDayEndEvent,
    PromptProcessingOutputEvent,
    input_event_adapter,
)


class ServiceConfig(pydantic.BaseModel):
    """Environment variables used to configure the Butler writer service."""

    BUTLER_REPOSITORY: str
    KAFKA_CLUSTER: str
    KAFKA_TOPIC: str
    """Input topic where messages are received from worker pods, signaling the
    availability of new files for ingest.
    """
    KAFKA_OUTPUT_TOPIC: str
    """Output topic where messages are sent after ingesting a batch of
    datasets, to signal the Prompt Publication Service that new datasets are
    available.
    """
    KAFKA_USERNAME: str | None = None
    KAFKA_PASSWORD: str | None = None
    KAFKA_DEBUG: Literal["0", "1"] = "0"
    """If '1', enable additional kafka debug logging."""
    OUTPUT_DATASET_LIST_DIRECTORY: str
    """
    Directory URI (in `lsst.resources.ResourcePath` format) where lists of
    ingested datasets will be written.  This storage location is referenced
    by Kafka messages sent to `KAFKA_OUTPUT_TOPIC`, above.
    """
    WRITER_DEBUG_LEVEL: str | None = None


_LOG = logging.getLogger(__name__)


def main():
    config = ServiceConfig.model_validate_strings(dict(os.environ))
    level_mapping = logging.getLevelNamesMapping()
    level = level_mapping.get(config.WRITER_DEBUG_LEVEL, logging.INFO)
    logging.basicConfig(level=level)
    _LOG.info("Connecting to Butler...")
    butler = Butler(config.BUTLER_REPOSITORY, writeable=True)
    _LOG.info("Connecting to Kafka...")
    reader = KafkaConnection(
        cluster=config.KAFKA_CLUSTER,
        input_topic=config.KAFKA_TOPIC,
        output_topic=config.KAFKA_OUTPUT_TOPIC,
        username=config.KAFKA_USERNAME,
        password=config.KAFKA_PASSWORD,
        debug=config.KAFKA_DEBUG == "1",
    )

    processor = MessageProcessor(config, butler, reader)
    try:
        _LOG.info("Waiting for messages...")
        while True:
            processor.process_messages()
    finally:
        reader.close()


class MessageProcessor:
    def __init__(self, config: ServiceConfig, butler: Butler, reader: KafkaConnection) -> None:
        self._config = config
        self._butler = butler
        self._reader = reader

    @backoff.on_exception(
        backoff.expo, exception=Exception, logger=_LOG, base=10, max_value=30, max_tries=5, jitter=None
    )
    def process_messages(self) -> None:
        self._reader.read_and_write_messages(self._handle_messages)
        _LOG.info("Committed output to Kafka.")

    def _handle_messages(self, messages: list[str]) -> list[str]:
        output_events: list[PromptProcessingOutputEvent] = []
        dayend_events: list[PromptProcessingDayEndEvent] = []
        discarded = 0
        for msg in messages:
            try:
                event = input_event_adapter.validate_json(msg)
            except pydantic.ValidationError as e:
                _LOG.error("Discarding unparseable message:\n%s", msg, exc_info=e)
                discarded += 1
                continue
            if isinstance(event, PromptProcessingOutputEvent):
                output_events.append(event)
            else:
                dayend_events.append(event)
        _LOG.info(
            f"Received {len(output_events)} output messages, "
            f"{len(dayend_events)} day-end messages, "
            f"and discarded {discarded} unparseable messages"
        )

        output_messages: list[str] = []
        if output_events:
            dataset_ids = handle_prompt_processing_completion(self._butler, output_events)
            _LOG.info(f"Ingested {len(output_events)} messages into the Butler")
            output = self._write_output_message(dataset_ids)
            _LOG.info(f"Wrote dataset list to '{output.batch_file}'")
            output_messages.append(output.model_dump_json())

        for event in dayend_events:
            _LOG.info(f"Forwarding day-end signal for {event.instrument} day_obs {event.day_obs}")
            output_messages.append(event.model_dump_json())

        _LOG.info(f"Successfully processed {len(output_events) + len(dayend_events)} messages")
        return output_messages

    def _write_output_message(self, datasets: list[DatasetId]) -> BatchIngestedEvent:
        current_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%f")
        batch_id = uuid4()
        filename = f"{current_time}-{batch_id}.json"
        output_data = DatasetBatch(batch_id=batch_id, datasets=datasets).model_dump_json().encode("utf-8")
        output_path = ResourcePath(self._config.OUTPUT_DATASET_LIST_DIRECTORY).join(filename)
        output_path.write(output_data)

        return BatchIngestedEvent(
            type="batch-ingested", batch_id=batch_id, origin="prompt_processing", batch_file=filename
        )


if __name__ == "__main__":
    main()
