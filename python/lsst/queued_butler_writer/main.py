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
from uuid import uuid4

import backoff
import pydantic
from lsst.daf.butler import Butler, DatasetId
from lsst.resources import ResourcePath

from .butler import handle_prompt_processing_completion
from .kafka import KafkaReader
from .messages import PromptProcessingOutputEvent, DatasetBatch, BatchIngestedEvent


class ServiceConfig(pydantic.BaseModel):
    """Environment variables used to configure the Butler writer service."""

    BUTLER_REPOSITORY: str
    KAFKA_CLUSTER: str
    KAFKA_TOPIC: str
    """Input topic where messages are received from worker pods, signaling the
    availability of new files for ingest.
    """
    KAFKA_USERNAME: str | None = None
    KAFKA_PASSWORD: str | None = None
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
    reader = KafkaReader(
        config.KAFKA_CLUSTER, config.KAFKA_TOPIC, config.KAFKA_USERNAME, config.KAFKA_PASSWORD
    )

    processor = MessageProcessor(config, butler, reader)
    try:
        _LOG.info("Waiting for messages...")
        while True:
            processor.process_messages()
    finally:
        reader.close()


class MessageProcessor:
    def __init__(self, config: ServiceConfig, butler: Butler, reader: KafkaReader) -> None:
        self._config = config
        self._butler = butler
        self._reader = reader

    @backoff.on_exception(
        backoff.expo, exception=Exception, logger=_LOG, base=10, max_value=30, max_tries=5, jitter=None
    )
    def process_messages(self) -> BatchIngestedEvent:
        with self._reader.read_messages() as messages:
            events = [PromptProcessingOutputEvent.model_validate_json(msg) for msg in messages]
            _LOG.info(f"Received {len(events)} messages")
            dataset_ids = handle_prompt_processing_completion(self._butler, events)
            _LOG.info(f"Ingested {len(events)} messages into the Butler")
            output = self._write_output_message(dataset_ids)
            _LOG.info(f"Wrote dataset list to '{output.batch_file}'")
            _LOG.info(f"Successfully processed {len(events)} messages")
            return output

    def _write_output_message(self, datasets: list[DatasetId]) -> BatchIngestedEvent:
        current_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%f")
        batch_id = uuid4()
        filename = f"{current_time}-{batch_id}.json"
        output_data = DatasetBatch(datasets=datasets).model_dump_json().encode("utf-8")
        output_path = ResourcePath(self._config.OUTPUT_DATASET_LIST_DIRECTORY).join(filename)
        output_path.write(output_data)

        return BatchIngestedEvent(
            type="batch-ingested", batch_id=batch_id, origin="prompt_processing", batch_file=filename
        )


if __name__ == "__main__":
    main()
