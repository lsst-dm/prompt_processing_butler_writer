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
import os

import pydantic
from lsst.daf.butler import Butler

from .butler import handle_prompt_processing_completion
from .kafka import KafkaReader
from .messages import PromptProcessingOutputEvent


class ServiceConfig(pydantic.BaseModel):
    """Environment variables used to configure the Butler writer service."""

    BUTLER_REPOSITORY: str
    KAFKA_CLUSTER: str
    KAFKA_TOPIC: str
    KAFKA_USERNAME: str | None = None
    KAFKA_PASSWORD: str | None = None
    FILE_STAGING_ROOT_PATH: str
    """Path to the directory where files will be stored prior to ingestion into
    the Butler, as a URI string in the format understood by
    `lsst.resources.ResourcePath`.
    """


_LOG = logging.getLogger(__name__)


def main():
    logging.basicConfig()
    logging.getLogger().setLevel("INFO")
    config = ServiceConfig.model_validate_strings(dict(os.environ))
    _LOG.info("Connecting to Butler...")
    butler = Butler(config.BUTLER_REPOSITORY, writeable=True)
    _LOG.info("Connecting to Kafka...")
    reader = KafkaReader(
        config.KAFKA_CLUSTER, config.KAFKA_TOPIC, config.KAFKA_USERNAME, config.KAFKA_PASSWORD
    )

    _LOG.info("Waiting for messages...")
    while True:
        messages = reader.read_messages()
        events = [PromptProcessingOutputEvent.model_validate_json(msg) for msg in messages]
        _LOG.info(f"Received {len(events)} messages")
        handle_prompt_processing_completion(butler, events, config.FILE_STAGING_ROOT_PATH)


if __name__ == "__main__":
    main()
