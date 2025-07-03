from __future__ import annotations

import logging
import os

import pydantic
from lsst.daf.butler import Butler

from .butler import handle_prompt_processing_completion
from .kafka import KafkaReader
from .messages import PromptProcessingOutputEvent


class ServiceConfig(pydantic.BaseModel):
    BUTLER_REPOSITORY: str
    KAFKA_CLUSTER: str
    KAFKA_TOPIC: str
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
    reader = KafkaReader(config.KAFKA_CLUSTER, config.KAFKA_TOPIC)

    _LOG.info("Waiting for messages...")
    while True:
        messages = reader.read_messages()
        events = [PromptProcessingOutputEvent.model_validate_json(msg) for msg in messages]
        _LOG.info(f"Received {len(events)} messages")
        handle_prompt_processing_completion(butler, events, config.FILE_STAGING_ROOT_PATH)


if __name__ == "__main__":
    main()
