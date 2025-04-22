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


_LOG = logging.getLogger(__name__)


def main():
    logging.basicConfig()
    logging.getLogger().setLevel("INFO")
    config = ServiceConfig.model_validate_strings(dict(os.environ))
    _LOG.info("Connecting to Butler...")
    butler = Butler(config.BUTLER_REPOSITORY, writeable=True)
    _LOG.info("Connecting to Kafka...")
    reader = KafkaReader(config.KAFKA_CLUSTER, config.KAFKA_TOPIC)

    _LOG.info("Waiting for messages.")
    while True:
        message = reader.read_message()
        event = PromptProcessingOutputEvent.model_validate_json(message)
        _LOG.info(
            f"Received message {event.type} with {len(event.datasets)} and {len(event.dimension_records)} dimension records"
        )
        handle_prompt_processing_completion(butler, event)


if __name__ == "__main__":
    main()
