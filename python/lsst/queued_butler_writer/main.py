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
    config = ServiceConfig.model_validate_strings(os.environ)
    butler = Butler(config.BUTLER_REPOSITORY)
    reader = KafkaReader(config.KAFKA_CLUSTER, config.KAFKA_TOPIC)

    while True:
        message = reader.read_message()
        event = PromptProcessingOutputEvent.model_validate_json(message)
        handle_prompt_processing_completion(butler, event)


if __name__ == "__main__":
    main()
