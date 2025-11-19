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

from typing import Literal
from uuid import UUID

import pydantic
from lsst.daf.butler import (
    SerializedDatasetType,
    SerializedDimensionRecord,
    SerializedFileDataset,
)


class PromptProcessingOutputEvent(pydantic.BaseModel):
    """Kafka message sent from worker pods to the Butler Writer Service,
    signaling that new datasets have been generated and are available for
    ingest.
    """

    type: Literal["pp-output"]
    dimension_records: list[SerializedDimensionRecord]
    dataset_types: list[SerializedDatasetType] = pydantic.Field(default_factory=list)
    datasets: list[SerializedFileDataset]


class BatchIngestedEvent(pydantic.BaseModel):
    """Kafka message sent from Butler Writer Service to Prompt Publication
    Service, signaling that new datasets have been ingested and can be
    considered for future un-embargo.
    """

    type: Literal["batch-ingested"]
    batch_id: UUID
    origin: str
    """The name of the service that these datasets originated from."""
    batch_file: str
    """Path to file containing JSON-serialized version of `DatasetBatch`
    model.
    """


class DatasetBatch(pydantic.BaseModel):
    datasets: list[UUID]
