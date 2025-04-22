from __future__ import annotations

from typing import Literal

import pydantic
from lsst.daf.butler import (
    SerializedDatasetType,
    SerializedDimensionRecord,
    SerializedFileDataset,
)


class PromptProcessingOutputEvent(pydantic.BaseModel):
    type: Literal["pp-output"]
    root_directory: str
    dimension_records: list[SerializedDimensionRecord]
    dataset_types: list[SerializedDatasetType]
    datasets: list[SerializedFileDataset]
