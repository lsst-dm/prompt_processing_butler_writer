from __future__ import annotations

from typing import Literal

import pydantic
from lsst.daf.butler import SerializedDimensionRecord, SerializedFileDataset


class PromptProcessingOutputEvent(pydantic.BaseModel):
    type: Literal["pp-output"]
    dimension_records: list[SerializedDimensionRecord]
    datasets: list[SerializedFileDataset]
