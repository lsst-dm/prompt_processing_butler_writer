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

import pydantic
from lsst.daf.butler import (
    SerializedDatasetType,
    SerializedDimensionRecord,
    SerializedFileDataset,
)


class PromptProcessingOutputEvent(pydantic.BaseModel):
    type: Literal["pp-output"]
    dimension_records: list[SerializedDimensionRecord]
    dataset_types: list[SerializedDatasetType] = pydantic.Field(default_factory=list)
    datasets: list[SerializedFileDataset]
