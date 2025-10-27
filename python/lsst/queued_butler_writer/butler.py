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

import logging
from lsst.utils.timer import time_this

from lsst.daf.butler import (
    Butler,
    DatasetType,
    DimensionRecord,
    DimensionRecordSet,
    FileDataset,
)
from lsst.daf.butler.registry import ConflictingDefinitionError

from .messages import PromptProcessingOutputEvent

_LOG = logging.getLogger(__name__)


def handle_prompt_processing_completion(butler: Butler, events: list[PromptProcessingOutputEvent]) -> None:
    try:
        _insert_data_from_messages(butler, events)
    except ConflictingDefinitionError as e:
        # If there is a mismatch between the data we are inserting and the data
        # already in the database, Butler will throw
        # ConflictingDefinitionError.
        #
        # One way this could happen is if two pods accidentally handle the same
        # image, generating different dataset UUIDs for the same data ID.
        #
        # This is not expected to occur during normal operation, but if it
        # does we could get stuck on this message because retrying will not
        # solve this issue.  In this case we move past the problematic message
        # by retrying one at a time so we don't lose the entire batch.
        _LOG.error(
            "Encountered unrecoverable error while ingesting batch."
            "Retrying one message at a time to recover.",
            exc_info=e,
        )
        for event in events:
            try:
                _insert_data_from_messages(butler, [event])
            except ConflictingDefinitionError as single_message_error:
                _LOG.error(
                    "Unrecoverable error for message:\n%s",
                    event.model_dump_json(indent=2),
                    exc_info=single_message_error,
                )


def _insert_data_from_messages(butler: Butler, events: list[PromptProcessingOutputEvent]) -> None:
    _insert_dimension_records(butler, events)
    _insert_datasets(butler, events)


def _deserialize_dimension_records(
    butler: Butler, events: list[PromptProcessingOutputEvent]
) -> list[DimensionRecord]:
    output = []
    for event in events:
        deserialized_records = [
            DimensionRecord.from_simple(record, universe=butler.dimensions)
            for record in event.dimension_records
        ]
        output.extend(deserialized_records)

    return output


def _insert_dimension_records(butler: Butler, events: list[PromptProcessingOutputEvent]) -> None:
    records = _deserialize_dimension_records(butler, events)
    grouped_records = _group_and_deduplicate_dimension_records(records)
    dimensions = butler.dimensions.sorted(grouped_records.keys())
    for dimension in dimensions:
        records = grouped_records[dimension.name]
        with time_this(_LOG, msg=f"inserted {len(records)} dimension data records", level=logging.DEBUG):
            butler.registry.insertDimensionData(dimension, *records, skip_existing=True)


def _group_and_deduplicate_dimension_records(records: list[DimensionRecord]) -> dict[str, DimensionRecordSet]:
    sets: dict[str, DimensionRecordSet] = {}
    for record in records:
        dimension = record.definition.name
        if (set := sets.get(dimension)) is None:
            set = DimensionRecordSet(record.definition)
            sets[dimension] = set
        set.add(record)

    return sets


def _deserialize_datasets(butler: Butler, event: PromptProcessingOutputEvent) -> list[FileDataset]:
    dataset_types = {
        dt.name: DatasetType.from_simple(dt, universe=butler.dimensions) for dt in event.dataset_types
    }

    def get_dataset_type(name: str) -> DatasetType:
        if (dt := dataset_types.get(name)) is not None:
            return dt

        return butler.get_dataset_type(name)

    datasets = [
        FileDataset.from_simple(ds, universe=butler.dimensions, dataset_type_loader=get_dataset_type)
        for ds in event.datasets
    ]

    return datasets


def _insert_datasets(butler: Butler, events: list[PromptProcessingOutputEvent]) -> None:
    datasets = []
    for event in events:
        deserialized_datasets = _deserialize_datasets(butler, event)
        datasets.extend(deserialized_datasets)

    with time_this(_LOG, msg=f"ingested {len(datasets)} datasets", level=logging.DEBUG):
        butler.ingest(
            *datasets, transfer=None, skip_existing=True, record_validation_info=False
        )
