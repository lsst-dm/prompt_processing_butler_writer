from lsst.daf.butler import (
    Butler,
    DimensionRecord,
    DimensionRecordSet,
    FileDataset,
    SerializedDimensionRecord,
    SerializedFileDataset,
)

from .messages import PromptProcessingOutputEvent


def handle_prompt_processing_completion(butler: Butler, event: PromptProcessingOutputEvent) -> None:
    _insert_dimension_records(butler, event.dimension_records)
    _insert_datasets(butler, event.datasets)


def _insert_dimension_records(butler: Butler, serialized_records: list[SerializedDimensionRecord]) -> None:
    deserialized_records = [
        DimensionRecord.from_simple(record, universe=butler.dimensions) for record in serialized_records
    ]
    grouped_records = _group_and_deduplicate_dimension_records(deserialized_records)
    for dimension, records in grouped_records.items():
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


def _insert_datasets(butler: Butler, serialized_datasets=list[SerializedFileDataset]) -> None:
    datasets = [FileDataset.from_simple(ds, butler=butler) for ds in serialized_datasets]
    butler.ingest(*datasets, transfer="move")
