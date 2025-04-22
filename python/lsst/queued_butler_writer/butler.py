from lsst.daf.butler import (
    Butler,
    DatasetType,
    DimensionRecord,
    DimensionRecordSet,
    FileDataset,
    SerializedDatasetType,
    SerializedDimensionRecord,
    SerializedFileDataset,
)

from .messages import PromptProcessingOutputEvent


def handle_prompt_processing_completion(butler: Butler, event: PromptProcessingOutputEvent) -> None:
    _insert_dimension_records(butler, event.dimension_records)
    _insert_datasets(butler, event.datasets, event.dataset_types)


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


def _insert_datasets(
    butler: Butler,
    serialized_datasets: list[SerializedFileDataset],
    serialized_types: list[SerializedDatasetType],
) -> None:
    dataset_types = {
        dt.name: DatasetType.from_simple(dt, universe=butler.dimensions) for dt in serialized_types
    }

    def get_dataset_type(name: str) -> DatasetType:
        return dataset_types[name]

    datasets = [
        FileDataset.from_simple(ds, universe=butler.dimensions, dataset_type_loader=get_dataset_type)
        for ds in serialized_datasets
    ]
    butler.ingest(*datasets, transfer="move")
