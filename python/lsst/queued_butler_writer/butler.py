from lsst.daf.butler import (
    Butler,
    DatasetType,
    DimensionRecord,
    DimensionRecordSet,
    FileDataset,
)
from lsst.resources import ResourcePath

from .messages import PromptProcessingOutputEvent


def handle_prompt_processing_completion(
    butler: Butler, events: list[PromptProcessingOutputEvent], file_staging_root_path: str
) -> None:
    dimension_records = _deserialize_dimension_records(butler, events)
    _insert_dimension_records(butler, dimension_records)

    _insert_datasets(butler, events, file_staging_root_path)


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


def _insert_dimension_records(butler: Butler, records: list[DimensionRecord]) -> None:
    grouped_records = _group_and_deduplicate_dimension_records(records)
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


def _deserialize_datasets(
    butler: Butler, event: PromptProcessingOutputEvent, root_path: ResourcePath
) -> list[FileDataset]:
    dataset_types = {
        dt.name: DatasetType.from_simple(dt, universe=butler.dimensions) for dt in event.dataset_types
    }

    def get_dataset_type(name: str) -> DatasetType:
        return dataset_types[name]

    subdirectory = _safely_join_path(root_path, event.root_directory, is_directory=True)

    datasets = [
        FileDataset.from_simple(ds, universe=butler.dimensions, dataset_type_loader=get_dataset_type)
        for ds in event.datasets
    ]
    for dataset in datasets:
        dataset.path = _safely_join_path(subdirectory, dataset.path, is_directory=False)

    return datasets


def _insert_datasets(
    butler: Butler, events: list[PromptProcessingOutputEvent], file_staging_root_path: str
) -> None:
    root = ResourcePath(file_staging_root_path)
    datasets = []
    for event in events:
        deserialized_datasets = _deserialize_datasets(butler, event, root)
        datasets.extend(deserialized_datasets)

    butler.ingest(*datasets, transfer="move")


def _safely_join_path(
    root: ResourcePath, join_path: str | ResourcePath, *, is_directory: bool
) -> ResourcePath:
    output = root.join(join_path, forceDirectory=is_directory)
    if output.relative_to(root) is None:
        raise ValueError(f"Input path '{join_path}' escapes outside expected location '{root}'")
    if output == root:
        raise ValueError(f"Input path '{join_path}' was equivalent to an empty path")

    return output
