import click
from confluent_kafka import Producer
from lsst.daf.butler import Butler, DatasetRef, DimensionRecord
from lsst.queued_butler_writer.messages import PromptProcessingOutputEvent


@click.command()
@click.option("--broker", default="localhost:9092")
@click.option("--topic", default="rubin-prompt-processing-butler-output")
# By default, pull data from a copy of ci_hsc checked out adjacent to this
# repository.
@click.option("--repo", default="../ci_hsc_gen3/DATA")
@click.option("--collection", default="HSC/runs/ci_hsc")
@click.option("--where", default="")
def main(repo: str, broker: str, where: str, collection: str, topic: str) -> None:
    producer = Producer({"bootstrap.servers": broker})
    butler = Butler.from_config(repo)
    dimension_records = _find_dimension_records(butler, where)
    datasets = _find_datasets(butler, where, collection)
    file_datasets = butler._datastore.export(datasets, transfer=None)
    dataset_types = {dataset.datasetType for dataset in datasets}

    message = PromptProcessingOutputEvent(
        type="pp-output",
        dimension_records=[record.to_simple() for record in dimension_records],
        datasets=[dataset.to_simple() for dataset in file_datasets],
        dataset_types=[dt.to_simple() for dt in dataset_types],
    )

    producer.produce(topic, message.model_dump_json())
    producer.flush()
    print(message)
    print(len(message.dimension_records))
    print(len(message.datasets))
    print(len(message.dataset_types))


def _find_dimension_records(butler: Butler, where: str) -> list[DimensionRecord]:
    """
    Find all dimension records that might be associated with the given where
    clause.
    """
    records = []
    for dimension in butler.dimensions.dimensions:
        if dimension.has_own_table:
            records.extend(butler.query_dimension_records(dimension.name, where=where, explain=False))
    return records


def _find_datasets(butler: Butler, where: str, collection: str) -> list[DatasetRef]:
    dataset_types = [
        dstype
        for dstype in butler.registry.queryDatasetTypes(...)
        if "detector" in dstype.dimensions and not dstype.isCalibration()
    ]
    datasets = []
    for dt in dataset_types:
        datasets.extend(butler.query_datasets(dt, collection, where=where, explain=False, limit=5))
    return datasets


if __name__ == "__main__":
    main()
