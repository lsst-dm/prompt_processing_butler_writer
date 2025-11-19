#!/bin/bash

export KAFKA_CLUSTER=localhost:9092
export KAFKA_TOPIC=rubin-prompt-processing-butler-output
export BUTLER_REPOSITORY=$(dirname "$0")/testrepo
export OUTPUT_DATASET_LIST_DIRECTORY=$(dirname "$0")/output-directory

docker compose up --detach --wait --remove-orphans
echo Docker startup complete.
if [ ! -f "$BUTLER_REPOSITORY/butler.yaml" ]; then
    echo Butler repository does not exist, creating it
    butler create "$BUTLER_REPOSITORY"
fi
mkdir -p "$OUTPUT_DATASET_LIST_DIRECTORY"

echo Starting service
python -m lsst.queued_butler_writer.main
