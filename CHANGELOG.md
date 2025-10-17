# Prompt Processing Butler Writer service releases

## 2.2.0

We no longer record file sizes for ingested datasets in the Butler database.
This avoids an S3 HEAD call per dataset.

We now log timing information for the Butler dimension record inserts and
dataset ingestion.

There is a new optional environment variable `WRITER_DEBUG_LEVEL` to control
the Python log level.

## 2.1.0

When reading from Kafka, we now commit the read offset only after successfully
writing to the Butler database. This ensures that each message will be
completely processed at least once.

We now retry message processing a few times before terminating the process, so
that transient network errors don't require a full service restart to recover.

We now explicitly handle ConflictingDefinitionError exceptions from the Butler,
so that a single message containing invalid data does not cause us to lose
an entire batch of data.

## 2.0.1

Update the base stack image to `d_2025_09_05`, which contains an updated
`daf_butler` to fix an issue where the service could not ingest into a Butler
configured with a ChainedDatastore.

## 2.0.0
Files are now assumed to have been written to their final location in the
datastore prior to ingestion. This lets us avoid unnecessary S3 I/O. (See
[DM-52180](https://rubinobs.atlassian.net/browse/DM-52180)).

## 1.0.0
Initial release.