# Prompt Processing Butler Writer service releases

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