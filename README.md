# Prompt Processing Butler Writer

This is a microservice to batch [Butler](https://github.com/lsst/daf_butler) database writes for
[Rubin Observatory's](https://rubinobservatory.org/)
[Prompt Processing](https://github.com/lsst-dm/prompt_processing) framework.
The rationale for its existence is discussed in [DMTN-310](https://dmtn-310.lsst.io/).

## Testing

There are a few things in this repo to help with testing:
* `docker-compose.yaml` file with a Docker configuration for a local Kafka broker setup.
* A script `insert_test_messages.py` that can be used to send messages to 
  the broker for testing the service.
* A script `run_service.sh` that starts up the broker and the writer service.

### Prerequisites
In order for the test setup to work, you need an installation of:
* Docker Desktop
* [The LSST science pipelines](https://developer.lsst.io/stack/lsstsw.html)
* A built copy of [ci_hsc](https://github.com/lsst/ci_hsc) or some other Butler repository containing data.

### Running tests

A typical test session looks like:
```
# Load science pipelines
. $LSSTSW/bin/envconfig
# Set up this repository
setup -r .
scons

# Start up the service.  Note that it has to be sourced, rather than invoked,
# so that the LSST stack environment variables carry over.
. ./run_service.sh

# See python insert_test_messages.py --help for flags that control
# which Butler data is inserted.
python insert_test_messages.py
```

## Build and Deploy

A Docker container for the service is built as part of the GitHub actions run
for all PRs and tags.  It is deployed as part of the
[Prompt Processing Phalanx deployment](https://github.com/lsst-sqre/phalanx/tree/main/charts/prompt-keda).