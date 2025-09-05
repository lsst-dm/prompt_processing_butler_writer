ARG PIPE_CONTAINER=ghcr.io/lsst/scipipe
ARG STACK_TAG=al9-d_2025_09_05
FROM ${PIPE_CONTAINER}:${STACK_TAG}
WORKDIR /app
COPY python python
COPY ups ups
CMD source /opt/lsst/software/stack/loadLSST.bash \
    && setup lsst_distrib \
    && setup -r /app \
    && python -m lsst.queued_butler_writer.main