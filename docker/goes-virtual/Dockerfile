# GOES ABI-L1b-RadF virtual-reference ingest.
#
# Built from a purpose-staged context (see build_and_push.sh), not the repo
# root: planetary_datasets/ holds ~1.8TB of downloaded data, so the repo is not
# a usable build context. Only the six modules the ingest imports are copied.
#
# linux/amd64 only. Run on an x86_64 EC2 instance, not Graviton.
#
# No credentials are baked in; Source Cooperative keys are read from the
# environment at run time.
FROM --platform=linux/amd64 mambaorg/micromamba:2.0.5-debian12-slim

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        procps tini ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV MAMBA_DOCKERFILE_ACTIVATE=1 \
    PYTHONUNBUFFERED=1

COPY environment.yml /tmp/environment.yml
RUN micromamba install -y -n base -f /tmp/environment.yml \
    && micromamba clean --all --yes \
    && rm /tmp/environment.yml

WORKDIR /app

# Only the modules the ingest imports: the four satellite definitions, their
# shared implementation, and the CLI.
COPY planetary_datasets ./planetary_datasets
COPY run_goes_virtual.sh /usr/local/bin/run_goes_virtual
COPY memory_watchdog.py /usr/local/bin/memory_watchdog.py
RUN chmod +x /usr/local/bin/run_goes_virtual

# Logs and scratch. Mount a volume here so the record survives restarts.
#
# MALLOC_ARENA_MAX caps glibc's per-thread heap arenas. A batch churns
# hundreds of MB of chunk-manifest data across obstore's many worker threads,
# and glibc would otherwise open up to 8 arenas per core, each fragmenting
# and retaining its own free spans — RSS that gc.collect() cannot touch. The
# ingest is I/O bound, so the reduced allocator concurrency costs nothing.
#
# PLANETARY_DATASETS_DATA_DIR points planetary_datasets.config at the same
# volume: without it the config would default to /app/data, which is inside
# the image and not persisted.
ENV GOES_OUT=/data \
    PLANETARY_DATASETS_DATA_DIR=/data \
    PLANETARY_DATASETS_SCRATCH_DIR=/data/scratch \
    PYTHONPATH=/app \
    MALLOC_ARENA_MAX=2
RUN mkdir -p /data/logs /data/scratch && chown -R $MAMBA_USER:$MAMBA_USER /data /app

USER $MAMBA_USER
VOLUME ["/data"]

# tini reaps the ProcessPoolExecutor workers: they are spawned rather than
# forked, so without an init they accumulate as zombies under PID 1.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/_entrypoint.sh", "/usr/local/bin/run_goes_virtual"]
