# MeteoSwiss KENDA-CH1 downloader, launched by the `nwp/kenda_download` Dagster asset.
#
# Built from a staged context (see build.sh), not the repo root: the only project file
# the image needs is planetary_datasets/providers/kenda_download.py.
#
# No credentials: the MeteoSwiss Open Government Data API is public.
FROM mambaorg/micromamba:2.0.5-debian12-slim

USER root
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

ENV MAMBA_DOCKERFILE_ACTIVATE=1 \
    PYTHONUNBUFFERED=1

COPY environment.yml /tmp/environment.yml
RUN micromamba install -y -n base -f /tmp/environment.yml \
    && micromamba clean --all --yes \
    && rm /tmp/environment.yml

WORKDIR /app
COPY planetary_datasets ./planetary_datasets

# The Dagster asset runs the container as the host user so the files it writes into the
# bind-mounted archive are owned by that user, and that uid has no home directory here.
ENV PYTHONPATH=/app \
    HOME=/tmp \
    KENDA_ARCHIVE_DIR=/data/meteoswiss
RUN mkdir -p /data/meteoswiss && chmod -R a+rwX /data /app
VOLUME ["/data/meteoswiss"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/_entrypoint.sh", \
            "python", "-m", "planetary_datasets.providers.kenda_download"]
