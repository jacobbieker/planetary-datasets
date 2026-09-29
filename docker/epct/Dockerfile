# EUMETSAT Data Tailor downloader, launched by the metop-*-download Dagster assets.
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

ENV PYTHONPATH=/app \
    HOME=/tmp
RUN mkdir -p /data/epct
VOLUME ["/data/epct"]

USER $MAMBA_USER

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/_entrypoint.sh", \
            "python", "-m", "planetary_datasets.providers.polar.epct_download"]
