# EUMETNET OPERA radar downloader, launched by the `radar/opera_*_download` Dagster assets.
#
# Built from a staged context (see build.sh), not the repo root: the only project file
# the image needs is planetary_datasets/providers/opera_download.py.
#
# No credentials: the OPERA archive on CloudFerro is public, and the image only stages
# netCDF on the mounted volume. Writing the icechunk stores happens outside it.
FROM mambaorg/micromamba:2.0.5-debian12-slim

USER root
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

ENV MAMBA_DOCKERFILE_ACTIVATE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY environment.yml requirements.txt /tmp/
RUN micromamba install -y -n base -f /tmp/environment.yml \
    && micromamba clean --all --yes \
    && pip install --extra-index-url https://download.pytorch.org/whl/cpu -r /tmp/requirements.txt \
    && rm /tmp/environment.yml /tmp/requirements.txt

WORKDIR /app
COPY planetary_datasets ./planetary_datasets

# The Dagster asset runs the container as the host user so the staged files are owned by
# that user, and that uid has no home directory here; earth2studio caches under $HOME.
ENV PYTHONPATH=/app \
    HOME=/tmp \
    OPERA_ARCHIVE_DIR=/data/opera
RUN mkdir -p /data/opera && chmod -R a+rwX /data /app
VOLUME ["/data/opera"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/_entrypoint.sh", \
            "python", "-m", "planetary_datasets.providers.opera_download"]
