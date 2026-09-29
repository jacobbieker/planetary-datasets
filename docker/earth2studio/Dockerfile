# earth2studio-backed downloaders, launched by the Dagster assets in dags/assets/radar.py
# (OPERA) and dags/assets/earth2studio_obs.py (the direct observation sources).
#
# Built from a staged context (see build.sh), not the repo root: the only project files
# the image needs are the two downloader modules, which import nothing else from
# planetary_datasets.
#
# No credentials are baked in. The public sources need none; the EUMETSAT ones read
# EUMETSAT_CONSUMER_KEY / EUMETSAT_CONSUMER_SECRET, which the asset passes at run time.
# Nothing here writes to a store: partitions are staged on the mounted volume and
# published from the project environment.
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
COPY run.sh /usr/local/bin/run_earth2studio

# The Dagster assets run the container as the host user so staged files are owned by that
# user, and that uid has no home directory here; earth2studio caches under $HOME.
#
# LD_LIBRARY_PATH puts conda's libstdc++ first. Otherwise a pip manylinux wheel loads
# the older system one, and conda's libproj then fails with "CXXABI_1.3.15 not found".
ENV PYTHONPATH=/app \
    LD_LIBRARY_PATH=/opt/conda/lib \
    HOME=/tmp \
    OPERA_ARCHIVE_DIR=/data/opera \
    E2S_OBS_ARCHIVE_DIR=/data/earth2studio
# The staging roots are always bind mounts, which keep the host directory's ownership, so
# the image directories need no write access of their own; nor does /app.
RUN mkdir -p /data/opera /data/earth2studio
VOLUME ["/data"]

# Root only for the install above. Dagster runs the container as the host user; run by
# hand, it falls back to the unprivileged image user rather than root.
USER $MAMBA_USER

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/_entrypoint.sh", "/usr/local/bin/run_earth2studio"]
