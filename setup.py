#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="planetary_datasets",
    version="0.0.1",
    description="Datasets for ML models for planetary use-cases (e.g. solar mapping, land use, forecasting)",
    author="Jacob Bieker",
    author_email="jacob@bieker.tech",
    url="https://github.com/jacobbieker/planetary-datasets",
    install_requires=["odc-stac", "pystac-client", "planetary-computer", "rioxarray", "fsspec", "geopandas", "dask"],
    packages=find_packages(),
)
