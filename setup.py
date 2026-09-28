#!/usr/bin/env python

from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()
install_requires = (this_directory / "requirements.txt").read_text().splitlines()

setup(
    name="planetary_datasets",
    version="0.0.2",
    description="Datasets for ML models for planetary use-cases (e.g. solar mapping, land use, forecasting)",
    author="Jacob Bieker",
    author_email="jacob@bieker.tech",
    url="https://github.com/jacobbieker/planetary-datasets",
    install_requires=install_requires,
    long_description=long_description,
    long_description_content_type="text/markdown",
    # Scoped deliberately: a bare find_packages() also picks up dags/, which would put a
    # top-level "dags" module into every environment that installs this.
    packages=find_packages(include=["planetary_datasets", "planetary_datasets.*"]),
    python_requires=">=3.12",
)
