# Planetary Datasets
Open source dataset loading and creation from Planetary Computer, GCP, and AWS. Built to support reproducible training of weather, energy, and mapping models.

This repository's goal is to make it easier to gather and use global and regional geospatial datasets for machine learning. It should provide
a fairly simple way of getting data, converting it to Xarray, combining it with other datasets, and saving it to disk for training and inference. 
A lot of the data sources are from the [Planetary Computer](https://planetarycomputer.microsoft.com/), but we also include data not available there from
Google Cloud and AWS, or converted from other sources and uploaded to Hugging Face.

## Examples

To get started, here are a few examples of using this library to fetch preprocessed datasets that are similar to ones used for training Google's MetNet regional forecasting model, and GraphCast global forecasting model.

## Installation

```bash
pip install planetary-datasets
```

## Usage

### Processing data

To preprocess data (i.e. native to Zarr), one option is the Planetary Computer.
To do that, you need to install `kbatch` and then, after signing in, run the following:

```bash
kbatch job submit -f pc/eumetsat-0deg.yaml
```

This will create a job that downloads EUMETSAT 0-Deg imagery, convert to zarr, and upload to Hugging Face.
You will need to set the EUMETSAT API key and secret, and Hugging Face token for it to work.

### Saving Processed data

### Using the datasets

## Citing

If you find this library useful, it would be great if you could cite the repo! There is the cite button on the side.

## License
