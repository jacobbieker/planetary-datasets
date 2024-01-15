# planetary-datasets
Open source dataset loading and creation from Planetary Computer, GCP, and AWS. To support reproducible training of weather, energy, and mapping models.

This repository's goal is to make it easier to gather and use global and regional geospatial datasets for machine learning. It should provide
a fairly simple way of getting data, converting it to Xarray, combining it with other datasets, and saving it to disk for training and inference. 
A lot of the data sources are from the [Planetary Computer](https://planetarycomputer.microsoft.com/), but we also include data not available there from
Google Cloud and AWS.

## Installation

```bash
pip install planetary-datasets
```

## Usage

### Preprocessing data

To preprocess data (i.e. native to Zarr), one option is the Planetary Computer.
To do that, you need to install `kbatch` and then, after signing in, run the following:

```bash
kbatch job submit -f pc/eumetsat-0deg.yaml
```

This will create a job that downloads EUMETSAT 0-Deg imagery, convert to zarr, and upload to Hugging Face.
You will need to set the EUMETSAT API key and secret, and Hugging Face token for it to work.