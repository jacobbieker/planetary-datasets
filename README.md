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

