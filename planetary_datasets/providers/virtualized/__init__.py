"""Virtual-reference ingests for the geostationary imager archives.

These providers never copy pixel data. They read only the chunk layout of the
source netCDF/HDF5 files and record it in an Icechunk store as virtual chunk
references, so a multi-petabyte archive becomes an analysis-ready Zarr hierarchy
that costs kilobytes per scene to publish.

The heavy lifting — era detection, batched commits, codec validation, resume —
lives in :mod:`goes_radf_common`. Each satellite module supplies only the parts
that differ: bucket layout, filename grammar, preprocess and expected codecs.

:mod:`nsrdb` applies the same idea to NREL's NSRDB HDF5 archive, one store per dataset.
"""
