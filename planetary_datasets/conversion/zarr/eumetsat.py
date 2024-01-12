"""Convert EUMETSAT raw imagery files to Zarr"""
try:
    import satip
except ImportError:
    print("Please install Satip to continue")

