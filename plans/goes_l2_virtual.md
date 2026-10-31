# GOES ABI L2 as virtual Icechunk stores

Code: `planetary_datasets/providers/virtualized/goes_l2.py`. Lanes: `goes{16,17,18,19}_l2`
in `_backfill_box.sh`.

## Sources

- AWS `s3://noaa-goes{16,17,18,19}` (us-east-1, anonymous) and GCP
  `gs://gcp-public-data-goes-{16,17,18,19}` hold the same product prefixes, and every
  sampled hour (one per product and satellite, 320 in all) is byte-identical by name and
  size. GCP is therefore a mirror: references point at AWS only (the box is in AWS,
  anonymous S3 containers are already supported, and one source keeps the stores
  single-container). The GCP location is recorded in each store's `source` attribute.
- Every ABI L2 granule is HDF5 (netCDF-4) with row-band chunks and shuffle+zlib, which
  virtualizarr's HDF parser references directly; all 85 product prefixes parsed.
- Not ABI L2 and not covered: SUVI, EXIS, SEIS and MAG are published only as L1b; the
  `ABI-Flood-*` prefixes are derived day/hourly flood maps (including GeoTIFF and
  shapefiles).

## Layout

- One store per satellite, product and sector, `bkr/geo/virtualized/goes{NN}_l2/`:
  `acm_f.icechunk`, `acha2km_c.icechunk`, `mcmip_f.icechunk`, and CMIP per channel
  (`cmip_f_c13.icechunk`) like the L1b `goes{NN}_radf_C{nn}` stores. MCMIP keeps its 16
  channels in one store, as published.
- Image variables (on `y`/`x`, or `lat`/`lon` for DSRC/RSRC) are references; everything
  else (times, bounds, projection, per-file statistics) is loaded and stored inline, all
  with a leading `t`.
- A day is one commit and is recorded in the root attribute `days_written`. The last 7
  days go first, newest first, then the backlog oldest first, so `t` is not sorted.
- A change in layout (variables, grid, chunking, codecs) starts a generation named for
  the first day written in it (`acm_f_15062024.icechunk`); the layout's signature is kept
  in `layout_signature` and each day goes to the generation whose signature it matches.
- Names are NOAA's (`CMI`, `ACM`, `DQF`) with their `long_name` and units: like the other
  virtual stores, these are not renamed.
- Raw stored values are bit-identical to the source files. Decoded values of scaled
  integers come out in float64 where the files decode in float32, because zarr keeps
  `scale_factor` as a JSON (float64) number.

## Cost

- Manifests: about 2.6 KB per file (CONUS cloud mask) to 8 KB (full-disk CMIP channel),
  inline metadata included: about 0.5 TB of stores for the ~95M files in scope.
- Throughput measured from outside AWS: about 0.6 s per file per product worker (a
  288-file day in 177 s). With `--workers 4` per satellite lane, the four lanes reference
  about 25 files/s: roughly six weeks for the backlog. More workers shorten it; memory per
  worker is small (one day's manifests).

## Deferred, for a decision

- **Mesoscale sectors (M1/M2):** about 560M one-minute files, about 300 TB, whose grid
  moves with each sector. Virtualisable like the others, but the stores would hold most
  of the archive's references, and `x`/`y` would need storing per time.
- **Point products:** derived motion winds (`DMW*`, `DMWV*`) and GLM lightning
  (`GLM-L2-LCFA`) have a different length in every file, which references cannot append;
  they would need materialising, as the WIS2 observation targets do.

## Volumes

Estimated from one complete recent day per product and satellite, times the span of days
the archive covers.

| Product | Sector | Satellites (first–last day) | Files/day (per sat) | Median MB | Files (all sats) | TB | In scope |
|---|---|---|---|---|---|---|---|
| ABI-L2-ACHA2KMC | CONUS/PACUS | 16: 2023-03-23–2025-04-07<br>18: 2023-03-27–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 4.61 | 808,992 | 4.0 | yes |
| ABI-L2-ACHA2KMF | full disk | 16: 2023-03-23–2025-04-07<br>18: 2023-03-27–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 30.66 | 404,496 | 12.6 | yes |
| ABI-L2-ACHA2KMM | mesoscale | 16: 2023-03-23–2025-04-07<br>18: 2023-03-27–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.44 | 8,083,944 | 3.4 | deferred (mesoscale) |
| ABI-L2-ACHAC | CONUS/PACUS | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 0.33 | 1,575,936 | 0.5 | yes |
| ABI-L2-ACHAF | full disk | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 1.61 | 787,968 | 1.2 | yes |
| ABI-L2-ACHAM | mesoscale | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.2 | 15,743,728 | 3.0 | deferred (mesoscale) |
| ABI-L2-ACHP2KMC | CONUS/PACUS | 16: 2023-03-23–2025-04-07<br>18: 2023-03-27–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 4.63 | 808,992 | 4.0 | yes |
| ABI-L2-ACHP2KMF | full disk | 16: 2023-03-23–2025-04-07<br>18: 2023-03-27–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 31.11 | 404,496 | 12.8 | yes |
| ABI-L2-ACHP2KMM | mesoscale | 16: 2023-03-23–2025-04-07<br>18: 2023-03-27–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.43 | 8,083,944 | 3.4 | deferred (mesoscale) |
| ABI-L2-ACHTF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 30.29 | 787,104 | 23.2 | yes |
| ABI-L2-ACHTM | mesoscale | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.43 | 15,726,472 | 6.6 | deferred (mesoscale) |
| ABI-L2-ACMC | CONUS/PACUS | 16: 2017-04-19–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 4.3 | 1,984,608 | 8.2 | yes |
| ABI-L2-ACMF | full disk | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 25.85 | 787,968 | 19.0 | yes |
| ABI-L2-ACMM | mesoscale | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.34 | 15,743,728 | 5.3 | deferred (mesoscale) |
| ABI-L2-ACTPC | CONUS/PACUS | 16: 2017-05-16–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 0.65 | 1,976,832 | 1.2 | yes |
| ABI-L2-ACTPF | full disk | 16: 2017-05-16–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 3.52 | 988,416 | 3.5 | yes |
| ABI-L2-ACTPM | mesoscale | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.1 | 15,743,728 | 1.5 | deferred (mesoscale) |
| ABI-L2-ADPC | CONUS/PACUS | 16: 2019-12-03–2025-04-07<br>17: 2019-12-03–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 0.64 | 787,680 | 0.4 | yes |
| ABI-L2-ADPF | full disk | 16: 2019-12-03–2025-04-07<br>17: 2019-12-03–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 3.99 | 787,680 | 2.6 | yes |
| ABI-L2-ADPM | mesoscale | 16: 2019-12-03–2025-04-07<br>17: 2019-12-03–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 576 | 0.13 | 3,148,767 | 0.4 | deferred (mesoscale) |
| ABI-L2-AICEF | full disk | 16: 2021-02-24–2025-04-07<br>17: 2021-02-25–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 8 | 6.45 | 36,568 | 0.2 | yes |
| ABI-L2-AITAF | full disk | 16: 2021-02-24–2025-04-07<br>17: 2021-02-25–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 8 | 3.47 | 36,568 | 0.2 | yes |
| ABI-L2-AODC | CONUS/PACUS | 16: 2017-05-24–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 2.38 | 1,974,528 | 7.1 | yes |
| ABI-L2-AODF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 15.29 | 787,104 | 14.4 | yes |
| ABI-L2-BRFC | CONUS/PACUS | 16: 2021-08-18–2025-04-07<br>17: 2021-08-18–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 0.33 | 1,215,936 | 3.1 | yes |
| ABI-L2-BRFF | full disk | 16: 2021-08-18–2025-04-07<br>17: 2021-08-18–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 3.59 | 607,968 | 5.6 | yes |
| ABI-L2-BRFM | mesoscale | 16: 2021-08-18–2025-04-07<br>17: 2021-08-18–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.16 | 12,148,728 | 5.1 | deferred (mesoscale) |
| ABI-L2-CCLC | CONUS/PACUS | 16: 2023-05-17–2025-04-07<br>18: 2023-05-17–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 0.46 | 778,464 | 0.4 | yes |
| ABI-L2-CCLF | full disk | 16: 2023-05-17–2025-04-07<br>18: 2023-05-17–2026-10-31<br>19: 2024-10-15–2026-10-31 | 24 | 2.16 | 64,872 | 0.1 | yes |
| ABI-L2-CCLM | mesoscale | 16: 2023-05-17–2025-04-07<br>18: 2023-05-17–2026-10-31<br>19: 2024-10-15–2026-10-31 | 576 | 0.55 | 1,556,236 | 0.7 | deferred (mesoscale) |
| ABI-L2-CMIPC | CONUS/PACUS | 16: 2017-02-28–2025-04-07<br>17: 2018-08-28–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-10–2026-10-31 | 4608 | 3.83 | 32,002,560 | 207.3 | yes |
| ABI-L2-CMIPF | full disk | 16: 2017-02-28–2025-04-07<br>17: 2018-08-28–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-10–2026-10-31 | 2304 | 23.89 | 16,001,280 | 692.8 | yes |
| ABI-L2-CMIPM | mesoscale | 16: 2017-02-28–2025-04-07<br>17: 2018-08-28–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-18–2026-10-31 | 46080 | 0.32 | 319,325,328 | 159.5 | deferred (mesoscale) |
| ABI-L2-COD2KMF | full disk | 16: 2023-03-23–2025-04-07<br>18: 2023-03-27–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 21.84 | 404,496 | 9.3 | yes |
| ABI-L2-CODC | CONUS/PACUS | 16: 2017-06-08–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 4.83 | 1,970,208 | 7.8 | yes |
| ABI-L2-CODF | full disk | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 6.58 | 787,968 | 5.0 | yes |
| ABI-L2-CPSC | CONUS/PACUS | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 3.7 | 1,574,208 | 5.9 | yes |
| ABI-L2-CPSF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 21.57 | 787,104 | 17.1 | yes |
| ABI-L2-CPSM | mesoscale | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.32 | 15,726,472 | 5.6 | deferred (mesoscale) |
| ABI-L2-CTPC | CONUS/PACUS | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 0.33 | 1,574,208 | 0.5 | yes |
| ABI-L2-CTPF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 1.65 | 787,104 | 1.3 | yes |
| ABI-L2-DMWC | CONUS/PACUS | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 576 | 0.17 | 3,151,872 | 2.1 | deferred (point data) |
| ABI-L2-DMWF | full disk | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 0.49 | 787,968 | 3.1 | deferred (point data) |
| ABI-L2-DMWM | mesoscale | 16: 2019-12-02–2025-04-07<br>17: 2019-12-02–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 3456 | 0.13 | 18,598,556 | 2.5 | deferred (point data) |
| ABI-L2-DMWVC | CONUS/PACUS | 16: 2021-04-12–2025-04-07<br>17: 2021-04-12–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 96 | 0.14 | 429,888 | 0.1 | deferred (point data) |
| ABI-L2-DMWVF | full disk | 16: 2021-04-12–2025-04-07<br>17: 2021-04-12–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 24 | 0.23 | 107,472 | 0.2 | deferred (point data) |
| ABI-L2-DMWVM | mesoscale | 16: 2021-04-12–2025-04-07<br>17: 2021-04-12–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 576 | 0.13 | 2,482,821 | 0.3 | deferred (point data) |
| ABI-L2-DSIC | CONUS/PACUS | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 0.82 | 1,574,208 | 1.2 | yes |
| ABI-L2-DSIF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 3.54 | 787,104 | 2.8 | yes |
| ABI-L2-DSIM | mesoscale | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.27 | 15,726,472 | 3.2 | deferred (mesoscale) |
| ABI-L2-DSRC | CONUS/PACUS | 16: 2019-12-05–2024-04-17<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2024-04-17 | 24 | 0.11 | 82,488 | 0.0 | yes |
| ABI-L2-DSRF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 19.43 | 651,144 | 12.0 | yes |
| ABI-L2-DSRM | mesoscale | 16: 2019-12-05–2024-04-17<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2024-04-17 | 48 | 0.11 | 164,976 | 0.0 | deferred (mesoscale) |
| ABI-L2-FDCC | CONUS/PACUS | 16: 2017-05-24–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-17–2026-10-31 | 288 | 0.44 | 1,973,952 | 0.8 | yes |
| ABI-L2-FDCF | full disk | 16: 2017-05-24–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-17–2026-10-31 | 144 | 1.85 | 986,976 | 1.8 | yes |
| ABI-L2-FDCM | mesoscale | 16: 2021-05-17–2025-04-07<br>17: 2021-05-17–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.15 | 12,683,664 | 1.9 | deferred (mesoscale) |
| ABI-L2-FSCC | CONUS/PACUS | 16: 2022-12-12–2025-04-07<br>18: 2022-12-12–2026-10-31<br>19: 2024-10-15–2026-10-31 | 24 | 0.11 | 72,360 | 0.0 | yes |
| ABI-L2-FSCF | full disk | 16: 2022-12-12–2025-04-07<br>18: 2022-12-12–2026-10-31<br>19: 2024-10-15–2026-10-31 | 24 | 0.52 | 72,360 | 0.0 | yes |
| ABI-L2-FSCM | mesoscale | 16: 2022-12-12–2025-04-07<br>18: 2022-12-12–2026-10-31<br>19: 2024-10-15–2026-10-31 | 48 | 0.08 | 144,720 | 0.0 | deferred (mesoscale) |
| ABI-L2-LSAC | CONUS/PACUS | 16: 2021-08-18–2025-04-07<br>17: 2021-08-18–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 0.15 | 1,215,936 | 0.8 | yes |
| ABI-L2-LSAF | full disk | 16: 2021-08-18–2025-04-07<br>17: 2021-08-18–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 0.98 | 607,968 | 1.3 | yes |
| ABI-L2-LSAM | mesoscale | 16: 2021-08-18–2025-04-07<br>17: 2021-08-18–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.08 | 12,148,728 | 1.6 | deferred (mesoscale) |
| ABI-L2-LST2KMF | full disk | 16: 2021-10-13–2025-04-07<br>17: 2021-10-13–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 24 | 8.77 | 98,640 | 0.8 | yes |
| ABI-L2-LSTC | CONUS/PACUS | 16: 2017-05-24–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 24 | 2.24 | 164,544 | 0.3 | yes |
| ABI-L2-LSTF | full disk | 16: 2017-05-24–2025-04-07<br>17: 2018-08-27–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 24 | 0.61 | 164,544 | 0.1 | yes |
| ABI-L2-LSTM | mesoscale | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 48 | 0.2 | 262,368 | 0.1 | deferred (mesoscale) |
| ABI-L2-LVMPC | CONUS/PACUS | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 10.72 | 1,574,208 | 21.3 | yes |
| ABI-L2-LVMPF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 47.77 | 787,104 | 66.8 | yes |
| ABI-L2-LVMPM | mesoscale | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.97 | 15,726,472 | 12.6 | deferred (mesoscale) |
| ABI-L2-LVTPC | CONUS/PACUS | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 10.8 | 1,574,208 | 21.1 | yes |
| ABI-L2-LVTPF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 46.69 | 787,104 | 66.9 | yes |
| ABI-L2-LVTPM | mesoscale | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.94 | 15,726,472 | 12.5 | deferred (mesoscale) |
| ABI-L2-MCMIPC | CONUS/PACUS | 16: 2017-02-28–2025-04-07<br>17: 2018-08-28–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-10–2026-10-31 | 288 | 44.95 | 2,000,160 | 94.8 | yes |
| ABI-L2-MCMIPF | full disk | 16: 2017-02-28–2025-04-07<br>17: 2018-08-28–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-10–2026-10-31 | 144 | 305.47 | 1,000,080 | 311.9 | yes |
| ABI-L2-MCMIPM | mesoscale | 16: 2017-02-28–2025-04-07<br>17: 2018-08-28–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-17–2026-10-31 | 2880 | 3.48 | 19,960,713 | 73.7 | deferred (mesoscale) |
| ABI-L2-PARF | full disk | 18: 2025-04-29–2026-10-31<br>19: 2025-05-12–2026-10-31 | 144 | 19.04 | 156,816 | 3.0 | yes |
| ABI-L2-RRQPEF | full disk | 16: 2019-12-03–2025-04-07<br>17: 2019-12-03–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 1.53 | 787,680 | 1.2 | yes |
| ABI-L2-RSRC | CONUS/PACUS | 16: 2019-12-05–2024-04-17<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2024-04-17 | 24 | 0.11 | 82,488 | 0.0 | yes |
| ABI-L2-RSRF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 18.84 | 651,144 | 11.8 | yes |
| ABI-L2-SSTF | full disk | 16: 2019-12-05–2025-04-07<br>17: 2019-12-05–2023-01-10<br>18: 2022-05-11–2026-10-30<br>19: 2024-10-15–2026-10-30 | 24 | 32.38 | 131,136 | 4.0 | yes |
| ABI-L2-TPWC | CONUS/PACUS | 16: 2019-12-03–2025-04-07<br>17: 2019-12-03–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 288 | 0.26 | 1,575,360 | 0.4 | yes |
| ABI-L2-TPWF | full disk | 16: 2019-12-03–2025-04-07<br>17: 2019-12-03–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 144 | 1.05 | 787,680 | 0.8 | yes |
| ABI-L2-TPWM | mesoscale | 16: 2019-12-03–2025-04-07<br>17: 2019-12-03–2023-01-10<br>18: 2022-05-11–2026-10-31<br>19: 2024-10-15–2026-10-31 | 2880 | 0.14 | 15,737,976 | 1.7 | deferred (mesoscale) |
| ABI-L2-VAAF | full disk | 16: 2019-12-05–2020-12-02<br>17: 2019-12-05–2020-12-02 | 144 | 44.17 | 104,468 | 4.1 | yes |
| GLM-L2-LCFA | — | 16: 2018-02-13–2025-04-07<br>17: 2018-08-22–2023-01-10<br>18: 2022-09-19–2026-10-31<br>19: 2025-01-15–2026-10-31 | 4320 | 0.33 | 27,521,026 | 7.6 | deferred (point data) |

In scope: 94,734,172 files, 1,701 TB referenced.