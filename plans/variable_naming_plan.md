# Readable, consistent variable names (and ERA5 units) across the stores

Status: **approved; being applied**.

Changes since review: float16 variables keep their source units (xarray decodes a scale
factor on float16 in float16: 1013 hPa x 100 overflows, 25.5 degC + 273.15 reads 298.8);
ICON/KENDA keep `values` and the IFS native stores keep all their dimension names (their
writers address them by name); the HAFS/HRRR/NAM virtual nests and `obs/sondehub_flights`
are left for a later pass (their writers compare against a schema of stored names, or live
outside the repository). Final: 658 stores, 12,472 renames, 405 unit conversions, 1,107
dimension renames.

## Scope

From a metadata scan of every store in the inventory (778 stores, 38,819 arrays):

| Group | Stores | Renamed? |
|---|---|---|
| Written by the box's lanes (incl. new generations, OSMC platforms, Himawari shards, UK/Norway radar) | 288 | yes |
| Static: nothing has committed to them in 14+ days | 416 | yes |
| Written by another host or Dagster (GOES/Himawari/GK2A virtual, `mtg_2000m`, `iodc_3000m_test`, `gmgsi/gmgsi_v3`, `haziel/*`, GloFAS, GPM, ...) | 192 | **no** — their writers would recreate the old names |

Re-sorting time axes is **not** part of this change.

## Coordinates and dimensions

One name per concept, dimensions renamed with their coordinate variables (1,458 dimension
renames in 560 stores, `planetary_datasets/naming/store_dims.json`):

| Concept | Standard | From |
|---|---|---|
| valid time | `time` | `t` (305 stores), `Time` |
| forecast reference / lead time | `init_time` / `step` | `forecast_reference_time`, `forecast_period` |
| horizontal | `latitude`, `longitude`; projection `x`, `y` | `lat`/`lon`, Met Office `projection_x_coordinate`, GMGSI `xc`/`yc`, WRF `west_east`/`south_north` |
| geostationary scan coordinates | `x_geostationary`, `y_geostationary` | kept in the satellite-consumer stores (metres along the scan); the `x`/`y` (radians) of the GOES/Himawari/GK2A virtual and CONUS/PACUS stores renamed to match |
| pressure levels | `pressure_level` | `level`/`pressure` (when in hPa/Pa), `isobaricInhPa`, `isobar`, `P_Level` |
| model levels | `model_level`, `model_half_level` | `hybrid`, `hybid`, `bottom_top`, `model_level_half`, `halflevel` |
| height | `height_above_ground`, `height_above_sea_level` | `height` (m), `hag`, `heightAboveGround`, `asl` |
| other | `ensemble_member`, `channel`, `cell` | `number`/`member`, `Channel`, ICON's `values` |

A rename that would land on a name the store already uses is skipped.

## Convention

ARCO-ERA5's: lower snake case, a single-level height leading (`2m_temperature`,
`10m_u_component_of_wind`, `1_5m_temperature`), fields on levels named for the quantity
(`temperature`, `specific_humidity`). ERA5's names are ECMWF parameter names in snake
case, so GRIB short names are named through ecCodes' WMO, ECMWF and DWD tables and the same
quantity reads the same from any model.

Rules, in order (`planetary_datasets/common/naming.py`):

1. Coordinates, grid mappings and bookkeeping are left alone.
2. Satellite channels keep their channel number:
   `channel_{number}_{reflectance|brightness_temperature}_{wavelength}um` (GOES `C13` ->
   `channel_13_brightness_temperature_10_3um`, SEVIRI `IR_108` ->
   `channel_09_brightness_temperature_10_8um`), from per-instrument tables (ABI, AHI, AMI,
   SEVIRI, FCI 1-16; AVHRR 1/2/3a/3b/4/5); microwave sounders as
   `channel_05_brightness_temperature`.
3. Hand overrides where generic rules disagree between stores (ICON `clch` ->
   `high_cloud_cover`, GEOS `NO2` -> `nitrogen_dioxide`, ...).
4. Station abbreviations (`atmp` -> `air_temperature`, `ghi` ->
   `global_horizontal_irradiance`, `bangle` -> `bending_angle`, ...).
5. The GRIB name, a CF `standard_name` with an ERA5 equivalent (only if it shares a word
   with the variable's own name: GEOS tags everything `northward_wind`), the long name or
   WRF `description` (if it is a name, not a paragraph), the ecCodes name of the short name.
6. Otherwise the source name tidied into snake case; failing that, kept and flagged.

Long names are shortened to ERA5's where one exists (ICON's time-averaged fluxes take ERA5's
mean-rate names, `aswdir_s` -> `mean_surface_direct_short_wave_radiation_flux`; `hzerocl`
-> `zero_degree_level`; `z0` -> `forecast_surface_roughness`), and a generated name over 45
characters loses to a source name that already reads well (`valid_pixel_count` stays).

Two variables a store would give one name are told apart by what their source names say
about their level (`_on_model_levels`, `_on_height_levels`, `_at_surface`, ...), then by
their tidied source name. Every renamed variable records its source name in `source_name`.

## Units

Where a variable's quantity has an ERA5 unit and its units differ (430 variables), the
conversion is written as CF `scale_factor`/`add_offset` plus ERA5 `units`, so **no stored
value changes** (virtual stores included) and `xarray.open_zarr` decodes in ERA5 units:

| From | To | How | Variables |
|---|---|---|---|
| Deg C / Celsius / C | K | `add_offset = 273.15` | 254 |
| hPa / mb | Pa | `scale_factor = 100` | 73 |
| m / gpm (heights) | m² s⁻² | `scale_factor = 9.80665`, renamed `geopotential` or `geopotential_at_surface` | 46 |
| % (cloud cover) | (0 - 1) | `scale_factor = 0.01` | 41 |
| kg m⁻² (precipitation) | m | `scale_factor = 0.001` | 16 |

Precision: the factors are stored as plain (float64) numbers, so xarray decodes them in
float64 whatever the stored dtype — tested: a float16 1013.25 hPa decodes to 101300.0 Pa,
a uint16 60000 times 100 to 6000000.0, with no overflow. A variable that already has a
scale factor gets the composition of both. Writing goes the other way (ERA5 units are
encoded back into the stored dtype): every such write is checked to round-trip exactly and
to stay inside the stored dtype's range (e.g. geopotential / g must fit float16's 65504),
and refused otherwise. Quantities whose meaning differs from ERA5's (radiation fluxes in
W m⁻² against ERA5's accumulated J m⁻²) are not converted.

## Code changes

- `common/naming.py`: the registry above; `store_names(prefix)` and the units table read the
  reviewed `planetary_datasets/naming/store_names.json` and `store_units.json`, so a store's
  names and units can never drift between its renaming and its later writes.
- Every write path renames and applies the units before writing: `BaseProvider`'s write
  (data converted into ERA5 units, xarray encodes back to the stored units, checked
  exact), `common/multidim.py` (EarthCARE, PACE, Aeolus), `pace_virtual` and the other
  virtual writers (names and attributes only), and the satellite-consumer lanes (a rename
  hook in that repository, for MTG/MSG/RSS/IODC/Himawari).
- `scripts/rename_store_variables.py`: per store, in one rearrange-session commit, moves
  each array to its new name, sets `source_name`, the units attributes and rewrites any
  `coordinates` attributes that name renamed variables. Metadata only; a store's rename is
  one commit and can be reverted.

## The outage

1. Stop every lane (cron `start` disabled, guard told not to restart, processes waited out).
2. Rename the 676 stores (metadata only: minutes).
3. Check every renamed store opens with `xr.open_zarr` and decodes; compare a sample of
   values against the old snapshot.
4. Deploy the new code, re-enable cron, restart all lanes.
5. Add the UK and Norway radar lanes; drop MetOp AVHRR and GOME to fewer concurrent
   EUMETSAT downloads (both lanes keep running).

## For review

- `planetary_datasets/naming/store_names.json` — every rename, per store.
- `planetary_datasets/naming/store_units.json` — every unit conversion, per store.
- `plans/variable_naming_review.md` — one table per store family with the rule each name
  came from; `kept` (2,340) and `clash` (937) rows deserve the closest look.
