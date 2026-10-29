# Variable naming review

676 stores renamed, 14503 variables. One table per store family (its first store); `kept` and `clash` rows need a look.

Rules: coordinate 10468, tidied 10164, long_name 5792, kept 2340, eccodes 2152, grib 1242, channel 944, cf 905, clash 885, abbreviation 790, override 424, abbreviation+units 217, cf+units 73, grib+units 42, long_name+units 24, cf+geopotential+units 23, override+units 14, tidied+units 14, long_name+geopotential+units 12, grib+geopotential+units 11, override+geopotential+units 2

## bkr/aoml/aoml (14 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units | air temperature | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation | cloud cover | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units | dew point temperature | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation | relative humidity |  |
| `precip` | `precipitation` | abbreviation | precipitation | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation | surface downwelling longwave flux in air | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation | surface downwelling shortwave flux in air | W m-2 |
| `sea_water_elec_conductivity` | `sea_water_electrical_conductivity` | long_name | sea water electrical conductivity | S m-1 |
| `slp` | `sea_level_pressure` | abbreviation+units | sea level pressure | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation | sea surface salinity | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units | sea surface temperature | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation | eastward sea water velocity | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation | northward sea water velocity | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation | water column height | m |
| `waterlevel_met_res` | `meteorological_residual_tidal_elevation` | long_name | meteorological residual tidal elevation | m |
| `waterlevel_wrt_lcd` | `tidal_elevation_wrt_local_chart_datum` | long_name | tidal elevation WRT local chart datum | m |
| `winddir` | `wind_direction` | abbreviation | wind from direction | Deg true |
| `windspd` | `wind_speed` | abbreviation | wind speed | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation | upward sea water velocity | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation | sea surface wave significant height | m |
| `zsal` | `sea_water_salinity` | abbreviation | profile salinity |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units | profile water temperature | Deg C -> K |

## bkr/aoml/aoml_all (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_bouys (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units | air temperature | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation | cloud cover | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units | dew point temperature | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation | relative humidity |  |
| `precip` | `precipitation` | abbreviation | precipitation | mm |
| `sea_water_elec_conductivity` | `sea_water_electrical_conductivity` | long_name | sea water electrical conductivity | S m-1 |
| `slp` | `sea_level_pressure` | abbreviation+units | sea level pressure | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation | sea surface salinity | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units | sea surface temperature | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation | eastward sea water velocity | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation | northward sea water velocity | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation | water column height | m |
| `waterlevel_met_res` | `meteorological_residual_tidal_elevation` | long_name | meteorological residual tidal elevation | m |
| `waterlevel_wrt_lcd` | `tidal_elevation_wrt_local_chart_datum` | long_name | tidal elevation WRT local chart datum | m |
| `winddir` | `wind_direction` | abbreviation | wind from direction | Deg true |
| `windspd` | `wind_speed` | abbreviation | wind speed | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation | upward sea water velocity | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation | sea surface wave significant height | m |
| `zsal` | `sea_water_salinity` | abbreviation | profile salinity |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units | profile water temperature | Deg C -> K |

## bkr/aoml/aoml_drifters (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_glider (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_moored (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_ships (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_shore (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_station (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_tide (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_volunteerships (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_vosclim (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/aoml_weather (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `temperature` | cf+units | air temperature | Deg C -> K |
| `dew_point_temperature` | `dewpoint_temperature` | cf+units | dew point temperature | Deg C -> K |
| `sea_level_pressure` | `mean_sea_level_pressure` | cf+units | sea level pressure | hPa -> Pa |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | sea surface wave significant height | m |
| `wind_from_direction` | `wind_direction` | cf | wind from direction | Deg true |

## bkr/aoml/v2/osmc_all (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_c_man_weather_stations (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_climate_reference_moored_buoys (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_drifting_buoys (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_gliders (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_gloss (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_hurricane_drifters (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_ice_buoys (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_moored_buoys (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_ocean_transport_stations (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_profiling_floats_and_gliders (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_research (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_ship_fishing_vessel (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_ships (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_shore_and_bottom_stations (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_tagged_animal (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_tide_gauge_stations (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_tropical_moored_buoys (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_tsunami_warning_stations (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_uncrewed_surface_vehicle (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_underway_carbon_ships (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_unknown (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_volunteer_observing_ships (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_vosclim (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_weather_and_ocean_obs (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_weather_buoys (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/aoml/v2/osmc_weather_obs (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `atmp` | `air_temperature` | abbreviation+units |  | Deg C -> K |
| `clouds` | `cloud_cover` | abbreviation |  | oktas |
| `dewpoint` | `dewpoint_temperature` | abbreviation+units |  | Deg C -> K |
| `hur` | `relative_humidity` | abbreviation |  |  |
| `precip` | `precipitation` | abbreviation |  | mm |
| `rlds` | `surface_downwelling_longwave_flux` | abbreviation |  | W m-2 |
| `rsds` | `surface_downwelling_shortwave_flux` | abbreviation |  | W m-2 |
| `slp` | `sea_level_pressure` | abbreviation+units |  | hPa -> Pa |
| `sss` | `sea_surface_salinity` | abbreviation |  | 1 |
| `sst` | `sea_surface_temperature` | abbreviation+units |  | Deg C -> K |
| `uo` | `eastward_sea_water_velocity` | abbreviation |  | m s-1 |
| `vo` | `northward_sea_water_velocity` | abbreviation |  | m s-1 |
| `water_col_ht` | `water_column_height` | abbreviation |  | m |
| `winddir` | `wind_direction` | abbreviation |  | Deg true |
| `windspd` | `wind_speed` | abbreviation |  | m/s |
| `wo` | `upward_sea_water_velocity` | abbreviation |  | m s-1 |
| `wvht` | `significant_wave_height` | abbreviation |  | m |
| `zsal` | `sea_water_salinity` | abbreviation |  |  |
| `ztmp` | `sea_water_temperature` | abbreviation+units |  | Deg C -> K |

## bkr/cams/cams (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `aerosol_backscatter_coefficient_at_1064_nm_from_ground` | `aerosol_backscatter_coefficient_at_1064_nm_from_ground` | clash | Aerosol backscatter coefficient at 1064 nm (from ground) | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_1064_nm_from_top_of_atmosphere` | `aerosol_backscatter_coefficient_at_1064_nm_from_top_of_atmosphere` | clash | Aerosol backscatter coefficient at 1064 nm (from top of atmo | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_355_nm_from_ground` | `aerosol_backscatter_coefficient_at_355_nm_from_ground` | clash | Aerosol backscatter coefficient at 355 nm (from ground) | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_355_nm_from_top_of_atmosphere` | `aerosol_backscatter_coefficient_at_355_nm_from_top_of_atmosphere` | clash | Aerosol backscatter coefficient at 355 nm (from top of atmos | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_532_nm_from_ground` | `aerosol_backscatter_coefficient_at_532_nm_from_ground` | clash | Aerosol backscatter coefficient at 532 nm (from ground) | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_532_nm_from_top_of_atmosphere` | `aerosol_backscatter_coefficient_at_532_nm_from_top_of_atmosphere` | clash | Aerosol backscatter coefficient at 532 nm (from top of atmos | m**-1 sr**-1 |
| `dust_aerosol_0.03_to_0.55_um_mixing_ratio` | `dust_aerosol_0_03_to_0_55_um_mixing_ratio` | clash | Dust Aerosol (0.03 - 0.55 um) Mixing Ratio | kg kg**-1 |
| `dust_aerosol_0.03_to_0.55_um_optical_depth` | `dust_aerosol_0_03_to_0_55_um_optical_depth` | clash | Dust aerosol (0.03 - 0.55 um) optical depth | ~ |
| `dust_aerosol_0.55_to_0.9_um_mixing_ratio` | `dust_aerosol_0_55_to_0_9_um_mixing_ratio` | clash | Dust Aerosol (0.55 - 0.9 um) Mixing Ratio | kg kg**-1 |
| `dust_aerosol_0.55_to_0.9_um_optical_depth` | `dust_aerosol_0_55_to_0_9_um_optical_depth` | clash | Dust aerosol (0.55 - 0.9 um) optical depth | ~ |
| `dust_aerosol_0.9_to_20_um_mixing_ratio` | `dust_aerosol_0_9_to_20_um_mixing_ratio` | clash | Dust Aerosol (0.9 - 20 um) Mixing Ratio | kg kg**-1 |
| `dust_aerosol_0.9_to_20_um_optical_depth` | `dust_aerosol_0_9_to_20_um_optical_depth` | clash | Dust aerosol (0.9 - 20 um) optical depth | ~ |
| `sea_salt_aerosol_0.03_to_0.5_um_optical_depth` | `sea_salt_aerosol_0_03_to_0_5_um_optical_depth` | clash | Sea salt aerosol (0.03 - 0.5 um) optical depth | ~ |
| `sea_salt_aerosol_0.5_to_5_um_optical_depth` | `sea_salt_aerosol_0_5_to_5_um_optical_depth` | clash | Sea salt aerosol (0.5 - 5 um) optical depth | ~ |
| `sea_salt_aerosol_5_to_20_um_optical_depth` | `sea_salt_aerosol_5_to_20_um_optical_depth` | clash | Sea salt aerosol (5 - 20 um) optical depth | ~ |
| `surface_direct_normal_short_wave_solar_radiation` | `surface_direct_normal_short_wave_radiation` | grib | Surface direct normal short-wave (solar) radiation | J m**-2 |
| `surface_direct_short_wave_solar_radiation` | `surface_direct_short_wave_radiation` | grib | Surface direct short-wave (solar) radiation | J m**-2 |
| `surface_net_long_wave_thermal_radiation` | `surface_net_long_wave_radiation` | grib | Surface net long-wave (thermal) radiation | J m**-2 |
| `surface_net_short_wave_solar_radiation` | `surface_net_short_wave_radiation` | grib | Surface net short-wave (solar) radiation | J m**-2 |
| `total_column__ethane` | `total_column_ethane` | grib | Total column  ethane | kg m**-2 |
| `total_column__isoprene` | `total_column_isoprene` | grib | Total column  isoprene | kg m**-2 |
| `total_column__peroxyacetyl_nitrate` | `total_column_peroxyacetyl_nitrate` | grib | Total column  peroxyacetyl nitrate | kg m**-2 |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_1020_nm` | `total_fine_mode_aerosol_optical_depth_at_1020_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 1020 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_1064_nm` | `total_fine_mode_aerosol_optical_depth_at_1064_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 1064 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_1240_nm` | `total_fine_mode_aerosol_optical_depth_at_1240_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 1240 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_1640_nm` | `total_fine_mode_aerosol_optical_depth_at_1640_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 1640 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_2130_nm` | `total_fine_mode_aerosol_optical_depth_at_2130_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 2130 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_340_nm` | `total_fine_mode_aerosol_optical_depth_at_340_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 340 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_355_nm` | `total_fine_mode_aerosol_optical_depth_at_355_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 355 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_380_nm` | `total_fine_mode_aerosol_optical_depth_at_380_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 380 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_400_nm` | `total_fine_mode_aerosol_optical_depth_at_400_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 400 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_440_nm` | `total_fine_mode_aerosol_optical_depth_at_440_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 440 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_469_nm` | `total_fine_mode_aerosol_optical_depth_at_469_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 469 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_500_nm` | `total_fine_mode_aerosol_optical_depth_at_500_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 500 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_532_nm` | `total_fine_mode_aerosol_optical_depth_at_532_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 532 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_550_nm` | `total_fine_mode_aerosol_optical_depth_at_550_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 550 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_645_nm` | `total_fine_mode_aerosol_optical_depth_at_645_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 645 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_670_nm` | `total_fine_mode_aerosol_optical_depth_at_670_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 670 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_800_nm` | `total_fine_mode_aerosol_optical_depth_at_800_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 800 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_858_nm` | `total_fine_mode_aerosol_optical_depth_at_858_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 858 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_865_nm` | `total_fine_mode_aerosol_optical_depth_at_865_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 865 nm | ~ |

## bkr/cams/cams_analysis_and_forecast (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `aerosol_backscatter_coefficient_at_1064_nm_from_ground` | `aerosol_backscatter_coefficient_at_1064_nm_from_ground` | clash | Aerosol backscatter coefficient at 1064 nm (from ground) | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_1064_nm_from_top_of_atmosphere` | `aerosol_backscatter_coefficient_at_1064_nm_from_top_of_atmosphere` | clash | Aerosol backscatter coefficient at 1064 nm (from top of atmo | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_355_nm_from_ground` | `aerosol_backscatter_coefficient_at_355_nm_from_ground` | clash | Aerosol backscatter coefficient at 355 nm (from ground) | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_355_nm_from_top_of_atmosphere` | `aerosol_backscatter_coefficient_at_355_nm_from_top_of_atmosphere` | clash | Aerosol backscatter coefficient at 355 nm (from top of atmos | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_532_nm_from_ground` | `aerosol_backscatter_coefficient_at_532_nm_from_ground` | clash | Aerosol backscatter coefficient at 532 nm (from ground) | m**-1 sr**-1 |
| `aerosol_backscatter_coefficient_at_532_nm_from_top_of_atmosphere` | `aerosol_backscatter_coefficient_at_532_nm_from_top_of_atmosphere` | clash | Aerosol backscatter coefficient at 532 nm (from top of atmos | m**-1 sr**-1 |
| `dust_aerosol_0.03_to_0.55_um_mixing_ratio` | `dust_aerosol_0_03_to_0_55_um_mixing_ratio` | clash | Dust Aerosol (0.03 - 0.55 um) Mixing Ratio | kg kg**-1 |
| `dust_aerosol_0.03_to_0.55_um_optical_depth` | `dust_aerosol_0_03_to_0_55_um_optical_depth` | clash | Dust aerosol (0.03 - 0.55 um) optical depth | ~ |
| `dust_aerosol_0.55_to_0.9_um_mixing_ratio` | `dust_aerosol_0_55_to_0_9_um_mixing_ratio` | clash | Dust Aerosol (0.55 - 0.9 um) Mixing Ratio | kg kg**-1 |
| `dust_aerosol_0.55_to_0.9_um_optical_depth` | `dust_aerosol_0_55_to_0_9_um_optical_depth` | clash | Dust aerosol (0.55 - 0.9 um) optical depth | ~ |
| `dust_aerosol_0.9_to_20_um_mixing_ratio` | `dust_aerosol_0_9_to_20_um_mixing_ratio` | clash | Dust Aerosol (0.9 - 20 um) Mixing Ratio | kg kg**-1 |
| `dust_aerosol_0.9_to_20_um_optical_depth` | `dust_aerosol_0_9_to_20_um_optical_depth` | clash | Dust aerosol (0.9 - 20 um) optical depth | ~ |
| `sea_salt_aerosol_0.03_to_0.5_um_optical_depth` | `sea_salt_aerosol_0_03_to_0_5_um_optical_depth` | clash | Sea salt aerosol (0.03 - 0.5 um) optical depth | ~ |
| `sea_salt_aerosol_0.5_to_5_um_optical_depth` | `sea_salt_aerosol_0_5_to_5_um_optical_depth` | clash | Sea salt aerosol (0.5 - 5 um) optical depth | ~ |
| `sea_salt_aerosol_5_to_20_um_optical_depth` | `sea_salt_aerosol_5_to_20_um_optical_depth` | clash | Sea salt aerosol (5 - 20 um) optical depth | ~ |
| `surface_direct_normal_short_wave_solar_radiation` | `surface_direct_normal_short_wave_radiation` | grib | Surface direct normal short-wave (solar) radiation | J m**-2 |
| `surface_direct_short_wave_solar_radiation` | `surface_direct_short_wave_radiation` | grib | Surface direct short-wave (solar) radiation | J m**-2 |
| `surface_net_long_wave_thermal_radiation` | `surface_net_long_wave_radiation` | grib | Surface net long-wave (thermal) radiation | J m**-2 |
| `surface_net_short_wave_solar_radiation` | `surface_net_short_wave_radiation` | grib | Surface net short-wave (solar) radiation | J m**-2 |
| `total_column__ethane` | `total_column_ethane` | grib | Total column  ethane | kg m**-2 |
| `total_column__isoprene` | `total_column_isoprene` | grib | Total column  isoprene | kg m**-2 |
| `total_column__peroxyacetyl_nitrate` | `total_column_peroxyacetyl_nitrate` | grib | Total column  peroxyacetyl nitrate | kg m**-2 |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_1020_nm` | `total_fine_mode_aerosol_optical_depth_at_1020_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 1020 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_1064_nm` | `total_fine_mode_aerosol_optical_depth_at_1064_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 1064 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_1240_nm` | `total_fine_mode_aerosol_optical_depth_at_1240_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 1240 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_1640_nm` | `total_fine_mode_aerosol_optical_depth_at_1640_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 1640 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_2130_nm` | `total_fine_mode_aerosol_optical_depth_at_2130_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 2130 n | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_340_nm` | `total_fine_mode_aerosol_optical_depth_at_340_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 340 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_355_nm` | `total_fine_mode_aerosol_optical_depth_at_355_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 355 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_380_nm` | `total_fine_mode_aerosol_optical_depth_at_380_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 380 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_400_nm` | `total_fine_mode_aerosol_optical_depth_at_400_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 400 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_440_nm` | `total_fine_mode_aerosol_optical_depth_at_440_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 440 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_469_nm` | `total_fine_mode_aerosol_optical_depth_at_469_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 469 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_500_nm` | `total_fine_mode_aerosol_optical_depth_at_500_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 500 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_532_nm` | `total_fine_mode_aerosol_optical_depth_at_532_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 532 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_550_nm` | `total_fine_mode_aerosol_optical_depth_at_550_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 550 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_645_nm` | `total_fine_mode_aerosol_optical_depth_at_645_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 645 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_670_nm` | `total_fine_mode_aerosol_optical_depth_at_670_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 670 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_800_nm` | `total_fine_mode_aerosol_optical_depth_at_800_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 800 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_858_nm` | `total_fine_mode_aerosol_optical_depth_at_858_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 858 nm | ~ |
| `total_fine_mode_r_<_0.5_um_aerosol_optical_depth_at_865_nm` | `total_fine_mode_aerosol_optical_depth_at_865_nm` | grib | Total fine mode (r < 0.5 um) aerosol optical depth at 865 nm | ~ |

## bkr/dmi/alaska_hrrr (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `10_metre_u_wind_component_at_10.0m` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_10.0m` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `2_metre_dewpoint_temperature_at_2.0m` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_2.0m` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_2.0m` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_2.0m` | `2m_temperature` | grib | 2 metre temperature | K |
| `baseflow-groundwater_runoff_at_surface` | `baseflow_groundwater_runoff` | grib | Baseflow-groundwater runoff | kg m**-2 |
| `boundary_layer_height_at_surface` | `boundary_layer_height` | grib | Boundary layer height | m |
| `categorical_freezing_rain_at_surface` | `categorical_freezing_rain` | grib | Categorical freezing rain | (Code table 4.222) |
| `categorical_ice_pellets_at_surface` | `categorical_ice_pellets` | grib | Categorical ice pellets | (Code table 4.222) |
| `categorical_rain_at_surface` | `categorical_rain` | grib | Categorical rain | (Code table 4.222) |
| `categorical_snow_at_surface` | `categorical_snow` | grib | Categorical snow | (Code table 4.222) |
| `cloud_forcing_net_solar_flux_at_surface` | `cloud_forcing_net_solar_flux` | grib | Cloud Forcing Net Solar Flux | W m**-2 |
| `convective_available_potential_energy_at_surface` | `convective_available_potential_energy` | grib | Convective available potential energy | J kg**-1 |
| `convective_inhibition_at_surface` | `convective_inhibition` | grib | Convective inhibition | J kg**-1 |
| `dew_point_temperature` | `dewpoint_temperature` | grib | Dew point temperature | K |
| `forecast_surface_roughness_at_surface` | `forecast_surface_roughness` | grib | Forecast surface roughness | m |
| `freezing_rain_at_surface` | `freezing_rain` | grib | Freezing Rain | kg m**-2 |
| `frictional_velocity_at_surface` | `frictional_velocity` | grib | Frictional velocity | m s**-1 |
| `geopotential_height` | `geopotential` | grib+geopotential+units | Geopotential height | gpm -> m**2 s**-2 |
| `graupel_(snow_pellets)` | `graupel` | grib | Graupel (snow pellets) | kg kg**-1 |
| `ground_heat_flux_at_surface` | `ground_heat_flux` | grib | Ground heat flux | W m**-2 |
| `hail_at_surface` | `hail_at_surface` | clash | Hail | m |
| `instantaneous_surface_sensible_heat_net_flux_at_surface` | `instantaneous_surface_sensible_heat_net_flux` | grib | Instantaneous surface sensible heat net flux | W m**-2 |
| `land-sea_mask_at_surface` | `land_sea_mask` | grib | Land-sea mask | (0 - 1) |
| `leaf_area_index_at_surface` | `leaf_area_index` | grib | Leaf Area Index | Numeric |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `orography_at_surface` | `geopotential_at_surface` | grib+geopotential+units | Orography | m -> m**2 s**-2 |
| `percent_frozen_precipitation_at_surface` | `percent_frozen_precipitation` | grib | Percent frozen precipitation | % |
| `plant_canopy_surface_water_at_surface` | `plant_canopy_surface_water` | grib | Plant canopy surface water | kg m**-2 |
| `potential_temperature_at_2.0m` | `2m_potential_temperature` | grib | Potential temperature | K |
| `precipitable_water_atmosphere_single_layer` | `precipitable_water` | grib | Precipitable water | kg m**-2 |
| `precipitation_rate_at_surface` | `precipitation_rate` | grib | Precipitation rate | kg m**-2 s**-1 |
| `sea_ice_area_fraction_at_surface` | `sea_ice_area_fraction` | grib | Sea ice area fraction | (0 - 1) |
| `snow_cover_at_surface` | `snow_cover` | grib | Snow cover | % |
| `snow_depth_at_surface` | `snow_depth` | grib | Snow depth | m |
| `storm_surface_runoff_at_surface` | `storm_surface_runoff` | grib | Storm surface runoff | kg m**-2 |
| `surface_downward_long-wave_radiation_flux_at_surface` | `surface_downward_long_wave_radiation_flux` | grib | Surface downward long-wave radiation flux | W m**-2 |
| `surface_downward_short-wave_radiation_flux_at_surface` | `surface_downward_short_wave_radiation_flux` | grib | Surface downward short-wave radiation flux | W m**-2 |
| `surface_latent_heat_net_flux_at_surface` | `surface_latent_heat_net_flux` | grib | Surface latent heat net flux | W m**-2 |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_upward_long-wave_radiation_flux_at_surface` | `surface_upward_long_wave_radiation_flux` | grib | Surface upward long-wave radiation flux | W m**-2 |
| `surface_upward_short-wave_radiation_flux_at_surface` | `surface_upward_short_wave_radiation_flux` | grib | Surface upward short-wave radiation flux | W m**-2 |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-maximum_10_metre_wind_speed_at_10.0m` | `maximum_10m_wind_speed` | grib | Time-maximum 10 metre wind speed | m s**-1 |
| `total_precipitation_at_surface` | `total_precipitation` | grib+units | Total Precipitation | kg m**-2 -> m |
| `u_component_of_wind_at_80.0m` | `80m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `v_component_of_wind_at_80.0m` | `80m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `vegetation_at_surface` | `vegetation` | grib | Vegetation | % |
| `vegetation_type_at_surface` | `vegetation_type` | grib | Vegetation Type | Integer(0-13) |
| `vertically-integrated_liquid` | `vertically_integrated_liquid` | grib | Vertically-integrated liquid | kg m**-1 |
| `visibility_at_surface` | `visibility` | grib | Visibility | m |
| `visible_beam_downward_solar_flux_at_surface` | `visible_beam_downward_solar_flux` | grib | Visible Beam Downward Solar Flux | W m**-2 |
| `visible_diffuse_downward_solar_flux_at_surface` | `visible_diffuse_downward_solar_flux` | grib | Visible Diffuse Downward Solar Flux | W m**-2 |
| `wind_speed_(gust)_at_surface` | `wind_speed` | grib | Wind speed (gust) | m s**-1 |

## bkr/dmi/amps_d1 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `albedo` | `albedo` | kept | Albedo | % |
| `boundary_layer_height` | `planetary_boundary_layer_height` | long_name | Planetary boundary layer height | m |
| `cloud_base_height` | `geopotential_height_of_cloud_base` | long_name | Geopotential height of cloud base | gpm |
| `cloud_water_mixing_ratio_at_2m` | `2m_cloud_water_mixing_ratio` | long_name | 2 m cloud water mixing ratio | kg kg-1 |
| `geopotential_height` | `geopotential_on_height_levels` | clash | Geopotential height | gpm |
| `geopotential_height_model_level` | `geopotential_on_model_levels` | clash | Geopotential height | gpm |
| `ice_mixing_ratio_at_2m` | `2m_ice_mixing_ratio` | long_name | 2 m ice mixing ratio | kg kg-1 |
| `level` | `pressure_level` | coordinate | Pressure level | hPa |
| `mean_sea_level_pressure` | `sea_level_pressure` | long_name | Sea-level pressure | Pa |
| `orography` | `geopotential_at_surface` | long_name+geopotential+units | Terrain height | m -> m**2 s**-2 |
| `rain_water_mixing_ratio_at_2m` | `2m_rain_water_mixing_ratio` | long_name | 2 m rain water mixing ratio | kg kg-1 |
| `relative_humidity_at_2m` | `2m_relative_humidity` | long_name | 2 m relative humidity (with respect to water) | % |
| `sea_ice_cover` | `sea_ice` | long_name | Sea ice | 1 |
| `snow_mixing_ratio_at_2m` | `2m_snow_mixing_ratio` | long_name | 2 m snow mixing ratio | kg kg-1 |
| `temperature_at_2m` | `2m_temperature` | long_name | 2 m temperature | K |
| `total_precipitation_3h` | `precipitation_over_the_3_h_ending_at_the_step` | long_name | Precipitation over the 3 h ending at the step (since the ini | kg m-2 |
| `u_component_of_wind_at_10m` | `10m_u_component_of_wind` | long_name | 10 m U-component of wind (grid-relative) | m s-1 |
| `u_component_of_wind_at_150m_above_sea_level` | `u_component_of_wind_150_m_above_sea_level` | long_name | U-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_at_300m_above_sea_level` | `u_component_of_wind_300_m_above_sea_level` | long_name | U-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_model_level` | `u_component_of_wind_on_model_levels` | clash | U-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_10m` | `10m_v_component_of_wind` | long_name | 10 m V-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_150m_above_sea_level` | `v_component_of_wind_150_m_above_sea_level` | long_name | V-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_300m_above_sea_level` | `v_component_of_wind_300_m_above_sea_level` | long_name | V-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_model_level` | `v_component_of_wind_on_model_levels` | clash | V-component of wind (grid-relative) | m s-1 |
| `water_vapor_mixing_ratio_at_2m` | `2m_water_vapor_mixing_ratio` | long_name | 2 m water vapor mixing ratio | kg kg-1 |
| `water_vapor_mixing_ratio_model_level` | `water_vapor_mixing_ratio_on_model_levels` | clash | Water vapor mixing ratio | kg kg-1 |

## bkr/dmi/amps_d2 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `albedo` | `albedo` | kept | Albedo | % |
| `boundary_layer_height` | `planetary_boundary_layer_height` | long_name | Planetary boundary layer height | m |
| `cloud_base_height` | `geopotential_height_of_cloud_base` | long_name | Geopotential height of cloud base | gpm |
| `cloud_water_mixing_ratio_at_2m` | `2m_cloud_water_mixing_ratio` | long_name | 2 m cloud water mixing ratio | kg kg-1 |
| `geopotential_height` | `geopotential_on_height_levels` | clash | Geopotential height | gpm |
| `geopotential_height_model_level` | `geopotential_on_model_levels` | clash | Geopotential height | gpm |
| `ice_mixing_ratio_at_2m` | `2m_ice_mixing_ratio` | long_name | 2 m ice mixing ratio | kg kg-1 |
| `level` | `pressure_level` | coordinate | Pressure level | hPa |
| `mean_sea_level_pressure` | `sea_level_pressure` | long_name | Sea-level pressure | Pa |
| `orography` | `geopotential_at_surface` | long_name+geopotential+units | Terrain height | m -> m**2 s**-2 |
| `rain_water_mixing_ratio_at_2m` | `2m_rain_water_mixing_ratio` | long_name | 2 m rain water mixing ratio | kg kg-1 |
| `relative_humidity_at_2m` | `2m_relative_humidity` | long_name | 2 m relative humidity (with respect to water) | % |
| `sea_ice_cover` | `sea_ice` | long_name | Sea ice | 1 |
| `snow_mixing_ratio_at_2m` | `2m_snow_mixing_ratio` | long_name | 2 m snow mixing ratio | kg kg-1 |
| `temperature_at_2m` | `2m_temperature` | long_name | 2 m temperature | K |
| `total_precipitation_3h` | `precipitation_over_the_3_h_ending_at_the_step` | long_name | Precipitation over the 3 h ending at the step (since the ini | kg m-2 |
| `u_component_of_wind_at_10m` | `10m_u_component_of_wind` | long_name | 10 m U-component of wind (grid-relative) | m s-1 |
| `u_component_of_wind_at_150m_above_sea_level` | `u_component_of_wind_150_m_above_sea_level` | long_name | U-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_at_300m_above_sea_level` | `u_component_of_wind_300_m_above_sea_level` | long_name | U-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_model_level` | `u_component_of_wind_on_model_levels` | clash | U-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_10m` | `10m_v_component_of_wind` | long_name | 10 m V-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_150m_above_sea_level` | `v_component_of_wind_150_m_above_sea_level` | long_name | V-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_300m_above_sea_level` | `v_component_of_wind_300_m_above_sea_level` | long_name | V-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_model_level` | `v_component_of_wind_on_model_levels` | clash | V-component of wind (grid-relative) | m s-1 |
| `water_vapor_mixing_ratio_at_2m` | `2m_water_vapor_mixing_ratio` | long_name | 2 m water vapor mixing ratio | kg kg-1 |
| `water_vapor_mixing_ratio_model_level` | `water_vapor_mixing_ratio_on_model_levels` | clash | Water vapor mixing ratio | kg kg-1 |

## bkr/dmi/amps_d3 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `albedo` | `albedo` | kept | Albedo | % |
| `boundary_layer_height` | `planetary_boundary_layer_height` | long_name | Planetary boundary layer height | m |
| `cloud_base_height` | `geopotential_height_of_cloud_base` | long_name | Geopotential height of cloud base | gpm |
| `cloud_water_mixing_ratio_at_2m` | `2m_cloud_water_mixing_ratio` | long_name | 2 m cloud water mixing ratio | kg kg-1 |
| `geopotential_height` | `geopotential_on_height_levels` | clash | Geopotential height | gpm |
| `geopotential_height_model_level` | `geopotential_on_model_levels` | clash | Geopotential height | gpm |
| `ice_mixing_ratio_at_2m` | `2m_ice_mixing_ratio` | long_name | 2 m ice mixing ratio | kg kg-1 |
| `level` | `pressure_level` | coordinate | Pressure level | hPa |
| `mean_sea_level_pressure` | `sea_level_pressure` | long_name | Sea-level pressure | Pa |
| `orography` | `geopotential_at_surface` | long_name+geopotential+units | Terrain height | m -> m**2 s**-2 |
| `rain_water_mixing_ratio_at_2m` | `2m_rain_water_mixing_ratio` | long_name | 2 m rain water mixing ratio | kg kg-1 |
| `relative_humidity_at_2m` | `2m_relative_humidity` | long_name | 2 m relative humidity (with respect to water) | % |
| `sea_ice_cover` | `sea_ice` | long_name | Sea ice | 1 |
| `snow_mixing_ratio_at_2m` | `2m_snow_mixing_ratio` | long_name | 2 m snow mixing ratio | kg kg-1 |
| `temperature_at_2m` | `2m_temperature` | long_name | 2 m temperature | K |
| `total_precipitation_3h` | `precipitation_over_the_3_h_ending_at_the_step` | long_name | Precipitation over the 3 h ending at the step (since the ini | kg m-2 |
| `u_component_of_wind_at_10m` | `10m_u_component_of_wind` | long_name | 10 m U-component of wind (grid-relative) | m s-1 |
| `u_component_of_wind_at_150m_above_sea_level` | `u_component_of_wind_150_m_above_sea_level` | long_name | U-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_at_300m_above_sea_level` | `u_component_of_wind_300_m_above_sea_level` | long_name | U-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_model_level` | `u_component_of_wind_on_model_levels` | clash | U-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_10m` | `10m_v_component_of_wind` | long_name | 10 m V-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_150m_above_sea_level` | `v_component_of_wind_150_m_above_sea_level` | long_name | V-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_300m_above_sea_level` | `v_component_of_wind_300_m_above_sea_level` | long_name | V-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_model_level` | `v_component_of_wind_on_model_levels` | clash | V-component of wind (grid-relative) | m s-1 |
| `water_vapor_mixing_ratio_at_2m` | `2m_water_vapor_mixing_ratio` | long_name | 2 m water vapor mixing ratio | kg kg-1 |
| `water_vapor_mixing_ratio_model_level` | `water_vapor_mixing_ratio_on_model_levels` | clash | Water vapor mixing ratio | kg kg-1 |

## bkr/dmi/amps_d5 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `albedo` | `albedo` | kept | Albedo | % |
| `boundary_layer_height` | `planetary_boundary_layer_height` | long_name | Planetary boundary layer height | m |
| `cloud_base_height` | `geopotential_height_of_cloud_base` | long_name | Geopotential height of cloud base | gpm |
| `cloud_water_mixing_ratio_at_2m` | `2m_cloud_water_mixing_ratio` | long_name | 2 m cloud water mixing ratio | kg kg-1 |
| `geopotential_height` | `geopotential_on_height_levels` | clash | Geopotential height | gpm |
| `geopotential_height_model_level` | `geopotential_on_model_levels` | clash | Geopotential height | gpm |
| `ice_mixing_ratio_at_2m` | `2m_ice_mixing_ratio` | long_name | 2 m ice mixing ratio | kg kg-1 |
| `level` | `pressure_level` | coordinate | Pressure level | hPa |
| `mean_sea_level_pressure` | `sea_level_pressure` | long_name | Sea-level pressure | Pa |
| `orography` | `geopotential_at_surface` | long_name+geopotential+units | Terrain height | m -> m**2 s**-2 |
| `rain_water_mixing_ratio_at_2m` | `2m_rain_water_mixing_ratio` | long_name | 2 m rain water mixing ratio | kg kg-1 |
| `relative_humidity_at_2m` | `2m_relative_humidity` | long_name | 2 m relative humidity (with respect to water) | % |
| `sea_ice_cover` | `sea_ice` | long_name | Sea ice | 1 |
| `snow_mixing_ratio_at_2m` | `2m_snow_mixing_ratio` | long_name | 2 m snow mixing ratio | kg kg-1 |
| `temperature_at_2m` | `2m_temperature` | long_name | 2 m temperature | K |
| `total_precipitation_3h` | `precipitation_over_the_3_h_ending_at_the_step` | long_name | Precipitation over the 3 h ending at the step (since the ini | kg m-2 |
| `u_component_of_wind_at_10m` | `10m_u_component_of_wind` | long_name | 10 m U-component of wind (grid-relative) | m s-1 |
| `u_component_of_wind_at_150m_above_sea_level` | `u_component_of_wind_150_m_above_sea_level` | long_name | U-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_at_300m_above_sea_level` | `u_component_of_wind_300_m_above_sea_level` | long_name | U-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_model_level` | `u_component_of_wind_on_model_levels` | clash | U-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_10m` | `10m_v_component_of_wind` | long_name | 10 m V-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_150m_above_sea_level` | `v_component_of_wind_150_m_above_sea_level` | long_name | V-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_300m_above_sea_level` | `v_component_of_wind_300_m_above_sea_level` | long_name | V-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_model_level` | `v_component_of_wind_on_model_levels` | clash | V-component of wind (grid-relative) | m s-1 |
| `water_vapor_mixing_ratio_at_2m` | `2m_water_vapor_mixing_ratio` | long_name | 2 m water vapor mixing ratio | kg kg-1 |
| `water_vapor_mixing_ratio_model_level` | `water_vapor_mixing_ratio_on_model_levels` | clash | Water vapor mixing ratio | kg kg-1 |

## bkr/dmi/amps_d6 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `albedo` | `albedo` | kept | Albedo | % |
| `boundary_layer_height` | `planetary_boundary_layer_height` | long_name | Planetary boundary layer height | m |
| `cloud_base_height` | `geopotential_height_of_cloud_base` | long_name | Geopotential height of cloud base | gpm |
| `cloud_water_mixing_ratio_at_2m` | `2m_cloud_water_mixing_ratio` | long_name | 2 m cloud water mixing ratio | kg kg-1 |
| `geopotential_height` | `geopotential_on_height_levels` | clash | Geopotential height | gpm |
| `geopotential_height_model_level` | `geopotential_on_model_levels` | clash | Geopotential height | gpm |
| `ice_mixing_ratio_at_2m` | `2m_ice_mixing_ratio` | long_name | 2 m ice mixing ratio | kg kg-1 |
| `level` | `pressure_level` | coordinate | Pressure level | hPa |
| `mean_sea_level_pressure` | `sea_level_pressure` | long_name | Sea-level pressure | Pa |
| `orography` | `geopotential_at_surface` | long_name+geopotential+units | Terrain height | m -> m**2 s**-2 |
| `rain_water_mixing_ratio_at_2m` | `2m_rain_water_mixing_ratio` | long_name | 2 m rain water mixing ratio | kg kg-1 |
| `relative_humidity_at_2m` | `2m_relative_humidity` | long_name | 2 m relative humidity (with respect to water) | % |
| `sea_ice_cover` | `sea_ice` | long_name | Sea ice | 1 |
| `snow_mixing_ratio_at_2m` | `2m_snow_mixing_ratio` | long_name | 2 m snow mixing ratio | kg kg-1 |
| `temperature_at_2m` | `2m_temperature` | long_name | 2 m temperature | K |
| `total_precipitation_3h` | `precipitation_over_the_3_h_ending_at_the_step` | long_name | Precipitation over the 3 h ending at the step (since the ini | kg m-2 |
| `u_component_of_wind_at_10m` | `10m_u_component_of_wind` | long_name | 10 m U-component of wind (grid-relative) | m s-1 |
| `u_component_of_wind_at_150m_above_sea_level` | `u_component_of_wind_150_m_above_sea_level` | long_name | U-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_at_300m_above_sea_level` | `u_component_of_wind_300_m_above_sea_level` | long_name | U-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_model_level` | `u_component_of_wind_on_model_levels` | clash | U-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_10m` | `10m_v_component_of_wind` | long_name | 10 m V-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_150m_above_sea_level` | `v_component_of_wind_150_m_above_sea_level` | long_name | V-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_300m_above_sea_level` | `v_component_of_wind_300_m_above_sea_level` | long_name | V-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_model_level` | `v_component_of_wind_on_model_levels` | clash | V-component of wind (grid-relative) | m s-1 |
| `water_vapor_mixing_ratio_at_2m` | `2m_water_vapor_mixing_ratio` | long_name | 2 m water vapor mixing ratio | kg kg-1 |
| `water_vapor_mixing_ratio_model_level` | `water_vapor_mixing_ratio_on_model_levels` | clash | Water vapor mixing ratio | kg kg-1 |

## bkr/dmi/amps_drake (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `albedo` | `albedo` | kept | Albedo | % |
| `boundary_layer_height` | `planetary_boundary_layer_height` | long_name | Planetary boundary layer height | m |
| `cloud_base_height` | `geopotential_height_of_cloud_base` | long_name | Geopotential height of cloud base | gpm |
| `cloud_water_mixing_ratio_at_2m` | `2m_cloud_water_mixing_ratio` | long_name | 2 m cloud water mixing ratio | kg kg-1 |
| `geopotential_height` | `geopotential` | long_name+geopotential+units | Geopotential height | gpm -> m**2 s**-2 |
| `ice_mixing_ratio_at_2m` | `2m_ice_mixing_ratio` | long_name | 2 m ice mixing ratio | kg kg-1 |
| `level` | `pressure_level` | coordinate | Pressure level | hPa |
| `mean_sea_level_pressure` | `sea_level_pressure` | long_name | Sea-level pressure | Pa |
| `orography` | `geopotential_at_surface` | long_name+geopotential+units | Terrain height | m -> m**2 s**-2 |
| `rain_water_mixing_ratio_at_2m` | `2m_rain_water_mixing_ratio` | long_name | 2 m rain water mixing ratio | kg kg-1 |
| `relative_humidity_at_2m` | `2m_relative_humidity` | long_name | 2 m relative humidity (with respect to water) | % |
| `sea_ice_cover` | `sea_ice` | long_name | Sea ice | 1 |
| `snow_mixing_ratio_at_2m` | `2m_snow_mixing_ratio` | long_name | 2 m snow mixing ratio | kg kg-1 |
| `temperature_at_2m` | `2m_temperature` | long_name | 2 m temperature | K |
| `total_precipitation_3h` | `precipitation_over_the_3_h_ending_at_the_step` | long_name | Precipitation over the 3 h ending at the step (since the ini | kg m-2 |
| `u_component_of_wind_at_10m` | `10m_u_component_of_wind` | long_name | 10 m U-component of wind (grid-relative) | m s-1 |
| `u_component_of_wind_at_150m_above_sea_level` | `u_component_of_wind_150_m_above_sea_level` | long_name | U-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_at_300m_above_sea_level` | `u_component_of_wind_300_m_above_sea_level` | long_name | U-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_10m` | `10m_v_component_of_wind` | long_name | 10 m V-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_150m_above_sea_level` | `v_component_of_wind_150_m_above_sea_level` | long_name | V-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_300m_above_sea_level` | `v_component_of_wind_300_m_above_sea_level` | long_name | V-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `water_vapor_mixing_ratio_at_2m` | `2m_water_vapor_mixing_ratio` | long_name | 2 m water vapor mixing ratio | kg kg-1 |

## bkr/dmi/amps_nz (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `albedo` | `albedo` | kept | Albedo | % |
| `boundary_layer_height` | `planetary_boundary_layer_height` | long_name | Planetary boundary layer height | m |
| `cloud_base_height` | `geopotential_height_of_cloud_base` | long_name | Geopotential height of cloud base | gpm |
| `cloud_water_mixing_ratio_at_2m` | `2m_cloud_water_mixing_ratio` | long_name | 2 m cloud water mixing ratio | kg kg-1 |
| `geopotential_height` | `geopotential` | long_name+geopotential+units | Geopotential height | gpm -> m**2 s**-2 |
| `ice_mixing_ratio_at_2m` | `2m_ice_mixing_ratio` | long_name | 2 m ice mixing ratio | kg kg-1 |
| `level` | `pressure_level` | coordinate | Pressure level | hPa |
| `mean_sea_level_pressure` | `sea_level_pressure` | long_name | Sea-level pressure | Pa |
| `orography` | `geopotential_at_surface` | long_name+geopotential+units | Terrain height | m -> m**2 s**-2 |
| `rain_water_mixing_ratio_at_2m` | `2m_rain_water_mixing_ratio` | long_name | 2 m rain water mixing ratio | kg kg-1 |
| `relative_humidity_at_2m` | `2m_relative_humidity` | long_name | 2 m relative humidity (with respect to water) | % |
| `sea_ice_cover` | `sea_ice` | long_name | Sea ice | 1 |
| `snow_mixing_ratio_at_2m` | `2m_snow_mixing_ratio` | long_name | 2 m snow mixing ratio | kg kg-1 |
| `temperature_at_2m` | `2m_temperature` | long_name | 2 m temperature | K |
| `total_precipitation_3h` | `precipitation_over_the_3_h_ending_at_the_step` | long_name | Precipitation over the 3 h ending at the step (since the ini | kg m-2 |
| `u_component_of_wind_at_10m` | `10m_u_component_of_wind` | long_name | 10 m U-component of wind (grid-relative) | m s-1 |
| `u_component_of_wind_at_150m_above_sea_level` | `u_component_of_wind_150_m_above_sea_level` | long_name | U-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `u_component_of_wind_at_300m_above_sea_level` | `u_component_of_wind_300_m_above_sea_level` | long_name | U-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_10m` | `10m_v_component_of_wind` | long_name | 10 m V-component of wind (grid-relative) | m s-1 |
| `v_component_of_wind_at_150m_above_sea_level` | `v_component_of_wind_150_m_above_sea_level` | long_name | V-component of wind 150 m above sea level (grid-relative) | m s-1 |
| `v_component_of_wind_at_300m_above_sea_level` | `v_component_of_wind_300_m_above_sea_level` | long_name | V-component of wind 300 m above sea level (grid-relative) | m s-1 |
| `water_vapor_mixing_ratio_at_2m` | `2m_water_vapor_mixing_ratio` | long_name | 2 m water vapor mixing ratio | kg kg-1 |

## bkr/dmi/arome_caribbean (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `10_metre_u_wind_component_at_surface` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_surface` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `10_metre_wind_direction_at_surface` | `10m_wind_direction` | grib | 10 metre wind direction | Degree true |
| `10_metre_wind_speed_at_surface` | `10m_wind_speed` | grib | 10 metre wind speed | m s**-1 |
| `2_metre_dewpoint_temperature_at_surface` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_surface` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_surface` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_surface` | `2m_temperature` | grib | 2 metre temperature | K |
| `boundary_layer_height_at_surface` | `boundary_layer_height` | grib | Boundary layer height | m |
| `convective_available_potential_energy_instantaneous_at_surface` | `convective_available_potential_energy_instantaneous` | grib | Convective Available Potential Energy instantaneous | m**2 s**-2 |
| `dew_point_temperature` | `dewpoint_temperature` | grib | Dew point temperature | K |
| `dew_point_temperature_at_height` | `dewpoint_temperature_on_height_levels` | clash | Dew point temperature | K |
| `fraction_of_cloud_cover_at_height` | `fraction_of_cloud_cover_on_height_levels` | clash | Fraction of cloud cover | (0 - 1) |
| `geopotential_at_height` | `geopotential_on_height_levels` | clash | Geopotential | m**2 s**-2 |
| `graupel_snow_pellets_precipitation_at_surface` | `graupel_precipitation` | grib | Graupel (snow pellets) precipitation | kg m**-2 |
| `height` | `height_above_ground` | coordinate | height above the surface | m |
| `high_cloud_cover_at_surface` | `high_cloud_cover` | grib+units | High cloud cover | % -> (0 - 1) |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `low_cloud_cover_at_surface` | `low_cloud_cover` | grib+units | Low cloud cover | % -> (0 - 1) |
| `medium_cloud_cover_at_surface` | `medium_cloud_cover` | grib+units | Medium cloud cover | % -> (0 - 1) |
| `pressure_at_height` | `pressure` | grib | Pressure | Pa |
| `pressure_reduced_to_msl_at_surface` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `relative_humidity_at_height` | `relative_humidity_on_height_levels` | clash | Relative humidity | % |
| `specific_cloud_ice_water_content_at_height` | `specific_cloud_ice_water_content_on_height_levels` | clash | Specific cloud ice water content | kg kg**-1 |
| `specific_cloud_liquid_water_content_at_height` | `specific_cloud_liquid_water_content_on_height_levels` | clash | Specific cloud liquid water content | kg kg**-1 |
| `specific_humidity_at_height` | `specific_humidity_on_height_levels` | clash | Specific humidity | kg kg**-1 |
| `specific_rain_water_content_at_height` | `specific_rain_water_content_on_height_levels` | clash | Specific rain water content | kg kg**-1 |
| `specific_snow_water_content_at_height` | `specific_snow_water_content_on_height_levels` | clash | Specific snow water content | kg kg**-1 |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_short-wave_solar_radiation_downwards_at_surface` | `surface_short_wave_radiation_downwards` | grib | Surface short-wave (solar) radiation downwards | J m**-2 |
| `temperature_at_height` | `temperature_on_height_levels` | clash | Temperature | K |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-maximum_10_metre_eastward_wind_gust_at_surface` | `maximum_10m_eastward_wind_gust` | grib | Time-maximum 10 metre eastward wind gust | m s**-1 |
| `time-maximum_10_metre_northward_wind_gust_at_surface` | `maximum_10m_northward_wind_gust` | grib | Time-maximum 10 metre northward wind gust | m s**-1 |
| `time-maximum_10_metre_wind_gust_at_surface` | `maximum_10m_wind_gust` | grib | Time-maximum 10 metre wind gust | m s**-1 |
| `time-maximum_2_metre_temperature_at_surface` | `maximum_2m_temperature` | grib | Time-maximum 2 metre temperature | K |
| `time-minimum_2_metre_temperature_at_surface` | `minimum_2m_temperature` | grib | Time-minimum 2 metre temperature | K |
| `time_integral_of_rain_flux_at_surface` | `integral_of_rain_flux` | grib | Time integral of rain flux | kg m**-2 |
| `total_precipitation_at_surface` | `total_precipitation` | grib+units | Total Precipitation | kg m**-2 -> m |
| `total_snow_precipitation_at_surface` | `total_snow_precipitation` | grib | Total snow precipitation | kg m**-2 |
| `turbulent_kinetic_energy_at_height` | `turbulent_kinetic_energy_on_height_levels` | clash | Turbulent kinetic energy | J kg**-1 |
| `u_component_of_wind_at_height` | `u_component_of_wind_on_height_levels` | clash | U component of wind | m s**-1 |
| `v_component_of_wind_at_height` | `v_component_of_wind_on_height_levels` | clash | V component of wind | m s**-1 |
| `vorticity_relative` | `vorticity` | grib | Vorticity (relative) | s**-1 |
| `wind_direction_at_height` | `wind_direction_on_height_levels` | clash | Wind direction | Degree true |
| `wind_speed_at_height` | `wind_speed_on_height_levels` | clash | Wind speed | m s**-1 |

## bkr/dmi/arome_france (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `10_metre_u_wind_component_at_height` | `10m_u_component_of_wind_on_height_levels` | clash | 10 metre U wind component | m s**-1 |
| `10_metre_u_wind_component_at_surface` | `10m_u_component_of_wind_at_surface` | clash | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_height` | `10m_v_component_of_wind_on_height_levels` | clash | 10 metre V wind component | m s**-1 |
| `10_metre_v_wind_component_at_surface` | `10m_v_component_of_wind_at_surface` | clash | 10 metre V wind component | m s**-1 |
| `10_metre_wind_direction_at_height` | `10m_wind_direction` | grib | 10 metre wind direction | Degree true |
| `10_metre_wind_speed_at_height` | `10m_wind_speed` | grib | 10 metre wind speed | m s**-1 |
| `2_metre_relative_humidity_at_surface` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_temperature_at_surface` | `2m_temperature` | grib | 2 metre temperature | K |
| `convective_available_potential_energy_instantaneous_at_surface` | `convective_available_potential_energy_instantaneous` | grib | Convective Available Potential Energy instantaneous | m**2 s**-2 |
| `geometrical_height_above_ground_at_surface` | `geometrical_height_above_ground` | grib | Geometrical height above ground | m |
| `graupel_snow_pellets_precipitation_at_surface` | `graupel_precipitation` | grib | Graupel (snow pellets) precipitation | kg m**-2 |
| `height` | `height_above_ground` | coordinate | height above the surface | m |
| `high_cloud_cover_at_surface` | `high_cloud_cover` | grib+units | High cloud cover | % -> (0 - 1) |
| `low_cloud_cover_at_surface` | `low_cloud_cover` | grib+units | Low cloud cover | % -> (0 - 1) |
| `medium_cloud_cover_at_surface` | `medium_cloud_cover` | grib+units | Medium cloud cover | % -> (0 - 1) |
| `relative_humidity_at_height` | `relative_humidity` | grib | Relative humidity | % |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `time-maximum_10_metre_eastward_wind_gust_at_surface` | `maximum_10m_eastward_wind_gust` | grib | Time-maximum 10 metre eastward wind gust | m s**-1 |
| `time-maximum_10_metre_northward_wind_gust_at_surface` | `maximum_10m_northward_wind_gust` | grib | Time-maximum 10 metre northward wind gust | m s**-1 |
| `time_integral_of_rain_flux_at_surface` | `integral_of_rain_flux` | grib | Time integral of rain flux | kg m**-2 |
| `total_snow_precipitation_at_surface` | `total_snow_precipitation` | grib | Total snow precipitation | kg m**-2 |
| `u_component_of_wind_at_height` | `u_component_of_wind` | grib | U component of wind | m s**-1 |
| `v_component_of_wind_at_height` | `v_component_of_wind` | grib | V component of wind | m s**-1 |
| `wind_direction_at_height` | `wind_direction` | grib | Wind direction | Degree true |
| `wind_speed_at_height` | `wind_speed` | grib | Wind speed | m s**-1 |

## bkr/dmi/arome_french_guiana (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `10_metre_u_wind_component_at_surface` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_surface` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `10_metre_wind_direction_at_surface` | `10m_wind_direction` | grib | 10 metre wind direction | Degree true |
| `10_metre_wind_speed_at_surface` | `10m_wind_speed` | grib | 10 metre wind speed | m s**-1 |
| `2_metre_dewpoint_temperature_at_surface` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_surface` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_surface` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_surface` | `2m_temperature` | grib | 2 metre temperature | K |
| `boundary_layer_height_at_surface` | `boundary_layer_height` | grib | Boundary layer height | m |
| `convective_available_potential_energy_instantaneous_at_surface` | `convective_available_potential_energy_instantaneous` | grib | Convective Available Potential Energy instantaneous | m**2 s**-2 |
| `dew_point_temperature` | `dewpoint_temperature` | grib | Dew point temperature | K |
| `dew_point_temperature_at_height` | `dewpoint_temperature_on_height_levels` | clash | Dew point temperature | K |
| `fraction_of_cloud_cover_at_height` | `fraction_of_cloud_cover_on_height_levels` | clash | Fraction of cloud cover | (0 - 1) |
| `geopotential_at_height` | `geopotential_on_height_levels` | clash | Geopotential | m**2 s**-2 |
| `graupel_snow_pellets_precipitation_at_surface` | `graupel_precipitation` | grib | Graupel (snow pellets) precipitation | kg m**-2 |
| `height` | `height_above_ground` | coordinate | height above the surface | m |
| `high_cloud_cover_at_surface` | `high_cloud_cover` | grib+units | High cloud cover | % -> (0 - 1) |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `low_cloud_cover_at_surface` | `low_cloud_cover` | grib+units | Low cloud cover | % -> (0 - 1) |
| `medium_cloud_cover_at_surface` | `medium_cloud_cover` | grib+units | Medium cloud cover | % -> (0 - 1) |
| `pressure_at_height` | `pressure` | grib | Pressure | Pa |
| `pressure_reduced_to_msl_at_surface` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `relative_humidity_at_height` | `relative_humidity_on_height_levels` | clash | Relative humidity | % |
| `specific_cloud_ice_water_content_at_height` | `specific_cloud_ice_water_content_on_height_levels` | clash | Specific cloud ice water content | kg kg**-1 |
| `specific_cloud_liquid_water_content_at_height` | `specific_cloud_liquid_water_content_on_height_levels` | clash | Specific cloud liquid water content | kg kg**-1 |
| `specific_humidity_at_height` | `specific_humidity_on_height_levels` | clash | Specific humidity | kg kg**-1 |
| `specific_rain_water_content_at_height` | `specific_rain_water_content_on_height_levels` | clash | Specific rain water content | kg kg**-1 |
| `specific_snow_water_content_at_height` | `specific_snow_water_content_on_height_levels` | clash | Specific snow water content | kg kg**-1 |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_short-wave_solar_radiation_downwards_at_surface` | `surface_short_wave_radiation_downwards` | grib | Surface short-wave (solar) radiation downwards | J m**-2 |
| `temperature_at_height` | `temperature_on_height_levels` | clash | Temperature | K |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-maximum_10_metre_eastward_wind_gust_at_surface` | `maximum_10m_eastward_wind_gust` | grib | Time-maximum 10 metre eastward wind gust | m s**-1 |
| `time-maximum_10_metre_northward_wind_gust_at_surface` | `maximum_10m_northward_wind_gust` | grib | Time-maximum 10 metre northward wind gust | m s**-1 |
| `time-maximum_10_metre_wind_gust_at_surface` | `maximum_10m_wind_gust` | grib | Time-maximum 10 metre wind gust | m s**-1 |
| `time-maximum_2_metre_temperature_at_surface` | `maximum_2m_temperature` | grib | Time-maximum 2 metre temperature | K |
| `time-minimum_2_metre_temperature_at_surface` | `minimum_2m_temperature` | grib | Time-minimum 2 metre temperature | K |
| `time_integral_of_rain_flux_at_surface` | `integral_of_rain_flux` | grib | Time integral of rain flux | kg m**-2 |
| `total_precipitation_at_surface` | `total_precipitation` | grib+units | Total Precipitation | kg m**-2 -> m |
| `total_snow_precipitation_at_surface` | `total_snow_precipitation` | grib | Total snow precipitation | kg m**-2 |
| `turbulent_kinetic_energy_at_height` | `turbulent_kinetic_energy_on_height_levels` | clash | Turbulent kinetic energy | J kg**-1 |
| `u_component_of_wind_at_height` | `u_component_of_wind_on_height_levels` | clash | U component of wind | m s**-1 |
| `v_component_of_wind_at_height` | `v_component_of_wind_on_height_levels` | clash | V component of wind | m s**-1 |
| `vorticity_relative` | `vorticity` | grib | Vorticity (relative) | s**-1 |
| `wind_direction_at_height` | `wind_direction_on_height_levels` | clash | Wind direction | Degree true |
| `wind_speed_at_height` | `wind_speed_on_height_levels` | clash | Wind speed | m s**-1 |

## bkr/dmi/arome_indian_ocean (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `10_metre_u_wind_component_at_surface` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_surface` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `10_metre_wind_direction_at_surface` | `10m_wind_direction` | grib | 10 metre wind direction | Degree true |
| `10_metre_wind_speed_at_surface` | `10m_wind_speed` | grib | 10 metre wind speed | m s**-1 |
| `2_metre_dewpoint_temperature_at_surface` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_surface` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_surface` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_surface` | `2m_temperature` | grib | 2 metre temperature | K |
| `boundary_layer_height_at_surface` | `boundary_layer_height` | grib | Boundary layer height | m |
| `convective_available_potential_energy_instantaneous_at_surface` | `convective_available_potential_energy_instantaneous` | grib | Convective Available Potential Energy instantaneous | m**2 s**-2 |
| `dew_point_temperature` | `dewpoint_temperature` | grib | Dew point temperature | K |
| `dew_point_temperature_at_height` | `dewpoint_temperature_on_height_levels` | clash | Dew point temperature | K |
| `fraction_of_cloud_cover_at_height` | `fraction_of_cloud_cover_on_height_levels` | clash | Fraction of cloud cover | (0 - 1) |
| `geopotential_at_height` | `geopotential_on_height_levels` | clash | Geopotential | m**2 s**-2 |
| `graupel_snow_pellets_precipitation_at_surface` | `graupel_precipitation` | grib | Graupel (snow pellets) precipitation | kg m**-2 |
| `height` | `height_above_ground` | coordinate | height above the surface | m |
| `high_cloud_cover_at_surface` | `high_cloud_cover` | grib+units | High cloud cover | % -> (0 - 1) |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `low_cloud_cover_at_surface` | `low_cloud_cover` | grib+units | Low cloud cover | % -> (0 - 1) |
| `medium_cloud_cover_at_surface` | `medium_cloud_cover` | grib+units | Medium cloud cover | % -> (0 - 1) |
| `pressure_at_height` | `pressure` | grib | Pressure | Pa |
| `pressure_reduced_to_msl_at_surface` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `relative_humidity_at_height` | `relative_humidity_on_height_levels` | clash | Relative humidity | % |
| `specific_cloud_ice_water_content_at_height` | `specific_cloud_ice_water_content_on_height_levels` | clash | Specific cloud ice water content | kg kg**-1 |
| `specific_cloud_liquid_water_content_at_height` | `specific_cloud_liquid_water_content_on_height_levels` | clash | Specific cloud liquid water content | kg kg**-1 |
| `specific_humidity_at_height` | `specific_humidity_on_height_levels` | clash | Specific humidity | kg kg**-1 |
| `specific_rain_water_content_at_height` | `specific_rain_water_content_on_height_levels` | clash | Specific rain water content | kg kg**-1 |
| `specific_snow_water_content_at_height` | `specific_snow_water_content_on_height_levels` | clash | Specific snow water content | kg kg**-1 |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_short-wave_solar_radiation_downwards_at_surface` | `surface_short_wave_radiation_downwards` | grib | Surface short-wave (solar) radiation downwards | J m**-2 |
| `temperature_at_height` | `temperature_on_height_levels` | clash | Temperature | K |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-maximum_10_metre_eastward_wind_gust_at_surface` | `maximum_10m_eastward_wind_gust` | grib | Time-maximum 10 metre eastward wind gust | m s**-1 |
| `time-maximum_10_metre_northward_wind_gust_at_surface` | `maximum_10m_northward_wind_gust` | grib | Time-maximum 10 metre northward wind gust | m s**-1 |
| `time-maximum_10_metre_wind_gust_at_surface` | `maximum_10m_wind_gust` | grib | Time-maximum 10 metre wind gust | m s**-1 |
| `time-maximum_2_metre_temperature_at_surface` | `maximum_2m_temperature` | grib | Time-maximum 2 metre temperature | K |
| `time-minimum_2_metre_temperature_at_surface` | `minimum_2m_temperature` | grib | Time-minimum 2 metre temperature | K |
| `time_integral_of_rain_flux_at_surface` | `integral_of_rain_flux` | grib | Time integral of rain flux | kg m**-2 |
| `total_precipitation_at_surface` | `total_precipitation` | grib+units | Total Precipitation | kg m**-2 -> m |
| `total_snow_precipitation_at_surface` | `total_snow_precipitation` | grib | Total snow precipitation | kg m**-2 |
| `turbulent_kinetic_energy_at_height` | `turbulent_kinetic_energy_on_height_levels` | clash | Turbulent kinetic energy | J kg**-1 |
| `u_component_of_wind_at_height` | `u_component_of_wind_on_height_levels` | clash | U component of wind | m s**-1 |
| `v_component_of_wind_at_height` | `v_component_of_wind_on_height_levels` | clash | V component of wind | m s**-1 |
| `vorticity_relative` | `vorticity` | grib | Vorticity (relative) | s**-1 |
| `wind_direction_at_height` | `wind_direction_on_height_levels` | clash | Wind direction | Degree true |
| `wind_speed_at_height` | `wind_speed_on_height_levels` | clash | Wind speed | m s**-1 |

## bkr/dmi/arome_new_caledonia (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `10_metre_u_wind_component_at_surface` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_surface` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `10_metre_wind_direction_at_surface` | `10m_wind_direction` | grib | 10 metre wind direction | Degree true |
| `10_metre_wind_speed_at_surface` | `10m_wind_speed` | grib | 10 metre wind speed | m s**-1 |
| `2_metre_dewpoint_temperature_at_surface` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_surface` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_surface` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_surface` | `2m_temperature` | grib | 2 metre temperature | K |
| `boundary_layer_height_at_surface` | `boundary_layer_height` | grib | Boundary layer height | m |
| `convective_available_potential_energy_instantaneous_at_surface` | `convective_available_potential_energy_instantaneous` | grib | Convective Available Potential Energy instantaneous | m**2 s**-2 |
| `dew_point_temperature` | `dewpoint_temperature` | grib | Dew point temperature | K |
| `dew_point_temperature_at_height` | `dewpoint_temperature_on_height_levels` | clash | Dew point temperature | K |
| `fraction_of_cloud_cover_at_height` | `fraction_of_cloud_cover_on_height_levels` | clash | Fraction of cloud cover | (0 - 1) |
| `geopotential_at_height` | `geopotential_on_height_levels` | clash | Geopotential | m**2 s**-2 |
| `graupel_snow_pellets_precipitation_at_surface` | `graupel_precipitation` | grib | Graupel (snow pellets) precipitation | kg m**-2 |
| `height` | `height_above_ground` | coordinate | height above the surface | m |
| `high_cloud_cover_at_surface` | `high_cloud_cover` | grib+units | High cloud cover | % -> (0 - 1) |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `low_cloud_cover_at_surface` | `low_cloud_cover` | grib+units | Low cloud cover | % -> (0 - 1) |
| `medium_cloud_cover_at_surface` | `medium_cloud_cover` | grib+units | Medium cloud cover | % -> (0 - 1) |
| `pressure_at_height` | `pressure` | grib | Pressure | Pa |
| `pressure_reduced_to_msl_at_surface` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `relative_humidity_at_height` | `relative_humidity_on_height_levels` | clash | Relative humidity | % |
| `specific_cloud_ice_water_content_at_height` | `specific_cloud_ice_water_content_on_height_levels` | clash | Specific cloud ice water content | kg kg**-1 |
| `specific_cloud_liquid_water_content_at_height` | `specific_cloud_liquid_water_content_on_height_levels` | clash | Specific cloud liquid water content | kg kg**-1 |
| `specific_humidity_at_height` | `specific_humidity_on_height_levels` | clash | Specific humidity | kg kg**-1 |
| `specific_rain_water_content_at_height` | `specific_rain_water_content_on_height_levels` | clash | Specific rain water content | kg kg**-1 |
| `specific_snow_water_content_at_height` | `specific_snow_water_content_on_height_levels` | clash | Specific snow water content | kg kg**-1 |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_short-wave_solar_radiation_downwards_at_surface` | `surface_short_wave_radiation_downwards` | grib | Surface short-wave (solar) radiation downwards | J m**-2 |
| `temperature_at_height` | `temperature_on_height_levels` | clash | Temperature | K |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-maximum_10_metre_eastward_wind_gust_at_surface` | `maximum_10m_eastward_wind_gust` | grib | Time-maximum 10 metre eastward wind gust | m s**-1 |
| `time-maximum_10_metre_northward_wind_gust_at_surface` | `maximum_10m_northward_wind_gust` | grib | Time-maximum 10 metre northward wind gust | m s**-1 |
| `time-maximum_10_metre_wind_gust_at_surface` | `maximum_10m_wind_gust` | grib | Time-maximum 10 metre wind gust | m s**-1 |
| `time-maximum_2_metre_temperature_at_surface` | `maximum_2m_temperature` | grib | Time-maximum 2 metre temperature | K |
| `time-minimum_2_metre_temperature_at_surface` | `minimum_2m_temperature` | grib | Time-minimum 2 metre temperature | K |
| `time_integral_of_rain_flux_at_surface` | `integral_of_rain_flux` | grib | Time integral of rain flux | kg m**-2 |
| `total_precipitation_at_surface` | `total_precipitation` | grib+units | Total Precipitation | kg m**-2 -> m |
| `total_snow_precipitation_at_surface` | `total_snow_precipitation` | grib | Total snow precipitation | kg m**-2 |
| `turbulent_kinetic_energy_at_height` | `turbulent_kinetic_energy_on_height_levels` | clash | Turbulent kinetic energy | J kg**-1 |
| `u_component_of_wind_at_height` | `u_component_of_wind_on_height_levels` | clash | U component of wind | m s**-1 |
| `v_component_of_wind_at_height` | `v_component_of_wind_on_height_levels` | clash | V component of wind | m s**-1 |
| `vorticity_relative` | `vorticity` | grib | Vorticity (relative) | s**-1 |
| `wind_direction_at_height` | `wind_direction_on_height_levels` | clash | Wind direction | Degree true |
| `wind_speed_at_height` | `wind_speed_on_height_levels` | clash | Wind speed | m s**-1 |

## bkr/dmi/arome_polynesia (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `10_metre_u_wind_component_at_surface` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_surface` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `10_metre_wind_direction_at_surface` | `10m_wind_direction` | grib | 10 metre wind direction | Degree true |
| `10_metre_wind_speed_at_surface` | `10m_wind_speed` | grib | 10 metre wind speed | m s**-1 |
| `2_metre_dewpoint_temperature_at_surface` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_surface` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_surface` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_surface` | `2m_temperature` | grib | 2 metre temperature | K |
| `boundary_layer_height_at_surface` | `boundary_layer_height` | grib | Boundary layer height | m |
| `convective_available_potential_energy_instantaneous_at_surface` | `convective_available_potential_energy_instantaneous` | grib | Convective Available Potential Energy instantaneous | m**2 s**-2 |
| `dew_point_temperature` | `dewpoint_temperature` | grib | Dew point temperature | K |
| `dew_point_temperature_at_height` | `dewpoint_temperature_on_height_levels` | clash | Dew point temperature | K |
| `fraction_of_cloud_cover_at_height` | `fraction_of_cloud_cover_on_height_levels` | clash | Fraction of cloud cover | (0 - 1) |
| `geopotential_at_height` | `geopotential_on_height_levels` | clash | Geopotential | m**2 s**-2 |
| `graupel_snow_pellets_precipitation_at_surface` | `graupel_precipitation` | grib | Graupel (snow pellets) precipitation | kg m**-2 |
| `height` | `height_above_ground` | coordinate | height above the surface | m |
| `high_cloud_cover_at_surface` | `high_cloud_cover` | grib+units | High cloud cover | % -> (0 - 1) |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `low_cloud_cover_at_surface` | `low_cloud_cover` | grib+units | Low cloud cover | % -> (0 - 1) |
| `medium_cloud_cover_at_surface` | `medium_cloud_cover` | grib+units | Medium cloud cover | % -> (0 - 1) |
| `pressure_at_height` | `pressure` | grib | Pressure | Pa |
| `pressure_reduced_to_msl_at_surface` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `relative_humidity_at_height` | `relative_humidity_on_height_levels` | clash | Relative humidity | % |
| `specific_cloud_ice_water_content_at_height` | `specific_cloud_ice_water_content_on_height_levels` | clash | Specific cloud ice water content | kg kg**-1 |
| `specific_cloud_liquid_water_content_at_height` | `specific_cloud_liquid_water_content_on_height_levels` | clash | Specific cloud liquid water content | kg kg**-1 |
| `specific_humidity_at_height` | `specific_humidity_on_height_levels` | clash | Specific humidity | kg kg**-1 |
| `specific_rain_water_content_at_height` | `specific_rain_water_content_on_height_levels` | clash | Specific rain water content | kg kg**-1 |
| `specific_snow_water_content_at_height` | `specific_snow_water_content_on_height_levels` | clash | Specific snow water content | kg kg**-1 |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_short-wave_solar_radiation_downwards_at_surface` | `surface_short_wave_radiation_downwards` | grib | Surface short-wave (solar) radiation downwards | J m**-2 |
| `temperature_at_height` | `temperature_on_height_levels` | clash | Temperature | K |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-maximum_10_metre_eastward_wind_gust_at_surface` | `maximum_10m_eastward_wind_gust` | grib | Time-maximum 10 metre eastward wind gust | m s**-1 |
| `time-maximum_10_metre_northward_wind_gust_at_surface` | `maximum_10m_northward_wind_gust` | grib | Time-maximum 10 metre northward wind gust | m s**-1 |
| `time-maximum_10_metre_wind_gust_at_surface` | `maximum_10m_wind_gust` | grib | Time-maximum 10 metre wind gust | m s**-1 |
| `time-maximum_2_metre_temperature_at_surface` | `maximum_2m_temperature` | grib | Time-maximum 2 metre temperature | K |
| `time-minimum_2_metre_temperature_at_surface` | `minimum_2m_temperature` | grib | Time-minimum 2 metre temperature | K |
| `time_integral_of_rain_flux_at_surface` | `integral_of_rain_flux` | grib | Time integral of rain flux | kg m**-2 |
| `total_precipitation_at_surface` | `total_precipitation` | grib+units | Total Precipitation | kg m**-2 -> m |
| `total_snow_precipitation_at_surface` | `total_snow_precipitation` | grib | Total snow precipitation | kg m**-2 |
| `turbulent_kinetic_energy_at_height` | `turbulent_kinetic_energy_on_height_levels` | clash | Turbulent kinetic energy | J kg**-1 |
| `u_component_of_wind_at_height` | `u_component_of_wind_on_height_levels` | clash | U component of wind | m s**-1 |
| `v_component_of_wind_at_height` | `v_component_of_wind_on_height_levels` | clash | V component of wind | m s**-1 |
| `vorticity_relative` | `vorticity` | grib | Vorticity (relative) | s**-1 |
| `wind_direction_at_height` | `wind_direction_on_height_levels` | clash | Wind direction | Degree true |
| `wind_speed_at_height` | `wind_speed_on_height_levels` | clash | Wind speed | m s**-1 |

## bkr/dmi/harmonie_greenland_iceland (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `100_metre_u_wind_component` | `100_metre_u_wind_component` | clash | 100 metre U wind component | m s**-1 |
| `100_metre_v_wind_component` | `100_metre_v_wind_component` | clash | 100 metre V wind component | m s**-1 |
| `10_metre_u_wind_component` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `10_metre_wind_direction` | `10m_wind_direction` | grib | 10 metre wind direction | Degree true |
| `10_metre_wind_speed` | `10m_wind_speed` | grib | 10 metre wind speed | m s**-1 |
| `2_metre_dewpoint_temperature` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature` | `2m_temperature` | grib | 2 metre temperature | K |
| `cc_at_height_above_ground_0.0` | `0m_fraction_of_cloud_cover` | grib | Fraction of cloud cover | (0 - 1) |
| `cc_at_height_above_ground_2.0` | `2m_fraction_of_cloud_cover` | grib | Fraction of cloud cover | (0 - 1) |
| `geometric_vertical_velocity_at_surface` | `geometric_vertical_velocity` | grib | Geometric vertical velocity | m s**-1 |
| `geopotential_at_surface` | `geopotential` | grib | Geopotential | m**2 s**-2 |
| `height_of_adiabatic_condensation_level` | `height_of_adiabatic_condensation_level` | clash | Geometrical height above ground | m |
| `height_of_free_convection` | `height_of_free_convection` | clash | Geometrical height above ground | m |
| `height_of_isothermal_layer` | `height_of_isothermal_layer` | clash | Geometrical height above ground | m |
| `height_of_neutral_buoyancy` | `height_of_neutral_buoyancy` | clash | Geometrical height above ground | m |
| `height_of_zero_isotherm` | `height_of_zero_isotherm` | clash | Geometrical height above ground | m |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `pres_at_height_above_ground_0.0` | `0m_pressure` | grib | Pressure | Pa |
| `pres_at_height_above_ground_150.0` | `150m_pressure` | grib | Pressure | Pa |
| `pres_at_height_above_ground_300.0` | `300m_pressure` | grib | Pressure | Pa |
| `pressure_at_sea_level` | `pressure` | grib | Pressure | Pa |
| `pseudo-adiabatic_potential_temperature_at_surface` | `pseudo_adiabatic_potential_temperature` | grib | Pseudo-adiabatic potential temperature | K |
| `q_at_height_above_sea_0.0` | `q_at_height_above_sea_0_0` | clash | Specific humidity | kg kg**-1 |
| `q_at_height_above_sea_2.0` | `q_at_height_above_sea_2_0` | clash | Specific humidity | kg kg**-1 |
| `r_at_height_above_ground_100.0` | `100m_relative_humidity` | grib | Relative humidity | % |
| `r_at_height_above_ground_150.0` | `150m_relative_humidity` | grib | Relative humidity | % |
| `r_at_height_above_ground_250.0` | `250m_relative_humidity` | grib | Relative humidity | % |
| `r_at_height_above_ground_50.0` | `50m_relative_humidity` | grib | Relative humidity | % |
| `relative_humidity_at_surface` | `relative_humidity` | grib | Relative humidity | % |
| `t_at_height_above_ground_0.0` | `0m_temperature` | grib | Temperature | K |
| `t_at_height_above_ground_100.0` | `100m_temperature` | grib | Temperature | K |
| `t_at_height_above_ground_150.0` | `150m_temperature` | grib | Temperature | K |
| `t_at_height_above_ground_250.0` | `250m_temperature` | grib | Temperature | K |
| `t_at_height_above_ground_50.0` | `50m_temperature` | grib | Temperature | K |
| `t_at_height_above_sea_2.0` | `temperature` | grib | Temperature | K |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-maximum_10_metre_eastward_wind_gust` | `maximum_10m_eastward_wind_gust` | grib | Time-maximum 10 metre eastward wind gust | m s**-1 |
| `time-maximum_10_metre_northward_wind_gust` | `maximum_10m_northward_wind_gust` | grib | Time-maximum 10 metre northward wind gust | m s**-1 |
| `time-maximum_10_metre_wind_gust` | `maximum_10m_wind_gust` | grib | Time-maximum 10 metre wind gust | m s**-1 |
| `time-maximum_2_metre_temperature` | `maximum_2m_temperature` | grib | Time-maximum 2 metre temperature | K |
| `time-minimum_2_metre_temperature` | `minimum_2m_temperature` | grib | Time-minimum 2 metre temperature | K |
| `time_integral_of_total_solid_precipitation_flux` | `integral_of_total_solid_precipitation_flux` | grib | Time integral of total solid precipitation flux | kg m**-2 |
| `top_net_long-wave_thermal_radiation` | `top_net_long_wave_radiation` | grib | Top net long-wave (thermal) radiation | J m**-2 |
| `top_net_short-wave_solar_radiation` | `top_net_short_wave_radiation` | grib | Top net short-wave (solar) radiation | J m**-2 |
| `u_at_height_above_ground_100.0` | `u_at_height_above_ground_100_0` | clash | U component of wind | m s**-1 |
| `u_at_height_above_ground_150.0` | `150m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u_at_height_above_ground_250.0` | `250m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u_at_height_above_ground_350.0` | `350m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u_at_height_above_ground_450.0` | `450m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u_at_height_above_ground_50.0` | `50m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u_at_height_above_sea_10.0` | `u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u_component_of_wind_at_surface` | `u_component_of_wind_at_surface` | clash | U component of wind | m s**-1 |
| `v_at_height_above_ground_100.0` | `v_at_height_above_ground_100_0` | clash | V component of wind | m s**-1 |
| `v_at_height_above_ground_150.0` | `150m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v_at_height_above_ground_250.0` | `250m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v_at_height_above_ground_350.0` | `350m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v_at_height_above_ground_450.0` | `450m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v_at_height_above_ground_50.0` | `50m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v_at_height_above_sea_10.0` | `v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v_component_of_wind_at_surface` | `v_component_of_wind_at_surface` | clash | V component of wind | m s**-1 |

## bkr/dmi/harmonie_greenland_iceland_model_level (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `heightAboveGround` | `height_above_ground` | coordinate | height above the surface | m |
| `hybrid` | `model_level` | coordinate | hybrid level | 1 |

## bkr/dmi/hawaii_nams (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `10_metre_u_wind_component_at_10.0m` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_10.0m` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `2_metre_dewpoint_temperature_at_2.0m` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_2.0m` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_2.0m` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_2.0m` | `2m_temperature` | grib | 2 metre temperature | K |
| `dew_point_temperature` | `dewpoint_temperature` | grib | Dew point temperature | K |
| `geopotential_height` | `geopotential` | grib+geopotential+units | Geopotential height | gpm -> m**2 s**-2 |
| `ground_heat_flux_at_surface` | `ground_heat_flux` | grib | Ground heat flux | W m**-2 |
| `horizontal_moisture_convergence_atmosphere_single_layer` | `horizontal_moisture_convergence_entire_atmosphere` | clash | Horizontal moisture convergence | kg kg**-1 s**-1 |
| `large_scale_condensate_heating_rate_atmosphere_single_layer` | `large_scale_condensate_heating_rate` | grib | Large Scale Condensate Heating rate | K s**-1 |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `liquid_volumetric_soil_moisture_(non-frozen)` | `liquid_volumetric_soil_moisture` | grib | Liquid volumetric soil moisture (non-frozen) | Proportion |
| `long-wave_radiative_heating_rate_atmosphere_single_layer` | `long_wave_radiative_heating_rate` | grib | Long-Wave Radiative Heating Rate | K s**-1 |
| `maximum_relative_humidity_at_2.0m` | `2m_maximum_relative_humidity` | grib | Maximum relative humidity | % |
| `maximum_temperature_at_2.0m` | `2m_maximum_temperature` | grib | Maximum temperature | K |
| `minimum_relative_humidity_at_2.0m` | `2m_minimum_relative_humidity` | grib | Minimum Relative Humidity | % |
| `minimum_temperature_at_2.0m` | `2m_minimum_temperature` | grib | Minimum temperature | K |
| `mslp_(eta_model_reduction)` | `mslp` | grib | MSLP (Eta model reduction) | Pa |
| `potential_temperature_at_10.0m` | `10m_potential_temperature` | grib | Potential temperature | K |
| `precipitable_water_atmosphere_single_layer` | `precipitable_water` | grib | Precipitable water | kg m**-2 |
| `pressure_at_80.0m` | `80m_pressure` | grib | Pressure | Pa |
| `pressure_reduced_to_msl` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `solar_radiative_heating_rate_atmosphere_single_layer` | `solar_radiative_heating_rate` | grib | Solar Radiative Heating Rate | K s**-1 |
| `surface_downward_long-wave_radiation_flux_at_surface` | `surface_downward_long_wave_radiation_flux` | grib | Surface downward long-wave radiation flux | W m**-2 |
| `surface_downward_short-wave_radiation_flux_at_surface` | `surface_downward_short_wave_radiation_flux` | grib | Surface downward short-wave radiation flux | W m**-2 |
| `surface_upward_long-wave_radiation_flux_at_surface` | `surface_upward_long_wave_radiation_flux` | grib | Surface upward long-wave radiation flux | W m**-2 |
| `surface_upward_short-wave_radiation_flux_at_surface` | `surface_upward_short_wave_radiation_flux` | grib | Surface upward short-wave radiation flux | W m**-2 |
| `temperature_at_80.0m` | `80m_temperature` | grib | Temperature | K |
| `total_cloud_cover_atmosphere_single_layer` | `total_cloud_cover` | grib+units | Total Cloud Cover | % -> (0 - 1) |
| `total_column-integrated_cloud_ice_atmosphere_single_layer` | `total_column_integrated_cloud_ice` | grib | Total column-integrated cloud ice | kg m**-2 |
| `total_column-integrated_cloud_water_atmosphere_single_layer` | `total_column_integrated_cloud_water` | grib | Total column-integrated cloud water | kg m**-2 |
| `total_column-integrated_condensate_atmosphere_single_layer` | `total_column_integrated_condensate` | grib | Total column-integrated condensate | kg m**-2 |
| `total_column-integrated_melting_ice_atmosphere_single_layer` | `total_column_integrated_melting_ice` | grib | Total column-integrated melting ice | kg m**-2 |
| `total_column-integrated_supercooled_liquid_water_atmosphere_single_layer` | `total_column_integrated_supercooled_liquid_water` | grib | Total column-integrated supercooled liquid water | kg m**-2 |
| `total_column_integrated_rain_atmosphere_single_layer` | `total_column_integrated_rain` | grib | Total column integrated rain | kg m**-2 |
| `total_column_integrated_snow_atmosphere_single_layer` | `total_column_integrated_snow` | grib | Total column integrated snow | kg m**-2 |
| `u_component_of_wind_at_80.0m` | `80m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `v_component_of_wind_at_80.0m` | `80m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `vertically-integrated_liquid_atmosphere_single_layer` | `vertically_integrated_liquid` | grib | Vertically-integrated liquid | kg m**-1 |

## bkr/dmi/kenda_forecast_switzerland (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `aswdifd_s` | `mean_surface_diffuse_short_wave_radiation_flux` | override | unknown | unknown |
| `aswdir_s` | `mean_surface_direct_short_wave_radiation_flux` | override | unknown | unknown |
| `graupel_(snow_pellets)_precipitation` | `graupel_precipitation` | grib | Graupel (snow pellets) precipitation | kg m**-2 |
| `height_above_ground` | `geometrical_height_above_ground` | grib | Geometrical height above ground | m |
| `land-sea_mask` | `land_sea_mask` | grib | Land-sea mask | (0 - 1) |
| `large-scale_snowfall_water_equivalent` | `large_scale_snowfall_water_equivalent` | grib | Large-scale snowfall water equivalent | kg m**-2 |
| `model_level_half` | `model_half_level` | coordinate | original GRIB coordinate for key: level(generalVertical) | 1 |
| `surface_upward_short-wave_radiation_flux` | `surface_upward_short_wave_radiation_flux` | grib | Surface upward short-wave radiation flux | W m**-2 |
| `time-maximum_10_metre_wind_gust` | `maximum_10m_wind_gust` | grib | Time-maximum 10 metre wind gust | m s**-1 |
| `time-mean_surface_net_long-wave_radiation_flux` | `mean_surface_net_long_wave_radiation_flux` | grib | Time-mean surface net long-wave radiation flux | W m**-2 |
| `time-mean_surface_net_short-wave_radiation_flux` | `mean_surface_net_short_wave_radiation_flux` | grib | Time-mean surface net short-wave radiation flux | W m**-2 |

## bkr/dmi/kenda_switzerland (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `2_metre_dewpoint_temperature` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `cape_ml` | `mixed_layer_convective_available_potential_energy` | override | unknown | unknown |
| `cape_mu` | `most_unstable_convective_available_potential_energy` | override | unknown | unknown |
| `cin_ml` | `mixed_layer_convective_inhibition` | override | unknown | unknown |
| `cin_mu` | `most_unstable_convective_inhibition` | override | unknown | unknown |
| `height_above_ground` | `geometrical_height_above_ground` | grib | Geometrical height above ground | m |
| `land-sea_mask` | `land_sea_mask` | grib | Land-sea mask | (0 - 1) |
| `model_level_half` | `model_half_level` | coordinate | original GRIB coordinate for key: level(generalVertical) | 1 |
| `pressure_reduced_to_msl` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |

## bkr/dmi/meps_model_level (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature_ml` | `temperature` | cf | Air temperature model levels | K |
| `cloud_area_fraction_ml` | `total_cloud_cover` | cf+units | Cloud cover in model levels | % -> (0 - 1) |
| `hybrid` | `model_level` | coordinate | atmosphere_hybrid_sigma_pressure_coordinate |  |
| `mass_fraction_of_cloud_ice_in_air_ml` | `mass_fraction_of_cloud_ice_in_model_levels` | long_name | Mass fraction of cloud ice in model levels | kg/kg |
| `mass_fraction_of_graupel_in_air_ml` | `instantaneous_graupel_in_model_levels` | long_name | Instantaneous graupel in model levels | kg/kg |
| `mass_fraction_of_rain_in_air_ml` | `instantanous_rain_in_model_levels` | long_name | Instantanous rain in model levels | kg/kg |
| `mass_fraction_of_snow_in_air_ml` | `instantaneous_snow_in_model_levels` | long_name | Instantaneous snow in model levels | kg/kg |
| `specific_humidity_ml` | `specific_humidity` | cf | Specific humidity model levels | kg/kg |
| `turbulent_kinetic_energy_ml` | `turbulent_kinetic_energy` | long_name | Turbulent kinetic energy (TKE) | m^2/s^2 |
| `upward_air_velocity_ml` | `vertical_velocity` | cf | Vertical vind model levels | m/s |
| `x_wind_ml` | `u_component_of_wind` | cf | Wind along x-coordinate on model levels | m/s |
| `y_wind_ml` | `v_component_of_wind` | cf | Wind along y-coordinate on model levels | m/s |

## bkr/dmi/meps_postprocessed (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_pressure_at_sea_level` | `mean_sea_level_pressure` | cf |  | Pa |
| `air_temperature_2m` | `2m_temperature` | cf |  | K |
| `altitude` | `surface_altitude` | cf |  | m |
| `cloud_area_fraction` | `total_cloud_cover` | cf |  | 1 |
| `forecast_reference_time` | `init_time` | coordinate |  | nanoseconds since 1970-01-01 |
| `relative_humidity_2m` | `2m_relative_humidity` | cf |  | 1 |
| `wind_direction_10m` | `10m_wind_direction` | cf |  | degree |
| `wind_speed_10m` | `10m_wind_speed` | cf |  | m/s |

## bkr/dmi/scotland_woof (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `ACRUNOFF` | `accumulated_runoff` | long_name |  | kg m-2 |
| `ALBBCKSOL` | `background_albedo_corrected_for_solar_angle` | long_name |  |  |
| `ALBSOL` | `albedo_corrected_for_solar_angle` | long_name |  |  |
| `CLDFRA_BL` | `cloud_fraction_pbl` | long_name |  |  |
| `COSALPHA` | `local_cosine_of_map_rotation` | long_name |  |  |
| `COV` | `total_water_liquid_water_pottemp_covariance` | long_name |  | K kg/kg |
| `DEW` | `dew_mixing_ratio_at_the_surface` | long_name |  | kg kg-1 |
| `E` | `coriolis_cosine_latitude_term` | long_name |  | s-1 |
| `EL_PBL` | `length_scale_from_pbl` | long_name |  | m |
| `EXCH_H` | `scalar_exchange_coefficients` | long_name |  | m2 s-1 |
| `EXCH_M` | `exchange_coefficients` | long_name |  | m2 s-1 |
| `F` | `coriolis_sine_latitude_term` | long_name |  | s-1 |
| `GLW` | `downward_long_wave_flux_at_ground_surface` | long_name |  | W m-2 |
| `GRAUPELNC` | `accumulated_total_grid_scale_graupel` | long_name |  | mm |
| `GRDFLX` | `ground_heat_flux` | long_name |  | W m-2 |
| `GSW` | `net_shortwave_flux_at_ground` | long_name |  | W m-2 |
| `HAILNC` | `accumulated_total_grid_scale_hail` | long_name |  | mm |
| `HFX` | `upward_heat_flux_at_the_surface` | long_name |  | W m-2 |
| `HGT` | `geopotential_at_surface` | long_name+geopotential+units |  | m -> m**2 s**-2 |
| `ISLTYP` | `dominant_soil_category` | long_name |  |  |
| `ITIMESTEP` | `itimestep` | tidied |  |  |
| `IVGTYP` | `dominant_vegetation_category` | long_name |  |  |
| `KEEPFR3DFLAG` | `flag_1_frozen_soil_yes_0_no` | long_name |  |  |
| `KPBL` | `level_of_pbl_top` | long_name |  |  |
| `KTOP_PLUME` | `k_level_of_highest_pentrating_plume` | long_name |  |  |
| `LANDMASK` | `land_mask` | long_name |  |  |
| `LH` | `latent_heat_flux_at_the_surface` | long_name |  | W m-2 |
| `LU_INDEX` | `land_use_category` | long_name |  |  |
| `MAPFAC_M` | `map_scale_factor_on_mass_grid` | long_name |  |  |
| `MAPFAC_U` | `map_scale_factor_on_u_grid` | long_name |  |  |
| `MAPFAC_V` | `map_scale_factor_on_v_grid` | long_name |  |  |
| `MAXMF` | `maximum_mass_flux` | long_name |  | m/s * area |
| `MAXWIDTH` | `maximum_plume_width` | long_name |  | m |
| `MU` | `perturbation_dry_air_mass_in_column` | long_name |  | Pa |
| `MUB` | `base_state_dry_air_mass_in_column` | long_name |  | Pa |
| `OLR` | `toa_outgoing_long_wave` | long_name |  | W m-2 |
| `P` | `perturbation_pressure` | long_name |  | Pa |
| `PB` | `base_state_pressure` | long_name |  | Pa |
| `PBLH` | `pbl_height` | long_name |  | m |
| `PH` | `perturbation_geopotential` | long_name |  | m2 s-2 |
| `PHB` | `base_state_geopotential` | long_name |  | m2 s-2 |
| `PRECIPFR` | `step_frozen_precip_mm` | long_name |  |  |
| `PSFC` | `sfc_pressure` | long_name |  | Pa |
| `PSIH` | `similarity_stability_function_for_heat` | long_name |  |  |
| `PSIM` | `similarity_stability_function_for_momentum` | long_name |  |  |
| `P_TOP` | `pressure_top_of_the_model` | long_name |  | Pa |
| `Q2` | `qv_at_2_m` | long_name |  | kg kg-1 |
| `QCG` | `cloud_water_mixing_ratio_at_the_ground_surface` | long_name |  | kg kg-1 |
| `QCLOUD` | `cloud_water_mixing_ratio` | long_name |  | kg kg-1 |
| `QC_BL` | `cloud_water_mixing_ratio_in_pbl_schemes` | long_name |  | kg kg-1 |
| `QFX` | `upward_moisture_flux_at_the_surface` | long_name |  | kg m-2 s-1 |
| `QGRAUP` | `graupel_mixing_ratio` | long_name |  | kg kg-1 |
| `QICE` | `ice_mixing_ratio` | long_name |  | kg kg-1 |
| `QI_BL` | `cloud_ice_mixing_ratio_in_pbl_schemes` | long_name |  | kg kg-1 |
| `QKE` | `twice_tke_from_mynn` | long_name |  | m2 s-2 |
| `QNCLOUD` | `cloud_water_number_concentration` | long_name |  |   kg(-1) |
| `QNICE` | `ice_number_concentration` | long_name |  |   kg-1 |
| `QNIFA` | `ice_friendly_aerosol_number_con` | long_name |  |   kg(-1) |
| `QNIFA2D` | `surface_dust_number_conc_emission` | long_name |  | kg-1 s-1 |
| `QNRAIN` | `rain_number_concentration` | long_name |  |   kg(-1) |
| `QNWFA` | `water_friendly_aerosol_number_con` | long_name |  |   kg(-1) |
| `QNWFA2D` | `surface_aerosol_number_conc_emission` | long_name |  | kg-1 s-1 |
| `QRAIN` | `rain_water_mixing_ratio` | long_name |  | kg kg-1 |
| `QSG` | `surface_saturation_water_vapor_mixing_ratio` | long_name |  | kg kg-1 |
| `QSNOW` | `snow_mixing_ratio` | long_name |  | kg kg-1 |
| `QSQ` | `total_water_variance` | long_name |  | (kg/kg)**2 |
| `QVAPOR` | `water_vapor_mixing_ratio` | long_name |  | kg kg-1 |
| `QVG` | `water_vapor_mixing_ratio_at_the_surface` | long_name |  | kg kg-1 |
| `RAINC` | `accumulated_total_cumulus_precipitation` | long_name |  | mm |
| `RAINNC` | `accumulated_total_grid_scale_precipitation` | long_name |  | mm |
| `RAINSH` | `accumulated_shallow_cumulus_precipitation` | long_name |  | mm |
| `REFL_10CM` | `radar_reflectivity` | long_name |  | dBZ |
| `RHOSNF` | `density_of_frozen_precip` | long_name |  | kg/m^3 |
| `RMOL` | `1_monin_ob_length` | long_name |  |  |
| `RTHRATLW` | `RTHRATLW` | kept |  |  |
| `RTHRATSW` | `RTHRATSW` | kept |  |  |
| `RUC_INFILTR` | `woof_diagnostic_lsmruc_infiltration_flux` | long_name |  | m s-1 |
| `RUC_RUNOFF1` | `woof_diagnostic_lsmruc_surface_runoff_rate` | long_name |  | m s-1 |
| `RUC_RUNOFF2` | `woof_diagnostic_lsmruc_underground_runoff_rate` | long_name |  | m s-1 |
| `RUC_SMELT` | `woof_diagnostic_lsmruc_snow_melt_water_flux` | long_name |  | m s-1 |
| `SEAICE` | `sea_ice_flag` | long_name |  |  |
| `SFCEVP` | `accumulated_surface_evaporation` | long_name |  | kg m-2 |
| `SFCEXC` | `surface_exchange_coefficient` | long_name |  | m s-1 |
| `SH2O` | `soil_liquid_water` | long_name |  | m3 m-3 |
| `SH3D` | `stability_function_for_heat` | long_name |  |  |
| `SINALPHA` | `local_sine_of_map_rotation` | long_name |  |  |
| `SM3D` | `stability_function_for_momentum` | long_name |  |  |
| `SMFR3D` | `soil_ice` | long_name |  |  |
| `SMOIS` | `soil_moisture` | long_name |  | m3 m-3 |
| `SNOW` | `snow_water_equivalent` | long_name |  | kg m-2 |
| `SNOWC` | `flag_indicating_snow_coverage` | long_name |  |  |
| `SNOWFALLAC` | `run_total_accumulated_snowfall_mm` | long_name |  |  |
| `SNOWH` | `physical_snow_depth` | long_name |  | m |
| `SNOWNC` | `accumulated_total_grid_scale_snow_and_ice` | long_name |  | mm |
| `SOILT1` | `temperature_inside_snow` | long_name |  | K |
| `SWDOWN` | `downward_short_wave_flux_at_ground_surface` | long_name |  | W m-2 |
| `T` | `temperature` | override |  | K |
| `T2` | `temp_at_2_m` | long_name |  | K |
| `TH2` | `pot_temp_at_2_m` | long_name |  | K |
| `TMN` | `soil_temperature_at_lower_boundary` | long_name |  | K |
| `TSK` | `surface_skin_temperature` | long_name |  | K |
| `TSLB` | `soil_temperature` | long_name |  | K |
| `TSNAV` | `average_snow_temperature` | long_name+units |  | C -> K |
| `TSQ` | `liquid_water_pottemp_variance` | long_name |  | K2 |
| `Times` | `Times` | kept |  |  |
| `U` | `u_component_of_wind` | override |  | m s-1 |
| `U10` | `u_at_10_m` | long_name |  | m s-1 |
| `UP_HELI_MAX` | `max_updraft_helicity` | long_name |  | m2 s-2 |
| `UST` | `u_in_similarity_theory` | long_name |  | m s-1 |
| `V` | `v_component_of_wind` | override |  | m s-1 |
| `V10` | `v_at_10_m` | long_name |  | m s-1 |
| `VEGFRA` | `vegetation_fraction` | long_name |  |  |
| `W` | `z_wind_component` | long_name |  | m s-1 |
| `XKHH` | `horizontal_heat_eddy_viscosity` | long_name |  | m2 s-1 |
| `XKMH` | `horizontal_momentum_eddy_viscosity` | long_name |  | m2 s-1 |
| `XLAT` | `xlat` | clash |  | degree_north |
| `XLAT_U` | `xlat_u` | clash |  | degree_north |
| `XLAT_V` | `xlat_v` | clash |  | degree_north |
| `XLONG` | `xlong` | clash |  | degree_east |
| `XLONG_U` | `xlong_u` | clash |  | degree_east |
| `XLONG_V` | `xlong_v` | clash |  | degree_east |
| `XTIME` | `minutes_since_2026_10_05_06_00_00` | long_name |  | minutes since 2026-10-05T06:00:00 |
| `ZNU` | `eta_values_on_half_levels` | long_name |  |  |
| `ZNW` | `eta_values_on_full_levels` | long_name |  |  |
| `ZTOP_PLUME` | `height_of_tallest_plume` | long_name |  | m |

## bkr/dmi/uk_woof (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `ACRUNOFF` | `accumulated_runoff` | long_name |  | kg m-2 |
| `ALBBCKSOL` | `background_albedo_corrected_for_solar_angle` | long_name |  |  |
| `ALBSOL` | `albedo_corrected_for_solar_angle` | long_name |  |  |
| `CLDFRA_BL` | `cloud_fraction_pbl` | long_name |  |  |
| `COSALPHA` | `local_cosine_of_map_rotation` | long_name |  |  |
| `COV` | `total_water_liquid_water_pottemp_covariance` | long_name |  | K kg/kg |
| `DEW` | `dew_mixing_ratio_at_the_surface` | long_name |  | kg kg-1 |
| `E` | `coriolis_cosine_latitude_term` | long_name |  | s-1 |
| `EL_PBL` | `length_scale_from_pbl` | long_name |  | m |
| `EXCH_H` | `scalar_exchange_coefficients` | long_name |  | m2 s-1 |
| `EXCH_M` | `exchange_coefficients` | long_name |  | m2 s-1 |
| `F` | `coriolis_sine_latitude_term` | long_name |  | s-1 |
| `GLW` | `downward_long_wave_flux_at_ground_surface` | long_name |  | W m-2 |
| `GRAUPELNC` | `accumulated_total_grid_scale_graupel` | long_name |  | mm |
| `GRDFLX` | `ground_heat_flux` | long_name |  | W m-2 |
| `GSW` | `net_shortwave_flux_at_ground` | long_name |  | W m-2 |
| `HAILNC` | `accumulated_total_grid_scale_hail` | long_name |  | mm |
| `HFX` | `upward_heat_flux_at_the_surface` | long_name |  | W m-2 |
| `HGT` | `geopotential_at_surface` | long_name+geopotential+units |  | m -> m**2 s**-2 |
| `ISLTYP` | `dominant_soil_category` | long_name |  |  |
| `ITIMESTEP` | `itimestep` | tidied |  |  |
| `IVGTYP` | `dominant_vegetation_category` | long_name |  |  |
| `KEEPFR3DFLAG` | `flag_1_frozen_soil_yes_0_no` | long_name |  |  |
| `KPBL` | `level_of_pbl_top` | long_name |  |  |
| `KTOP_PLUME` | `k_level_of_highest_pentrating_plume` | long_name |  |  |
| `LANDMASK` | `land_mask` | long_name |  |  |
| `LH` | `latent_heat_flux_at_the_surface` | long_name |  | W m-2 |
| `LU_INDEX` | `land_use_category` | long_name |  |  |
| `MAPFAC_M` | `map_scale_factor_on_mass_grid` | long_name |  |  |
| `MAPFAC_U` | `map_scale_factor_on_u_grid` | long_name |  |  |
| `MAPFAC_V` | `map_scale_factor_on_v_grid` | long_name |  |  |
| `MAXMF` | `maximum_mass_flux` | long_name |  | m/s * area |
| `MAXWIDTH` | `maximum_plume_width` | long_name |  | m |
| `MU` | `perturbation_dry_air_mass_in_column` | long_name |  | Pa |
| `MUB` | `base_state_dry_air_mass_in_column` | long_name |  | Pa |
| `OLR` | `toa_outgoing_long_wave` | long_name |  | W m-2 |
| `P` | `perturbation_pressure` | long_name |  | Pa |
| `PB` | `base_state_pressure` | long_name |  | Pa |
| `PBLH` | `pbl_height` | long_name |  | m |
| `PH` | `perturbation_geopotential` | long_name |  | m2 s-2 |
| `PHB` | `base_state_geopotential` | long_name |  | m2 s-2 |
| `PRECIPFR` | `step_frozen_precip_mm` | long_name |  |  |
| `PSFC` | `sfc_pressure` | long_name |  | Pa |
| `PSIH` | `similarity_stability_function_for_heat` | long_name |  |  |
| `PSIM` | `similarity_stability_function_for_momentum` | long_name |  |  |
| `P_TOP` | `pressure_top_of_the_model` | long_name |  | Pa |
| `Q2` | `qv_at_2_m` | long_name |  | kg kg-1 |
| `QCG` | `cloud_water_mixing_ratio_at_the_ground_surface` | long_name |  | kg kg-1 |
| `QCLOUD` | `cloud_water_mixing_ratio` | long_name |  | kg kg-1 |
| `QC_BL` | `cloud_water_mixing_ratio_in_pbl_schemes` | long_name |  | kg kg-1 |
| `QFX` | `upward_moisture_flux_at_the_surface` | long_name |  | kg m-2 s-1 |
| `QGRAUP` | `graupel_mixing_ratio` | long_name |  | kg kg-1 |
| `QICE` | `ice_mixing_ratio` | long_name |  | kg kg-1 |
| `QI_BL` | `cloud_ice_mixing_ratio_in_pbl_schemes` | long_name |  | kg kg-1 |
| `QKE` | `twice_tke_from_mynn` | long_name |  | m2 s-2 |
| `QNCLOUD` | `cloud_water_number_concentration` | long_name |  |   kg(-1) |
| `QNICE` | `ice_number_concentration` | long_name |  |   kg-1 |
| `QNIFA` | `ice_friendly_aerosol_number_con` | long_name |  |   kg(-1) |
| `QNIFA2D` | `surface_dust_number_conc_emission` | long_name |  | kg-1 s-1 |
| `QNRAIN` | `rain_number_concentration` | long_name |  |   kg(-1) |
| `QNWFA` | `water_friendly_aerosol_number_con` | long_name |  |   kg(-1) |
| `QNWFA2D` | `surface_aerosol_number_conc_emission` | long_name |  | kg-1 s-1 |
| `QRAIN` | `rain_water_mixing_ratio` | long_name |  | kg kg-1 |
| `QSG` | `surface_saturation_water_vapor_mixing_ratio` | long_name |  | kg kg-1 |
| `QSNOW` | `snow_mixing_ratio` | long_name |  | kg kg-1 |
| `QSQ` | `total_water_variance` | long_name |  | (kg/kg)**2 |
| `QVAPOR` | `water_vapor_mixing_ratio` | long_name |  | kg kg-1 |
| `QVG` | `water_vapor_mixing_ratio_at_the_surface` | long_name |  | kg kg-1 |
| `RAINC` | `accumulated_total_cumulus_precipitation` | long_name |  | mm |
| `RAINNC` | `accumulated_total_grid_scale_precipitation` | long_name |  | mm |
| `RAINSH` | `accumulated_shallow_cumulus_precipitation` | long_name |  | mm |
| `REFL_10CM` | `radar_reflectivity` | long_name |  | dBZ |
| `RHOSNF` | `density_of_frozen_precip` | long_name |  | kg/m^3 |
| `RMOL` | `1_monin_ob_length` | long_name |  |  |
| `RTHRATLW` | `RTHRATLW` | kept |  |  |
| `RTHRATSW` | `RTHRATSW` | kept |  |  |
| `RUC_INFILTR` | `woof_diagnostic_lsmruc_infiltration_flux` | long_name |  | m s-1 |
| `RUC_RUNOFF1` | `woof_diagnostic_lsmruc_surface_runoff_rate` | long_name |  | m s-1 |
| `RUC_RUNOFF2` | `woof_diagnostic_lsmruc_underground_runoff_rate` | long_name |  | m s-1 |
| `RUC_SMELT` | `woof_diagnostic_lsmruc_snow_melt_water_flux` | long_name |  | m s-1 |
| `SEAICE` | `sea_ice_flag` | long_name |  |  |
| `SFCEVP` | `accumulated_surface_evaporation` | long_name |  | kg m-2 |
| `SFCEXC` | `surface_exchange_coefficient` | long_name |  | m s-1 |
| `SH2O` | `soil_liquid_water` | long_name |  | m3 m-3 |
| `SH3D` | `stability_function_for_heat` | long_name |  |  |
| `SINALPHA` | `local_sine_of_map_rotation` | long_name |  |  |
| `SM3D` | `stability_function_for_momentum` | long_name |  |  |
| `SMFR3D` | `soil_ice` | long_name |  |  |
| `SMOIS` | `soil_moisture` | long_name |  | m3 m-3 |
| `SNOW` | `snow_water_equivalent` | long_name |  | kg m-2 |
| `SNOWC` | `flag_indicating_snow_coverage` | long_name |  |  |
| `SNOWFALLAC` | `run_total_accumulated_snowfall_mm` | long_name |  |  |
| `SNOWH` | `physical_snow_depth` | long_name |  | m |
| `SNOWNC` | `accumulated_total_grid_scale_snow_and_ice` | long_name |  | mm |
| `SOILT1` | `temperature_inside_snow` | long_name |  | K |
| `SWDOWN` | `downward_short_wave_flux_at_ground_surface` | long_name |  | W m-2 |
| `T` | `temperature` | override |  | K |
| `T2` | `temp_at_2_m` | long_name |  | K |
| `TH2` | `pot_temp_at_2_m` | long_name |  | K |
| `TMN` | `soil_temperature_at_lower_boundary` | long_name |  | K |
| `TSK` | `surface_skin_temperature` | long_name |  | K |
| `TSLB` | `soil_temperature` | long_name |  | K |
| `TSNAV` | `average_snow_temperature` | long_name+units |  | C -> K |
| `TSQ` | `liquid_water_pottemp_variance` | long_name |  | K2 |
| `Times` | `Times` | kept |  |  |
| `U` | `u_component_of_wind` | override |  | m s-1 |
| `U10` | `u_at_10_m` | long_name |  | m s-1 |
| `UP_HELI_MAX` | `max_updraft_helicity` | long_name |  | m2 s-2 |
| `UST` | `u_in_similarity_theory` | long_name |  | m s-1 |
| `V` | `v_component_of_wind` | override |  | m s-1 |
| `V10` | `v_at_10_m` | long_name |  | m s-1 |
| `VEGFRA` | `vegetation_fraction` | long_name |  |  |
| `W` | `z_wind_component` | long_name |  | m s-1 |
| `XKHH` | `horizontal_heat_eddy_viscosity` | long_name |  | m2 s-1 |
| `XKMH` | `horizontal_momentum_eddy_viscosity` | long_name |  | m2 s-1 |
| `XLAT` | `xlat` | clash |  | degree_north |
| `XLAT_U` | `xlat_u` | clash |  | degree_north |
| `XLAT_V` | `xlat_v` | clash |  | degree_north |
| `XLONG` | `xlong` | clash |  | degree_east |
| `XLONG_U` | `xlong_u` | clash |  | degree_east |
| `XLONG_V` | `xlong_v` | clash |  | degree_east |
| `XTIME` | `minutes_since_2026_10_05_06_00_00` | long_name |  | minutes since 2026-10-05T06:00:00 |
| `ZNU` | `eta_values_on_half_levels` | long_name |  |  |
| `ZNW` | `eta_values_on_full_levels` | long_name |  |  |
| `ZTOP_PLUME` | `height_of_tallest_plume` | long_name |  | m |

## bkr/dmi/virtualized/hafs_a_parent (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `clt/instant/tmp` | `clt/instant/tmp` | kept | temperature |  |
| `entire_atm/avg/tcdc` | `entire_atm/avg/tcdc` | kept | totalcloudcover |  |
| `entire_atm/avg3h/tcdc` | `entire_atm/avg3h/tcdc` | kept | totalcloudcover |  |
| `entire_atm/instant/pwat` | `entire_atm/instant/precipitable_water` | eccodes | precipitablewater |  |
| `entire_atm/instant/refc` | `entire_atm/instant/composite_reflectivity` | abbreviation | compositereflectivity |  |
| `entire_atm/instant/tcdc` | `entire_atm/instant/tcdc` | kept | totalcloudcover |  |
| `hag/instant/dpt` | `hag/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `hag/instant/hlcy` | `hag/instant/storm_relative_helicity` | eccodes | stormrelativehelicity |  |
| `hag/instant/rh` | `hag/instant/rh` | kept | relativehumidity |  |
| `hag/instant/spfh` | `hag/instant/spfh` | kept | specifichumidity |  |
| `hag/instant/tmp` | `hag/instant/tmp` | kept | temperature |  |
| `hag/instant/ugrd` | `hag/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hag/instant/vgrd` | `hag/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `hag/max/hag` | `hag/max/height_above_ground` | coordinate |  |  |
| `hag/max/mxuphl` | `hag/max/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max/wind` | `hag/max/10m_wind_speed` | eccodes | windspeed |  |
| `hag/max1h/hag` | `hag/max1h/height_above_ground` | coordinate |  |  |
| `hag/max1h/mxuphl` | `hag/max1h/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max1h/wind` | `hag/max1h/10m_wind_speed` | eccodes | windspeed |  |
| `isobar/instant/absv` | `isobar/instant/absolute_vorticity` | eccodes | absolutevorticity |  |
| `isobar/instant/clwmr` | `isobar/instant/cloud_mixing_ratio` | eccodes | cloudmixingratio |  |
| `isobar/instant/dpt` | `isobar/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `isobar/instant/dzdt` | `isobar/instant/dzdt` | kept | verticalvelocitygeometric |  |
| `isobar/instant/grle` | `isobar/instant/graupel` | eccodes | graupel |  |
| `isobar/instant/hgt` | `isobar/instant/hgt` | kept | geopotentialheight |  |
| `isobar/instant/icmr` | `isobar/instant/ice_water_mixing_ratio` | eccodes | icewatermixingratio |  |
| `isobar/instant/isobar` | `isobar/instant/pressure_level` | coordinate |  |  |
| `isobar/instant/rh` | `isobar/instant/rh` | kept | relativehumidity |  |
| `isobar/instant/rwmr` | `isobar/instant/rain_mixing_ratio` | eccodes | rainmixingratio |  |
| `isobar/instant/snmr` | `isobar/instant/snow_mixing_ratio` | eccodes | snowmixingratio |  |
| `isobar/instant/spfh` | `isobar/instant/spfh` | kept | specifichumidity |  |
| `isobar/instant/tmp` | `isobar/instant/tmp` | kept | temperature |  |
| `isobar/instant/ugrd` | `isobar/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `isobar/instant/vgrd` | `isobar/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `isobar/instant/vvel` | `isobar/instant/vvel` | kept | verticalvelocitypressure |  |
| `isobar/max/maxdvv` | `isobar/max/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `isobar/max/maxuvv` | `isobar/max/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `isobar/max1h/maxdvv` | `isobar/max1h/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `isobar/max1h/maxuvv` | `isobar/max1h/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `msl/instant/mslet` | `msl/instant/mslet` | kept | mslp |  |
| `msl/instant/prmsl` | `msl/instant/mean_sea_level_pressure` | eccodes | pressurereducedmsl |  |
| `nominal_top/avg/uswrf` | `nominal_top/avg/uswrf` | kept | upwardshortwaveradiationflux |  |
| `nominal_top/avg3h/uswrf` | `nominal_top/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/acc/acpcp` | `sfc/acc/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc/apcp` | `sfc/acc/apcp` | kept | totalprecipitation |  |
| `sfc/acc/ncpcp` | `sfc/acc/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc102h/acpcp` | `sfc/acc102h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc102h/apcp` | `sfc/acc102h/apcp` | kept | totalprecipitation |  |
| `sfc/acc102h/ncpcp` | `sfc/acc102h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc105h/acpcp` | `sfc/acc105h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc105h/apcp` | `sfc/acc105h/apcp` | kept | totalprecipitation |  |
| `sfc/acc105h/ncpcp` | `sfc/acc105h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc108h/acpcp` | `sfc/acc108h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc108h/apcp` | `sfc/acc108h/apcp` | kept | totalprecipitation |  |
| `sfc/acc108h/ncpcp` | `sfc/acc108h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc111h/acpcp` | `sfc/acc111h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc111h/apcp` | `sfc/acc111h/apcp` | kept | totalprecipitation |  |
| `sfc/acc111h/ncpcp` | `sfc/acc111h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc114h/acpcp` | `sfc/acc114h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc114h/apcp` | `sfc/acc114h/apcp` | kept | totalprecipitation |  |
| `sfc/acc114h/ncpcp` | `sfc/acc114h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc117h/acpcp` | `sfc/acc117h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc117h/apcp` | `sfc/acc117h/apcp` | kept | totalprecipitation |  |
| `sfc/acc117h/ncpcp` | `sfc/acc117h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc120h/acpcp` | `sfc/acc120h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc120h/apcp` | `sfc/acc120h/apcp` | kept | totalprecipitation |  |
| `sfc/acc120h/ncpcp` | `sfc/acc120h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc123h/acpcp` | `sfc/acc123h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc123h/apcp` | `sfc/acc123h/apcp` | kept | totalprecipitation |  |
| `sfc/acc123h/ncpcp` | `sfc/acc123h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc126h/acpcp` | `sfc/acc126h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc126h/apcp` | `sfc/acc126h/apcp` | kept | totalprecipitation |  |
| `sfc/acc126h/ncpcp` | `sfc/acc126h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc12h/acpcp` | `sfc/acc12h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc12h/apcp` | `sfc/acc12h/apcp` | kept | totalprecipitation |  |
| `sfc/acc12h/ncpcp` | `sfc/acc12h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc15h/acpcp` | `sfc/acc15h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc15h/apcp` | `sfc/acc15h/apcp` | kept | totalprecipitation |  |
| `sfc/acc15h/ncpcp` | `sfc/acc15h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc18h/acpcp` | `sfc/acc18h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc18h/apcp` | `sfc/acc18h/apcp` | kept | totalprecipitation |  |
| `sfc/acc18h/ncpcp` | `sfc/acc18h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc21h/acpcp` | `sfc/acc21h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc21h/apcp` | `sfc/acc21h/apcp` | kept | totalprecipitation |  |
| `sfc/acc21h/ncpcp` | `sfc/acc21h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc3h/acpcp` | `sfc/acc3h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc3h/apcp` | `sfc/acc3h/apcp` | kept | totalprecipitation |  |
| `sfc/acc3h/ncpcp` | `sfc/acc3h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc63h/acpcp` | `sfc/acc63h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc63h/apcp` | `sfc/acc63h/apcp` | kept | totalprecipitation |  |
| `sfc/acc63h/ncpcp` | `sfc/acc63h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc66h/acpcp` | `sfc/acc66h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc66h/apcp` | `sfc/acc66h/apcp` | kept | totalprecipitation |  |
| `sfc/acc66h/ncpcp` | `sfc/acc66h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc69h/acpcp` | `sfc/acc69h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc69h/apcp` | `sfc/acc69h/apcp` | kept | totalprecipitation |  |
| `sfc/acc69h/ncpcp` | `sfc/acc69h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc6h/acpcp` | `sfc/acc6h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc6h/apcp` | `sfc/acc6h/apcp` | kept | totalprecipitation |  |
| `sfc/acc6h/ncpcp` | `sfc/acc6h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc72h/acpcp` | `sfc/acc72h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc72h/apcp` | `sfc/acc72h/apcp` | kept | totalprecipitation |  |
| `sfc/acc72h/ncpcp` | `sfc/acc72h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc75h/acpcp` | `sfc/acc75h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc75h/apcp` | `sfc/acc75h/apcp` | kept | totalprecipitation |  |
| `sfc/acc75h/ncpcp` | `sfc/acc75h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc78h/acpcp` | `sfc/acc78h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc78h/apcp` | `sfc/acc78h/apcp` | kept | totalprecipitation |  |
| `sfc/acc78h/ncpcp` | `sfc/acc78h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc81h/acpcp` | `sfc/acc81h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc81h/apcp` | `sfc/acc81h/apcp` | kept | totalprecipitation |  |
| `sfc/acc81h/ncpcp` | `sfc/acc81h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc84h/acpcp` | `sfc/acc84h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc84h/apcp` | `sfc/acc84h/apcp` | kept | totalprecipitation |  |
| `sfc/acc84h/ncpcp` | `sfc/acc84h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc87h/acpcp` | `sfc/acc87h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc87h/apcp` | `sfc/acc87h/apcp` | kept | totalprecipitation |  |
| `sfc/acc87h/ncpcp` | `sfc/acc87h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc90h/acpcp` | `sfc/acc90h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc90h/apcp` | `sfc/acc90h/apcp` | kept | totalprecipitation |  |
| `sfc/acc90h/ncpcp` | `sfc/acc90h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc93h/acpcp` | `sfc/acc93h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc93h/apcp` | `sfc/acc93h/apcp` | kept | totalprecipitation |  |
| `sfc/acc93h/ncpcp` | `sfc/acc93h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc96h/acpcp` | `sfc/acc96h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc96h/apcp` | `sfc/acc96h/apcp` | kept | totalprecipitation |  |
| `sfc/acc96h/ncpcp` | `sfc/acc96h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc99h/acpcp` | `sfc/acc99h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc99h/apcp` | `sfc/acc99h/apcp` | kept | totalprecipitation |  |
| `sfc/acc99h/ncpcp` | `sfc/acc99h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc9h/acpcp` | `sfc/acc9h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc9h/apcp` | `sfc/acc9h/apcp` | kept | totalprecipitation |  |
| `sfc/acc9h/ncpcp` | `sfc/acc9h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/avg/cprat` | `sfc/avg/cprat` | kept | convectiveprecipitationwmo |  |
| `sfc/avg/dswrf` | `sfc/avg/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg/prate` | `sfc/avg/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/avg/uswrf` | `sfc/avg/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/avg3h/cprat` | `sfc/avg3h/cprat` | kept | convectiveprecipitationwmo |  |
| `sfc/avg3h/dswrf` | `sfc/avg3h/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg3h/prate` | `sfc/avg3h/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/avg3h/uswrf` | `sfc/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/cape` | `sfc/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `sfc/instant/cin` | `sfc/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `sfc/instant/dswrf` | `sfc/instant/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/instant/elon` | `sfc/instant/edge_midpoint_longitude` | eccodes | eastlongitude |  |
| `sfc/instant/fricv` | `sfc/instant/frictional_velocity` | eccodes | frictionalvelocityncep |  |
| `sfc/instant/gust` | `sfc/instant/wind_speed` | eccodes | windgust |  |
| `sfc/instant/hgt` | `sfc/instant/hgt` | kept | geopotentialheight |  |
| `sfc/instant/hpbl` | `sfc/instant/planetary_boundary_layer_height` | eccodes | planetaryboundarylayerheightncep |  |
| `sfc/instant/land` | `sfc/instant/land` | kept | landcover |  |
| `sfc/instant/lhtfl` | `sfc/instant/lhtfl` | kept | latentheatnetflux |  |
| `sfc/instant/nlat` | `sfc/instant/nlat` | kept | latitude |  |
| `sfc/instant/pres` | `sfc/instant/air_pressure` | abbreviation | pressure |  |
| `sfc/instant/sfcr` | `sfc/instant/sfcr` | kept | surfaceroughness |  |
| `sfc/instant/shtfl` | `sfc/instant/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/instant/tmp` | `sfc/instant/tmp` | kept | temperature |  |
| `sfc/instant/uflx` | `sfc/instant/uflx` | kept | momentumfluxu |  |
| `sfc/instant/uswrf` | `sfc/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/vflx` | `sfc/instant/vflx` | kept | momentumfluxv |  |
| `sfc/instant/wtmp` | `sfc/instant/sea_surface_temperature` | abbreviation | watertemperature |  |
| `tro/instant/hgt` | `tro/instant/hgt` | kept | geopotentialheight |  |
| `tro/instant/pot` | `tro/instant/pot` | kept | potentialtemperature |  |
| `tro/instant/pres` | `tro/instant/air_pressure` | abbreviation | pressure |  |
| `tro/instant/tmp` | `tro/instant/tmp` | kept | temperature |  |
| `tro/instant/ugrd` | `tro/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `tro/instant/vgrd` | `tro/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `tro/instant/vwsh` | `tro/instant/vertical_speed_shear` | eccodes | verticalspeedshearncep |  |

## bkr/dmi/virtualized/hafs_a_storm (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `clt/instant/tmp` | `clt/instant/tmp` | kept | temperature |  |
| `entire_atm/avg3h/tcdc` | `entire_atm/avg3h/tcdc` | kept | totalcloudcover |  |
| `entire_atm/instant/pwat` | `entire_atm/instant/precipitable_water` | eccodes | precipitablewater |  |
| `entire_atm/instant/refc` | `entire_atm/instant/composite_reflectivity` | abbreviation | compositereflectivity |  |
| `entire_atm/instant/tcdc` | `entire_atm/instant/tcdc` | kept | totalcloudcover |  |
| `hag/instant/dpt` | `hag/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `hag/instant/hlcy` | `hag/instant/storm_relative_helicity` | eccodes | stormrelativehelicity |  |
| `hag/instant/rh` | `hag/instant/rh` | kept | relativehumidity |  |
| `hag/instant/spfh` | `hag/instant/spfh` | kept | specifichumidity |  |
| `hag/instant/tmp` | `hag/instant/tmp` | kept | temperature |  |
| `hag/instant/ugrd` | `hag/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hag/instant/vgrd` | `hag/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `hag/max1h/hag` | `hag/max1h/height_above_ground` | coordinate |  |  |
| `hag/max1h/mxuphl` | `hag/max1h/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max1h/wind` | `hag/max1h/10m_wind_speed` | eccodes | windspeed |  |
| `isobar/instant/absv` | `isobar/instant/absolute_vorticity` | eccodes | absolutevorticity |  |
| `isobar/instant/clwmr` | `isobar/instant/cloud_mixing_ratio` | eccodes | cloudmixingratio |  |
| `isobar/instant/dpt` | `isobar/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `isobar/instant/dzdt` | `isobar/instant/dzdt` | kept | verticalvelocitygeometric |  |
| `isobar/instant/grle` | `isobar/instant/graupel` | eccodes | graupel |  |
| `isobar/instant/hgt` | `isobar/instant/hgt` | kept | geopotentialheight |  |
| `isobar/instant/icmr` | `isobar/instant/ice_water_mixing_ratio` | eccodes | icewatermixingratio |  |
| `isobar/instant/isobar` | `isobar/instant/pressure_level` | coordinate |  |  |
| `isobar/instant/rh` | `isobar/instant/rh` | kept | relativehumidity |  |
| `isobar/instant/rwmr` | `isobar/instant/rain_mixing_ratio` | eccodes | rainmixingratio |  |
| `isobar/instant/snmr` | `isobar/instant/snow_mixing_ratio` | eccodes | snowmixingratio |  |
| `isobar/instant/spfh` | `isobar/instant/spfh` | kept | specifichumidity |  |
| `isobar/instant/tmp` | `isobar/instant/tmp` | kept | temperature |  |
| `isobar/instant/ugrd` | `isobar/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `isobar/instant/vgrd` | `isobar/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `isobar/instant/vvel` | `isobar/instant/vvel` | kept | verticalvelocitypressure |  |
| `isobar/max1h/maxdvv` | `isobar/max1h/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `isobar/max1h/maxuvv` | `isobar/max1h/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `msl/instant/mslet` | `msl/instant/mslet` | kept | mslp |  |
| `msl/instant/prmsl` | `msl/instant/mean_sea_level_pressure` | eccodes | pressurereducedmsl |  |
| `nominal_top/avg3h/uswrf` | `nominal_top/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/acc102h/acpcp` | `sfc/acc102h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc102h/apcp` | `sfc/acc102h/apcp` | kept | totalprecipitation |  |
| `sfc/acc102h/ncpcp` | `sfc/acc102h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc105h/acpcp` | `sfc/acc105h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc105h/apcp` | `sfc/acc105h/apcp` | kept | totalprecipitation |  |
| `sfc/acc105h/ncpcp` | `sfc/acc105h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc108h/acpcp` | `sfc/acc108h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc108h/apcp` | `sfc/acc108h/apcp` | kept | totalprecipitation |  |
| `sfc/acc108h/ncpcp` | `sfc/acc108h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc111h/acpcp` | `sfc/acc111h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc111h/apcp` | `sfc/acc111h/apcp` | kept | totalprecipitation |  |
| `sfc/acc111h/ncpcp` | `sfc/acc111h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc114h/acpcp` | `sfc/acc114h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc114h/apcp` | `sfc/acc114h/apcp` | kept | totalprecipitation |  |
| `sfc/acc114h/ncpcp` | `sfc/acc114h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc117h/acpcp` | `sfc/acc117h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc117h/apcp` | `sfc/acc117h/apcp` | kept | totalprecipitation |  |
| `sfc/acc117h/ncpcp` | `sfc/acc117h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc120h/acpcp` | `sfc/acc120h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc120h/apcp` | `sfc/acc120h/apcp` | kept | totalprecipitation |  |
| `sfc/acc120h/ncpcp` | `sfc/acc120h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc123h/acpcp` | `sfc/acc123h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc123h/apcp` | `sfc/acc123h/apcp` | kept | totalprecipitation |  |
| `sfc/acc123h/ncpcp` | `sfc/acc123h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc126h/acpcp` | `sfc/acc126h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc126h/apcp` | `sfc/acc126h/apcp` | kept | totalprecipitation |  |
| `sfc/acc126h/ncpcp` | `sfc/acc126h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc3h/acpcp` | `sfc/acc3h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc3h/apcp` | `sfc/acc3h/apcp` | kept | totalprecipitation |  |
| `sfc/acc3h/ncpcp` | `sfc/acc3h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc63h/acpcp` | `sfc/acc63h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc63h/apcp` | `sfc/acc63h/apcp` | kept | totalprecipitation |  |
| `sfc/acc63h/ncpcp` | `sfc/acc63h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc66h/acpcp` | `sfc/acc66h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc66h/apcp` | `sfc/acc66h/apcp` | kept | totalprecipitation |  |
| `sfc/acc66h/ncpcp` | `sfc/acc66h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc69h/acpcp` | `sfc/acc69h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc69h/apcp` | `sfc/acc69h/apcp` | kept | totalprecipitation |  |
| `sfc/acc69h/ncpcp` | `sfc/acc69h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc72h/acpcp` | `sfc/acc72h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc72h/apcp` | `sfc/acc72h/apcp` | kept | totalprecipitation |  |
| `sfc/acc72h/ncpcp` | `sfc/acc72h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc75h/acpcp` | `sfc/acc75h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc75h/apcp` | `sfc/acc75h/apcp` | kept | totalprecipitation |  |
| `sfc/acc75h/ncpcp` | `sfc/acc75h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc78h/acpcp` | `sfc/acc78h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc78h/apcp` | `sfc/acc78h/apcp` | kept | totalprecipitation |  |
| `sfc/acc78h/ncpcp` | `sfc/acc78h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc81h/acpcp` | `sfc/acc81h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc81h/apcp` | `sfc/acc81h/apcp` | kept | totalprecipitation |  |
| `sfc/acc81h/ncpcp` | `sfc/acc81h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc84h/acpcp` | `sfc/acc84h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc84h/apcp` | `sfc/acc84h/apcp` | kept | totalprecipitation |  |
| `sfc/acc84h/ncpcp` | `sfc/acc84h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc87h/acpcp` | `sfc/acc87h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc87h/apcp` | `sfc/acc87h/apcp` | kept | totalprecipitation |  |
| `sfc/acc87h/ncpcp` | `sfc/acc87h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc90h/acpcp` | `sfc/acc90h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc90h/apcp` | `sfc/acc90h/apcp` | kept | totalprecipitation |  |
| `sfc/acc90h/ncpcp` | `sfc/acc90h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc93h/acpcp` | `sfc/acc93h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc93h/apcp` | `sfc/acc93h/apcp` | kept | totalprecipitation |  |
| `sfc/acc93h/ncpcp` | `sfc/acc93h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc96h/acpcp` | `sfc/acc96h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc96h/apcp` | `sfc/acc96h/apcp` | kept | totalprecipitation |  |
| `sfc/acc96h/ncpcp` | `sfc/acc96h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc99h/acpcp` | `sfc/acc99h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc99h/apcp` | `sfc/acc99h/apcp` | kept | totalprecipitation |  |
| `sfc/acc99h/ncpcp` | `sfc/acc99h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/avg3h/cprat` | `sfc/avg3h/cprat` | kept | convectiveprecipitationwmo |  |
| `sfc/avg3h/dswrf` | `sfc/avg3h/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg3h/prate` | `sfc/avg3h/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/avg3h/uswrf` | `sfc/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/cape` | `sfc/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `sfc/instant/cin` | `sfc/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `sfc/instant/dswrf` | `sfc/instant/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/instant/elon` | `sfc/instant/edge_midpoint_longitude` | eccodes | eastlongitude |  |
| `sfc/instant/fricv` | `sfc/instant/frictional_velocity` | eccodes | frictionalvelocityncep |  |
| `sfc/instant/gust` | `sfc/instant/wind_speed` | eccodes | windgust |  |
| `sfc/instant/hgt` | `sfc/instant/hgt` | kept | geopotentialheight |  |
| `sfc/instant/hpbl` | `sfc/instant/planetary_boundary_layer_height` | eccodes | planetaryboundarylayerheightncep |  |
| `sfc/instant/land` | `sfc/instant/land` | kept | landcover |  |
| `sfc/instant/lhtfl` | `sfc/instant/lhtfl` | kept | latentheatnetflux |  |
| `sfc/instant/nlat` | `sfc/instant/nlat` | kept | latitude |  |
| `sfc/instant/pres` | `sfc/instant/air_pressure` | abbreviation | pressure |  |
| `sfc/instant/sfcr` | `sfc/instant/sfcr` | kept | surfaceroughness |  |
| `sfc/instant/shtfl` | `sfc/instant/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/instant/tmp` | `sfc/instant/tmp` | kept | temperature |  |
| `sfc/instant/uflx` | `sfc/instant/uflx` | kept | momentumfluxu |  |
| `sfc/instant/uswrf` | `sfc/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/vflx` | `sfc/instant/vflx` | kept | momentumfluxv |  |
| `sfc/instant/wtmp` | `sfc/instant/sea_surface_temperature` | abbreviation | watertemperature |  |
| `tro/instant/hgt` | `tro/instant/hgt` | kept | geopotentialheight |  |
| `tro/instant/pot` | `tro/instant/pot` | kept | potentialtemperature |  |
| `tro/instant/pres` | `tro/instant/air_pressure` | abbreviation | pressure |  |
| `tro/instant/tmp` | `tro/instant/tmp` | kept | temperature |  |
| `tro/instant/ugrd` | `tro/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `tro/instant/vgrd` | `tro/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `tro/instant/vwsh` | `tro/instant/vertical_speed_shear` | eccodes | verticalspeedshearncep |  |

## bkr/dmi/virtualized/hafs_b_parent (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `clt/instant/tmp` | `clt/instant/tmp` | kept | temperature |  |
| `entire_atm/avg/tcdc` | `entire_atm/avg/tcdc` | kept | totalcloudcover |  |
| `entire_atm/avg3h/tcdc` | `entire_atm/avg3h/tcdc` | kept | totalcloudcover |  |
| `entire_atm/instant/pwat` | `entire_atm/instant/precipitable_water` | eccodes | precipitablewater |  |
| `entire_atm/instant/refc` | `entire_atm/instant/composite_reflectivity` | abbreviation | compositereflectivity |  |
| `entire_atm/instant/tcdc` | `entire_atm/instant/tcdc` | kept | totalcloudcover |  |
| `hag/instant/dpt` | `hag/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `hag/instant/hlcy` | `hag/instant/storm_relative_helicity` | eccodes | stormrelativehelicity |  |
| `hag/instant/rh` | `hag/instant/rh` | kept | relativehumidity |  |
| `hag/instant/spfh` | `hag/instant/spfh` | kept | specifichumidity |  |
| `hag/instant/tmp` | `hag/instant/tmp` | kept | temperature |  |
| `hag/instant/ugrd` | `hag/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hag/instant/vgrd` | `hag/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `hag/max/hag` | `hag/max/height_above_ground` | coordinate |  |  |
| `hag/max/mxuphl` | `hag/max/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max/wind` | `hag/max/10m_wind_speed` | eccodes | windspeed |  |
| `hag/max1h/hag` | `hag/max1h/height_above_ground` | coordinate |  |  |
| `hag/max1h/mxuphl` | `hag/max1h/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max1h/wind` | `hag/max1h/10m_wind_speed` | eccodes | windspeed |  |
| `isobar/instant/absv` | `isobar/instant/absolute_vorticity` | eccodes | absolutevorticity |  |
| `isobar/instant/clwmr` | `isobar/instant/cloud_mixing_ratio` | eccodes | cloudmixingratio |  |
| `isobar/instant/dpt` | `isobar/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `isobar/instant/dzdt` | `isobar/instant/dzdt` | kept | verticalvelocitygeometric |  |
| `isobar/instant/grle` | `isobar/instant/graupel` | eccodes | graupel |  |
| `isobar/instant/hgt` | `isobar/instant/hgt` | kept | geopotentialheight |  |
| `isobar/instant/icmr` | `isobar/instant/ice_water_mixing_ratio` | eccodes | icewatermixingratio |  |
| `isobar/instant/isobar` | `isobar/instant/pressure_level` | coordinate |  |  |
| `isobar/instant/rh` | `isobar/instant/rh` | kept | relativehumidity |  |
| `isobar/instant/rwmr` | `isobar/instant/rain_mixing_ratio` | eccodes | rainmixingratio |  |
| `isobar/instant/snmr` | `isobar/instant/snow_mixing_ratio` | eccodes | snowmixingratio |  |
| `isobar/instant/spfh` | `isobar/instant/spfh` | kept | specifichumidity |  |
| `isobar/instant/tmp` | `isobar/instant/tmp` | kept | temperature |  |
| `isobar/instant/ugrd` | `isobar/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `isobar/instant/vgrd` | `isobar/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `isobar/instant/vvel` | `isobar/instant/vvel` | kept | verticalvelocitypressure |  |
| `isobar/max/maxdvv` | `isobar/max/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `isobar/max/maxuvv` | `isobar/max/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `isobar/max1h/maxdvv` | `isobar/max1h/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `isobar/max1h/maxuvv` | `isobar/max1h/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `msl/instant/mslet` | `msl/instant/mslet` | kept | mslp |  |
| `msl/instant/prmsl` | `msl/instant/mean_sea_level_pressure` | eccodes | pressurereducedmsl |  |
| `nominal_top/avg/uswrf` | `nominal_top/avg/uswrf` | kept | upwardshortwaveradiationflux |  |
| `nominal_top/avg3h/uswrf` | `nominal_top/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/acc/acpcp` | `sfc/acc/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc/apcp` | `sfc/acc/apcp` | kept | totalprecipitation |  |
| `sfc/acc/ncpcp` | `sfc/acc/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc102h/acpcp` | `sfc/acc102h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc102h/apcp` | `sfc/acc102h/apcp` | kept | totalprecipitation |  |
| `sfc/acc102h/ncpcp` | `sfc/acc102h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc105h/acpcp` | `sfc/acc105h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc105h/apcp` | `sfc/acc105h/apcp` | kept | totalprecipitation |  |
| `sfc/acc105h/ncpcp` | `sfc/acc105h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc108h/acpcp` | `sfc/acc108h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc108h/apcp` | `sfc/acc108h/apcp` | kept | totalprecipitation |  |
| `sfc/acc108h/ncpcp` | `sfc/acc108h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc111h/acpcp` | `sfc/acc111h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc111h/apcp` | `sfc/acc111h/apcp` | kept | totalprecipitation |  |
| `sfc/acc111h/ncpcp` | `sfc/acc111h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc114h/acpcp` | `sfc/acc114h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc114h/apcp` | `sfc/acc114h/apcp` | kept | totalprecipitation |  |
| `sfc/acc114h/ncpcp` | `sfc/acc114h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc117h/acpcp` | `sfc/acc117h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc117h/apcp` | `sfc/acc117h/apcp` | kept | totalprecipitation |  |
| `sfc/acc117h/ncpcp` | `sfc/acc117h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc120h/acpcp` | `sfc/acc120h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc120h/apcp` | `sfc/acc120h/apcp` | kept | totalprecipitation |  |
| `sfc/acc120h/ncpcp` | `sfc/acc120h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc123h/acpcp` | `sfc/acc123h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc123h/apcp` | `sfc/acc123h/apcp` | kept | totalprecipitation |  |
| `sfc/acc123h/ncpcp` | `sfc/acc123h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc126h/acpcp` | `sfc/acc126h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc126h/apcp` | `sfc/acc126h/apcp` | kept | totalprecipitation |  |
| `sfc/acc126h/ncpcp` | `sfc/acc126h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc12h/acpcp` | `sfc/acc12h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc12h/apcp` | `sfc/acc12h/apcp` | kept | totalprecipitation |  |
| `sfc/acc12h/ncpcp` | `sfc/acc12h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc15h/acpcp` | `sfc/acc15h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc15h/apcp` | `sfc/acc15h/apcp` | kept | totalprecipitation |  |
| `sfc/acc15h/ncpcp` | `sfc/acc15h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc18h/acpcp` | `sfc/acc18h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc18h/apcp` | `sfc/acc18h/apcp` | kept | totalprecipitation |  |
| `sfc/acc18h/ncpcp` | `sfc/acc18h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc21h/acpcp` | `sfc/acc21h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc21h/apcp` | `sfc/acc21h/apcp` | kept | totalprecipitation |  |
| `sfc/acc21h/ncpcp` | `sfc/acc21h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc24h/acpcp` | `sfc/acc24h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc24h/apcp` | `sfc/acc24h/apcp` | kept | totalprecipitation |  |
| `sfc/acc24h/ncpcp` | `sfc/acc24h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc27h/acpcp` | `sfc/acc27h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc27h/apcp` | `sfc/acc27h/apcp` | kept | totalprecipitation |  |
| `sfc/acc27h/ncpcp` | `sfc/acc27h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc30h/acpcp` | `sfc/acc30h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc30h/apcp` | `sfc/acc30h/apcp` | kept | totalprecipitation |  |
| `sfc/acc30h/ncpcp` | `sfc/acc30h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc33h/acpcp` | `sfc/acc33h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc33h/apcp` | `sfc/acc33h/apcp` | kept | totalprecipitation |  |
| `sfc/acc33h/ncpcp` | `sfc/acc33h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc36h/acpcp` | `sfc/acc36h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc36h/apcp` | `sfc/acc36h/apcp` | kept | totalprecipitation |  |
| `sfc/acc36h/ncpcp` | `sfc/acc36h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc39h/acpcp` | `sfc/acc39h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc39h/apcp` | `sfc/acc39h/apcp` | kept | totalprecipitation |  |
| `sfc/acc39h/ncpcp` | `sfc/acc39h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc3h/acpcp` | `sfc/acc3h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc3h/apcp` | `sfc/acc3h/apcp` | kept | totalprecipitation |  |
| `sfc/acc3h/ncpcp` | `sfc/acc3h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc42h/acpcp` | `sfc/acc42h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc42h/apcp` | `sfc/acc42h/apcp` | kept | totalprecipitation |  |
| `sfc/acc42h/ncpcp` | `sfc/acc42h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc45h/acpcp` | `sfc/acc45h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc45h/apcp` | `sfc/acc45h/apcp` | kept | totalprecipitation |  |
| `sfc/acc45h/ncpcp` | `sfc/acc45h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc48h/acpcp` | `sfc/acc48h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc48h/apcp` | `sfc/acc48h/apcp` | kept | totalprecipitation |  |
| `sfc/acc48h/ncpcp` | `sfc/acc48h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc51h/acpcp` | `sfc/acc51h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc51h/apcp` | `sfc/acc51h/apcp` | kept | totalprecipitation |  |
| `sfc/acc51h/ncpcp` | `sfc/acc51h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc54h/acpcp` | `sfc/acc54h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc54h/apcp` | `sfc/acc54h/apcp` | kept | totalprecipitation |  |
| `sfc/acc54h/ncpcp` | `sfc/acc54h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc57h/acpcp` | `sfc/acc57h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc57h/apcp` | `sfc/acc57h/apcp` | kept | totalprecipitation |  |
| `sfc/acc57h/ncpcp` | `sfc/acc57h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc60h/acpcp` | `sfc/acc60h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc60h/apcp` | `sfc/acc60h/apcp` | kept | totalprecipitation |  |
| `sfc/acc60h/ncpcp` | `sfc/acc60h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc63h/acpcp` | `sfc/acc63h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc63h/apcp` | `sfc/acc63h/apcp` | kept | totalprecipitation |  |
| `sfc/acc63h/ncpcp` | `sfc/acc63h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc66h/acpcp` | `sfc/acc66h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc66h/apcp` | `sfc/acc66h/apcp` | kept | totalprecipitation |  |
| `sfc/acc66h/ncpcp` | `sfc/acc66h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc69h/acpcp` | `sfc/acc69h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc69h/apcp` | `sfc/acc69h/apcp` | kept | totalprecipitation |  |
| `sfc/acc69h/ncpcp` | `sfc/acc69h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc6h/acpcp` | `sfc/acc6h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc6h/apcp` | `sfc/acc6h/apcp` | kept | totalprecipitation |  |
| `sfc/acc6h/ncpcp` | `sfc/acc6h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc72h/acpcp` | `sfc/acc72h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc72h/apcp` | `sfc/acc72h/apcp` | kept | totalprecipitation |  |
| `sfc/acc72h/ncpcp` | `sfc/acc72h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc75h/acpcp` | `sfc/acc75h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc75h/apcp` | `sfc/acc75h/apcp` | kept | totalprecipitation |  |
| `sfc/acc75h/ncpcp` | `sfc/acc75h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc78h/acpcp` | `sfc/acc78h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc78h/apcp` | `sfc/acc78h/apcp` | kept | totalprecipitation |  |
| `sfc/acc78h/ncpcp` | `sfc/acc78h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc81h/acpcp` | `sfc/acc81h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc81h/apcp` | `sfc/acc81h/apcp` | kept | totalprecipitation |  |
| `sfc/acc81h/ncpcp` | `sfc/acc81h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc84h/acpcp` | `sfc/acc84h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc84h/apcp` | `sfc/acc84h/apcp` | kept | totalprecipitation |  |
| `sfc/acc84h/ncpcp` | `sfc/acc84h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc87h/acpcp` | `sfc/acc87h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc87h/apcp` | `sfc/acc87h/apcp` | kept | totalprecipitation |  |
| `sfc/acc87h/ncpcp` | `sfc/acc87h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc90h/acpcp` | `sfc/acc90h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc90h/apcp` | `sfc/acc90h/apcp` | kept | totalprecipitation |  |
| `sfc/acc90h/ncpcp` | `sfc/acc90h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc93h/acpcp` | `sfc/acc93h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc93h/apcp` | `sfc/acc93h/apcp` | kept | totalprecipitation |  |
| `sfc/acc93h/ncpcp` | `sfc/acc93h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc96h/acpcp` | `sfc/acc96h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc96h/apcp` | `sfc/acc96h/apcp` | kept | totalprecipitation |  |
| `sfc/acc96h/ncpcp` | `sfc/acc96h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc99h/acpcp` | `sfc/acc99h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc99h/apcp` | `sfc/acc99h/apcp` | kept | totalprecipitation |  |
| `sfc/acc99h/ncpcp` | `sfc/acc99h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc9h/acpcp` | `sfc/acc9h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc9h/apcp` | `sfc/acc9h/apcp` | kept | totalprecipitation |  |
| `sfc/acc9h/ncpcp` | `sfc/acc9h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/avg/cprat` | `sfc/avg/cprat` | kept | convectiveprecipitationwmo |  |
| `sfc/avg/dswrf` | `sfc/avg/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg/prate` | `sfc/avg/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/avg/uswrf` | `sfc/avg/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/avg3h/cprat` | `sfc/avg3h/cprat` | kept | convectiveprecipitationwmo |  |
| `sfc/avg3h/dswrf` | `sfc/avg3h/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg3h/prate` | `sfc/avg3h/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/avg3h/uswrf` | `sfc/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/cape` | `sfc/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `sfc/instant/cin` | `sfc/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `sfc/instant/dswrf` | `sfc/instant/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/instant/elon` | `sfc/instant/edge_midpoint_longitude` | eccodes | eastlongitude |  |
| `sfc/instant/fricv` | `sfc/instant/frictional_velocity` | eccodes | frictionalvelocityncep |  |
| `sfc/instant/gust` | `sfc/instant/wind_speed` | eccodes | windgust |  |
| `sfc/instant/hgt` | `sfc/instant/hgt` | kept | geopotentialheight |  |
| `sfc/instant/hpbl` | `sfc/instant/planetary_boundary_layer_height` | eccodes | planetaryboundarylayerheightncep |  |
| `sfc/instant/land` | `sfc/instant/land` | kept | landcover |  |
| `sfc/instant/lhtfl` | `sfc/instant/lhtfl` | kept | latentheatnetflux |  |
| `sfc/instant/nlat` | `sfc/instant/nlat` | kept | latitude |  |
| `sfc/instant/pres` | `sfc/instant/air_pressure` | abbreviation | pressure |  |
| `sfc/instant/sfcr` | `sfc/instant/sfcr` | kept | surfaceroughness |  |
| `sfc/instant/shtfl` | `sfc/instant/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/instant/tmp` | `sfc/instant/tmp` | kept | temperature |  |
| `sfc/instant/uflx` | `sfc/instant/uflx` | kept | momentumfluxu |  |
| `sfc/instant/uswrf` | `sfc/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/vflx` | `sfc/instant/vflx` | kept | momentumfluxv |  |
| `sfc/instant/wtmp` | `sfc/instant/sea_surface_temperature` | abbreviation | watertemperature |  |
| `tro/instant/hgt` | `tro/instant/hgt` | kept | geopotentialheight |  |
| `tro/instant/pot` | `tro/instant/pot` | kept | potentialtemperature |  |
| `tro/instant/pres` | `tro/instant/air_pressure` | abbreviation | pressure |  |
| `tro/instant/tmp` | `tro/instant/tmp` | kept | temperature |  |
| `tro/instant/ugrd` | `tro/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `tro/instant/vgrd` | `tro/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `tro/instant/vwsh` | `tro/instant/vertical_speed_shear` | eccodes | verticalspeedshearncep |  |

## bkr/dmi/virtualized/hafs_b_storm (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `clt/instant/tmp` | `clt/instant/tmp` | kept | temperature |  |
| `entire_atm/avg/tcdc` | `entire_atm/avg/tcdc` | kept | totalcloudcover |  |
| `entire_atm/avg3h/tcdc` | `entire_atm/avg3h/tcdc` | kept | totalcloudcover |  |
| `entire_atm/instant/pwat` | `entire_atm/instant/precipitable_water` | eccodes | precipitablewater |  |
| `entire_atm/instant/refc` | `entire_atm/instant/composite_reflectivity` | abbreviation | compositereflectivity |  |
| `entire_atm/instant/tcdc` | `entire_atm/instant/tcdc` | kept | totalcloudcover |  |
| `hag/instant/dpt` | `hag/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `hag/instant/hlcy` | `hag/instant/storm_relative_helicity` | eccodes | stormrelativehelicity |  |
| `hag/instant/rh` | `hag/instant/rh` | kept | relativehumidity |  |
| `hag/instant/spfh` | `hag/instant/spfh` | kept | specifichumidity |  |
| `hag/instant/tmp` | `hag/instant/tmp` | kept | temperature |  |
| `hag/instant/ugrd` | `hag/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hag/instant/vgrd` | `hag/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `hag/max/hag` | `hag/max/height_above_ground` | coordinate |  |  |
| `hag/max/mxuphl` | `hag/max/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max/wind` | `hag/max/10m_wind_speed` | eccodes | windspeed |  |
| `hag/max1h/hag` | `hag/max1h/height_above_ground` | coordinate |  |  |
| `hag/max1h/mxuphl` | `hag/max1h/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max1h/wind` | `hag/max1h/10m_wind_speed` | eccodes | windspeed |  |
| `isobar/instant/absv` | `isobar/instant/absolute_vorticity` | eccodes | absolutevorticity |  |
| `isobar/instant/clwmr` | `isobar/instant/cloud_mixing_ratio` | eccodes | cloudmixingratio |  |
| `isobar/instant/dpt` | `isobar/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `isobar/instant/dzdt` | `isobar/instant/dzdt` | kept | verticalvelocitygeometric |  |
| `isobar/instant/grle` | `isobar/instant/graupel` | eccodes | graupel |  |
| `isobar/instant/hgt` | `isobar/instant/hgt` | kept | geopotentialheight |  |
| `isobar/instant/icmr` | `isobar/instant/ice_water_mixing_ratio` | eccodes | icewatermixingratio |  |
| `isobar/instant/isobar` | `isobar/instant/pressure_level` | coordinate |  |  |
| `isobar/instant/rh` | `isobar/instant/rh` | kept | relativehumidity |  |
| `isobar/instant/rwmr` | `isobar/instant/rain_mixing_ratio` | eccodes | rainmixingratio |  |
| `isobar/instant/snmr` | `isobar/instant/snow_mixing_ratio` | eccodes | snowmixingratio |  |
| `isobar/instant/spfh` | `isobar/instant/spfh` | kept | specifichumidity |  |
| `isobar/instant/tmp` | `isobar/instant/tmp` | kept | temperature |  |
| `isobar/instant/ugrd` | `isobar/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `isobar/instant/vgrd` | `isobar/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `isobar/instant/vvel` | `isobar/instant/vvel` | kept | verticalvelocitypressure |  |
| `isobar/max/maxdvv` | `isobar/max/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `isobar/max/maxuvv` | `isobar/max/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `isobar/max1h/maxdvv` | `isobar/max1h/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `isobar/max1h/maxuvv` | `isobar/max1h/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `msl/instant/mslet` | `msl/instant/mslet` | kept | mslp |  |
| `msl/instant/prmsl` | `msl/instant/mean_sea_level_pressure` | eccodes | pressurereducedmsl |  |
| `nominal_top/avg/uswrf` | `nominal_top/avg/uswrf` | kept | upwardshortwaveradiationflux |  |
| `nominal_top/avg3h/uswrf` | `nominal_top/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/acc/acpcp` | `sfc/acc/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc/apcp` | `sfc/acc/apcp` | kept | totalprecipitation |  |
| `sfc/acc/ncpcp` | `sfc/acc/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc102h/acpcp` | `sfc/acc102h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc102h/apcp` | `sfc/acc102h/apcp` | kept | totalprecipitation |  |
| `sfc/acc102h/ncpcp` | `sfc/acc102h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc105h/acpcp` | `sfc/acc105h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc105h/apcp` | `sfc/acc105h/apcp` | kept | totalprecipitation |  |
| `sfc/acc105h/ncpcp` | `sfc/acc105h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc108h/acpcp` | `sfc/acc108h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc108h/apcp` | `sfc/acc108h/apcp` | kept | totalprecipitation |  |
| `sfc/acc108h/ncpcp` | `sfc/acc108h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc111h/acpcp` | `sfc/acc111h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc111h/apcp` | `sfc/acc111h/apcp` | kept | totalprecipitation |  |
| `sfc/acc111h/ncpcp` | `sfc/acc111h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc114h/acpcp` | `sfc/acc114h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc114h/apcp` | `sfc/acc114h/apcp` | kept | totalprecipitation |  |
| `sfc/acc114h/ncpcp` | `sfc/acc114h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc117h/acpcp` | `sfc/acc117h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc117h/apcp` | `sfc/acc117h/apcp` | kept | totalprecipitation |  |
| `sfc/acc117h/ncpcp` | `sfc/acc117h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc120h/acpcp` | `sfc/acc120h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc120h/apcp` | `sfc/acc120h/apcp` | kept | totalprecipitation |  |
| `sfc/acc120h/ncpcp` | `sfc/acc120h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc123h/acpcp` | `sfc/acc123h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc123h/apcp` | `sfc/acc123h/apcp` | kept | totalprecipitation |  |
| `sfc/acc123h/ncpcp` | `sfc/acc123h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc126h/acpcp` | `sfc/acc126h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc126h/apcp` | `sfc/acc126h/apcp` | kept | totalprecipitation |  |
| `sfc/acc126h/ncpcp` | `sfc/acc126h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc12h/acpcp` | `sfc/acc12h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc12h/apcp` | `sfc/acc12h/apcp` | kept | totalprecipitation |  |
| `sfc/acc12h/ncpcp` | `sfc/acc12h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc15h/acpcp` | `sfc/acc15h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc15h/apcp` | `sfc/acc15h/apcp` | kept | totalprecipitation |  |
| `sfc/acc15h/ncpcp` | `sfc/acc15h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc18h/acpcp` | `sfc/acc18h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc18h/apcp` | `sfc/acc18h/apcp` | kept | totalprecipitation |  |
| `sfc/acc18h/ncpcp` | `sfc/acc18h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc21h/acpcp` | `sfc/acc21h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc21h/apcp` | `sfc/acc21h/apcp` | kept | totalprecipitation |  |
| `sfc/acc21h/ncpcp` | `sfc/acc21h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc24h/acpcp` | `sfc/acc24h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc24h/apcp` | `sfc/acc24h/apcp` | kept | totalprecipitation |  |
| `sfc/acc24h/ncpcp` | `sfc/acc24h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc27h/acpcp` | `sfc/acc27h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc27h/apcp` | `sfc/acc27h/apcp` | kept | totalprecipitation |  |
| `sfc/acc27h/ncpcp` | `sfc/acc27h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc30h/acpcp` | `sfc/acc30h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc30h/apcp` | `sfc/acc30h/apcp` | kept | totalprecipitation |  |
| `sfc/acc30h/ncpcp` | `sfc/acc30h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc33h/acpcp` | `sfc/acc33h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc33h/apcp` | `sfc/acc33h/apcp` | kept | totalprecipitation |  |
| `sfc/acc33h/ncpcp` | `sfc/acc33h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc36h/acpcp` | `sfc/acc36h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc36h/apcp` | `sfc/acc36h/apcp` | kept | totalprecipitation |  |
| `sfc/acc36h/ncpcp` | `sfc/acc36h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc39h/acpcp` | `sfc/acc39h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc39h/apcp` | `sfc/acc39h/apcp` | kept | totalprecipitation |  |
| `sfc/acc39h/ncpcp` | `sfc/acc39h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc3h/acpcp` | `sfc/acc3h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc3h/apcp` | `sfc/acc3h/apcp` | kept | totalprecipitation |  |
| `sfc/acc3h/ncpcp` | `sfc/acc3h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc42h/acpcp` | `sfc/acc42h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc42h/apcp` | `sfc/acc42h/apcp` | kept | totalprecipitation |  |
| `sfc/acc42h/ncpcp` | `sfc/acc42h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc45h/acpcp` | `sfc/acc45h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc45h/apcp` | `sfc/acc45h/apcp` | kept | totalprecipitation |  |
| `sfc/acc45h/ncpcp` | `sfc/acc45h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc48h/acpcp` | `sfc/acc48h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc48h/apcp` | `sfc/acc48h/apcp` | kept | totalprecipitation |  |
| `sfc/acc48h/ncpcp` | `sfc/acc48h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc51h/acpcp` | `sfc/acc51h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc51h/apcp` | `sfc/acc51h/apcp` | kept | totalprecipitation |  |
| `sfc/acc51h/ncpcp` | `sfc/acc51h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc54h/acpcp` | `sfc/acc54h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc54h/apcp` | `sfc/acc54h/apcp` | kept | totalprecipitation |  |
| `sfc/acc54h/ncpcp` | `sfc/acc54h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc57h/acpcp` | `sfc/acc57h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc57h/apcp` | `sfc/acc57h/apcp` | kept | totalprecipitation |  |
| `sfc/acc57h/ncpcp` | `sfc/acc57h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc60h/acpcp` | `sfc/acc60h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc60h/apcp` | `sfc/acc60h/apcp` | kept | totalprecipitation |  |
| `sfc/acc60h/ncpcp` | `sfc/acc60h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc63h/acpcp` | `sfc/acc63h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc63h/apcp` | `sfc/acc63h/apcp` | kept | totalprecipitation |  |
| `sfc/acc63h/ncpcp` | `sfc/acc63h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc66h/acpcp` | `sfc/acc66h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc66h/apcp` | `sfc/acc66h/apcp` | kept | totalprecipitation |  |
| `sfc/acc66h/ncpcp` | `sfc/acc66h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc69h/acpcp` | `sfc/acc69h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc69h/apcp` | `sfc/acc69h/apcp` | kept | totalprecipitation |  |
| `sfc/acc69h/ncpcp` | `sfc/acc69h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc6h/acpcp` | `sfc/acc6h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc6h/apcp` | `sfc/acc6h/apcp` | kept | totalprecipitation |  |
| `sfc/acc6h/ncpcp` | `sfc/acc6h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc72h/acpcp` | `sfc/acc72h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc72h/apcp` | `sfc/acc72h/apcp` | kept | totalprecipitation |  |
| `sfc/acc72h/ncpcp` | `sfc/acc72h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc75h/acpcp` | `sfc/acc75h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc75h/apcp` | `sfc/acc75h/apcp` | kept | totalprecipitation |  |
| `sfc/acc75h/ncpcp` | `sfc/acc75h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc78h/acpcp` | `sfc/acc78h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc78h/apcp` | `sfc/acc78h/apcp` | kept | totalprecipitation |  |
| `sfc/acc78h/ncpcp` | `sfc/acc78h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc81h/acpcp` | `sfc/acc81h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc81h/apcp` | `sfc/acc81h/apcp` | kept | totalprecipitation |  |
| `sfc/acc81h/ncpcp` | `sfc/acc81h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc84h/acpcp` | `sfc/acc84h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc84h/apcp` | `sfc/acc84h/apcp` | kept | totalprecipitation |  |
| `sfc/acc84h/ncpcp` | `sfc/acc84h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc87h/acpcp` | `sfc/acc87h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc87h/apcp` | `sfc/acc87h/apcp` | kept | totalprecipitation |  |
| `sfc/acc87h/ncpcp` | `sfc/acc87h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc90h/acpcp` | `sfc/acc90h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc90h/apcp` | `sfc/acc90h/apcp` | kept | totalprecipitation |  |
| `sfc/acc90h/ncpcp` | `sfc/acc90h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc93h/acpcp` | `sfc/acc93h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc93h/apcp` | `sfc/acc93h/apcp` | kept | totalprecipitation |  |
| `sfc/acc93h/ncpcp` | `sfc/acc93h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc96h/acpcp` | `sfc/acc96h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc96h/apcp` | `sfc/acc96h/apcp` | kept | totalprecipitation |  |
| `sfc/acc96h/ncpcp` | `sfc/acc96h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc99h/acpcp` | `sfc/acc99h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc99h/apcp` | `sfc/acc99h/apcp` | kept | totalprecipitation |  |
| `sfc/acc99h/ncpcp` | `sfc/acc99h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc9h/acpcp` | `sfc/acc9h/convective_precipitation` | eccodes | convectiveprecipitation |  |
| `sfc/acc9h/apcp` | `sfc/acc9h/apcp` | kept | totalprecipitation |  |
| `sfc/acc9h/ncpcp` | `sfc/acc9h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/avg/cprat` | `sfc/avg/cprat` | kept | convectiveprecipitationwmo |  |
| `sfc/avg/dswrf` | `sfc/avg/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg/prate` | `sfc/avg/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/avg/uswrf` | `sfc/avg/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/avg3h/cprat` | `sfc/avg3h/cprat` | kept | convectiveprecipitationwmo |  |
| `sfc/avg3h/dswrf` | `sfc/avg3h/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg3h/prate` | `sfc/avg3h/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/avg3h/uswrf` | `sfc/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/cape` | `sfc/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `sfc/instant/cin` | `sfc/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `sfc/instant/dswrf` | `sfc/instant/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/instant/elon` | `sfc/instant/edge_midpoint_longitude` | eccodes | eastlongitude |  |
| `sfc/instant/fricv` | `sfc/instant/frictional_velocity` | eccodes | frictionalvelocityncep |  |
| `sfc/instant/gust` | `sfc/instant/wind_speed` | eccodes | windgust |  |
| `sfc/instant/hgt` | `sfc/instant/hgt` | kept | geopotentialheight |  |
| `sfc/instant/hpbl` | `sfc/instant/planetary_boundary_layer_height` | eccodes | planetaryboundarylayerheightncep |  |
| `sfc/instant/land` | `sfc/instant/land` | kept | landcover |  |
| `sfc/instant/lhtfl` | `sfc/instant/lhtfl` | kept | latentheatnetflux |  |
| `sfc/instant/nlat` | `sfc/instant/nlat` | kept | latitude |  |
| `sfc/instant/pres` | `sfc/instant/air_pressure` | abbreviation | pressure |  |
| `sfc/instant/sfcr` | `sfc/instant/sfcr` | kept | surfaceroughness |  |
| `sfc/instant/shtfl` | `sfc/instant/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/instant/tmp` | `sfc/instant/tmp` | kept | temperature |  |
| `sfc/instant/uflx` | `sfc/instant/uflx` | kept | momentumfluxu |  |
| `sfc/instant/uswrf` | `sfc/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/vflx` | `sfc/instant/vflx` | kept | momentumfluxv |  |
| `sfc/instant/wtmp` | `sfc/instant/sea_surface_temperature` | abbreviation | watertemperature |  |
| `tro/instant/hgt` | `tro/instant/hgt` | kept | geopotentialheight |  |
| `tro/instant/pot` | `tro/instant/pot` | kept | potentialtemperature |  |
| `tro/instant/pres` | `tro/instant/air_pressure` | abbreviation | pressure |  |
| `tro/instant/tmp` | `tro/instant/tmp` | kept | temperature |  |
| `tro/instant/ugrd` | `tro/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `tro/instant/vgrd` | `tro/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `tro/instant/vwsh` | `tro/instant/vertical_speed_shear` | eccodes | verticalspeedshearncep |  |

## bkr/dmi/virtualized/hrrr_alaska_nat (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `adiabatic_condensation_lifted/instant/hgt` | `adiabatic_condensation_lifted/instant/hgt` | kept | geopotentialheight |  |
| `atm/instant/ltng` | `atm/instant/ltng` | kept | lightning |  |
| `atm/instant/refc` | `atm/instant/composite_reflectivity` | abbreviation | compositereflectivity |  |
| `atm/instant/rhpw` | `atm/instant/rhpw` | kept | relativehumidityprecipitablewater |  |
| `atm/instant/tcdc` | `atm/instant/tcdc` | kept | totalcloudcover |  |
| `atm/instant/veril` | `atm/instant/vertically_integrated_liquid` | eccodes | verticallyintegratedliquid |  |
| `clb/instant/hgt` | `clb/instant/hgt` | kept | geopotentialheight |  |
| `clb/instant/pres` | `clb/instant/air_pressure` | abbreviation | pressure |  |
| `cld_ceiling/instant/hgt` | `cld_ceiling/instant/hgt` | kept | geopotentialheight |  |
| `clt/instant/hgt` | `clt/instant/hgt` | kept | geopotentialheight |  |
| `clt/instant/pres` | `clt/instant/air_pressure` | abbreviation | pressure |  |
| `clt/instant/retop` | `clt/instant/retop` | kept | echotop |  |
| `depth_bls/instant/mstav` | `depth_bls/instant/moisture_availability` | eccodes | moistureavailabilityncep |  |
| `depth_bls/instant/soilw` | `depth_bls/instant/volumetric_soil_moisture_content` | eccodes | volumetricsoilmoisturecontentncep |  |
| `entire_atm/instant/apts` | `entire_atm/instant/apts` | kept | atmosphereopticalthickness |  |
| `entire_atm/instant/colmd` | `entire_atm/instant/colmd` | kept | columnintegratedmassdensity |  |
| `entire_atm/instant/pwat` | `entire_atm/instant/precipitable_water` | eccodes | precipitablewater |  |
| `entire_atm/max/tcolg` | `entire_atm/max/total_column_graupel` | override | totalcolumnintegratedgraupel |  |
| `entire_atm/max1h/tcolg` | `entire_atm/max1h/total_column_graupel` | override | totalcolumnintegratedgraupel |  |
| `eqm/instant/hgt` | `eqm/instant/hgt` | kept | geopotentialheight |  |
| `hag/instant/dpt` | `hag/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `hag/instant/hag` | `hag/instant/height_above_ground` | coordinate |  |  |
| `hag/instant/hlcy` | `hag/instant/storm_relative_helicity` | eccodes | stormrelativehelicity |  |
| `hag/instant/massden` | `hag/instant/massden` | kept | massdensity |  |
| `hag/instant/pot` | `hag/instant/pot` | kept | potentialtemperature |  |
| `hag/instant/refd` | `hag/instant/refd` | kept | reflectivity |  |
| `hag/instant/rh` | `hag/instant/rh` | kept | relativehumidity |  |
| `hag/instant/spfh` | `hag/instant/spfh` | kept | specifichumidity |  |
| `hag/instant/tmp` | `hag/instant/tmp` | kept | temperature |  |
| `hag/instant/ugrd` | `hag/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hag/instant/utsm` | `hag/instant/utsm` | kept | ucomponentstormmotion |  |
| `hag/instant/vgrd` | `hag/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `hag/instant/vstm` | `hag/instant/v_component_storm_motion` | eccodes | vcomponentstormmotion |  |
| `hag/instant/vucsh` | `hag/instant/vertical_u_component_shear` | eccodes | verticalucomponentshear |  |
| `hag/instant/vvcsh` | `hag/instant/vertical_v_component_shear` | eccodes | verticalvcomponentshear |  |
| `hag/max/hag` | `hag/max/height_above_ground` | coordinate |  |  |
| `hag/max/maxref` | `hag/max/maxref` | kept | hourlymaxsimulatedreflectivity |  |
| `hag/max/maxuw` | `hag/max/maxuw` | kept | ucomponenthourlymaximumwindspeed |  |
| `hag/max/maxvw` | `hag/max/maxvw` | kept | vcomponenthourlymaximumwindspeed |  |
| `hag/max/mxuphl` | `hag/max/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max/relv` | `hag/max/relative_vorticity` | eccodes | relativevorticity |  |
| `hag/max/wind` | `hag/max/10m_wind_speed` | eccodes | windspeed |  |
| `hag/max1h/hag` | `hag/max1h/height_above_ground` | coordinate |  |  |
| `hag/max1h/maxref` | `hag/max1h/maxref` | kept | hourlymaxsimulatedreflectivity |  |
| `hag/max1h/maxuw` | `hag/max1h/maxuw` | kept | ucomponenthourlymaximumwindspeed |  |
| `hag/max1h/maxvw` | `hag/max1h/maxvw` | kept | vcomponenthourlymaximumwindspeed |  |
| `hag/max1h/mxuphl` | `hag/max1h/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max1h/relv` | `hag/max1h/relative_vorticity` | eccodes | relativevorticity |  |
| `hag/max1h/wind` | `hag/max1h/10m_wind_speed` | eccodes | windspeed |  |
| `hag/min/hag` | `hag/min/height_above_ground` | coordinate |  |  |
| `hag/min/mnuphl` | `hag/min/mnuphl` | kept | hourlyminupdrafthelicity |  |
| `hag/min1h/hag` | `hag/min1h/height_above_ground` | coordinate |  |  |
| `hag/min1h/mnuphl` | `hag/min1h/mnuphl` | kept | hourlyminupdrafthelicity |  |
| `hcl/instant/hcdc` | `hcl/instant/hcdc` | kept | highcloudcover |  |
| `htfl/instant/hgt` | `htfl/instant/hgt` | kept | geopotentialheight |  |
| `htfl/instant/pres` | `htfl/instant/air_pressure` | abbreviation | pressure |  |
| `htfl/instant/rh` | `htfl/instant/rh` | kept | relativehumidity |  |
| `hybid/instant/cimixr` | `hybid/instant/cimixr` | kept | cloudicemixingratio |  |
| `hybid/instant/clwmr` | `hybid/instant/cloud_mixing_ratio` | eccodes | cloudmixingratio |  |
| `hybid/instant/grle` | `hybid/instant/graupel` | eccodes | graupel |  |
| `hybid/instant/hgt` | `hybid/instant/hgt` | kept | geopotentialheight |  |
| `hybid/instant/hybid` | `hybid/instant/model_level` | coordinate |  |  |
| `hybid/instant/massden` | `hybid/instant/massden` | kept | massdensity |  |
| `hybid/instant/pmtc` | `hybid/instant/pmtc` | kept | particulatemattercoarse |  |
| `hybid/instant/pmtf` | `hybid/instant/pmtf` | kept | particulatematterfine |  |
| `hybid/instant/pres` | `hybid/instant/air_pressure` | abbreviation | pressure |  |
| `hybid/instant/rwmr` | `hybid/instant/rain_mixing_ratio` | eccodes | rainmixingratio |  |
| `hybid/instant/snmr` | `hybid/instant/snow_mixing_ratio` | eccodes | snowmixingratio |  |
| `hybid/instant/spfh` | `hybid/instant/spfh` | kept | specifichumidity |  |
| `hybid/instant/tcdc` | `hybid/instant/tcdc` | kept | totalcloudcover |  |
| `hybid/instant/tmp` | `hybid/instant/tmp` | kept | temperature |  |
| `hybid/instant/ugrd` | `hybid/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hybid/instant/vgrd` | `hybid/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `hybid/instant/vvel` | `hybid/instant/vvel` | kept | verticalvelocitypressure |  |
| `isobar/instant/hgt` | `isobar/instant/hgt` | kept | geopotentialheight |  |
| `isobar/instant/lftx` | `isobar/instant/surface_lifted_index` | eccodes | surfaceliftedindex |  |
| `isotherm/instant/hgt` | `isotherm/instant/hgt` | kept | geopotentialheight |  |
| `isotherm/instant/refd` | `isotherm/instant/refd` | kept | reflectivity |  |
| `isotherm/max/refd` | `isotherm/max/refd` | kept | reflectivity |  |
| `isotherm/max1h/refd` | `isotherm/max1h/refd` | kept | reflectivity |  |
| `lcl/instant/lcdc` | `lcl/instant/lcdc` | kept | lowcloudcover |  |
| `mcl/instant/mcdc` | `mcl/instant/mcdc` | kept | mediumcloudcover |  |
| `msl/instant/mslma` | `msl/instant/mslma` | kept | mslpmaps |  |
| `nominal_top/instant/sbt113` | `nominal_top/instant/sbt113` | kept | simulatedbrightnesstempgoes11ch3 |  |
| `nominal_top/instant/sbt114` | `nominal_top/instant/sbt114` | kept | simulatedbrightnesstempgoes11ch4 |  |
| `nominal_top/instant/sbt123` | `nominal_top/instant/sbt123` | kept | simulatedbrightnesstempgoes12ch3 |  |
| `nominal_top/instant/sbt124` | `nominal_top/instant/sbt124` | kept | simulatedbrightnesstempgoes12ch4 |  |
| `nominal_top/instant/ulwrf` | `nominal_top/instant/upward_long_wave_radiation_flux` | eccodes | upwardlongwaveradiationflux |  |
| `nominal_top/instant/uswrf` | `nominal_top/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `pres_diff/instant/4lftx` | `pres_diff/instant/best_lifted_index` | eccodes | best4layerliftedindexncep |  |
| `pres_diff/instant/cape` | `pres_diff/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `pres_diff/instant/cin` | `pres_diff/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `pres_diff/instant/plpl` | `pres_diff/instant/plpl` | kept | pressureofliftedparcel |  |
| `pres_diff/max/maxdvv` | `pres_diff/max/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `pres_diff/max/maxuvv` | `pres_diff/max/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `pres_diff/max1h/maxdvv` | `pres_diff/max1h/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `pres_diff/max1h/maxuvv` | `pres_diff/max1h/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `sfc/acc/apcp` | `sfc/acc/apcp` | kept | totalprecipitation |  |
| `sfc/acc/asnow` | `sfc/acc/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc/bgrun` | `sfc/acc/baseflow_groundwater_runoff` | eccodes | baseflowgroundwaterrunoff |  |
| `sfc/acc/frozr` | `sfc/acc/frozr` | kept | frozenrain |  |
| `sfc/acc/frzr` | `sfc/acc/frzr` | kept | freezingrain |  |
| `sfc/acc/ssrun` | `sfc/acc/storm_surface_runoff` | eccodes | stormsurfacerunoff |  |
| `sfc/acc/weasd` | `sfc/acc/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc10h/apcp` | `sfc/acc10h/apcp` | kept | totalprecipitation |  |
| `sfc/acc10h/asnow` | `sfc/acc10h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc10h/frozr` | `sfc/acc10h/frozr` | kept | frozenrain |  |
| `sfc/acc10h/frzr` | `sfc/acc10h/frzr` | kept | freezingrain |  |
| `sfc/acc10h/weasd` | `sfc/acc10h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc11h/apcp` | `sfc/acc11h/apcp` | kept | totalprecipitation |  |
| `sfc/acc11h/asnow` | `sfc/acc11h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc11h/frozr` | `sfc/acc11h/frozr` | kept | frozenrain |  |
| `sfc/acc11h/frzr` | `sfc/acc11h/frzr` | kept | freezingrain |  |
| `sfc/acc11h/weasd` | `sfc/acc11h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc12h/apcp` | `sfc/acc12h/apcp` | kept | totalprecipitation |  |
| `sfc/acc12h/asnow` | `sfc/acc12h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc12h/frozr` | `sfc/acc12h/frozr` | kept | frozenrain |  |
| `sfc/acc12h/frzr` | `sfc/acc12h/frzr` | kept | freezingrain |  |
| `sfc/acc12h/weasd` | `sfc/acc12h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc13h/apcp` | `sfc/acc13h/apcp` | kept | totalprecipitation |  |
| `sfc/acc13h/asnow` | `sfc/acc13h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc13h/frozr` | `sfc/acc13h/frozr` | kept | frozenrain |  |
| `sfc/acc13h/frzr` | `sfc/acc13h/frzr` | kept | freezingrain |  |
| `sfc/acc13h/weasd` | `sfc/acc13h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc14h/apcp` | `sfc/acc14h/apcp` | kept | totalprecipitation |  |
| `sfc/acc14h/asnow` | `sfc/acc14h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc14h/frozr` | `sfc/acc14h/frozr` | kept | frozenrain |  |
| `sfc/acc14h/frzr` | `sfc/acc14h/frzr` | kept | freezingrain |  |
| `sfc/acc14h/weasd` | `sfc/acc14h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc15h/apcp` | `sfc/acc15h/apcp` | kept | totalprecipitation |  |
| `sfc/acc15h/asnow` | `sfc/acc15h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc15h/frozr` | `sfc/acc15h/frozr` | kept | frozenrain |  |
| `sfc/acc15h/frzr` | `sfc/acc15h/frzr` | kept | freezingrain |  |
| `sfc/acc15h/weasd` | `sfc/acc15h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc16h/apcp` | `sfc/acc16h/apcp` | kept | totalprecipitation |  |
| `sfc/acc16h/asnow` | `sfc/acc16h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc16h/frozr` | `sfc/acc16h/frozr` | kept | frozenrain |  |
| `sfc/acc16h/frzr` | `sfc/acc16h/frzr` | kept | freezingrain |  |
| `sfc/acc16h/weasd` | `sfc/acc16h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc17h/apcp` | `sfc/acc17h/apcp` | kept | totalprecipitation |  |
| `sfc/acc17h/asnow` | `sfc/acc17h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc17h/frozr` | `sfc/acc17h/frozr` | kept | frozenrain |  |
| `sfc/acc17h/frzr` | `sfc/acc17h/frzr` | kept | freezingrain |  |
| `sfc/acc17h/weasd` | `sfc/acc17h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc18h/apcp` | `sfc/acc18h/apcp` | kept | totalprecipitation |  |
| `sfc/acc18h/asnow` | `sfc/acc18h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc18h/frozr` | `sfc/acc18h/frozr` | kept | frozenrain |  |
| `sfc/acc18h/frzr` | `sfc/acc18h/frzr` | kept | freezingrain |  |
| `sfc/acc18h/weasd` | `sfc/acc18h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc19h/apcp` | `sfc/acc19h/apcp` | kept | totalprecipitation |  |
| `sfc/acc19h/asnow` | `sfc/acc19h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc19h/frozr` | `sfc/acc19h/frozr` | kept | frozenrain |  |
| `sfc/acc19h/frzr` | `sfc/acc19h/frzr` | kept | freezingrain |  |
| `sfc/acc19h/weasd` | `sfc/acc19h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc1h/apcp` | `sfc/acc1h/apcp` | kept | totalprecipitation |  |
| `sfc/acc1h/asnow` | `sfc/acc1h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc1h/bgrun` | `sfc/acc1h/baseflow_groundwater_runoff` | eccodes | baseflowgroundwaterrunoff |  |
| `sfc/acc1h/frozr` | `sfc/acc1h/frozr` | kept | frozenrain |  |
| `sfc/acc1h/frzr` | `sfc/acc1h/frzr` | kept | freezingrain |  |
| `sfc/acc1h/ssrun` | `sfc/acc1h/storm_surface_runoff` | eccodes | stormsurfacerunoff |  |
| `sfc/acc1h/weasd` | `sfc/acc1h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc20h/apcp` | `sfc/acc20h/apcp` | kept | totalprecipitation |  |
| `sfc/acc20h/asnow` | `sfc/acc20h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc20h/frozr` | `sfc/acc20h/frozr` | kept | frozenrain |  |
| `sfc/acc20h/frzr` | `sfc/acc20h/frzr` | kept | freezingrain |  |
| `sfc/acc20h/weasd` | `sfc/acc20h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc21h/apcp` | `sfc/acc21h/apcp` | kept | totalprecipitation |  |
| `sfc/acc21h/asnow` | `sfc/acc21h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc21h/frozr` | `sfc/acc21h/frozr` | kept | frozenrain |  |
| `sfc/acc21h/frzr` | `sfc/acc21h/frzr` | kept | freezingrain |  |
| `sfc/acc21h/weasd` | `sfc/acc21h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc22h/apcp` | `sfc/acc22h/apcp` | kept | totalprecipitation |  |
| `sfc/acc22h/asnow` | `sfc/acc22h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc22h/frozr` | `sfc/acc22h/frozr` | kept | frozenrain |  |
| `sfc/acc22h/frzr` | `sfc/acc22h/frzr` | kept | freezingrain |  |
| `sfc/acc22h/weasd` | `sfc/acc22h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc23h/apcp` | `sfc/acc23h/apcp` | kept | totalprecipitation |  |
| `sfc/acc23h/asnow` | `sfc/acc23h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc23h/frozr` | `sfc/acc23h/frozr` | kept | frozenrain |  |
| `sfc/acc23h/frzr` | `sfc/acc23h/frzr` | kept | freezingrain |  |
| `sfc/acc23h/weasd` | `sfc/acc23h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc24h/apcp` | `sfc/acc24h/apcp` | kept | totalprecipitation |  |
| `sfc/acc24h/asnow` | `sfc/acc24h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc24h/frozr` | `sfc/acc24h/frozr` | kept | frozenrain |  |
| `sfc/acc24h/frzr` | `sfc/acc24h/frzr` | kept | freezingrain |  |
| `sfc/acc24h/weasd` | `sfc/acc24h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc25h/apcp` | `sfc/acc25h/apcp` | kept | totalprecipitation |  |
| `sfc/acc25h/asnow` | `sfc/acc25h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc25h/frozr` | `sfc/acc25h/frozr` | kept | frozenrain |  |
| `sfc/acc25h/frzr` | `sfc/acc25h/frzr` | kept | freezingrain |  |
| `sfc/acc25h/weasd` | `sfc/acc25h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc26h/apcp` | `sfc/acc26h/apcp` | kept | totalprecipitation |  |
| `sfc/acc26h/asnow` | `sfc/acc26h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc26h/frozr` | `sfc/acc26h/frozr` | kept | frozenrain |  |
| `sfc/acc26h/frzr` | `sfc/acc26h/frzr` | kept | freezingrain |  |
| `sfc/acc26h/weasd` | `sfc/acc26h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc27h/apcp` | `sfc/acc27h/apcp` | kept | totalprecipitation |  |
| `sfc/acc27h/asnow` | `sfc/acc27h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc27h/frozr` | `sfc/acc27h/frozr` | kept | frozenrain |  |
| `sfc/acc27h/frzr` | `sfc/acc27h/frzr` | kept | freezingrain |  |
| `sfc/acc27h/weasd` | `sfc/acc27h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc28h/apcp` | `sfc/acc28h/apcp` | kept | totalprecipitation |  |
| `sfc/acc28h/asnow` | `sfc/acc28h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc28h/frozr` | `sfc/acc28h/frozr` | kept | frozenrain |  |
| `sfc/acc28h/frzr` | `sfc/acc28h/frzr` | kept | freezingrain |  |
| `sfc/acc28h/weasd` | `sfc/acc28h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc29h/apcp` | `sfc/acc29h/apcp` | kept | totalprecipitation |  |
| `sfc/acc29h/asnow` | `sfc/acc29h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc29h/frozr` | `sfc/acc29h/frozr` | kept | frozenrain |  |
| `sfc/acc29h/frzr` | `sfc/acc29h/frzr` | kept | freezingrain |  |
| `sfc/acc29h/weasd` | `sfc/acc29h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc2h/apcp` | `sfc/acc2h/apcp` | kept | totalprecipitation |  |
| `sfc/acc2h/asnow` | `sfc/acc2h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc2h/frozr` | `sfc/acc2h/frozr` | kept | frozenrain |  |
| `sfc/acc2h/frzr` | `sfc/acc2h/frzr` | kept | freezingrain |  |
| `sfc/acc2h/weasd` | `sfc/acc2h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc30h/apcp` | `sfc/acc30h/apcp` | kept | totalprecipitation |  |
| `sfc/acc30h/asnow` | `sfc/acc30h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc30h/frozr` | `sfc/acc30h/frozr` | kept | frozenrain |  |
| `sfc/acc30h/frzr` | `sfc/acc30h/frzr` | kept | freezingrain |  |
| `sfc/acc30h/weasd` | `sfc/acc30h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc31h/apcp` | `sfc/acc31h/apcp` | kept | totalprecipitation |  |
| `sfc/acc31h/asnow` | `sfc/acc31h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc31h/frozr` | `sfc/acc31h/frozr` | kept | frozenrain |  |
| `sfc/acc31h/frzr` | `sfc/acc31h/frzr` | kept | freezingrain |  |
| `sfc/acc31h/weasd` | `sfc/acc31h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc32h/apcp` | `sfc/acc32h/apcp` | kept | totalprecipitation |  |
| `sfc/acc32h/asnow` | `sfc/acc32h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc32h/frozr` | `sfc/acc32h/frozr` | kept | frozenrain |  |
| `sfc/acc32h/frzr` | `sfc/acc32h/frzr` | kept | freezingrain |  |
| `sfc/acc32h/weasd` | `sfc/acc32h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc33h/apcp` | `sfc/acc33h/apcp` | kept | totalprecipitation |  |
| `sfc/acc33h/asnow` | `sfc/acc33h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc33h/frozr` | `sfc/acc33h/frozr` | kept | frozenrain |  |
| `sfc/acc33h/frzr` | `sfc/acc33h/frzr` | kept | freezingrain |  |
| `sfc/acc33h/weasd` | `sfc/acc33h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc34h/apcp` | `sfc/acc34h/apcp` | kept | totalprecipitation |  |
| `sfc/acc34h/asnow` | `sfc/acc34h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc34h/frozr` | `sfc/acc34h/frozr` | kept | frozenrain |  |
| `sfc/acc34h/frzr` | `sfc/acc34h/frzr` | kept | freezingrain |  |
| `sfc/acc34h/weasd` | `sfc/acc34h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc35h/apcp` | `sfc/acc35h/apcp` | kept | totalprecipitation |  |
| `sfc/acc35h/asnow` | `sfc/acc35h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc35h/frozr` | `sfc/acc35h/frozr` | kept | frozenrain |  |
| `sfc/acc35h/frzr` | `sfc/acc35h/frzr` | kept | freezingrain |  |
| `sfc/acc35h/weasd` | `sfc/acc35h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc36h/apcp` | `sfc/acc36h/apcp` | kept | totalprecipitation |  |
| `sfc/acc36h/asnow` | `sfc/acc36h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc36h/frozr` | `sfc/acc36h/frozr` | kept | frozenrain |  |
| `sfc/acc36h/frzr` | `sfc/acc36h/frzr` | kept | freezingrain |  |
| `sfc/acc36h/weasd` | `sfc/acc36h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc37h/apcp` | `sfc/acc37h/apcp` | kept | totalprecipitation |  |
| `sfc/acc37h/asnow` | `sfc/acc37h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc37h/frozr` | `sfc/acc37h/frozr` | kept | frozenrain |  |
| `sfc/acc37h/frzr` | `sfc/acc37h/frzr` | kept | freezingrain |  |
| `sfc/acc37h/weasd` | `sfc/acc37h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc38h/apcp` | `sfc/acc38h/apcp` | kept | totalprecipitation |  |
| `sfc/acc38h/asnow` | `sfc/acc38h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc38h/frozr` | `sfc/acc38h/frozr` | kept | frozenrain |  |
| `sfc/acc38h/frzr` | `sfc/acc38h/frzr` | kept | freezingrain |  |
| `sfc/acc38h/weasd` | `sfc/acc38h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc39h/apcp` | `sfc/acc39h/apcp` | kept | totalprecipitation |  |
| `sfc/acc39h/asnow` | `sfc/acc39h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc39h/frozr` | `sfc/acc39h/frozr` | kept | frozenrain |  |
| `sfc/acc39h/frzr` | `sfc/acc39h/frzr` | kept | freezingrain |  |
| `sfc/acc39h/weasd` | `sfc/acc39h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc3h/apcp` | `sfc/acc3h/apcp` | kept | totalprecipitation |  |
| `sfc/acc3h/asnow` | `sfc/acc3h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc3h/frozr` | `sfc/acc3h/frozr` | kept | frozenrain |  |
| `sfc/acc3h/frzr` | `sfc/acc3h/frzr` | kept | freezingrain |  |
| `sfc/acc3h/weasd` | `sfc/acc3h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc40h/apcp` | `sfc/acc40h/apcp` | kept | totalprecipitation |  |
| `sfc/acc40h/asnow` | `sfc/acc40h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc40h/frozr` | `sfc/acc40h/frozr` | kept | frozenrain |  |
| `sfc/acc40h/frzr` | `sfc/acc40h/frzr` | kept | freezingrain |  |
| `sfc/acc40h/weasd` | `sfc/acc40h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc41h/apcp` | `sfc/acc41h/apcp` | kept | totalprecipitation |  |
| `sfc/acc41h/asnow` | `sfc/acc41h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc41h/frozr` | `sfc/acc41h/frozr` | kept | frozenrain |  |
| `sfc/acc41h/frzr` | `sfc/acc41h/frzr` | kept | freezingrain |  |
| `sfc/acc41h/weasd` | `sfc/acc41h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc42h/apcp` | `sfc/acc42h/apcp` | kept | totalprecipitation |  |
| `sfc/acc42h/asnow` | `sfc/acc42h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc42h/frozr` | `sfc/acc42h/frozr` | kept | frozenrain |  |
| `sfc/acc42h/frzr` | `sfc/acc42h/frzr` | kept | freezingrain |  |
| `sfc/acc42h/weasd` | `sfc/acc42h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc43h/apcp` | `sfc/acc43h/apcp` | kept | totalprecipitation |  |
| `sfc/acc43h/asnow` | `sfc/acc43h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc43h/frozr` | `sfc/acc43h/frozr` | kept | frozenrain |  |
| `sfc/acc43h/frzr` | `sfc/acc43h/frzr` | kept | freezingrain |  |
| `sfc/acc43h/weasd` | `sfc/acc43h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc44h/apcp` | `sfc/acc44h/apcp` | kept | totalprecipitation |  |
| `sfc/acc44h/asnow` | `sfc/acc44h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc44h/frozr` | `sfc/acc44h/frozr` | kept | frozenrain |  |
| `sfc/acc44h/frzr` | `sfc/acc44h/frzr` | kept | freezingrain |  |
| `sfc/acc44h/weasd` | `sfc/acc44h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc45h/apcp` | `sfc/acc45h/apcp` | kept | totalprecipitation |  |
| `sfc/acc45h/asnow` | `sfc/acc45h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc45h/frozr` | `sfc/acc45h/frozr` | kept | frozenrain |  |
| `sfc/acc45h/frzr` | `sfc/acc45h/frzr` | kept | freezingrain |  |
| `sfc/acc45h/weasd` | `sfc/acc45h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc46h/apcp` | `sfc/acc46h/apcp` | kept | totalprecipitation |  |
| `sfc/acc46h/asnow` | `sfc/acc46h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc46h/frozr` | `sfc/acc46h/frozr` | kept | frozenrain |  |
| `sfc/acc46h/frzr` | `sfc/acc46h/frzr` | kept | freezingrain |  |
| `sfc/acc46h/weasd` | `sfc/acc46h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc47h/apcp` | `sfc/acc47h/apcp` | kept | totalprecipitation |  |
| `sfc/acc47h/asnow` | `sfc/acc47h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc47h/frozr` | `sfc/acc47h/frozr` | kept | frozenrain |  |
| `sfc/acc47h/frzr` | `sfc/acc47h/frzr` | kept | freezingrain |  |
| `sfc/acc47h/weasd` | `sfc/acc47h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc48h/apcp` | `sfc/acc48h/apcp` | kept | totalprecipitation |  |
| `sfc/acc48h/asnow` | `sfc/acc48h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc48h/frozr` | `sfc/acc48h/frozr` | kept | frozenrain |  |
| `sfc/acc48h/frzr` | `sfc/acc48h/frzr` | kept | freezingrain |  |
| `sfc/acc48h/weasd` | `sfc/acc48h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc4h/apcp` | `sfc/acc4h/apcp` | kept | totalprecipitation |  |
| `sfc/acc4h/asnow` | `sfc/acc4h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc4h/frozr` | `sfc/acc4h/frozr` | kept | frozenrain |  |
| `sfc/acc4h/frzr` | `sfc/acc4h/frzr` | kept | freezingrain |  |
| `sfc/acc4h/weasd` | `sfc/acc4h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc5h/apcp` | `sfc/acc5h/apcp` | kept | totalprecipitation |  |
| `sfc/acc5h/asnow` | `sfc/acc5h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc5h/frozr` | `sfc/acc5h/frozr` | kept | frozenrain |  |
| `sfc/acc5h/frzr` | `sfc/acc5h/frzr` | kept | freezingrain |  |
| `sfc/acc5h/weasd` | `sfc/acc5h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc6h/apcp` | `sfc/acc6h/apcp` | kept | totalprecipitation |  |
| `sfc/acc6h/asnow` | `sfc/acc6h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc6h/frozr` | `sfc/acc6h/frozr` | kept | frozenrain |  |
| `sfc/acc6h/frzr` | `sfc/acc6h/frzr` | kept | freezingrain |  |
| `sfc/acc6h/weasd` | `sfc/acc6h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc7h/apcp` | `sfc/acc7h/apcp` | kept | totalprecipitation |  |
| `sfc/acc7h/asnow` | `sfc/acc7h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc7h/frozr` | `sfc/acc7h/frozr` | kept | frozenrain |  |
| `sfc/acc7h/frzr` | `sfc/acc7h/frzr` | kept | freezingrain |  |
| `sfc/acc7h/weasd` | `sfc/acc7h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc8h/apcp` | `sfc/acc8h/apcp` | kept | totalprecipitation |  |
| `sfc/acc8h/asnow` | `sfc/acc8h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc8h/frozr` | `sfc/acc8h/frozr` | kept | frozenrain |  |
| `sfc/acc8h/frzr` | `sfc/acc8h/frzr` | kept | freezingrain |  |
| `sfc/acc8h/weasd` | `sfc/acc8h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc9h/apcp` | `sfc/acc9h/apcp` | kept | totalprecipitation |  |
| `sfc/acc9h/asnow` | `sfc/acc9h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc9h/frozr` | `sfc/acc9h/frozr` | kept | frozenrain |  |
| `sfc/acc9h/frzr` | `sfc/acc9h/frzr` | kept | freezingrain |  |
| `sfc/acc9h/weasd` | `sfc/acc9h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/instant/cape` | `sfc/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `sfc/instant/cfnsf` | `sfc/instant/cfnsf` | kept | cloudforcingnetsolarflux |  |
| `sfc/instant/cfrzr` | `sfc/instant/categorical_freezing_rain` | eccodes | categoricalfreezingrain |  |
| `sfc/instant/cicep` | `sfc/instant/categorical_ice_pellets` | eccodes | categoricalicepellets |  |
| `sfc/instant/cin` | `sfc/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `sfc/instant/cnwat` | `sfc/instant/plant_canopy_surface_water` | eccodes | plantcanopysurfacewater |  |
| `sfc/instant/cpofp` | `sfc/instant/percent_frozen_precipitation` | eccodes | percentfrozenprecipitation |  |
| `sfc/instant/crain` | `sfc/instant/categorical_rain` | eccodes | categoricalrain |  |
| `sfc/instant/csnow` | `sfc/instant/categorical_snow` | eccodes | categoricalsnow |  |
| `sfc/instant/dlwrf` | `sfc/instant/downward_long_wave_radiation_flux` | eccodes | downwardlongwaveradiationflux |  |
| `sfc/instant/dswrf` | `sfc/instant/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/instant/fricv` | `sfc/instant/frictional_velocity` | eccodes | frictionalvelocity |  |
| `sfc/instant/gflux` | `sfc/instant/ground_heat_flux` | eccodes | groundheatflux |  |
| `sfc/instant/gust` | `sfc/instant/wind_speed` | eccodes | windgust |  |
| `sfc/instant/hgt` | `sfc/instant/hgt` | kept | geopotentialheight |  |
| `sfc/instant/hpbl` | `sfc/instant/planetary_boundary_layer_height` | eccodes | planetaryboundarylayerheight |  |
| `sfc/instant/icec` | `sfc/instant/icec` | kept | icecover |  |
| `sfc/instant/land` | `sfc/instant/land` | kept | landcover |  |
| `sfc/instant/lhtfl` | `sfc/instant/lhtfl` | kept | latentheatnetflux |  |
| `sfc/instant/prate` | `sfc/instant/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/instant/pres` | `sfc/instant/air_pressure` | abbreviation | pressure |  |
| `sfc/instant/sfcr` | `sfc/instant/sfcr` | kept | surfaceroughness |  |
| `sfc/instant/shtfl` | `sfc/instant/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/instant/snod` | `sfc/instant/snod` | kept | snowdepth |  |
| `sfc/instant/snowc` | `sfc/instant/snow_cover` | eccodes | snowcover |  |
| `sfc/instant/tmp` | `sfc/instant/tmp` | kept | temperature |  |
| `sfc/instant/ulwrf` | `sfc/instant/upward_long_wave_radiation_flux` | eccodes | upwardlongwaveradiationflux |  |
| `sfc/instant/uswrf` | `sfc/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/vbdsf` | `sfc/instant/vbdsf` | kept | visiblebeamdownwardsolarflux |  |
| `sfc/instant/vddsf` | `sfc/instant/vddsf` | kept | visiblediffusedownwardsolarflux |  |
| `sfc/instant/vgtyp` | `sfc/instant/vgtyp` | kept | vegetationtype |  |
| `sfc/instant/vis` | `sfc/instant/visible` | abbreviation | visibility |  |
| `sfc/instant/weasd` | `sfc/instant/weasd` | kept | waterequavalentsnowdepth |  |
| `sigma/avg/dzdt` | `sigma/avg/dzdt` | kept | verticalvelocitygeometric |  |
| `sigma/avg1h/dzdt` | `sigma/avg1h/dzdt` | kept | verticalvelocitygeometric |  |
| `zero_deg_isotherm/instant/hgt` | `zero_deg_isotherm/instant/hgt` | kept | geopotentialheight |  |
| `zero_deg_isotherm/instant/pres` | `zero_deg_isotherm/instant/air_pressure` | abbreviation | pressure |  |
| `zero_deg_isotherm/instant/rh` | `zero_deg_isotherm/instant/rh` | kept | relativehumidity |  |

## bkr/dmi/virtualized/hrrr_alaska_prs (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `adiabatic_condensation_lifted/instant/hgt` | `adiabatic_condensation_lifted/instant/hgt` | kept | geopotentialheight |  |
| `atm/instant/ltng` | `atm/instant/ltng` | kept | lightning |  |
| `atm/instant/refc` | `atm/instant/composite_reflectivity` | abbreviation | compositereflectivity |  |
| `atm/instant/rhpw` | `atm/instant/rhpw` | kept | relativehumidityprecipitablewater |  |
| `atm/instant/tcdc` | `atm/instant/tcdc` | kept | totalcloudcover |  |
| `atm/instant/veril` | `atm/instant/vertically_integrated_liquid` | eccodes | verticallyintegratedliquid |  |
| `clb/instant/hgt` | `clb/instant/hgt` | kept | geopotentialheight |  |
| `clb/instant/pres` | `clb/instant/air_pressure` | abbreviation | pressure |  |
| `cld_ceiling/instant/hgt` | `cld_ceiling/instant/hgt` | kept | geopotentialheight |  |
| `clt/instant/hgt` | `clt/instant/hgt` | kept | geopotentialheight |  |
| `clt/instant/pres` | `clt/instant/air_pressure` | abbreviation | pressure |  |
| `clt/instant/retop` | `clt/instant/retop` | kept | echotop |  |
| `depth_bls/instant/depth_bls` | `depth_bls/instant/depth_below_land_surface` | coordinate |  |  |
| `depth_bls/instant/mstav` | `depth_bls/instant/moisture_availability` | eccodes | moistureavailabilityncep |  |
| `depth_bls/instant/soilw` | `depth_bls/instant/volumetric_soil_moisture_content` | eccodes | volumetricsoilmoisturecontentncep |  |
| `depth_bls/instant/tsoil` | `depth_bls/instant/tsoil` | kept | soiltemperature |  |
| `entire_atm/instant/apts` | `entire_atm/instant/apts` | kept | atmosphereopticalthickness |  |
| `entire_atm/instant/colmd` | `entire_atm/instant/colmd` | kept | columnintegratedmassdensity |  |
| `entire_atm/instant/pwat` | `entire_atm/instant/precipitable_water` | eccodes | precipitablewater |  |
| `entire_atm/max/tcolg` | `entire_atm/max/total_column_graupel` | override | totalcolumnintegratedgraupel |  |
| `entire_atm/max1h/tcolg` | `entire_atm/max1h/total_column_graupel` | override | totalcolumnintegratedgraupel |  |
| `eqm/instant/hgt` | `eqm/instant/hgt` | kept | geopotentialheight |  |
| `hag/instant/dpt` | `hag/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `hag/instant/hag` | `hag/instant/height_above_ground` | coordinate |  |  |
| `hag/instant/hlcy` | `hag/instant/storm_relative_helicity` | eccodes | stormrelativehelicity |  |
| `hag/instant/massden` | `hag/instant/massden` | kept | massdensity |  |
| `hag/instant/pot` | `hag/instant/pot` | kept | potentialtemperature |  |
| `hag/instant/refd` | `hag/instant/refd` | kept | reflectivity |  |
| `hag/instant/rh` | `hag/instant/rh` | kept | relativehumidity |  |
| `hag/instant/spfh` | `hag/instant/spfh` | kept | specifichumidity |  |
| `hag/instant/tmp` | `hag/instant/tmp` | kept | temperature |  |
| `hag/instant/ugrd` | `hag/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hag/instant/utsm` | `hag/instant/utsm` | kept | ucomponentstormmotion |  |
| `hag/instant/vgrd` | `hag/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `hag/instant/vstm` | `hag/instant/v_component_storm_motion` | eccodes | vcomponentstormmotion |  |
| `hag/instant/vucsh` | `hag/instant/vertical_u_component_shear` | eccodes | verticalucomponentshear |  |
| `hag/instant/vvcsh` | `hag/instant/vertical_v_component_shear` | eccodes | verticalvcomponentshear |  |
| `hag/max/hag` | `hag/max/height_above_ground` | coordinate |  |  |
| `hag/max/maxref` | `hag/max/maxref` | kept | hourlymaxsimulatedreflectivity |  |
| `hag/max/maxuw` | `hag/max/maxuw` | kept | ucomponenthourlymaximumwindspeed |  |
| `hag/max/maxvw` | `hag/max/maxvw` | kept | vcomponenthourlymaximumwindspeed |  |
| `hag/max/mxuphl` | `hag/max/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max/relv` | `hag/max/relative_vorticity` | eccodes | relativevorticity |  |
| `hag/max/wind` | `hag/max/10m_wind_speed` | eccodes | windspeed |  |
| `hag/max1h/hag` | `hag/max1h/height_above_ground` | coordinate |  |  |
| `hag/max1h/maxref` | `hag/max1h/maxref` | kept | hourlymaxsimulatedreflectivity |  |
| `hag/max1h/maxuw` | `hag/max1h/maxuw` | kept | ucomponenthourlymaximumwindspeed |  |
| `hag/max1h/maxvw` | `hag/max1h/maxvw` | kept | vcomponenthourlymaximumwindspeed |  |
| `hag/max1h/mxuphl` | `hag/max1h/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max1h/relv` | `hag/max1h/relative_vorticity` | eccodes | relativevorticity |  |
| `hag/max1h/wind` | `hag/max1h/10m_wind_speed` | eccodes | windspeed |  |
| `hag/min/hag` | `hag/min/height_above_ground` | coordinate |  |  |
| `hag/min/mnuphl` | `hag/min/mnuphl` | kept | hourlyminupdrafthelicity |  |
| `hag/min1h/hag` | `hag/min1h/height_above_ground` | coordinate |  |  |
| `hag/min1h/mnuphl` | `hag/min1h/mnuphl` | kept | hourlyminupdrafthelicity |  |
| `hcl/instant/hcdc` | `hcl/instant/hcdc` | kept | highcloudcover |  |
| `htfl/instant/hgt` | `htfl/instant/hgt` | kept | geopotentialheight |  |
| `htfl/instant/pres` | `htfl/instant/air_pressure` | abbreviation | pressure |  |
| `htfl/instant/rh` | `htfl/instant/rh` | kept | relativehumidity |  |
| `isobar/instant/absv` | `isobar/instant/absolute_vorticity` | eccodes | absolutevorticity |  |
| `isobar/instant/cimixr` | `isobar/instant/cimixr` | kept | cloudicemixingratio |  |
| `isobar/instant/clwmr` | `isobar/instant/cloud_mixing_ratio` | eccodes | cloudmixingratio |  |
| `isobar/instant/dpt` | `isobar/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `isobar/instant/grle` | `isobar/instant/graupel` | eccodes | graupel |  |
| `isobar/instant/hgt` | `isobar/instant/hgt` | kept | geopotentialheight |  |
| `isobar/instant/isobar` | `isobar/instant/pressure_level` | coordinate |  |  |
| `isobar/instant/lftx` | `isobar/instant/surface_lifted_index` | eccodes | surfaceliftedindex |  |
| `isobar/instant/rh` | `isobar/instant/rh` | kept | relativehumidity |  |
| `isobar/instant/rwmr` | `isobar/instant/rain_mixing_ratio` | eccodes | rainmixingratio |  |
| `isobar/instant/snmr` | `isobar/instant/snow_mixing_ratio` | eccodes | snowmixingratio |  |
| `isobar/instant/spfh` | `isobar/instant/spfh` | kept | specifichumidity |  |
| `isobar/instant/tmp` | `isobar/instant/tmp` | kept | temperature |  |
| `isobar/instant/ugrd` | `isobar/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `isobar/instant/vgrd` | `isobar/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `isobar/instant/vvel` | `isobar/instant/vvel` | kept | verticalvelocitypressure |  |
| `isotherm/instant/hgt` | `isotherm/instant/hgt` | kept | geopotentialheight |  |
| `isotherm/instant/refd` | `isotherm/instant/refd` | kept | reflectivity |  |
| `isotherm/max/refd` | `isotherm/max/refd` | kept | reflectivity |  |
| `isotherm/max1h/refd` | `isotherm/max1h/refd` | kept | reflectivity |  |
| `lcl/instant/lcdc` | `lcl/instant/lcdc` | kept | lowcloudcover |  |
| `mcl/instant/mcdc` | `mcl/instant/mcdc` | kept | mediumcloudcover |  |
| `msl/instant/mslma` | `msl/instant/mslma` | kept | mslpmaps |  |
| `nominal_top/instant/sbt113` | `nominal_top/instant/sbt113` | kept | simulatedbrightnesstempgoes11ch3 |  |
| `nominal_top/instant/sbt114` | `nominal_top/instant/sbt114` | kept | simulatedbrightnesstempgoes11ch4 |  |
| `nominal_top/instant/sbt123` | `nominal_top/instant/sbt123` | kept | simulatedbrightnesstempgoes12ch3 |  |
| `nominal_top/instant/sbt124` | `nominal_top/instant/sbt124` | kept | simulatedbrightnesstempgoes12ch4 |  |
| `nominal_top/instant/ulwrf` | `nominal_top/instant/upward_long_wave_radiation_flux` | eccodes | upwardlongwaveradiationflux |  |
| `nominal_top/instant/uswrf` | `nominal_top/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `pres_diff/instant/4lftx` | `pres_diff/instant/best_lifted_index` | eccodes | best4layerliftedindexncep |  |
| `pres_diff/instant/cape` | `pres_diff/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `pres_diff/instant/cin` | `pres_diff/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `pres_diff/instant/plpl` | `pres_diff/instant/plpl` | kept | pressureofliftedparcel |  |
| `pres_diff/max/maxdvv` | `pres_diff/max/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `pres_diff/max/maxuvv` | `pres_diff/max/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `pres_diff/max1h/maxdvv` | `pres_diff/max1h/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `pres_diff/max1h/maxuvv` | `pres_diff/max1h/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `sfc/acc/apcp` | `sfc/acc/apcp` | kept | totalprecipitation |  |
| `sfc/acc/asnow` | `sfc/acc/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc/bgrun` | `sfc/acc/baseflow_groundwater_runoff` | eccodes | baseflowgroundwaterrunoff |  |
| `sfc/acc/frozr` | `sfc/acc/frozr` | kept | frozenrain |  |
| `sfc/acc/frzr` | `sfc/acc/frzr` | kept | freezingrain |  |
| `sfc/acc/ssrun` | `sfc/acc/storm_surface_runoff` | eccodes | stormsurfacerunoff |  |
| `sfc/acc/weasd` | `sfc/acc/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc10h/apcp` | `sfc/acc10h/apcp` | kept | totalprecipitation |  |
| `sfc/acc10h/asnow` | `sfc/acc10h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc10h/frozr` | `sfc/acc10h/frozr` | kept | frozenrain |  |
| `sfc/acc10h/frzr` | `sfc/acc10h/frzr` | kept | freezingrain |  |
| `sfc/acc10h/weasd` | `sfc/acc10h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc11h/apcp` | `sfc/acc11h/apcp` | kept | totalprecipitation |  |
| `sfc/acc11h/asnow` | `sfc/acc11h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc11h/frozr` | `sfc/acc11h/frozr` | kept | frozenrain |  |
| `sfc/acc11h/frzr` | `sfc/acc11h/frzr` | kept | freezingrain |  |
| `sfc/acc11h/weasd` | `sfc/acc11h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc12h/apcp` | `sfc/acc12h/apcp` | kept | totalprecipitation |  |
| `sfc/acc12h/asnow` | `sfc/acc12h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc12h/frozr` | `sfc/acc12h/frozr` | kept | frozenrain |  |
| `sfc/acc12h/frzr` | `sfc/acc12h/frzr` | kept | freezingrain |  |
| `sfc/acc12h/weasd` | `sfc/acc12h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc13h/apcp` | `sfc/acc13h/apcp` | kept | totalprecipitation |  |
| `sfc/acc13h/asnow` | `sfc/acc13h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc13h/frozr` | `sfc/acc13h/frozr` | kept | frozenrain |  |
| `sfc/acc13h/frzr` | `sfc/acc13h/frzr` | kept | freezingrain |  |
| `sfc/acc13h/weasd` | `sfc/acc13h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc14h/apcp` | `sfc/acc14h/apcp` | kept | totalprecipitation |  |
| `sfc/acc14h/asnow` | `sfc/acc14h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc14h/frozr` | `sfc/acc14h/frozr` | kept | frozenrain |  |
| `sfc/acc14h/frzr` | `sfc/acc14h/frzr` | kept | freezingrain |  |
| `sfc/acc14h/weasd` | `sfc/acc14h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc15h/apcp` | `sfc/acc15h/apcp` | kept | totalprecipitation |  |
| `sfc/acc15h/asnow` | `sfc/acc15h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc15h/frozr` | `sfc/acc15h/frozr` | kept | frozenrain |  |
| `sfc/acc15h/frzr` | `sfc/acc15h/frzr` | kept | freezingrain |  |
| `sfc/acc15h/weasd` | `sfc/acc15h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc16h/apcp` | `sfc/acc16h/apcp` | kept | totalprecipitation |  |
| `sfc/acc16h/asnow` | `sfc/acc16h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc16h/frozr` | `sfc/acc16h/frozr` | kept | frozenrain |  |
| `sfc/acc16h/frzr` | `sfc/acc16h/frzr` | kept | freezingrain |  |
| `sfc/acc16h/weasd` | `sfc/acc16h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc17h/apcp` | `sfc/acc17h/apcp` | kept | totalprecipitation |  |
| `sfc/acc17h/asnow` | `sfc/acc17h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc17h/frozr` | `sfc/acc17h/frozr` | kept | frozenrain |  |
| `sfc/acc17h/frzr` | `sfc/acc17h/frzr` | kept | freezingrain |  |
| `sfc/acc17h/weasd` | `sfc/acc17h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc18h/apcp` | `sfc/acc18h/apcp` | kept | totalprecipitation |  |
| `sfc/acc18h/asnow` | `sfc/acc18h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc18h/frozr` | `sfc/acc18h/frozr` | kept | frozenrain |  |
| `sfc/acc18h/frzr` | `sfc/acc18h/frzr` | kept | freezingrain |  |
| `sfc/acc18h/weasd` | `sfc/acc18h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc19h/apcp` | `sfc/acc19h/apcp` | kept | totalprecipitation |  |
| `sfc/acc19h/asnow` | `sfc/acc19h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc19h/frozr` | `sfc/acc19h/frozr` | kept | frozenrain |  |
| `sfc/acc19h/frzr` | `sfc/acc19h/frzr` | kept | freezingrain |  |
| `sfc/acc19h/weasd` | `sfc/acc19h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc1h/apcp` | `sfc/acc1h/apcp` | kept | totalprecipitation |  |
| `sfc/acc1h/asnow` | `sfc/acc1h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc1h/bgrun` | `sfc/acc1h/baseflow_groundwater_runoff` | eccodes | baseflowgroundwaterrunoff |  |
| `sfc/acc1h/frozr` | `sfc/acc1h/frozr` | kept | frozenrain |  |
| `sfc/acc1h/frzr` | `sfc/acc1h/frzr` | kept | freezingrain |  |
| `sfc/acc1h/ssrun` | `sfc/acc1h/storm_surface_runoff` | eccodes | stormsurfacerunoff |  |
| `sfc/acc1h/weasd` | `sfc/acc1h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc20h/apcp` | `sfc/acc20h/apcp` | kept | totalprecipitation |  |
| `sfc/acc20h/asnow` | `sfc/acc20h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc20h/frozr` | `sfc/acc20h/frozr` | kept | frozenrain |  |
| `sfc/acc20h/frzr` | `sfc/acc20h/frzr` | kept | freezingrain |  |
| `sfc/acc20h/weasd` | `sfc/acc20h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc21h/apcp` | `sfc/acc21h/apcp` | kept | totalprecipitation |  |
| `sfc/acc21h/asnow` | `sfc/acc21h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc21h/frozr` | `sfc/acc21h/frozr` | kept | frozenrain |  |
| `sfc/acc21h/frzr` | `sfc/acc21h/frzr` | kept | freezingrain |  |
| `sfc/acc21h/weasd` | `sfc/acc21h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc22h/apcp` | `sfc/acc22h/apcp` | kept | totalprecipitation |  |
| `sfc/acc22h/asnow` | `sfc/acc22h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc22h/frozr` | `sfc/acc22h/frozr` | kept | frozenrain |  |
| `sfc/acc22h/frzr` | `sfc/acc22h/frzr` | kept | freezingrain |  |
| `sfc/acc22h/weasd` | `sfc/acc22h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc23h/apcp` | `sfc/acc23h/apcp` | kept | totalprecipitation |  |
| `sfc/acc23h/asnow` | `sfc/acc23h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc23h/frozr` | `sfc/acc23h/frozr` | kept | frozenrain |  |
| `sfc/acc23h/frzr` | `sfc/acc23h/frzr` | kept | freezingrain |  |
| `sfc/acc23h/weasd` | `sfc/acc23h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc24h/apcp` | `sfc/acc24h/apcp` | kept | totalprecipitation |  |
| `sfc/acc24h/asnow` | `sfc/acc24h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc24h/frozr` | `sfc/acc24h/frozr` | kept | frozenrain |  |
| `sfc/acc24h/frzr` | `sfc/acc24h/frzr` | kept | freezingrain |  |
| `sfc/acc24h/weasd` | `sfc/acc24h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc25h/apcp` | `sfc/acc25h/apcp` | kept | totalprecipitation |  |
| `sfc/acc25h/asnow` | `sfc/acc25h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc25h/frozr` | `sfc/acc25h/frozr` | kept | frozenrain |  |
| `sfc/acc25h/frzr` | `sfc/acc25h/frzr` | kept | freezingrain |  |
| `sfc/acc25h/weasd` | `sfc/acc25h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc26h/apcp` | `sfc/acc26h/apcp` | kept | totalprecipitation |  |
| `sfc/acc26h/asnow` | `sfc/acc26h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc26h/frozr` | `sfc/acc26h/frozr` | kept | frozenrain |  |
| `sfc/acc26h/frzr` | `sfc/acc26h/frzr` | kept | freezingrain |  |
| `sfc/acc26h/weasd` | `sfc/acc26h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc27h/apcp` | `sfc/acc27h/apcp` | kept | totalprecipitation |  |
| `sfc/acc27h/asnow` | `sfc/acc27h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc27h/frozr` | `sfc/acc27h/frozr` | kept | frozenrain |  |
| `sfc/acc27h/frzr` | `sfc/acc27h/frzr` | kept | freezingrain |  |
| `sfc/acc27h/weasd` | `sfc/acc27h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc28h/apcp` | `sfc/acc28h/apcp` | kept | totalprecipitation |  |
| `sfc/acc28h/asnow` | `sfc/acc28h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc28h/frozr` | `sfc/acc28h/frozr` | kept | frozenrain |  |
| `sfc/acc28h/frzr` | `sfc/acc28h/frzr` | kept | freezingrain |  |
| `sfc/acc28h/weasd` | `sfc/acc28h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc29h/apcp` | `sfc/acc29h/apcp` | kept | totalprecipitation |  |
| `sfc/acc29h/asnow` | `sfc/acc29h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc29h/frozr` | `sfc/acc29h/frozr` | kept | frozenrain |  |
| `sfc/acc29h/frzr` | `sfc/acc29h/frzr` | kept | freezingrain |  |
| `sfc/acc29h/weasd` | `sfc/acc29h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc2h/apcp` | `sfc/acc2h/apcp` | kept | totalprecipitation |  |
| `sfc/acc2h/asnow` | `sfc/acc2h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc2h/frozr` | `sfc/acc2h/frozr` | kept | frozenrain |  |
| `sfc/acc2h/frzr` | `sfc/acc2h/frzr` | kept | freezingrain |  |
| `sfc/acc2h/weasd` | `sfc/acc2h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc30h/apcp` | `sfc/acc30h/apcp` | kept | totalprecipitation |  |
| `sfc/acc30h/asnow` | `sfc/acc30h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc30h/frozr` | `sfc/acc30h/frozr` | kept | frozenrain |  |
| `sfc/acc30h/frzr` | `sfc/acc30h/frzr` | kept | freezingrain |  |
| `sfc/acc30h/weasd` | `sfc/acc30h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc31h/apcp` | `sfc/acc31h/apcp` | kept | totalprecipitation |  |
| `sfc/acc31h/asnow` | `sfc/acc31h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc31h/frozr` | `sfc/acc31h/frozr` | kept | frozenrain |  |
| `sfc/acc31h/frzr` | `sfc/acc31h/frzr` | kept | freezingrain |  |
| `sfc/acc31h/weasd` | `sfc/acc31h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc32h/apcp` | `sfc/acc32h/apcp` | kept | totalprecipitation |  |
| `sfc/acc32h/asnow` | `sfc/acc32h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc32h/frozr` | `sfc/acc32h/frozr` | kept | frozenrain |  |
| `sfc/acc32h/frzr` | `sfc/acc32h/frzr` | kept | freezingrain |  |
| `sfc/acc32h/weasd` | `sfc/acc32h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc33h/apcp` | `sfc/acc33h/apcp` | kept | totalprecipitation |  |
| `sfc/acc33h/asnow` | `sfc/acc33h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc33h/frozr` | `sfc/acc33h/frozr` | kept | frozenrain |  |
| `sfc/acc33h/frzr` | `sfc/acc33h/frzr` | kept | freezingrain |  |
| `sfc/acc33h/weasd` | `sfc/acc33h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc34h/apcp` | `sfc/acc34h/apcp` | kept | totalprecipitation |  |
| `sfc/acc34h/asnow` | `sfc/acc34h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc34h/frozr` | `sfc/acc34h/frozr` | kept | frozenrain |  |
| `sfc/acc34h/frzr` | `sfc/acc34h/frzr` | kept | freezingrain |  |
| `sfc/acc34h/weasd` | `sfc/acc34h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc35h/apcp` | `sfc/acc35h/apcp` | kept | totalprecipitation |  |
| `sfc/acc35h/asnow` | `sfc/acc35h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc35h/frozr` | `sfc/acc35h/frozr` | kept | frozenrain |  |
| `sfc/acc35h/frzr` | `sfc/acc35h/frzr` | kept | freezingrain |  |
| `sfc/acc35h/weasd` | `sfc/acc35h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc36h/apcp` | `sfc/acc36h/apcp` | kept | totalprecipitation |  |
| `sfc/acc36h/asnow` | `sfc/acc36h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc36h/frozr` | `sfc/acc36h/frozr` | kept | frozenrain |  |
| `sfc/acc36h/frzr` | `sfc/acc36h/frzr` | kept | freezingrain |  |
| `sfc/acc36h/weasd` | `sfc/acc36h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc37h/apcp` | `sfc/acc37h/apcp` | kept | totalprecipitation |  |
| `sfc/acc37h/asnow` | `sfc/acc37h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc37h/frozr` | `sfc/acc37h/frozr` | kept | frozenrain |  |
| `sfc/acc37h/frzr` | `sfc/acc37h/frzr` | kept | freezingrain |  |
| `sfc/acc37h/weasd` | `sfc/acc37h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc38h/apcp` | `sfc/acc38h/apcp` | kept | totalprecipitation |  |
| `sfc/acc38h/asnow` | `sfc/acc38h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc38h/frozr` | `sfc/acc38h/frozr` | kept | frozenrain |  |
| `sfc/acc38h/frzr` | `sfc/acc38h/frzr` | kept | freezingrain |  |
| `sfc/acc38h/weasd` | `sfc/acc38h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc39h/apcp` | `sfc/acc39h/apcp` | kept | totalprecipitation |  |
| `sfc/acc39h/asnow` | `sfc/acc39h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc39h/frozr` | `sfc/acc39h/frozr` | kept | frozenrain |  |
| `sfc/acc39h/frzr` | `sfc/acc39h/frzr` | kept | freezingrain |  |
| `sfc/acc39h/weasd` | `sfc/acc39h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc3h/apcp` | `sfc/acc3h/apcp` | kept | totalprecipitation |  |
| `sfc/acc3h/asnow` | `sfc/acc3h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc3h/frozr` | `sfc/acc3h/frozr` | kept | frozenrain |  |
| `sfc/acc3h/frzr` | `sfc/acc3h/frzr` | kept | freezingrain |  |
| `sfc/acc3h/weasd` | `sfc/acc3h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc40h/apcp` | `sfc/acc40h/apcp` | kept | totalprecipitation |  |
| `sfc/acc40h/asnow` | `sfc/acc40h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc40h/frozr` | `sfc/acc40h/frozr` | kept | frozenrain |  |
| `sfc/acc40h/frzr` | `sfc/acc40h/frzr` | kept | freezingrain |  |
| `sfc/acc40h/weasd` | `sfc/acc40h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc41h/apcp` | `sfc/acc41h/apcp` | kept | totalprecipitation |  |
| `sfc/acc41h/asnow` | `sfc/acc41h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc41h/frozr` | `sfc/acc41h/frozr` | kept | frozenrain |  |
| `sfc/acc41h/frzr` | `sfc/acc41h/frzr` | kept | freezingrain |  |
| `sfc/acc41h/weasd` | `sfc/acc41h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc42h/apcp` | `sfc/acc42h/apcp` | kept | totalprecipitation |  |
| `sfc/acc42h/asnow` | `sfc/acc42h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc42h/frozr` | `sfc/acc42h/frozr` | kept | frozenrain |  |
| `sfc/acc42h/frzr` | `sfc/acc42h/frzr` | kept | freezingrain |  |
| `sfc/acc42h/weasd` | `sfc/acc42h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc43h/apcp` | `sfc/acc43h/apcp` | kept | totalprecipitation |  |
| `sfc/acc43h/asnow` | `sfc/acc43h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc43h/frozr` | `sfc/acc43h/frozr` | kept | frozenrain |  |
| `sfc/acc43h/frzr` | `sfc/acc43h/frzr` | kept | freezingrain |  |
| `sfc/acc43h/weasd` | `sfc/acc43h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc44h/apcp` | `sfc/acc44h/apcp` | kept | totalprecipitation |  |
| `sfc/acc44h/asnow` | `sfc/acc44h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc44h/frozr` | `sfc/acc44h/frozr` | kept | frozenrain |  |
| `sfc/acc44h/frzr` | `sfc/acc44h/frzr` | kept | freezingrain |  |
| `sfc/acc44h/weasd` | `sfc/acc44h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc45h/apcp` | `sfc/acc45h/apcp` | kept | totalprecipitation |  |
| `sfc/acc45h/asnow` | `sfc/acc45h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc45h/frozr` | `sfc/acc45h/frozr` | kept | frozenrain |  |
| `sfc/acc45h/frzr` | `sfc/acc45h/frzr` | kept | freezingrain |  |
| `sfc/acc45h/weasd` | `sfc/acc45h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc46h/apcp` | `sfc/acc46h/apcp` | kept | totalprecipitation |  |
| `sfc/acc46h/asnow` | `sfc/acc46h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc46h/frozr` | `sfc/acc46h/frozr` | kept | frozenrain |  |
| `sfc/acc46h/frzr` | `sfc/acc46h/frzr` | kept | freezingrain |  |
| `sfc/acc46h/weasd` | `sfc/acc46h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc47h/apcp` | `sfc/acc47h/apcp` | kept | totalprecipitation |  |
| `sfc/acc47h/asnow` | `sfc/acc47h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc47h/frozr` | `sfc/acc47h/frozr` | kept | frozenrain |  |
| `sfc/acc47h/frzr` | `sfc/acc47h/frzr` | kept | freezingrain |  |
| `sfc/acc47h/weasd` | `sfc/acc47h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc48h/apcp` | `sfc/acc48h/apcp` | kept | totalprecipitation |  |
| `sfc/acc48h/asnow` | `sfc/acc48h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc48h/frozr` | `sfc/acc48h/frozr` | kept | frozenrain |  |
| `sfc/acc48h/frzr` | `sfc/acc48h/frzr` | kept | freezingrain |  |
| `sfc/acc48h/weasd` | `sfc/acc48h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc4h/apcp` | `sfc/acc4h/apcp` | kept | totalprecipitation |  |
| `sfc/acc4h/asnow` | `sfc/acc4h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc4h/frozr` | `sfc/acc4h/frozr` | kept | frozenrain |  |
| `sfc/acc4h/frzr` | `sfc/acc4h/frzr` | kept | freezingrain |  |
| `sfc/acc4h/weasd` | `sfc/acc4h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc5h/apcp` | `sfc/acc5h/apcp` | kept | totalprecipitation |  |
| `sfc/acc5h/asnow` | `sfc/acc5h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc5h/frozr` | `sfc/acc5h/frozr` | kept | frozenrain |  |
| `sfc/acc5h/frzr` | `sfc/acc5h/frzr` | kept | freezingrain |  |
| `sfc/acc5h/weasd` | `sfc/acc5h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc6h/apcp` | `sfc/acc6h/apcp` | kept | totalprecipitation |  |
| `sfc/acc6h/asnow` | `sfc/acc6h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc6h/frozr` | `sfc/acc6h/frozr` | kept | frozenrain |  |
| `sfc/acc6h/frzr` | `sfc/acc6h/frzr` | kept | freezingrain |  |
| `sfc/acc6h/weasd` | `sfc/acc6h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc7h/apcp` | `sfc/acc7h/apcp` | kept | totalprecipitation |  |
| `sfc/acc7h/asnow` | `sfc/acc7h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc7h/frozr` | `sfc/acc7h/frozr` | kept | frozenrain |  |
| `sfc/acc7h/frzr` | `sfc/acc7h/frzr` | kept | freezingrain |  |
| `sfc/acc7h/weasd` | `sfc/acc7h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc8h/apcp` | `sfc/acc8h/apcp` | kept | totalprecipitation |  |
| `sfc/acc8h/asnow` | `sfc/acc8h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc8h/frozr` | `sfc/acc8h/frozr` | kept | frozenrain |  |
| `sfc/acc8h/frzr` | `sfc/acc8h/frzr` | kept | freezingrain |  |
| `sfc/acc8h/weasd` | `sfc/acc8h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc9h/apcp` | `sfc/acc9h/apcp` | kept | totalprecipitation |  |
| `sfc/acc9h/asnow` | `sfc/acc9h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc9h/frozr` | `sfc/acc9h/frozr` | kept | frozenrain |  |
| `sfc/acc9h/frzr` | `sfc/acc9h/frzr` | kept | freezingrain |  |
| `sfc/acc9h/weasd` | `sfc/acc9h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/instant/cape` | `sfc/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `sfc/instant/cfnsf` | `sfc/instant/cfnsf` | kept | cloudforcingnetsolarflux |  |
| `sfc/instant/cfrzr` | `sfc/instant/categorical_freezing_rain` | eccodes | categoricalfreezingrain |  |
| `sfc/instant/cicep` | `sfc/instant/categorical_ice_pellets` | eccodes | categoricalicepellets |  |
| `sfc/instant/cin` | `sfc/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `sfc/instant/cnwat` | `sfc/instant/plant_canopy_surface_water` | eccodes | plantcanopysurfacewater |  |
| `sfc/instant/cpofp` | `sfc/instant/percent_frozen_precipitation` | eccodes | percentfrozenprecipitation |  |
| `sfc/instant/crain` | `sfc/instant/categorical_rain` | eccodes | categoricalrain |  |
| `sfc/instant/csnow` | `sfc/instant/categorical_snow` | eccodes | categoricalsnow |  |
| `sfc/instant/dlwrf` | `sfc/instant/downward_long_wave_radiation_flux` | eccodes | downwardlongwaveradiationflux |  |
| `sfc/instant/dswrf` | `sfc/instant/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/instant/fricv` | `sfc/instant/frictional_velocity` | eccodes | frictionalvelocity |  |
| `sfc/instant/gflux` | `sfc/instant/ground_heat_flux` | eccodes | groundheatflux |  |
| `sfc/instant/gust` | `sfc/instant/wind_speed` | eccodes | windgust |  |
| `sfc/instant/hgt` | `sfc/instant/hgt` | kept | geopotentialheight |  |
| `sfc/instant/hpbl` | `sfc/instant/planetary_boundary_layer_height` | eccodes | planetaryboundarylayerheight |  |
| `sfc/instant/icec` | `sfc/instant/icec` | kept | icecover |  |
| `sfc/instant/land` | `sfc/instant/land` | kept | landcover |  |
| `sfc/instant/lhtfl` | `sfc/instant/lhtfl` | kept | latentheatnetflux |  |
| `sfc/instant/prate` | `sfc/instant/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/instant/pres` | `sfc/instant/air_pressure` | abbreviation | pressure |  |
| `sfc/instant/sfcr` | `sfc/instant/sfcr` | kept | surfaceroughness |  |
| `sfc/instant/shtfl` | `sfc/instant/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/instant/snod` | `sfc/instant/snod` | kept | snowdepth |  |
| `sfc/instant/snowc` | `sfc/instant/snow_cover` | eccodes | snowcover |  |
| `sfc/instant/tmp` | `sfc/instant/tmp` | kept | temperature |  |
| `sfc/instant/ulwrf` | `sfc/instant/upward_long_wave_radiation_flux` | eccodes | upwardlongwaveradiationflux |  |
| `sfc/instant/uswrf` | `sfc/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/vbdsf` | `sfc/instant/vbdsf` | kept | visiblebeamdownwardsolarflux |  |
| `sfc/instant/vddsf` | `sfc/instant/vddsf` | kept | visiblediffusedownwardsolarflux |  |
| `sfc/instant/vgtyp` | `sfc/instant/vgtyp` | kept | vegetationtype |  |
| `sfc/instant/vis` | `sfc/instant/visible` | abbreviation | visibility |  |
| `sfc/instant/weasd` | `sfc/instant/weasd` | kept | waterequavalentsnowdepth |  |
| `sigma/avg/dzdt` | `sigma/avg/dzdt` | kept | verticalvelocitygeometric |  |
| `sigma/avg1h/dzdt` | `sigma/avg1h/dzdt` | kept | verticalvelocitygeometric |  |
| `zero_deg_isotherm/instant/hgt` | `zero_deg_isotherm/instant/hgt` | kept | geopotentialheight |  |
| `zero_deg_isotherm/instant/pres` | `zero_deg_isotherm/instant/air_pressure` | abbreviation | pressure |  |
| `zero_deg_isotherm/instant/rh` | `zero_deg_isotherm/instant/rh` | kept | relativehumidity |  |

## bkr/dmi/virtualized/hrrr_alaska_sfc (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `adiabatic_condensation_lifted/instant/hgt` | `adiabatic_condensation_lifted/instant/hgt` | kept | geopotentialheight |  |
| `atm/instant/ltng` | `atm/instant/ltng` | kept | lightning |  |
| `atm/instant/refc` | `atm/instant/composite_reflectivity` | abbreviation | compositereflectivity |  |
| `atm/instant/rhpw` | `atm/instant/rhpw` | kept | relativehumidityprecipitablewater |  |
| `atm/instant/tcdc` | `atm/instant/tcdc` | kept | totalcloudcover |  |
| `atm/instant/veril` | `atm/instant/vertically_integrated_liquid` | eccodes | verticallyintegratedliquid |  |
| `clb/instant/hgt` | `clb/instant/hgt` | kept | geopotentialheight |  |
| `clb/instant/pres` | `clb/instant/air_pressure` | abbreviation | pressure |  |
| `cld_ceiling/instant/hgt` | `cld_ceiling/instant/hgt` | kept | geopotentialheight |  |
| `clt/instant/hgt` | `clt/instant/hgt` | kept | geopotentialheight |  |
| `clt/instant/pres` | `clt/instant/air_pressure` | abbreviation | pressure |  |
| `clt/instant/retop` | `clt/instant/retop` | kept | echotop |  |
| `depth_bls/instant/mstav` | `depth_bls/instant/moisture_availability` | eccodes | moistureavailabilityncep |  |
| `entire_atm/instant/pwat` | `entire_atm/instant/precipitable_water` | eccodes | precipitablewater |  |
| `entire_atm/max/tcolg` | `entire_atm/max/total_column_graupel` | override | totalcolumnintegratedgraupel |  |
| `entire_atm/max1h/tcolg` | `entire_atm/max1h/total_column_graupel` | override | totalcolumnintegratedgraupel |  |
| `eqm/instant/hgt` | `eqm/instant/hgt` | kept | geopotentialheight |  |
| `hag/instant/dpt` | `hag/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `hag/instant/hag` | `hag/instant/height_above_ground` | coordinate |  |  |
| `hag/instant/hlcy` | `hag/instant/storm_relative_helicity` | eccodes | stormrelativehelicity |  |
| `hag/instant/pot` | `hag/instant/pot` | kept | potentialtemperature |  |
| `hag/instant/refd` | `hag/instant/refd` | kept | reflectivity |  |
| `hag/instant/rh` | `hag/instant/rh` | kept | relativehumidity |  |
| `hag/instant/spfh` | `hag/instant/spfh` | kept | specifichumidity |  |
| `hag/instant/tmp` | `hag/instant/tmp` | kept | temperature |  |
| `hag/instant/ugrd` | `hag/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hag/instant/utsm` | `hag/instant/utsm` | kept | ucomponentstormmotion |  |
| `hag/instant/vgrd` | `hag/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `hag/instant/vstm` | `hag/instant/v_component_storm_motion` | eccodes | vcomponentstormmotion |  |
| `hag/instant/vucsh` | `hag/instant/vertical_u_component_shear` | eccodes | verticalucomponentshear |  |
| `hag/instant/vvcsh` | `hag/instant/vertical_v_component_shear` | eccodes | verticalvcomponentshear |  |
| `hag/max/hag` | `hag/max/height_above_ground` | coordinate |  |  |
| `hag/max/maxref` | `hag/max/maxref` | kept | hourlymaxsimulatedreflectivity |  |
| `hag/max/maxuw` | `hag/max/maxuw` | kept | ucomponenthourlymaximumwindspeed |  |
| `hag/max/maxvw` | `hag/max/maxvw` | kept | vcomponenthourlymaximumwindspeed |  |
| `hag/max/mxuphl` | `hag/max/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max/relv` | `hag/max/relative_vorticity` | eccodes | relativevorticity |  |
| `hag/max/wind` | `hag/max/10m_wind_speed` | eccodes | windspeed |  |
| `hag/max1h/hag` | `hag/max1h/height_above_ground` | coordinate |  |  |
| `hag/max1h/maxref` | `hag/max1h/maxref` | kept | hourlymaxsimulatedreflectivity |  |
| `hag/max1h/maxuw` | `hag/max1h/maxuw` | kept | ucomponenthourlymaximumwindspeed |  |
| `hag/max1h/maxvw` | `hag/max1h/maxvw` | kept | vcomponenthourlymaximumwindspeed |  |
| `hag/max1h/mxuphl` | `hag/max1h/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max1h/relv` | `hag/max1h/relative_vorticity` | eccodes | relativevorticity |  |
| `hag/max1h/wind` | `hag/max1h/10m_wind_speed` | eccodes | windspeed |  |
| `hag/min/hag` | `hag/min/height_above_ground` | coordinate |  |  |
| `hag/min/mnuphl` | `hag/min/mnuphl` | kept | hourlyminupdrafthelicity |  |
| `hag/min1h/hag` | `hag/min1h/height_above_ground` | coordinate |  |  |
| `hag/min1h/mnuphl` | `hag/min1h/mnuphl` | kept | hourlyminupdrafthelicity |  |
| `hcl/instant/hcdc` | `hcl/instant/hcdc` | kept | highcloudcover |  |
| `htfl/instant/hgt` | `htfl/instant/hgt` | kept | geopotentialheight |  |
| `htfl/instant/pres` | `htfl/instant/air_pressure` | abbreviation | pressure |  |
| `htfl/instant/rh` | `htfl/instant/rh` | kept | relativehumidity |  |
| `isobar/instant/dpt` | `isobar/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `isobar/instant/hgt` | `isobar/instant/hgt` | kept | geopotentialheight |  |
| `isobar/instant/isobar` | `isobar/instant/pressure_level` | coordinate |  |  |
| `isobar/instant/lftx` | `isobar/instant/surface_lifted_index` | eccodes | surfaceliftedindex |  |
| `isobar/instant/tmp` | `isobar/instant/tmp` | kept | temperature |  |
| `isobar/instant/ugrd` | `isobar/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `isobar/instant/vgrd` | `isobar/instant/v_component_of_wind_gradient` | eccodes | vcomponentwindspeed |  |
| `isotherm/instant/hgt` | `isotherm/instant/hgt` | kept | geopotentialheight |  |
| `isotherm/instant/refd` | `isotherm/instant/refd` | kept | reflectivity |  |
| `isotherm/max/refd` | `isotherm/max/refd` | kept | reflectivity |  |
| `isotherm/max1h/refd` | `isotherm/max1h/refd` | kept | reflectivity |  |
| `lcl/instant/lcdc` | `lcl/instant/lcdc` | kept | lowcloudcover |  |
| `mcl/instant/mcdc` | `mcl/instant/mcdc` | kept | mediumcloudcover |  |
| `msl/instant/mslma` | `msl/instant/mslma` | kept | mslpmaps |  |
| `nominal_top/instant/sbt113` | `nominal_top/instant/sbt113` | kept | simulatedbrightnesstempgoes11ch3 |  |
| `nominal_top/instant/sbt114` | `nominal_top/instant/sbt114` | kept | simulatedbrightnesstempgoes11ch4 |  |
| `nominal_top/instant/sbt123` | `nominal_top/instant/sbt123` | kept | simulatedbrightnesstempgoes12ch3 |  |
| `nominal_top/instant/sbt124` | `nominal_top/instant/sbt124` | kept | simulatedbrightnesstempgoes12ch4 |  |
| `nominal_top/instant/ulwrf` | `nominal_top/instant/upward_long_wave_radiation_flux` | eccodes | upwardlongwaveradiationflux |  |
| `nominal_top/instant/uswrf` | `nominal_top/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `pres_diff/instant/4lftx` | `pres_diff/instant/best_lifted_index` | eccodes | best4layerliftedindexncep |  |
| `pres_diff/instant/cape` | `pres_diff/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `pres_diff/instant/cin` | `pres_diff/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `pres_diff/instant/plpl` | `pres_diff/instant/plpl` | kept | pressureofliftedparcel |  |
| `pres_diff/max/maxdvv` | `pres_diff/max/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `pres_diff/max/maxuvv` | `pres_diff/max/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `pres_diff/max1h/maxdvv` | `pres_diff/max1h/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `pres_diff/max1h/maxuvv` | `pres_diff/max1h/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `sfc/acc/apcp` | `sfc/acc/apcp` | kept | totalprecipitation |  |
| `sfc/acc/asnow` | `sfc/acc/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc/bgrun` | `sfc/acc/baseflow_groundwater_runoff` | eccodes | baseflowgroundwaterrunoff |  |
| `sfc/acc/frozr` | `sfc/acc/frozr` | kept | frozenrain |  |
| `sfc/acc/frzr` | `sfc/acc/frzr` | kept | freezingrain |  |
| `sfc/acc/ssrun` | `sfc/acc/storm_surface_runoff` | eccodes | stormsurfacerunoff |  |
| `sfc/acc/weasd` | `sfc/acc/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc10h/apcp` | `sfc/acc10h/apcp` | kept | totalprecipitation |  |
| `sfc/acc10h/asnow` | `sfc/acc10h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc10h/frozr` | `sfc/acc10h/frozr` | kept | frozenrain |  |
| `sfc/acc10h/frzr` | `sfc/acc10h/frzr` | kept | freezingrain |  |
| `sfc/acc10h/weasd` | `sfc/acc10h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc11h/apcp` | `sfc/acc11h/apcp` | kept | totalprecipitation |  |
| `sfc/acc11h/asnow` | `sfc/acc11h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc11h/frozr` | `sfc/acc11h/frozr` | kept | frozenrain |  |
| `sfc/acc11h/frzr` | `sfc/acc11h/frzr` | kept | freezingrain |  |
| `sfc/acc11h/weasd` | `sfc/acc11h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc12h/apcp` | `sfc/acc12h/apcp` | kept | totalprecipitation |  |
| `sfc/acc12h/asnow` | `sfc/acc12h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc12h/frozr` | `sfc/acc12h/frozr` | kept | frozenrain |  |
| `sfc/acc12h/frzr` | `sfc/acc12h/frzr` | kept | freezingrain |  |
| `sfc/acc12h/weasd` | `sfc/acc12h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc13h/apcp` | `sfc/acc13h/apcp` | kept | totalprecipitation |  |
| `sfc/acc13h/asnow` | `sfc/acc13h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc13h/frozr` | `sfc/acc13h/frozr` | kept | frozenrain |  |
| `sfc/acc13h/frzr` | `sfc/acc13h/frzr` | kept | freezingrain |  |
| `sfc/acc13h/weasd` | `sfc/acc13h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc14h/apcp` | `sfc/acc14h/apcp` | kept | totalprecipitation |  |
| `sfc/acc14h/asnow` | `sfc/acc14h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc14h/frozr` | `sfc/acc14h/frozr` | kept | frozenrain |  |
| `sfc/acc14h/frzr` | `sfc/acc14h/frzr` | kept | freezingrain |  |
| `sfc/acc14h/weasd` | `sfc/acc14h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc15h/apcp` | `sfc/acc15h/apcp` | kept | totalprecipitation |  |
| `sfc/acc15h/asnow` | `sfc/acc15h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc15h/frozr` | `sfc/acc15h/frozr` | kept | frozenrain |  |
| `sfc/acc15h/frzr` | `sfc/acc15h/frzr` | kept | freezingrain |  |
| `sfc/acc15h/weasd` | `sfc/acc15h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc16h/apcp` | `sfc/acc16h/apcp` | kept | totalprecipitation |  |
| `sfc/acc16h/asnow` | `sfc/acc16h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc16h/frozr` | `sfc/acc16h/frozr` | kept | frozenrain |  |
| `sfc/acc16h/frzr` | `sfc/acc16h/frzr` | kept | freezingrain |  |
| `sfc/acc16h/weasd` | `sfc/acc16h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc17h/apcp` | `sfc/acc17h/apcp` | kept | totalprecipitation |  |
| `sfc/acc17h/asnow` | `sfc/acc17h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc17h/frozr` | `sfc/acc17h/frozr` | kept | frozenrain |  |
| `sfc/acc17h/frzr` | `sfc/acc17h/frzr` | kept | freezingrain |  |
| `sfc/acc17h/weasd` | `sfc/acc17h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc18h/apcp` | `sfc/acc18h/apcp` | kept | totalprecipitation |  |
| `sfc/acc18h/asnow` | `sfc/acc18h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc18h/frozr` | `sfc/acc18h/frozr` | kept | frozenrain |  |
| `sfc/acc18h/frzr` | `sfc/acc18h/frzr` | kept | freezingrain |  |
| `sfc/acc18h/weasd` | `sfc/acc18h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc19h/apcp` | `sfc/acc19h/apcp` | kept | totalprecipitation |  |
| `sfc/acc19h/asnow` | `sfc/acc19h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc19h/frozr` | `sfc/acc19h/frozr` | kept | frozenrain |  |
| `sfc/acc19h/frzr` | `sfc/acc19h/frzr` | kept | freezingrain |  |
| `sfc/acc19h/weasd` | `sfc/acc19h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc1h/apcp` | `sfc/acc1h/apcp` | kept | totalprecipitation |  |
| `sfc/acc1h/asnow` | `sfc/acc1h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc1h/bgrun` | `sfc/acc1h/baseflow_groundwater_runoff` | eccodes | baseflowgroundwaterrunoff |  |
| `sfc/acc1h/frozr` | `sfc/acc1h/frozr` | kept | frozenrain |  |
| `sfc/acc1h/frzr` | `sfc/acc1h/frzr` | kept | freezingrain |  |
| `sfc/acc1h/ssrun` | `sfc/acc1h/storm_surface_runoff` | eccodes | stormsurfacerunoff |  |
| `sfc/acc1h/weasd` | `sfc/acc1h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc20h/apcp` | `sfc/acc20h/apcp` | kept | totalprecipitation |  |
| `sfc/acc20h/asnow` | `sfc/acc20h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc20h/frozr` | `sfc/acc20h/frozr` | kept | frozenrain |  |
| `sfc/acc20h/frzr` | `sfc/acc20h/frzr` | kept | freezingrain |  |
| `sfc/acc20h/weasd` | `sfc/acc20h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc21h/apcp` | `sfc/acc21h/apcp` | kept | totalprecipitation |  |
| `sfc/acc21h/asnow` | `sfc/acc21h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc21h/frozr` | `sfc/acc21h/frozr` | kept | frozenrain |  |
| `sfc/acc21h/frzr` | `sfc/acc21h/frzr` | kept | freezingrain |  |
| `sfc/acc21h/weasd` | `sfc/acc21h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc22h/apcp` | `sfc/acc22h/apcp` | kept | totalprecipitation |  |
| `sfc/acc22h/asnow` | `sfc/acc22h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc22h/frozr` | `sfc/acc22h/frozr` | kept | frozenrain |  |
| `sfc/acc22h/frzr` | `sfc/acc22h/frzr` | kept | freezingrain |  |
| `sfc/acc22h/weasd` | `sfc/acc22h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc23h/apcp` | `sfc/acc23h/apcp` | kept | totalprecipitation |  |
| `sfc/acc23h/asnow` | `sfc/acc23h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc23h/frozr` | `sfc/acc23h/frozr` | kept | frozenrain |  |
| `sfc/acc23h/frzr` | `sfc/acc23h/frzr` | kept | freezingrain |  |
| `sfc/acc23h/weasd` | `sfc/acc23h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc24h/apcp` | `sfc/acc24h/apcp` | kept | totalprecipitation |  |
| `sfc/acc24h/asnow` | `sfc/acc24h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc24h/frozr` | `sfc/acc24h/frozr` | kept | frozenrain |  |
| `sfc/acc24h/frzr` | `sfc/acc24h/frzr` | kept | freezingrain |  |
| `sfc/acc24h/weasd` | `sfc/acc24h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc25h/apcp` | `sfc/acc25h/apcp` | kept | totalprecipitation |  |
| `sfc/acc25h/asnow` | `sfc/acc25h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc25h/frozr` | `sfc/acc25h/frozr` | kept | frozenrain |  |
| `sfc/acc25h/frzr` | `sfc/acc25h/frzr` | kept | freezingrain |  |
| `sfc/acc25h/weasd` | `sfc/acc25h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc26h/apcp` | `sfc/acc26h/apcp` | kept | totalprecipitation |  |
| `sfc/acc26h/asnow` | `sfc/acc26h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc26h/frozr` | `sfc/acc26h/frozr` | kept | frozenrain |  |
| `sfc/acc26h/frzr` | `sfc/acc26h/frzr` | kept | freezingrain |  |
| `sfc/acc26h/weasd` | `sfc/acc26h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc27h/apcp` | `sfc/acc27h/apcp` | kept | totalprecipitation |  |
| `sfc/acc27h/asnow` | `sfc/acc27h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc27h/frozr` | `sfc/acc27h/frozr` | kept | frozenrain |  |
| `sfc/acc27h/frzr` | `sfc/acc27h/frzr` | kept | freezingrain |  |
| `sfc/acc27h/weasd` | `sfc/acc27h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc28h/apcp` | `sfc/acc28h/apcp` | kept | totalprecipitation |  |
| `sfc/acc28h/asnow` | `sfc/acc28h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc28h/frozr` | `sfc/acc28h/frozr` | kept | frozenrain |  |
| `sfc/acc28h/frzr` | `sfc/acc28h/frzr` | kept | freezingrain |  |
| `sfc/acc28h/weasd` | `sfc/acc28h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc29h/apcp` | `sfc/acc29h/apcp` | kept | totalprecipitation |  |
| `sfc/acc29h/asnow` | `sfc/acc29h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc29h/frozr` | `sfc/acc29h/frozr` | kept | frozenrain |  |
| `sfc/acc29h/frzr` | `sfc/acc29h/frzr` | kept | freezingrain |  |
| `sfc/acc29h/weasd` | `sfc/acc29h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc2h/apcp` | `sfc/acc2h/apcp` | kept | totalprecipitation |  |
| `sfc/acc2h/asnow` | `sfc/acc2h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc2h/frozr` | `sfc/acc2h/frozr` | kept | frozenrain |  |
| `sfc/acc2h/frzr` | `sfc/acc2h/frzr` | kept | freezingrain |  |
| `sfc/acc2h/weasd` | `sfc/acc2h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc30h/apcp` | `sfc/acc30h/apcp` | kept | totalprecipitation |  |
| `sfc/acc30h/asnow` | `sfc/acc30h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc30h/frozr` | `sfc/acc30h/frozr` | kept | frozenrain |  |
| `sfc/acc30h/frzr` | `sfc/acc30h/frzr` | kept | freezingrain |  |
| `sfc/acc30h/weasd` | `sfc/acc30h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc31h/apcp` | `sfc/acc31h/apcp` | kept | totalprecipitation |  |
| `sfc/acc31h/asnow` | `sfc/acc31h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc31h/frozr` | `sfc/acc31h/frozr` | kept | frozenrain |  |
| `sfc/acc31h/frzr` | `sfc/acc31h/frzr` | kept | freezingrain |  |
| `sfc/acc31h/weasd` | `sfc/acc31h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc32h/apcp` | `sfc/acc32h/apcp` | kept | totalprecipitation |  |
| `sfc/acc32h/asnow` | `sfc/acc32h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc32h/frozr` | `sfc/acc32h/frozr` | kept | frozenrain |  |
| `sfc/acc32h/frzr` | `sfc/acc32h/frzr` | kept | freezingrain |  |
| `sfc/acc32h/weasd` | `sfc/acc32h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc33h/apcp` | `sfc/acc33h/apcp` | kept | totalprecipitation |  |
| `sfc/acc33h/asnow` | `sfc/acc33h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc33h/frozr` | `sfc/acc33h/frozr` | kept | frozenrain |  |
| `sfc/acc33h/frzr` | `sfc/acc33h/frzr` | kept | freezingrain |  |
| `sfc/acc33h/weasd` | `sfc/acc33h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc34h/apcp` | `sfc/acc34h/apcp` | kept | totalprecipitation |  |
| `sfc/acc34h/asnow` | `sfc/acc34h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc34h/frozr` | `sfc/acc34h/frozr` | kept | frozenrain |  |
| `sfc/acc34h/frzr` | `sfc/acc34h/frzr` | kept | freezingrain |  |
| `sfc/acc34h/weasd` | `sfc/acc34h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc35h/apcp` | `sfc/acc35h/apcp` | kept | totalprecipitation |  |
| `sfc/acc35h/asnow` | `sfc/acc35h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc35h/frozr` | `sfc/acc35h/frozr` | kept | frozenrain |  |
| `sfc/acc35h/frzr` | `sfc/acc35h/frzr` | kept | freezingrain |  |
| `sfc/acc35h/weasd` | `sfc/acc35h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc36h/apcp` | `sfc/acc36h/apcp` | kept | totalprecipitation |  |
| `sfc/acc36h/asnow` | `sfc/acc36h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc36h/frozr` | `sfc/acc36h/frozr` | kept | frozenrain |  |
| `sfc/acc36h/frzr` | `sfc/acc36h/frzr` | kept | freezingrain |  |
| `sfc/acc36h/weasd` | `sfc/acc36h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc3h/apcp` | `sfc/acc3h/apcp` | kept | totalprecipitation |  |
| `sfc/acc3h/asnow` | `sfc/acc3h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc3h/frozr` | `sfc/acc3h/frozr` | kept | frozenrain |  |
| `sfc/acc3h/frzr` | `sfc/acc3h/frzr` | kept | freezingrain |  |
| `sfc/acc3h/weasd` | `sfc/acc3h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc4h/apcp` | `sfc/acc4h/apcp` | kept | totalprecipitation |  |
| `sfc/acc4h/asnow` | `sfc/acc4h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc4h/frozr` | `sfc/acc4h/frozr` | kept | frozenrain |  |
| `sfc/acc4h/frzr` | `sfc/acc4h/frzr` | kept | freezingrain |  |
| `sfc/acc4h/weasd` | `sfc/acc4h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc5h/apcp` | `sfc/acc5h/apcp` | kept | totalprecipitation |  |
| `sfc/acc5h/asnow` | `sfc/acc5h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc5h/frozr` | `sfc/acc5h/frozr` | kept | frozenrain |  |
| `sfc/acc5h/frzr` | `sfc/acc5h/frzr` | kept | freezingrain |  |
| `sfc/acc5h/weasd` | `sfc/acc5h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc6h/apcp` | `sfc/acc6h/apcp` | kept | totalprecipitation |  |
| `sfc/acc6h/asnow` | `sfc/acc6h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc6h/frozr` | `sfc/acc6h/frozr` | kept | frozenrain |  |
| `sfc/acc6h/frzr` | `sfc/acc6h/frzr` | kept | freezingrain |  |
| `sfc/acc6h/weasd` | `sfc/acc6h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc7h/apcp` | `sfc/acc7h/apcp` | kept | totalprecipitation |  |
| `sfc/acc7h/asnow` | `sfc/acc7h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc7h/frozr` | `sfc/acc7h/frozr` | kept | frozenrain |  |
| `sfc/acc7h/frzr` | `sfc/acc7h/frzr` | kept | freezingrain |  |
| `sfc/acc7h/weasd` | `sfc/acc7h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc8h/apcp` | `sfc/acc8h/apcp` | kept | totalprecipitation |  |
| `sfc/acc8h/asnow` | `sfc/acc8h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc8h/frozr` | `sfc/acc8h/frozr` | kept | frozenrain |  |
| `sfc/acc8h/frzr` | `sfc/acc8h/frzr` | kept | freezingrain |  |
| `sfc/acc8h/weasd` | `sfc/acc8h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc9h/apcp` | `sfc/acc9h/apcp` | kept | totalprecipitation |  |
| `sfc/acc9h/asnow` | `sfc/acc9h/total_snowfall` | eccodes | totalsnowfall |  |
| `sfc/acc9h/frozr` | `sfc/acc9h/frozr` | kept | frozenrain |  |
| `sfc/acc9h/frzr` | `sfc/acc9h/frzr` | kept | freezingrain |  |
| `sfc/acc9h/weasd` | `sfc/acc9h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/instant/cape` | `sfc/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `sfc/instant/cfrzr` | `sfc/instant/categorical_freezing_rain` | eccodes | categoricalfreezingrain |  |
| `sfc/instant/cicep` | `sfc/instant/categorical_ice_pellets` | eccodes | categoricalicepellets |  |
| `sfc/instant/cin` | `sfc/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `sfc/instant/cnwat` | `sfc/instant/plant_canopy_surface_water` | eccodes | plantcanopysurfacewater |  |
| `sfc/instant/cpofp` | `sfc/instant/percent_frozen_precipitation` | eccodes | percentfrozenprecipitation |  |
| `sfc/instant/crain` | `sfc/instant/categorical_rain` | eccodes | categoricalrain |  |
| `sfc/instant/csnow` | `sfc/instant/categorical_snow` | eccodes | categoricalsnow |  |
| `sfc/instant/dlwrf` | `sfc/instant/downward_long_wave_radiation_flux` | eccodes | downwardlongwaveradiationflux |  |
| `sfc/instant/dswrf` | `sfc/instant/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/instant/fricv` | `sfc/instant/frictional_velocity` | eccodes | frictionalvelocity |  |
| `sfc/instant/gflux` | `sfc/instant/ground_heat_flux` | eccodes | groundheatflux |  |
| `sfc/instant/gust` | `sfc/instant/wind_speed` | eccodes | windgust |  |
| `sfc/instant/hgt` | `sfc/instant/hgt` | kept | geopotentialheight |  |
| `sfc/instant/hpbl` | `sfc/instant/planetary_boundary_layer_height` | eccodes | planetaryboundarylayerheight |  |
| `sfc/instant/icec` | `sfc/instant/icec` | kept | icecover |  |
| `sfc/instant/land` | `sfc/instant/land` | kept | landcover |  |
| `sfc/instant/lhtfl` | `sfc/instant/lhtfl` | kept | latentheatnetflux |  |
| `sfc/instant/prate` | `sfc/instant/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/instant/pres` | `sfc/instant/air_pressure` | abbreviation | pressure |  |
| `sfc/instant/sfcr` | `sfc/instant/sfcr` | kept | surfaceroughness |  |
| `sfc/instant/shtfl` | `sfc/instant/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/instant/snod` | `sfc/instant/snod` | kept | snowdepth |  |
| `sfc/instant/snowc` | `sfc/instant/snow_cover` | eccodes | snowcover |  |
| `sfc/instant/tmp` | `sfc/instant/tmp` | kept | temperature |  |
| `sfc/instant/ulwrf` | `sfc/instant/upward_long_wave_radiation_flux` | eccodes | upwardlongwaveradiationflux |  |
| `sfc/instant/uswrf` | `sfc/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/vbdsf` | `sfc/instant/vbdsf` | kept | visiblebeamdownwardsolarflux |  |
| `sfc/instant/vddsf` | `sfc/instant/vddsf` | kept | visiblediffusedownwardsolarflux |  |
| `sfc/instant/vgtyp` | `sfc/instant/vgtyp` | kept | vegetationtype |  |
| `sfc/instant/vis` | `sfc/instant/visible` | abbreviation | visibility |  |
| `sfc/instant/weasd` | `sfc/instant/weasd` | kept | waterequavalentsnowdepth |  |
| `sigma/avg/dzdt` | `sigma/avg/dzdt` | kept | verticalvelocitygeometric |  |
| `sigma/avg1h/dzdt` | `sigma/avg1h/dzdt` | kept | verticalvelocitygeometric |  |
| `zero_deg_isotherm/instant/hgt` | `zero_deg_isotherm/instant/hgt` | kept | geopotentialheight |  |
| `zero_deg_isotherm/instant/pres` | `zero_deg_isotherm/instant/air_pressure` | abbreviation | pressure |  |
| `zero_deg_isotherm/instant/rh` | `zero_deg_isotherm/instant/rh` | kept | relativehumidity |  |

## bkr/dmi/virtualized/nam_hawaii (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `adiabatic_condensation_lifted/instant/hgt` | `adiabatic_condensation_lifted/instant/hgt` | kept | geopotentialheight |  |
| `adiabatic_condensation_lifted/instant/pres` | `adiabatic_condensation_lifted/instant/air_pressure` | abbreviation | pressure |  |
| `asl/instant/asl` | `asl/instant/height_above_sea_level` | coordinate |  |  |
| `asl/instant/tmp` | `asl/instant/tmp` | kept | temperature |  |
| `asl/instant/ugrd` | `asl/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `clb/instant/hgt` | `clb/instant/hgt` | kept | geopotentialheight |  |
| `clb/instant/pres` | `clb/instant/air_pressure` | abbreviation | pressure |  |
| `cld_ceiling/instant/hgt` | `cld_ceiling/instant/hgt` | kept | geopotentialheight |  |
| `clt/instant/hgt` | `clt/instant/hgt` | kept | geopotentialheight |  |
| `clt/instant/pres` | `clt/instant/air_pressure` | abbreviation | pressure |  |
| `clt/instant/tmp` | `clt/instant/tmp` | kept | temperature |  |
| `clt/instant/vis` | `clt/instant/visible` | abbreviation | visibility |  |
| `depth_bls/instant/depth_bls` | `depth_bls/instant/depth_below_land_surface` | coordinate |  |  |
| `depth_bls/instant/mstav` | `depth_bls/instant/moisture_availability` | eccodes | moistureavailabilityncep |  |
| `depth_bls/instant/soill` | `depth_bls/instant/soill` | kept | liquidvolumetricsoilmoisturenonfrozen |  |
| `depth_bls/instant/soilw` | `depth_bls/instant/volumetric_soil_moisture_content` | eccodes | volumetricsoilmoisturecontentncep |  |
| `depth_bls/instant/tsoil` | `depth_bls/instant/tsoil` | kept | soiltemperature |  |
| `entire_atm/instant/pwat` | `entire_atm/instant/precipitable_water` | eccodes | precipitablewater |  |
| `entire_atm/instant/refc` | `entire_atm/instant/composite_reflectivity` | abbreviation | compositereflectivity |  |
| `entire_atm/instant/swhr` | `entire_atm/instant/short_wave_heating_rate` | eccodes | solarradiativeheatingrate |  |
| `entire_atm/instant/tcdc` | `entire_atm/instant/tcdc` | kept | totalcloudcover |  |
| `entire_atm/instant/veril` | `entire_atm/instant/vertically_integrated_liquid` | eccodes | verticallyintegratedliquid |  |
| `hag/instant/dpt` | `hag/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `hag/instant/hag` | `hag/instant/height_above_ground` | coordinate |  |  |
| `hag/instant/hlcy` | `hag/instant/storm_relative_helicity` | eccodes | stormrelativehelicity |  |
| `hag/instant/pot` | `hag/instant/pot` | kept | potentialtemperature |  |
| `hag/instant/pres` | `hag/instant/air_pressure` | abbreviation | pressure |  |
| `hag/instant/refd` | `hag/instant/refd` | kept | reflectivity |  |
| `hag/instant/rh` | `hag/instant/rh` | kept | relativehumidity |  |
| `hag/instant/spfh` | `hag/instant/spfh` | kept | specifichumidity |  |
| `hag/instant/tmp` | `hag/instant/tmp` | kept | temperature |  |
| `hag/instant/ugrd` | `hag/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hag/instant/ustm` | `hag/instant/u_component_storm_motion` | eccodes | ucomponentstormmotionncep |  |
| `hag/instant/vucsh` | `hag/instant/vertical_u_component_shear` | eccodes | verticalucomponentshear |  |
| `hag/instant/vvcsh` | `hag/instant/vertical_v_component_shear` | eccodes | verticalvcomponentshear |  |
| `hag/max1h/maxref` | `hag/max1h/maxref` | kept | hourlymaxsimulatedreflectivity |  |
| `hag/max1h/maxrh` | `hag/max1h/maximum_relative_humidity` | eccodes | maximumrelativehumidity |  |
| `hag/max1h/maxuw` | `hag/max1h/maxuw` | kept | ucomponenthourlymaximumwindspeed |  |
| `hag/max1h/maxvw` | `hag/max1h/maxvw` | kept | vcomponenthourlymaximumwindspeed |  |
| `hag/max1h/mxuphl` | `hag/max1h/mxuphl` | kept | hourlymaxupdrafthelicity |  |
| `hag/max1h/tmax` | `hag/max1h/period_corresponding_to_maximum_individual_wave_height` | eccodes | maximumtemperature |  |
| `hag/min1h/sf` | `hag/min1h/snowfall_water_equivalent` | eccodes | snowfallecmwf |  |
| `hag/min1h/tmin` | `hag/min1h/minimum_temperature` | eccodes | minimumtemperature |  |
| `hcl/instant/hcdc` | `hcl/instant/hcdc` | kept | highcloudcover |  |
| `htfl/instant/hgt` | `htfl/instant/hgt` | kept | geopotentialheight |  |
| `hybid/instant/clwmr` | `hybid/instant/cloud_mixing_ratio` | eccodes | cloudmixingratio |  |
| `hybid/instant/hgt` | `hybid/instant/hgt` | kept | geopotentialheight |  |
| `hybid/instant/hybid` | `hybid/instant/model_level` | coordinate |  |  |
| `hybid/instant/pres` | `hybid/instant/air_pressure` | abbreviation | pressure |  |
| `hybid/instant/refd` | `hybid/instant/refd` | kept | reflectivity |  |
| `hybid/instant/rh` | `hybid/instant/rh` | kept | relativehumidity |  |
| `hybid/instant/rwmr` | `hybid/instant/rain_mixing_ratio` | eccodes | rainmixingratio |  |
| `hybid/instant/snmr` | `hybid/instant/snow_mixing_ratio` | eccodes | snowmixingratio |  |
| `hybid/instant/spfh` | `hybid/instant/spfh` | kept | specifichumidity |  |
| `hybid/instant/tmp` | `hybid/instant/tmp` | kept | temperature |  |
| `hybid/instant/ugrd` | `hybid/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `hybid/instant/vvel` | `hybid/instant/vvel` | kept | verticalvelocitypressure |  |
| `instant/hgt` | `instant/hgt` | kept | geopotentialheight |  |
| `instant/pres` | `instant/air_pressure` | abbreviation | pressure |  |
| `isobar/instant/absv` | `isobar/instant/absolute_vorticity` | eccodes | absolutevorticity |  |
| `isobar/instant/clwmr` | `isobar/instant/cloud_mixing_ratio` | eccodes | cloudmixingratio |  |
| `isobar/instant/dpt` | `isobar/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `isobar/instant/dzdt` | `isobar/instant/dzdt` | kept | verticalvelocitygeometric |  |
| `isobar/instant/hgt` | `isobar/instant/hgt` | kept | geopotentialheight |  |
| `isobar/instant/isobar` | `isobar/instant/pressure_level` | coordinate |  |  |
| `isobar/instant/lftx` | `isobar/instant/surface_lifted_index` | eccodes | surfaceliftedindexncep |  |
| `isobar/instant/relv` | `isobar/instant/relative_vorticity` | eccodes | shortwaveradiation |  |
| `isobar/instant/rh` | `isobar/instant/rh` | kept | relativehumidity |  |
| `isobar/instant/rwmr` | `isobar/instant/rain_mixing_ratio` | eccodes | rainmixingratio |  |
| `isobar/instant/snmr` | `isobar/instant/snow_mixing_ratio` | eccodes | snowmixingratio |  |
| `isobar/instant/spfh` | `isobar/instant/spfh` | kept | specifichumidity |  |
| `isobar/instant/tmp` | `isobar/instant/tmp` | kept | temperature |  |
| `isobar/instant/ugrd` | `isobar/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `isobar/instant/vvel` | `isobar/instant/vvel` | kept | verticalvelocitypressure |  |
| `isobar/max1h/maxdvv` | `isobar/max1h/maxdvv` | kept | hourlymaxdownwardverticalvelocity |  |
| `isobar/max1h/maxuvv` | `isobar/max1h/maxuvv` | kept | hourlymaxupwardverticalvelocity |  |
| `lcl/instant/lcdc` | `lcl/instant/lcdc` | kept | lowcloudcover |  |
| `mcl/instant/mcdc` | `mcl/instant/mcdc` | kept | mediumcloudcover |  |
| `msl/instant/mslet` | `msl/instant/mslet` | kept | mslp |  |
| `msl/instant/prmsl` | `msl/instant/mean_sea_level_pressure` | eccodes | pressurereducedmsl |  |
| `mwl/instant/hgt` | `mwl/instant/hgt` | kept | geopotentialheight |  |
| `mwl/instant/pres` | `mwl/instant/air_pressure` | abbreviation | pressure |  |
| `mwl/instant/ugrd` | `mwl/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `nominal_top/avg/uswrf` | `nominal_top/avg/uswrf` | kept | upwardshortwaveradiationflux |  |
| `nominal_top/avg1h/uswrf` | `nominal_top/avg1h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `nominal_top/avg2h/uswrf` | `nominal_top/avg2h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `nominal_top/avg3h/uswrf` | `nominal_top/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `nominal_top/instant/brtmp` | `nominal_top/instant/brtmp` | kept | brightnesstemperature |  |
| `pbl/instant/hgt` | `pbl/instant/hgt` | kept | geopotentialheight |  |
| `pbl/instant/ugrd` | `pbl/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `pbl/instant/vrate` | `pbl/instant/vrate` | kept | ventilationrate |  |
| `pres_diff/instant/4lftx` | `pres_diff/instant/best_lifted_index` | eccodes | best4layerliftedindexncep |  |
| `pres_diff/instant/cape` | `pres_diff/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `pres_diff/instant/cin` | `pres_diff/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `pres_diff/instant/dpt` | `pres_diff/instant/dewpoint_temperature` | eccodes | dewpointtemperature |  |
| `pres_diff/instant/pli` | `pres_diff/instant/parcel_lifted_index` | eccodes | parcelliftedindex |  |
| `pres_diff/instant/plpl` | `pres_diff/instant/plpl` | kept | pressureofliftedparcel |  |
| `pres_diff/instant/pot` | `pres_diff/instant/pot` | kept | potentialtemperature |  |
| `pres_diff/instant/pres` | `pres_diff/instant/air_pressure` | abbreviation | pressure |  |
| `pres_diff/instant/pwat` | `pres_diff/instant/precipitable_water` | eccodes | precipitablewater |  |
| `pres_diff/instant/rh` | `pres_diff/instant/rh` | kept | relativehumidity |  |
| `pres_diff/instant/spfh` | `pres_diff/instant/spfh` | kept | specifichumidity |  |
| `pres_diff/instant/tmp` | `pres_diff/instant/tmp` | kept | temperature |  |
| `pres_diff/instant/ugrd` | `pres_diff/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `pres_diff/instant/vvel` | `pres_diff/instant/vvel` | kept | verticalvelocitypressure |  |
| `sfc/acc/apcp` | `sfc/acc/apcp` | kept | totalprecipitation |  |
| `sfc/acc/ncpcp` | `sfc/acc/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc/weasd` | `sfc/acc/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc1h/apcp` | `sfc/acc1h/apcp` | kept | totalprecipitation |  |
| `sfc/acc1h/ncpcp` | `sfc/acc1h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc1h/weasd` | `sfc/acc1h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc2h/apcp` | `sfc/acc2h/apcp` | kept | totalprecipitation |  |
| `sfc/acc2h/ncpcp` | `sfc/acc2h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc2h/weasd` | `sfc/acc2h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/acc3h/apcp` | `sfc/acc3h/apcp` | kept | totalprecipitation |  |
| `sfc/acc3h/ncpcp` | `sfc/acc3h/large_scale_precipitation` | eccodes | largescaleprecipitation |  |
| `sfc/acc3h/weasd` | `sfc/acc3h/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/avg/dswrf` | `sfc/avg/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg/gflux` | `sfc/avg/ground_heat_flux` | eccodes | groundheatfluxncep |  |
| `sfc/avg/lhtfl` | `sfc/avg/lhtfl` | kept | latentheatnetflux |  |
| `sfc/avg/shtfl` | `sfc/avg/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/avg/uswrf` | `sfc/avg/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/avg1h/dswrf` | `sfc/avg1h/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg1h/gflux` | `sfc/avg1h/ground_heat_flux` | eccodes | groundheatfluxncep |  |
| `sfc/avg1h/lhtfl` | `sfc/avg1h/lhtfl` | kept | latentheatnetflux |  |
| `sfc/avg1h/shtfl` | `sfc/avg1h/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/avg1h/uswrf` | `sfc/avg1h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/avg2h/dswrf` | `sfc/avg2h/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg2h/gflux` | `sfc/avg2h/ground_heat_flux` | eccodes | groundheatfluxncep |  |
| `sfc/avg2h/lhtfl` | `sfc/avg2h/lhtfl` | kept | latentheatnetflux |  |
| `sfc/avg2h/shtfl` | `sfc/avg2h/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/avg2h/uswrf` | `sfc/avg2h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/avg3h/dswrf` | `sfc/avg3h/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/avg3h/gflux` | `sfc/avg3h/ground_heat_flux` | eccodes | groundheatfluxncep |  |
| `sfc/avg3h/lhtfl` | `sfc/avg3h/lhtfl` | kept | latentheatnetflux |  |
| `sfc/avg3h/shtfl` | `sfc/avg3h/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/avg3h/uswrf` | `sfc/avg3h/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/albdo` | `sfc/instant/albdo` | kept | albedo |  |
| `sfc/instant/asn` | `sfc/instant/snow_albedo` | eccodes | snowalbedo |  |
| `sfc/instant/cape` | `sfc/instant/convective_available_potential_energy` | eccodes | convectiveavailablepotentialenergy |  |
| `sfc/instant/cfrzr` | `sfc/instant/categorical_freezing_rain` | eccodes | categoricalfreezingrainncep |  |
| `sfc/instant/cicep` | `sfc/instant/categorical_ice_pellets` | eccodes | categoricalicepelletsncep |  |
| `sfc/instant/cin` | `sfc/instant/convective_inhibition` | eccodes | convectiveinhibition |  |
| `sfc/instant/cnwat` | `sfc/instant/plant_canopy_surface_water` | eccodes | plantcanopysurfacewaterncep |  |
| `sfc/instant/crain` | `sfc/instant/categorical_rain` | eccodes | categoricalrainncep |  |
| `sfc/instant/csdsf` | `sfc/instant/csdsf` | kept | clearskydownwardsolarflux |  |
| `sfc/instant/csnow` | `sfc/instant/categorical_snow` | eccodes | categoricalsnowncep |  |
| `sfc/instant/dswrf` | `sfc/instant/downward_short_wave_radiation_flux` | eccodes | downwardshortwaveradiationflux |  |
| `sfc/instant/fricv` | `sfc/instant/frictional_velocity` | eccodes | frictionalvelocityncep |  |
| `sfc/instant/gflux` | `sfc/instant/ground_heat_flux` | eccodes | groundheatfluxncep |  |
| `sfc/instant/gust` | `sfc/instant/wind_speed` | eccodes | windgust |  |
| `sfc/instant/hgt` | `sfc/instant/hgt` | kept | geopotentialheight |  |
| `sfc/instant/hindex` | `sfc/instant/haines_index` | eccodes | hainesindex |  |
| `sfc/instant/hpbl` | `sfc/instant/planetary_boundary_layer_height` | eccodes | planetaryboundarylayerheightncep |  |
| `sfc/instant/icec` | `sfc/instant/icec` | kept | icecover |  |
| `sfc/instant/land` | `sfc/instant/land` | kept | landcover |  |
| `sfc/instant/lhtfl` | `sfc/instant/lhtfl` | kept | latentheatnetflux |  |
| `sfc/instant/ltng` | `sfc/instant/ltng` | kept | lightning |  |
| `sfc/instant/prate` | `sfc/instant/precipitation_rate` | eccodes | precipitationrate |  |
| `sfc/instant/pres` | `sfc/instant/air_pressure` | abbreviation | pressure |  |
| `sfc/instant/sfcr` | `sfc/instant/sfcr` | kept | surfaceroughness |  |
| `sfc/instant/shtfl` | `sfc/instant/shtfl` | kept | sensibleheatnetflux |  |
| `sfc/instant/snod` | `sfc/instant/snod` | kept | snowdepth |  |
| `sfc/instant/snowc` | `sfc/instant/snow_cover` | eccodes | snowcoverncep |  |
| `sfc/instant/sotyp` | `sfc/instant/sotyp` | kept | soiltype |  |
| `sfc/instant/spfh` | `sfc/instant/spfh` | kept | specifichumidity |  |
| `sfc/instant/tmp` | `sfc/instant/tmp` | kept | temperature |  |
| `sfc/instant/uflx` | `sfc/instant/uflx` | kept | momentumfluxu |  |
| `sfc/instant/uswrf` | `sfc/instant/uswrf` | kept | upwardshortwaveradiationflux |  |
| `sfc/instant/veg` | `sfc/instant/vegetation` | eccodes | vegetation |  |
| `sfc/instant/vflx` | `sfc/instant/vflx` | kept | momentumfluxv |  |
| `sfc/instant/vgtyp` | `sfc/instant/vgtyp` | kept | vegetationtype |  |
| `sfc/instant/vis` | `sfc/instant/visible` | abbreviation | visibility |  |
| `sfc/instant/weasd` | `sfc/instant/weasd` | kept | waterequavalentsnowdepth |  |
| `sfc/instant/wilt` | `sfc/instant/wilting_point` | eccodes | wiltingpoint |  |
| `sfc/instant/wtmp` | `sfc/instant/sea_surface_temperature` | abbreviation | watertemperature |  |
| `sfc/max1h/prate` | `sfc/max1h/precipitation_rate` | eccodes | precipitationrate |  |
| `tro/instant/pres` | `tro/instant/air_pressure` | abbreviation | pressure |  |
| `tro/instant/tmp` | `tro/instant/tmp` | kept | temperature |  |
| `tro/instant/ugrd` | `tro/instant/u_component_of_wind_gradient` | eccodes | ucomponentwindspeed |  |
| `tro/instant/vwsh` | `tro/instant/vertical_speed_shear` | eccodes | verticalspeedshearncep |  |
| `zero_deg_isotherm/instant/hgt` | `zero_deg_isotherm/instant/hgt` | kept | geopotentialheight |  |
| `zero_deg_isotherm/instant/rh` | `zero_deg_isotherm/instant/rh` | kept | relativehumidity |  |

## bkr/dmi/wales_woof (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `ACRUNOFF` | `accumulated_runoff` | long_name |  | kg m-2 |
| `CLDFRA_BL` | `cloud_fraction_pbl` | long_name |  |  |
| `COSALPHA` | `local_cosine_of_map_rotation` | long_name |  |  |
| `COV` | `total_water_liquid_water_pottemp_covariance` | long_name |  | K kg/kg |
| `DEW` | `dew_mixing_ratio_at_the_surface` | long_name |  | kg kg-1 |
| `E` | `coriolis_cosine_latitude_term` | long_name |  | s-1 |
| `EL_PBL` | `length_scale_from_pbl` | long_name |  | m |
| `EXCH_H` | `scalar_exchange_coefficients` | long_name |  | m2 s-1 |
| `EXCH_M` | `exchange_coefficients` | long_name |  | m2 s-1 |
| `F` | `coriolis_sine_latitude_term` | long_name |  | s-1 |
| `GLW` | `downward_long_wave_flux_at_ground_surface` | long_name |  | W m-2 |
| `GRAUPELNC` | `accumulated_total_grid_scale_graupel` | long_name |  | mm |
| `GRDFLX` | `ground_heat_flux` | long_name |  | W m-2 |
| `HAILNC` | `accumulated_total_grid_scale_hail` | long_name |  | mm |
| `HFX` | `upward_heat_flux_at_the_surface` | long_name |  | W m-2 |
| `HGT` | `geopotential_at_surface` | long_name+geopotential+units |  | m -> m**2 s**-2 |
| `ISLTYP` | `dominant_soil_category` | long_name |  |  |
| `ITIMESTEP` | `itimestep` | tidied |  |  |
| `IVGTYP` | `dominant_vegetation_category` | long_name |  |  |
| `KEEPFR3DFLAG` | `flag_1_frozen_soil_yes_0_no` | long_name |  |  |
| `KPBL` | `level_of_pbl_top` | long_name |  |  |
| `KTOP_PLUME` | `k_level_of_highest_pentrating_plume` | long_name |  |  |
| `LANDMASK` | `land_mask` | long_name |  |  |
| `LH` | `latent_heat_flux_at_the_surface` | long_name |  | W m-2 |
| `LU_INDEX` | `land_use_category` | long_name |  |  |
| `MAPFAC_M` | `map_scale_factor_on_mass_grid` | long_name |  |  |
| `MAPFAC_U` | `map_scale_factor_on_u_grid` | long_name |  |  |
| `MAPFAC_V` | `map_scale_factor_on_v_grid` | long_name |  |  |
| `MAXMF` | `maximum_mass_flux` | long_name |  | m/s * area |
| `MAXWIDTH` | `maximum_plume_width` | long_name |  | m |
| `MU` | `perturbation_dry_air_mass_in_column` | long_name |  | Pa |
| `MUB` | `base_state_dry_air_mass_in_column` | long_name |  | Pa |
| `OLR` | `toa_outgoing_long_wave` | long_name |  | W m-2 |
| `P` | `perturbation_pressure` | long_name |  | Pa |
| `PB` | `base_state_pressure` | long_name |  | Pa |
| `PBLH` | `pbl_height` | long_name |  | m |
| `PH` | `perturbation_geopotential` | long_name |  | m2 s-2 |
| `PHB` | `base_state_geopotential` | long_name |  | m2 s-2 |
| `PRECIPFR` | `step_frozen_precip_mm` | long_name |  |  |
| `PSFC` | `sfc_pressure` | long_name |  | Pa |
| `PSIH` | `similarity_stability_function_for_heat` | long_name |  |  |
| `PSIM` | `similarity_stability_function_for_momentum` | long_name |  |  |
| `P_TOP` | `pressure_top_of_the_model` | long_name |  | Pa |
| `Q2` | `qv_at_2_m` | long_name |  | kg kg-1 |
| `QCG` | `cloud_water_mixing_ratio_at_the_ground_surface` | long_name |  | kg kg-1 |
| `QCLOUD` | `cloud_water_mixing_ratio` | long_name |  | kg kg-1 |
| `QC_BL` | `cloud_water_mixing_ratio_in_pbl_schemes` | long_name |  | kg kg-1 |
| `QFX` | `upward_moisture_flux_at_the_surface` | long_name |  | kg m-2 s-1 |
| `QGRAUP` | `graupel_mixing_ratio` | long_name |  | kg kg-1 |
| `QICE` | `ice_mixing_ratio` | long_name |  | kg kg-1 |
| `QI_BL` | `cloud_ice_mixing_ratio_in_pbl_schemes` | long_name |  | kg kg-1 |
| `QKE` | `twice_tke_from_mynn` | long_name |  | m2 s-2 |
| `QNICE` | `ice_number_concentration` | long_name |  |   kg-1 |
| `QNRAIN` | `rain_number_concentration` | long_name |  |   kg(-1) |
| `QRAIN` | `rain_water_mixing_ratio` | long_name |  | kg kg-1 |
| `QSG` | `surface_saturation_water_vapor_mixing_ratio` | long_name |  | kg kg-1 |
| `QSNOW` | `snow_mixing_ratio` | long_name |  | kg kg-1 |
| `QSQ` | `total_water_variance` | long_name |  | (kg/kg)**2 |
| `QVAPOR` | `water_vapor_mixing_ratio` | long_name |  | kg kg-1 |
| `QVG` | `water_vapor_mixing_ratio_at_the_surface` | long_name |  | kg kg-1 |
| `RAINC` | `accumulated_total_cumulus_precipitation` | long_name |  | mm |
| `RAINNC` | `accumulated_total_grid_scale_precipitation` | long_name |  | mm |
| `RAINSH` | `accumulated_shallow_cumulus_precipitation` | long_name |  | mm |
| `REFL_10CM` | `radar_reflectivity` | long_name |  | dBZ |
| `RHOSNF` | `density_of_frozen_precip` | long_name |  | kg/m^3 |
| `RMOL` | `1_monin_ob_length` | long_name |  |  |
| `RTHRATLW` | `RTHRATLW` | kept |  |  |
| `RTHRATSW` | `RTHRATSW` | kept |  |  |
| `RUC_INFILTR` | `woof_diagnostic_lsmruc_infiltration_flux` | long_name |  | m s-1 |
| `RUC_RUNOFF1` | `woof_diagnostic_lsmruc_surface_runoff_rate` | long_name |  | m s-1 |
| `RUC_RUNOFF2` | `woof_diagnostic_lsmruc_underground_runoff_rate` | long_name |  | m s-1 |
| `RUC_SMELT` | `woof_diagnostic_lsmruc_snow_melt_water_flux` | long_name |  | m s-1 |
| `SEAICE` | `sea_ice_flag` | long_name |  |  |
| `SFCEVP` | `accumulated_surface_evaporation` | long_name |  | kg m-2 |
| `SFCEXC` | `surface_exchange_coefficient` | long_name |  | m s-1 |
| `SH2O` | `soil_liquid_water` | long_name |  | m3 m-3 |
| `SH3D` | `stability_function_for_heat` | long_name |  |  |
| `SINALPHA` | `local_sine_of_map_rotation` | long_name |  |  |
| `SM3D` | `stability_function_for_momentum` | long_name |  |  |
| `SMFR3D` | `soil_ice` | long_name |  |  |
| `SMOIS` | `soil_moisture` | long_name |  | m3 m-3 |
| `SNOW` | `snow_water_equivalent` | long_name |  | kg m-2 |
| `SNOWC` | `flag_indicating_snow_coverage` | long_name |  |  |
| `SNOWFALLAC` | `run_total_accumulated_snowfall_mm` | long_name |  |  |
| `SNOWH` | `physical_snow_depth` | long_name |  | m |
| `SNOWNC` | `accumulated_total_grid_scale_snow_and_ice` | long_name |  | mm |
| `SOILT1` | `temperature_inside_snow` | long_name |  | K |
| `SWDOWN` | `downward_short_wave_flux_at_ground_surface` | long_name |  | W m-2 |
| `T` | `temperature` | override |  | K |
| `T2` | `temp_at_2_m` | long_name |  | K |
| `TH2` | `pot_temp_at_2_m` | long_name |  | K |
| `TMN` | `soil_temperature_at_lower_boundary` | long_name |  | K |
| `TSK` | `surface_skin_temperature` | long_name |  | K |
| `TSLB` | `soil_temperature` | long_name |  | K |
| `TSNAV` | `average_snow_temperature` | long_name+units |  | C -> K |
| `TSQ` | `liquid_water_pottemp_variance` | long_name |  | K2 |
| `Times` | `Times` | kept |  |  |
| `U` | `u_component_of_wind` | override |  | m s-1 |
| `U10` | `u_at_10_m` | long_name |  | m s-1 |
| `UP_HELI_MAX` | `max_updraft_helicity` | long_name |  | m2 s-2 |
| `UST` | `u_in_similarity_theory` | long_name |  | m s-1 |
| `V` | `v_component_of_wind` | override |  | m s-1 |
| `V10` | `v_at_10_m` | long_name |  | m s-1 |
| `VEGFRA` | `vegetation_fraction` | long_name |  |  |
| `W` | `z_wind_component` | long_name |  | m s-1 |
| `XLAT` | `xlat` | clash |  | degree_north |
| `XLAT_U` | `xlat_u` | clash |  | degree_north |
| `XLAT_V` | `xlat_v` | clash |  | degree_north |
| `XLONG` | `xlong` | clash |  | degree_east |
| `XLONG_U` | `xlong_u` | clash |  | degree_east |
| `XLONG_V` | `xlong_v` | clash |  | degree_east |
| `XTIME` | `minutes_since_2026_10_05_06_00_00` | long_name |  | minutes since 2026-10-05T06:00:00 |
| `ZNU` | `eta_values_on_half_levels` | long_name |  |  |
| `ZNW` | `eta_values_on_full_levels` | long_name |  |  |
| `ZTOP_PLUME` | `height_of_tallest_plume` | long_name |  | m |

## bkr/err/eeps_cleaned (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `DQF` | kept | L3 Rainfall Rate - Quantitative Precipitation Estimates Data | 1 |
| `RRQPE` | `rainfall_rate_quantitative_precipitation_estimates` | long_name | Rainfall Rate Quantitative Precipitation Estimates | mm/h |

## bkr/err/ghe (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `rainfall_estimate` | `ghe_global_instantaneous_rain_total_for_201906241945` | long_name | GHE Global Instantaneous rain total for 201906241945 | mm |

## bkr/geo/gk2a_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `VI004` | `channel_01_reflectance_0_47um` | channel |  |  |
| `VI005` | `channel_02_reflectance_0_51um` | channel |  |  |
| `VI008` | `channel_04_reflectance_0_86um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/gk2a_1000m_test (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `VI004` | `channel_01_reflectance_0_47um` | channel |  |  |
| `VI005` | `channel_02_reflectance_0_51um` | channel |  |  |
| `VI008` | `channel_04_reflectance_0_86um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/gk2a_2000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `IR087` | `channel_11_brightness_temperature_8_7um` | channel |  |  |
| `IR096` | `channel_12_brightness_temperature_9_6um` | channel |  |  |
| `IR105` | `channel_13_brightness_temperature_10_5um` | channel |  |  |
| `IR112` | `channel_14_brightness_temperature_11_2um` | channel |  |  |
| `IR123` | `channel_15_brightness_temperature_12_3um` | channel |  |  |
| `IR133` | `channel_16_brightness_temperature_13_3um` | channel |  |  |
| `NR013` | `channel_05_reflectance_1_37um` | channel |  |  |
| `NR016` | `channel_06_reflectance_1_6um` | channel |  |  |
| `SW038` | `channel_07_brightness_temperature_3_8um` | channel |  |  |
| `WV063` | `channel_08_brightness_temperature_6_3um` | channel |  |  |
| `WV069` | `channel_09_brightness_temperature_6_9um` | channel |  |  |
| `WV073` | `channel_10_brightness_temperature_7_3um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/gk2a_2000m_test (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `IR087` | `channel_11_brightness_temperature_8_7um` | channel |  |  |
| `IR096` | `channel_12_brightness_temperature_9_6um` | channel |  |  |
| `IR105` | `channel_13_brightness_temperature_10_5um` | channel |  |  |
| `IR112` | `channel_14_brightness_temperature_11_2um` | channel |  |  |
| `IR123` | `channel_15_brightness_temperature_12_3um` | channel |  |  |
| `IR133` | `channel_16_brightness_temperature_13_3um` | channel |  |  |
| `NR013` | `channel_05_reflectance_1_37um` | channel |  |  |
| `NR016` | `channel_06_reflectance_1_6um` | channel |  |  |
| `SW038` | `channel_07_brightness_temperature_3_8um` | channel |  |  |
| `WV063` | `channel_08_brightness_temperature_6_3um` | channel |  |  |
| `WV069` | `channel_09_brightness_temperature_6_9um` | channel |  |  |
| `WV073` | `channel_10_brightness_temperature_7_3um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/gk2a_500m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `VI006` | `channel_03_reflectance_0_64um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/gk2a_500m_test (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `VI006` | `channel_03_reflectance_0_64um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/goes-east_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `C01` | `channel_01_reflectance_0_47um` | channel | Bidirectional Reflectance | % |
| `C03` | `channel_03_reflectance_0_86um` | channel | Bidirectional Reflectance | % |
| `C05` | `channel_05_reflectance_1_6um` | channel | Bidirectional Reflectance | % |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/goes-east_2000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `C04` | `channel_04_reflectance_1_37um` | channel | Bidirectional Reflectance | % |
| `C06` | `channel_06_reflectance_2_2um` | channel | Bidirectional Reflectance | % |
| `C07` | `channel_07_brightness_temperature_3_9um` | channel | Brightness Temperature | K |
| `C08` | `channel_08_brightness_temperature_6_2um` | channel | Brightness Temperature | K |
| `C09` | `channel_09_brightness_temperature_6_9um` | channel | Brightness Temperature | K |
| `C10` | `channel_10_brightness_temperature_7_3um` | channel | Brightness Temperature | K |
| `C11` | `channel_11_brightness_temperature_8_4um` | channel | Brightness Temperature | K |
| `C12` | `channel_12_brightness_temperature_9_6um` | channel | Brightness Temperature | K |
| `C13` | `channel_13_brightness_temperature_10_3um` | channel | Brightness Temperature | K |
| `C14` | `channel_14_brightness_temperature_11_2um` | channel | Brightness Temperature | K |
| `C15` | `channel_15_brightness_temperature_12_3um` | channel | Brightness Temperature | K |
| `C16` | `channel_16_brightness_temperature_13_3um` | channel | Brightness Temperature | K |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/goes-east_2000m_test (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `C04` | `channel_04_reflectance_1_37um` | channel |  |  |
| `C06` | `channel_06_reflectance_2_2um` | channel |  |  |
| `C07` | `channel_07_brightness_temperature_3_9um` | channel |  |  |
| `C08` | `channel_08_brightness_temperature_6_2um` | channel |  |  |
| `C09` | `channel_09_brightness_temperature_6_9um` | channel |  |  |
| `C10` | `channel_10_brightness_temperature_7_3um` | channel |  |  |
| `C11` | `channel_11_brightness_temperature_8_4um` | channel |  |  |
| `C12` | `channel_12_brightness_temperature_9_6um` | channel |  |  |
| `C13` | `channel_13_brightness_temperature_10_3um` | channel |  |  |
| `C14` | `channel_14_brightness_temperature_11_2um` | channel |  |  |
| `C15` | `channel_15_brightness_temperature_12_3um` | channel |  |  |
| `C16` | `channel_16_brightness_temperature_13_3um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/goes-east_500m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `C02` | `channel_02_reflectance_0_64um` | channel | Bidirectional Reflectance | % |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/goes-west_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `C01` | `channel_01_reflectance_0_47um` | channel | Bidirectional Reflectance | % |
| `C03` | `channel_03_reflectance_0_86um` | channel | Bidirectional Reflectance | % |
| `C05` | `channel_05_reflectance_1_6um` | channel | Bidirectional Reflectance | % |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/goes-west_2000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `C04` | `channel_04_reflectance_1_37um` | channel | Bidirectional Reflectance | % |
| `C06` | `channel_06_reflectance_2_2um` | channel | Bidirectional Reflectance | % |
| `C07` | `channel_07_brightness_temperature_3_9um` | channel | Brightness Temperature | K |
| `C08` | `channel_08_brightness_temperature_6_2um` | channel | Brightness Temperature | K |
| `C09` | `channel_09_brightness_temperature_6_9um` | channel | Brightness Temperature | K |
| `C10` | `channel_10_brightness_temperature_7_3um` | channel | Brightness Temperature | K |
| `C11` | `channel_11_brightness_temperature_8_4um` | channel | Brightness Temperature | K |
| `C12` | `channel_12_brightness_temperature_9_6um` | channel | Brightness Temperature | K |
| `C13` | `channel_13_brightness_temperature_10_3um` | channel | Brightness Temperature | K |
| `C14` | `channel_14_brightness_temperature_11_2um` | channel | Brightness Temperature | K |
| `C15` | `channel_15_brightness_temperature_12_3um` | channel | Brightness Temperature | K |
| `C16` | `channel_16_brightness_temperature_13_3um` | channel | Brightness Temperature | K |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/goes-west_500m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `C02` | `channel_02_reflectance_0_64um` | channel | Bidirectional Reflectance | % |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/goes16_radf_C* (16 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `abi_l1b_radiances_data_quality_flags` | long_name | ABI L1b Radiances data quality flags | 1 |
| `Rad` | `abi_l1b_radiances` | long_name | ABI L1b Radiances | W m-2 sr-1 um-1 |
| `earth_sun_distance_anomaly_in_AU` | `earth_sun_distance_anomaly_in_au` | tidied | earth sun distance anomaly in astronomical units | ua |
| `esun` | `esun` | kept | bandpass-weighted solar irradiance at the mean Earth-Sun dis | W m-2 um-1 |
| `geospatial_lat_lon_extent` | `geospatial_latitude_and_longitude_references` | long_name | geospatial latitude and longitude references |  |
| `kappa0` | `kappa0` | kept | Inverse of the incoming top of atmosphere radiance at curren | (W m-2 um-1)-1 |
| `max_radiance_value_of_valid_pixels` | `maximum_radiance_value_of_pixels` | long_name | maximum radiance value of pixels | W m-2 sr-1 um-1 |
| `mean_radiance_value_of_valid_pixels` | `mean_radiance_value_of_pixels` | long_name | mean radiance value of pixels | W m-2 sr-1 um-1 |
| `min_radiance_value_of_valid_pixels` | `minimum_radiance_value_of_pixels` | long_name | minimum radiance value of pixels | W m-2 sr-1 um-1 |
| `missing_pixel_count` | `number_of_missing_pixels` | long_name | number of missing pixels | count |
| `nominal_satellite_subpoint_lat` | `nominal_satellite_subpoint_latitude` | long_name | nominal satellite subpoint latitude (platform latitude) | degrees_north |
| `nominal_satellite_subpoint_lon` | `nominal_satellite_subpoint_longitude` | long_name | nominal satellite subpoint longitude (platform longitude) | degrees_east |
| `saturated_pixel_count` | `number_of_saturated_pixels` | long_name | number of saturated pixels | count |
| `t` | `time` | coordinate | J2000 epoch mid-point between the start and end image scan i | seconds since 2000-01-01T12:00:00 |
| `t_star_look` | `j2000_epoch_time_of_star_observed_in_seconds` | long_name | J2000 epoch time of star observed in seconds | seconds since 2000-01-01T12:00:00 |
| `undersaturated_pixel_count` | `number_of_undersaturated_pixels` | long_name | number of undersaturated pixels | count |
| `x` | `geostationary_x` | coordinate | GOES fixed grid projection x-coordinate | rad |
| `x_coord` | `goes_fixed_grid_projection_x_coordinate` | long_name | GOES fixed grid projection x-coordinate | rad |
| `x_image` | `goes_r_fixed_grid_projection_x_coordinate_center_of_image` | long_name | GOES-R fixed grid projection x-coordinate center of image | rad |
| `y` | `geostationary_y` | coordinate | GOES fixed grid projection y-coordinate | rad |
| `y_coord` | `goes_fixed_grid_projection_y_coordinate` | long_name | GOES fixed grid projection y-coordinate | rad |
| `y_image` | `goes_r_fixed_grid_projection_y_coordinate_center_of_image` | long_name | GOES-R fixed grid projection y-coordinate center of image | rad |

## bkr/geo/goes17_radf_C* (15 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `abi_l1b_radiances_data_quality_flags` | long_name | ABI L1b Radiances data quality flags | 1 |
| `Rad` | `abi_l1b_radiances` | long_name | ABI L1b Radiances | W m-2 sr-1 um-1 |
| `earth_sun_distance_anomaly_in_AU` | `earth_sun_distance_anomaly_in_au` | tidied | earth sun distance anomaly in astronomical units | ua |
| `esun` | `esun` | kept | bandpass-weighted solar irradiance at the mean Earth-Sun dis | W m-2 um-1 |
| `geospatial_lat_lon_extent` | `geospatial_latitude_and_longitude_references` | long_name | geospatial latitude and longitude references |  |
| `kappa0` | `kappa0` | kept |  | (W m-2 um-1)-1 |
| `max_radiance_value_of_valid_pixels` | `maximum_radiance_value_of_pixels` | long_name | maximum radiance value of pixels | W m-2 sr-1 um-1 |
| `mean_radiance_value_of_valid_pixels` | `mean_radiance_value_of_pixels` | long_name | mean radiance value of pixels | W m-2 sr-1 um-1 |
| `min_radiance_value_of_valid_pixels` | `minimum_radiance_value_of_pixels` | long_name | minimum radiance value of pixels | W m-2 sr-1 um-1 |
| `missing_pixel_count` | `number_of_missing_pixels` | long_name | number of missing pixels | count |
| `saturated_pixel_count` | `number_of_saturated_pixels` | long_name | number of saturated pixels | count |
| `t` | `time` | coordinate | Scan start and end times in seconds since epoch (2000-01-01  | seconds since 2000-01-01T12:00:00 |
| `t_star_look` | `j2000_epoch_time_of_star_observed_in_seconds` | long_name | J2000 epoch time of star observed in seconds | seconds since 2000-01-01T12:00:00 |
| `undersaturated_pixel_count` | `number_of_undersaturated_pixels` | long_name | number of undersaturated pixels | count |
| `x` | `geostationary_x` | coordinate | GOES fixed grid projection x-coordinate | rad |
| `x_coord` | `goes_fixed_grid_projection_x_coordinate` | long_name | GOES fixed grid projection x-coordinate | rad |
| `x_image` | `x_image` | clash | GOES-R fixed grid projection x-coordinate center of image | rad |
| `y` | `geostationary_y` | coordinate | GOES fixed grid projection y-coordinate | rad |
| `y_coord` | `goes_fixed_grid_projection_y_coordinate` | long_name | GOES fixed grid projection y-coordinate | rad |
| `y_image` | `y_image` | clash | GOES-R fixed grid projection x-coordinate center of image | rad |

## bkr/geo/goes18_radf_C* (16 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `abi_l1b_radiances_data_quality_flags` | long_name | ABI L1b Radiances data quality flags | 1 |
| `Rad` | `abi_l1b_radiances` | long_name | ABI L1b Radiances | W m-2 sr-1 um-1 |
| `earth_sun_distance_anomaly_in_AU` | `earth_sun_distance_anomaly_in_au` | tidied | earth sun distance anomaly in astronomical units | ua |
| `esun` | `esun` | kept | bandpass-weighted solar irradiance at the mean Earth-Sun dis | W m-2 um-1 |
| `geospatial_lat_lon_extent` | `geospatial_latitude_and_longitude_references` | long_name | geospatial latitude and longitude references |  |
| `kappa0` | `kappa0` | kept | Inverse of the incoming top of atmosphere radiance at curren | (W m-2 um-1)-1 |
| `max_radiance_value_of_valid_pixels` | `maximum_radiance_value_of_pixels` | long_name | maximum radiance value of pixels | W m-2 sr-1 um-1 |
| `mean_radiance_value_of_valid_pixels` | `mean_radiance_value_of_pixels` | long_name | mean radiance value of pixels | W m-2 sr-1 um-1 |
| `min_radiance_value_of_valid_pixels` | `minimum_radiance_value_of_pixels` | long_name | minimum radiance value of pixels | W m-2 sr-1 um-1 |
| `missing_pixel_count` | `number_of_missing_pixels` | long_name | number of missing pixels | count |
| `nominal_satellite_subpoint_lat` | `nominal_satellite_subpoint_latitude` | long_name | nominal satellite subpoint latitude (platform latitude) | degrees_north |
| `nominal_satellite_subpoint_lon` | `nominal_satellite_subpoint_longitude` | long_name | nominal satellite subpoint longitude (platform longitude) | degrees_east |
| `saturated_pixel_count` | `number_of_saturated_pixels` | long_name | number of saturated pixels | count |
| `t` | `time` | coordinate | J2000 epoch mid-point between the start and end image scan i | seconds since 2000-01-01T12:00:00 |
| `undersaturated_pixel_count` | `number_of_undersaturated_pixels` | long_name | number of undersaturated pixels | count |
| `x` | `geostationary_x` | coordinate | GOES fixed grid projection x-coordinate | rad |
| `x_coord` | `goes_fixed_grid_projection_x_coordinate` | long_name | GOES fixed grid projection x-coordinate | rad |
| `x_image` | `goes_r_fixed_grid_projection_x_coordinate_center_of_image` | long_name | GOES-R fixed grid projection x-coordinate center of image | rad |
| `y` | `geostationary_y` | coordinate | GOES fixed grid projection y-coordinate | rad |
| `y_coord` | `goes_fixed_grid_projection_y_coordinate` | long_name | GOES fixed grid projection y-coordinate | rad |
| `y_image` | `goes_r_fixed_grid_projection_y_coordinate_center_of_image` | long_name | GOES-R fixed grid projection y-coordinate center of image | rad |

## bkr/geo/goes19_radf_C* (28 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `abi_l1b_radiances_data_quality_flags` | long_name | ABI L1b Radiances data quality flags | 1 |
| `Rad` | `abi_l1b_radiances` | long_name | ABI L1b Radiances | W m-2 sr-1 um-1 |
| `earth_sun_distance_anomaly_in_AU` | `earth_sun_distance_anomaly_in_au` | tidied | earth sun distance anomaly in astronomical units | ua |
| `esun` | `esun` | kept | bandpass-weighted solar irradiance at the mean Earth-Sun dis | W m-2 um-1 |
| `geospatial_lat_lon_extent` | `geospatial_latitude_and_longitude_references` | long_name | geospatial latitude and longitude references |  |
| `kappa0` | `kappa0` | kept | Inverse of the incoming top of atmosphere radiance at curren | (W m-2 um-1)-1 |
| `max_radiance_value_of_valid_pixels` | `maximum_radiance_value_of_pixels` | long_name | maximum radiance value of pixels | W m-2 sr-1 um-1 |
| `mean_radiance_value_of_valid_pixels` | `mean_radiance_value_of_pixels` | long_name | mean radiance value of pixels | W m-2 sr-1 um-1 |
| `min_radiance_value_of_valid_pixels` | `minimum_radiance_value_of_pixels` | long_name | minimum radiance value of pixels | W m-2 sr-1 um-1 |
| `missing_pixel_count` | `number_of_missing_pixels` | long_name | number of missing pixels | count |
| `nominal_satellite_subpoint_lat` | `nominal_satellite_subpoint_latitude` | long_name | nominal satellite subpoint latitude (platform latitude) | degrees_north |
| `nominal_satellite_subpoint_lon` | `nominal_satellite_subpoint_longitude` | long_name | nominal satellite subpoint longitude (platform longitude) | degrees_east |
| `saturated_pixel_count` | `number_of_saturated_pixels` | long_name | number of saturated pixels | count |
| `t` | `time` | coordinate | J2000 epoch mid-point between the start and end image scan i | seconds since 2000-01-01T12:00:00 |
| `t_star_look` | `j2000_epoch_time_of_star_observed_in_seconds` | long_name | J2000 epoch time of star observed in seconds | seconds since 2000-01-01T12:00:00 |
| `undersaturated_pixel_count` | `number_of_undersaturated_pixels` | long_name | number of undersaturated pixels | count |
| `x` | `geostationary_x` | coordinate | GOES fixed grid projection x-coordinate | rad |
| `x_coord` | `goes_fixed_grid_projection_x_coordinate` | long_name | GOES fixed grid projection x-coordinate | rad |
| `x_image` | `goes_r_fixed_grid_projection_x_coordinate_center_of_image` | long_name | GOES-R fixed grid projection x-coordinate center of image | rad |
| `y` | `geostationary_y` | coordinate | GOES fixed grid projection y-coordinate | rad |
| `y_coord` | `goes_fixed_grid_projection_y_coordinate` | long_name | GOES fixed grid projection y-coordinate | rad |
| `y_image` | `goes_r_fixed_grid_projection_y_coordinate_center_of_image` | long_name | GOES-R fixed grid projection y-coordinate center of image | rad |

## bkr/geo/himawari_1000m (45 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `B01` | `channel_01_reflectance_0_47um` | channel |  | % |
| `B02` | `channel_02_reflectance_0_51um` | channel |  | % |
| `B04` | `channel_04_reflectance_0_86um` | channel |  | % |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/himawari_2000m (48 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `B05` | `channel_05_reflectance_1_6um` | channel |  |  |
| `B06` | `channel_06_reflectance_2_3um` | channel |  |  |
| `B07` | `channel_07_brightness_temperature_3_9um` | channel |  |  |
| `B08` | `channel_08_brightness_temperature_6_2um` | channel |  |  |
| `B09` | `channel_09_brightness_temperature_6_9um` | channel |  |  |
| `B10` | `channel_10_brightness_temperature_7_3um` | channel |  |  |
| `B11` | `channel_11_brightness_temperature_8_6um` | channel |  |  |
| `B12` | `channel_12_brightness_temperature_9_6um` | channel |  |  |
| `B13` | `channel_13_brightness_temperature_10_4um` | channel |  |  |
| `B14` | `channel_14_brightness_temperature_11_2um` | channel |  |  |
| `B15` | `channel_15_brightness_temperature_12_4um` | channel |  |  |
| `B16` | `channel_16_brightness_temperature_13_3um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/himawari_500m (28 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `B03` | `channel_03_reflectance_0_64um` | channel |  | % |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/iodc_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `HRV` | `channel_12_reflectance_0_7um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/iodc_1000m_test (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `HRV` | `channel_12_reflectance_0_7um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/msg_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `HRV` | `channel_12_reflectance_0_7um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/msg_3000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `IR_016` | `channel_03_reflectance_1_6um` | channel |  |  |
| `IR_039` | `channel_04_brightness_temperature_3_9um` | channel |  |  |
| `IR_087` | `channel_07_brightness_temperature_8_7um` | channel |  |  |
| `IR_097` | `channel_08_brightness_temperature_9_7um` | channel |  |  |
| `IR_108` | `channel_09_brightness_temperature_10_8um` | channel |  |  |
| `IR_120` | `channel_10_brightness_temperature_12um` | channel |  |  |
| `IR_134` | `channel_11_brightness_temperature_13_4um` | channel |  |  |
| `VIS006` | `channel_01_reflectance_0_6um` | channel |  |  |
| `VIS008` | `channel_02_reflectance_0_8um` | channel |  |  |
| `WV_062` | `channel_05_brightness_temperature_6_2um` | channel |  |  |
| `WV_073` | `channel_06_brightness_temperature_7_3um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/mtg_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `nir_13` | `channel_06_reflectance_1_38um` | channel |  |  |
| `nir_16` | `channel_07_reflectance_1_61um` | channel |  |  |
| `nir_22` | `channel_08_reflectance_2_25um` | channel |  |  |
| `vis_04` | `channel_01_reflectance_0_44um` | channel |  |  |
| `vis_05` | `channel_02_reflectance_0_51um` | channel |  |  |
| `vis_06` | `channel_03_reflectance_0_64um` | channel |  |  |
| `vis_08` | `channel_04_reflectance_0_86um` | channel |  |  |
| `vis_09` | `channel_05_reflectance_0_91um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/mtg_500m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `nir_22` | `channel_08_reflectance_2_25um` | channel |  |  |
| `vis_06` | `channel_03_reflectance_0_64um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/mtg_highres_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `ir_105` | `channel_14_brightness_temperature_10_5um` | channel |  |  |
| `ir_38` | `channel_09_brightness_temperature_3_8um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/mtg_irs_l1 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `cloud_fraction` | `cloud_fraction_for_spatial_sample` | long_name | Cloud fraction for spatial sample |  |
| `cloud_signal` | `cloud_signal_for_spatial_sample` | long_name | Cloud signal for spatial sample |  |
| `dust_warning` | `dust_warning_raised` | long_name | Dust warning raised |  |
| `lac` | `local_area_coverage_1_4_south_to_north` | long_name | Local Area Coverage, 1-4 south to north |  |
| `lwir_effective_radiance` | `effective_radiance_lwir` | long_name | Effective radiance, LWIR | W m-2 sr-1 (m-1)-1 |
| `lwir_phase_average_warning` | `lwir_phase_average_warning` | clash | Pixel phase average too high warning flag |  |
| `lwir_rad_rts_noise_warning` | `lwir_rad_rts_noise_warning` | clash | RTS noise flag |  |
| `lwir_spatial_sample_quality` | `lwir_spatial_sample_quality` | clash |  Spatial sample quality flags |  |
| `mwir_effective_radiance` | `effective_radiance_mwir` | long_name | Effective radiance, MWIR | W m-2 sr-1 (m-1)-1 |
| `mwir_phase_average_warning` | `mwir_phase_average_warning` | clash | Pixel phase average too high warning flag |  |
| `mwir_rad_rts_noise_warning` | `mwir_rad_rts_noise_warning` | clash | RTS noise flag |  |
| `mwir_spatial_sample_quality` | `mwir_spatial_sample_quality` | clash |  Spatial sample quality flags |  |

## bkr/geo/odegree-12-highres_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `ir_105` | `channel_14_brightness_temperature_10_5um` | channel |  | K |
| `ir_38` | `channel_09_brightness_temperature_3_8um` | channel |  | K |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/odegree-12-highres_500m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `nir_22` | `channel_08_reflectance_2_25um` | channel |  | % |
| `vis_06` | `channel_03_reflectance_0_64um` | channel |  | % |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/odegree-12_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `nir_13` | `channel_06_reflectance_1_38um` | channel |  | % |
| `nir_16` | `channel_07_reflectance_1_61um` | channel |  | % |
| `nir_22` | `channel_08_reflectance_2_25um` | channel |  | % |
| `vis_04` | `channel_01_reflectance_0_44um` | channel |  | % |
| `vis_05` | `channel_02_reflectance_0_51um` | channel |  | % |
| `vis_06` | `channel_03_reflectance_0_64um` | channel |  | % |
| `vis_08` | `channel_04_reflectance_0_86um` | channel |  | % |
| `vis_09` | `channel_05_reflectance_0_91um` | channel |  | % |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/odegree-12_2000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `ir_105` | `channel_14_brightness_temperature_10_5um` | channel |  | K |
| `ir_123` | `channel_15_brightness_temperature_12_3um` | channel |  | K |
| `ir_133` | `channel_16_brightness_temperature_13_3um` | channel |  | K |
| `ir_38` | `channel_09_brightness_temperature_3_8um` | channel |  | K |
| `ir_87` | `channel_12_brightness_temperature_8_7um` | channel |  | K |
| `ir_97` | `channel_13_brightness_temperature_9_66um` | channel |  | K |
| `wv_63` | `channel_10_brightness_temperature_6_3um` | channel |  | K |
| `wv_73` | `channel_11_brightness_temperature_7_35um` | channel |  | K |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/odegree_3000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `IR_016` | `channel_03_reflectance_1_6um` | channel |  | % |
| `IR_039` | `channel_04_brightness_temperature_3_9um` | channel |  | K |
| `IR_087` | `channel_07_brightness_temperature_8_7um` | channel |  | K |
| `IR_097` | `channel_08_brightness_temperature_9_7um` | channel |  | K |
| `IR_108` | `channel_09_brightness_temperature_10_8um` | channel |  | K |
| `IR_120` | `channel_10_brightness_temperature_12um` | channel |  | K |
| `IR_134` | `channel_11_brightness_temperature_13_4um` | channel |  | K |
| `VIS006` | `channel_01_reflectance_0_6um` | channel |  | % |
| `VIS008` | `channel_02_reflectance_0_8um` | channel |  | % |
| `WV_062` | `channel_05_brightness_temperature_6_2um` | channel |  | K |
| `WV_073` | `channel_06_brightness_temperature_7_3um` | channel |  | K |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/rss_1000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `HRV` | `channel_12_reflectance_0_7um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/rss_3000m (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `IR_016` | `channel_03_reflectance_1_6um` | channel |  |  |
| `IR_039` | `channel_04_brightness_temperature_3_9um` | channel |  |  |
| `IR_087` | `channel_07_brightness_temperature_8_7um` | channel |  |  |
| `IR_097` | `channel_08_brightness_temperature_9_7um` | channel |  |  |
| `IR_108` | `channel_09_brightness_temperature_10_8um` | channel |  |  |
| `IR_120` | `channel_10_brightness_temperature_12um` | channel |  |  |
| `IR_134` | `channel_11_brightness_temperature_13_4um` | channel |  |  |
| `VIS006` | `channel_01_reflectance_0_6um` | channel |  |  |
| `VIS008` | `channel_02_reflectance_0_8um` | channel |  |  |
| `WV_062` | `channel_05_brightness_temperature_6_2um` | channel |  |  |
| `WV_073` | `channel_06_brightness_temperature_7_3um` | channel |  |  |
| `x_geostationary` | `geostationary_x` | coordinate |  | meter |
| `y_geostationary` | `geostationary_y` | coordinate |  | meter |

## bkr/geo/virtualized/gk2a_ami_fd_ir087 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_ir096 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_ir105 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_ir112 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_ir123 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_ir133 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_nr013 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_nr016 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_sw038 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_vi004 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_vi005 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_vi006 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_vi008 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_wv063 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_wv069 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/gk2a_ami_fd_wv073 (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `t` | `time` | coordinate | observation start time | nanoseconds since 2023-02-16 00:00:32.227242112 |
| `t_end` | `observation_end_time` | long_name | observation end time | nanoseconds since 2023-02-16 00:09:35.876406528 |

## bkr/geo/virtualized/goes16_radf_C* (46 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `abi_l1b_radiances_data_quality_flags` | long_name | ABI L1b Radiances data quality flags | 1 |
| `Rad` | `abi_l1b_radiances` | long_name | ABI L1b Radiances | W m-2 sr-1 um-1 |
| `earth_sun_distance_anomaly_in_AU` | `earth_sun_distance_anomaly_in_au` | tidied | earth sun distance anomaly in astronomical units | ua |
| `esun` | `esun` | kept | bandpass-weighted solar irradiance at the mean Earth-Sun dis | W m-2 um-1 |
| `geospatial_lat_lon_extent` | `geospatial_latitude_and_longitude_references` | long_name | geospatial latitude and longitude references |  |
| `kappa0` | `kappa0` | kept | Inverse of the incoming top of atmosphere radiance at curren | (W m-2 um-1)-1 |
| `max_radiance_value_of_valid_pixels` | `maximum_radiance_value_of_pixels` | long_name | maximum radiance value of pixels | W m-2 sr-1 um-1 |
| `mean_radiance_value_of_valid_pixels` | `mean_radiance_value_of_pixels` | long_name | mean radiance value of pixels | W m-2 sr-1 um-1 |
| `min_radiance_value_of_valid_pixels` | `minimum_radiance_value_of_pixels` | long_name | minimum radiance value of pixels | W m-2 sr-1 um-1 |
| `missing_pixel_count` | `number_of_missing_pixels` | long_name | number of missing pixels | count |
| `nominal_satellite_subpoint_lat` | `nominal_satellite_subpoint_latitude` | long_name | nominal satellite subpoint latitude (platform latitude) | degrees_north |
| `nominal_satellite_subpoint_lon` | `nominal_satellite_subpoint_longitude` | long_name | nominal satellite subpoint longitude (platform longitude) | degrees_east |
| `saturated_pixel_count` | `number_of_saturated_pixels` | long_name | number of saturated pixels | count |
| `t` | `time` | coordinate | J2000 epoch mid-point between the start and end image scan i | seconds since 2000-01-01T12:00:00 |
| `t_star_look` | `j2000_epoch_time_of_star_observed_in_seconds` | long_name | J2000 epoch time of star observed in seconds | seconds since 2000-01-01T12:00:00 |
| `undersaturated_pixel_count` | `number_of_undersaturated_pixels` | long_name | number of undersaturated pixels | count |
| `x` | `geostationary_x` | coordinate | GOES fixed grid projection x-coordinate | rad |
| `x_coord` | `goes_fixed_grid_projection_x_coordinate` | long_name | GOES fixed grid projection x-coordinate | rad |
| `x_image` | `goes_r_fixed_grid_projection_x_coordinate_center_of_image` | long_name | GOES-R fixed grid projection x-coordinate center of image | rad |
| `y` | `geostationary_y` | coordinate | GOES fixed grid projection y-coordinate | rad |
| `y_coord` | `goes_fixed_grid_projection_y_coordinate` | long_name | GOES fixed grid projection y-coordinate | rad |
| `y_image` | `goes_r_fixed_grid_projection_y_coordinate_center_of_image` | long_name | GOES-R fixed grid projection y-coordinate center of image | rad |

## bkr/geo/virtualized/goes17_radf_C* (32 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `abi_l1b_radiances_data_quality_flags` | long_name | ABI L1b Radiances data quality flags | 1 |
| `Rad` | `abi_l1b_radiances` | long_name | ABI L1b Radiances | W m-2 sr-1 um-1 |
| `earth_sun_distance_anomaly_in_AU` | `earth_sun_distance_anomaly_in_au` | tidied | earth sun distance anomaly in astronomical units | ua |
| `esun` | `esun` | kept | bandpass-weighted solar irradiance at the mean Earth-Sun dis | W m-2 um-1 |
| `geospatial_lat_lon_extent` | `geospatial_latitude_and_longitude_references` | long_name | geospatial latitude and longitude references |  |
| `kappa0` | `kappa0` | kept | Inverse of the incoming top of atmosphere radiance at curren | (W m-2 um-1)-1 |
| `max_radiance_value_of_valid_pixels` | `maximum_radiance_value_of_pixels` | long_name | maximum radiance value of pixels | W m-2 sr-1 um-1 |
| `mean_radiance_value_of_valid_pixels` | `mean_radiance_value_of_pixels` | long_name | mean radiance value of pixels | W m-2 sr-1 um-1 |
| `min_radiance_value_of_valid_pixels` | `minimum_radiance_value_of_pixels` | long_name | minimum radiance value of pixels | W m-2 sr-1 um-1 |
| `missing_pixel_count` | `number_of_missing_pixels` | long_name | number of missing pixels | count |
| `nominal_satellite_subpoint_lat` | `nominal_satellite_subpoint_latitude` | long_name | nominal satellite subpoint latitude (platform latitude) | degrees_north |
| `nominal_satellite_subpoint_lon` | `nominal_satellite_subpoint_longitude` | long_name | nominal satellite subpoint longitude (platform longitude) | degrees_east |
| `saturated_pixel_count` | `number_of_saturated_pixels` | long_name | number of saturated pixels | count |
| `t` | `time` | coordinate | J2000 epoch mid-point between the start and end image scan i | seconds since 2000-01-01T12:00:00 |
| `t_star_look` | `j2000_epoch_time_of_star_observed_in_seconds` | long_name | J2000 epoch time of star observed in seconds | seconds since 2000-01-01T12:00:00 |
| `undersaturated_pixel_count` | `number_of_undersaturated_pixels` | long_name | number of undersaturated pixels | count |
| `x` | `geostationary_x` | coordinate | GOES fixed grid projection x-coordinate | rad |
| `x_coord` | `goes_fixed_grid_projection_x_coordinate` | long_name | GOES fixed grid projection x-coordinate | rad |
| `x_image` | `goes_r_fixed_grid_projection_x_coordinate_center_of_image` | long_name | GOES-R fixed grid projection x-coordinate center of image | rad |
| `y` | `geostationary_y` | coordinate | GOES fixed grid projection y-coordinate | rad |
| `y_coord` | `goes_fixed_grid_projection_y_coordinate` | long_name | GOES fixed grid projection y-coordinate | rad |
| `y_image` | `goes_r_fixed_grid_projection_y_coordinate_center_of_image` | long_name | GOES-R fixed grid projection y-coordinate center of image | rad |

## bkr/geo/virtualized/goes18_radf_C* (30 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `abi_l1b_radiances_data_quality_flags` | long_name | ABI L1b Radiances data quality flags | 1 |
| `Rad` | `abi_l1b_radiances` | long_name | ABI L1b Radiances | W m-2 sr-1 um-1 |
| `earth_sun_distance_anomaly_in_AU` | `earth_sun_distance_anomaly_in_au` | tidied | earth sun distance anomaly in astronomical units | ua |
| `esun` | `esun` | kept | bandpass-weighted solar irradiance at the mean Earth-Sun dis | W m-2 um-1 |
| `geospatial_lat_lon_extent` | `geospatial_latitude_and_longitude_references` | long_name | geospatial latitude and longitude references |  |
| `kappa0` | `kappa0` | kept | Inverse of the incoming top of atmosphere radiance at curren | (W m-2 um-1)-1 |
| `max_radiance_value_of_valid_pixels` | `maximum_radiance_value_of_pixels` | long_name | maximum radiance value of pixels | W m-2 sr-1 um-1 |
| `mean_radiance_value_of_valid_pixels` | `mean_radiance_value_of_pixels` | long_name | mean radiance value of pixels | W m-2 sr-1 um-1 |
| `min_radiance_value_of_valid_pixels` | `minimum_radiance_value_of_pixels` | long_name | minimum radiance value of pixels | W m-2 sr-1 um-1 |
| `missing_pixel_count` | `number_of_missing_pixels` | long_name | number of missing pixels | count |
| `nominal_satellite_subpoint_lat` | `nominal_satellite_subpoint_latitude` | long_name | nominal satellite subpoint latitude (platform latitude) | degrees_north |
| `nominal_satellite_subpoint_lon` | `nominal_satellite_subpoint_longitude` | long_name | nominal satellite subpoint longitude (platform longitude) | degrees_east |
| `saturated_pixel_count` | `number_of_saturated_pixels` | long_name | number of saturated pixels | count |
| `t` | `time` | coordinate | J2000 epoch mid-point between the start and end image scan i | seconds since 2000-01-01T12:00:00 |
| `undersaturated_pixel_count` | `number_of_undersaturated_pixels` | long_name | number of undersaturated pixels | count |
| `x` | `geostationary_x` | coordinate | GOES fixed grid projection x-coordinate | rad |
| `x_coord` | `goes_fixed_grid_projection_x_coordinate` | long_name | GOES fixed grid projection x-coordinate | rad |
| `x_image` | `goes_r_fixed_grid_projection_x_coordinate_center_of_image` | long_name | GOES-R fixed grid projection x-coordinate center of image | rad |
| `y` | `geostationary_y` | coordinate | GOES fixed grid projection y-coordinate | rad |
| `y_coord` | `goes_fixed_grid_projection_y_coordinate` | long_name | GOES fixed grid projection y-coordinate | rad |
| `y_image` | `goes_r_fixed_grid_projection_y_coordinate_center_of_image` | long_name | GOES-R fixed grid projection y-coordinate center of image | rad |

## bkr/geo/virtualized/goes19_radf_C* (15 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `DQF` | `abi_l1b_radiances_data_quality_flags` | long_name | ABI L1b Radiances data quality flags | 1 |
| `Rad` | `abi_l1b_radiances` | long_name | ABI L1b Radiances | W m-2 sr-1 um-1 |
| `earth_sun_distance_anomaly_in_AU` | `earth_sun_distance_anomaly_in_au` | tidied | earth sun distance anomaly in astronomical units | ua |
| `esun` | `esun` | kept | bandpass-weighted solar irradiance at the mean Earth-Sun dis | W m-2 um-1 |
| `geospatial_lat_lon_extent` | `geospatial_latitude_and_longitude_references` | long_name | geospatial latitude and longitude references |  |
| `kappa0` | `kappa0` | kept | Inverse of the incoming top of atmosphere radiance at curren | (W m-2 um-1)-1 |
| `max_radiance_value_of_valid_pixels` | `maximum_radiance_value_of_pixels` | long_name | maximum radiance value of pixels | W m-2 sr-1 um-1 |
| `mean_radiance_value_of_valid_pixels` | `mean_radiance_value_of_pixels` | long_name | mean radiance value of pixels | W m-2 sr-1 um-1 |
| `min_radiance_value_of_valid_pixels` | `minimum_radiance_value_of_pixels` | long_name | minimum radiance value of pixels | W m-2 sr-1 um-1 |
| `missing_pixel_count` | `number_of_missing_pixels` | long_name | number of missing pixels | count |
| `nominal_satellite_subpoint_lat` | `nominal_satellite_subpoint_latitude` | long_name | nominal satellite subpoint latitude (platform latitude) | degrees_north |
| `nominal_satellite_subpoint_lon` | `nominal_satellite_subpoint_longitude` | long_name | nominal satellite subpoint longitude (platform longitude) | degrees_east |
| `saturated_pixel_count` | `number_of_saturated_pixels` | long_name | number of saturated pixels | count |
| `t` | `time` | coordinate | J2000 epoch mid-point between the start and end image scan i | seconds since 2000-01-01T12:00:00 |
| `t_star_look` | `j2000_epoch_time_of_star_observed_in_seconds` | long_name | J2000 epoch time of star observed in seconds | seconds since 2000-01-01T12:00:00 |
| `undersaturated_pixel_count` | `number_of_undersaturated_pixels` | long_name | number of undersaturated pixels | count |
| `x` | `geostationary_x` | coordinate | GOES fixed grid projection x-coordinate | rad |
| `x_coord` | `goes_fixed_grid_projection_x_coordinate` | long_name | GOES fixed grid projection x-coordinate | rad |
| `x_image` | `goes_r_fixed_grid_projection_x_coordinate_center_of_image` | long_name | GOES-R fixed grid projection x-coordinate center of image | rad |
| `y` | `geostationary_y` | coordinate | GOES fixed grid projection y-coordinate | rad |
| `y_coord` | `goes_fixed_grid_projection_y_coordinate` | long_name | GOES fixed grid projection y-coordinate | rad |
| `y_image` | `goes_r_fixed_grid_projection_y_coordinate_center_of_image` | long_name | GOES-R fixed grid projection y-coordinate center of image | rad |

## bkr/geo/virtualized/himawari8_isatss_C* (44 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `Sectorized_CMI` | `sectorized_cmi` | tidied |  | 1 |
| `t` | `time` | coordinate | scene start time | minutes since 2022-01-01 00:00:00 |
| `x` | `geostationary_x` | coordinate |  | microradian |
| `y` | `geostationary_y` | coordinate |  | microradian |

## bkr/geo/virtualized/himawari9_isatss_C* (15 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `Sectorized_CMI` | `sectorized_cmi` | tidied |  | 1 |
| `t` | `time` | coordinate | scene start time | minutes since 2025-01-01 00:00:00 |
| `x` | `geostationary_x` | coordinate |  | microradian |
| `y` | `geostationary_y` | coordinate |  | microradian |

## bkr/geos/geos_15min (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `CO` | `carbon_monoxide` | override | northward_wind | m s-1 |
| `NO2` | `nitrogen_dioxide` | override | northward_wind | m s-1 |
| `O3` | `ozone` | override | northward_wind | m s-1 |
| `PM25_RH35_GCC` | `particulate_matter_2_5um_rh35_gcc` | override | northward_wind | m s-1 |
| `PM25_RH35_GOCART` | `particulate_matter_2_5um_rh35_gocart` | override | northward_wind | m s-1 |
| `Q` | `specific_humidity` | override | northward_wind | m s-1 |
| `RH` | `relative_humidity` | override | northward_wind | m s-1 |
| `SLP` | `sea_level_pressure` | override | sea_level_pressure | Pa |
| `SO2` | `sulphur_dioxide` | override | northward_wind | m s-1 |
| `T` | `temperature` | override | northward_wind | m s-1 |
| `U` | `u_component_of_wind` | override | northward_wind | m s-1 |
| `V` | `v_component_of_wind` | override | northward_wind | m s-1 |

## bkr/geos/geos_v2_15min (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `CO` | `carbon_monoxide` | override | Carbon monoxide (CO, MW=28.01 g mol-1) mole fraction in dry  | mol mol-1 |
| `NO2` | `nitrogen_dioxide` | override | Nitrogen dioxide (NO2, MW=46.01 g mol-1) mole fraction in dr | mol mol-1 |
| `O3` | `ozone` | override | Ozone (O3, MW=48.0 g mol-1) mole fraction in dry air | mol mol-1 |
| `PM25_RH35` | `pm25_rh35` | tidied | Particulate matter with diameter less than 2.5 um mass at RH | ug m-3 |
| `SLP` | `sea_level_pressure` | override | sea_level_pressure | Pa |
| `SO2` | `sulphur_dioxide` | override | Sulfur dioxide (SO2, MW=64.04 g mol-1) mole fraction in dry  | mol mol-1 |
| `U` | `u_component_of_wind` | override | eastward_wind | m s-1 |
| `V` | `v_component_of_wind` | override | northward_wind | m s-1 |

## bkr/gfs/gfs (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `gh` | `geopotential` | grib+geopotential+units | Geopotential height | gpm -> m**2 s**-2 |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `prmsl` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `q` | `specific_humidity` | grib | Specific humidity | kg kg**-1 |
| `t2m` | `2m_temperature` | grib | 2 metre temperature | K |
| `u` | `u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u10` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `v` | `v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v10` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |

## bkr/gfs/gfs_analysis (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `gh` | `geopotential` | grib+geopotential+units | Geopotential height | gpm -> m**2 s**-2 |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `prmsl` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `q` | `specific_humidity` | grib | Specific humidity | kg kg**-1 |
| `t2m` | `2m_temperature` | grib | 2 metre temperature | K |
| `u` | `u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u10` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `v` | `v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v10` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |

## bkr/gfs/gfs_forecast_6hr (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `100_metre_u_wind_component_at_100.0m` | `100m_u_component_of_wind` | grib | 100 metre U wind component | m s**-1 |
| `100_metre_v_wind_component_at_100.0m` | `100m_v_component_of_wind` | grib | 100 metre V wind component | m s**-1 |
| `10_metre_u_wind_component_at_10.0m` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_10.0m` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `2_metre_dewpoint_temperature_at_2.0m` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_2.0m` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_2.0m` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_2.0m` | `2m_temperature` | grib | 2 metre temperature | K |
| `5-wave_geopotential_height` | `5_wave_geopotential` | grib+geopotential+units | 5-wave geopotential height | gpm -> m**2 s**-2 |
| `apparent_temperature_at_2.0m` | `2m_apparent_temperature` | grib | Apparent temperature | K |
| `best_(4-layer)_lifted_index_at_surface` | `best_lifted_index` | grib | Best (4-layer) lifted index | K |
| `categorical_freezing_rain_at_surface` | `categorical_freezing_rain` | grib | Categorical freezing rain | (Code table 4.222) |
| `categorical_ice_pellets_at_surface` | `categorical_ice_pellets` | grib | Categorical ice pellets | (Code table 4.222) |
| `categorical_rain_at_surface` | `categorical_rain` | grib | Categorical rain | (Code table 4.222) |
| `categorical_snow_at_surface` | `categorical_snow` | grib | Categorical snow | (Code table 4.222) |
| `clear_sky_uv-b_downward_solar_flux_at_surface` | `clear_sky_uv_b_downward_solar_flux` | grib | Clear sky UV-B downward solar flux | W m**-2 |
| `cloud_water_atmosphere_single_layer` | `cloud_water` | grib | Cloud water | kg m**-2 |
| `cloud_work_function_atmosphere_single_layer` | `cloud_work_function` | grib | Cloud work function | J kg**-1 |
| `convective_available_potential_energy_at_surface` | `convective_available_potential_energy` | grib | Convective available potential energy | J kg**-1 |
| `convective_inhibition_at_surface` | `convective_inhibition` | grib | Convective inhibition | J kg**-1 |
| `convective_precipitation_(water)_at_surface` | `convective_precipitation` | grib | Convective precipitation (water) | kg m**-2 |
| `convective_precipitation_rate_at_surface` | `convective_precipitation_rate` | grib | Convective precipitation rate | kg m**-2 s**-1 |
| `field_capacity_at_surface` | `field_capacity` | grib | Field Capacity | Fraction |
| `geopotential_height` | `geopotential` | grib+geopotential+units | Geopotential height | gpm -> m**2 s**-2 |
| `ground_heat_flux_at_surface` | `ground_heat_flux` | grib | Ground heat flux | W m**-2 |
| `haines_index_at_surface` | `haines_index` | grib | Haines Index | Numeric |
| `instantaneous_eastward_gravity_wave_surface_stress_at_surface` | `instantaneous_eastward_gravity_wave_surface_stress` | grib | Instantaneous eastward gravity wave surface stress | N m**-2 |
| `instantaneous_northward_gravity_wave_surface_stress_at_surface` | `instantaneous_northward_gravity_wave_surface_stress` | grib | Instantaneous northward gravity wave surface stress | N m**-2 |
| `land-sea_mask_at_surface` | `land_sea_mask` | grib | Land-sea mask | (0 - 1) |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `liquid_volumetric_soil_moisture_(non-frozen)` | `liquid_volumetric_soil_moisture` | grib | Liquid volumetric soil moisture (non-frozen) | Proportion |
| `maximum_temperature_at_2.0m` | `2m_maximum_temperature` | grib | Maximum temperature | K |
| `minimum_temperature_at_2.0m` | `2m_minimum_temperature` | grib | Minimum temperature | K |
| `mslp_(eta_model_reduction)` | `mslp` | grib | MSLP (Eta model reduction) | Pa |
| `orography_at_surface` | `geopotential_at_surface` | grib+geopotential+units | Orography | m -> m**2 s**-2 |
| `percent_frozen_precipitation_at_surface` | `percent_frozen_precipitation` | grib | Percent frozen precipitation | % |
| `plant_canopy_surface_water_at_surface` | `plant_canopy_surface_water` | grib | Plant canopy surface water | kg m**-2 |
| `precipitable_water_atmosphere_single_layer` | `precipitable_water` | grib | Precipitable water | kg m**-2 |
| `precipitation_rate_at_surface` | `precipitation_rate` | grib | Precipitation rate | kg m**-2 s**-1 |
| `pressure_at_80.0m` | `80m_pressure` | grib | Pressure | Pa |
| `pressure_reduced_to_msl` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `relative_humidity_atmosphere_single_layer` | `relative_humidity_entire_atmosphere` | clash | Relative humidity | % |
| `sea_ice_area_fraction_at_surface` | `sea_ice_area_fraction` | grib | Sea ice area fraction | (0 - 1) |
| `sea_ice_thickness_at_surface` | `sea_ice_thickness` | grib | Sea ice thickness | m |
| `snow_depth_at_surface` | `snow_depth` | grib | Snow depth | m |
| `specific_humidity_at_80.0m` | `80m_specific_humidity` | grib | Specific humidity | kg kg**-1 |
| `sunshine_duration_at_surface` | `sunshine_duration` | grib | Sunshine Duration | s |
| `surface_downward_long-wave_radiation_flux_at_surface` | `surface_downward_long_wave_radiation_flux` | grib | Surface downward long-wave radiation flux | W m**-2 |
| `surface_downward_short-wave_radiation_flux_at_surface` | `surface_downward_short_wave_radiation_flux` | grib | Surface downward short-wave radiation flux | W m**-2 |
| `surface_lifted_index_at_surface` | `surface_lifted_index` | grib | Surface lifted index | K |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_upward_long-wave_radiation_flux_at_surface` | `surface_upward_long_wave_radiation_flux` | grib | Surface upward long-wave radiation flux | W m**-2 |
| `surface_upward_short-wave_radiation_flux_at_surface` | `surface_upward_short_wave_radiation_flux` | grib | Surface upward short-wave radiation flux | W m**-2 |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-mean_forecast_albedo_at_surface` | `mean_forecast_albedo` | grib | Time-mean forecast albedo | % |
| `time-mean_surface_latent_heat_flux_at_surface` | `mean_surface_latent_heat_flux` | grib | Time-mean surface latent heat flux | W m**-2 |
| `time-mean_surface_sensible_heat_flux_at_surface` | `mean_surface_sensible_heat_flux` | grib | Time-mean surface sensible heat flux | W m**-2 |
| `time-mean_u-component_of_atmospheric_surface_momentum_flux_at_surface` | `mean_u_component_of_atmospheric_surface_momentum_flux` | grib | Time-mean U-component of atmospheric surface momentum flux | N m**-2 |
| `time-mean_v-component_of_atmospheric_surface_momentum_flux_at_surface` | `mean_v_component_of_atmospheric_surface_momentum_flux` | grib | Time-mean V-component of atmospheric surface momentum flux | N m**-2 |
| `total_ozone_atmosphere_single_layer` | `total_ozone` | grib | Total ozone | DU |
| `total_precipitation_at_surface` | `total_precipitation` | grib+units | Total Precipitation | kg m**-2 -> m |
| `u_component_of_wind_at_80.0m` | `80m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `uv-b_downward_solar_flux_at_surface` | `uv_b_downward_solar_flux` | grib | UV-B downward solar flux | W m**-2 |
| `v_component_of_wind_at_80.0m` | `80m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `water_equivalent_of_accumulated_snow_depth_(deprecated)_at_surface` | `water_equivalent_of_accumulated_snow_depth` | grib | Water equivalent of accumulated snow depth (deprecated) | kg m**-2 |
| `water_runoff_at_surface` | `water_runoff` | grib | Water runoff | kg m**-2 |
| `wilting_point_at_surface` | `wilting_point` | grib | Wilting Point | Fraction |
| `wind_speed_(gust)_at_surface` | `wind_speed` | grib | Wind speed (gust) | m s**-1 |

## bkr/gfs/gfs_forecast_6hr_1deg (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `100_metre_u_wind_component_at_100.0m` | `100m_u_component_of_wind` | grib | 100 metre U wind component | m s**-1 |
| `100_metre_v_wind_component_at_100.0m` | `100m_v_component_of_wind` | grib | 100 metre V wind component | m s**-1 |
| `10_metre_u_wind_component_at_10.0m` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_at_10.0m` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `2_metre_dewpoint_temperature_at_2.0m` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_relative_humidity_at_2.0m` | `2m_relative_humidity` | grib | 2 metre relative humidity | % |
| `2_metre_specific_humidity_at_2.0m` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `2_metre_temperature_at_2.0m` | `2m_temperature` | grib | 2 metre temperature | K |
| `5-wave_geopotential_height` | `5_wave_geopotential` | grib+geopotential+units | 5-wave geopotential height | gpm -> m**2 s**-2 |
| `apparent_temperature_at_2.0m` | `2m_apparent_temperature` | grib | Apparent temperature | K |
| `best_(4-layer)_lifted_index_at_surface` | `best_lifted_index` | grib | Best (4-layer) lifted index | K |
| `categorical_freezing_rain_at_surface` | `categorical_freezing_rain` | grib | Categorical freezing rain | (Code table 4.222) |
| `categorical_ice_pellets_at_surface` | `categorical_ice_pellets` | grib | Categorical ice pellets | (Code table 4.222) |
| `categorical_rain_at_surface` | `categorical_rain` | grib | Categorical rain | (Code table 4.222) |
| `categorical_snow_at_surface` | `categorical_snow` | grib | Categorical snow | (Code table 4.222) |
| `clear_sky_uv-b_downward_solar_flux_at_surface` | `clear_sky_uv_b_downward_solar_flux` | grib | Clear sky UV-B downward solar flux | W m**-2 |
| `cloud_water_atmosphere_single_layer` | `cloud_water` | grib | Cloud water | kg m**-2 |
| `cloud_work_function_atmosphere_single_layer` | `cloud_work_function` | grib | Cloud work function | J kg**-1 |
| `convective_available_potential_energy_at_surface` | `convective_available_potential_energy` | grib | Convective available potential energy | J kg**-1 |
| `convective_inhibition_at_surface` | `convective_inhibition` | grib | Convective inhibition | J kg**-1 |
| `convective_precipitation_(water)_at_surface` | `convective_precipitation` | grib | Convective precipitation (water) | kg m**-2 |
| `convective_precipitation_rate_at_surface` | `convective_precipitation_rate` | grib | Convective precipitation rate | kg m**-2 s**-1 |
| `field_capacity_at_surface` | `field_capacity` | grib | Field Capacity | Fraction |
| `geopotential_height` | `geopotential` | grib+geopotential+units | Geopotential height | gpm -> m**2 s**-2 |
| `ground_heat_flux_at_surface` | `ground_heat_flux` | grib | Ground heat flux | W m**-2 |
| `haines_index_at_surface` | `haines_index` | grib | Haines Index | Numeric |
| `instantaneous_eastward_gravity_wave_surface_stress_at_surface` | `instantaneous_eastward_gravity_wave_surface_stress` | grib | Instantaneous eastward gravity wave surface stress | N m**-2 |
| `instantaneous_northward_gravity_wave_surface_stress_at_surface` | `instantaneous_northward_gravity_wave_surface_stress` | grib | Instantaneous northward gravity wave surface stress | N m**-2 |
| `land-sea_mask_at_surface` | `land_sea_mask` | grib | Land-sea mask | (0 - 1) |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `liquid_volumetric_soil_moisture_(non-frozen)` | `liquid_volumetric_soil_moisture` | grib | Liquid volumetric soil moisture (non-frozen) | Proportion |
| `maximum_temperature_at_2.0m` | `2m_maximum_temperature` | grib | Maximum temperature | K |
| `minimum_temperature_at_2.0m` | `2m_minimum_temperature` | grib | Minimum temperature | K |
| `mslp_(eta_model_reduction)` | `mslp` | grib | MSLP (Eta model reduction) | Pa |
| `orography_at_surface` | `geopotential_at_surface` | grib+geopotential+units | Orography | m -> m**2 s**-2 |
| `percent_frozen_precipitation_at_surface` | `percent_frozen_precipitation` | grib | Percent frozen precipitation | % |
| `plant_canopy_surface_water_at_surface` | `plant_canopy_surface_water` | grib | Plant canopy surface water | kg m**-2 |
| `precipitable_water_atmosphere_single_layer` | `precipitable_water` | grib | Precipitable water | kg m**-2 |
| `precipitation_rate_at_surface` | `precipitation_rate` | grib | Precipitation rate | kg m**-2 s**-1 |
| `pressure_at_80.0m` | `80m_pressure` | grib | Pressure | Pa |
| `pressure_reduced_to_msl` | `mean_sea_level_pressure` | grib | Pressure reduced to MSL | Pa |
| `relative_humidity_atmosphere_single_layer` | `relative_humidity_entire_atmosphere` | clash | Relative humidity | % |
| `sea_ice_area_fraction_at_surface` | `sea_ice_area_fraction` | grib | Sea ice area fraction | (0 - 1) |
| `sea_ice_thickness_at_surface` | `sea_ice_thickness` | grib | Sea ice thickness | m |
| `snow_depth_at_surface` | `snow_depth` | grib | Snow depth | m |
| `specific_humidity_at_80.0m` | `80m_specific_humidity` | grib | Specific humidity | kg kg**-1 |
| `sunshine_duration_at_surface` | `sunshine_duration` | grib | Sunshine Duration | s |
| `surface_downward_long-wave_radiation_flux_at_surface` | `surface_downward_long_wave_radiation_flux` | grib | Surface downward long-wave radiation flux | W m**-2 |
| `surface_downward_short-wave_radiation_flux_at_surface` | `surface_downward_short_wave_radiation_flux` | grib | Surface downward short-wave radiation flux | W m**-2 |
| `surface_lifted_index_at_surface` | `surface_lifted_index` | grib | Surface lifted index | K |
| `surface_pressure_at_surface` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_upward_long-wave_radiation_flux_at_surface` | `surface_upward_long_wave_radiation_flux` | grib | Surface upward long-wave radiation flux | W m**-2 |
| `surface_upward_short-wave_radiation_flux_at_surface` | `surface_upward_short_wave_radiation_flux` | grib | Surface upward short-wave radiation flux | W m**-2 |
| `temperature_at_surface` | `temperature_at_surface` | clash | Temperature | K |
| `time-mean_forecast_albedo_at_surface` | `mean_forecast_albedo` | grib | Time-mean forecast albedo | % |
| `time-mean_surface_latent_heat_flux_at_surface` | `mean_surface_latent_heat_flux` | grib | Time-mean surface latent heat flux | W m**-2 |
| `time-mean_surface_sensible_heat_flux_at_surface` | `mean_surface_sensible_heat_flux` | grib | Time-mean surface sensible heat flux | W m**-2 |
| `time-mean_u-component_of_atmospheric_surface_momentum_flux_at_surface` | `mean_u_component_of_atmospheric_surface_momentum_flux` | grib | Time-mean U-component of atmospheric surface momentum flux | N m**-2 |
| `time-mean_v-component_of_atmospheric_surface_momentum_flux_at_surface` | `mean_v_component_of_atmospheric_surface_momentum_flux` | grib | Time-mean V-component of atmospheric surface momentum flux | N m**-2 |
| `total_ozone_atmosphere_single_layer` | `total_ozone` | grib | Total ozone | DU |
| `total_precipitation_at_surface` | `total_precipitation` | grib+units | Total Precipitation | kg m**-2 -> m |
| `u_component_of_wind_at_80.0m` | `80m_u_component_of_wind` | grib | U component of wind | m s**-1 |
| `uv-b_downward_solar_flux_at_surface` | `uv_b_downward_solar_flux` | grib | UV-B downward solar flux | W m**-2 |
| `v_component_of_wind_at_80.0m` | `80m_v_component_of_wind` | grib | V component of wind | m s**-1 |
| `water_equivalent_of_accumulated_snow_depth_(deprecated)_at_surface` | `water_equivalent_of_accumulated_snow_depth` | grib | Water equivalent of accumulated snow depth (deprecated) | kg m**-2 |
| `water_runoff_at_surface` | `water_runoff` | grib | Water runoff | kg m**-2 |
| `wilting_point_at_surface` | `wilting_point` | grib | Wilting Point | Fraction |
| `wind_speed_(gust)_at_surface` | `wind_speed` | grib | Wind speed (gust) | m s**-1 |

## bkr/gmgi/gmgsi (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `lwir` | `longwave_infrared` | abbreviation | 0-255 Brightness Temperature | none |
| `swir` | `shortwave_infrared` | abbreviation | 0-255 Brightness Temperature | none |
| `vis` | `visible` | abbreviation | 0-255 Brightness Temperature | none |
| `wv` | `water_vapour` | abbreviation | 0-255 Brightness Temperature | none |
| `xc` | `x` | coordinate |  |  |

## bkr/gmgi/gmgsi_v1 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `lwir` | `longwave_infrared` | abbreviation | 0-255 Brightness Temperature | none |
| `ssr` | `shortwave_reflectance` | abbreviation | 0-255 Brightness Temperature | K |
| `swir` | `shortwave_infrared` | abbreviation | 0-255 Brightness Temperature | none |
| `vis` | `visible` | abbreviation | 0-255 Brightness Temperature | none |
| `wv` | `water_vapour` | abbreviation | 0-255 Brightness Temperature | none |

## bkr/gmgi/gmgsi_v3 (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `lwir` | `longwave_infrared` | abbreviation | 0-255 Brightness Temperature | K |
| `lwir_dqf` | `lwir_dqf` | clash | Data Quality Flag | 1 |
| `swir` | `shortwave_infrared` | abbreviation | 0-255 Brightness Temperature | K |
| `swir_dqf` | `swir_dqf` | clash | Data Quality Flag | 1 |
| `vis` | `visible` | abbreviation | 0-255 Brightness Temperature | K |
| `vis_dqf` | `vis_dqf` | clash | Data Quality Flag | 1 |
| `wv` | `water_vapour` | abbreviation | 0-255 Brightness Temperature | K |
| `wv_dqf` | `wv_dqf` | clash | Data Quality Flag | 1 |

## bkr/icon/icon_d2_ruc_15min (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `asob_s` | `mean_surface_net_short_wave_radiation_flux` | override | Time-mean surface net short-wave radiation flux | W m**-2 |
| `aswdifd_s` | `mean_surface_diffuse_short_wave_radiation_flux` | override | Downward diffusive short wave radiation flux at surface (mea | W m-2 |
| `aswdir_s` | `mean_surface_direct_short_wave_radiation_flux` | override | Downward direct short wave radiation flux at surface (mean o | W m-2 |
| `cape_ml` | `mixed_layer_convective_available_potential_energy` | override | Convective Available Potential Energy, mean layer | J kg-1 |
| `cin_ml` | `mixed_layer_convective_inhibition` | override | Convective Inhibition, mean layer | J kg-1 |
| `echotop` | `minimum_pressure` | grib | Time-minimum pressure | Pa |
| `echotopinm` | `echotop_height_largest_height_where_radar_reflectivity_above_a_threshold_is_present` | grib | Echotop-height: largest height where radar reflectivity abov | m |
| `grau_gsp` | `graupel_precipitation` | grib | Graupel (snow pellets) precipitation | kg m**-2 |
| `hail_gsp` | `hail_precipitation_rate_accumulation` | grib | Hail  precipitation rate, accumulation | kg m-2 |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `lpi` | `lightning_potential_index` | grib | Lightning Potential Index | J kg-1 |
| `lpi_max` | `maximum_of_lightning_potential_index` | grib | Maximum of Lightning Potential Index (over given time interv | J kg-1 |
| `prg_gsp` | `graupel_precipitation_rate` | grib | Graupel (snow pellets) precipitation rate | kg m-2 s-1 |
| `prh_gsp` | `hail_precipitation_rate` | grib | Hail  precipitation rate  | kg m-2 s-1 |
| `prr_gsp` | `large_scale_rain_rate` | grib | Large scale rain rate | kg m**-2 s**-1 |
| `prs_gsp` | `large_scale_snowfall_rate_water_equivalent` | grib | Large scale snowfall rate water equivalent | kg m**-2 s**-1 |
| `rain_gsp` | `large_scale_rainfall` | override | Large scale rain rate | kg m**-2 s**-1 |
| `relhum` | `relative_humidity` | grib | Relative humidity | % |
| `sdi_2` | `supercell_detection_index_2` | grib | Supercell detection index 2 (only rot. updrafts) | s-1 |
| `snow_gsp` | `large_scale_snowfall` | override | Large-scale snowfall water equivalent | kg m**-2 |
| `snowlmt` | `height_of_snow_fall_limit_above_msl` | grib | Height of snow fall limit above MSL | m |
| `tcond10_mx` | `maximum_total_column_integrated_condensed_water_above_10_c_isotherm` | grib | Maximum total-column integrated condensed water above -10 C  | kg m-2 |
| `tcond_max` | `maximum_total_column_integrated_condensed_water` | grib | Maximum total-column integrated condensed  water (over given | kg m-2 |
| `tot_pr_max` | `maximum_total_precipitation_rate` | grib | Time-maximum total precipitation rate | kg m**-2 s**-1 |
| `tqc` | `total_column_cloud_liquid_water` | override | Total Column-Integrated Cloud Water | kg m-2 |
| `tqc_dia` | `total_column_integrated_cloud_water` | grib | Total column integrated cloud water (diagnostic)  | kg m-2 |
| `tqg` | `total_column_integrated_grauple` | grib | Total column integrated grauple | kg m-2 |
| `tqh` | `hail` | grib | Hail   | kg m-2 |
| `tqi` | `total_column_cloud_ice_water` | override | Total Column-Integrated Cloud Ice | kg m-2 |
| `tqi_dia` | `total_column_integrated_cloud_ice` | grib | Total column integrated cloud ice (diagnostic)  | kg m-2 |
| `tqr` | `total_column_integrated_rain` | grib | Total column integrated rain | kg m**-2 |
| `tqs` | `total_column_integrated_snow` | grib | Total column integrated snow | kg m**-2 |
| `tqv` | `total_column_water_vapour` | override | Total column integrated water vapour | kg m-2 |
| `tqv_dia` | `total_column_integrated_water_vapour` | grib | Total column integrated water vapour (diagnostic)  | kg m-2 |
| `uh_max` | `maximum_amplitude_of_updraft_helicity` | grib | Maximum amplitude (positive or negative) of updraft helicity | m2 s-2 |
| `vis` | `visible` | abbreviation | Visibility | m |
| `vorw_ctmax` | `maximum_rotation_amplitude` | grib | Maximum rotation amplitude (positive or negative) (over give | s-1 |
| `w_ctmax` | `maximum_updraft_track` | grib | Maximum updraft track (over given time interval and column) | m s-1 |
| `wz_pressure_level` | `geometric_vertical_velocity` | grib | Geometric vertical velocity | m s**-1 |

## bkr/icon/icon_d2_ruc_5min (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `pr_gsp` | `large_scale_precipitation_rate` | grib | Large scale precipitation rate | kg m**-2 s**-1 |
| `prec_gsp` | `large_scale_precipitation` | grib | Large-scale precipitation | kg m**-2 |
| `tot_pr` | `total_precipitation_rate` | grib | Total precipitation rate | kg m**-2 s**-1 |
| `tot_prec` | `total_precipitation` | override+units | Total Precipitation | kg m**-2 -> m |

## bkr/icon/icon_d2_ruc_60min (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alb_rad` | `surface_albedo` | override | Forecast albedo | % |
| `cape_mu` | `most_unstable_convective_available_potential_energy` | override | Convective Available Potential Energy, most unstable | J kg-1 |
| `cin_mu` | `most_unstable_convective_inhibition` | override | Convective Inhibition, most unstable | J kg-1 |
| `clc` | `cloud_cover` | grib | Cloud cover | % |
| `clch` | `high_cloud_cover` | override+units | Cloud Cover (0 - 400 hPa) | % -> (0 - 1) |
| `clcl` | `low_cloud_cover` | override+units | Cloud Cover (800 hPa - Soil) | % -> (0 - 1) |
| `clcm` | `medium_cloud_cover` | override+units | Cloud Cover (400 - 800 hPa) | % -> (0 - 1) |
| `clct` | `total_cloud_cover` | override+units | Total Cloud Cover | % -> (0 - 1) |
| `depth_lk` | `lake_depth` | grib | Lake depth | m |
| `fi` | `geopotential` | override | Geopotential | m**2 s**-2 |
| `fr_ice` | `sea_ice_area_fraction` | grib | Sea ice area fraction | (0 - 1) |
| `fr_lake` | `lake_cover` | grib | Lake cover | (0 - 1) |
| `fr_land` | `land_sea_mask` | grib | Land-sea mask | (0 - 1) |
| `h_snow` | `snow_depth` | override | Snow depth | m |
| `hhl` | `height_of_model_half_levels` | override | Geometric Height of the layer limits above sea level(NN) | m |
| `hsurf` | `geopotential_at_surface` | override+geopotential+units | Geometric Height of the earths surface above sea level | m -> m**2 s**-2 |
| `lai` | `leaf_area_index` | grib | Leaf Area Index | Numeric |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `model_level_half` | `model_half_level` | coordinate | original GRIB coordinate for key: level(generalVertical) | 1 |
| `p` | `pressure` | grib | Pressure | Pa |
| `plcov` | `vegetation` | grib | Vegetation | % |
| `pmsl` | `mean_sea_level_pressure` | override | Pressure reduced to MSL | Pa |
| `ps` | `surface_pressure` | override | Surface pressure | Pa |
| `q_model_level` | `specific_humidity_on_model_levels` | clash | Specific humidity | kg kg**-1 |
| `q_pressure_level` | `specific_humidity_on_pressure_levels` | clash | Specific humidity | kg kg**-1 |
| `qc` | `specific_cloud_liquid_water_content` | override | Cloud mixing ratio | kg kg**-1 |
| `qc_dia` | `specific_cloud_water_content` | grib | Specific cloud water content (diagnostic) | kg kg-1 |
| `qg` | `specific_graupel_content` | override | Graupel (snow pellets) | kg kg**-1 |
| `qh` | `hail_mixing_ratio` | grib | Hail  mixing ratio | kg kg-1 |
| `qi` | `specific_cloud_ice_water_content` | override | Cloud Ice Mixing Ratio | kg kg-1 |
| `qi_dia` | `specific_cloud_ice_content` | grib | Specific cloud ice content (diagnostic)  | kg kg-1 |
| `qr` | `specific_rain_water_content` | override | Rain mixing ratio | kg kg**-1 |
| `qs` | `specific_snow_water_content` | override | Snow mixing ratio | kg kg**-1 |
| `qv_2m` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `qv_s` | `specific_humidity` | grib | Specific Humidity (S) | kg kg-1 |
| `relhum_2m` | `2m_relative_humidity` | override | 2 metre relative humidity | % |
| `rootdp` | `root_depth_of_vegetation` | grib | root depth of vegetation | m |
| `srh` | `storm_relative_helicity` | grib | Storm relative helicity | m**2 s**-2 |
| `t_2m` | `2m_temperature` | override | 2 metre temperature | K |
| `t_g` | `skin_temperature` | override | Temperature (G) | K |
| `t_model_level` | `temperature_on_model_levels` | clash | Temperature | K |
| `t_pressure_level` | `temperature_on_pressure_levels` | clash | Temperature | K |
| `tch` | `turbulent_transfer_coefficient_for_heat` | grib | Turbulent transfer coefficient for heat (and Moisture) | Numeric |
| `tcm` | `drag_coefficient` | grib | Drag coefficient | Numeric |
| `td_2m` | `2m_dewpoint_temperature` | override | 2 metre dewpoint temperature | K |
| `tke` | `turbulent_kinetic_energy` | grib | Turbulent kinetic energy | J kg**-1 |
| `tot_prec_d` | `total_precipitation_rate` | grib | Total precipitation rate | kg m**-2 s**-1 |
| `u_10m` | `10m_u_component_of_wind` | override | 10 metre U wind component | m s**-1 |
| `u_model_level` | `u_component_of_wind_on_model_levels` | clash | U component of wind | m s**-1 |
| `u_pressure_level` | `u_component_of_wind_on_pressure_levels` | clash | U component of wind | m s**-1 |
| `v_10m` | `10m_v_component_of_wind` | override | 10 metre V wind component | m s**-1 |
| `v_model_level` | `v_component_of_wind_on_model_levels` | clash | V component of wind | m s**-1 |
| `v_pressure_level` | `v_component_of_wind_on_pressure_levels` | clash | V component of wind | m s**-1 |
| `vmax_10m` | `10m_wind_gust` | override | Time-maximum 10 metre wind gust | m s**-1 |
| `wshear_u` | `u_component_of_wind_shear_vector_between_two_levels` | grib | U-component of (vertical) wind shear vector between two leve | m s-1 |
| `wshear_v` | `v_component_of_wind_shear_vector_between_two_levels` | grib | V-component of (vertical) wind shear vector between two leve | m s-1 |
| `wz_model_level` | `geometric_vertical_velocity` | grib | Geometric vertical velocity | m s**-1 |
| `z0` | `forecast_surface_roughness` | override | Forecast surface roughness | m |

## bkr/icon/icon_d2_ruc_hourly (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alb_rad` | `surface_albedo` | override | Forecast albedo | % |
| `cape_mu` | `most_unstable_convective_available_potential_energy` | override | Convective Available Potential Energy, most unstable | J kg-1 |
| `cin_mu` | `most_unstable_convective_inhibition` | override | Convective Inhibition, most unstable | J kg-1 |
| `clc` | `cloud_cover` | grib | Cloud cover | % |
| `clch` | `high_cloud_cover` | override+units | Cloud Cover (0 - 400 hPa) | % -> (0 - 1) |
| `clcl` | `low_cloud_cover` | override+units | Cloud Cover (800 hPa - Soil) | % -> (0 - 1) |
| `clcm` | `medium_cloud_cover` | override+units | Cloud Cover (400 - 800 hPa) | % -> (0 - 1) |
| `clct` | `total_cloud_cover` | override+units | Total Cloud Cover | % -> (0 - 1) |
| `depth_lk` | `lake_depth` | grib | Lake depth | m |
| `fi` | `geopotential` | override | Geopotential | m**2 s**-2 |
| `fr_ice` | `sea_ice_area_fraction` | grib | Sea ice area fraction | (0 - 1) |
| `fr_lake` | `lake_cover` | grib | Lake cover | (0 - 1) |
| `fr_land` | `land_sea_mask` | grib | Land-sea mask | (0 - 1) |
| `h_snow` | `snow_depth` | override | Snow depth | m |
| `hhl` | `height_of_model_half_levels` | override | Geometric Height of the layer limits above sea level(NN) | m |
| `hsurf` | `geopotential_at_surface` | override+geopotential+units | Geometric Height of the earths surface above sea level | m -> m**2 s**-2 |
| `lai` | `leaf_area_index` | grib | Leaf Area Index | Numeric |
| `level` | `pressure_level` | coordinate | pressure | hPa |
| `model_level_half` | `model_half_level` | coordinate | original GRIB coordinate for key: level(generalVertical) | 1 |
| `p` | `pressure` | grib | Pressure | Pa |
| `plcov` | `vegetation` | grib | Vegetation | % |
| `pmsl` | `mean_sea_level_pressure` | override | Pressure reduced to MSL | Pa |
| `ps` | `surface_pressure` | override | Surface pressure | Pa |
| `q_model_level` | `specific_humidity_on_model_levels` | clash | Specific humidity | kg kg**-1 |
| `q_pressure_level` | `specific_humidity_on_pressure_levels` | clash | Specific humidity | kg kg**-1 |
| `qc` | `specific_cloud_liquid_water_content` | override | Cloud mixing ratio | kg kg**-1 |
| `qc_dia` | `specific_cloud_water_content` | grib | Specific cloud water content (diagnostic) | kg kg-1 |
| `qg` | `specific_graupel_content` | override | Graupel (snow pellets) | kg kg**-1 |
| `qh` | `hail_mixing_ratio` | grib | Hail  mixing ratio | kg kg-1 |
| `qi` | `specific_cloud_ice_water_content` | override | Cloud Ice Mixing Ratio | kg kg-1 |
| `qi_dia` | `specific_cloud_ice_content` | grib | Specific cloud ice content (diagnostic)  | kg kg-1 |
| `qr` | `specific_rain_water_content` | override | Rain mixing ratio | kg kg**-1 |
| `qs` | `specific_snow_water_content` | override | Snow mixing ratio | kg kg**-1 |
| `qv` | `qv` | clash | Snow mixing ratio | kg kg**-1 |
| `qv_2m` | `2m_specific_humidity` | grib | 2 metre specific humidity | kg kg**-1 |
| `qv_s` | `qv_s` | clash | Specific Humidity (S) | kg kg-1 |
| `relhum_2m` | `2m_relative_humidity` | override | 2 metre relative humidity | % |
| `rootdp` | `root_depth_of_vegetation` | grib | root depth of vegetation | m |
| `srh` | `storm_relative_helicity` | grib | Storm relative helicity | m**2 s**-2 |
| `t_2m` | `2m_temperature` | override | 2 metre temperature | K |
| `t_g` | `skin_temperature` | override | Temperature (G) | K |
| `t_model_level` | `temperature_on_model_levels` | clash | Temperature | K |
| `t_pressure_level` | `temperature_on_pressure_levels` | clash | Temperature | K |
| `tch` | `turbulent_transfer_coefficient_for_heat` | grib | Turbulent transfer coefficient for heat (and Moisture) | Numeric |
| `tcm` | `drag_coefficient` | grib | Drag coefficient | Numeric |
| `td_2m` | `2m_dewpoint_temperature` | override | 2 metre dewpoint temperature | K |
| `tke` | `turbulent_kinetic_energy` | grib | Turbulent kinetic energy | J kg**-1 |
| `tot_prec_d` | `total_precipitation_rate` | grib | Total precipitation rate | kg m**-2 s**-1 |
| `u` | `temperature` | grib | Temperature (G) | K |
| `u_10m` | `u_10m` | clash | 10 metre U wind component | m s**-1 |
| `u_model_level` | `u_component_of_wind_on_model_levels` | clash | U component of wind | m s**-1 |
| `u_pressure_level` | `u_component_of_wind_on_pressure_levels` | clash | U component of wind | m s**-1 |
| `v` | `v` | clash | 10 metre U wind component | m s**-1 |
| `v_10m` | `v_10m` | clash | 10 metre V wind component | m s**-1 |
| `v_model_level` | `v_component_of_wind_on_model_levels` | clash | V component of wind | m s**-1 |
| `v_pressure_level` | `v_component_of_wind_on_pressure_levels` | clash | V component of wind | m s**-1 |
| `vmax_10m` | `10m_wind_gust` | override | Time-maximum 10 metre wind gust | m s**-1 |
| `w` | `w` | clash | 10 metre V wind component | m s**-1 |
| `wshear_u` | `u_component_of_wind_shear_vector_between_two_levels` | grib | U-component of (vertical) wind shear vector between two leve | m s-1 |
| `wshear_v` | `v_component_of_wind_shear_vector_between_two_levels` | grib | V-component of (vertical) wind shear vector between two leve | m s-1 |
| `wz_model_level` | `geometric_vertical_velocity` | grib | Geometric vertical velocity | m s**-1 |
| `z0` | `forecast_surface_roughness` | override | Forecast surface roughness | m |

## bkr/icon/icon_eu_art (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `accdrydepo_dusta` | `accumulated_dry_deposition_of_dust_for_mode_1` | eccodes |  |  |
| `accdrydepo_dustb` | `accumulated_dry_deposition_of_dust_for_mode_2` | eccodes |  |  |
| `accdrydepo_dustc` | `accumulated_dry_deposition_of_dust_for_mode_3` | eccodes |  |  |
| `accemiss_dusta` | `accumulated_dust_emission_for_mode_1` | eccodes |  |  |
| `accemiss_dustb` | `accumulated_dust_emission_for_mode_2` | eccodes |  |  |
| `accemiss_dustc` | `accumulated_dust_emission_for_mode_3` | eccodes |  |  |
| `accsedim_dusta` | `accumulated_sedimentation_of_dust_for_mode_1` | eccodes |  |  |
| `accsedim_dustb` | `accumulated_sedimentation_of_dust_for_mode_2` | eccodes |  |  |
| `accsedim_dustc` | `accumulated_sedimentation_of_dust_for_mode_3` | eccodes |  |  |
| `aer_dust_ml` | `total_soil_dust_aerosol_model_level` | eccodes |  |  |
| `alb_rad` | `surface_albedo` | override |  |  |
| `alb_seaice` | `sea_ice_albedo_diffusive_solar` | eccodes |  |  |
| `alhfl_bs` | `latent_heat_flux_from_bare_soil` | eccodes |  |  |
| `alhfl_s` | `mean_surface_latent_heat_flux` | override |  |  |
| `ashfl_s` | `mean_surface_sensible_heat_flux` | override |  |  |
| `asob_s` | `mean_surface_net_short_wave_radiation_flux` | override |  |  |
| `asob_s_cs` | `mean_surface_net_short_wave_radiation_flux_clear_sky` | override |  |  |
| `asob_t` | `mean_top_net_short_wave_radiation_flux` | override |  |  |
| `aswdifd_s` | `mean_surface_diffuse_short_wave_radiation_flux` | override |  |  |
| `aswdifu_s` | `mean_surface_upward_short_wave_radiation_flux` | override |  |  |
| `aswdir_s` | `mean_surface_direct_short_wave_radiation_flux` | override |  |  |
| `athb_s` | `mean_surface_net_long_wave_radiation_flux` | override |  |  |
| `athb_t` | `mean_top_net_long_wave_radiation_flux` | override |  |  |
| `cape_ml` | `mixed_layer_convective_available_potential_energy` | override |  |  |
| `cape_mu` | `most_unstable_convective_available_potential_energy` | override |  |  |
| `ceil_bsc_dust_ml` | `attenuated_backscatter_from_ground_for_dust_model_level` | eccodes |  |  |
| `clch` | `high_cloud_cover` | override |  |  |
| `clcl` | `low_cloud_cover` | override |  |  |
| `clcm` | `medium_cloud_cover` | override |  |  |
| `clct` | `total_cloud_cover` | override |  |  |
| `clct_mod` | `modified_cloud_cover_for_media` | eccodes |  |  |
| `cldepth` | `modified_cloud_depth_for_media` | eccodes |  |  |
| `den_ml` | `density_model_level` | eccodes |  |  |
| `depth_lk` | `lake_depth` | eccodes |  |  |
| `dust_total_mc_ml` | `diagnostic_total_mass_concentration_of_mineral_dust_aerosol_model_level` | eccodes |  |  |
| `dusta0_ml` | `dusta0_ml` | clash |  |  |
| `dusta_ml` | `dusta_ml` | clash |  |  |
| `dustb0_ml` | `dustb0_ml` | clash |  |  |
| `dustb_ml` | `dustb_ml` | clash |  |  |
| `dustc0_ml` | `dustc0_ml` | clash |  |  |
| `dustc_ml` | `dustc_ml` | clash |  |  |
| `evap_pl` | `evaporation_of_plants` | eccodes |  |  |
| `fr_ice` | `sea_ice_cover` | eccodes |  |  |
| `fr_lake` | `water_fraction` | eccodes |  |  |
| `fr_land` | `land_cover` | eccodes |  |  |
| `freshsnw` | `fresh_snow_factor` | eccodes |  |  |
| `h_ice` | `sea_ice_thickness` | eccodes |  |  |
| `h_ml_lk` | `mixed_layer_depth` | eccodes |  |  |
| `h_snow` | `snow_depth` | override |  |  |
| `hbas_con` | `cloud_base_convective_clouds` | eccodes |  |  |
| `hhl_ml` | `height_of_model_half_levels` | override |  |  |
| `hsnow_max` | `maximum_snow_height_during_contiguous_accumulating_snow_period` | eccodes |  |  |
| `hsurf` | `orography` | override |  |  |
| `htop_con` | `cloud_top_convective_clouds` | eccodes |  |  |
| `htop_dc` | `top_of_dry_convection` | eccodes |  |  |
| `hzerocl` | `zero_degree_level` | override |  |  |
| `isobaricInhPa` | `pressure_level` | coordinate | pressure level | hPa |
| `lai` | `leaf_area_index` | eccodes |  |  |
| `model_level_half` | `model_half_level` | coordinate | ICON model half level, 1 at the model top |  |
| `p_ml` | `pressure_model_level` | eccodes |  |  |
| `plcov` | `plant_cover` | eccodes |  |  |
| `pmsl` | `mean_sea_level_pressure` | override |  |  |
| `ps` | `surface_pressure` | override |  |  |
| `qc_ml` | `specific_cloud_liquid_water_content` | override |  |  |
| `qi_ml` | `specific_cloud_ice_water_content` | override |  |  |
| `qr_ml` | `specific_rain_water_content` | override |  |  |
| `qs_ml` | `specific_snow_water_content` | override |  |  |
| `qv_ml` | `specific_humidity_on_model_levels` | clash |  |  |
| `qv_s` | `specific_humidity` | eccodes |  |  |
| `rain_con` | `convective_rainfall` | override |  |  |
| `rain_gsp` | `large_scale_rainfall` | override |  |  |
| `relhum_2m` | `2m_relative_humidity` | override |  |  |
| `rho_snow` | `snow_density` | eccodes |  |  |
| `rootdp` | `root_depth_of_vegetation` | eccodes |  |  |
| `rstom` | `stomatal_resistance` | eccodes |  |  |
| `sat_bsc_dust_ml` | `attenuated_backscatter_from_satellite_for_dust_model_level` | eccodes |  |  |
| `smi` | `soil_moisture_index` | eccodes |  |  |
| `snoag` | `snow_age` | eccodes |  |  |
| `snow_con` | `convective_snowfall` | override |  |  |
| `snow_gsp` | `large_scale_snowfall` | override |  |  |
| `soiltyp` | `soil_type` | eccodes |  |  |
| `sso_stdh` | `standard_deviation_of_orography` | override |  |  |
| `t_2m` | `2m_temperature` | override |  |  |
| `t_2m_l` | `2m_temperature_restricted_to_land` | eccodes |  |  |
| `t_bot_lk` | `bottom_temperature` | eccodes |  |  |
| `t_g` | `skin_temperature` | override |  |  |
| `t_ice` | `ice_surface_temperature` | eccodes |  |  |
| `t_ml` | `temperature_model_level` | eccodes |  |  |
| `t_mnw_lk` | `mean_temperature_of_the_water_column` | eccodes |  |  |
| `t_snow` | `snow_temperature` | eccodes |  |  |
| `t_so` | `soil_temperature` | override |  |  |
| `t_wml_lk` | `mixed_layer_temperature` | eccodes |  |  |
| `taod_dust` | `total_atmosphere_optical_depth_due_to_mineral_dust_aerosol` | eccodes |  |  |
| `tch` | `turbulent_transfer_coefficient_for_heat` | eccodes |  |  |
| `tcm` | `turbulent_transfer_coefficient_for_impulse` | eccodes |  |  |
| `td_2m` | `2m_dewpoint_temperature` | override |  |  |
| `td_2m_l` | `2m_dewpoint_temperature_restricted_to_land` | eccodes |  |  |
| `tmax_2m` | `max_2m_temperature` | eccodes |  |  |
| `tmin_2m` | `min_2m_temperature` | eccodes |  |  |
| `tot_prec` | `total_precipitation` | override |  |  |
| `tqc` | `total_column_cloud_liquid_water` | override |  |  |
| `tqi` | `total_column_cloud_ice_water` | override |  |  |
| `tqr` | `total_column_integrated_rain` | eccodes |  |  |
| `tqs` | `total_column_integrated_snow` | eccodes |  |  |
| `tqv` | `total_column_water_vapour` | override |  |  |
| `u_10m` | `10m_u_component_of_wind` | override |  |  |
| `u_ml` | `u_component_of_wind_model_level` | eccodes |  |  |
| `ustar` | `friction_velocity` | eccodes |  |  |
| `ustar_thres` | `threshold_friction_velocity` | eccodes |  |  |
| `v_10m` | `10m_v_component_of_wind` | override |  |  |
| `v_ml` | `v_component_of_wind_model_level` | eccodes |  |  |
| `vis` | `visible` | abbreviation |  |  |
| `vmax_10m` | `10m_wind_gust` | override |  |  |
| `w_i` | `water_content_of_interception_store` | eccodes |  |  |
| `w_ml` | `vertical_velocity_model_level` | eccodes |  |  |
| `w_snow` | `snow_water_equivalent` | override |  |  |
| `w_so` | `soil_moisture` | override |  |  |
| `w_so_ice` | `soil_ice_content` | eccodes |  |  |
| `z0` | `forecast_surface_roughness` | override |  |  |

## bkr/icon/icon_global (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alb_rad` | `surface_albedo` | override |  |  |
| `alhfl_s` | `mean_surface_latent_heat_flux` | override |  |  |
| `ashfl_s` | `mean_surface_sensible_heat_flux` | override |  |  |
| `asob_s` | `mean_surface_net_short_wave_radiation_flux` | override |  |  |
| `asob_t` | `mean_top_net_short_wave_radiation_flux` | override |  |  |
| `aswdifd_s` | `mean_surface_diffuse_short_wave_radiation_flux` | override |  |  |
| `aswdifu_s` | `mean_surface_upward_short_wave_radiation_flux` | override |  |  |
| `aswdir_s` | `mean_surface_direct_short_wave_radiation_flux` | override |  |  |
| `athb_s` | `mean_surface_net_long_wave_radiation_flux` | override |  |  |
| `athb_t` | `mean_top_net_long_wave_radiation_flux` | override |  |  |
| `aumfl_s` | `mean_eastward_turbulent_surface_stress` | override |  |  |
| `avmfl_s` | `mean_northward_turbulent_surface_stress` | override |  |  |
| `cape_con` | `convective_available_potential_energy` | eccodes |  |  |
| `cape_ml` | `mixed_layer_convective_available_potential_energy` | override |  |  |
| `clch` | `high_cloud_cover` | override |  |  |
| `clcl` | `low_cloud_cover` | override |  |  |
| `clcm` | `medium_cloud_cover` | override |  |  |
| `clct` | `total_cloud_cover` | override |  |  |
| `clct_mod` | `modified_cloud_cover_for_media` | eccodes |  |  |
| `cldepth` | `modified_cloud_depth_for_media` | eccodes |  |  |
| `fi` | `geopotential` | override |  |  |
| `fr_ice` | `sea_ice_cover` | eccodes |  |  |
| `freshsnw` | `fresh_snow_factor` | eccodes |  |  |
| `h_ice` | `sea_ice_thickness` | eccodes |  |  |
| `h_ml_lk` | `mixed_layer_depth` | eccodes |  |  |
| `h_snow` | `snow_depth` | override |  |  |
| `hbas_con` | `cloud_base_convective_clouds` | eccodes |  |  |
| `htop_con` | `cloud_top_convective_clouds` | eccodes |  |  |
| `htop_dc` | `top_of_dry_convection` | eccodes |  |  |
| `hzerocl` | `zero_degree_level` | override |  |  |
| `isobaricInhPa` | `pressure_level` | coordinate |  |  |
| `pmsl` | `mean_sea_level_pressure` | override |  |  |
| `ps` | `surface_pressure` | override |  |  |
| `qv_s` | `specific_humidity` | eccodes |  |  |
| `rain_con` | `convective_rainfall` | override |  |  |
| `rain_gsp` | `large_scale_rainfall` | override |  |  |
| `relhum` | `relative_humidity` | eccodes |  |  |
| `relhum_2m` | `2m_relative_humidity` | override |  |  |
| `rho_snow` | `snow_density` | eccodes |  |  |
| `runoff_g` | `runoff_g` | clash |  |  |
| `runoff_s` | `runoff_s` | clash |  |  |
| `snow_con` | `convective_snowfall` | override |  |  |
| `snow_gsp` | `large_scale_snowfall` | override |  |  |
| `t_2m` | `2m_temperature` | override |  |  |
| `t_g` | `skin_temperature` | override |  |  |
| `t_ice` | `ice_surface_temperature` | eccodes |  |  |
| `t_s` | `soil_temperature` | eccodes |  |  |
| `t_snow` | `snow_temperature` | eccodes |  |  |
| `tch` | `turbulent_transfer_coefficient_for_heat` | eccodes |  |  |
| `tcm` | `turbulent_transfer_coefficient_for_impulse` | eccodes |  |  |
| `td_2m` | `2m_dewpoint_temperature` | override |  |  |
| `tmax_2m` | `max_2m_temperature` | eccodes |  |  |
| `tmin_2m` | `min_2m_temperature` | eccodes |  |  |
| `tot_prec` | `total_precipitation` | override |  |  |
| `tqc` | `total_column_cloud_liquid_water` | override |  |  |
| `tqi` | `total_column_cloud_ice_water` | override |  |  |
| `tqr` | `total_column_integrated_rain` | eccodes |  |  |
| `tqs` | `total_column_integrated_snow` | eccodes |  |  |
| `tqv` | `total_column_water_vapour` | override |  |  |
| `u` | `u_component_of_wind` | eccodes |  |  |
| `u_10m` | `10m_u_component_of_wind` | override |  |  |
| `v` | `v_component_of_wind` | eccodes |  |  |
| `v_10m` | `10m_v_component_of_wind` | override |  |  |
| `values` | `cell` | coordinate |  |  |
| `vmax_10m` | `10m_wind_gust` | override |  |  |
| `w_snow` | `snow_water_equivalent` | override |  |  |
| `ww` | `weather_interpretation` | eccodes |  |  |
| `z0` | `forecast_surface_roughness` | override |  |  |

## bkr/icon/icon_global_art (2 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `accdrydepo_dusta` | `accumulated_dry_deposition_of_dust_for_mode_1` | eccodes |  |  |
| `accdrydepo_dustb` | `accumulated_dry_deposition_of_dust_for_mode_2` | eccodes |  |  |
| `accdrydepo_dustc` | `accumulated_dry_deposition_of_dust_for_mode_3` | eccodes |  |  |
| `accemiss_dusta` | `accumulated_dust_emission_for_mode_1` | eccodes |  |  |
| `accemiss_dustb` | `accumulated_dust_emission_for_mode_2` | eccodes |  |  |
| `accemiss_dustc` | `accumulated_dust_emission_for_mode_3` | eccodes |  |  |
| `accsedim_dusta` | `accumulated_sedimentation_of_dust_for_mode_1` | eccodes |  |  |
| `accsedim_dustb` | `accumulated_sedimentation_of_dust_for_mode_2` | eccodes |  |  |
| `accsedim_dustc` | `accumulated_sedimentation_of_dust_for_mode_3` | eccodes |  |  |
| `aer_dust_ml` | `total_soil_dust_aerosol_model_level` | eccodes |  |  |
| `alb_rad` | `surface_albedo` | override |  |  |
| `alb_seaice` | `sea_ice_albedo_diffusive_solar` | eccodes |  |  |
| `alhfl_bs` | `latent_heat_flux_from_bare_soil` | eccodes |  |  |
| `alhfl_s` | `mean_surface_latent_heat_flux` | override |  |  |
| `ashfl_s` | `mean_surface_sensible_heat_flux` | override |  |  |
| `asob_s` | `mean_surface_net_short_wave_radiation_flux` | override |  |  |
| `asob_s_cs` | `mean_surface_net_short_wave_radiation_flux_clear_sky` | override |  |  |
| `asob_t` | `mean_top_net_short_wave_radiation_flux` | override |  |  |
| `aswdifd_s` | `mean_surface_diffuse_short_wave_radiation_flux` | override |  |  |
| `aswdifu_s` | `mean_surface_upward_short_wave_radiation_flux` | override |  |  |
| `aswdir_s` | `mean_surface_direct_short_wave_radiation_flux` | override |  |  |
| `athb_s` | `mean_surface_net_long_wave_radiation_flux` | override |  |  |
| `athb_t` | `mean_top_net_long_wave_radiation_flux` | override |  |  |
| `aumfl_s` | `mean_eastward_turbulent_surface_stress` | override |  |  |
| `avmfl_s` | `mean_northward_turbulent_surface_stress` | override |  |  |
| `cape_con` | `convective_available_potential_energy` | eccodes |  |  |
| `cape_ml` | `mixed_layer_convective_available_potential_energy` | override |  |  |
| `ceil_bsc_dust_ml` | `attenuated_backscatter_from_ground_for_dust_model_level` | eccodes |  |  |
| `clch` | `high_cloud_cover` | override |  |  |
| `clcl` | `low_cloud_cover` | override |  |  |
| `clcm` | `medium_cloud_cover` | override |  |  |
| `clct` | `total_cloud_cover` | override |  |  |
| `clct_mod` | `modified_cloud_cover_for_media` | eccodes |  |  |
| `cldepth` | `modified_cloud_depth_for_media` | eccodes |  |  |
| `depth_lk` | `lake_depth` | eccodes |  |  |
| `dust_total_mc_ml` | `diagnostic_total_mass_concentration_of_mineral_dust_aerosol_model_level` | eccodes |  |  |
| `dusta0_ml` | `dusta0_ml` | clash |  |  |
| `dusta_ml` | `dusta_ml` | clash |  |  |
| `dustb0_ml` | `dustb0_ml` | clash |  |  |
| `dustb_ml` | `dustb_ml` | clash |  |  |
| `dustc0_ml` | `dustc0_ml` | clash |  |  |
| `dustc_ml` | `dustc_ml` | clash |  |  |
| `evap_pl` | `evaporation_of_plants` | eccodes |  |  |
| `fi` | `geopotential` | override |  |  |
| `fr_ice` | `sea_ice_cover` | eccodes |  |  |
| `fr_lake` | `water_fraction` | eccodes |  |  |
| `fr_land` | `land_cover` | eccodes |  |  |
| `freshsnw` | `fresh_snow_factor` | eccodes |  |  |
| `h_ice` | `sea_ice_thickness` | eccodes |  |  |
| `h_ml_lk` | `mixed_layer_depth` | eccodes |  |  |
| `h_snow` | `snow_depth` | override |  |  |
| `hbas_con` | `cloud_base_convective_clouds` | eccodes |  |  |
| `hhl_ml` | `height_of_model_half_levels` | override |  |  |
| `hsnow_max` | `maximum_snow_height_during_contiguous_accumulating_snow_period` | eccodes |  |  |
| `hsurf` | `orography` | override |  |  |
| `htop_con` | `cloud_top_convective_clouds` | eccodes |  |  |
| `htop_dc` | `top_of_dry_convection` | eccodes |  |  |
| `hzerocl` | `zero_degree_level` | override |  |  |
| `isobaricInhPa` | `pressure_level` | coordinate | pressure level | hPa |
| `lai` | `leaf_area_index` | eccodes |  |  |
| `model_level_half` | `model_half_level` | coordinate | ICON model half level, 1 at the model top |  |
| `ndviratio` | `ratio_of_ndvi_to_annual_maximum` | eccodes |  |  |
| `p_ml` | `pressure_model_level` | eccodes |  |  |
| `plcov` | `plant_cover` | eccodes |  |  |
| `pmsl` | `mean_sea_level_pressure` | override |  |  |
| `ps` | `surface_pressure` | override |  |  |
| `qc_ml` | `specific_cloud_liquid_water_content` | override |  |  |
| `qi_ml` | `specific_cloud_ice_water_content` | override |  |  |
| `qr_ml` | `specific_rain_water_content` | override |  |  |
| `qs_ml` | `specific_snow_water_content` | override |  |  |
| `qv_ml` | `specific_humidity_on_model_levels` | clash |  |  |
| `qv_s` | `specific_humidity` | eccodes |  |  |
| `rain_con` | `convective_rainfall` | override |  |  |
| `rain_gsp` | `large_scale_rainfall` | override |  |  |
| `relhum` | `relative_humidity` | eccodes |  |  |
| `relhum_2m` | `2m_relative_humidity` | override |  |  |
| `rho_snow` | `snow_density` | eccodes |  |  |
| `rootdp` | `root_depth_of_vegetation` | eccodes |  |  |
| `rstom` | `stomatal_resistance` | eccodes |  |  |
| `runoff_g` | `runoff_g` | clash |  |  |
| `runoff_s` | `runoff_s` | clash |  |  |
| `sat_bsc_dust_ml` | `attenuated_backscatter_from_satellite_for_dust_model_level` | eccodes |  |  |
| `smi` | `soil_moisture_index` | eccodes |  |  |
| `snoag` | `snow_age` | eccodes |  |  |
| `snow_con` | `convective_snowfall` | override |  |  |
| `snow_gsp` | `large_scale_snowfall` | override |  |  |
| `soiltyp` | `soil_type` | eccodes |  |  |
| `sso_stdh` | `standard_deviation_of_orography` | override |  |  |
| `t_2m` | `2m_temperature` | override |  |  |
| `t_2m_l` | `2m_temperature_restricted_to_land` | eccodes |  |  |
| `t_bot_lk` | `bottom_temperature` | eccodes |  |  |
| `t_g` | `skin_temperature` | override |  |  |
| `t_ice` | `ice_surface_temperature` | eccodes |  |  |
| `t_ml` | `temperature_model_level` | eccodes |  |  |
| `t_mnw_lk` | `mean_temperature_of_the_water_column` | eccodes |  |  |
| `t_snow` | `snow_temperature` | eccodes |  |  |
| `t_so` | `soil_temperature` | override |  |  |
| `t_wml_lk` | `mixed_layer_temperature` | eccodes |  |  |
| `taod_dust` | `total_atmosphere_optical_depth_due_to_mineral_dust_aerosol` | eccodes |  |  |
| `tch` | `turbulent_transfer_coefficient_for_heat` | eccodes |  |  |
| `tcm` | `turbulent_transfer_coefficient_for_impulse` | eccodes |  |  |
| `td_2m` | `2m_dewpoint_temperature` | override |  |  |
| `td_2m_l` | `2m_dewpoint_temperature_restricted_to_land` | eccodes |  |  |
| `tmax_2m` | `max_2m_temperature` | eccodes |  |  |
| `tmin_2m` | `min_2m_temperature` | eccodes |  |  |
| `tot_prec` | `total_precipitation` | override |  |  |
| `tqc` | `total_column_cloud_liquid_water` | override |  |  |
| `tqc_dia` | `total_column_integrated_cloud_water` | eccodes |  |  |
| `tqi` | `total_column_cloud_ice_water` | override |  |  |
| `tqi_dia` | `total_column_integrated_cloud_ice` | eccodes |  |  |
| `tqr` | `total_column_integrated_rain` | eccodes |  |  |
| `tqs` | `total_column_integrated_snow` | eccodes |  |  |
| `tqv` | `total_column_water_vapour` | override |  |  |
| `u_10m` | `10m_u_component_of_wind` | override |  |  |
| `u_ml` | `u_component_of_wind_model_level` | eccodes |  |  |
| `ustar` | `friction_velocity` | eccodes |  |  |
| `ustar_thres` | `threshold_friction_velocity` | eccodes |  |  |
| `v_10m` | `10m_v_component_of_wind` | override |  |  |
| `v_ml` | `v_component_of_wind_model_level` | eccodes |  |  |
| `vmax_10m` | `10m_wind_gust` | override |  |  |
| `w_i` | `water_content_of_interception_store` | eccodes |  |  |
| `w_ml` | `vertical_velocity_model_level` | eccodes |  |  |
| `w_snow` | `snow_water_equivalent` | override |  |  |
| `w_so` | `soil_moisture` | override |  |  |
| `w_so_ice` | `soil_ice_content` | eccodes |  |  |
| `ww` | `weather_interpretation` | eccodes |  |  |
| `z0` | `forecast_surface_roughness` | override |  |  |

## bkr/icon/icon_global_model_level (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alb_rad` | `surface_albedo` | override | Forecast albedo | % |
| `alhfl_s` | `mean_surface_latent_heat_flux` | override | Time-mean surface latent heat flux | W m**-2 |
| `ashfl_s` | `mean_surface_sensible_heat_flux` | override | Time-mean surface sensible heat flux | W m**-2 |
| `asob_s` | `mean_surface_net_short_wave_radiation_flux` | override | Time-mean surface net short-wave radiation flux | W m**-2 |
| `asob_t` | `mean_top_net_short_wave_radiation_flux` | override | Time-mean top net short-wave radiation flux | W m**-2 |
| `aswdifd_s` | `mean_surface_diffuse_short_wave_radiation_flux` | override | Downward diffusive short wave radiation flux at surface (mea | W m-2 |
| `aswdifu_s` | `mean_surface_upward_short_wave_radiation_flux` | override | Upward diffusive short wave radiation flux at surface ( mean | W m-2 |
| `aswdir_s` | `mean_surface_direct_short_wave_radiation_flux` | override | Downward direct short wave radiation flux at surface (mean o | W m-2 |
| `athb_s` | `mean_surface_net_long_wave_radiation_flux` | override | Time-mean surface net long-wave radiation flux | W m**-2 |
| `athb_t` | `mean_top_net_long_wave_radiation_flux` | override | Time-mean top net long-wave radiation flux | W m**-2 |
| `aumfl_s` | `mean_eastward_turbulent_surface_stress` | override | Momentum Flux, U-Component (m) | N m-2 |
| `avmfl_s` | `mean_northward_turbulent_surface_stress` | override | Momentum Flux, V-Component (m) | N m-2 |
| `cape_con` | `convective_available_potential_energy` | grib | Convective Available Potential Energy | J kg-1 |
| `cape_ml` | `mixed_layer_convective_available_potential_energy` | override | Convective Available Potential Energy, mean layer | J kg-1 |
| `clc` | `cloud_cover` | grib | Cloud cover | % |
| `clch` | `high_cloud_cover` | override+units | Cloud Cover (0 - 400 hPa) | % -> (0 - 1) |
| `clcl` | `low_cloud_cover` | override+units | Cloud Cover (800 hPa - Soil) | % -> (0 - 1) |
| `clcm` | `medium_cloud_cover` | override+units | Cloud Cover (400 - 800 hPa) | % -> (0 - 1) |
| `clct` | `total_cloud_cover` | override+units | Total Cloud Cover | % -> (0 - 1) |
| `clct_mod` | `modified_cloud_cover_for_media` | grib | Modified cloud cover for media | Numeric |
| `cldepth` | `modified_cloud_depth_for_media` | grib | Modified cloud depth for media | Numeric |
| `fr_ice` | `sea_ice_area_fraction` | grib | Sea ice area fraction | (0 - 1) |
| `freshsnw` | `fresh_snow_factor` | grib | Fresh snow factor (weighting function for albedo indicating  | Numeric |
| `h_ice` | `sea_ice_thickness` | grib | Sea ice thickness | m |
| `h_ml_lk` | `mixed_layer_depth` | grib | Mixed-layer depth | m |
| `h_snow` | `snow_depth` | override | Snow depth | m |
| `hbas_con` | `height_of_convective_cloud_base_above_msl` | grib | Height of Convective Cloud Base above msl | m |
| `htop_con` | `height_of_convective_cloud_top_above_msl` | grib | Height of Convective Cloud Top above msl | m |
| `htop_dc` | `height_of_top_of_dry_convection_above_msl` | grib | Height of top of dry convection above MSL | m |
| `hzerocl` | `zero_degree_level` | override | Height of 0 degree Celsius isotherm above msl | m |
| `model_level_half` | `model_half_level` | coordinate | original GRIB coordinate for key: level(generalVertical) | 1 |
| `p` | `pressure` | grib | Pressure | Pa |
| `pmsl` | `mean_sea_level_pressure` | override | Pressure reduced to MSL | Pa |
| `ps` | `surface_pressure` | override | Surface pressure | Pa |
| `qc` | `specific_cloud_liquid_water_content` | override | Cloud mixing ratio | kg kg**-1 |
| `qi` | `specific_cloud_ice_water_content` | override | Cloud Ice Mixing Ratio | kg kg-1 |
| `qv` | `qv` | clash | Specific humidity | kg kg**-1 |
| `qv_s` | `qv_s` | clash | Specific Humidity (S) | kg kg-1 |
| `rain_con` | `convective_rainfall` | override | Convective rain rate | kg m**-2 s**-1 |
| `rain_gsp` | `large_scale_rainfall` | override | Large scale rain rate | kg m**-2 s**-1 |
| `relhum_2m` | `2m_relative_humidity` | override | 2 metre relative humidity | % |
| `rho_snow` | `snow_density` | grib | Snow density | kg m**-3 |
| `runoff_g` | `runoff_g` | clash | Water Runoff | kg m-2 |
| `runoff_s` | `runoff_s` | clash | Water Runoff (s) | kg m-2 |
| `snow_con` | `convective_snowfall` | override | Convective snowfall water equivalent | kg m**-2 |
| `snow_gsp` | `large_scale_snowfall` | override | Large-scale snowfall water equivalent | kg m**-2 |
| `t_2m` | `2m_temperature` | override | 2 metre temperature | K |
| `t_g` | `skin_temperature` | override | Temperature (G) | K |
| `t_ice` | `sea_ice_temperature` | grib | Sea ice temperature | K |
| `t_snow` | `snow_temperature` | grib | Snow temperature (top of snow) | K |
| `tch` | `turbulent_transfer_coefficient_for_heat` | grib | Turbulent transfer coefficient for heat (and Moisture) | Numeric |
| `tcm` | `drag_coefficient` | grib | Drag coefficient | Numeric |
| `td_2m` | `2m_dewpoint_temperature` | override | 2 metre dewpoint temperature | K |
| `tke` | `turbulent_kinetic_energy` | grib | Turbulent kinetic energy | J kg**-1 |
| `tmax_2m` | `maximum_2m_temperature` | grib | Time-maximum 2 metre temperature | K |
| `tmin_2m` | `minimum_2m_temperature` | grib | Time-minimum 2 metre temperature | K |
| `tot_prec` | `total_precipitation` | override+units | Total Precipitation | kg m**-2 -> m |
| `tqc` | `total_column_cloud_liquid_water` | override | Total Column-Integrated Cloud Water | kg m-2 |
| `tqi` | `total_column_cloud_ice_water` | override | Total Column-Integrated Cloud Ice | kg m-2 |
| `tqr` | `total_column_integrated_rain` | grib | Total column integrated rain | kg m**-2 |
| `tqs` | `total_column_integrated_snow` | grib | Total column integrated snow | kg m**-2 |
| `tqv` | `total_column_water_vapour` | override | Total column integrated water vapour | kg m-2 |
| `u` | `u_component_of_wind` | grib | U component of wind | m s**-1 |
| `u_10m` | `10m_u_component_of_wind` | override | 10 metre U wind component | m s**-1 |
| `v` | `v_component_of_wind` | grib | V component of wind | m s**-1 |
| `v_10m` | `10m_v_component_of_wind` | override | 10 metre V wind component | m s**-1 |
| `vmax_10m` | `10m_wind_gust` | override | Time-maximum 10 metre wind gust | m s**-1 |
| `w` | `geometric_vertical_velocity` | grib | Geometric vertical velocity | m s**-1 |
| `w_snow` | `snow_water_equivalent` | override | Snow depth water equivalent | kg m**-2 |
| `ww` | `weather_interpretation` | grib | weather interpretation (WMO) | Numeric |
| `z0` | `forecast_surface_roughness` | override | Forecast surface roughness | m |

## bkr/icon/icon_global_model_level_half_heights_static (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `HHL` | `geometric_height_of_the_layer_limits_above_sea_level` | grib | Geometric Height of the layer limits above sea level(NN) | m |
| `model_level_half` | `model_half_level` | coordinate | original GRIB coordinate for key: level(generalVertical) | 1 |

## bkr/ifs/hres_analysis (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `100_metre_u_wind_component` | `100m_u_component_of_wind` | long_name | 100 metre U wind component | m s**-1 |
| `100_metre_v_wind_component` | `100m_v_component_of_wind` | long_name | 100 metre V wind component | m s**-1 |
| `10_metre_u_wind_component` | `10m_u_component_of_wind` | long_name | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component` | `10m_v_component_of_wind` | long_name | 10 metre V wind component | m s**-1 |
| `2_metre_dewpoint_temperature` | `2m_dewpoint_temperature` | long_name | 2 metre dewpoint temperature | K |
| `2_metre_temperature` | `2m_temperature` | long_name | 2 metre temperature | K |
| `albedo` | `albedo` | kept | Albedo | (0-1) |
| `level` | `pressure_level` | coordinate | pressure level | hPa |
| `sea-ice_cover` | `sea_ice_cover` | long_name | Sea-ice cover | (0-1) |

## bkr/ifs/hres_analysis_025deg (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `100_metre_u_wind_component_sfc` | `100m_u_component_of_wind` | grib | 100 metre U wind component | m s**-1 |
| `100_metre_v_wind_component_sfc` | `100m_v_component_of_wind` | grib | 100 metre V wind component | m s**-1 |
| `10_metre_u_wind_component_sfc` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_sfc` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `2_metre_dewpoint_temperature_sfc` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_temperature_sfc` | `2m_temperature` | grib | 2 metre temperature | K |
| `albedo_climatological_sfc` | `albedo` | grib | Albedo (climatological) | (0 - 1) |
| `angle_of_sub-gridscale_orography_sfc` | `angle_of_sub_gridscale_orography` | grib | Angle of sub-gridscale orography | radians |
| `anisotropy_of_sub-gridscale_orography_sfc` | `anisotropy_of_sub_gridscale_orography` | grib | Anisotropy of sub-gridscale orography | Numeric |
| `charnock_sfc` | `charnock` | grib | Charnock | Numeric |
| `depthBelowLandLayer` | `soil_depth` | long_name | soil depth | m |
| `geopotential_sfc` | `geopotential_at_surface` | clash | Geopotential | m**2 s**-2 |
| `high_cloud_cover_sfc` | `high_cloud_cover` | grib | High cloud cover | (0 - 1) |
| `ice_temperature_layer_1_sfc` | `ice_temperature_layer_1` | grib | Ice temperature layer 1 | K |
| `ice_temperature_layer_2_sfc` | `ice_temperature_layer_2` | grib | Ice temperature layer 2 | K |
| `ice_temperature_layer_3_sfc` | `ice_temperature_layer_3` | grib | Ice temperature layer 3 | K |
| `ice_temperature_layer_4_sfc` | `ice_temperature_layer_4` | grib | Ice temperature layer 4 | K |
| `isobaricInhPa` | `pressure_level` | coordinate | pressure | hPa |
| `leaf_area_index,_high_vegetation_sfc` | `leaf_area_index_high_vegetation` | grib | Leaf area index, high vegetation | m**2 m**-2 |
| `leaf_area_index,_low_vegetation_sfc` | `leaf_area_index_low_vegetation` | grib | Leaf area index, low vegetation | m**2 m**-2 |
| `logarithm_of_surface_roughness_length_for_heat_climatological_sfc` | `logarithm_of_surface_roughness_length_for_heat` | grib | Logarithm of surface roughness length for heat (climatologic | Numeric |
| `low_cloud_cover_sfc` | `low_cloud_cover` | grib | Low cloud cover | (0 - 1) |
| `mean_sea_level_pressure_sfc` | `mean_sea_level_pressure` | grib | Mean sea level pressure | Pa |
| `medium_cloud_cover_sfc` | `medium_cloud_cover` | grib | Medium cloud cover | (0 - 1) |
| `number` | `ensemble_member` | coordinate | ensemble member numerical id | 1 |
| `sea_ice_area_fraction_sfc` | `sea_ice_area_fraction` | grib | Sea ice area fraction | (0 - 1) |
| `sea_surface_temperature_sfc` | `sea_surface_temperature` | grib | Sea surface temperature | K |
| `skin_reservoir_content_sfc` | `skin_reservoir_content` | grib | Skin reservoir content | m of water equivalent |
| `skin_temperature_sfc` | `skin_temperature` | grib | Skin temperature | K |
| `slope_of_sub-gridscale_orography_sfc` | `slope_of_sub_gridscale_orography` | grib | Slope of sub-gridscale orography | Numeric |
| `snow_albedo_sfc` | `snow_albedo` | grib | Snow albedo | (0 - 1) |
| `snow_density_sfc` | `snow_density` | grib | Snow density | kg m**-3 |
| `snow_depth_sfc` | `snow_depth` | grib | Snow depth | m of water equivalent |
| `soil_temperature_level_1_sfc` | `soil_temperature_level_1` | grib | Soil temperature level 1 | K |
| `soil_temperature_level_2_sfc` | `soil_temperature_level_2` | grib | Soil temperature level 2 | K |
| `soil_temperature_level_3_sfc` | `soil_temperature_level_3` | grib | Soil temperature level 3 | K |
| `soil_temperature_level_4_sfc` | `soil_temperature_level_4` | grib | Soil temperature level 4 | K |
| `soil_type_sfc` | `soil_type` | grib | Soil type | (Code table 4.213) |
| `standard_deviation_of_filtered_subgrid_orography_climatological_sfc` | `standard_deviation_of_filtered_subgrid_orography` | grib | Standard deviation of filtered subgrid orography (climatolog | m |
| `standard_deviation_of_sub-gridscale_orography_sfc` | `standard_deviation_of_sub_gridscale_orography` | grib | Standard deviation of sub-gridscale orography | m |
| `surface` | `original_grib_coordinate_for_key_level` | long_name | original GRIB coordinate for key: level(surface) | 1 |
| `surface_pressure_sfc` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_roughness_climatological_sfc` | `surface_roughness` | grib | Surface roughness (climatological) | m |
| `temperature_of_snow_layer_sfc` | `temperature_of_snow_layer` | grib | Temperature of snow layer | K |
| `total_cloud_cover_sfc` | `total_cloud_cover` | grib | Total cloud cover | (0 - 1) |
| `total_column_ozone_sfc` | `total_column_ozone` | grib | Total column ozone | kg m**-2 |
| `total_column_vertically-integrated_water_vapour_sfc` | `total_column_vertically_integrated_water_vapour` | grib | Total column vertically-integrated water vapour | kg m**-2 |
| `total_column_water_sfc` | `total_column_water` | grib | Total column water | kg m**-2 |
| `volumetric_soil_water_layer_1_sfc` | `volumetric_soil_water_layer_1` | grib | Volumetric soil water layer 1 | m**3 m**-3 |
| `volumetric_soil_water_layer_2_sfc` | `volumetric_soil_water_layer_2` | grib | Volumetric soil water layer 2 | m**3 m**-3 |
| `volumetric_soil_water_layer_3_sfc` | `volumetric_soil_water_layer_3` | grib | Volumetric soil water layer 3 | m**3 m**-3 |
| `volumetric_soil_water_layer_4_sfc` | `volumetric_soil_water_layer_4` | grib | Volumetric soil water layer 4 | m**3 m**-3 |

## bkr/ifs/hres_analysis_1deg (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `100_metre_u_wind_component_sfc` | `100m_u_component_of_wind` | grib | 100 metre U wind component | m s**-1 |
| `100_metre_v_wind_component_sfc` | `100m_v_component_of_wind` | grib | 100 metre V wind component | m s**-1 |
| `10_metre_u_wind_component_sfc` | `10m_u_component_of_wind` | grib | 10 metre U wind component | m s**-1 |
| `10_metre_v_wind_component_sfc` | `10m_v_component_of_wind` | grib | 10 metre V wind component | m s**-1 |
| `2_metre_dewpoint_temperature_sfc` | `2m_dewpoint_temperature` | grib | 2 metre dewpoint temperature | K |
| `2_metre_temperature_sfc` | `2m_temperature` | grib | 2 metre temperature | K |
| `albedo_climatological_sfc` | `albedo` | grib | Albedo (climatological) | (0 - 1) |
| `angle_of_sub-gridscale_orography_sfc` | `angle_of_sub_gridscale_orography` | grib | Angle of sub-gridscale orography | radians |
| `anisotropy_of_sub-gridscale_orography_sfc` | `anisotropy_of_sub_gridscale_orography` | grib | Anisotropy of sub-gridscale orography | Numeric |
| `charnock_sfc` | `charnock` | grib | Charnock | Numeric |
| `depthBelowLandLayer` | `soil_depth` | long_name | soil depth | m |
| `geopotential_sfc` | `geopotential_at_surface` | clash | Geopotential | m**2 s**-2 |
| `high_cloud_cover_sfc` | `high_cloud_cover` | grib | High cloud cover | (0 - 1) |
| `ice_temperature_layer_1_sfc` | `ice_temperature_layer_1` | grib | Ice temperature layer 1 | K |
| `ice_temperature_layer_2_sfc` | `ice_temperature_layer_2` | grib | Ice temperature layer 2 | K |
| `ice_temperature_layer_3_sfc` | `ice_temperature_layer_3` | grib | Ice temperature layer 3 | K |
| `ice_temperature_layer_4_sfc` | `ice_temperature_layer_4` | grib | Ice temperature layer 4 | K |
| `isobaricInhPa` | `pressure_level` | coordinate | pressure | hPa |
| `leaf_area_index,_high_vegetation_sfc` | `leaf_area_index_high_vegetation` | grib | Leaf area index, high vegetation | m**2 m**-2 |
| `leaf_area_index,_low_vegetation_sfc` | `leaf_area_index_low_vegetation` | grib | Leaf area index, low vegetation | m**2 m**-2 |
| `logarithm_of_surface_roughness_length_for_heat_climatological_sfc` | `logarithm_of_surface_roughness_length_for_heat` | grib | Logarithm of surface roughness length for heat (climatologic | Numeric |
| `low_cloud_cover_sfc` | `low_cloud_cover` | grib | Low cloud cover | (0 - 1) |
| `mean_sea_level_pressure_sfc` | `mean_sea_level_pressure` | grib | Mean sea level pressure | Pa |
| `medium_cloud_cover_sfc` | `medium_cloud_cover` | grib | Medium cloud cover | (0 - 1) |
| `number` | `ensemble_member` | coordinate | ensemble member numerical id | 1 |
| `sea_ice_area_fraction_sfc` | `sea_ice_area_fraction` | grib | Sea ice area fraction | (0 - 1) |
| `sea_surface_temperature_sfc` | `sea_surface_temperature` | grib | Sea surface temperature | K |
| `skin_reservoir_content_sfc` | `skin_reservoir_content` | grib | Skin reservoir content | m of water equivalent |
| `skin_temperature_sfc` | `skin_temperature` | grib | Skin temperature | K |
| `slope_of_sub-gridscale_orography_sfc` | `slope_of_sub_gridscale_orography` | grib | Slope of sub-gridscale orography | Numeric |
| `snow_albedo_sfc` | `snow_albedo` | grib | Snow albedo | (0 - 1) |
| `snow_density_sfc` | `snow_density` | grib | Snow density | kg m**-3 |
| `snow_depth_sfc` | `snow_depth` | grib | Snow depth | m of water equivalent |
| `soil_temperature_level_1_sfc` | `soil_temperature_level_1` | grib | Soil temperature level 1 | K |
| `soil_temperature_level_2_sfc` | `soil_temperature_level_2` | grib | Soil temperature level 2 | K |
| `soil_temperature_level_3_sfc` | `soil_temperature_level_3` | grib | Soil temperature level 3 | K |
| `soil_temperature_level_4_sfc` | `soil_temperature_level_4` | grib | Soil temperature level 4 | K |
| `soil_type_sfc` | `soil_type` | grib | Soil type | (Code table 4.213) |
| `standard_deviation_of_filtered_subgrid_orography_climatological_sfc` | `standard_deviation_of_filtered_subgrid_orography` | grib | Standard deviation of filtered subgrid orography (climatolog | m |
| `standard_deviation_of_sub-gridscale_orography_sfc` | `standard_deviation_of_sub_gridscale_orography` | grib | Standard deviation of sub-gridscale orography | m |
| `surface` | `original_grib_coordinate_for_key_level` | long_name | original GRIB coordinate for key: level(surface) | 1 |
| `surface_pressure_sfc` | `surface_pressure` | grib | Surface pressure | Pa |
| `surface_roughness_climatological_sfc` | `surface_roughness` | grib | Surface roughness (climatological) | m |
| `temperature_of_snow_layer_sfc` | `temperature_of_snow_layer` | grib | Temperature of snow layer | K |
| `total_cloud_cover_sfc` | `total_cloud_cover` | grib | Total cloud cover | (0 - 1) |
| `total_column_ozone_sfc` | `total_column_ozone` | grib | Total column ozone | kg m**-2 |
| `total_column_vertically-integrated_water_vapour_sfc` | `total_column_vertically_integrated_water_vapour` | grib | Total column vertically-integrated water vapour | kg m**-2 |
| `total_column_water_sfc` | `total_column_water` | grib | Total column water | kg m**-2 |
| `volumetric_soil_water_layer_1_sfc` | `volumetric_soil_water_layer_1` | grib | Volumetric soil water layer 1 | m**3 m**-3 |
| `volumetric_soil_water_layer_2_sfc` | `volumetric_soil_water_layer_2` | grib | Volumetric soil water layer 2 | m**3 m**-3 |
| `volumetric_soil_water_layer_3_sfc` | `volumetric_soil_water_layer_3` | grib | Volumetric soil water layer 3 | m**3 m**-3 |
| `volumetric_soil_water_layer_4_sfc` | `volumetric_soil_water_layer_4` | grib | Volumetric soil water layer 4 | m**3 m**-3 |

## bkr/ifs/ifs_native (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `acwh` | `altimeter_corrected_wave_height` | eccodes |  |  |
| `arrc` | `altimeter_range_relative_correction` | eccodes |  |  |
| `awh` | `altimeter_wave_height` | eccodes |  |  |
| `bfi` | `benjamin_feir_index` | eccodes |  |  |
| `cat` | `clear_air_turbulence` | eccodes |  |  |
| `cc` | `fraction_of_cloud_cover` | eccodes |  |  |
| `cdww` | `coefficient_of_drag_with_waves` | eccodes |  |  |
| `ciwc` | `specific_cloud_ice_water_content` | eccodes |  |  |
| `clwc` | `specific_cloud_liquid_water_content` | eccodes |  |  |
| `crwc` | `specific_rain_water_content` | eccodes |  |  |
| `cswc` | `specific_snow_water_content` | eccodes |  |  |
| `d2fd` | `2_d_wave_spectra_log10_of_spectral_density` | long_name | 2D wave spectra, log10 of spectral density | log10(m2 s radian-1) |
| `dwi` | `10m_wind_direction` | eccodes |  |  |
| `dwps` | `wave_spectral_directional_width_for_swell` | eccodes |  |  |
| `dwww` | `wave_spectral_directional_width_for_wind_waves` | eccodes |  |  |
| `etadot` | `eta_coordinate_vertical_velocity` | eccodes |  |  |
| `h1012` | `significant_wave_height_of_all_waves_with_periods_within_the_inclusive_range_from_10_to_12_seconds` | eccodes |  |  |
| `h1214` | `significant_wave_height_of_all_waves_with_periods_within_the_inclusive_range_from_12_to_14_seconds` | eccodes |  |  |
| `h1417` | `significant_wave_height_of_all_waves_with_periods_within_the_inclusive_range_from_14_to_17_seconds` | eccodes |  |  |
| `h1721` | `significant_wave_height_of_all_waves_with_periods_within_the_inclusive_range_from_17_to_21_seconds` | eccodes |  |  |
| `h2125` | `significant_wave_height_of_all_waves_with_periods_within_the_inclusive_range_from_21_to_25_seconds` | eccodes |  |  |
| `h2530` | `significant_wave_height_of_all_waves_with_periods_within_the_inclusive_range_from_25_to_30_seconds` | eccodes |  |  |
| `hmax` | `envelop_maximum_individual_wave_height` | eccodes |  |  |
| `hybrid_a` | `hybrid_level_a_coefficient` | long_name | hybrid level A coefficient | Pa |
| `hybrid_b` | `hybrid_level_b_coefficient` | long_name | hybrid level B coefficient | 1 |
| `lnsp` | `logarithm_of_surface_pressure` | eccodes |  |  |
| `mdts` | `mean_direction_of_total_swell` | eccodes |  |  |
| `mdww` | `mean_direction_of_wind_waves` | eccodes |  |  |
| `mp1` | `mean_wave_period_based_on_first_moment` | eccodes |  |  |
| `mp2` | `mean_zero_crossing_wave_period` | eccodes |  |  |
| `mpts` | `mean_period_of_total_swell` | eccodes |  |  |
| `mpww` | `mean_period_of_wind_waves` | eccodes |  |  |
| `msqs` | `mean_square_slope_of_waves` | eccodes |  |  |
| `mwd` | `mean_wave_direction` | eccodes |  |  |
| `mwd1` | `mean_wave_direction_of_first_swell_partition` | eccodes |  |  |
| `mwd2` | `mean_wave_direction_of_second_swell_partition` | eccodes |  |  |
| `mwd3` | `mean_wave_direction_of_third_swell_partition` | eccodes |  |  |
| `mwp` | `mean_wave_period` | eccodes |  |  |
| `mwp1` | `mean_wave_period_of_first_swell_partition` | eccodes |  |  |
| `mwp2` | `mean_wave_period_of_second_swell_partition` | eccodes |  |  |
| `mwp3` | `mean_wave_period_of_third_swell_partition` | eccodes |  |  |
| `p1ps` | `mean_wave_period_based_on_first_moment_for_swell` | eccodes |  |  |
| `p1ww` | `mean_wave_period_based_on_first_moment_for_wind_waves` | eccodes |  |  |
| `p2ps` | `mean_wave_period_based_on_second_moment_for_swell` | eccodes |  |  |
| `p2ww` | `mean_wave_period_based_on_second_moment_for_wind_waves` | eccodes |  |  |
| `phiaw` | `normalized_energy_flux_into_waves` | eccodes |  |  |
| `phioc` | `normalized_energy_flux_into_ocean` | eccodes |  |  |
| `pl` | `number_of_grid_points_per_latitude_row` | long_name | number of grid points per latitude row |  |
| `pp1d` | `peak_wave_period` | eccodes |  |  |
| `q` | `specific_humidity` | eccodes |  |  |
| `rhoao` | `air_density_over_the_oceans` | eccodes |  |  |
| `sh10` | `significant_wave_height_of_all_waves_with_period_larger_than_10s` | eccodes |  |  |
| `shts` | `significant_height_of_total_swell` | eccodes |  |  |
| `shww` | `significant_height_of_wind_waves` | eccodes |  |  |
| `stcmax` | `space_time_maximum_individual_crest_height` | eccodes |  |  |
| `sthmax` | `space_time_maximum_individual_wave_height` | eccodes |  |  |
| `swh` | `significant_height_of_combined_wind_waves_and_swell` | eccodes |  |  |
| `swh1` | `significant_wave_height_of_first_swell_partition` | eccodes |  |  |
| `swh2` | `significant_wave_height_of_second_swell_partition` | eccodes |  |  |
| `swh3` | `significant_wave_height_of_third_swell_partition` | eccodes |  |  |
| `tauoc` | `normalized_stress_into_ocean` | eccodes |  |  |
| `tdcmax` | `domain_maximum_individual_crest_height` | eccodes |  |  |
| `tdhmax` | `domain_maximum_individual_wave_height` | eccodes |  |  |
| `tmax` | `period_corresponding_to_maximum_individual_wave_height` | eccodes |  |  |
| `u` | `u_component_of_wind` | eccodes |  |  |
| `ust` | `u_component_surface_stokes_drift` | eccodes |  |  |
| `utaua` | `u_component_of_atmospheric_surface_momentum_flux` | eccodes |  |  |
| `utauo` | `u_component_of_surface_momentum_flux_into_ocean` | eccodes |  |  |
| `v` | `v_component_of_wind` | eccodes |  |  |
| `vst` | `v_component_surface_stokes_drift` | eccodes |  |  |
| `vtaua` | `v_component_of_atmospheric_surface_momentum_flux` | eccodes |  |  |
| `vtauo` | `v_component_of_surface_momentum_flux_into_ocean` | eccodes |  |  |
| `w` | `vertical_velocity` | eccodes |  |  |
| `wdw` | `wave_spectral_directional_width` | eccodes |  |  |
| `wefxd` | `wave_energy_flux_mean_direction` | eccodes |  |  |
| `wefxm` | `wave_energy_flux_magnitude` | eccodes |  |  |
| `weta` | `wave_induced_mean_sea_level_correction` | eccodes |  |  |
| `wind` | `10m_wind_speed` | eccodes |  |  |
| `wmb` | `model_bathymetry` | eccodes |  |  |
| `wnslc` | `number_of_events_in_freak_waves_statistics` | eccodes |  |  |
| `wphio` | `wave_turbulent_energy_flux_into_ocean` | eccodes |  |  |
| `wraf` | `ratio_of_wave_angular_and_frequency_width` | eccodes |  |  |
| `wsk` | `wave_spectral_kurtosis` | eccodes |  |  |
| `wsp` | `wave_spectral_peakedness` | eccodes |  |  |
| `wss` | `wave_spectral_skewness` | eccodes |  |  |
| `wstar` | `free_convective_velocity_over_the_oceans` | eccodes |  |  |
| `z` | `geopotential` | eccodes |  |  |

## bkr/imerg/imerg_final (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `precipitationQualityIndex` | `precipitation_quality_index` | tidied |  |  |
| `probabilityLiquidPrecipitation` | `probability_liquid_precipitation` | tidied |  | percent |
| `randomError` | `random_error` | tidied |  | mm/hr |

## bkr/marine/copernicus_global_ocean_forecast (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `change_in_sea_floor_height_above_reference_ellipsoid_due_to_ocean_tide_loading` | `tide_loading_elevation` | long_name | Tide loading elevation | m |
| `change_in_sea_surface_height_due_to_change_in_air_pressure` | `invert_barometer` | long_name | Invert barometer | m |
| `eastward_sea_water_velocity` | `eastward_surface_velocity` | long_name | Eastward surface velocity | m s-1 |
| `northward_sea_water_velocity` | `northward_surface_velocity` | long_name | Northward surface velocity | m s-1 |
| `sea_surface_height_above_geoid` | `sea_surface_height` | long_name | Sea surface height | m |
| `sea_surface_height_above_geoid_due_to_ocean_tide` | `ocean_tide_elevation` | long_name | Ocean tide elevation | m |
| `sea_surface_wave_stokes_drift_x_velocity` | `eastward_wave_induced_velocity` | long_name | Eastward wave-induced velocity (Stokes drift) | m s-1 |
| `sea_surface_wave_stokes_drift_y_velocity` | `northward_wave_induced_velocity` | long_name | Northward wave-induced velocity (Stokes drift) | m s-1 |
| `sea_water_potential_temperature` | `sea_surface_temperature` | long_name | sea_surface_temperature | degrees_C |
| `sea_water_salinity` | `sea_surface_salinity` | long_name | sea surface salinity | 1e-3 |
| `surface_sea_water_x_velocity` | `eastward_total_velocity` | long_name | Eastward total velocity (Eulerian + Waves + Tide) | m s-1 |
| `surface_sea_water_x_velocity_due_to_tide` | `eastward_tide_induced_velocity` | long_name | Eastward tide-induced velocity (Tide current) | m s-1 |
| `surface_sea_water_y_velocity` | `northward_total_velocity` | long_name | Northward total velocity (Eulerian + Waves + Tide)  | m s-1 |
| `surface_sea_water_y_velocity_due_to_tide` | `northward_tide_induced_velocity` | long_name | Northward tide-induced velocity (Tide current) | m s-1 |

## bkr/marine/copernicus_global_ocean_forecast_6h (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `eastward_sea_water_velocity` | `eastward_velocity` | long_name | Eastward velocity | m s-1 |
| `northward_sea_water_velocity` | `northward_velocity` | long_name | Northward velocity | m s-1 |

## bkr/marine/copernicus_wave_reanalysis (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `sea_surface_primary_swell_wave_from_direction` | `mean_primary_swell_wave_direction_from` | long_name | Mean primary swell wave direction from | degree |
| `sea_surface_primary_swell_wave_mean_period` | `spectral_moments_primary_swell_wave_period` | long_name | Spectral moments (0,1) primary swell wave period | s |
| `sea_surface_primary_swell_wave_significant_height` | `spectral_significant_primary_swell_wave_height` | long_name | Spectral significant primary swell wave height | m |
| `sea_surface_secondary_swell_wave_from_direction` | `mean_secondary_swell_wave_direction_from` | long_name | Mean secondary swell wave direction from | degree |
| `sea_surface_secondary_swell_wave_mean_period` | `spectral_moments_secondary_swell_wave_period` | long_name | Spectral moments (0,1) secondary swell wave period | s |
| `sea_surface_secondary_swell_wave_significant_height` | `spectral_significant_secondary_swell_wave_height` | long_name | Spectral significant secondary swell wave height | m |
| `sea_surface_wave_from_direction` | `mean_wave_direction` | cf | Mean wave direction from (Mdir) | degree |
| `sea_surface_wave_from_direction_at_variance_spectral_density_maximum` | `wave_principal_direction_at_spectral_peak` | long_name | Wave principal direction at spectral peak | degree |
| `sea_surface_wave_mean_period_from_variance_spectral_density_inverse_frequency_moment` | `sea_surface_wave_mean_period_from_variance_spectral_density_inverse_frequency_moment` | clash | Spectral moments (-1,0) wave period (Tm-10) | s |
| `sea_surface_wave_mean_period_from_variance_spectral_density_second_frequency_moment` | `sea_surface_wave_mean_period_from_variance_spectral_density_second_frequency_moment` | clash | Spectral moments (0,2) wave period (Tm02) | s |
| `sea_surface_wave_period_at_variance_spectral_density_maximum` | `wave_period_at_spectral_peak_peak_period` | long_name | Wave period at spectral peak / peak period (Tp) | s |
| `sea_surface_wave_significant_height` | `significant_height_of_combined_wind_waves_and_swell` | cf | Spectral significant wave height (Hm0) | m |
| `sea_surface_wave_stokes_drift_x_velocity` | `stokes_drift_u` | long_name | Stokes drift U | m s-1 |
| `sea_surface_wave_stokes_drift_y_velocity` | `stokes_drift_v` | long_name | Stokes drift V | m s-1 |
| `sea_surface_wind_wave_from_direction` | `mean_wind_wave_direction_from` | long_name | Mean wind wave direction from | degree |
| `sea_surface_wind_wave_mean_period` | `spectral_moments_wind_wave_period` | long_name | Spectral moments (0,1) wind wave period | s |
| `sea_surface_wind_wave_significant_height` | `spectral_significant_wind_wave_height` | long_name | Spectral significant wind wave height | m |

## bkr/metoffice/metoffice_global_deterministic_10km (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_pressure_at_sea_level` | `mean_sea_level_pressure` | cf |  | Pa |
| `air_temperature` | `temperature` | cf |  | K |
| `air_temperature_1.5m` | `1_5m_temperature` | cf |  | K |
| `air_temperature_1.5m_max_1h` | `1_5m_temperature_max_1h` | cf |  | K |
| `air_temperature_1.5m_min_1h` | `1_5m_temperature_min_1h` | cf |  | K |
| `atmosphere_convective_available_potential_energy_below_500hPa` | `atmosphere_convective_available_potential_energy_below_500hpa` | clash | atmosphere_convective_available_potential_energy_assuming_mo | J kg-1 |
| `atmosphere_convective_available_potential_energy_lowest_500m` | `atmosphere_convective_available_potential_energy_lowest_500m` | clash | atmosphere_convective_available_potential_energy_assuming_mi | J kg-1 |
| `atmosphere_convective_inhibition_below_500hPa` | `atmosphere_convective_inhibition_below_500hpa` | clash | atmosphere_convective_inhibition_assuming_most_unstable_parc | J kg-1 |
| `atmosphere_convective_inhibition_lowest_500m` | `atmosphere_convective_inhibition_lowest_500m` | clash | atmosphere_convective_inhibition_assuming_mixed_layer_parcel | J kg-1 |
| `cloud_area_fraction` | `total_cloud_cover` | cf |  | 1 |
| `convective_rainfall_rate_max_1h` | `convective_rainfall_rate_max_1h` | clash |  | m s-1 |
| `dew_point_temperature_1.5m` | `1_5m_dewpoint_temperature` | cf |  | K |
| `fog_area_fraction_1.5m` | `fog_area_fraction` | cf |  | 1 |
| `geopotential_height` | `geopotential` | cf+geopotential+units |  | m -> m**2 s**-2 |
| `high_type_cloud_area_fraction` | `high_cloud_cover` | cf |  | 1 |
| `low_type_cloud_area_fraction` | `low_cloud_cover` | cf |  | 1 |
| `lwe_convective_snowfall_rate_max_1h` | `lwe_convective_snowfall_rate_max_1h` | clash |  | m s-1 |
| `lwe_convective_snowfall_rate_mean_1h` | `lwe_convective_snowfall_rate_mean_1h` | clash |  | m s-1 |
| `medium_type_cloud_area_fraction` | `medium_cloud_cover` | cf |  | 1 |
| `pressure` | `pressure_level` | coordinate | pressure | Pa |
| `relative_humidity_1.5m` | `1_5m_relative_humidity` | cf |  | 1 |
| `surface_upward_latent_heat_flux_mean_1h` | `surface_upward_latent_heat_flux` | cf |  | W m-2 |
| `upward_air_velocity` | `vertical_velocity` | cf |  | m s-1 |
| `visibility_in_air_1.5m` | `1_5m_visibility` | cf |  | m |
| `wind_from_direction` | `wind_direction` | cf |  | degrees |
| `wind_from_direction_10.0m` | `10m_wind_direction` | cf | wind_direction_adjusted_by_model_surface_scheme | degrees |
| `wind_speed_10.0m` | `10m_wind_speed` | cf | wind_speed_adjusted_by_model_surface_scheme | m s-1 |
| `wind_speed_of_gust_10.0m` | `10m_wind_gust` | cf |  | m s-1 |
| `wind_speed_of_gust_10.0m_max_1h` | `10m_wind_gust_max_1h` | cf |  | m s-1 |

## bkr/metoffice/metoffice_global_deterministic_10km_11hour (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_pressure_at_sea_level` | `mean_sea_level_pressure` | cf |  | Pa |
| `air_temperature` | `temperature` | cf |  | K |
| `air_temperature_1.5m` | `1_5m_temperature` | cf |  | K |
| `air_temperature_1.5m_max_1h` | `1_5m_temperature_max_1h` | cf |  | K |
| `air_temperature_1.5m_min_1h` | `1_5m_temperature_min_1h` | cf |  | K |
| `atmosphere_convective_available_potential_energy_below_500hPa` | `atmosphere_convective_available_potential_energy_below_500hpa` | clash | atmosphere_convective_available_potential_energy_assuming_mo | J kg-1 |
| `atmosphere_convective_available_potential_energy_lowest_500m` | `atmosphere_convective_available_potential_energy_lowest_500m` | clash | atmosphere_convective_available_potential_energy_assuming_mi | J kg-1 |
| `atmosphere_convective_inhibition_below_500hPa` | `atmosphere_convective_inhibition_below_500hpa` | clash | atmosphere_convective_inhibition_assuming_most_unstable_parc | J kg-1 |
| `atmosphere_convective_inhibition_lowest_500m` | `atmosphere_convective_inhibition_lowest_500m` | clash | atmosphere_convective_inhibition_assuming_mixed_layer_parcel | J kg-1 |
| `cloud_area_fraction` | `total_cloud_cover` | cf |  | 1 |
| `convective_rainfall_rate_max_1h` | `convective_rainfall_rate_max_1h` | clash |  | m s-1 |
| `dew_point_temperature_1.5m` | `1_5m_dewpoint_temperature` | cf |  | K |
| `fog_area_fraction_1.5m` | `fog_area_fraction` | cf |  | 1 |
| `geopotential_height` | `geopotential` | cf+geopotential+units |  | m -> m**2 s**-2 |
| `high_type_cloud_area_fraction` | `high_cloud_cover` | cf |  | 1 |
| `low_type_cloud_area_fraction` | `low_cloud_cover` | cf |  | 1 |
| `lwe_convective_snowfall_rate_max_1h` | `lwe_convective_snowfall_rate_max_1h` | clash |  | m s-1 |
| `lwe_convective_snowfall_rate_mean_1h` | `lwe_convective_snowfall_rate_mean_1h` | clash |  | m s-1 |
| `medium_type_cloud_area_fraction` | `medium_cloud_cover` | cf |  | 1 |
| `pressure` | `pressure_level` | coordinate | pressure | Pa |
| `relative_humidity_1.5m` | `1_5m_relative_humidity` | cf |  | 1 |
| `surface_upward_latent_heat_flux_mean_1h` | `surface_upward_latent_heat_flux` | cf |  | W m-2 |
| `upward_air_velocity` | `vertical_velocity` | cf |  | m s-1 |
| `visibility_in_air_1.5m` | `1_5m_visibility` | cf |  | m |
| `wind_from_direction` | `wind_direction` | cf |  | degrees |
| `wind_from_direction_10.0m` | `10m_wind_direction` | cf | wind_direction_adjusted_by_model_surface_scheme | degrees |
| `wind_speed_10.0m` | `10m_wind_speed` | cf | wind_speed_adjusted_by_model_surface_scheme | m s-1 |
| `wind_speed_of_gust_10.0m` | `10m_wind_gust` | cf |  | m s-1 |
| `wind_speed_of_gust_10.0m_max_1h` | `10m_wind_gust_max_1h` | cf |  | m s-1 |

## bkr/metoffice/metoffice_global_deterministic_10km_6hourly_24hr (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_pressure_at_sea_level` | `mean_sea_level_pressure` | cf |  | Pa |
| `air_temperature_1.5m` | `1_5m_temperature` | cf |  | K |
| `atmosphere_convective_available_potential_energy_below_500hPa` | `atmosphere_convective_available_potential_energy_below_500hpa` | clash | atmosphere_convective_available_potential_energy_assuming_mo | J kg-1 |
| `atmosphere_convective_available_potential_energy_lowest_500m` | `atmosphere_convective_available_potential_energy_lowest_500m` | clash | atmosphere_convective_available_potential_energy_assuming_mi | J kg-1 |
| `atmosphere_convective_inhibition_below_500hPa` | `atmosphere_convective_inhibition_below_500hpa` | clash | atmosphere_convective_inhibition_assuming_most_unstable_parc | J kg-1 |
| `atmosphere_convective_inhibition_lowest_500m` | `atmosphere_convective_inhibition_lowest_500m` | clash | atmosphere_convective_inhibition_assuming_mixed_layer_parcel | J kg-1 |
| `cloud_area_fraction` | `total_cloud_cover` | cf |  | 1 |
| `cloud_volume_fraction_in_atmosphere_layer_[5.00e+00 1.00e+01 2.00e+01 3.00e+01 5.00e+01 7.50e+01 1.00e+02 1.50e+02
 2.00e+02 2.50e+02 3.00e+02 4.00e+02 5.00e+02 6.00e+02 7.00e+02 8.00e+02
 1.00e+03 1.25e+03 1.50e+03 1.75e+03 2.00e+03 2.25e+03 2.50e+03 2.75e+03
 3.00e+03 3.25e+03 3.50e+03 3.75e+03 4.00e+03 4.50e+03 5.00e+03 5.50e+03
 6.00e+03]m` | `cloud_volume_fraction_in_atmosphere_layer` | long_name | cloud_volume_fraction_in_atmosphere_layer | 1 |
| `dew_point_temperature_1.5m` | `1_5m_dewpoint_temperature` | cf |  | K |
| `fog_area_fraction_1.5m` | `fog_area_fraction` | cf |  | 1 |
| `geopotential_height` | `geopotential` | cf+geopotential+units |  | m -> m**2 s**-2 |
| `high_type_cloud_area_fraction` | `high_cloud_cover` | cf |  | 1 |
| `low_type_cloud_area_fraction` | `low_cloud_cover` | cf |  | 1 |
| `medium_type_cloud_area_fraction` | `medium_cloud_cover` | cf |  | 1 |
| `pressure` | `pressure_level` | coordinate | pressure | Pa |
| `relative_humidity_1.5m` | `1_5m_relative_humidity` | cf |  | 1 |
| `upward_air_velocity` | `vertical_velocity` | cf |  | m s-1 |
| `visibility_in_air_1.5m` | `1_5m_visibility` | cf |  | m |
| `wind_from_direction` | `wind_direction` | cf |  | degrees |
| `wind_from_direction_10.0m` | `10m_wind_direction` | cf | wind_direction_adjusted_by_model_surface_scheme | degrees |
| `wind_speed_10.0m` | `10m_wind_speed` | cf | wind_speed_adjusted_by_model_surface_scheme | m s-1 |
| `wind_speed_of_gust_10.0m` | `10m_wind_gust` | cf |  | m s-1 |

## bkr/metoffice/metoffice_global_hourly_ocean_depth_analysis (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `sea_ice_area_fraction` | `sea_ice_cover` | cf | Sea ice area fraction | 1 |

## bkr/metoffice/metoffice_global_wave (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `mean_wave_direction_from_mdir` | `mean_wave_direction_mean` | cf | Mean wave direction from (Mdir) | degree |
| `spectral_moments_0,1_wave_period_tm01` | `spectral_moments_wave_period` | long_name | Spectral moments (0,1) wave period (Tm01) | s |
| `spectral_moments_0,2_wave_period_tm02` | `spectral_moments_wave_period_tm02` | long_name | Spectral moments (0,2) wave period Tm02 | s |
| `spectral_significant_wave_height_hm0` | `significant_height_of_combined_wind_waves_and_swell` | cf | Spectral significant wave height (Hm0) | m |
| `wave_period_at_spectral_peak___peak_period_tp` | `wave_period_at_spectral_peak_peak_period` | long_name | Wave period at spectral peak / peak period (Tp) | s |
| `wind_direction_from` | `wind_direction` | cf | Wind direction (from) | degrees |

## bkr/metoffice/metoffice_nws_ocean_depth_hourly (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `eastward_current_velocity_in_the_water_column_cur` | `eastward_current_velocity_in_the_water_column` | long_name | Eastward Current Velocity in the Water Column | m s-1 |
| `northward_current_velocity_in_the_water_column_cur` | `northward_current_velocity_in_the_water_column` | long_name | Northward Current Velocity in the Water Column | m s-1 |
| `sea_water_potential_temperature_tem` | `sea_water_potential_temperature` | long_name | Sea Water Potential Temperature | degrees_C |
| `sea_water_salinity_sal` | `sea_water_salinity` | long_name | Sea Water Salinity | 1e-3 |

## bkr/metoffice/metoffice_nws_ocean_surface_hourly (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `_depth_max_current_in_the_water_column` | `depth_max_current_in_the_water_column` | long_name |  Depth Max Current in the Water Column | m |
| `eastward_current_velocity_in_the_water_column_ssc` | `eastward_current_velocity_in_the_water_column` | long_name | Eastward Current Velocity in the Water Column | m s-1 |
| `northward_current_velocity_in_the_water_column_ssc` | `northward_current_velocity_in_the_water_column` | long_name | Northward Current Velocity in the Water Column | m s-1 |
| `ocean_mixed_layer_thickness_defined_by_sigma_theta_reference_depth_of_3m,_dt_crit_of__0.2` | `ocean_mixed_layer_thickness_defined_by_sigma_theta` | long_name | Ocean mixed layer thickness defined by sigma theta (referenc | m |
| `sea_water_potential_temperature_sst` | `sea_water_potential_temperature` | long_name | Sea Water Potential Temperature | degrees_C |
| `sea_water_salinity_sss` | `sea_water_salinity` | long_name | Sea Water Salinity | 1e-3 |

## bkr/metoffice/metoffice_nws_wave (3 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `mean_wave_direction_from_mdir` | `mean_wave_direction_mean` | cf | Mean wave direction from (Mdir) | degree |
| `spectral_moments_0,1_wave_period_tm01` | `spectral_moments_wave_period` | long_name | Spectral moments (0,1) wave period (Tm01) | s |
| `spectral_moments_0,2_wave_period_tm02` | `spectral_moments_wave_period_tm02` | long_name | Spectral moments (0,2) wave period Tm02 | s |
| `spectral_significant_wave_height_hm0` | `significant_height_of_combined_wind_waves_and_swell` | cf | Spectral significant wave height (Hm0) | m |
| `wave_period_at_spectral_peak___peak_period_tp` | `wave_period_at_spectral_peak_peak_period` | long_name | Wave period at spectral peak / peak period (Tp) | s |
| `wind_direction_from` | `wind_direction` | cf | Wind direction (from) | degrees |

## bkr/metoffice/metoffice_uk_deterministic_2km (7 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_pressure_at_sea_level` | `mean_sea_level_pressure` | cf |  | Pa |
| `air_temperature` | `temperature` | cf |  | K |
| `air_temperature_1.5m` | `1_5m_temperature` | cf |  | K |
| `air_temperature_on_height_levels` | `temperature_on_height_levels` | clash |  | K |
| `cloud_area_fraction` | `total_cloud_cover` | cf |  | 1 |
| `cloud_volume_fraction_in_atmosphere_layer_on_height_levels` | `cloud_volume_fraction_in_atmosphere_layer` | long_name | cloud_volume_fraction_in_atmosphere_layer | 1 |
| `dew_point_temperature_1.5m` | `1_5m_dewpoint_temperature` | cf |  | K |
| `fog_area_fraction_1.5m` | `fog_area_fraction` | cf |  | 1 |
| `geopotential_height` | `geopotential` | cf+geopotential+units |  | m -> m**2 s**-2 |
| `height` | `height_above_ground` | coordinate |  | m |
| `high_type_cloud_area_fraction` | `high_cloud_cover` | cf |  | 1 |
| `land_binary_mask` | `land_sea_mask` | cf |  | 1 |
| `low_type_cloud_area_fraction` | `low_cloud_cover` | cf |  | 1 |
| `medium_type_cloud_area_fraction` | `medium_cloud_cover` | cf |  | 1 |
| `pressure` | `pressure_level` | coordinate | pressure | Pa |
| `projection_x_coordinate` | `x` | coordinate |  | m |
| `projection_y_coordinate` | `y` | coordinate |  | m |
| `relative_humidity_1.5m` | `1_5m_relative_humidity` | cf |  | 1 |
| `surface_air_pressure` | `surface_pressure` | cf |  | Pa |
| `visibility_in_air_1.5m` | `1_5m_visibility` | cf |  | m |
| `wind_from_direction` | `wind_direction` | cf |  | degrees |
| `wind_from_direction_10.0m` | `10m_wind_direction` | cf | wind_direction_adjusted_by_model_surface_scheme | degrees |
| `wind_from_direction_on_height_levels` | `wind_direction_on_height_levels` | clash |  | degrees |
| `wind_speed_10.0m` | `10m_wind_speed` | cf | wind_speed_adjusted_by_model_surface_scheme | m s-1 |
| `wind_speed_of_gust_10.0m` | `10m_wind_gust` | cf |  | m s-1 |
| `wind_speed_on_height_levels` | `wind_speed_on_height_levels` | clash |  | m s-1 |

## bkr/metoffice/metoffice_uk_deterministic_2km_20082025 (7 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_pressure_at_sea_level` | `mean_sea_level_pressure` | cf |  | Pa |
| `air_temperature_1.5m` | `1_5m_temperature` | cf |  | K |
| `cloud_area_fraction` | `total_cloud_cover` | cf |  | 1 |
| `cloud_volume_fraction_in_atmosphere_layer_on_height_levels` | `cloud_volume_fraction_in_atmosphere_layer` | long_name | cloud_volume_fraction_in_atmosphere_layer | 1 |
| `dew_point_temperature_1.5m` | `1_5m_dewpoint_temperature` | cf |  | K |
| `fog_area_fraction_1.5m` | `fog_area_fraction` | cf |  | 1 |
| `height` | `height_above_ground` | coordinate |  | m |
| `high_type_cloud_area_fraction` | `high_cloud_cover` | cf |  | 1 |
| `land_binary_mask` | `land_sea_mask` | cf |  | 1 |
| `low_type_cloud_area_fraction` | `low_cloud_cover` | cf |  | 1 |
| `medium_type_cloud_area_fraction` | `medium_cloud_cover` | cf |  | 1 |
| `pressure` | `pressure_level` | coordinate | pressure | Pa |
| `projection_x_coordinate` | `x` | coordinate |  | m |
| `projection_y_coordinate` | `y` | coordinate |  | m |
| `relative_humidity_1.5m` | `1_5m_relative_humidity` | cf |  | 1 |
| `surface_air_pressure` | `surface_pressure` | cf |  | Pa |
| `visibility_in_air_1.5m` | `1_5m_visibility` | cf |  | m |
| `wind_from_direction` | `wind_direction` | cf |  | degrees |
| `wind_from_direction_10.0m` | `10m_wind_direction` | cf | wind_direction_adjusted_by_model_surface_scheme | degrees |
| `wind_from_direction_on_height_levels` | `wind_direction_on_height_levels` | clash |  | degrees |
| `wind_speed_10.0m` | `10m_wind_speed` | cf | wind_speed_adjusted_by_model_surface_scheme | m s-1 |
| `wind_speed_of_gust_10.0m` | `10m_wind_gust` | cf |  | m s-1 |
| `wind_speed_on_height_levels` | `wind_speed` | cf |  | m s-1 |

## bkr/metoffice/metoffice_uk_deterministic_2km_21082025 (4 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_pressure_at_sea_level` | `mean_sea_level_pressure` | cf |  | Pa |
| `air_temperature` | `temperature` | cf |  | K |
| `air_temperature_1.5m` | `1_5m_temperature` | cf |  | K |
| `air_temperature_on_height_levels` | `temperature_on_height_levels` | clash |  | K |
| `atmosphere_convective_available_potential_energy_below_500hPa` | `atmosphere_convective_available_potential_energy` | cf | atmosphere_convective_available_potential_energy_assuming_mo | J kg-1 |
| `cloud_area_fraction` | `total_cloud_cover` | cf |  | 1 |
| `cloud_volume_fraction_in_atmosphere_layer_on_height_levels` | `cloud_volume_fraction_in_atmosphere_layer` | long_name | cloud_volume_fraction_in_atmosphere_layer | 1 |
| `dew_point_temperature_1.5m` | `1_5m_dewpoint_temperature` | cf |  | K |
| `fog_area_fraction_1.5m` | `fog_area_fraction` | cf |  | 1 |
| `geopotential_height` | `geopotential` | cf+geopotential+units |  | m -> m**2 s**-2 |
| `height` | `height_above_ground` | coordinate |  | m |
| `high_type_cloud_area_fraction` | `high_cloud_cover` | cf |  | 1 |
| `land_binary_mask` | `land_sea_mask` | cf |  | 1 |
| `low_type_cloud_area_fraction` | `low_cloud_cover` | cf |  | 1 |
| `medium_type_cloud_area_fraction` | `medium_cloud_cover` | cf |  | 1 |
| `pressure` | `pressure_level` | coordinate | pressure | Pa |
| `projection_x_coordinate` | `x` | coordinate |  | m |
| `projection_y_coordinate` | `y` | coordinate |  | m |
| `relative_humidity_1.5m` | `1_5m_relative_humidity` | cf |  | 1 |
| `surface_air_pressure` | `surface_pressure` | cf |  | Pa |
| `visibility_in_air_1.5m` | `1_5m_visibility` | cf |  | m |
| `wind_from_direction` | `wind_direction` | cf |  | degrees |
| `wind_from_direction_10.0m` | `10m_wind_direction` | cf | wind_direction_adjusted_by_model_surface_scheme | degrees |
| `wind_from_direction_on_height_levels` | `wind_direction_on_height_levels` | clash |  | degrees |
| `wind_speed_of_gust_10.0m` | `10m_wind_gust` | cf |  | m s-1 |
| `wind_speed_on_height_levels` | `wind_speed_on_height_levels` | clash |  | m s-1 |

## bkr/nsrdb/goes_full_disc_v4 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alpha` | `angstrom_exponent` | abbreviation |  | unitless |
| `aod` | `aerosol_optical_depth` | abbreviation |  | unitless |
| `asymmetry` | `aerosol_asymmetry_parameter` | abbreviation |  | unitless |
| `clearsky_dhi` | `clear_sky_diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_dni` | `clear_sky_direct_normal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_ghi` | `clear_sky_global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dew_point` | `dewpoint_temperature` | abbreviation+units |  | Celsius -> K |
| `dhi` | `diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dni` | `direct_normal_irradiance` | abbreviation |  | W/m2 |
| `ghi` | `global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `ozone` | `ozone` | kept |  | atm-cm |
| `ssa` | `aerosol_single_scattering_albedo` | abbreviation |  | unitless |
| `wind_speed_10m` | `wind_speed_at_10m_above_the_surface` | long_name |  | m/s |

## bkr/nsrdb/himawari7 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alpha` | `angstrom_exponent` | abbreviation |  | unitless |
| `aod` | `aerosol_optical_depth` | abbreviation |  | unitless |
| `asymmetry` | `aerosol_asymmetry_parameter` | abbreviation |  | unitless |
| `clearsky_dhi` | `clear_sky_diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_dni` | `clear_sky_direct_normal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_ghi` | `clear_sky_global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dew_point` | `dewpoint_temperature` | abbreviation+units |  | Celsius -> K |
| `dhi` | `diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dni` | `direct_normal_irradiance` | abbreviation |  | W/m2 |
| `ghi` | `global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `layout_1348x742/alpha` | `layout_1348x742/angstrom_exponent` | abbreviation |  | unitless |
| `layout_1348x742/aod` | `layout_1348x742/aerosol_optical_depth` | abbreviation |  | unitless |
| `layout_1348x742/asymmetry` | `layout_1348x742/aerosol_asymmetry_parameter` | abbreviation |  | unitless |
| `layout_1348x742/clearsky_dhi` | `layout_1348x742/clear_sky_diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `layout_1348x742/clearsky_dni` | `layout_1348x742/clear_sky_direct_normal_irradiance` | abbreviation |  | W/m2 |
| `layout_1348x742/clearsky_ghi` | `layout_1348x742/clear_sky_global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `layout_1348x742/dew_point` | `layout_1348x742/dewpoint_temperature` | abbreviation+units |  | Celsius -> K |
| `layout_1348x742/dhi` | `layout_1348x742/diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `layout_1348x742/dni` | `layout_1348x742/direct_normal_irradiance` | abbreviation |  | W/m2 |
| `layout_1348x742/ghi` | `layout_1348x742/global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `layout_1348x742/ozone` | `layout_1348x742/ozone` | kept |  | atm-cm |
| `layout_1348x742/ssa` | `layout_1348x742/aerosol_single_scattering_albedo` | abbreviation |  | unitless |
| `ozone` | `ozone` | kept |  | atm-cm |
| `ssa` | `aerosol_single_scattering_albedo` | abbreviation |  | unitless |

## bkr/nsrdb/himawari8 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alpha` | `angstrom_exponent` | abbreviation |  | unitless |
| `aod` | `aerosol_optical_depth` | abbreviation |  | unitless |
| `asymmetry` | `aerosol_asymmetry_parameter` | abbreviation |  | unitless |
| `clearsky_dhi` | `clear_sky_diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_dni` | `clear_sky_direct_normal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_ghi` | `clear_sky_global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dew_point` | `dewpoint_temperature` | abbreviation+units |  | Celsius -> K |
| `dhi` | `diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dni` | `direct_normal_irradiance` | abbreviation |  | W/m2 |
| `ghi` | `global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `ozone` | `ozone` | kept |  | atm-cm |
| `ssa` | `aerosol_single_scattering_albedo` | abbreviation |  | unitless |

## bkr/nsrdb/meteosat (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alpha` | `angstrom_exponent` | abbreviation |  | unitless |
| `aod` | `aerosol_optical_depth` | abbreviation |  | unitless |
| `asymmetry` | `aerosol_asymmetry_parameter` | abbreviation |  | unitless |
| `clearsky_dhi` | `clear_sky_diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_dni` | `clear_sky_direct_normal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_ghi` | `clear_sky_global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dew_point` | `dewpoint_temperature` | abbreviation+units |  | Celsius -> K |
| `dhi` | `diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dni` | `direct_normal_irradiance` | abbreviation |  | W/m2 |
| `ghi` | `global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `ozone` | `ozone` | kept |  | atm-cm |
| `ssa` | `aerosol_single_scattering_albedo` | abbreviation |  | unitless |

## bkr/nsrdb/msg_v4 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alpha` | `angstrom_exponent` | abbreviation |  | unitless |
| `aod` | `aerosol_optical_depth` | abbreviation |  | unitless |
| `asymmetry` | `aerosol_asymmetry_parameter` | abbreviation |  | unitless |
| `clearsky_dhi` | `clear_sky_diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_dni` | `clear_sky_direct_normal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_ghi` | `clear_sky_global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dew_point` | `dewpoint_temperature` | abbreviation+units |  | Celsius -> K |
| `dhi` | `diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dni` | `direct_normal_irradiance` | abbreviation |  | W/m2 |
| `ghi` | `global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `ozone` | `ozone` | kept |  | atm-cm |
| `ssa` | `aerosol_single_scattering_albedo` | abbreviation |  | unitless |

## bkr/nsrdb/nsrdb_himawari7 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `dhi` | `diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dni` | `direct_normal_irradiance` | abbreviation |  | W/m2 |
| `ghi` | `global_horizontal_irradiance` | abbreviation |  | W/m2 |

## bkr/nsrdb/nsrdb_india (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `clearsky_dhi` | `clear_sky_diffuse_horizontal_irradiance` | abbreviation |  |  |
| `clearsky_dni` | `clear_sky_direct_normal_irradiance` | abbreviation |  |  |
| `clearsky_ghi` | `clear_sky_global_horizontal_irradiance` | abbreviation |  |  |
| `dew_point` | `dewpoint_temperature` | abbreviation |  |  |
| `dhi` | `diffuse_horizontal_irradiance` | abbreviation |  |  |
| `dni` | `direct_normal_irradiance` | abbreviation |  |  |
| `ghi` | `global_horizontal_irradiance` | abbreviation |  |  |

## bkr/nsrdb/polar_v4 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `air_temperature` | `air_temperature_at_2m_above_the_surface` | long_name |  | Celsius |
| `alpha` | `angstrom_exponent` | abbreviation |  | unitless |
| `aod` | `aerosol_optical_depth` | abbreviation |  | unitless |
| `asymmetry` | `aerosol_asymmetry_parameter` | abbreviation |  | unitless |
| `clearsky_dhi` | `clear_sky_diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_dni` | `clear_sky_direct_normal_irradiance` | abbreviation |  | W/m2 |
| `clearsky_ghi` | `clear_sky_global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dew_point` | `dewpoint_temperature` | abbreviation+units |  | Celsius -> K |
| `dhi` | `diffuse_horizontal_irradiance` | abbreviation |  | W/m2 |
| `dni` | `direct_normal_irradiance` | abbreviation |  | W/m2 |
| `ghi` | `global_horizontal_irradiance` | abbreviation |  | W/m2 |
| `ozone` | `reduced_ozone_vertical_pathlength` | long_name |  | atm-cm |
| `ssa` | `aerosol_single_scattering_albedo` | abbreviation |  | unitless |
| `wind_speed` | `wind_speed_at_2m_above_the_surface` | long_name |  | m/s |
| `wind_speed_10m` | `wind_speed_at_10m_above_the_surface` | long_name |  | m/s |

## bkr/obs/gnss (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `uncertainty_value1` | `random_uncertainty` | long_name | random_uncertainty |  |
| `uncertainty_value5` | `total_uncertainty` | long_name | total_uncertainty |  |

## bkr/obs/goes_east_conus (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `abi01c` | `channel_01_reflectance_0_47um` | channel |  |  |
| `abi02c` | `channel_02_reflectance_0_64um` | channel |  |  |
| `abi03c` | `channel_03_reflectance_0_86um` | channel |  |  |
| `abi04c` | `channel_04_reflectance_1_37um` | channel |  |  |
| `abi05c` | `channel_05_reflectance_1_6um` | channel |  |  |
| `abi06c` | `channel_06_reflectance_2_2um` | channel |  |  |
| `abi07c` | `channel_07_brightness_temperature_3_9um` | channel |  |  |
| `abi08c` | `channel_08_brightness_temperature_6_2um` | channel |  |  |
| `abi09c` | `channel_09_brightness_temperature_6_9um` | channel |  |  |
| `abi10c` | `channel_10_brightness_temperature_7_3um` | channel |  |  |
| `abi11c` | `channel_11_brightness_temperature_8_4um` | channel |  |  |
| `abi12c` | `channel_12_brightness_temperature_9_6um` | channel |  |  |
| `abi13c` | `channel_13_brightness_temperature_10_3um` | channel |  |  |
| `abi14c` | `channel_14_brightness_temperature_11_2um` | channel |  |  |
| `abi15c` | `channel_15_brightness_temperature_12_3um` | channel |  |  |
| `abi16c` | `channel_16_brightness_temperature_13_3um` | channel |  |  |
| `x` | `geostationary_x` | coordinate |  |  |
| `y` | `geostationary_y` | coordinate |  |  |

## bkr/obs/goes_west_pacus (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `abi01c` | `channel_01_reflectance_0_47um` | channel |  |  |
| `abi02c` | `channel_02_reflectance_0_64um` | channel |  |  |
| `abi03c` | `channel_03_reflectance_0_86um` | channel |  |  |
| `abi04c` | `channel_04_reflectance_1_37um` | channel |  |  |
| `abi05c` | `channel_05_reflectance_1_6um` | channel |  |  |
| `abi06c` | `channel_06_reflectance_2_2um` | channel |  |  |
| `abi07c` | `channel_07_brightness_temperature_3_9um` | channel |  |  |
| `abi08c` | `channel_08_brightness_temperature_6_2um` | channel |  |  |
| `abi09c` | `channel_09_brightness_temperature_6_9um` | channel |  |  |
| `abi10c` | `channel_10_brightness_temperature_7_3um` | channel |  |  |
| `abi11c` | `channel_11_brightness_temperature_8_4um` | channel |  |  |
| `abi12c` | `channel_12_brightness_temperature_9_6um` | channel |  |  |
| `abi13c` | `channel_13_brightness_temperature_10_3um` | channel |  |  |
| `abi14c` | `channel_14_brightness_temperature_11_2um` | channel |  |  |
| `abi15c` | `channel_15_brightness_temperature_12_3um` | channel |  |  |
| `abi16c` | `channel_16_brightness_temperature_13_3um` | channel |  |  |
| `x` | `geostationary_x` | coordinate |  |  |
| `y` | `geostationary_y` | coordinate |  |  |

## bkr/obs/igra_v2_16_levels (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `flag_int` | `flag_interpolation` | cf |  | 1 |
| `geopotential_height` | `geopotential` | cf+geopotential+units |  | m -> m**2 s**-2 |
| `level` | `pressure_level` | coordinate |  | Pa |
| `wind_direction` | `wind_to_direction` | cf |  | degree |

## bkr/obs/loon (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `acs` | `acs` | kept |  |  |
| `altitude` | `altitude` | kept |  |  |
| `omega` | `vertical_velocity` | eccodes |  |  |
| `pressure` | `pressure` | kept |  |  |

## bkr/obs/metop_sg_mws (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `L1B_quality_flag` | `l1_b_quality_flag` | tidied |  |  |

## bkr/obs/metop_sg_ro (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `bangle` | `bending_angle` | abbreviation |  |  |
| `impact` | `impact_parameter` | abbreviation |  |  |

## bkr/obs/mrms_conus (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `refc` | `composite_reflectivity` | abbreviation |  |  |
| `refc_base` | `base_reflectivity` | abbreviation |  |  |

## bkr/obs/ncep_adpupa (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `instrument_type` | `prepbufr_instrument_type` | long_name |  |  |
| `message_type` | `prepbufr_message_type_aircar_aircft_or_adpupa` | long_name |  |  |
| `pressure` | `pressure` | kept |  | Pa |
| `report_type` | `prepbufr_report_type_the_mass_type_when_merged_with_winds` | long_name |  |  |

## bkr/obs/ncep_amdar (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `instrument_type` | `prepbufr_instrument_type` | long_name |  |  |
| `message_type` | `prepbufr_message_type_aircar_aircft_or_adpupa` | long_name |  |  |
| `pressure` | `pressure` | kept |  | Pa |
| `report_type` | `prepbufr_report_type_the_mass_type_when_merged_with_winds` | long_name |  |  |

## bkr/obs/sondehub_flights (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `_serial_flight` | `serial_flight` | tidied |  |  |
| `_serial_sorted` | `serial_sorted` | tidied |  |  |
| `battery` | `sonde_battery_voltage` | long_name | sonde battery voltage | V |
| `frame_counter` | `sonde_frame_counter` | long_name | sonde frame counter |  |
| `horizontal_velocity` | `ground_speed` | long_name | ground speed | m s-1 |
| `humidity` | `relative_humidity` | cf |  | % |
| `obs_count` | `number_of_samples` | long_name | number of samples |  |
| `obs_start` | `index_of_this_flight_s_first_sample_in_obs` | long_name | index of this flight's first sample in obs |  |
| `pressure` | `air_pressure` | cf+units |  | hPa -> Pa |
| `receiver_count` | `distinct_receivers_that_uploaded_this_sample` | long_name | distinct receivers that uploaded this sample |  |
| `rssi` | `receiver_signal_strength` | long_name | receiver signal strength | dBm |
| `snr` | `receiver_signal_to_noise_ratio` | long_name | receiver signal-to-noise ratio | dB |
| `type` | `type` | kept |  |  |
| `vertical_velocity` | `ascent_rate` | long_name | ascent rate | m s-1 |

## bkr/observation/sondehub (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `altitude` | `altitude` | kept |  |  |
| `humidity` | `humidity` | kept |  |  |
| `pressure` | `pressure` | kept |  |  |

## bkr/polar/aeolus (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `l1b_measurement_AOCS_pitch_angle` | `l1b_measurement_aocs_pitch_angle` | tidied |  | degrees |
| `l1b_measurement_AOCS_roll_angle` | `l1b_measurement_aocs_roll_angle` | tidied |  | degrees |
| `l1b_measurement_AOCS_yaw_angle` | `l1b_measurement_aocs_yaw_angle` | tidied |  | degrees |
| `l1b_measurement_altitude_of_DEM_intersection` | `l1b_measurement_altitude_of_dem_intersection` | tidied |  | m |
| `l1b_measurement_latitude_of_DEM_intersection` | `l1b_measurement_latitude_of_dem_intersection` | tidied |  | degrees_north |
| `l1b_measurement_longitude_of_DEM_intersection` | `l1b_measurement_longitude_of_dem_intersection` | tidied |  | degrees_east |
| `l1b_measurement_mie_HLOS_wind_speed` | `l1b_measurement_mie_hlos_wind_speed` | tidied |  | m/s |
| `l1b_measurement_mie_SNR` | `l1b_measurement_mie_snr` | tidied |  |  |
| `l1b_measurement_rayleigh_HLOS_wind_speed` | `l1b_measurement_rayleigh_hlos_wind_speed` | tidied |  | m/s |
| `l1b_measurement_rayleigh_SNR` | `l1b_measurement_rayleigh_snr` | tidied |  |  |
| `l1b_measurement_rayleigh_channel_A_SNR` | `l1b_measurement_rayleigh_channel_a_snr` | tidied |  |  |
| `l1b_measurement_rayleigh_channel_B_SNR` | `l1b_measurement_rayleigh_channel_b_snr` | tidied |  |  |
| `l1b_measurement_rayleigh_signal_channel_A_intensity` | `l1b_measurement_rayleigh_signal_channel_a_intensity` | tidied |  | ACCD counts |
| `l1b_measurement_rayleigh_signal_channel_B_intensity` | `l1b_measurement_rayleigh_signal_channel_b_intensity` | tidied |  | ACCD counts |
| `l1b_measurement_velocity_at_DEM_intersection` | `l1b_measurement_velocity_at_dem_intersection` | tidied |  | m/s |
| `l1b_mie_HBE_ground_velocity` | `l1b_mie_hbe_ground_velocity` | tidied |  | m/s |
| `l1b_mie_total_ZWC` | `l1b_mie_total_zwc` | tidied |  | m/s |
| `l1b_observation_AOCS_pitch_angle` | `l1b_observation_aocs_pitch_angle` | tidied |  | degrees |
| `l1b_observation_AOCS_roll_angle` | `l1b_observation_aocs_roll_angle` | tidied |  | degrees |
| `l1b_observation_AOCS_yaw_angle` | `l1b_observation_aocs_yaw_angle` | tidied |  | degrees |
| `l1b_observation_altitude_of_DEM_intersection` | `l1b_observation_altitude_of_dem_intersection` | tidied |  | m |
| `l1b_observation_latitude_of_DEM_intersection` | `l1b_observation_latitude_of_dem_intersection` | tidied |  | degrees_north |
| `l1b_observation_longitude_of_DEM_intersection` | `l1b_observation_longitude_of_dem_intersection` | tidied |  | degrees_east |
| `l1b_observation_mie_HLOS_wind_speed` | `l1b_observation_mie_hlos_wind_speed` | tidied |  | m/s |
| `l1b_observation_mie_SNR` | `l1b_observation_mie_snr` | tidied |  |  |
| `l1b_observation_rayleigh_HLOS_wind_speed` | `l1b_observation_rayleigh_hlos_wind_speed` | tidied |  | m/s |
| `l1b_observation_rayleigh_SNR` | `l1b_observation_rayleigh_snr` | tidied |  |  |
| `l1b_observation_rayleigh_channel_A_SNR` | `l1b_observation_rayleigh_channel_a_snr` | tidied |  |  |
| `l1b_observation_rayleigh_channel_B_SNR` | `l1b_observation_rayleigh_channel_b_snr` | tidied |  |  |
| `l1b_observation_rayleigh_signal_channel_A_intensity` | `l1b_observation_rayleigh_signal_channel_a_intensity` | tidied |  | ACCD counts |
| `l1b_observation_rayleigh_signal_channel_B_intensity` | `l1b_observation_rayleigh_signal_channel_b_intensity` | tidied |  | ACCD counts |
| `l1b_observation_velocity_at_DEM_intersection` | `l1b_observation_velocity_at_dem_intersection` | tidied |  | m/s |
| `l1b_rayleigh_HBE_ground_velocity` | `l1b_rayleigh_hbe_ground_velocity` | tidied |  | m/s |
| `l1b_rayleigh_total_ZWC` | `l1b_rayleigh_total_zwc` | tidied |  | m/s |
| `l2a_L1B_centroid_time_obs` | `l2a_l1_b_centroid_time_obs` | tidied |  | seconds since 2000-01-01 |
| `l2a_L1B_num_of_meas_per_obs` | `l2a_l1_b_num_of_meas_per_obs` | tidied |  |  |
| `l2a_L1B_start_time_obs` | `l2a_l1_b_start_time_obs` | tidied |  | seconds since 2000-01-01 |
| `l2a_L1B_time_meas` | `l2a_l1_b_time_meas` | tidied |  | seconds since 2000-01-01 |
| `l2a_MCA_LOD` | `l2a_mca_lod` | tidied |  |  |
| `l2a_MCA_clim_BER` | `l2a_mca_clim_ber` | tidied |  | sr⁻¹ |
| `l2a_MCA_extinction` | `l2a_mca_extinction` | tidied |  | 10⁻⁶*m⁻¹ |
| `l2a_MCA_time_obs` | `l2a_mca_time_obs` | tidied |  | seconds since 2000-01-01 |
| `l2a_SCA_LOD` | `l2a_sca_lod` | tidied |  |  |
| `l2a_SCA_LOD_variance` | `l2a_sca_lod_variance` | tidied |  |  |
| `l2a_SCA_QC_flag` | `l2a_sca_qc_flag` | tidied |  |  |
| `l2a_SCA_SR` | `l2a_sca_sr` | tidied |  |  |
| `l2a_SCA_attenuated_molecular_backscatter` | `l2a_sca_attenuated_molecular_backscatter` | tidied |  | sr⁻¹*m⁻¹ |
| `l2a_SCA_attenuated_particulate_backscatter` | `l2a_sca_attenuated_particulate_backscatter` | tidied |  | sr⁻¹*m⁻¹ |
| `l2a_SCA_backscatter` | `l2a_sca_backscatter` | tidied |  | 10⁻⁶*m⁻¹* sr⁻¹ |
| `l2a_SCA_backscatter_variance` | `l2a_sca_backscatter_variance` | tidied |  | m⁻²*sr⁻² |
| `l2a_SCA_ber_variance` | `l2a_sca_ber_variance` | tidied |  |  |
| `l2a_SCA_cloud_mask` | `l2a_sca_cloud_mask` | tidied |  |  |
| `l2a_SCA_extinction` | `l2a_sca_extinction` | tidied |  | 10⁻⁶*m⁻¹ |
| `l2a_SCA_extinction_variance` | `l2a_sca_extinction_variance` | tidied |  | m⁻² |
| `l2a_SCA_lr_variance` | `l2a_sca_lr_variance` | tidied |  |  |
| `l2a_SCA_middle_bin_BER` | `l2a_sca_middle_bin_ber` | tidied |  | sr⁻¹ |
| `l2a_SCA_middle_bin_BER_variance` | `l2a_sca_middle_bin_ber_variance` | tidied |  | sr⁻² |
| `l2a_SCA_middle_bin_Kmie` | `l2a_sca_middle_bin_kmie` | tidied |  |  |
| `l2a_SCA_middle_bin_Kray` | `l2a_sca_middle_bin_kray` | tidied |  |  |
| `l2a_SCA_middle_bin_LOD` | `l2a_sca_middle_bin_lod` | tidied |  |  |
| `l2a_SCA_middle_bin_LOD_variance` | `l2a_sca_middle_bin_lod_variance` | tidied |  |  |
| `l2a_SCA_middle_bin_altitude_obs` | `l2a_sca_middle_bin_altitude_obs` | tidied |  | m |
| `l2a_SCA_middle_bin_backscatter` | `l2a_sca_middle_bin_backscatter` | tidied |  | 10⁻⁶*m⁻¹* sr⁻¹ |
| `l2a_SCA_middle_bin_backscatter_variance` | `l2a_sca_middle_bin_backscatter_variance` | tidied |  | m⁻²*sr⁻² |
| `l2a_SCA_middle_bin_cloud_mask` | `l2a_sca_middle_bin_cloud_mask` | tidied |  |  |
| `l2a_SCA_middle_bin_extinction` | `l2a_sca_middle_bin_extinction` | tidied |  | 10⁻⁶*m⁻¹ |
| `l2a_SCA_middle_bin_extinction_variance` | `l2a_sca_middle_bin_extinction_variance` | tidied |  | m⁻² |
| `l2a_SCA_middle_bin_lr_variance` | `l2a_sca_middle_bin_lr_variance` | tidied |  | sr² |
| `l2a_SCA_middle_bin_processing_qc_flag` | `l2a_sca_middle_bin_processing_qc_flag` | tidied |  |  |
| `l2a_SCA_processing_qc_flag` | `l2a_sca_processing_qc_flag` | tidied |  |  |
| `l2a_SCA_time_obs` | `l2a_sca_time_obs` | tidied |  | seconds since 2000-01-01 |
| `l2a_altitude_of_DEM_intersection_meas` | `l2a_altitude_of_dem_intersection_meas` | tidied |  | m |
| `l2a_altitude_of_DEM_intersection_obs` | `l2a_altitude_of_dem_intersection_obs` | tidied |  | m |
| `l2a_group_LOD` | `l2a_group_lod` | tidied |  |  |
| `l2a_group_LOD_variance` | `l2a_group_lod_variance` | tidied |  |  |
| `l2a_group_SR` | `l2a_group_sr` | tidied |  |  |
| `l2a_group_middle_bin_BER_bottom` | `l2a_group_middle_bin_ber_bottom` | tidied |  |  |
| `l2a_group_middle_bin_BER_top` | `l2a_group_middle_bin_ber_top` | tidied |  |  |
| `l2a_group_middle_bin_BER_variance_bottom` | `l2a_group_middle_bin_ber_variance_bottom` | tidied |  | sr⁻² |
| `l2a_group_middle_bin_BER_variance_top` | `l2a_group_middle_bin_ber_variance_top` | tidied |  | sr⁻² |
| `l2a_group_middle_bin_LOD_bottom` | `l2a_group_middle_bin_lod_bottom` | tidied |  |  |
| `l2a_group_middle_bin_LOD_top` | `l2a_group_middle_bin_lod_top` | tidied |  |  |
| `l2a_group_middle_bin_LOD_variance_bottom` | `l2a_group_middle_bin_lod_variance_bottom` | tidied |  |  |
| `l2a_group_middle_bin_LOD_variance_top` | `l2a_group_middle_bin_lod_variance_top` | tidied |  |  |
| `l2a_latitude_of_DEM_intersection_meas` | `l2a_latitude_of_dem_intersection_meas` | tidied |  | degrees_north |
| `l2a_latitude_of_DEM_intersection_obs` | `l2a_latitude_of_dem_intersection_obs` | tidied |  | degrees_north |
| `l2a_longitude_of_DEM_intersection_meas` | `l2a_longitude_of_dem_intersection_meas` | tidied |  | degrees_east |
| `l2a_longitude_of_DEM_intersection_obs` | `l2a_longitude_of_dem_intersection_obs` | tidied |  | degrees_east |
| `l2a_scene_classification_NWP_cloud_flag` | `l2a_scene_classification_nwp_cloud_flag` | tidied |  |  |
| `l2b_l1B_num_of_measurements_per_obs` | `l2b_l1_b_num_of_measurements_per_obs` | tidied |  |  |
| `l2b_l1B_obs_number` | `l2b_l1_b_obs_number` | tidied |  |  |
| `l2b_mie_profile_alt_of_DEM_intersection` | `l2b_mie_profile_alt_of_dem_intersection` | tidied |  | m |
| `l2b_mie_profile_lat_of_DEM_intersection` | `l2b_mie_profile_lat_of_dem_intersection` | tidied |  | degrees_north |
| `l2b_mie_profile_lon_of_DEM_intersection` | `l2b_mie_profile_lon_of_dem_intersection` | tidied |  | degrees_east |
| `l2b_mie_wind_result_COG_altitude` | `l2b_mie_wind_result_cog_altitude` | tidied |  | m |
| `l2b_mie_wind_result_COG_latitude` | `l2b_mie_wind_result_cog_latitude` | tidied |  | degrees_north |
| `l2b_mie_wind_result_COG_longitude` | `l2b_mie_wind_result_cog_longitude` | tidied |  | degrees_east |
| `l2b_mie_wind_result_COG_range` | `l2b_mie_wind_result_cog_range` | tidied |  | m |
| `l2b_mie_wind_result_COG_time` | `l2b_mie_wind_result_cog_time` | tidied |  | seconds since 2000-01-01 |
| `l2b_mie_wind_result_HLOS_error` | `l2b_mie_wind_result_hlos_error` | tidied |  | cm/s |
| `l2b_mie_wind_result_QC_flags_1` | `l2b_mie_wind_result_qc_flags_1` | tidied |  |  |
| `l2b_mie_wind_result_QC_flags_2` | `l2b_mie_wind_result_qc_flags_2` | tidied |  |  |
| `l2b_mie_wind_result_QC_flags_3` | `l2b_mie_wind_result_qc_flags_3` | tidied |  |  |
| `l2b_mie_wind_result_SNR` | `l2b_mie_wind_result_snr` | tidied |  |  |
| `l2b_mie_wind_result_alt_of_DEM_intersection` | `l2b_mie_wind_result_alt_of_dem_intersection` | tidied |  | m |
| `l2b_mie_wind_result_arg_of_lat_of_DEM_intersection` | `l2b_mie_wind_result_arg_of_lat_of_dem_intersection` | tidied |  | 10⁻⁶deg |
| `l2b_mie_wind_result_lat_of_DEM_intersection` | `l2b_mie_wind_result_lat_of_dem_intersection` | tidied |  | degrees_north |
| `l2b_mie_wind_result_lon_of_DEM_intersection` | `l2b_mie_wind_result_lon_of_dem_intersection` | tidied |  | degrees_east |
| `l2b_rayleigh_profile_alt_of_DEM_intersection` | `l2b_rayleigh_profile_alt_of_dem_intersection` | tidied |  | m |
| `l2b_rayleigh_profile_lat_of_DEM_intersection` | `l2b_rayleigh_profile_lat_of_dem_intersection` | tidied |  | degrees_north |
| `l2b_rayleigh_profile_lon_of_DEM_intersection` | `l2b_rayleigh_profile_lon_of_dem_intersection` | tidied |  | degrees_east |
| `l2b_rayleigh_wind_result_COG_altitude` | `l2b_rayleigh_wind_result_cog_altitude` | tidied |  | m |
| `l2b_rayleigh_wind_result_COG_latitude` | `l2b_rayleigh_wind_result_cog_latitude` | tidied |  | degrees_north |
| `l2b_rayleigh_wind_result_COG_longitude` | `l2b_rayleigh_wind_result_cog_longitude` | tidied |  | degrees_east |
| `l2b_rayleigh_wind_result_COG_range` | `l2b_rayleigh_wind_result_cog_range` | tidied |  | m |
| `l2b_rayleigh_wind_result_COG_time` | `l2b_rayleigh_wind_result_cog_time` | tidied |  | seconds since 2000-01-01 |
| `l2b_rayleigh_wind_result_HLOS_error` | `l2b_rayleigh_wind_result_hlos_error` | tidied |  | cm/s |
| `l2b_rayleigh_wind_result_QC_flags_1` | `l2b_rayleigh_wind_result_qc_flags_1` | tidied |  |  |
| `l2b_rayleigh_wind_result_QC_flags_2` | `l2b_rayleigh_wind_result_qc_flags_2` | tidied |  |  |
| `l2b_rayleigh_wind_result_QC_flags_3` | `l2b_rayleigh_wind_result_qc_flags_3` | tidied |  |  |
| `l2b_rayleigh_wind_result_alt_of_DEM_intersection` | `l2b_rayleigh_wind_result_alt_of_dem_intersection` | tidied |  | m |
| `l2b_rayleigh_wind_result_arg_of_lat_of_DEM_intersection` | `l2b_rayleigh_wind_result_arg_of_lat_of_dem_intersection` | tidied |  | 10⁻⁶deg |
| `l2b_rayleigh_wind_result_lat_of_DEM_intersection` | `l2b_rayleigh_wind_result_lat_of_dem_intersection` | tidied |  | degrees_north |
| `l2b_rayleigh_wind_result_lon_of_DEM_intersection` | `l2b_rayleigh_wind_result_lon_of_dem_intersection` | tidied |  | degrees_east |
| `l2c_l1B_measurement_time` | `l2c_l1_b_measurement_time` | tidied |  | seconds since 2000-01-01 |
| `l2c_l1B_num_of_measurements_per_obs` | `l2c_l1_b_num_of_measurements_per_obs` | tidied |  |  |
| `l2c_l1B_obs_number` | `l2c_l1_b_obs_number` | tidied |  |  |
| `l2c_mie_assimilation_L2B_HLOS_reliability` | `l2c_mie_assimilation_l2_b_hlos_reliability` | tidied |  |  |
| `l2c_mie_assimilation_L2B_QC` | `l2c_mie_assimilation_l2_b_qc` | tidied |  |  |
| `l2c_mie_assimilation_analysis_HLOS` | `l2c_mie_assimilation_analysis_hlos` | tidied |  | cm/s |
| `l2c_mie_assimilation_background_HLOS` | `l2c_mie_assimilation_background_hlos` | tidied |  | cm/s |
| `l2c_mie_assimilation_background_HLOS_error` | `l2c_mie_assimilation_background_hlos_error` | tidied |  | cm/s |
| `l2c_mie_assimilation_est_L2B_bias` | `l2c_mie_assimilation_est_l2_b_bias` | tidied |  | cm/s |
| `l2c_mie_profile_alt_of_DEM_intersection` | `l2c_mie_profile_alt_of_dem_intersection` | tidied |  | m |
| `l2c_mie_profile_lat_of_DEM_intersection` | `l2c_mie_profile_lat_of_dem_intersection` | tidied |  | degrees_north |
| `l2c_mie_profile_lon_of_DEM_intersection` | `l2c_mie_profile_lon_of_dem_intersection` | tidied |  | degrees_east |
| `l2c_mie_wind_result_COG_altitude` | `l2c_mie_wind_result_cog_altitude` | tidied |  | m |
| `l2c_mie_wind_result_COG_latitude` | `l2c_mie_wind_result_cog_latitude` | tidied |  | degrees_north |
| `l2c_mie_wind_result_COG_longitude` | `l2c_mie_wind_result_cog_longitude` | tidied |  | degrees_east |
| `l2c_mie_wind_result_COG_range` | `l2c_mie_wind_result_cog_range` | tidied |  | m |
| `l2c_mie_wind_result_COG_time` | `l2c_mie_wind_result_cog_time` | tidied |  | seconds since 2000-01-01 |
| `l2c_mie_wind_result_HLOS_error` | `l2c_mie_wind_result_hlos_error` | tidied |  | cm/s |
| `l2c_mie_wind_result_QC_flags_1` | `l2c_mie_wind_result_qc_flags_1` | tidied |  |  |
| `l2c_mie_wind_result_QC_flags_2` | `l2c_mie_wind_result_qc_flags_2` | tidied |  |  |
| `l2c_mie_wind_result_QC_flags_3` | `l2c_mie_wind_result_qc_flags_3` | tidied |  |  |
| `l2c_mie_wind_result_SNR` | `l2c_mie_wind_result_snr` | tidied |  |  |
| `l2c_mie_wind_result_alt_of_DEM_intersection` | `l2c_mie_wind_result_alt_of_dem_intersection` | tidied |  | m |
| `l2c_mie_wind_result_lat_of_DEM_intersection` | `l2c_mie_wind_result_lat_of_dem_intersection` | tidied |  | degrees_north |
| `l2c_mie_wind_result_lon_of_DEM_intersection` | `l2c_mie_wind_result_lon_of_dem_intersection` | tidied |  | degrees_east |
| `l2c_rayleigh_assimilation_L2B_HLOS_reliability` | `l2c_rayleigh_assimilation_l2_b_hlos_reliability` | tidied |  |  |
| `l2c_rayleigh_assimilation_L2B_QC` | `l2c_rayleigh_assimilation_l2_b_qc` | tidied |  |  |
| `l2c_rayleigh_assimilation_analysis_HLOS` | `l2c_rayleigh_assimilation_analysis_hlos` | tidied |  | cm/s |
| `l2c_rayleigh_assimilation_background_HLOS` | `l2c_rayleigh_assimilation_background_hlos` | tidied |  | cm/s |
| `l2c_rayleigh_assimilation_background_HLOS_error` | `l2c_rayleigh_assimilation_background_hlos_error` | tidied |  | cm/s |
| `l2c_rayleigh_assimilation_est_L2B_bias` | `l2c_rayleigh_assimilation_est_l2_b_bias` | tidied |  | cm/s |
| `l2c_rayleigh_profile_alt_of_DEM_intersection` | `l2c_rayleigh_profile_alt_of_dem_intersection` | tidied |  | m |
| `l2c_rayleigh_profile_lat_of_DEM_intersection` | `l2c_rayleigh_profile_lat_of_dem_intersection` | tidied |  | degrees_north |
| `l2c_rayleigh_profile_lon_of_DEM_intersection` | `l2c_rayleigh_profile_lon_of_dem_intersection` | tidied |  | degrees_east |
| `l2c_rayleigh_wind_result_COG_altitude` | `l2c_rayleigh_wind_result_cog_altitude` | tidied |  | m |
| `l2c_rayleigh_wind_result_COG_latitude` | `l2c_rayleigh_wind_result_cog_latitude` | tidied |  | degrees_north |
| `l2c_rayleigh_wind_result_COG_longitude` | `l2c_rayleigh_wind_result_cog_longitude` | tidied |  | degrees_east |
| `l2c_rayleigh_wind_result_COG_range` | `l2c_rayleigh_wind_result_cog_range` | tidied |  | m |
| `l2c_rayleigh_wind_result_COG_time` | `l2c_rayleigh_wind_result_cog_time` | tidied |  | seconds since 2000-01-01 |
| `l2c_rayleigh_wind_result_HLOS_error` | `l2c_rayleigh_wind_result_hlos_error` | tidied |  | cm/s |
| `l2c_rayleigh_wind_result_QC_flags_1` | `l2c_rayleigh_wind_result_qc_flags_1` | tidied |  |  |
| `l2c_rayleigh_wind_result_QC_flags_2` | `l2c_rayleigh_wind_result_qc_flags_2` | tidied |  |  |
| `l2c_rayleigh_wind_result_QC_flags_3` | `l2c_rayleigh_wind_result_qc_flags_3` | tidied |  |  |
| `l2c_rayleigh_wind_result_alt_of_DEM_intersection` | `l2c_rayleigh_wind_result_alt_of_dem_intersection` | tidied |  | m |
| `l2c_rayleigh_wind_result_lat_of_DEM_intersection` | `l2c_rayleigh_wind_result_lat_of_dem_intersection` | tidied |  | degrees_north |
| `l2c_rayleigh_wind_result_lon_of_DEM_intersection` | `l2c_rayleigh_wind_result_lon_of_dem_intersection` | tidied |  | degrees_east |

## bkr/polar/earthcare (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `ac_tc_2b_ATLID_detection_status` | `ac_tc_2b_atlid_detection_status` | long_name | ATLID detection status |  |
| `ac_tc_2b_ATLID_target_classification` | `ac_tc_2b_atlid_target_classification` | long_name | ATLID target classification |  |
| `ac_tc_2b_ATLID_target_classification_low_resolution` | `ac_tc_2b_low_resolution_of_atlid_target_classification` | long_name | Low resolution of ATLID target classification |  |
| `ac_tc_2b_ATLID_target_classification_medium_resolution` | `ac_tc_2b_atlid_target_classification_medium_resolution` | tidied | Medium resolution of ATLID target classification |  |
| `ac_tc_2b_CPR_ATLID_low_resolution_status` | `ac_tc_2b_cpr_atlid_low_resolution_status` | long_name | CPR ATLID low resolution status |  |
| `ac_tc_2b_CPR_ATLID_medium_resolution_status` | `ac_tc_2b_medium_resolution_cpr_atlid_status` | long_name | Medium resolution CPR ATLID status |  |
| `ac_tc_2b_CPR_ATLID_status` | `ac_tc_2b_cpr_atlid_status` | long_name | CPR ATLID status |  |
| `ac_tc_2b_CPR_detection_status` | `ac_tc_2b_cpr_detection_status` | long_name | CPR detection status |  |
| `ac_tc_2b_CPR_target_classification` | `ac_tc_2b_cpr_target_classification` | long_name | CPR target classification |  |
| `ac_tc_2b_header_ANXLongitude` | `ac_tc_2b_header_anx_longitude` | tidied |  |  |
| `ac_tc_2b_header_ANXTime` | `ac_tc_2b_header_anx_time` | tidied |  |  |
| `ac_tc_2b_header_File_Class` | `ac_tc_2b_header_File_Class` | clash |  |  |
| `ac_tc_2b_header_File_Description` | `ac_tc_2b_header_file_description` | tidied |  |  |
| `ac_tc_2b_header_File_Name` | `ac_tc_2b_header_file_name` | tidied |  |  |
| `ac_tc_2b_header_File_Type` | `ac_tc_2b_header_file_type` | tidied |  |  |
| `ac_tc_2b_header_File_Version` | `ac_tc_2b_header_file_version` | tidied |  |  |
| `ac_tc_2b_header_Mission` | `ac_tc_2b_header_mission` | tidied |  |  |
| `ac_tc_2b_header_Notes` | `ac_tc_2b_header_notes` | tidied |  |  |
| `ac_tc_2b_header_Source_Creation_Date` | `ac_tc_2b_header_source_creation_date` | tidied |  |  |
| `ac_tc_2b_header_Source_Creator` | `ac_tc_2b_header_source_creator` | tidied |  |  |
| `ac_tc_2b_header_Source_Creator_Version` | `ac_tc_2b_header_source_creator_version` | tidied |  |  |
| `ac_tc_2b_header_Source_System` | `ac_tc_2b_header_source_system` | tidied |  |  |
| `ac_tc_2b_header_Validity_Period_Validity_Start` | `ac_tc_2b_header_validity_period_validity_start` | tidied |  |  |
| `ac_tc_2b_header_Validity_Period_Validity_Stop` | `ac_tc_2b_header_validity_period_validity_stop` | tidied |  |  |
| `ac_tc_2b_header_acquisitionStation` | `ac_tc_2b_header_acquisition_station` | tidied |  |  |
| `ac_tc_2b_header_degradedProductQualityFlag` | `ac_tc_2b_header_degraded_product_quality_flag` | tidied |  |  |
| `ac_tc_2b_header_executableMajorVersion` | `ac_tc_2b_header_executable_major_version` | tidied |  |  |
| `ac_tc_2b_header_executableMinorVersion` | `ac_tc_2b_header_executable_minor_version` | tidied |  |  |
| `ac_tc_2b_header_fileCategory` | `ac_tc_2b_header_file_category` | tidied |  |  |
| `ac_tc_2b_header_fileClass` | `ac_tc_2b_header_fileClass` | clash |  |  |
| `ac_tc_2b_header_formatMajorVersion` | `ac_tc_2b_header_format_major_version` | tidied |  |  |
| `ac_tc_2b_header_formatMinorVersion` | `ac_tc_2b_header_format_minor_version` | tidied |  |  |
| `ac_tc_2b_header_frameID` | `ac_tc_2b_header_frame_id` | tidied |  |  |
| `ac_tc_2b_header_frameStartCoordinates_geographicLatitude` | `ac_tc_2b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `ac_tc_2b_header_frameStartCoordinates_geographicLongitude` | `ac_tc_2b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `ac_tc_2b_header_frameStartMargin` | `ac_tc_2b_header_frame_start_margin` | tidied |  |  |
| `ac_tc_2b_header_frameStartTime` | `ac_tc_2b_header_frame_start_time` | tidied |  |  |
| `ac_tc_2b_header_frameStopCoordinates_geographicLatitude` | `ac_tc_2b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `ac_tc_2b_header_frameStopCoordinates_geographicLongitude` | `ac_tc_2b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `ac_tc_2b_header_frameStopMargin` | `ac_tc_2b_header_frame_stop_margin` | tidied |  |  |
| `ac_tc_2b_header_frameStopTime` | `ac_tc_2b_header_frame_stop_time` | tidied |  |  |
| `ac_tc_2b_header_meanAnomaly` | `ac_tc_2b_header_mean_anomaly` | tidied |  |  |
| `ac_tc_2b_header_missionID` | `ac_tc_2b_header_mission_id` | tidied |  |  |
| `ac_tc_2b_header_orbitEccentricity` | `ac_tc_2b_header_orbit_eccentricity` | tidied |  |  |
| `ac_tc_2b_header_orbitInclination` | `ac_tc_2b_header_orbit_inclination` | tidied |  |  |
| `ac_tc_2b_header_orbitNumber` | `ac_tc_2b_header_orbit_number` | tidied |  |  |
| `ac_tc_2b_header_orbitSemiMajorAxis` | `ac_tc_2b_header_orbit_semi_major_axis` | tidied |  |  |
| `ac_tc_2b_header_originalProductName` | `ac_tc_2b_header_original_product_name` | tidied |  |  |
| `ac_tc_2b_header_perigeeArgument` | `ac_tc_2b_header_perigee_argument` | tidied |  |  |
| `ac_tc_2b_header_processingCentre` | `ac_tc_2b_header_processing_centre` | tidied |  |  |
| `ac_tc_2b_header_processingStartTime` | `ac_tc_2b_header_processing_start_time` | tidied |  |  |
| `ac_tc_2b_header_processingStopTime` | `ac_tc_2b_header_processing_stop_time` | tidied |  |  |
| `ac_tc_2b_header_processorMajorVersion` | `ac_tc_2b_header_processor_major_version` | tidied |  |  |
| `ac_tc_2b_header_processorMinorVersion` | `ac_tc_2b_header_processor_minor_version` | tidied |  |  |
| `ac_tc_2b_header_processorName` | `ac_tc_2b_header_processor_name` | tidied |  |  |
| `ac_tc_2b_header_productLevel` | `ac_tc_2b_header_product_level` | tidied |  |  |
| `ac_tc_2b_header_productName` | `ac_tc_2b_header_product_name` | tidied |  |  |
| `ac_tc_2b_header_productType` | `ac_tc_2b_header_product_type` | tidied |  |  |
| `ac_tc_2b_header_rightAscension` | `ac_tc_2b_header_right_ascension` | tidied |  |  |
| `ac_tc_2b_header_sensingStartTime` | `ac_tc_2b_header_sensing_start_time` | tidied |  |  |
| `ac_tc_2b_header_sensingStopTime` | `ac_tc_2b_header_sensing_stop_time` | tidied |  |  |
| `ac_tc_2b_header_stateVectorSource` | `ac_tc_2b_header_state_vector_source` | tidied |  |  |
| `ac_tc_2b_header_stateVectorTime` | `ac_tc_2b_header_state_vector_time` | tidied |  |  |
| `ac_tc_2b_header_subsettedProduct` | `ac_tc_2b_header_subsetted_product` | tidied |  |  |
| `ac_tc_2b_header_xPosition` | `ac_tc_2b_header_x_position` | tidied |  |  |
| `ac_tc_2b_header_xVelocity` | `ac_tc_2b_header_x_velocity` | tidied |  |  |
| `ac_tc_2b_header_yPosition` | `ac_tc_2b_header_y_position` | tidied |  |  |
| `ac_tc_2b_header_yVelocity` | `ac_tc_2b_header_y_velocity` | tidied |  |  |
| `ac_tc_2b_header_zPosition` | `ac_tc_2b_header_z_position` | tidied |  |  |
| `ac_tc_2b_header_zVelocity` | `ac_tc_2b_header_z_velocity` | tidied |  |  |
| `ac_tc_2b_pressure` | `ac_tc_2b_pressure` | kept | Pressure | Pa |
| `ac_tc_2b_quality_low_resolution_status` | `ac_tc_2b_low_resolution_quality_status` | long_name | Low resolution quality status |  |
| `ac_tc_2b_quality_medium_resolution_status` | `ac_tc_2b_medium_resolution_quality_status` | long_name | Medium resolution quality status |  |
| `ac_tc_2b_size_JSG_height` | `ac_tc_2b_size_jsg_height` | tidied |  |  |
| `acm_cap_2b_ATLID_backscatter_mie` | `acm_cap_2b_mie_channel_apparent_backscatter_coefficient` | long_name | Mie-channel apparent backscatter coefficient | m-1 sr-1 |
| `acm_cap_2b_ATLID_backscatter_mie_assimilation_status` | `acm_cap_2b_atlid_backscatter_mie_assimilation_status` | tidied | ATLID Mie-channel apparent backscatter coefficient assimilat |  |
| `acm_cap_2b_ATLID_backscatter_mie_forward` | `acm_cap_2b_atlid_backscatter_mie_forward` | tidied | Forward modelled Mie-channel apparent backscatter coefficien | m-1 sr-1 |
| `acm_cap_2b_ATLID_backscatter_rayleigh` | `acm_cap_2b_atlid_backscatter_rayleigh` | tidied | Rayleigh-channel apparent backscatter coefficient | m-1 sr-1 |
| `acm_cap_2b_ATLID_backscatter_rayleigh_assimilation_status` | `acm_cap_2b_atlid_backscatter_rayleigh_assimilation_status` | tidied | ATLID Rayleigh-channel apparent backscatter coefficient assi |  |
| `acm_cap_2b_ATLID_backscatter_rayleigh_forward` | `acm_cap_2b_atlid_backscatter_rayleigh_forward` | tidied | Forward modelled Rayleigh-channel apparent backscatter coeff | m-1 sr-1 |
| `acm_cap_2b_ATLID_bscat_extinction_ratio` | `acm_cap_2b_atlid_bscat_extinction_ratio` | tidied | ATLID ratio of backscatter to extinction coefficient | sr-1 |
| `acm_cap_2b_ATLID_cost_function` | `acm_cap_2b_atlid_cost_function` | tidied | Contribution to final cost function from ATLID | 1 |
| `acm_cap_2b_ATLID_observation_variable_count` | `acm_cap_2b_atlid_observation_variable_count` | tidied | Number of observation-vector elements associated with ATLID |  |
| `acm_cap_2b_ATLID_optical_depth_forward` | `acm_cap_2b_atlid_optical_depth_forward` | tidied | Forward modelled atmospheric optical depth at the wavelength | 1 |
| `acm_cap_2b_CPR_cost_function` | `acm_cap_2b_contribution_to_final_cost_function_from_cpr` | long_name | Contribution to final cost function from CPR | 1 |
| `acm_cap_2b_CPR_doppler_velocity` | `acm_cap_2b_doppler_velocity` | long_name | Doppler velocity | m s-1 |
| `acm_cap_2b_CPR_doppler_velocity_assimilation_status` | `acm_cap_2b_cpr_doppler_velocity_assimilation_status` | long_name | CPR Doppler velocity assimilation status |  |
| `acm_cap_2b_CPR_doppler_velocity_forward` | `acm_cap_2b_forward_modelled_doppler_velocity` | long_name | Forward modelled Doppler velocity | m s-1 |
| `acm_cap_2b_CPR_observation_variable_count` | `acm_cap_2b_cpr_observation_variable_count` | tidied | Number of observation-vector elements associated with CPR |  |
| `acm_cap_2b_CPR_optical_depth_forward` | `acm_cap_2b_cpr_optical_depth_forward` | tidied | Forward modelled atmospheric optical depth at the wavelength | 1 |
| `acm_cap_2b_CPR_path_integrated_attenuation` | `acm_cap_2b_path_integrated_attenuation` | long_name | Path-integrated attenuation | dB |
| `acm_cap_2b_CPR_path_integrated_attenuation_assimilation_status` | `acm_cap_2b_cpr_path_integrated_attenuation_assimilation_status` | long_name | CPR path-integrated attenuation assimilation status |  |
| `acm_cap_2b_CPR_path_integrated_attenuation_forward` | `acm_cap_2b_forward_modelled_path_integrated_attenuation` | long_name | Forward modelled path-integrated attenuation | dB |
| `acm_cap_2b_CPR_reflectivity_factor` | `acm_cap_2b_radar_reflectivity_factor` | long_name | Radar reflectivity factor | dBZ |
| `acm_cap_2b_CPR_reflectivity_factor_assimilation_status` | `acm_cap_2b_cpr_reflectivity_factor_assimilation_status` | tidied | CPR radar reflectivity factor assimilation status |  |
| `acm_cap_2b_CPR_reflectivity_factor_forward` | `acm_cap_2b_forward_modelled_radar_reflectivity_factor` | long_name | Forward modelled radar reflectivity factor | dBZ |
| `acm_cap_2b_MSI_longwave_brightness_temperature` | `acm_cap_2b_msi_longwave_brightness_temperature` | long_name | MSI_longwave brightness temperature | K |
| `acm_cap_2b_MSI_longwave_brightness_temperature_assimilation_status` | `acm_cap_2b_msi_longwave_brightness_temperature_assimilation_status` | long_name | MSI_longwave brightness temperature assimilation status |  |
| `acm_cap_2b_MSI_longwave_brightness_temperature_forward` | `acm_cap_2b_forward_modelled_brightness_temperature` | long_name | Forward modelled brightness temperature | K |
| `acm_cap_2b_MSI_longwave_cost_function` | `acm_cap_2b_msi_longwave_cost_function` | tidied | Contribution to final cost function from MSI_longwave | 1 |
| `acm_cap_2b_MSI_longwave_observation_variable_count` | `acm_cap_2b_msi_longwave_observation_variable_count` | tidied | Number of observation-vector elements associated with MSI_lo |  |
| `acm_cap_2b_MSI_longwave_wavelength` | `acm_cap_2b_msi_longwave_channel_wavelengths` | long_name | MSI_longwave channel wavelengths | m |
| `acm_cap_2b_MSI_shortwave_albedo` | `acm_cap_2b_msi_shortwave_albedo` | long_name | MSI_shortwave albedo | 1 |
| `acm_cap_2b_MSI_shortwave_albedo_assimilation_status` | `acm_cap_2b_msi_shortwave_albedo_assimilation_status` | long_name | MSI_shortwave albedo assimilation status |  |
| `acm_cap_2b_MSI_shortwave_albedo_forward` | `acm_cap_2b_forward_modelled_albedo` | long_name | Forward modelled albedo | 1 |
| `acm_cap_2b_MSI_shortwave_cost_function` | `acm_cap_2b_msi_shortwave_cost_function` | tidied | Contribution to final cost function from MSI_shortwave | 1 |
| `acm_cap_2b_MSI_shortwave_observation_variable_count` | `acm_cap_2b_msi_shortwave_observation_variable_count` | tidied | Number of observation-vector elements associated with MSI_sh |  |
| `acm_cap_2b_MSI_shortwave_wavelength` | `acm_cap_2b_msi_shortwave_channel_wavelengths` | long_name | MSI_shortwave channel wavelengths | m |
| `acm_cap_2b_aerosol_extinction` | `acm_cap_2b_aerosol_geometric_extinction_coefficient` | long_name | Aerosol geometric extinction coefficient | m-1 |
| `acm_cap_2b_aerosol_number_concentration` | `acm_cap_2b_aerosol_total_number_concentration` | long_name | Aerosol total number concentration | m-3 |
| `acm_cap_2b_aerosol_optical_depth` | `acm_cap_2b_aerosol_geometric_optical_depth` | long_name | Aerosol geometric optical depth | 1 |
| `acm_cap_2b_cost_function` | `acm_cap_2b_value_of_cost_function_after_final_iteration` | long_name | Value of cost function after final iteration | 1 |
| `acm_cap_2b_header_ANXLongitude` | `acm_cap_2b_header_anx_longitude` | tidied |  |  |
| `acm_cap_2b_header_ANXTime` | `acm_cap_2b_header_anx_time` | tidied |  |  |
| `acm_cap_2b_header_File_Class` | `acm_cap_2b_header_File_Class` | clash |  |  |
| `acm_cap_2b_header_File_Description` | `acm_cap_2b_header_file_description` | tidied |  |  |
| `acm_cap_2b_header_File_Name` | `acm_cap_2b_header_file_name` | tidied |  |  |
| `acm_cap_2b_header_File_Type` | `acm_cap_2b_header_file_type` | tidied |  |  |
| `acm_cap_2b_header_File_Version` | `acm_cap_2b_header_file_version` | tidied |  |  |
| `acm_cap_2b_header_Mission` | `acm_cap_2b_header_mission` | tidied |  |  |
| `acm_cap_2b_header_Notes` | `acm_cap_2b_header_notes` | tidied |  |  |
| `acm_cap_2b_header_Source_Creation_Date` | `acm_cap_2b_header_source_creation_date` | tidied |  |  |
| `acm_cap_2b_header_Source_Creator` | `acm_cap_2b_header_source_creator` | tidied |  |  |
| `acm_cap_2b_header_Source_Creator_Version` | `acm_cap_2b_header_source_creator_version` | tidied |  |  |
| `acm_cap_2b_header_Source_System` | `acm_cap_2b_header_source_system` | tidied |  |  |
| `acm_cap_2b_header_Validity_Period_Validity_Start` | `acm_cap_2b_header_validity_period_validity_start` | tidied |  |  |
| `acm_cap_2b_header_Validity_Period_Validity_Stop` | `acm_cap_2b_header_validity_period_validity_stop` | tidied |  |  |
| `acm_cap_2b_header_acquisitionStation` | `acm_cap_2b_header_acquisition_station` | tidied |  |  |
| `acm_cap_2b_header_degradedProductQualityFlag` | `acm_cap_2b_header_degraded_product_quality_flag` | tidied |  |  |
| `acm_cap_2b_header_executableMajorVersion` | `acm_cap_2b_header_executable_major_version` | tidied |  |  |
| `acm_cap_2b_header_executableMinorVersion` | `acm_cap_2b_header_executable_minor_version` | tidied |  |  |
| `acm_cap_2b_header_fileCategory` | `acm_cap_2b_header_file_category` | tidied |  |  |
| `acm_cap_2b_header_fileClass` | `acm_cap_2b_header_fileClass` | clash |  |  |
| `acm_cap_2b_header_formatMajorVersion` | `acm_cap_2b_header_format_major_version` | tidied |  |  |
| `acm_cap_2b_header_formatMinorVersion` | `acm_cap_2b_header_format_minor_version` | tidied |  |  |
| `acm_cap_2b_header_frameID` | `acm_cap_2b_header_frame_id` | tidied |  |  |
| `acm_cap_2b_header_frameStartCoordinates_geographicLatitude` | `acm_cap_2b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `acm_cap_2b_header_frameStartCoordinates_geographicLongitude` | `acm_cap_2b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `acm_cap_2b_header_frameStartMargin` | `acm_cap_2b_header_frame_start_margin` | tidied |  |  |
| `acm_cap_2b_header_frameStartTime` | `acm_cap_2b_header_frame_start_time` | tidied |  |  |
| `acm_cap_2b_header_frameStopCoordinates_geographicLatitude` | `acm_cap_2b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `acm_cap_2b_header_frameStopCoordinates_geographicLongitude` | `acm_cap_2b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `acm_cap_2b_header_frameStopMargin` | `acm_cap_2b_header_frame_stop_margin` | tidied |  |  |
| `acm_cap_2b_header_frameStopTime` | `acm_cap_2b_header_frame_stop_time` | tidied |  |  |
| `acm_cap_2b_header_meanAnomaly` | `acm_cap_2b_header_mean_anomaly` | tidied |  |  |
| `acm_cap_2b_header_missionID` | `acm_cap_2b_header_mission_id` | tidied |  |  |
| `acm_cap_2b_header_orbitEccentricity` | `acm_cap_2b_header_orbit_eccentricity` | tidied |  |  |
| `acm_cap_2b_header_orbitInclination` | `acm_cap_2b_header_orbit_inclination` | tidied |  |  |
| `acm_cap_2b_header_orbitNumber` | `acm_cap_2b_header_orbit_number` | tidied |  |  |
| `acm_cap_2b_header_orbitSemiMajorAxis` | `acm_cap_2b_header_orbit_semi_major_axis` | tidied |  |  |
| `acm_cap_2b_header_originalProductName` | `acm_cap_2b_header_original_product_name` | tidied |  |  |
| `acm_cap_2b_header_perigeeArgument` | `acm_cap_2b_header_perigee_argument` | tidied |  |  |
| `acm_cap_2b_header_processingCentre` | `acm_cap_2b_header_processing_centre` | tidied |  |  |
| `acm_cap_2b_header_processingStartTime` | `acm_cap_2b_header_processing_start_time` | tidied |  |  |
| `acm_cap_2b_header_processingStopTime` | `acm_cap_2b_header_processing_stop_time` | tidied |  |  |
| `acm_cap_2b_header_processorMajorVersion` | `acm_cap_2b_header_processor_major_version` | tidied |  |  |
| `acm_cap_2b_header_processorMinorVersion` | `acm_cap_2b_header_processor_minor_version` | tidied |  |  |
| `acm_cap_2b_header_processorName` | `acm_cap_2b_header_processor_name` | tidied |  |  |
| `acm_cap_2b_header_productLevel` | `acm_cap_2b_header_product_level` | tidied |  |  |
| `acm_cap_2b_header_productName` | `acm_cap_2b_header_product_name` | tidied |  |  |
| `acm_cap_2b_header_productType` | `acm_cap_2b_header_product_type` | tidied |  |  |
| `acm_cap_2b_header_rightAscension` | `acm_cap_2b_header_right_ascension` | tidied |  |  |
| `acm_cap_2b_header_sensingStartTime` | `acm_cap_2b_header_sensing_start_time` | tidied |  |  |
| `acm_cap_2b_header_sensingStopTime` | `acm_cap_2b_header_sensing_stop_time` | tidied |  |  |
| `acm_cap_2b_header_stateVectorSource` | `acm_cap_2b_header_state_vector_source` | tidied |  |  |
| `acm_cap_2b_header_stateVectorTime` | `acm_cap_2b_header_state_vector_time` | tidied |  |  |
| `acm_cap_2b_header_subsettedProduct` | `acm_cap_2b_header_subsetted_product` | tidied |  |  |
| `acm_cap_2b_header_xPosition` | `acm_cap_2b_header_x_position` | tidied |  |  |
| `acm_cap_2b_header_xVelocity` | `acm_cap_2b_header_x_velocity` | tidied |  |  |
| `acm_cap_2b_header_yPosition` | `acm_cap_2b_header_y_position` | tidied |  |  |
| `acm_cap_2b_header_yVelocity` | `acm_cap_2b_header_y_velocity` | tidied |  |  |
| `acm_cap_2b_header_zPosition` | `acm_cap_2b_header_z_position` | tidied |  |  |
| `acm_cap_2b_header_zVelocity` | `acm_cap_2b_header_z_velocity` | tidied |  |  |
| `acm_cap_2b_ice_N0prime` | `acm_cap_2b_ice_primed_number_concentration` | long_name | Ice primed number concentration | m-3.4 |
| `acm_cap_2b_ice_N0prime_error` | `acm_cap_2b_ice_n0prime_error` | tidied | One-sigma error in natural logarithm of ice primed number co | 1 |
| `acm_cap_2b_ice_N0prime_error_corr_scale` | `acm_cap_2b_ice_n0prime_error_corr_scale` | tidied | Decorrelation scale in error covariance of ice primed number | m |
| `acm_cap_2b_ice_N0prime_kernel_corr_scale` | `acm_cap_2b_ice_n0prime_kernel_corr_scale` | tidied | Decorrelation scale in averaging kernel of ice primed number | m |
| `acm_cap_2b_ice_N0prime_kernel_sum` | `acm_cap_2b_ice_n0prime_kernel_sum` | tidied | Row-sum of averaging kernel of ice primed number concentrati | 1 |
| `acm_cap_2b_ice_N0prime_lidar_bscat_extinction_ratio_error_corr` | `acm_cap_2b_ice_n0prime_lidar_bscat_extinction_ratio_error_corr` | tidied | Error correlation between natural logs of ice primed number  | 1 |
| `acm_cap_2b_ice_N0prime_riming_index_error_corr` | `acm_cap_2b_ice_n0prime_riming_index_error_corr` | tidied | Error correlation between natural logs of ice primed number  | 1 |
| `acm_cap_2b_ice_cost_function` | `acm_cap_2b_contribution_to_final_cost_function_from_ice` | long_name | Contribution to final cost function from ice | 1 |
| `acm_cap_2b_ice_extinction` | `acm_cap_2b_ice_geometric_extinction_coefficient` | long_name | Ice geometric extinction coefficient | m-1 |
| `acm_cap_2b_ice_extinction_N0prime_error_corr` | `acm_cap_2b_ice_extinction_n0prime_error_corr` | tidied | Error correlation between natural logs of ice geometric exti | 1 |
| `acm_cap_2b_ice_lidar_bscat_extinction_ratio` | `acm_cap_2b_ice_lidar_backscatter_to_extinction_ratio` | long_name | Ice lidar backscatter-to-extinction ratio | sr-1 |
| `acm_cap_2b_ice_riming_factor_error` | `acm_cap_2b_one_sigma_error_in_ice_riming_factor` | long_name | One-sigma error in Ice riming factor | 1 |
| `acm_cap_2b_iterations_count` | `acm_cap_2b_total_number_of_iterations_carried_out` | long_name | Total number of iterations carried out |  |
| `acm_cap_2b_liquid_extinction` | `acm_cap_2b_liquid_geometric_extinction_coefficient` | long_name | Liquid geometric extinction coefficient | m-1 |
| `acm_cap_2b_liquid_lidar_bscat_extinction_ratio` | `acm_cap_2b_liquid_lidar_backscatter_to_extinction_ratio` | long_name | Liquid lidar backscatter-to-extinction ratio | sr-1 |
| `acm_cap_2b_liquid_number_concentration` | `acm_cap_2b_liquid_total_number_concentration` | long_name | Liquid total number concentration | m-3 |
| `acm_cap_2b_liquid_optical_depth` | `acm_cap_2b_liquid_geometric_optical_depth` | long_name | Liquid geometric optical depth | 1 |
| `acm_cap_2b_observation_variable_count` | `acm_cap_2b_number_of_elements_in_observation_vector` | long_name | Number of elements in observation vector |  |
| `acm_cap_2b_pressure` | `acm_cap_2b_atmospheric_pressure_from_x_met` | long_name | atmospheric pressure from X-MET | Pa |
| `acm_cap_2b_rain_cost_function` | `acm_cap_2b_contribution_to_final_cost_function_from_rain` | long_name | Contribution to final cost function from rain | 1 |
| `acm_cap_2b_rain_rate_kernel_sum` | `acm_cap_2b_row_sum_of_averaging_kernel_of_rain_rate` | long_name | Row-sum of averaging kernel of rain rate | 1 |
| `acm_cap_2b_size_JSG_height` | `acm_cap_2b_size_jsg_height` | tidied |  |  |
| `acm_cap_2b_size_MSI_longwave_channel` | `acm_cap_2b_size_msi_longwave_channel` | tidied |  |  |
| `acm_cap_2b_size_MSI_shortwave_channel` | `acm_cap_2b_size_msi_shortwave_channel` | tidied |  |  |
| `acm_cap_2b_specific_humidity` | `acm_cap_2b_atmospheric_specific_humidity_from_x_met` | long_name | atmospheric specific humidity from X-MET | kg/kg |
| `acm_cap_2b_state_variable_count` | `acm_cap_2b_number_of_elements_in_state_vector` | long_name | Number of elements in state vector |  |
| `acm_cap_2b_temperature` | `acm_cap_2b_atmospheric_temperature_from_x_met` | long_name | atmospheric temperature from X-MET | K |
| `acm_com_2b_BRDF_geo_near_infrared` | `acm_com_2b_brdf_geometric_factor_for_the_near_infrared` | long_name | BRDF geometric factor for the near infrared | 1 |
| `acm_com_2b_BRDF_geo_visible` | `acm_com_2b_brdf_geometric_factor_for_the_uv_visible` | long_name | BRDF geometric factor for the UV/visible | 1 |
| `acm_com_2b_BRDF_iso_near_infrared` | `acm_com_2b_brdf_isotropic_factor_for_the_near_infrared` | long_name | BRDF isotropic factor for the near infrared | 1 |
| `acm_com_2b_BRDF_iso_visible` | `acm_com_2b_brdf_isotropic_factor_for_the_uv_visible` | long_name | BRDF isotropic factor for the UV/visible | 1 |
| `acm_com_2b_BRDF_vol_near_infrared` | `acm_com_2b_brdf_volumic_factor_for_the_near_infrared` | long_name | BRDF volumic factor for the near infrared | 1 |
| `acm_com_2b_BRDF_vol_visible` | `acm_com_2b_brdf_volumic_factor_for_the_uv_visible` | long_name | BRDF volumic factor for the UV/visible | 1 |
| `acm_com_2b_aerosol_extinction` | `acm_com_2b_aerosol_extinction_coefficient_at_0_355_micro_meter` | long_name | Aerosol extinction coefficient at 0.355 micro-meter | m-1 |
| `acm_com_2b_aerosol_extinction_quality_status` | `acm_com_2b_quality_status_of_aerosol_extinction` | long_name | Quality status of aerosol extinction |  |
| `acm_com_2b_albedo_diffuse_radiation_surface_near_infrared` | `acm_com_2b_diffuse_surface_albedo_for_near_infrared` | long_name | Diffuse surface albedo for near infrared | 1 |
| `acm_com_2b_albedo_diffuse_radiation_surface_visible` | `acm_com_2b_diffuse_surface_albedo_for_the_uv_visible` | long_name | Diffuse surface albedo for the UV/visible | 1 |
| `acm_com_2b_albedo_direct_radiation_surface_near_infrared` | `acm_com_2b_direct_surface_albedo_for_near_infrared` | long_name | Direct surface albedo for near infrared | 1 |
| `acm_com_2b_albedo_direct_radiation_surface_visible` | `acm_com_2b_direct_surface_albedo_for_the_uv_visible` | long_name | Direct surface albedo for the UV/visible | 1 |
| `acm_com_2b_atmosphere_classification` | `acm_com_2b_atmosphere_indices` | long_name | Atmosphere indices |  |
| `acm_com_2b_cloud_flag` | `acm_com_2b_flag_indicating_if_cloud_is_present_or_not` | long_name | Flag indicating if cloud is present or not |  |
| `acm_com_2b_cloud_retrieval_quality_status` | `acm_com_2b_quality_status_of_cloud_retrieval` | long_name | Quality status of cloud retrieval |  |
| `acm_com_2b_composite_source_ice` | `acm_com_2b_source_of_composite_ice_cloud_retrieval` | long_name | Source of composite ice cloud retrieval |  |
| `acm_com_2b_composite_source_liquid` | `acm_com_2b_source_of_composite_liquid_cloud_retrieval` | long_name | Source of composite liquid cloud retrieval |  |
| `acm_com_2b_doppler_velocity` | `acm_com_2b_cpr_doppler_velocity` | long_name | CPR doppler_velocity | m s-1 |
| `acm_com_2b_geoid_offset` | `acm_com_2b_offset_between_geoid_egm96_and_ellipsoid_wgs84` | long_name | Offset between geoid EGM96 and ellipsoid WGS84 | m |
| `acm_com_2b_header_ANXLongitude` | `acm_com_2b_header_anx_longitude` | tidied |  |  |
| `acm_com_2b_header_ANXTime` | `acm_com_2b_header_anx_time` | tidied |  |  |
| `acm_com_2b_header_File_Class` | `acm_com_2b_header_File_Class` | clash |  |  |
| `acm_com_2b_header_File_Description` | `acm_com_2b_header_file_description` | tidied |  |  |
| `acm_com_2b_header_File_Name` | `acm_com_2b_header_file_name` | tidied |  |  |
| `acm_com_2b_header_File_Type` | `acm_com_2b_header_file_type` | tidied |  |  |
| `acm_com_2b_header_File_Version` | `acm_com_2b_header_file_version` | tidied |  |  |
| `acm_com_2b_header_Mission` | `acm_com_2b_header_mission` | tidied |  |  |
| `acm_com_2b_header_Notes` | `acm_com_2b_header_notes` | tidied |  |  |
| `acm_com_2b_header_Source_Creation_Date` | `acm_com_2b_header_source_creation_date` | tidied |  |  |
| `acm_com_2b_header_Source_Creator` | `acm_com_2b_header_source_creator` | tidied |  |  |
| `acm_com_2b_header_Source_Creator_Version` | `acm_com_2b_header_source_creator_version` | tidied |  |  |
| `acm_com_2b_header_Source_System` | `acm_com_2b_header_source_system` | tidied |  |  |
| `acm_com_2b_header_Validity_Period_Validity_Start` | `acm_com_2b_header_validity_period_validity_start` | tidied |  |  |
| `acm_com_2b_header_Validity_Period_Validity_Stop` | `acm_com_2b_header_validity_period_validity_stop` | tidied |  |  |
| `acm_com_2b_header_acquisitionStation` | `acm_com_2b_header_acquisition_station` | tidied |  |  |
| `acm_com_2b_header_degradedProductQualityFlag` | `acm_com_2b_header_degraded_product_quality_flag` | tidied |  |  |
| `acm_com_2b_header_executableMajorVersion` | `acm_com_2b_header_executable_major_version` | tidied |  |  |
| `acm_com_2b_header_executableMinorVersion` | `acm_com_2b_header_executable_minor_version` | tidied |  |  |
| `acm_com_2b_header_fileCategory` | `acm_com_2b_header_file_category` | tidied |  |  |
| `acm_com_2b_header_fileClass` | `acm_com_2b_header_fileClass` | clash |  |  |
| `acm_com_2b_header_formatMajorVersion` | `acm_com_2b_header_format_major_version` | tidied |  |  |
| `acm_com_2b_header_formatMinorVersion` | `acm_com_2b_header_format_minor_version` | tidied |  |  |
| `acm_com_2b_header_frameID` | `acm_com_2b_header_frame_id` | tidied |  |  |
| `acm_com_2b_header_frameStartCoordinates_geographicLatitude` | `acm_com_2b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `acm_com_2b_header_frameStartCoordinates_geographicLongitude` | `acm_com_2b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `acm_com_2b_header_frameStartMargin` | `acm_com_2b_header_frame_start_margin` | tidied |  |  |
| `acm_com_2b_header_frameStartTime` | `acm_com_2b_header_frame_start_time` | tidied |  |  |
| `acm_com_2b_header_frameStopCoordinates_geographicLatitude` | `acm_com_2b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `acm_com_2b_header_frameStopCoordinates_geographicLongitude` | `acm_com_2b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `acm_com_2b_header_frameStopMargin` | `acm_com_2b_header_frame_stop_margin` | tidied |  |  |
| `acm_com_2b_header_frameStopTime` | `acm_com_2b_header_frame_stop_time` | tidied |  |  |
| `acm_com_2b_header_meanAnomaly` | `acm_com_2b_header_mean_anomaly` | tidied |  |  |
| `acm_com_2b_header_missionID` | `acm_com_2b_header_mission_id` | tidied |  |  |
| `acm_com_2b_header_orbitEccentricity` | `acm_com_2b_header_orbit_eccentricity` | tidied |  |  |
| `acm_com_2b_header_orbitInclination` | `acm_com_2b_header_orbit_inclination` | tidied |  |  |
| `acm_com_2b_header_orbitNumber` | `acm_com_2b_header_orbit_number` | tidied |  |  |
| `acm_com_2b_header_orbitSemiMajorAxis` | `acm_com_2b_header_orbit_semi_major_axis` | tidied |  |  |
| `acm_com_2b_header_originalProductName` | `acm_com_2b_header_original_product_name` | tidied |  |  |
| `acm_com_2b_header_perigeeArgument` | `acm_com_2b_header_perigee_argument` | tidied |  |  |
| `acm_com_2b_header_processingCentre` | `acm_com_2b_header_processing_centre` | tidied |  |  |
| `acm_com_2b_header_processingStartTime` | `acm_com_2b_header_processing_start_time` | tidied |  |  |
| `acm_com_2b_header_processingStopTime` | `acm_com_2b_header_processing_stop_time` | tidied |  |  |
| `acm_com_2b_header_processorMajorVersion` | `acm_com_2b_header_processor_major_version` | tidied |  |  |
| `acm_com_2b_header_processorMinorVersion` | `acm_com_2b_header_processor_minor_version` | tidied |  |  |
| `acm_com_2b_header_processorName` | `acm_com_2b_header_processor_name` | tidied |  |  |
| `acm_com_2b_header_productLevel` | `acm_com_2b_header_product_level` | tidied |  |  |
| `acm_com_2b_header_productName` | `acm_com_2b_header_product_name` | tidied |  |  |
| `acm_com_2b_header_productType` | `acm_com_2b_header_product_type` | tidied |  |  |
| `acm_com_2b_header_rightAscension` | `acm_com_2b_header_right_ascension` | tidied |  |  |
| `acm_com_2b_header_sensingStartTime` | `acm_com_2b_header_sensing_start_time` | tidied |  |  |
| `acm_com_2b_header_sensingStopTime` | `acm_com_2b_header_sensing_stop_time` | tidied |  |  |
| `acm_com_2b_header_stateVectorSource` | `acm_com_2b_header_state_vector_source` | tidied |  |  |
| `acm_com_2b_header_stateVectorTime` | `acm_com_2b_header_state_vector_time` | tidied |  |  |
| `acm_com_2b_header_subsettedProduct` | `acm_com_2b_header_subsetted_product` | tidied |  |  |
| `acm_com_2b_header_xPosition` | `acm_com_2b_header_x_position` | tidied |  |  |
| `acm_com_2b_header_xVelocity` | `acm_com_2b_header_x_velocity` | tidied |  |  |
| `acm_com_2b_header_yPosition` | `acm_com_2b_header_y_position` | tidied |  |  |
| `acm_com_2b_header_yVelocity` | `acm_com_2b_header_y_velocity` | tidied |  |  |
| `acm_com_2b_header_zPosition` | `acm_com_2b_header_z_position` | tidied |  |  |
| `acm_com_2b_header_zVelocity` | `acm_com_2b_header_z_velocity` | tidied |  |  |
| `acm_com_2b_height_layer` | `acm_com_2b_height_of_layers` | long_name | Height of layers | m |
| `acm_com_2b_height_level` | `acm_com_2b_height_of_levels` | long_name | Height of levels | m |
| `acm_com_2b_ice_water_content` | `acm_com_2b_cloud_ice_water_content` | long_name | Cloud ice water content | kg m-3 |
| `acm_com_2b_ice_water_content_error` | `acm_com_2b_relative_1_sigma_error_for_ice_water_content` | long_name | relative 1-sigma error for ice water content | 1 |
| `acm_com_2b_input_ATLID_first_product_flag` | `acm_com_2b_input_atlid_first_product_flag` | tidied | Flag indicating the availability of A-AER product |  |
| `acm_com_2b_input_ATLID_fourth_product_flag` | `acm_com_2b_input_atlid_fourth_product_flag` | tidied | Flag indicating the availability of A-CTH product |  |
| `acm_com_2b_input_ATLID_second_product_flag` | `acm_com_2b_input_atlid_second_product_flag` | tidied | Flag indicating the availability of A-ICE product |  |
| `acm_com_2b_input_ATLID_third_product_flag` | `acm_com_2b_input_atlid_third_product_flag` | tidied | Flag indicating the availability of A-TC product |  |
| `acm_com_2b_input_CPR_product_flag` | `acm_com_2b_input_cpr_product_flag` | tidied | Flag indicating the availability of C-CLD product |  |
| `acm_com_2b_input_MSI_first_product_flag` | `acm_com_2b_input_msi_first_product_flag` | tidied | Flag indicating the availability of M-COP product |  |
| `acm_com_2b_input_MSI_second_product_flag` | `acm_com_2b_input_msi_second_product_flag` | tidied | Flag indicating the availability of M-CM product |  |
| `acm_com_2b_irradiance_solar` | `acm_com_2b_toa_downwelling_solar_irradiance` | long_name | TOA downwelling solar irradiance | W m-2 |
| `acm_com_2b_latitude_active` | `acm_com_2b_latitude_under_active_sensors` | long_name | Latitude under active sensors | degree_north |
| `acm_com_2b_liquid_water_content` | `acm_com_2b_cloud_liquid_water_content` | long_name | Cloud liquid water content | kg m-3 |
| `acm_com_2b_longitude_active` | `acm_com_2b_longitude_under_active_sensor` | long_name | Longitude under active sensor | degree_east |
| `acm_com_2b_number_layers_above_surface` | `acm_com_2b_number_of_layers_above_the_surface` | long_name | Number of layers above the surface |  |
| `acm_com_2b_quality_status` | `acm_com_2b_quality_status_of_cloud_and_aerosol_retrievals` | long_name | Quality status of cloud and aerosol retrievals |  |
| `acm_com_2b_radar_reflectivity` | `acm_com_2b_cpr_radar_reflectivity` | long_name | CPR radar reflectivity | dBZ |
| `acm_com_2b_rain_rate_error` | `acm_com_2b_rain_rate_relative_error` | long_name | Rain rate relative error | 1 |
| `acm_com_2b_rain_water_content_quality_status` | `acm_com_2b_quality_status_of_rain_water_retrieval` | long_name | Quality status of rain water retrieval |  |
| `acm_com_2b_snow_water_content_quality_status` | `acm_com_2b_quality_status_of_snow_water_retrieval` | long_name | Quality status of snow water retrieval |  |
| `acm_com_2b_specific_humidity_layer_mean` | `acm_com_2b_specific_humidity` | long_name | Specific humidity | kg kg-1 |
| `acm_com_2b_surface_albedo_classification` | `acm_com_2b_flag_to_source_of_surface_albedo_data` | long_name | Flag to source of surface albedo data |  |
| `acm_com_2b_surface_albedo_quality_status` | `acm_com_2b_quality_status_of_surface_albedo` | long_name | Quality status of surface albedo |  |
| `acm_com_2b_surface_emissivity_classification` | `acm_com_2b_flag_to_source_of_surface_emissivity_data` | long_name | Flag to source of surface emissivity data |  |
| `acm_com_2b_surface_emissivity_quality_status` | `acm_com_2b_quality_status_of_surface_emissivity` | long_name | Quality status of surface emissivity |  |
| `acm_com_2b_surface_emissivity_table` | `acm_com_2b_emissivity_averaged_over_wavelength` | long_name | Emissivity averaged over wavelength | 1 |
| `acm_com_2b_surface_temperature_quality_status` | `acm_com_2b_quality_status_of_surface_temperature` | long_name | Quality status of surface temperature |  |
| `acm_com_2b_trace_gases_quality_status` | `acm_com_2b_quality_status_of_trace_gases` | long_name | Quality status of trace gases |  |
| `acm_com_2b_tropopause_height` | `acm_com_2b_tropopause_height_from_a_ebd` | long_name | Tropopause height from A-EBD | m |
| `acm_com_2b_volume_mixing_ratio_layer_mean_CCL4` | `acm_com_2b_tetrachloromethane_volume_mixing_ratio` | long_name | Tetrachloromethane volume mixing ratio (ppmv) | mol mol-1 |
| `acm_com_2b_volume_mixing_ratio_layer_mean_CFC11` | `acm_com_2b_cfc_11_volume_mixing_ratio` | long_name | CFC-11 volume mixing ratio (ppv) | mol mol-1 |
| `acm_com_2b_volume_mixing_ratio_layer_mean_CFC12` | `acm_com_2b_cfc_12_volume_mixing_ratio` | long_name | CFC-12 volume mixing ratio (ppmv) | mol mol-1 |
| `acm_com_2b_volume_mixing_ratio_layer_mean_CFC22` | `acm_com_2b_cfc_22_volume_mixing_ratio` | long_name | CFC-22 volume mixing ratio (ppmv) | mol mol-1 |
| `acm_com_2b_volume_mixing_ratio_layer_mean_CH4` | `acm_com_2b_methane_volume_mixing_ratio` | long_name | Methane volume mixing ratio (ppmv) | mol mol-1 |
| `acm_com_2b_volume_mixing_ratio_layer_mean_CO2` | `acm_com_2b_carbon_dioxide_volume_mixing_ratio` | long_name | Carbon dioxide volume mixing ratio (ppv) | mol mol-1 |
| `acm_com_2b_volume_mixing_ratio_layer_mean_N2O` | `acm_com_2b_nitrous_oxide_volume_mixing_ratio` | long_name | Nitrous oxide volume mixing ratio (ppmv) | mol mol-1 |
| `acm_com_2b_volume_mixing_ratio_layer_mean_O2` | `acm_com_2b_molecular_oxygen_volume_mixing_ratio` | long_name | Molecular Oxygen volume mixing ratio (ppv) | mol mol-1 |
| `acm_com_2b_volume_mixing_ratio_layer_mean_O3` | `acm_com_2b_ozone_volume_mixing_ratio` | long_name | Ozone volume mixing ratio (ppmv) | mol mol-1 |
| `acm_com_2b_wavenumbers_thermal_surface_emissivity` | `acm_com_2b_lw_emissivity_central_wavenumbers_for_bands` | long_name | LW emissivity central wavenumbers for bands | m-1 |
| `acm_com_2b_wind_speed_at_10_meters` | `acm_com_2b_wind_speed_at_10m_above_surface` | long_name | Wind speed at 10m above surface | m s-1 |
| `acm_rt_2b_BBR_along_track_view` | `acm_rt_2b_bbr_radiance_index` | long_name | BBR radiance index | 1 |
| `acm_rt_2b_atmosphere_classification_1D_RT` | `acm_rt_2b_atmosphere_classification_1_d_rt` | tidied | Atmosphere classification for 1D radiative transfer | 1 |
| `acm_rt_2b_atmosphere_classification_3D_RT` | `acm_rt_2b_atmosphere_classification_3_d_rt` | tidied | Atmosphere classification for 3D radiative transfer | 1 |
| `acm_rt_2b_flux_down_diffuse_solar_3d_surface_all_sky` | `acm_rt_2b_3_d_downward_diffuse_solar_fluxes_for_all_sky` | long_name | 3D downward diffuse solar fluxes for all-sky | W m-2 |
| `acm_rt_2b_flux_down_direct_solar_3d_surface_all_sky` | `acm_rt_2b_3_d_downward_direct_solar_fluxes_for_all_sky` | long_name | 3D downward direct solar fluxes for all-sky | W m-2 |
| `acm_rt_2b_flux_down_solar_1d_all_sky` | `acm_rt_2b_1_d_sw_downwelling_fluxes_for_all_sky` | long_name | 1D SW downwelling fluxes for all-sky | W m-2 |
| `acm_rt_2b_flux_down_solar_1d_clear_sky` | `acm_rt_2b_1_d_sw_downwelling_fluxes_for_clear_sky` | long_name | 1D SW downwelling fluxes for clear-sky | W m-2 |
| `acm_rt_2b_flux_down_solar_1d_pristine_sky` | `acm_rt_2b_1_d_sw_downwelling_fluxes_for_pristine_sky` | long_name | 1D SW downwelling fluxes for pristine sky | W m-2 |
| `acm_rt_2b_flux_down_solar_3d_all_sky` | `acm_rt_2b_3_d_downward_solar_fluxes_for_all_sky` | long_name | 3D downward solar fluxes for all-sky | W m-2 |
| `acm_rt_2b_flux_down_solar_3d_all_sky_error_a` | `acm_rt_2b_3_d_downward_solar_fluxes_for_all_sky_type_a` | long_name | 3D downward solar fluxes for all-sky, type a | W m-2 |
| `acm_rt_2b_flux_down_solar_3d_all_sky_error_b` | `acm_rt_2b_3_d_downward_solar_fluxes_for_all_sky_type_b` | long_name | 3D downward solar fluxes for all-sky, type b | W m-2 |
| `acm_rt_2b_flux_down_thermal_1d_all_sky` | `acm_rt_2b_flux_down_thermal_1d_all_sky` | clash | 1D LW downwelling fluxes for clear-sky | W m-2 |
| `acm_rt_2b_flux_down_thermal_1d_clear_sky` | `acm_rt_2b_flux_down_thermal_1d_clear_sky` | clash | 1D LW downwelling fluxes for clear-sky | W m-2 |
| `acm_rt_2b_flux_down_thermal_1d_pristine_sky` | `acm_rt_2b_1_d_lw_downwelling_fluxes_for_pristine_sky` | long_name | 1D LW downwelling fluxes for pristine-sky | W m-2 |
| `acm_rt_2b_flux_up_solar_1d_all_sky` | `acm_rt_2b_1_d_sw_upwelling_fluxes_for_all_sky` | long_name | 1D SW upwelling fluxes for all-sky | W m-2 |
| `acm_rt_2b_flux_up_solar_1d_clear_sky` | `acm_rt_2b_1_d_sw_upwelling_fluxes_for_clear_sky` | long_name | 1D SW upwelling fluxes for clear-sky | W m-2 |
| `acm_rt_2b_flux_up_solar_1d_pristine_sky` | `acm_rt_2b_1_d_sw_upwelling_fluxes_for_pristine_sky` | long_name | 1D SW upwelling fluxes for pristine-sky | W m-2 |
| `acm_rt_2b_flux_up_solar_3d_all_sky` | `acm_rt_2b_3_d_upward_solar_fluxes_for_all_sky` | long_name | 3D upward solar fluxes for all-sky | W m-2 |
| `acm_rt_2b_flux_up_thermal_1d_all_sky` | `acm_rt_2b_1_d_lw_upwelling_fluxes_for_all_sky` | long_name | 1D LW upwelling fluxes for all-sky | W m-2 |
| `acm_rt_2b_flux_up_thermal_1d_clear_sky` | `acm_rt_2b_1_d_lw_upwelling_fluxes_for_clear_sky` | long_name | 1D LW upwelling fluxes for clear-sky | W m-2 |
| `acm_rt_2b_flux_up_thermal_1d_pristine_sky` | `acm_rt_2b_1_d_lw_upwelling_fluxes_for_pristine_sky` | long_name | 1D LW upwelling fluxes for pristine-sky | W m-2 |
| `acm_rt_2b_flux_up_thermal_3d_reference_height_all_sky` | `acm_rt_2b_3_d_lw_upwelling_flux_at_reference_altitude` | long_name | 3D LW upwelling flux at reference altitude | W m-2 |
| `acm_rt_2b_geoid_offset` | `acm_rt_2b_offset_between_geoid_egm96_and_ellipsoid_wgs84` | long_name | Offset between geoid EGM96 and ellipsoid WGS84 | m |
| `acm_rt_2b_header_ANXLongitude` | `acm_rt_2b_header_anx_longitude` | tidied |  |  |
| `acm_rt_2b_header_ANXTime` | `acm_rt_2b_header_anx_time` | tidied |  |  |
| `acm_rt_2b_header_File_Class` | `acm_rt_2b_header_File_Class` | clash |  |  |
| `acm_rt_2b_header_File_Description` | `acm_rt_2b_header_file_description` | tidied |  |  |
| `acm_rt_2b_header_File_Name` | `acm_rt_2b_header_file_name` | tidied |  |  |
| `acm_rt_2b_header_File_Type` | `acm_rt_2b_header_file_type` | tidied |  |  |
| `acm_rt_2b_header_File_Version` | `acm_rt_2b_header_file_version` | tidied |  |  |
| `acm_rt_2b_header_Mission` | `acm_rt_2b_header_mission` | tidied |  |  |
| `acm_rt_2b_header_Notes` | `acm_rt_2b_header_notes` | tidied |  |  |
| `acm_rt_2b_header_Source_Creation_Date` | `acm_rt_2b_header_source_creation_date` | tidied |  |  |
| `acm_rt_2b_header_Source_Creator` | `acm_rt_2b_header_source_creator` | tidied |  |  |
| `acm_rt_2b_header_Source_Creator_Version` | `acm_rt_2b_header_source_creator_version` | tidied |  |  |
| `acm_rt_2b_header_Source_System` | `acm_rt_2b_header_source_system` | tidied |  |  |
| `acm_rt_2b_header_Validity_Period_Validity_Start` | `acm_rt_2b_header_validity_period_validity_start` | tidied |  |  |
| `acm_rt_2b_header_Validity_Period_Validity_Stop` | `acm_rt_2b_header_validity_period_validity_stop` | tidied |  |  |
| `acm_rt_2b_header_acquisitionStation` | `acm_rt_2b_header_acquisition_station` | tidied |  |  |
| `acm_rt_2b_header_degradedProductQualityFlag` | `acm_rt_2b_header_degraded_product_quality_flag` | tidied |  |  |
| `acm_rt_2b_header_executableMajorVersion` | `acm_rt_2b_header_executable_major_version` | tidied |  |  |
| `acm_rt_2b_header_executableMinorVersion` | `acm_rt_2b_header_executable_minor_version` | tidied |  |  |
| `acm_rt_2b_header_fileCategory` | `acm_rt_2b_header_file_category` | tidied |  |  |
| `acm_rt_2b_header_fileClass` | `acm_rt_2b_header_fileClass` | clash |  |  |
| `acm_rt_2b_header_formatMajorVersion` | `acm_rt_2b_header_format_major_version` | tidied |  |  |
| `acm_rt_2b_header_formatMinorVersion` | `acm_rt_2b_header_format_minor_version` | tidied |  |  |
| `acm_rt_2b_header_frameID` | `acm_rt_2b_header_frame_id` | tidied |  |  |
| `acm_rt_2b_header_frameStartCoordinates_geographicLatitude` | `acm_rt_2b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `acm_rt_2b_header_frameStartCoordinates_geographicLongitude` | `acm_rt_2b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `acm_rt_2b_header_frameStartMargin` | `acm_rt_2b_header_frame_start_margin` | tidied |  |  |
| `acm_rt_2b_header_frameStartTime` | `acm_rt_2b_header_frame_start_time` | tidied |  |  |
| `acm_rt_2b_header_frameStopCoordinates_geographicLatitude` | `acm_rt_2b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `acm_rt_2b_header_frameStopCoordinates_geographicLongitude` | `acm_rt_2b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `acm_rt_2b_header_frameStopMargin` | `acm_rt_2b_header_frame_stop_margin` | tidied |  |  |
| `acm_rt_2b_header_frameStopTime` | `acm_rt_2b_header_frame_stop_time` | tidied |  |  |
| `acm_rt_2b_header_meanAnomaly` | `acm_rt_2b_header_mean_anomaly` | tidied |  |  |
| `acm_rt_2b_header_missionID` | `acm_rt_2b_header_mission_id` | tidied |  |  |
| `acm_rt_2b_header_orbitEccentricity` | `acm_rt_2b_header_orbit_eccentricity` | tidied |  |  |
| `acm_rt_2b_header_orbitInclination` | `acm_rt_2b_header_orbit_inclination` | tidied |  |  |
| `acm_rt_2b_header_orbitNumber` | `acm_rt_2b_header_orbit_number` | tidied |  |  |
| `acm_rt_2b_header_orbitSemiMajorAxis` | `acm_rt_2b_header_orbit_semi_major_axis` | tidied |  |  |
| `acm_rt_2b_header_originalProductName` | `acm_rt_2b_header_original_product_name` | tidied |  |  |
| `acm_rt_2b_header_perigeeArgument` | `acm_rt_2b_header_perigee_argument` | tidied |  |  |
| `acm_rt_2b_header_processingCentre` | `acm_rt_2b_header_processing_centre` | tidied |  |  |
| `acm_rt_2b_header_processingStartTime` | `acm_rt_2b_header_processing_start_time` | tidied |  |  |
| `acm_rt_2b_header_processingStopTime` | `acm_rt_2b_header_processing_stop_time` | tidied |  |  |
| `acm_rt_2b_header_processorMajorVersion` | `acm_rt_2b_header_processor_major_version` | tidied |  |  |
| `acm_rt_2b_header_processorMinorVersion` | `acm_rt_2b_header_processor_minor_version` | tidied |  |  |
| `acm_rt_2b_header_processorName` | `acm_rt_2b_header_processor_name` | tidied |  |  |
| `acm_rt_2b_header_productLevel` | `acm_rt_2b_header_product_level` | tidied |  |  |
| `acm_rt_2b_header_productName` | `acm_rt_2b_header_product_name` | tidied |  |  |
| `acm_rt_2b_header_productType` | `acm_rt_2b_header_product_type` | tidied |  |  |
| `acm_rt_2b_header_rightAscension` | `acm_rt_2b_header_right_ascension` | tidied |  |  |
| `acm_rt_2b_header_sensingStartTime` | `acm_rt_2b_header_sensing_start_time` | tidied |  |  |
| `acm_rt_2b_header_sensingStopTime` | `acm_rt_2b_header_sensing_stop_time` | tidied |  |  |
| `acm_rt_2b_header_stateVectorSource` | `acm_rt_2b_header_state_vector_source` | tidied |  |  |
| `acm_rt_2b_header_stateVectorTime` | `acm_rt_2b_header_state_vector_time` | tidied |  |  |
| `acm_rt_2b_header_subsettedProduct` | `acm_rt_2b_header_subsetted_product` | tidied |  |  |
| `acm_rt_2b_header_xPosition` | `acm_rt_2b_header_x_position` | tidied |  |  |
| `acm_rt_2b_header_xVelocity` | `acm_rt_2b_header_x_velocity` | tidied |  |  |
| `acm_rt_2b_header_yPosition` | `acm_rt_2b_header_y_position` | tidied |  |  |
| `acm_rt_2b_header_yVelocity` | `acm_rt_2b_header_y_velocity` | tidied |  |  |
| `acm_rt_2b_header_zPosition` | `acm_rt_2b_header_z_position` | tidied |  |  |
| `acm_rt_2b_header_zVelocity` | `acm_rt_2b_header_z_velocity` | tidied |  |  |
| `acm_rt_2b_heating_rate_solar_1d_all_sky` | `acm_rt_2b_1_d_sw_heating_rates_for_all_sky` | long_name | 1D SW heating rates for all-sky | K day-1 |
| `acm_rt_2b_heating_rate_solar_1d_clear_sky` | `acm_rt_2b_1_d_sw_heating_rates_for_clear_sky` | long_name | 1D SW heating rates for clear-sky | K day-1 |
| `acm_rt_2b_heating_rate_solar_1d_pristine_sky` | `acm_rt_2b_1_d_sw_heating_rates_for_pristine_sky` | long_name | 1D SW heating rates for pristine-sky | K day-1 |
| `acm_rt_2b_heating_rate_solar_3d_all_sky` | `acm_rt_2b_3_d_solar_heating_rates_for_all_sky` | long_name | 3D solar heating rates for all-sky | K day-1 |
| `acm_rt_2b_heating_rate_thermal_1d_all_sky` | `acm_rt_2b_1_d_lw_heating_rates_for_all_sky` | long_name | 1D LW heating rates for all-sky | K day-1 |
| `acm_rt_2b_heating_rate_thermal_1d_clear_sky` | `acm_rt_2b_1_d_lw_heating_rates_for_clear_sky` | long_name | 1D LW heating rates for clear-sky | K day-1 |
| `acm_rt_2b_heating_rate_thermal_1d_pristine_sky` | `acm_rt_2b_1_d_lw_heating_rates_for_pristine_sky` | long_name | 1D LW heating rates for pristine-sky | K day-1 |
| `acm_rt_2b_height_layers` | `acm_rt_2b_height_of_layers` | long_name | Height of layers | m |
| `acm_rt_2b_height_levels` | `acm_rt_2b_height_of_levels` | long_name | Height of levels | m |
| `acm_rt_2b_latitude_active` | `acm_rt_2b_latitude_under_active_sensors` | long_name | Latitude under active sensors | degree_north |
| `acm_rt_2b_longitude_active` | `acm_rt_2b_longitude_under_active_sensor` | long_name | Longitude under active sensor | degree_east |
| `acm_rt_2b_one_dimensional_radiative_transfer_status` | `acm_rt_2b_status_of_radiation_computations` | long_name | status of radiation computations | 1 |
| `acm_rt_2b_quality_status` | `acm_rt_2b_overall_quality_status_for_radiative_transfer_calculations` | long_name | Overall quality status for radiative transfer calculations | 1 |
| `acm_rt_2b_radiance_solar_3d_all_sky` | `acm_rt_2b_3_d_solar_radiances_for_all_sky` | long_name | 3D solar radiances for all-sky | W m-2 sr-1 |
| `acm_rt_2b_radiance_solar_3d_all_sky_error_a` | `acm_rt_2b_3_d_solar_radiances_for_all_sky_error_type_a` | long_name | 3D solar radiances for all-sky, error type a | W m-2 sr-1 |
| `acm_rt_2b_radiance_solar_3d_all_sky_error_b` | `acm_rt_2b_3_d_solar_radiances_for_all_sky_error_type_b` | long_name | 3D solar radiances for all-sky, error type b | W m-2 sr-1 |
| `acm_rt_2b_radiance_thermal_3d_all_sky` | `acm_rt_2b_3_d_lw_radiances` | long_name | 3D LW radiances | W m-2 sr-1 |
| `acm_rt_2b_reference_height_3d_thermal_fluxes` | `acm_rt_2b_reference_altitude_for_upward_3_d_lw_fluxes` | long_name | Reference altitude for upward 3D LW fluxes | m |
| `acm_rt_2b_three_dimensional_radiative_transfer_status` | `acm_rt_2b_status_of_3_d_radiative_transfer_calculations` | long_name | status of 3D radiative transfer calculations | 1 |
| `all_3d_2b_geoid_offset` | `all_3d_2b_offset_between_geoid_egm96_and_ellipsoid_wgs84` | long_name | Offset between geoid EGM96 and ellipsoid WGS84 | m |
| `all_3d_2b_header_ANXLongitude` | `all_3d_2b_header_anx_longitude` | tidied |  |  |
| `all_3d_2b_header_ANXTime` | `all_3d_2b_header_anx_time` | tidied |  |  |
| `all_3d_2b_header_File_Class` | `all_3d_2b_header_File_Class` | clash |  |  |
| `all_3d_2b_header_File_Description` | `all_3d_2b_header_file_description` | tidied |  |  |
| `all_3d_2b_header_File_Name` | `all_3d_2b_header_file_name` | tidied |  |  |
| `all_3d_2b_header_File_Type` | `all_3d_2b_header_file_type` | tidied |  |  |
| `all_3d_2b_header_File_Version` | `all_3d_2b_header_file_version` | tidied |  |  |
| `all_3d_2b_header_Mission` | `all_3d_2b_header_mission` | tidied |  |  |
| `all_3d_2b_header_Notes` | `all_3d_2b_header_notes` | tidied |  |  |
| `all_3d_2b_header_Source_Creation_Date` | `all_3d_2b_header_source_creation_date` | tidied |  |  |
| `all_3d_2b_header_Source_Creator` | `all_3d_2b_header_source_creator` | tidied |  |  |
| `all_3d_2b_header_Source_Creator_Version` | `all_3d_2b_header_source_creator_version` | tidied |  |  |
| `all_3d_2b_header_Source_System` | `all_3d_2b_header_source_system` | tidied |  |  |
| `all_3d_2b_header_Validity_Period_Validity_Start` | `all_3d_2b_header_validity_period_validity_start` | tidied |  |  |
| `all_3d_2b_header_Validity_Period_Validity_Stop` | `all_3d_2b_header_validity_period_validity_stop` | tidied |  |  |
| `all_3d_2b_header_acquisitionStation` | `all_3d_2b_header_acquisition_station` | tidied |  |  |
| `all_3d_2b_header_degradedProductQualityFlag` | `all_3d_2b_header_degraded_product_quality_flag` | tidied |  |  |
| `all_3d_2b_header_executableMajorVersion` | `all_3d_2b_header_executable_major_version` | tidied |  |  |
| `all_3d_2b_header_executableMinorVersion` | `all_3d_2b_header_executable_minor_version` | tidied |  |  |
| `all_3d_2b_header_fileCategory` | `all_3d_2b_header_file_category` | tidied |  |  |
| `all_3d_2b_header_fileClass` | `all_3d_2b_header_fileClass` | clash |  |  |
| `all_3d_2b_header_formatMajorVersion` | `all_3d_2b_header_format_major_version` | tidied |  |  |
| `all_3d_2b_header_formatMinorVersion` | `all_3d_2b_header_format_minor_version` | tidied |  |  |
| `all_3d_2b_header_frameID` | `all_3d_2b_header_frame_id` | tidied |  |  |
| `all_3d_2b_header_frameStartCoordinates_geographicLatitude` | `all_3d_2b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `all_3d_2b_header_frameStartCoordinates_geographicLongitude` | `all_3d_2b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `all_3d_2b_header_frameStartMargin` | `all_3d_2b_header_frame_start_margin` | tidied |  |  |
| `all_3d_2b_header_frameStartTime` | `all_3d_2b_header_frame_start_time` | tidied |  |  |
| `all_3d_2b_header_frameStopCoordinates_geographicLatitude` | `all_3d_2b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `all_3d_2b_header_frameStopCoordinates_geographicLongitude` | `all_3d_2b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `all_3d_2b_header_frameStopMargin` | `all_3d_2b_header_frame_stop_margin` | tidied |  |  |
| `all_3d_2b_header_frameStopTime` | `all_3d_2b_header_frame_stop_time` | tidied |  |  |
| `all_3d_2b_header_meanAnomaly` | `all_3d_2b_header_mean_anomaly` | tidied |  |  |
| `all_3d_2b_header_missionID` | `all_3d_2b_header_mission_id` | tidied |  |  |
| `all_3d_2b_header_orbitEccentricity` | `all_3d_2b_header_orbit_eccentricity` | tidied |  |  |
| `all_3d_2b_header_orbitInclination` | `all_3d_2b_header_orbit_inclination` | tidied |  |  |
| `all_3d_2b_header_orbitNumber` | `all_3d_2b_header_orbit_number` | tidied |  |  |
| `all_3d_2b_header_orbitSemiMajorAxis` | `all_3d_2b_header_orbit_semi_major_axis` | tidied |  |  |
| `all_3d_2b_header_originalProductName` | `all_3d_2b_header_original_product_name` | tidied |  |  |
| `all_3d_2b_header_perigeeArgument` | `all_3d_2b_header_perigee_argument` | tidied |  |  |
| `all_3d_2b_header_processingCentre` | `all_3d_2b_header_processing_centre` | tidied |  |  |
| `all_3d_2b_header_processingStartTime` | `all_3d_2b_header_processing_start_time` | tidied |  |  |
| `all_3d_2b_header_processingStopTime` | `all_3d_2b_header_processing_stop_time` | tidied |  |  |
| `all_3d_2b_header_processorMajorVersion` | `all_3d_2b_header_processor_major_version` | tidied |  |  |
| `all_3d_2b_header_processorMinorVersion` | `all_3d_2b_header_processor_minor_version` | tidied |  |  |
| `all_3d_2b_header_processorName` | `all_3d_2b_header_processor_name` | tidied |  |  |
| `all_3d_2b_header_productLevel` | `all_3d_2b_header_product_level` | tidied |  |  |
| `all_3d_2b_header_productName` | `all_3d_2b_header_product_name` | tidied |  |  |
| `all_3d_2b_header_productType` | `all_3d_2b_header_product_type` | tidied |  |  |
| `all_3d_2b_header_rightAscension` | `all_3d_2b_header_right_ascension` | tidied |  |  |
| `all_3d_2b_header_sensingStartTime` | `all_3d_2b_header_sensing_start_time` | tidied |  |  |
| `all_3d_2b_header_sensingStopTime` | `all_3d_2b_header_sensing_stop_time` | tidied |  |  |
| `all_3d_2b_header_stateVectorSource` | `all_3d_2b_header_state_vector_source` | tidied |  |  |
| `all_3d_2b_header_stateVectorTime` | `all_3d_2b_header_state_vector_time` | tidied |  |  |
| `all_3d_2b_header_subsettedProduct` | `all_3d_2b_header_subsetted_product` | tidied |  |  |
| `all_3d_2b_header_xPosition` | `all_3d_2b_header_x_position` | tidied |  |  |
| `all_3d_2b_header_xVelocity` | `all_3d_2b_header_x_velocity` | tidied |  |  |
| `all_3d_2b_header_yPosition` | `all_3d_2b_header_y_position` | tidied |  |  |
| `all_3d_2b_header_yVelocity` | `all_3d_2b_header_y_velocity` | tidied |  |  |
| `all_3d_2b_header_zPosition` | `all_3d_2b_header_z_position` | tidied |  |  |
| `all_3d_2b_header_zVelocity` | `all_3d_2b_header_z_velocity` | tidied |  |  |
| `all_3d_2b_index_construction` | `all_3d_2b_index_of_the_donor_pixel_along_the_nadir` | long_name | Index of the donor pixel along the nadir |  |
| `all_3d_2b_index_construction_quality_status` | `all_3d_2b_quality_of_each_off_nadir_js_gh_pixel` | long_name | Quality of each off-nadir JSGh pixel |  |
| `all_3d_2b_radiance` | `all_3d_2b_radiance` | kept | Radiance | W m-2 sr-1 |
| `all_3d_2b_screening_1D_status` | `all_3d_2b_screening_1_d_status` | tidied | Screening status for 1D assessment domain for all-sky condit |  |
| `all_3d_2b_screening_3D_status` | `all_3d_2b_screening_3_d_status` | tidied | Screening status for 3D assessment domain for all-sky condit |  |
| `all_3d_2b_start_along_track_assessment_domain_1d` | `all_3d_2b_sampled_1_d_assessment_domain` | long_name | Sampled 1D assessment domain |  |
| `all_df_2b_BBR_along_track_view` | `all_df_2b_bbr_radiance_index` | long_name | BBR radiance index | 1 |
| `all_df_2b_BBR_effective_flux_solar` | `all_df_2b_combined_solar_upwelling_flux_from_bma_flx` | long_name | Combined solar upwelling flux from BMA-FLX | W m-2 |
| `all_df_2b_BBR_effective_flux_solar_error` | `all_df_2b_bbr_effective_flux_solar_error` | tidied | Combined solar upwelling flux uncertainity from BMA-FLX | W m-2 |
| `all_df_2b_BBR_effective_flux_thermal` | `all_df_2b_combined_thermal_upwelling_flux_from_bma_flx` | long_name | Combined thermal upwelling flux from BMA-FLX | W m-2 |
| `all_df_2b_BBR_effective_flux_thermal_error` | `all_df_2b_bbr_effective_flux_thermal_error` | tidied | Combined thermal upwelling flux uncertainity from BMA-FLX | W m-2 |
| `all_df_2b_BBR_flux_solar` | `all_df_2b_bbr_flux_solar` | tidied | Upwelling solar fluxes from BMA-FLX for each BBR view | W m-2 |
| `all_df_2b_BBR_flux_solar_error` | `all_df_2b_bbr_flux_solar_error` | tidied | Uncertainty in upwelling solar fluxes from BMA-FLX for each  | W m-2 |
| `all_df_2b_BBR_flux_thermal` | `all_df_2b_bbr_flux_thermal` | tidied | Upwelling thermal fluxes from BMA-FLX for each BBR view | W m-2 |
| `all_df_2b_BBR_flux_thermal_error` | `all_df_2b_bbr_flux_thermal_error` | tidied | Uncertainty in upwelling thermal fluxes from BMA-FLX for eac | W m-2 |
| `all_df_2b_BBR_radiance_solar` | `all_df_2b_upwelling_solar_radiances_from_bma_flx` | long_name | Upwelling solar radiances from BMA-FLX | W m-2 sr-1 |
| `all_df_2b_BBR_radiance_solar_error` | `all_df_2b_bbr_radiance_solar_error` | tidied | Uncertainty in upwelling solar radiances from BMA-FLX | W m-2 sr-1 |
| `all_df_2b_BBR_radiance_thermal` | `all_df_2b_upwelling_thermal_radiances_from_bma_flx` | long_name | Upwelling thermal radiances from BMA-FLX | W m-2 sr-1 |
| `all_df_2b_BBR_radiance_thermal_error` | `all_df_2b_bbr_radiance_thermal_error` | tidied | Uncertainty in upwelling thermal radiances from BMA-FLX | W m-2 sr-1 |
| `all_df_2b_BBR_solar_radiance_to_flux_factor` | `all_df_2b_bbr_solar_radiance_to_flux_factor` | tidied | Factor to convert upwelling solar radiances to fluxes for ea | 1 |
| `all_df_2b_BBR_thermal_radiance_to_flux_factor` | `all_df_2b_bbr_thermal_radiance_to_flux_factor` | tidied | Factor to convert the thermal radiances to fluxes for each v | 1 |
| `all_df_2b_atmosphere_classification_1D_RT` | `all_df_2b_atmosphere_classification_1_d_rt` | clash | Atmosphere indices (consistent with ACM-COM) | 1 |
| `all_df_2b_atmosphere_classification_3D_RT` | `all_df_2b_atmosphere_classification_3_d_rt` | clash | Atmosphere indices (consistent with ACM-COM) | 1 |
| `all_df_2b_delta_flux_solar` | `all_df_2b_delta_flux_for_solar_radiative_closure` | long_name | Delta flux for solar radiative closure | W m-2 |
| `all_df_2b_delta_flux_thermal` | `all_df_2b_delta_flux_for_thermal_radiative_closure` | long_name | Delta flux for thermal radiative closure | W m-2 |
| `all_df_2b_effective_flux_solar_3d` | `all_df_2b_combined_solar_upwelling_3_d_flux` | long_name | Combined solar upwelling 3D flux | W m-2 |
| `all_df_2b_effective_flux_thermal_3d` | `all_df_2b_combined_thermal_upwelling_3_d` | long_name | Combined thermal upwelling 3D | W m-2 |
| `all_df_2b_flux_solar_1d` | `all_df_2b_solar_all_sky_upwelling_1_d_flux_from_acm_rt` | long_name | Solar all-sky upwelling 1D flux (assessment domain mean) fro | W m-2 |
| `all_df_2b_flux_solar_3d` | `all_df_2b_solar_all_sky_3_d_fluxes_for_each_bbr_view` | long_name | Solar all-sky 3D fluxes for each BBR view (derived from 3D r | W m-2 |
| `all_df_2b_flux_thermal_3d` | `all_df_2b_thermal_all_sky_3_d_fluxes_for_each_bbr_view` | long_name | Thermal all-sky 3D fluxes for each BBR view (derived from 3D | W m-2 |
| `all_df_2b_geoid_offset` | `all_df_2b_offset_between_geoid_egm96_and_ellipsoid_wgs84` | long_name | Offset between geoid EGM96 and ellipsoid WGS84 | m |
| `all_df_2b_header_ANXLongitude` | `all_df_2b_header_anx_longitude` | tidied |  |  |
| `all_df_2b_header_ANXTime` | `all_df_2b_header_anx_time` | tidied |  |  |
| `all_df_2b_header_File_Class` | `all_df_2b_header_File_Class` | clash |  |  |
| `all_df_2b_header_File_Description` | `all_df_2b_header_file_description` | tidied |  |  |
| `all_df_2b_header_File_Name` | `all_df_2b_header_file_name` | tidied |  |  |
| `all_df_2b_header_File_Type` | `all_df_2b_header_file_type` | tidied |  |  |
| `all_df_2b_header_File_Version` | `all_df_2b_header_file_version` | tidied |  |  |
| `all_df_2b_header_Mission` | `all_df_2b_header_mission` | tidied |  |  |
| `all_df_2b_header_Notes` | `all_df_2b_header_notes` | tidied |  |  |
| `all_df_2b_header_Source_Creation_Date` | `all_df_2b_header_source_creation_date` | tidied |  |  |
| `all_df_2b_header_Source_Creator` | `all_df_2b_header_source_creator` | tidied |  |  |
| `all_df_2b_header_Source_Creator_Version` | `all_df_2b_header_source_creator_version` | tidied |  |  |
| `all_df_2b_header_Source_System` | `all_df_2b_header_source_system` | tidied |  |  |
| `all_df_2b_header_Validity_Period_Validity_Start` | `all_df_2b_header_validity_period_validity_start` | tidied |  |  |
| `all_df_2b_header_Validity_Period_Validity_Stop` | `all_df_2b_header_validity_period_validity_stop` | tidied |  |  |
| `all_df_2b_header_acquisitionStation` | `all_df_2b_header_acquisition_station` | tidied |  |  |
| `all_df_2b_header_degradedProductQualityFlag` | `all_df_2b_header_degraded_product_quality_flag` | tidied |  |  |
| `all_df_2b_header_executableMajorVersion` | `all_df_2b_header_executable_major_version` | tidied |  |  |
| `all_df_2b_header_executableMinorVersion` | `all_df_2b_header_executable_minor_version` | tidied |  |  |
| `all_df_2b_header_fileCategory` | `all_df_2b_header_file_category` | tidied |  |  |
| `all_df_2b_header_fileClass` | `all_df_2b_header_fileClass` | clash |  |  |
| `all_df_2b_header_formatMajorVersion` | `all_df_2b_header_format_major_version` | tidied |  |  |
| `all_df_2b_header_formatMinorVersion` | `all_df_2b_header_format_minor_version` | tidied |  |  |
| `all_df_2b_header_frameID` | `all_df_2b_header_frame_id` | tidied |  |  |
| `all_df_2b_header_frameStartCoordinates_geographicLatitude` | `all_df_2b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `all_df_2b_header_frameStartCoordinates_geographicLongitude` | `all_df_2b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `all_df_2b_header_frameStartMargin` | `all_df_2b_header_frame_start_margin` | tidied |  |  |
| `all_df_2b_header_frameStartTime` | `all_df_2b_header_frame_start_time` | tidied |  |  |
| `all_df_2b_header_frameStopCoordinates_geographicLatitude` | `all_df_2b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `all_df_2b_header_frameStopCoordinates_geographicLongitude` | `all_df_2b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `all_df_2b_header_frameStopMargin` | `all_df_2b_header_frame_stop_margin` | tidied |  |  |
| `all_df_2b_header_frameStopTime` | `all_df_2b_header_frame_stop_time` | tidied |  |  |
| `all_df_2b_header_meanAnomaly` | `all_df_2b_header_mean_anomaly` | tidied |  |  |
| `all_df_2b_header_missionID` | `all_df_2b_header_mission_id` | tidied |  |  |
| `all_df_2b_header_orbitEccentricity` | `all_df_2b_header_orbit_eccentricity` | tidied |  |  |
| `all_df_2b_header_orbitInclination` | `all_df_2b_header_orbit_inclination` | tidied |  |  |
| `all_df_2b_header_orbitNumber` | `all_df_2b_header_orbit_number` | tidied |  |  |
| `all_df_2b_header_orbitSemiMajorAxis` | `all_df_2b_header_orbit_semi_major_axis` | tidied |  |  |
| `all_df_2b_header_originalProductName` | `all_df_2b_header_original_product_name` | tidied |  |  |
| `all_df_2b_header_perigeeArgument` | `all_df_2b_header_perigee_argument` | tidied |  |  |
| `all_df_2b_header_processingCentre` | `all_df_2b_header_processing_centre` | tidied |  |  |
| `all_df_2b_header_processingStartTime` | `all_df_2b_header_processing_start_time` | tidied |  |  |
| `all_df_2b_header_processingStopTime` | `all_df_2b_header_processing_stop_time` | tidied |  |  |
| `all_df_2b_header_processorMajorVersion` | `all_df_2b_header_processor_major_version` | tidied |  |  |
| `all_df_2b_header_processorMinorVersion` | `all_df_2b_header_processor_minor_version` | tidied |  |  |
| `all_df_2b_header_processorName` | `all_df_2b_header_processor_name` | tidied |  |  |
| `all_df_2b_header_productLevel` | `all_df_2b_header_product_level` | tidied |  |  |
| `all_df_2b_header_productName` | `all_df_2b_header_product_name` | tidied |  |  |
| `all_df_2b_header_productType` | `all_df_2b_header_product_type` | tidied |  |  |
| `all_df_2b_header_rightAscension` | `all_df_2b_header_right_ascension` | tidied |  |  |
| `all_df_2b_header_sensingStartTime` | `all_df_2b_header_sensing_start_time` | tidied |  |  |
| `all_df_2b_header_sensingStopTime` | `all_df_2b_header_sensing_stop_time` | tidied |  |  |
| `all_df_2b_header_stateVectorSource` | `all_df_2b_header_state_vector_source` | tidied |  |  |
| `all_df_2b_header_stateVectorTime` | `all_df_2b_header_state_vector_time` | tidied |  |  |
| `all_df_2b_header_subsettedProduct` | `all_df_2b_header_subsetted_product` | tidied |  |  |
| `all_df_2b_header_xPosition` | `all_df_2b_header_x_position` | tidied |  |  |
| `all_df_2b_header_xVelocity` | `all_df_2b_header_x_velocity` | tidied |  |  |
| `all_df_2b_header_yPosition` | `all_df_2b_header_y_position` | tidied |  |  |
| `all_df_2b_header_yVelocity` | `all_df_2b_header_y_velocity` | tidied |  |  |
| `all_df_2b_header_zPosition` | `all_df_2b_header_z_position` | tidied |  |  |
| `all_df_2b_header_zVelocity` | `all_df_2b_header_z_velocity` | tidied |  |  |
| `all_df_2b_quality_status` | `all_df_2b_overall_quality_status_for_radiative_closure_calculations` | long_name | Overall quality status for radiative closure calculations | 1 |
| `am_acd_2b_aerosol_angstrom_exponent` | `am_acd_2b_angstroem_exponent` | long_name | Angstroem exponent (355/670, 670/865) | 1 |
| `am_acd_2b_aerosol_angstrom_exponent_error` | `am_acd_2b_error_of_the_angstroem_exponent` | long_name | error of the Angstroem exponent (355/670, 670/865) | 1 |
| `am_acd_2b_aerosol_dominant_type_ATLID` | `am_acd_2b_aerosol_dominant_type_atlid` | tidied | dominant aerosol type from A-ALD & A-TC:\\n\\t 1: Dust\\n\\t |  |
| `am_acd_2b_aerosol_type_quality` | `am_acd_2b_quality_indicator_for_aerosol_type` | long_name | quality indicator for aerosol type |  |
| `am_acd_2b_geoid_offset` | `am_acd_2b_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `am_acd_2b_header_ANXLongitude` | `am_acd_2b_header_anx_longitude` | tidied |  |  |
| `am_acd_2b_header_ANXTime` | `am_acd_2b_header_anx_time` | tidied |  |  |
| `am_acd_2b_header_File_Class` | `am_acd_2b_header_File_Class` | clash |  |  |
| `am_acd_2b_header_File_Description` | `am_acd_2b_header_file_description` | tidied |  |  |
| `am_acd_2b_header_File_Name` | `am_acd_2b_header_file_name` | tidied |  |  |
| `am_acd_2b_header_File_Type` | `am_acd_2b_header_file_type` | tidied |  |  |
| `am_acd_2b_header_File_Version` | `am_acd_2b_header_file_version` | tidied |  |  |
| `am_acd_2b_header_Mission` | `am_acd_2b_header_mission` | tidied |  |  |
| `am_acd_2b_header_Notes` | `am_acd_2b_header_notes` | tidied |  |  |
| `am_acd_2b_header_Source_Creation_Date` | `am_acd_2b_header_source_creation_date` | tidied |  |  |
| `am_acd_2b_header_Source_Creator` | `am_acd_2b_header_source_creator` | tidied |  |  |
| `am_acd_2b_header_Source_Creator_Version` | `am_acd_2b_header_source_creator_version` | tidied |  |  |
| `am_acd_2b_header_Source_System` | `am_acd_2b_header_source_system` | tidied |  |  |
| `am_acd_2b_header_Validity_Period_Validity_Start` | `am_acd_2b_header_validity_period_validity_start` | tidied |  |  |
| `am_acd_2b_header_Validity_Period_Validity_Stop` | `am_acd_2b_header_validity_period_validity_stop` | tidied |  |  |
| `am_acd_2b_header_acquisitionStation` | `am_acd_2b_header_acquisition_station` | tidied |  |  |
| `am_acd_2b_header_degradedProductQualityFlag` | `am_acd_2b_header_degraded_product_quality_flag` | tidied |  |  |
| `am_acd_2b_header_executableMajorVersion` | `am_acd_2b_header_executable_major_version` | tidied |  |  |
| `am_acd_2b_header_executableMinorVersion` | `am_acd_2b_header_executable_minor_version` | tidied |  |  |
| `am_acd_2b_header_fileCategory` | `am_acd_2b_header_file_category` | tidied |  |  |
| `am_acd_2b_header_fileClass` | `am_acd_2b_header_fileClass` | clash |  |  |
| `am_acd_2b_header_formatMajorVersion` | `am_acd_2b_header_format_major_version` | tidied |  |  |
| `am_acd_2b_header_formatMinorVersion` | `am_acd_2b_header_format_minor_version` | tidied |  |  |
| `am_acd_2b_header_frameID` | `am_acd_2b_header_frame_id` | tidied |  |  |
| `am_acd_2b_header_frameStartCoordinates_geographicLatitude` | `am_acd_2b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `am_acd_2b_header_frameStartCoordinates_geographicLongitude` | `am_acd_2b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `am_acd_2b_header_frameStartMargin` | `am_acd_2b_header_frame_start_margin` | tidied |  |  |
| `am_acd_2b_header_frameStartTime` | `am_acd_2b_header_frame_start_time` | tidied |  |  |
| `am_acd_2b_header_frameStopCoordinates_geographicLatitude` | `am_acd_2b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `am_acd_2b_header_frameStopCoordinates_geographicLongitude` | `am_acd_2b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `am_acd_2b_header_frameStopMargin` | `am_acd_2b_header_frame_stop_margin` | tidied |  |  |
| `am_acd_2b_header_frameStopTime` | `am_acd_2b_header_frame_stop_time` | tidied |  |  |
| `am_acd_2b_header_meanAnomaly` | `am_acd_2b_header_mean_anomaly` | tidied |  |  |
| `am_acd_2b_header_missionID` | `am_acd_2b_header_mission_id` | tidied |  |  |
| `am_acd_2b_header_orbitEccentricity` | `am_acd_2b_header_orbit_eccentricity` | tidied |  |  |
| `am_acd_2b_header_orbitInclination` | `am_acd_2b_header_orbit_inclination` | tidied |  |  |
| `am_acd_2b_header_orbitNumber` | `am_acd_2b_header_orbit_number` | tidied |  |  |
| `am_acd_2b_header_orbitSemiMajorAxis` | `am_acd_2b_header_orbit_semi_major_axis` | tidied |  |  |
| `am_acd_2b_header_originalProductName` | `am_acd_2b_header_original_product_name` | tidied |  |  |
| `am_acd_2b_header_perigeeArgument` | `am_acd_2b_header_perigee_argument` | tidied |  |  |
| `am_acd_2b_header_processingCentre` | `am_acd_2b_header_processing_centre` | tidied |  |  |
| `am_acd_2b_header_processingStartTime` | `am_acd_2b_header_processing_start_time` | tidied |  |  |
| `am_acd_2b_header_processingStopTime` | `am_acd_2b_header_processing_stop_time` | tidied |  |  |
| `am_acd_2b_header_processorMajorVersion` | `am_acd_2b_header_processor_major_version` | tidied |  |  |
| `am_acd_2b_header_processorMinorVersion` | `am_acd_2b_header_processor_minor_version` | tidied |  |  |
| `am_acd_2b_header_processorName` | `am_acd_2b_header_processor_name` | tidied |  |  |
| `am_acd_2b_header_productLevel` | `am_acd_2b_header_product_level` | tidied |  |  |
| `am_acd_2b_header_productName` | `am_acd_2b_header_product_name` | tidied |  |  |
| `am_acd_2b_header_productType` | `am_acd_2b_header_product_type` | tidied |  |  |
| `am_acd_2b_header_rightAscension` | `am_acd_2b_header_right_ascension` | tidied |  |  |
| `am_acd_2b_header_sensingStartTime` | `am_acd_2b_header_sensing_start_time` | tidied |  |  |
| `am_acd_2b_header_sensingStopTime` | `am_acd_2b_header_sensing_stop_time` | tidied |  |  |
| `am_acd_2b_header_stateVectorSource` | `am_acd_2b_header_state_vector_source` | tidied |  |  |
| `am_acd_2b_header_stateVectorTime` | `am_acd_2b_header_state_vector_time` | tidied |  |  |
| `am_acd_2b_header_subsettedProduct` | `am_acd_2b_header_subsetted_product` | tidied |  |  |
| `am_acd_2b_header_xPosition` | `am_acd_2b_header_x_position` | tidied |  |  |
| `am_acd_2b_header_xVelocity` | `am_acd_2b_header_x_velocity` | tidied |  |  |
| `am_acd_2b_header_yPosition` | `am_acd_2b_header_y_position` | tidied |  |  |
| `am_acd_2b_header_yVelocity` | `am_acd_2b_header_y_velocity` | tidied |  |  |
| `am_acd_2b_header_zPosition` | `am_acd_2b_header_z_position` | tidied |  |  |
| `am_acd_2b_header_zVelocity` | `am_acd_2b_header_z_velocity` | tidied |  |  |
| `am_acd_2b_quality_status` | `am_acd_2b_quality_status_for_aerosol_columnar_descriptor` | long_name | quality status for aerosol columnar descriptor |  |
| `am_cth_2b_cloud_fraction` | `am_cth_2b_cloud_fraction_for_each_jsg_pixel` | long_name | cloud fraction for each JSG pixel | 1 |
| `am_cth_2b_cloud_top_height_MSI` | `am_cth_2b_msi_cloud_top_height_on_jsg` | long_name | MSI cloud top height on JSG | m |
| `am_cth_2b_cloud_top_height_difference_ATLID_MSI` | `am_cth_2b_cloud_top_height_difference_atlid_msi` | tidied | synergistic cloud top height difference (ATLID CTH minus MSI | m |
| `am_cth_2b_geoid_offset` | `am_cth_2b_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `am_cth_2b_header_ANXLongitude` | `am_cth_2b_header_anx_longitude` | tidied |  |  |
| `am_cth_2b_header_ANXTime` | `am_cth_2b_header_anx_time` | tidied |  |  |
| `am_cth_2b_header_File_Class` | `am_cth_2b_header_File_Class` | clash |  |  |
| `am_cth_2b_header_File_Description` | `am_cth_2b_header_file_description` | tidied |  |  |
| `am_cth_2b_header_File_Name` | `am_cth_2b_header_file_name` | tidied |  |  |
| `am_cth_2b_header_File_Type` | `am_cth_2b_header_file_type` | tidied |  |  |
| `am_cth_2b_header_File_Version` | `am_cth_2b_header_file_version` | tidied |  |  |
| `am_cth_2b_header_Mission` | `am_cth_2b_header_mission` | tidied |  |  |
| `am_cth_2b_header_Notes` | `am_cth_2b_header_notes` | tidied |  |  |
| `am_cth_2b_header_Source_Creation_Date` | `am_cth_2b_header_source_creation_date` | tidied |  |  |
| `am_cth_2b_header_Source_Creator` | `am_cth_2b_header_source_creator` | tidied |  |  |
| `am_cth_2b_header_Source_Creator_Version` | `am_cth_2b_header_source_creator_version` | tidied |  |  |
| `am_cth_2b_header_Source_System` | `am_cth_2b_header_source_system` | tidied |  |  |
| `am_cth_2b_header_Validity_Period_Validity_Start` | `am_cth_2b_header_validity_period_validity_start` | tidied |  |  |
| `am_cth_2b_header_Validity_Period_Validity_Stop` | `am_cth_2b_header_validity_period_validity_stop` | tidied |  |  |
| `am_cth_2b_header_acquisitionStation` | `am_cth_2b_header_acquisition_station` | tidied |  |  |
| `am_cth_2b_header_degradedProductQualityFlag` | `am_cth_2b_header_degraded_product_quality_flag` | tidied |  |  |
| `am_cth_2b_header_executableMajorVersion` | `am_cth_2b_header_executable_major_version` | tidied |  |  |
| `am_cth_2b_header_executableMinorVersion` | `am_cth_2b_header_executable_minor_version` | tidied |  |  |
| `am_cth_2b_header_fileCategory` | `am_cth_2b_header_file_category` | tidied |  |  |
| `am_cth_2b_header_fileClass` | `am_cth_2b_header_fileClass` | clash |  |  |
| `am_cth_2b_header_formatMajorVersion` | `am_cth_2b_header_format_major_version` | tidied |  |  |
| `am_cth_2b_header_formatMinorVersion` | `am_cth_2b_header_format_minor_version` | tidied |  |  |
| `am_cth_2b_header_frameID` | `am_cth_2b_header_frame_id` | tidied |  |  |
| `am_cth_2b_header_frameStartCoordinates_geographicLatitude` | `am_cth_2b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `am_cth_2b_header_frameStartCoordinates_geographicLongitude` | `am_cth_2b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `am_cth_2b_header_frameStartMargin` | `am_cth_2b_header_frame_start_margin` | tidied |  |  |
| `am_cth_2b_header_frameStartTime` | `am_cth_2b_header_frame_start_time` | tidied |  |  |
| `am_cth_2b_header_frameStopCoordinates_geographicLatitude` | `am_cth_2b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `am_cth_2b_header_frameStopCoordinates_geographicLongitude` | `am_cth_2b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `am_cth_2b_header_frameStopMargin` | `am_cth_2b_header_frame_stop_margin` | tidied |  |  |
| `am_cth_2b_header_frameStopTime` | `am_cth_2b_header_frame_stop_time` | tidied |  |  |
| `am_cth_2b_header_meanAnomaly` | `am_cth_2b_header_mean_anomaly` | tidied |  |  |
| `am_cth_2b_header_missionID` | `am_cth_2b_header_mission_id` | tidied |  |  |
| `am_cth_2b_header_orbitEccentricity` | `am_cth_2b_header_orbit_eccentricity` | tidied |  |  |
| `am_cth_2b_header_orbitInclination` | `am_cth_2b_header_orbit_inclination` | tidied |  |  |
| `am_cth_2b_header_orbitNumber` | `am_cth_2b_header_orbit_number` | tidied |  |  |
| `am_cth_2b_header_orbitSemiMajorAxis` | `am_cth_2b_header_orbit_semi_major_axis` | tidied |  |  |
| `am_cth_2b_header_originalProductName` | `am_cth_2b_header_original_product_name` | tidied |  |  |
| `am_cth_2b_header_perigeeArgument` | `am_cth_2b_header_perigee_argument` | tidied |  |  |
| `am_cth_2b_header_processingCentre` | `am_cth_2b_header_processing_centre` | tidied |  |  |
| `am_cth_2b_header_processingStartTime` | `am_cth_2b_header_processing_start_time` | tidied |  |  |
| `am_cth_2b_header_processingStopTime` | `am_cth_2b_header_processing_stop_time` | tidied |  |  |
| `am_cth_2b_header_processorMajorVersion` | `am_cth_2b_header_processor_major_version` | tidied |  |  |
| `am_cth_2b_header_processorMinorVersion` | `am_cth_2b_header_processor_minor_version` | tidied |  |  |
| `am_cth_2b_header_processorName` | `am_cth_2b_header_processor_name` | tidied |  |  |
| `am_cth_2b_header_productLevel` | `am_cth_2b_header_product_level` | tidied |  |  |
| `am_cth_2b_header_productName` | `am_cth_2b_header_product_name` | tidied |  |  |
| `am_cth_2b_header_productType` | `am_cth_2b_header_product_type` | tidied |  |  |
| `am_cth_2b_header_rightAscension` | `am_cth_2b_header_right_ascension` | tidied |  |  |
| `am_cth_2b_header_sensingStartTime` | `am_cth_2b_header_sensing_start_time` | tidied |  |  |
| `am_cth_2b_header_sensingStopTime` | `am_cth_2b_header_sensing_stop_time` | tidied |  |  |
| `am_cth_2b_header_stateVectorSource` | `am_cth_2b_header_state_vector_source` | tidied |  |  |
| `am_cth_2b_header_stateVectorTime` | `am_cth_2b_header_state_vector_time` | tidied |  |  |
| `am_cth_2b_header_subsettedProduct` | `am_cth_2b_header_subsetted_product` | tidied |  |  |
| `am_cth_2b_header_xPosition` | `am_cth_2b_header_x_position` | tidied |  |  |
| `am_cth_2b_header_xVelocity` | `am_cth_2b_header_x_velocity` | tidied |  |  |
| `am_cth_2b_header_yPosition` | `am_cth_2b_header_y_position` | tidied |  |  |
| `am_cth_2b_header_yVelocity` | `am_cth_2b_header_y_velocity` | tidied |  |  |
| `am_cth_2b_header_zPosition` | `am_cth_2b_header_z_position` | tidied |  |  |
| `am_cth_2b_header_zVelocity` | `am_cth_2b_header_z_velocity` | tidied |  |  |
| `am_cth_2b_quality_status` | `am_cth_2b_quality_status_for_the_synergistic_cloud_top_height` | long_name | quality status for the synergistic cloud top height |  |
| `atl_aer_2a_aerosol_classification_no_error` | `atl_aer_2a_aerosol_classification_no_error` | clash | aerosol classification (no errors) | 1 |
| `atl_aer_2a_aerosol_classification_prob` | `atl_aer_2a_aerosol_classification_prob` | clash | Aerosol classification probabilities | 10-2 |
| `atl_aer_2a_aerosol_classification_prob_no_errors` | `atl_aer_2a_aerosol_classification_prob_no_errors` | clash | Aerosol classification probabilities (no errors) | 10-2 |
| `atl_aer_2a_classification` | `atl_aer_2a_combined_aerosol_cloud_mask` | long_name | Combined aerosol/cloud mask | 1 |
| `atl_aer_2a_extended_data_quality_status` | `atl_aer_2a_additional_quality_status` | long_name | Additional Quality Status | 1 |
| `atl_aer_2a_header_ANXLongitude` | `atl_aer_2a_header_anx_longitude` | tidied |  |  |
| `atl_aer_2a_header_ANXTime` | `atl_aer_2a_header_anx_time` | tidied |  |  |
| `atl_aer_2a_header_File_Class` | `atl_aer_2a_header_File_Class` | clash |  |  |
| `atl_aer_2a_header_File_Description` | `atl_aer_2a_header_file_description` | tidied |  |  |
| `atl_aer_2a_header_File_Name` | `atl_aer_2a_header_file_name` | tidied |  |  |
| `atl_aer_2a_header_File_Type` | `atl_aer_2a_header_file_type` | tidied |  |  |
| `atl_aer_2a_header_File_Version` | `atl_aer_2a_header_file_version` | tidied |  |  |
| `atl_aer_2a_header_Mission` | `atl_aer_2a_header_mission` | tidied |  |  |
| `atl_aer_2a_header_Notes` | `atl_aer_2a_header_notes` | tidied |  |  |
| `atl_aer_2a_header_Source_Creation_Date` | `atl_aer_2a_header_source_creation_date` | tidied |  |  |
| `atl_aer_2a_header_Source_Creator` | `atl_aer_2a_header_source_creator` | tidied |  |  |
| `atl_aer_2a_header_Source_Creator_Version` | `atl_aer_2a_header_source_creator_version` | tidied |  |  |
| `atl_aer_2a_header_Source_System` | `atl_aer_2a_header_source_system` | tidied |  |  |
| `atl_aer_2a_header_Validity_Period_Validity_Start` | `atl_aer_2a_header_validity_period_validity_start` | tidied |  |  |
| `atl_aer_2a_header_Validity_Period_Validity_Stop` | `atl_aer_2a_header_validity_period_validity_stop` | tidied |  |  |
| `atl_aer_2a_header_acquisitionStation` | `atl_aer_2a_header_acquisition_station` | tidied |  |  |
| `atl_aer_2a_header_degradedProductQualityFlag` | `atl_aer_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `atl_aer_2a_header_executableMajorVersion` | `atl_aer_2a_header_executable_major_version` | tidied |  |  |
| `atl_aer_2a_header_executableMinorVersion` | `atl_aer_2a_header_executable_minor_version` | tidied |  |  |
| `atl_aer_2a_header_fileCategory` | `atl_aer_2a_header_file_category` | tidied |  |  |
| `atl_aer_2a_header_fileClass` | `atl_aer_2a_header_fileClass` | clash |  |  |
| `atl_aer_2a_header_formatMajorVersion` | `atl_aer_2a_header_format_major_version` | tidied |  |  |
| `atl_aer_2a_header_formatMinorVersion` | `atl_aer_2a_header_format_minor_version` | tidied |  |  |
| `atl_aer_2a_header_frameID` | `atl_aer_2a_header_frame_id` | tidied |  |  |
| `atl_aer_2a_header_frameStartCoordinates_geographicLatitude` | `atl_aer_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `atl_aer_2a_header_frameStartCoordinates_geographicLongitude` | `atl_aer_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `atl_aer_2a_header_frameStartMargin` | `atl_aer_2a_header_frame_start_margin` | tidied |  |  |
| `atl_aer_2a_header_frameStartTime` | `atl_aer_2a_header_frame_start_time` | tidied |  |  |
| `atl_aer_2a_header_frameStopCoordinates_geographicLatitude` | `atl_aer_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `atl_aer_2a_header_frameStopCoordinates_geographicLongitude` | `atl_aer_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `atl_aer_2a_header_frameStopMargin` | `atl_aer_2a_header_frame_stop_margin` | tidied |  |  |
| `atl_aer_2a_header_frameStopTime` | `atl_aer_2a_header_frame_stop_time` | tidied |  |  |
| `atl_aer_2a_header_meanAnomaly` | `atl_aer_2a_header_mean_anomaly` | tidied |  |  |
| `atl_aer_2a_header_missionID` | `atl_aer_2a_header_mission_id` | tidied |  |  |
| `atl_aer_2a_header_orbitEccentricity` | `atl_aer_2a_header_orbit_eccentricity` | tidied |  |  |
| `atl_aer_2a_header_orbitInclination` | `atl_aer_2a_header_orbit_inclination` | tidied |  |  |
| `atl_aer_2a_header_orbitNumber` | `atl_aer_2a_header_orbit_number` | tidied |  |  |
| `atl_aer_2a_header_orbitSemiMajorAxis` | `atl_aer_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `atl_aer_2a_header_originalProductName` | `atl_aer_2a_header_original_product_name` | tidied |  |  |
| `atl_aer_2a_header_perigeeArgument` | `atl_aer_2a_header_perigee_argument` | tidied |  |  |
| `atl_aer_2a_header_processingCentre` | `atl_aer_2a_header_processing_centre` | tidied |  |  |
| `atl_aer_2a_header_processingStartTime` | `atl_aer_2a_header_processing_start_time` | tidied |  |  |
| `atl_aer_2a_header_processingStopTime` | `atl_aer_2a_header_processing_stop_time` | tidied |  |  |
| `atl_aer_2a_header_processorMajorVersion` | `atl_aer_2a_header_processor_major_version` | tidied |  |  |
| `atl_aer_2a_header_processorMinorVersion` | `atl_aer_2a_header_processor_minor_version` | tidied |  |  |
| `atl_aer_2a_header_processorName` | `atl_aer_2a_header_processor_name` | tidied |  |  |
| `atl_aer_2a_header_productLevel` | `atl_aer_2a_header_product_level` | tidied |  |  |
| `atl_aer_2a_header_productName` | `atl_aer_2a_header_product_name` | tidied |  |  |
| `atl_aer_2a_header_productType` | `atl_aer_2a_header_product_type` | tidied |  |  |
| `atl_aer_2a_header_rightAscension` | `atl_aer_2a_header_right_ascension` | tidied |  |  |
| `atl_aer_2a_header_sensingStartTime` | `atl_aer_2a_header_sensing_start_time` | tidied |  |  |
| `atl_aer_2a_header_sensingStopTime` | `atl_aer_2a_header_sensing_stop_time` | tidied |  |  |
| `atl_aer_2a_header_stateVectorSource` | `atl_aer_2a_header_state_vector_source` | tidied |  |  |
| `atl_aer_2a_header_stateVectorTime` | `atl_aer_2a_header_state_vector_time` | tidied |  |  |
| `atl_aer_2a_header_subsettedProduct` | `atl_aer_2a_header_subsetted_product` | tidied |  |  |
| `atl_aer_2a_header_xPosition` | `atl_aer_2a_header_x_position` | tidied |  |  |
| `atl_aer_2a_header_xVelocity` | `atl_aer_2a_header_x_velocity` | tidied |  |  |
| `atl_aer_2a_header_yPosition` | `atl_aer_2a_header_y_position` | tidied |  |  |
| `atl_aer_2a_header_yVelocity` | `atl_aer_2a_header_y_velocity` | tidied |  |  |
| `atl_aer_2a_header_zPosition` | `atl_aer_2a_header_z_position` | tidied |  |  |
| `atl_aer_2a_header_zVelocity` | `atl_aer_2a_header_z_velocity` | tidied |  |  |
| `atl_aer_2a_internal_lidar_feature_mask` | `atl_aer_2a_internal_featuremask` | long_name | Internal Featuremask | 1 |
| `atl_aer_2a_lidar_ratio_355nm` | `atl_aer_2a_extinction_to_backscatter_ratio_at_355nm` | long_name | extinction to backscatter ratio at 355nm | sr |
| `atl_aer_2a_mie_detection_status` | `atl_aer_2a_mie_channel_detection_status` | long_name | Mie channel detection status | 1 |
| `atl_aer_2a_number_of_layers` | `atl_aer_2a_number_of_layers_used_in_the_inversion` | long_name | Number of Layers used in the inversion | 1 |
| `atl_aer_2a_particle_backscatter_coefficient_355nm_error` | `atl_aer_2a_estimated_1_sigma_derived_backscatter_error` | long_name | Estimated 1-sigma derived Backscatter error | m-1 sr-1 |
| `atl_aer_2a_particle_extinction_coefficient_355nm_error` | `atl_aer_2a_estimated_1_sigma_extinction_error` | long_name | Estimated 1-sigma extinction error | 1 |
| `atl_aer_2a_particle_linear_depol_ratio_355nm_error` | `atl_aer_2a_estimated_1_sigma_depolarization_error` | long_name | Estimated 1-sigma depolarization error | 1 |
| `atl_aer_2a_range` | `atl_aer_2a_range_from_lidar` | long_name | Range from lidar | m |
| `atl_aer_2a_rayleigh_detection_status` | `atl_aer_2a_rayleigh_channel_detection_status` | long_name | Rayleigh channel detection status | 1 |
| `atl_aer_2a_size_JSG_height` | `atl_aer_2a_size_jsg_height` | tidied |  |  |
| `atl_aer_2a_tropopause_height` | `atl_aer_2a_tropopause_height_used` | long_name | Tropopause height used | m |
| `atl_ald_2a_aerosol_layer_base` | `atl_ald_2a_base_height_of_jth_aerosol_layer` | long_name | Base height of jth aerosol layer | m |
| `atl_ald_2a_aerosol_layer_confidence` | `atl_ald_2a_level_of_confidence_for_aerosol_layer_j` | long_name | Level of confidence for aerosol layer j |  |
| `atl_ald_2a_aerosol_layer_mean_lidar_ratio_355nm` | `atl_ald_2a_mean_lidar_ratio_of_layer_j_at_355_nm` | long_name | Mean lidar ratio of layer j at 355 nm | sr |
| `atl_ald_2a_aerosol_layer_number` | `atl_ald_2a_number_of_detected_aerosol_layers` | long_name | Number of detected aerosol layers |  |
| `atl_ald_2a_aerosol_layer_top` | `atl_ald_2a_top_height_of_jth_aerosol_layer` | long_name | Top height of jth aerosol layer | m |
| `atl_ald_2a_aerosol_optical_thickness_355nm` | `atl_ald_2a_column_aerosol_optical_thickness_at_355_nm` | long_name | Column aerosol optical thickness at 355 nm | 1 |
| `atl_ald_2a_geoid_offset` | `atl_ald_2a_height_of_the_geoid_above_wgs84_ellipsoid` | long_name | Height of the geoid above WGS84 ellipsoid | m |
| `atl_ald_2a_header_ANXLongitude` | `atl_ald_2a_header_anx_longitude` | tidied |  |  |
| `atl_ald_2a_header_ANXTime` | `atl_ald_2a_header_anx_time` | tidied |  |  |
| `atl_ald_2a_header_File_Class` | `atl_ald_2a_header_File_Class` | clash |  |  |
| `atl_ald_2a_header_File_Description` | `atl_ald_2a_header_file_description` | tidied |  |  |
| `atl_ald_2a_header_File_Name` | `atl_ald_2a_header_file_name` | tidied |  |  |
| `atl_ald_2a_header_File_Type` | `atl_ald_2a_header_file_type` | tidied |  |  |
| `atl_ald_2a_header_File_Version` | `atl_ald_2a_header_file_version` | tidied |  |  |
| `atl_ald_2a_header_Mission` | `atl_ald_2a_header_mission` | tidied |  |  |
| `atl_ald_2a_header_Notes` | `atl_ald_2a_header_notes` | tidied |  |  |
| `atl_ald_2a_header_Source_Creation_Date` | `atl_ald_2a_header_source_creation_date` | tidied |  |  |
| `atl_ald_2a_header_Source_Creator` | `atl_ald_2a_header_source_creator` | tidied |  |  |
| `atl_ald_2a_header_Source_Creator_Version` | `atl_ald_2a_header_source_creator_version` | tidied |  |  |
| `atl_ald_2a_header_Source_System` | `atl_ald_2a_header_source_system` | tidied |  |  |
| `atl_ald_2a_header_Validity_Period_Validity_Start` | `atl_ald_2a_header_validity_period_validity_start` | tidied |  |  |
| `atl_ald_2a_header_Validity_Period_Validity_Stop` | `atl_ald_2a_header_validity_period_validity_stop` | tidied |  |  |
| `atl_ald_2a_header_acquisitionStation` | `atl_ald_2a_header_acquisition_station` | tidied |  |  |
| `atl_ald_2a_header_degradedProductQualityFlag` | `atl_ald_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `atl_ald_2a_header_executableMajorVersion` | `atl_ald_2a_header_executable_major_version` | tidied |  |  |
| `atl_ald_2a_header_executableMinorVersion` | `atl_ald_2a_header_executable_minor_version` | tidied |  |  |
| `atl_ald_2a_header_fileCategory` | `atl_ald_2a_header_file_category` | tidied |  |  |
| `atl_ald_2a_header_fileClass` | `atl_ald_2a_header_fileClass` | clash |  |  |
| `atl_ald_2a_header_formatMajorVersion` | `atl_ald_2a_header_format_major_version` | tidied |  |  |
| `atl_ald_2a_header_formatMinorVersion` | `atl_ald_2a_header_format_minor_version` | tidied |  |  |
| `atl_ald_2a_header_frameID` | `atl_ald_2a_header_frame_id` | tidied |  |  |
| `atl_ald_2a_header_frameStartCoordinates_geographicLatitude` | `atl_ald_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `atl_ald_2a_header_frameStartCoordinates_geographicLongitude` | `atl_ald_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `atl_ald_2a_header_frameStartMargin` | `atl_ald_2a_header_frame_start_margin` | tidied |  |  |
| `atl_ald_2a_header_frameStartTime` | `atl_ald_2a_header_frame_start_time` | tidied |  |  |
| `atl_ald_2a_header_frameStopCoordinates_geographicLatitude` | `atl_ald_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `atl_ald_2a_header_frameStopCoordinates_geographicLongitude` | `atl_ald_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `atl_ald_2a_header_frameStopMargin` | `atl_ald_2a_header_frame_stop_margin` | tidied |  |  |
| `atl_ald_2a_header_frameStopTime` | `atl_ald_2a_header_frame_stop_time` | tidied |  |  |
| `atl_ald_2a_header_meanAnomaly` | `atl_ald_2a_header_mean_anomaly` | tidied |  |  |
| `atl_ald_2a_header_missionID` | `atl_ald_2a_header_mission_id` | tidied |  |  |
| `atl_ald_2a_header_orbitEccentricity` | `atl_ald_2a_header_orbit_eccentricity` | tidied |  |  |
| `atl_ald_2a_header_orbitInclination` | `atl_ald_2a_header_orbit_inclination` | tidied |  |  |
| `atl_ald_2a_header_orbitNumber` | `atl_ald_2a_header_orbit_number` | tidied |  |  |
| `atl_ald_2a_header_orbitSemiMajorAxis` | `atl_ald_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `atl_ald_2a_header_originalProductName` | `atl_ald_2a_header_original_product_name` | tidied |  |  |
| `atl_ald_2a_header_perigeeArgument` | `atl_ald_2a_header_perigee_argument` | tidied |  |  |
| `atl_ald_2a_header_processingCentre` | `atl_ald_2a_header_processing_centre` | tidied |  |  |
| `atl_ald_2a_header_processingStartTime` | `atl_ald_2a_header_processing_start_time` | tidied |  |  |
| `atl_ald_2a_header_processingStopTime` | `atl_ald_2a_header_processing_stop_time` | tidied |  |  |
| `atl_ald_2a_header_processorMajorVersion` | `atl_ald_2a_header_processor_major_version` | tidied |  |  |
| `atl_ald_2a_header_processorMinorVersion` | `atl_ald_2a_header_processor_minor_version` | tidied |  |  |
| `atl_ald_2a_header_processorName` | `atl_ald_2a_header_processor_name` | tidied |  |  |
| `atl_ald_2a_header_productLevel` | `atl_ald_2a_header_product_level` | tidied |  |  |
| `atl_ald_2a_header_productName` | `atl_ald_2a_header_product_name` | tidied |  |  |
| `atl_ald_2a_header_productType` | `atl_ald_2a_header_product_type` | tidied |  |  |
| `atl_ald_2a_header_rightAscension` | `atl_ald_2a_header_right_ascension` | tidied |  |  |
| `atl_ald_2a_header_sensingStartTime` | `atl_ald_2a_header_sensing_start_time` | tidied |  |  |
| `atl_ald_2a_header_sensingStopTime` | `atl_ald_2a_header_sensing_stop_time` | tidied |  |  |
| `atl_ald_2a_header_stateVectorSource` | `atl_ald_2a_header_state_vector_source` | tidied |  |  |
| `atl_ald_2a_header_stateVectorTime` | `atl_ald_2a_header_state_vector_time` | tidied |  |  |
| `atl_ald_2a_header_subsettedProduct` | `atl_ald_2a_header_subsetted_product` | tidied |  |  |
| `atl_ald_2a_header_xPosition` | `atl_ald_2a_header_x_position` | tidied |  |  |
| `atl_ald_2a_header_xVelocity` | `atl_ald_2a_header_x_velocity` | tidied |  |  |
| `atl_ald_2a_header_yPosition` | `atl_ald_2a_header_y_position` | tidied |  |  |
| `atl_ald_2a_header_yVelocity` | `atl_ald_2a_header_y_velocity` | tidied |  |  |
| `atl_ald_2a_header_zPosition` | `atl_ald_2a_header_z_position` | tidied |  |  |
| `atl_ald_2a_header_zVelocity` | `atl_ald_2a_header_z_velocity` | tidied |  |  |
| `atl_ald_2a_quality_status` | `atl_ald_2a_quality_status_of_aerosol_layer_descriptor` | long_name | Quality status of aerosol layer descriptor |  |
| `atl_cth_2a_ATLID_cloud_top_height` | `atl_cth_2a_atlid_cloud_top_height` | tidied | Cloud top height retrieved from ATLID Mie co-polar signal, 1 | m |
| `atl_cth_2a_ATLID_cloud_top_height_confidence` | `atl_cth_2a_atlid_cloud_top_height_confidence` | tidied | Level of confidence for ATLID cloud top height |  |
| `atl_cth_2a_ATLID_cloud_top_height_consistency` | `atl_cth_2a_atlid_cloud_top_height_consistency` | tidied | Level of consistency of ATLID cloud top height with A-TC pro |  |
| `atl_cth_2a_ATLID_thick_cloud_top_height` | `atl_cth_2a_atlid_thick_cloud_top_height` | tidied | Cloud top height of thick clouds retrieved from ATLID Mie co | m |
| `atl_cth_2a_geoid_offset` | `atl_cth_2a_height_of_the_geoid_above_wgs84_ellipsoid` | long_name | Height of the geoid above WGS84 ellipsoid | m |
| `atl_cth_2a_header_ANXLongitude` | `atl_cth_2a_header_anx_longitude` | tidied |  |  |
| `atl_cth_2a_header_ANXTime` | `atl_cth_2a_header_anx_time` | tidied |  |  |
| `atl_cth_2a_header_File_Class` | `atl_cth_2a_header_File_Class` | clash |  |  |
| `atl_cth_2a_header_File_Description` | `atl_cth_2a_header_file_description` | tidied |  |  |
| `atl_cth_2a_header_File_Name` | `atl_cth_2a_header_file_name` | tidied |  |  |
| `atl_cth_2a_header_File_Type` | `atl_cth_2a_header_file_type` | tidied |  |  |
| `atl_cth_2a_header_File_Version` | `atl_cth_2a_header_file_version` | tidied |  |  |
| `atl_cth_2a_header_Mission` | `atl_cth_2a_header_mission` | tidied |  |  |
| `atl_cth_2a_header_Notes` | `atl_cth_2a_header_notes` | tidied |  |  |
| `atl_cth_2a_header_Source_Creation_Date` | `atl_cth_2a_header_source_creation_date` | tidied |  |  |
| `atl_cth_2a_header_Source_Creator` | `atl_cth_2a_header_source_creator` | tidied |  |  |
| `atl_cth_2a_header_Source_Creator_Version` | `atl_cth_2a_header_source_creator_version` | tidied |  |  |
| `atl_cth_2a_header_Source_System` | `atl_cth_2a_header_source_system` | tidied |  |  |
| `atl_cth_2a_header_Validity_Period_Validity_Start` | `atl_cth_2a_header_validity_period_validity_start` | tidied |  |  |
| `atl_cth_2a_header_Validity_Period_Validity_Stop` | `atl_cth_2a_header_validity_period_validity_stop` | tidied |  |  |
| `atl_cth_2a_header_acquisitionStation` | `atl_cth_2a_header_acquisition_station` | tidied |  |  |
| `atl_cth_2a_header_degradedProductQualityFlag` | `atl_cth_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `atl_cth_2a_header_executableMajorVersion` | `atl_cth_2a_header_executable_major_version` | tidied |  |  |
| `atl_cth_2a_header_executableMinorVersion` | `atl_cth_2a_header_executable_minor_version` | tidied |  |  |
| `atl_cth_2a_header_fileCategory` | `atl_cth_2a_header_file_category` | tidied |  |  |
| `atl_cth_2a_header_fileClass` | `atl_cth_2a_header_fileClass` | clash |  |  |
| `atl_cth_2a_header_formatMajorVersion` | `atl_cth_2a_header_format_major_version` | tidied |  |  |
| `atl_cth_2a_header_formatMinorVersion` | `atl_cth_2a_header_format_minor_version` | tidied |  |  |
| `atl_cth_2a_header_frameID` | `atl_cth_2a_header_frame_id` | tidied |  |  |
| `atl_cth_2a_header_frameStartCoordinates_geographicLatitude` | `atl_cth_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `atl_cth_2a_header_frameStartCoordinates_geographicLongitude` | `atl_cth_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `atl_cth_2a_header_frameStartMargin` | `atl_cth_2a_header_frame_start_margin` | tidied |  |  |
| `atl_cth_2a_header_frameStartTime` | `atl_cth_2a_header_frame_start_time` | tidied |  |  |
| `atl_cth_2a_header_frameStopCoordinates_geographicLatitude` | `atl_cth_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `atl_cth_2a_header_frameStopCoordinates_geographicLongitude` | `atl_cth_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `atl_cth_2a_header_frameStopMargin` | `atl_cth_2a_header_frame_stop_margin` | tidied |  |  |
| `atl_cth_2a_header_frameStopTime` | `atl_cth_2a_header_frame_stop_time` | tidied |  |  |
| `atl_cth_2a_header_meanAnomaly` | `atl_cth_2a_header_mean_anomaly` | tidied |  |  |
| `atl_cth_2a_header_missionID` | `atl_cth_2a_header_mission_id` | tidied |  |  |
| `atl_cth_2a_header_orbitEccentricity` | `atl_cth_2a_header_orbit_eccentricity` | tidied |  |  |
| `atl_cth_2a_header_orbitInclination` | `atl_cth_2a_header_orbit_inclination` | tidied |  |  |
| `atl_cth_2a_header_orbitNumber` | `atl_cth_2a_header_orbit_number` | tidied |  |  |
| `atl_cth_2a_header_orbitSemiMajorAxis` | `atl_cth_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `atl_cth_2a_header_originalProductName` | `atl_cth_2a_header_original_product_name` | tidied |  |  |
| `atl_cth_2a_header_perigeeArgument` | `atl_cth_2a_header_perigee_argument` | tidied |  |  |
| `atl_cth_2a_header_processingCentre` | `atl_cth_2a_header_processing_centre` | tidied |  |  |
| `atl_cth_2a_header_processingStartTime` | `atl_cth_2a_header_processing_start_time` | tidied |  |  |
| `atl_cth_2a_header_processingStopTime` | `atl_cth_2a_header_processing_stop_time` | tidied |  |  |
| `atl_cth_2a_header_processorMajorVersion` | `atl_cth_2a_header_processor_major_version` | tidied |  |  |
| `atl_cth_2a_header_processorMinorVersion` | `atl_cth_2a_header_processor_minor_version` | tidied |  |  |
| `atl_cth_2a_header_processorName` | `atl_cth_2a_header_processor_name` | tidied |  |  |
| `atl_cth_2a_header_productLevel` | `atl_cth_2a_header_product_level` | tidied |  |  |
| `atl_cth_2a_header_productName` | `atl_cth_2a_header_product_name` | tidied |  |  |
| `atl_cth_2a_header_productType` | `atl_cth_2a_header_product_type` | tidied |  |  |
| `atl_cth_2a_header_rightAscension` | `atl_cth_2a_header_right_ascension` | tidied |  |  |
| `atl_cth_2a_header_sensingStartTime` | `atl_cth_2a_header_sensing_start_time` | tidied |  |  |
| `atl_cth_2a_header_sensingStopTime` | `atl_cth_2a_header_sensing_stop_time` | tidied |  |  |
| `atl_cth_2a_header_stateVectorSource` | `atl_cth_2a_header_state_vector_source` | tidied |  |  |
| `atl_cth_2a_header_stateVectorTime` | `atl_cth_2a_header_state_vector_time` | tidied |  |  |
| `atl_cth_2a_header_subsettedProduct` | `atl_cth_2a_header_subsetted_product` | tidied |  |  |
| `atl_cth_2a_header_xPosition` | `atl_cth_2a_header_x_position` | tidied |  |  |
| `atl_cth_2a_header_xVelocity` | `atl_cth_2a_header_x_velocity` | tidied |  |  |
| `atl_cth_2a_header_yPosition` | `atl_cth_2a_header_y_position` | tidied |  |  |
| `atl_cth_2a_header_yVelocity` | `atl_cth_2a_header_y_velocity` | tidied |  |  |
| `atl_cth_2a_header_zPosition` | `atl_cth_2a_header_z_position` | tidied |  |  |
| `atl_cth_2a_header_zVelocity` | `atl_cth_2a_header_z_velocity` | tidied |  |  |
| `atl_cth_2a_quality_status` | `atl_cth_2a_quality_status_of_cloud_top_height` | long_name | Quality status of cloud top height |  |
| `atl_ebd_2a_extended_data_quality_status` | `atl_ebd_2a_extended_quality_status` | long_name | Extended Quality Status | - |
| `atl_ebd_2a_final_chi_square` | `atl_ebd_2a_final_value_of_the_cost_function` | long_name | Final value of the cost function | 1 |
| `atl_ebd_2a_header_ANXLongitude` | `atl_ebd_2a_header_anx_longitude` | tidied |  |  |
| `atl_ebd_2a_header_ANXTime` | `atl_ebd_2a_header_anx_time` | tidied |  |  |
| `atl_ebd_2a_header_File_Class` | `atl_ebd_2a_header_File_Class` | clash |  |  |
| `atl_ebd_2a_header_File_Description` | `atl_ebd_2a_header_file_description` | tidied |  |  |
| `atl_ebd_2a_header_File_Name` | `atl_ebd_2a_header_file_name` | tidied |  |  |
| `atl_ebd_2a_header_File_Type` | `atl_ebd_2a_header_file_type` | tidied |  |  |
| `atl_ebd_2a_header_File_Version` | `atl_ebd_2a_header_file_version` | tidied |  |  |
| `atl_ebd_2a_header_Mission` | `atl_ebd_2a_header_mission` | tidied |  |  |
| `atl_ebd_2a_header_Notes` | `atl_ebd_2a_header_notes` | tidied |  |  |
| `atl_ebd_2a_header_Source_Creation_Date` | `atl_ebd_2a_header_source_creation_date` | tidied |  |  |
| `atl_ebd_2a_header_Source_Creator` | `atl_ebd_2a_header_source_creator` | tidied |  |  |
| `atl_ebd_2a_header_Source_Creator_Version` | `atl_ebd_2a_header_source_creator_version` | tidied |  |  |
| `atl_ebd_2a_header_Source_System` | `atl_ebd_2a_header_source_system` | tidied |  |  |
| `atl_ebd_2a_header_Validity_Period_Validity_Start` | `atl_ebd_2a_header_validity_period_validity_start` | tidied |  |  |
| `atl_ebd_2a_header_Validity_Period_Validity_Stop` | `atl_ebd_2a_header_validity_period_validity_stop` | tidied |  |  |
| `atl_ebd_2a_header_acquisitionStation` | `atl_ebd_2a_header_acquisition_station` | tidied |  |  |
| `atl_ebd_2a_header_degradedProductQualityFlag` | `atl_ebd_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `atl_ebd_2a_header_executableMajorVersion` | `atl_ebd_2a_header_executable_major_version` | tidied |  |  |
| `atl_ebd_2a_header_executableMinorVersion` | `atl_ebd_2a_header_executable_minor_version` | tidied |  |  |
| `atl_ebd_2a_header_fileCategory` | `atl_ebd_2a_header_file_category` | tidied |  |  |
| `atl_ebd_2a_header_fileClass` | `atl_ebd_2a_header_fileClass` | clash |  |  |
| `atl_ebd_2a_header_formatMajorVersion` | `atl_ebd_2a_header_format_major_version` | tidied |  |  |
| `atl_ebd_2a_header_formatMinorVersion` | `atl_ebd_2a_header_format_minor_version` | tidied |  |  |
| `atl_ebd_2a_header_frameID` | `atl_ebd_2a_header_frame_id` | tidied |  |  |
| `atl_ebd_2a_header_frameStartCoordinates_geographicLatitude` | `atl_ebd_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `atl_ebd_2a_header_frameStartCoordinates_geographicLongitude` | `atl_ebd_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `atl_ebd_2a_header_frameStartMargin` | `atl_ebd_2a_header_frame_start_margin` | tidied |  |  |
| `atl_ebd_2a_header_frameStartTime` | `atl_ebd_2a_header_frame_start_time` | tidied |  |  |
| `atl_ebd_2a_header_frameStopCoordinates_geographicLatitude` | `atl_ebd_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `atl_ebd_2a_header_frameStopCoordinates_geographicLongitude` | `atl_ebd_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `atl_ebd_2a_header_frameStopMargin` | `atl_ebd_2a_header_frame_stop_margin` | tidied |  |  |
| `atl_ebd_2a_header_frameStopTime` | `atl_ebd_2a_header_frame_stop_time` | tidied |  |  |
| `atl_ebd_2a_header_meanAnomaly` | `atl_ebd_2a_header_mean_anomaly` | tidied |  |  |
| `atl_ebd_2a_header_missionID` | `atl_ebd_2a_header_mission_id` | tidied |  |  |
| `atl_ebd_2a_header_orbitEccentricity` | `atl_ebd_2a_header_orbit_eccentricity` | tidied |  |  |
| `atl_ebd_2a_header_orbitInclination` | `atl_ebd_2a_header_orbit_inclination` | tidied |  |  |
| `atl_ebd_2a_header_orbitNumber` | `atl_ebd_2a_header_orbit_number` | tidied |  |  |
| `atl_ebd_2a_header_orbitSemiMajorAxis` | `atl_ebd_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `atl_ebd_2a_header_originalProductName` | `atl_ebd_2a_header_original_product_name` | tidied |  |  |
| `atl_ebd_2a_header_perigeeArgument` | `atl_ebd_2a_header_perigee_argument` | tidied |  |  |
| `atl_ebd_2a_header_processingCentre` | `atl_ebd_2a_header_processing_centre` | tidied |  |  |
| `atl_ebd_2a_header_processingStartTime` | `atl_ebd_2a_header_processing_start_time` | tidied |  |  |
| `atl_ebd_2a_header_processingStopTime` | `atl_ebd_2a_header_processing_stop_time` | tidied |  |  |
| `atl_ebd_2a_header_processorMajorVersion` | `atl_ebd_2a_header_processor_major_version` | tidied |  |  |
| `atl_ebd_2a_header_processorMinorVersion` | `atl_ebd_2a_header_processor_minor_version` | tidied |  |  |
| `atl_ebd_2a_header_processorName` | `atl_ebd_2a_header_processor_name` | tidied |  |  |
| `atl_ebd_2a_header_productLevel` | `atl_ebd_2a_header_product_level` | tidied |  |  |
| `atl_ebd_2a_header_productName` | `atl_ebd_2a_header_product_name` | tidied |  |  |
| `atl_ebd_2a_header_productType` | `atl_ebd_2a_header_product_type` | tidied |  |  |
| `atl_ebd_2a_header_rightAscension` | `atl_ebd_2a_header_right_ascension` | tidied |  |  |
| `atl_ebd_2a_header_sensingStartTime` | `atl_ebd_2a_header_sensing_start_time` | tidied |  |  |
| `atl_ebd_2a_header_sensingStopTime` | `atl_ebd_2a_header_sensing_stop_time` | tidied |  |  |
| `atl_ebd_2a_header_stateVectorSource` | `atl_ebd_2a_header_state_vector_source` | tidied |  |  |
| `atl_ebd_2a_header_stateVectorTime` | `atl_ebd_2a_header_state_vector_time` | tidied |  |  |
| `atl_ebd_2a_header_subsettedProduct` | `atl_ebd_2a_header_subsetted_product` | tidied |  |  |
| `atl_ebd_2a_header_xPosition` | `atl_ebd_2a_header_x_position` | tidied |  |  |
| `atl_ebd_2a_header_xVelocity` | `atl_ebd_2a_header_x_velocity` | tidied |  |  |
| `atl_ebd_2a_header_yPosition` | `atl_ebd_2a_header_y_position` | tidied |  |  |
| `atl_ebd_2a_header_yVelocity` | `atl_ebd_2a_header_y_velocity` | tidied |  |  |
| `atl_ebd_2a_header_zPosition` | `atl_ebd_2a_header_z_position` | tidied |  |  |
| `atl_ebd_2a_header_zVelocity` | `atl_ebd_2a_header_z_velocity` | tidied |  |  |
| `atl_ebd_2a_initial_chi_square` | `atl_ebd_2a_initial_value_of_the_cost_function` | long_name | Initial value of the cost function | 1 |
| `atl_ebd_2a_lidar_ratio_355nm` | `atl_ebd_2a_lidar_ratio_355nm` | clash | extinction to backscatter ratio at 355nm | sr |
| `atl_ebd_2a_lidar_ratio_355nm_low_resolution` | `atl_ebd_2a_lidar_ratio_355nm_low_resolution` | clash | Extinction to backscatter ratio at 355nm (low horizontal res | sr |
| `atl_ebd_2a_lidar_ratio_355nm_medium_resolution` | `atl_ebd_2a_lidar_ratio_355nm_medium_resolution` | clash | Extinction to Backscatter ratio at 355nm (medium horizontal  | sr |
| `atl_ebd_2a_mie_attenuated_backscatter_355nm_fit` | `atl_ebd_2a_fitted_mie_attenuated_backscatter` | long_name | Fitted Mie Attenuated backscatter | m-1 sr-1 |
| `atl_ebd_2a_mie_detection_status` | `atl_ebd_2a_mie_channel_detection_status` | long_name | Mie Channel Detection Status | - |
| `atl_ebd_2a_mie_total_attenuated_backscatter_355nm` | `atl_ebd_2a_total_attenuated_mie_backscatter` | long_name | Total attenuated Mie backscatter | m-1 sr-1 |
| `atl_ebd_2a_number_of_layers` | `atl_ebd_2a_number_of_layers_used_in_the_inversion` | long_name | Number of Layers used in the inversion | - |
| `atl_ebd_2a_particle_backscatter_coefficient_355nm_error` | `atl_ebd_2a_estimated_1_sigma_derived_backscatter_error` | long_name | Estimated 1-sigma derived Backscatter error | m-1 sr-1 |
| `atl_ebd_2a_particle_backscatter_coefficient_355nm_low_resolution` | `atl_ebd_2a_backscatter_at_355nm` | long_name | Backscatter (low horizontal resolution version) at 355nm | m-1 sr-1 |
| `atl_ebd_2a_particle_backscatter_coefficient_355nm_low_resolution_error` | `atl_ebd_2a_estimated_1_sigma_error_in_backscatter` | long_name | Estimated 1-sigma error in Backscatter (low horizontal resol | m-1 sr-1 |
| `atl_ebd_2a_particle_backscatter_coefficient_355nm_medium_resolution` | `atl_ebd_2a_backscatter_at_355nm_from_the_atlid_data` | long_name | Backscatter (medium horizontal resolution version) at 355nm  | m-1 sr-1 |
| `atl_ebd_2a_particle_backscatter_coefficient_355nm_medium_resolution_error` | `atl_ebd_2a_estimated_1_sigma_error_in_derivied_backscatter` | long_name | Estimated 1-sigma error in derivied backscatter (medium hori | m-1 sr-1 |
| `atl_ebd_2a_particle_effective_area_radius_error` | `atl_ebd_2a_particle_effective_area_radius_1_sigma_error` | long_name | Particle effective area radius 1-sigma error | m |
| `atl_ebd_2a_particle_extinction_coefficient_355nm_error` | `atl_ebd_2a_estimated_1_sigma_derivied_extinction_error` | long_name | Estimated 1-sigma derivied Extinction error | m-1 |
| `atl_ebd_2a_particle_extinction_coefficient_355nm_low_resolution` | `atl_ebd_2a_particle_extinction_coefficient_355nm_low_resolution` | clash | Extinction (low horizontal resolution version) at 355nm | m-1 |
| `atl_ebd_2a_particle_extinction_coefficient_355nm_low_resolution_error` | `atl_ebd_2a_particle_extinction_coefficient_355nm_low_resolution_error` | clash | Estimated 1-sigma error in extinction (low horizontal resolu | m-1 |
| `atl_ebd_2a_particle_extinction_coefficient_355nm_medium_resolution` | `atl_ebd_2a_particle_extinction_coefficient_355nm_medium_resolution` | clash | Extinction (medium horizontal resolution version) at 355nm | m-1 |
| `atl_ebd_2a_particle_extinction_coefficient_355nm_medium_resolution_error` | `atl_ebd_2a_particle_extinction_coefficient_355nm_medium_resolution_error` | clash | Estimated 1-sigma error in Extinction (medium horizontal res | m-1 |
| `atl_ebd_2a_particle_linear_depol_ratio_355nm_error` | `atl_ebd_2a_estimated_1_sigma_depolarization_error` | long_name | Estimated 1-sigma depolarization error | - |
| `atl_ebd_2a_particle_linear_depol_ratio_355nm_low_resolution` | `atl_ebd_2a_linear_depolarization_ratio_at_355nm` | long_name | Linear depolarization ratio (low horizontal resolution versi | 1 |
| `atl_ebd_2a_particle_linear_depol_ratio_355nm_low_resolution_error` | `atl_ebd_2a_particle_linear_depol_ratio_355nm_low_resolution_error` | clash | Estimated 1-sigma error in linear depolarization ratio (low  | 1 |
| `atl_ebd_2a_particle_linear_depol_ratio_355nm_medium_resolution_error` | `atl_ebd_2a_particle_linear_depol_ratio_355nm_medium_resolution_error` | clash | Estimated 1-sigma error in linear depolarization ratio (medi | 1 |
| `atl_ebd_2a_particle_optical_depth_355nm` | `atl_ebd_2a_particle_optical_depth_355nm` | clash | Optical Depth | 1 |
| `atl_ebd_2a_particle_optical_depth_355nm_error` | `atl_ebd_2a_particle_optical_depth_355nm_error` | clash | Estimated 1-sigma error in Optical Depth | 1 |
| `atl_ebd_2a_particle_optical_depth_355nm_low_resolution` | `atl_ebd_2a_particle_optical_depth_355nm_low_resolution` | clash | Optical Depth (low horizontal resolution version) | 1 |
| `atl_ebd_2a_particle_optical_depth_355nm_low_resolution_error` | `atl_ebd_2a_particle_optical_depth_355nm_low_resolution_error` | clash | Estimated 1-sigma error in Optical Depth (low horizontal res | 1 |
| `atl_ebd_2a_particle_optical_depth_355nm_medium_resolution` | `atl_ebd_2a_particle_optical_depth_355nm_medium_resolution` | clash | Optical Depth (medium horizontal resolution version) | 1 |
| `atl_ebd_2a_particle_optical_depth_355nm_medium_resolution_error` | `atl_ebd_2a_particle_optical_depth_355nm_medium_resolution_error` | clash | Estimated 1-sigma error in Optical Depth (medium horizontal  | 1 |
| `atl_ebd_2a_range` | `atl_ebd_2a_range_from_lidar` | long_name | Range from lidar | m |
| `atl_ebd_2a_rayleigh_attenuated_backscatter_355nm` | `atl_ebd_2a_rayleigh_channel_attenuated_backscatter` | long_name | Rayleigh channel attenuated backscatter | m-1 sr-1 |
| `atl_ebd_2a_rayleigh_attenuated_backscatter_355nm_fit` | `atl_ebd_2a_fitted_rayleigh_attenuated_backscatter` | long_name | Fitted Rayleigh Attenuated backscatter | m-1 sr-1 |
| `atl_ebd_2a_rayleigh_backscatter_355nm_model` | `atl_ebd_2a_rayleigh_backscatter_from_atmospheric_density` | long_name | Rayleigh backscatter from atmospheric density | m-1 sr-1 |
| `atl_ebd_2a_rayleigh_detection_status` | `atl_ebd_2a_rayleigh_channel_detection_status` | long_name | Rayleigh channel detection status | - |
| `atl_ebd_2a_size_JSG_height` | `atl_ebd_2a_size_jsg_height` | tidied |  |  |
| `atl_ebd_2a_state_vector_prior` | `atl_ebd_2a_a_priori_state_vector` | long_name | a priori state vector | 1 |
| `atl_ebd_2a_state_vector_prior_error` | `atl_ebd_2a_a_priori_state_vector_uncertainties` | long_name | a priori state vector uncertainties | 1 |
| `atl_ebd_2a_total_attenuated_backscatter_355nm` | `atl_ebd_2a_total_attenuated_backscatter` | long_name | Total attenuated backscatter | m-1 sr-1 |
| `atl_ebd_2a_total_attenuated_backscatter_355nm_error` | `atl_ebd_2a_1_sigma_error_total_attenuated_backscatter` | long_name | 1-sigma error total attenuated backscatter | m-1 sr-1 |
| `atl_fm_2a_dem_surface_elevation` | `atl_fm_2a_input_surface_elevation` | long_name | Input Surface elevation (WGS84) | m |
| `atl_fm_2a_featuremask` | `atl_fm_2a_lidar_feature_mask_at_the_merged_lidar_radar_resolution` | long_name | Lidar Feature Mask at the merged lidar/radar resolution |  |
| `atl_fm_2a_gap_flag` | `atl_fm_2a_data_gap` | long_name | Data gap (1:start of new series) |  |
| `atl_fm_2a_geoid_offset` | `atl_fm_2a_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `atl_fm_2a_header_ANXLongitude` | `atl_fm_2a_header_anx_longitude` | tidied |  |  |
| `atl_fm_2a_header_ANXTime` | `atl_fm_2a_header_anx_time` | tidied |  |  |
| `atl_fm_2a_header_File_Class` | `atl_fm_2a_header_File_Class` | clash |  |  |
| `atl_fm_2a_header_File_Description` | `atl_fm_2a_header_file_description` | tidied |  |  |
| `atl_fm_2a_header_File_Name` | `atl_fm_2a_header_file_name` | tidied |  |  |
| `atl_fm_2a_header_File_Type` | `atl_fm_2a_header_file_type` | tidied |  |  |
| `atl_fm_2a_header_File_Version` | `atl_fm_2a_header_file_version` | tidied |  |  |
| `atl_fm_2a_header_Mission` | `atl_fm_2a_header_mission` | tidied |  |  |
| `atl_fm_2a_header_Notes` | `atl_fm_2a_header_notes` | tidied |  |  |
| `atl_fm_2a_header_Source_Creation_Date` | `atl_fm_2a_header_source_creation_date` | tidied |  |  |
| `atl_fm_2a_header_Source_Creator` | `atl_fm_2a_header_source_creator` | tidied |  |  |
| `atl_fm_2a_header_Source_Creator_Version` | `atl_fm_2a_header_source_creator_version` | tidied |  |  |
| `atl_fm_2a_header_Source_System` | `atl_fm_2a_header_source_system` | tidied |  |  |
| `atl_fm_2a_header_Validity_Period_Validity_Start` | `atl_fm_2a_header_validity_period_validity_start` | tidied |  |  |
| `atl_fm_2a_header_Validity_Period_Validity_Stop` | `atl_fm_2a_header_validity_period_validity_stop` | tidied |  |  |
| `atl_fm_2a_header_acquisitionStation` | `atl_fm_2a_header_acquisition_station` | tidied |  |  |
| `atl_fm_2a_header_degradedProductQualityFlag` | `atl_fm_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `atl_fm_2a_header_executableMajorVersion` | `atl_fm_2a_header_executable_major_version` | tidied |  |  |
| `atl_fm_2a_header_executableMinorVersion` | `atl_fm_2a_header_executable_minor_version` | tidied |  |  |
| `atl_fm_2a_header_fileCategory` | `atl_fm_2a_header_file_category` | tidied |  |  |
| `atl_fm_2a_header_fileClass` | `atl_fm_2a_header_fileClass` | clash |  |  |
| `atl_fm_2a_header_formatMajorVersion` | `atl_fm_2a_header_format_major_version` | tidied |  |  |
| `atl_fm_2a_header_formatMinorVersion` | `atl_fm_2a_header_format_minor_version` | tidied |  |  |
| `atl_fm_2a_header_frameID` | `atl_fm_2a_header_frame_id` | tidied |  |  |
| `atl_fm_2a_header_frameStartCoordinates_geographicLatitude` | `atl_fm_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `atl_fm_2a_header_frameStartCoordinates_geographicLongitude` | `atl_fm_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `atl_fm_2a_header_frameStartMargin` | `atl_fm_2a_header_frame_start_margin` | tidied |  |  |
| `atl_fm_2a_header_frameStartTime` | `atl_fm_2a_header_frame_start_time` | tidied |  |  |
| `atl_fm_2a_header_frameStopCoordinates_geographicLatitude` | `atl_fm_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `atl_fm_2a_header_frameStopCoordinates_geographicLongitude` | `atl_fm_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `atl_fm_2a_header_frameStopMargin` | `atl_fm_2a_header_frame_stop_margin` | tidied |  |  |
| `atl_fm_2a_header_frameStopTime` | `atl_fm_2a_header_frame_stop_time` | tidied |  |  |
| `atl_fm_2a_header_meanAnomaly` | `atl_fm_2a_header_mean_anomaly` | tidied |  |  |
| `atl_fm_2a_header_missionID` | `atl_fm_2a_header_mission_id` | tidied |  |  |
| `atl_fm_2a_header_orbitEccentricity` | `atl_fm_2a_header_orbit_eccentricity` | tidied |  |  |
| `atl_fm_2a_header_orbitInclination` | `atl_fm_2a_header_orbit_inclination` | tidied |  |  |
| `atl_fm_2a_header_orbitNumber` | `atl_fm_2a_header_orbit_number` | tidied |  |  |
| `atl_fm_2a_header_orbitSemiMajorAxis` | `atl_fm_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `atl_fm_2a_header_originalProductName` | `atl_fm_2a_header_original_product_name` | tidied |  |  |
| `atl_fm_2a_header_perigeeArgument` | `atl_fm_2a_header_perigee_argument` | tidied |  |  |
| `atl_fm_2a_header_processingCentre` | `atl_fm_2a_header_processing_centre` | tidied |  |  |
| `atl_fm_2a_header_processingStartTime` | `atl_fm_2a_header_processing_start_time` | tidied |  |  |
| `atl_fm_2a_header_processingStopTime` | `atl_fm_2a_header_processing_stop_time` | tidied |  |  |
| `atl_fm_2a_header_processorMajorVersion` | `atl_fm_2a_header_processor_major_version` | tidied |  |  |
| `atl_fm_2a_header_processorMinorVersion` | `atl_fm_2a_header_processor_minor_version` | tidied |  |  |
| `atl_fm_2a_header_processorName` | `atl_fm_2a_header_processor_name` | tidied |  |  |
| `atl_fm_2a_header_productLevel` | `atl_fm_2a_header_product_level` | tidied |  |  |
| `atl_fm_2a_header_productName` | `atl_fm_2a_header_product_name` | tidied |  |  |
| `atl_fm_2a_header_productType` | `atl_fm_2a_header_product_type` | tidied |  |  |
| `atl_fm_2a_header_rightAscension` | `atl_fm_2a_header_right_ascension` | tidied |  |  |
| `atl_fm_2a_header_sensingStartTime` | `atl_fm_2a_header_sensing_start_time` | tidied |  |  |
| `atl_fm_2a_header_sensingStopTime` | `atl_fm_2a_header_sensing_stop_time` | tidied |  |  |
| `atl_fm_2a_header_stateVectorSource` | `atl_fm_2a_header_state_vector_source` | tidied |  |  |
| `atl_fm_2a_header_stateVectorTime` | `atl_fm_2a_header_state_vector_time` | tidied |  |  |
| `atl_fm_2a_header_subsettedProduct` | `atl_fm_2a_header_subsetted_product` | tidied |  |  |
| `atl_fm_2a_header_xPosition` | `atl_fm_2a_header_x_position` | tidied |  |  |
| `atl_fm_2a_header_xVelocity` | `atl_fm_2a_header_x_velocity` | tidied |  |  |
| `atl_fm_2a_header_yPosition` | `atl_fm_2a_header_y_position` | tidied |  |  |
| `atl_fm_2a_header_yVelocity` | `atl_fm_2a_header_y_velocity` | tidied |  |  |
| `atl_fm_2a_header_zPosition` | `atl_fm_2a_header_z_position` | tidied |  |  |
| `atl_fm_2a_header_zVelocity` | `atl_fm_2a_header_z_velocity` | tidied |  |  |
| `atl_fm_2a_land_flag` | `atl_fm_2a_0_water_n1_land_n2_no_inputs` | long_name | 0:water\n1:land\n2:no inputs |  |
| `atl_fm_2a_lower_upper` | `atl_fm_2a_lower_and_upper_indices_of_the_regions` | long_name | Lower and upper indices of the regions |  |
| `atl_fm_2a_mie_noise` | `atl_fm_2a_average_signal_in_co_polar_mie_channel_between_30_and_40_km` | long_name | Average signal in Co-Polar Mie channel between 30 and 40 km | m-1 sr-1 |
| `atl_fm_2a_sigma0_crosspolar` | `atl_fm_2a_integrated_surface_return_for_the_cross_polar_mie_channel` | long_name | Integrated surface return for the Cross-Polar Mie channel | sr-1 |
| `atl_fm_2a_sigma0_mie` | `atl_fm_2a_integrated_surface_return_for_the_co_polar_mie_channel` | long_name | Integrated surface return for the Co-Polar Mie channel | sr-1 |
| `atl_fm_2a_sigma0_rayleigh` | `atl_fm_2a_integrated_surface_return_for_the_co_polar_rayleigh_channel` | long_name | Integrated surface return for the Co-Polar Rayleigh channel | sr-1 |
| `atl_fm_2a_size_ATLID_height` | `atl_fm_2a_size_atlid_height` | tidied |  |  |
| `atl_fm_2a_surface_elevation` | `atl_fm_2a_surface_elevation_m` | long_name | Surface elevation [m] | m |
| `atl_fm_2a_surface_rayleigh_tau` | `atl_fm_2a_rayleight_optical_thickness_at_surface` | long_name | Rayleight Optical thickness at Surface |  |
| `atl_ice_2a_extended_data_quality_status` | `atl_ice_2a_extended_quality_status` | long_name | Extended Quality Status | 1 |
| `atl_ice_2a_header_ANXLongitude` | `atl_ice_2a_header_anx_longitude` | tidied |  |  |
| `atl_ice_2a_header_ANXTime` | `atl_ice_2a_header_anx_time` | tidied |  |  |
| `atl_ice_2a_header_File_Class` | `atl_ice_2a_header_File_Class` | clash |  |  |
| `atl_ice_2a_header_File_Description` | `atl_ice_2a_header_file_description` | tidied |  |  |
| `atl_ice_2a_header_File_Name` | `atl_ice_2a_header_file_name` | tidied |  |  |
| `atl_ice_2a_header_File_Type` | `atl_ice_2a_header_file_type` | tidied |  |  |
| `atl_ice_2a_header_File_Version` | `atl_ice_2a_header_file_version` | tidied |  |  |
| `atl_ice_2a_header_Mission` | `atl_ice_2a_header_mission` | tidied |  |  |
| `atl_ice_2a_header_Notes` | `atl_ice_2a_header_notes` | tidied |  |  |
| `atl_ice_2a_header_Source_Creation_Date` | `atl_ice_2a_header_source_creation_date` | tidied |  |  |
| `atl_ice_2a_header_Source_Creator` | `atl_ice_2a_header_source_creator` | tidied |  |  |
| `atl_ice_2a_header_Source_Creator_Version` | `atl_ice_2a_header_source_creator_version` | tidied |  |  |
| `atl_ice_2a_header_Source_System` | `atl_ice_2a_header_source_system` | tidied |  |  |
| `atl_ice_2a_header_Validity_Period_Validity_Start` | `atl_ice_2a_header_validity_period_validity_start` | tidied |  |  |
| `atl_ice_2a_header_Validity_Period_Validity_Stop` | `atl_ice_2a_header_validity_period_validity_stop` | tidied |  |  |
| `atl_ice_2a_header_acquisitionStation` | `atl_ice_2a_header_acquisition_station` | tidied |  |  |
| `atl_ice_2a_header_degradedProductQualityFlag` | `atl_ice_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `atl_ice_2a_header_executableMajorVersion` | `atl_ice_2a_header_executable_major_version` | tidied |  |  |
| `atl_ice_2a_header_executableMinorVersion` | `atl_ice_2a_header_executable_minor_version` | tidied |  |  |
| `atl_ice_2a_header_fileCategory` | `atl_ice_2a_header_file_category` | tidied |  |  |
| `atl_ice_2a_header_fileClass` | `atl_ice_2a_header_fileClass` | clash |  |  |
| `atl_ice_2a_header_formatMajorVersion` | `atl_ice_2a_header_format_major_version` | tidied |  |  |
| `atl_ice_2a_header_formatMinorVersion` | `atl_ice_2a_header_format_minor_version` | tidied |  |  |
| `atl_ice_2a_header_frameID` | `atl_ice_2a_header_frame_id` | tidied |  |  |
| `atl_ice_2a_header_frameStartCoordinates_geographicLatitude` | `atl_ice_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `atl_ice_2a_header_frameStartCoordinates_geographicLongitude` | `atl_ice_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `atl_ice_2a_header_frameStartMargin` | `atl_ice_2a_header_frame_start_margin` | tidied |  |  |
| `atl_ice_2a_header_frameStartTime` | `atl_ice_2a_header_frame_start_time` | tidied |  |  |
| `atl_ice_2a_header_frameStopCoordinates_geographicLatitude` | `atl_ice_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `atl_ice_2a_header_frameStopCoordinates_geographicLongitude` | `atl_ice_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `atl_ice_2a_header_frameStopMargin` | `atl_ice_2a_header_frame_stop_margin` | tidied |  |  |
| `atl_ice_2a_header_frameStopTime` | `atl_ice_2a_header_frame_stop_time` | tidied |  |  |
| `atl_ice_2a_header_meanAnomaly` | `atl_ice_2a_header_mean_anomaly` | tidied |  |  |
| `atl_ice_2a_header_missionID` | `atl_ice_2a_header_mission_id` | tidied |  |  |
| `atl_ice_2a_header_orbitEccentricity` | `atl_ice_2a_header_orbit_eccentricity` | tidied |  |  |
| `atl_ice_2a_header_orbitInclination` | `atl_ice_2a_header_orbit_inclination` | tidied |  |  |
| `atl_ice_2a_header_orbitNumber` | `atl_ice_2a_header_orbit_number` | tidied |  |  |
| `atl_ice_2a_header_orbitSemiMajorAxis` | `atl_ice_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `atl_ice_2a_header_originalProductName` | `atl_ice_2a_header_original_product_name` | tidied |  |  |
| `atl_ice_2a_header_perigeeArgument` | `atl_ice_2a_header_perigee_argument` | tidied |  |  |
| `atl_ice_2a_header_processingCentre` | `atl_ice_2a_header_processing_centre` | tidied |  |  |
| `atl_ice_2a_header_processingStartTime` | `atl_ice_2a_header_processing_start_time` | tidied |  |  |
| `atl_ice_2a_header_processingStopTime` | `atl_ice_2a_header_processing_stop_time` | tidied |  |  |
| `atl_ice_2a_header_processorMajorVersion` | `atl_ice_2a_header_processor_major_version` | tidied |  |  |
| `atl_ice_2a_header_processorMinorVersion` | `atl_ice_2a_header_processor_minor_version` | tidied |  |  |
| `atl_ice_2a_header_processorName` | `atl_ice_2a_header_processor_name` | tidied |  |  |
| `atl_ice_2a_header_productLevel` | `atl_ice_2a_header_product_level` | tidied |  |  |
| `atl_ice_2a_header_productName` | `atl_ice_2a_header_product_name` | tidied |  |  |
| `atl_ice_2a_header_productType` | `atl_ice_2a_header_product_type` | tidied |  |  |
| `atl_ice_2a_header_rightAscension` | `atl_ice_2a_header_right_ascension` | tidied |  |  |
| `atl_ice_2a_header_sensingStartTime` | `atl_ice_2a_header_sensing_start_time` | tidied |  |  |
| `atl_ice_2a_header_sensingStopTime` | `atl_ice_2a_header_sensing_stop_time` | tidied |  |  |
| `atl_ice_2a_header_stateVectorSource` | `atl_ice_2a_header_state_vector_source` | tidied |  |  |
| `atl_ice_2a_header_stateVectorTime` | `atl_ice_2a_header_state_vector_time` | tidied |  |  |
| `atl_ice_2a_header_subsettedProduct` | `atl_ice_2a_header_subsetted_product` | tidied |  |  |
| `atl_ice_2a_header_xPosition` | `atl_ice_2a_header_x_position` | tidied |  |  |
| `atl_ice_2a_header_xVelocity` | `atl_ice_2a_header_x_velocity` | tidied |  |  |
| `atl_ice_2a_header_yPosition` | `atl_ice_2a_header_y_position` | tidied |  |  |
| `atl_ice_2a_header_yVelocity` | `atl_ice_2a_header_y_velocity` | tidied |  |  |
| `atl_ice_2a_header_zPosition` | `atl_ice_2a_header_z_position` | tidied |  |  |
| `atl_ice_2a_header_zVelocity` | `atl_ice_2a_header_z_velocity` | tidied |  |  |
| `atl_ice_2a_ice_effective_radius` | `atl_ice_2a_estimated_ice_crystal_effective_radius` | long_name | Estimated ice crystal effective radius | 10-6 m |
| `atl_ice_2a_ice_water_content_error` | `atl_ice_2a_estimated_1_sigma_error_in_ice_water_content` | long_name | Estimated 1-sigma error in Ice Water Content | 10-6 kg m-3 |
| `atl_ice_2a_range` | `atl_ice_2a_range_from_lidar` | long_name | Range from lidar | m |
| `atl_ice_2a_size_JSG_height` | `atl_ice_2a_size_jsg_height` | tidied |  |  |
| `atl_nom_1b_crosspolar_attenuated_backscatter` | `atl_nom_1b_attenuated_mie_crosspolar_backscatter_signal` | long_name | Attenuated Mie crosspolar backscatter signal | m-1 sr-1 |
| `atl_nom_1b_crosspolar_background_signal` | `atl_nom_1b_mie_cross_polar_channel_background_signal` | long_name | Mie cross-polar channel background signal | BU |
| `atl_nom_1b_crosspolar_lidar_constant_error` | `atl_nom_1b_mie_crosspolar_lidar_constant_error` | long_name | Mie crosspolar lidar constant error | BU m3 sr |
| `atl_nom_1b_crosspolar_offset` | `atl_nom_1b_mie_cross_polar_channel_signal_offset` | long_name | Mie cross-polar channel signal offset | BU |
| `atl_nom_1b_crosspolar_offset_variation` | `atl_nom_1b_mie_cross_polar_channel_offset_variation` | long_name | Mie cross-polar channel offset variation | BU |
| `atl_nom_1b_crosspolar_raw_signal` | `atl_nom_1b_mie_cross_polar_channel_raw_signal` | long_name | Mie cross-polar channel raw signal | BU |
| `atl_nom_1b_crosspolar_relative_backscatter` | `atl_nom_1b_mie_cross_polar_channel_relative_backscatter` | long_name | Mie cross-polar channel relative backscatter | 1 |
| `atl_nom_1b_energy_error_flag` | `atl_nom_1b_laser_energy_insufficient_flag` | long_name | Laser energy insufficient flag |  |
| `atl_nom_1b_header_ANXLongitude` | `atl_nom_1b_header_anx_longitude` | tidied |  |  |
| `atl_nom_1b_header_ANXTime` | `atl_nom_1b_header_anx_time` | tidied |  |  |
| `atl_nom_1b_header_File_Class` | `atl_nom_1b_header_File_Class` | clash |  |  |
| `atl_nom_1b_header_File_Description` | `atl_nom_1b_header_file_description` | tidied |  |  |
| `atl_nom_1b_header_File_Name` | `atl_nom_1b_header_file_name` | tidied |  |  |
| `atl_nom_1b_header_File_Type` | `atl_nom_1b_header_file_type` | tidied |  |  |
| `atl_nom_1b_header_File_Version` | `atl_nom_1b_header_file_version` | tidied |  |  |
| `atl_nom_1b_header_Mission` | `atl_nom_1b_header_mission` | tidied |  |  |
| `atl_nom_1b_header_Notes` | `atl_nom_1b_header_notes` | tidied |  |  |
| `atl_nom_1b_header_Source_Creation_Date` | `atl_nom_1b_header_source_creation_date` | tidied |  |  |
| `atl_nom_1b_header_Source_Creator` | `atl_nom_1b_header_source_creator` | tidied |  |  |
| `atl_nom_1b_header_Source_Creator_Version` | `atl_nom_1b_header_source_creator_version` | tidied |  |  |
| `atl_nom_1b_header_Source_System` | `atl_nom_1b_header_source_system` | tidied |  |  |
| `atl_nom_1b_header_Validity_Period_Validity_Start` | `atl_nom_1b_header_validity_period_validity_start` | tidied |  |  |
| `atl_nom_1b_header_Validity_Period_Validity_Stop` | `atl_nom_1b_header_validity_period_validity_stop` | tidied |  |  |
| `atl_nom_1b_header_acquisitionStation` | `atl_nom_1b_header_acquisition_station` | tidied |  |  |
| `atl_nom_1b_header_degradedProductQualityFlag` | `atl_nom_1b_header_degraded_product_quality_flag` | tidied |  |  |
| `atl_nom_1b_header_executableMajorVersion` | `atl_nom_1b_header_executable_major_version` | tidied |  |  |
| `atl_nom_1b_header_executableMinorVersion` | `atl_nom_1b_header_executable_minor_version` | tidied |  |  |
| `atl_nom_1b_header_fileCategory` | `atl_nom_1b_header_file_category` | tidied |  |  |
| `atl_nom_1b_header_fileClass` | `atl_nom_1b_header_fileClass` | clash |  |  |
| `atl_nom_1b_header_formatMajorVersion` | `atl_nom_1b_header_format_major_version` | tidied |  |  |
| `atl_nom_1b_header_formatMinorVersion` | `atl_nom_1b_header_format_minor_version` | tidied |  |  |
| `atl_nom_1b_header_frameID` | `atl_nom_1b_header_frame_id` | tidied |  |  |
| `atl_nom_1b_header_frameStartCoordinates_geographicLatitude` | `atl_nom_1b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `atl_nom_1b_header_frameStartCoordinates_geographicLongitude` | `atl_nom_1b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `atl_nom_1b_header_frameStartMargin` | `atl_nom_1b_header_frame_start_margin` | tidied |  |  |
| `atl_nom_1b_header_frameStartTime` | `atl_nom_1b_header_frame_start_time` | tidied |  |  |
| `atl_nom_1b_header_frameStopCoordinates_geographicLatitude` | `atl_nom_1b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `atl_nom_1b_header_frameStopCoordinates_geographicLongitude` | `atl_nom_1b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `atl_nom_1b_header_frameStopMargin` | `atl_nom_1b_header_frame_stop_margin` | tidied |  |  |
| `atl_nom_1b_header_frameStopTime` | `atl_nom_1b_header_frame_stop_time` | tidied |  |  |
| `atl_nom_1b_header_meanAnomaly` | `atl_nom_1b_header_mean_anomaly` | tidied |  |  |
| `atl_nom_1b_header_missionID` | `atl_nom_1b_header_mission_id` | tidied |  |  |
| `atl_nom_1b_header_orbitEccentricity` | `atl_nom_1b_header_orbit_eccentricity` | tidied |  |  |
| `atl_nom_1b_header_orbitInclination` | `atl_nom_1b_header_orbit_inclination` | tidied |  |  |
| `atl_nom_1b_header_orbitNumber` | `atl_nom_1b_header_orbit_number` | tidied |  |  |
| `atl_nom_1b_header_orbitSemiMajorAxis` | `atl_nom_1b_header_orbit_semi_major_axis` | tidied |  |  |
| `atl_nom_1b_header_originalProductName` | `atl_nom_1b_header_original_product_name` | tidied |  |  |
| `atl_nom_1b_header_perigeeArgument` | `atl_nom_1b_header_perigee_argument` | tidied |  |  |
| `atl_nom_1b_header_processingCentre` | `atl_nom_1b_header_processing_centre` | tidied |  |  |
| `atl_nom_1b_header_processingStartTime` | `atl_nom_1b_header_processing_start_time` | tidied |  |  |
| `atl_nom_1b_header_processingStopTime` | `atl_nom_1b_header_processing_stop_time` | tidied |  |  |
| `atl_nom_1b_header_processorMajorVersion` | `atl_nom_1b_header_processor_major_version` | tidied |  |  |
| `atl_nom_1b_header_processorMinorVersion` | `atl_nom_1b_header_processor_minor_version` | tidied |  |  |
| `atl_nom_1b_header_processorName` | `atl_nom_1b_header_processor_name` | tidied |  |  |
| `atl_nom_1b_header_productLevel` | `atl_nom_1b_header_product_level` | tidied |  |  |
| `atl_nom_1b_header_productName` | `atl_nom_1b_header_product_name` | tidied |  |  |
| `atl_nom_1b_header_productType` | `atl_nom_1b_header_product_type` | tidied |  |  |
| `atl_nom_1b_header_rightAscension` | `atl_nom_1b_header_right_ascension` | tidied |  |  |
| `atl_nom_1b_header_sensingStartTime` | `atl_nom_1b_header_sensing_start_time` | tidied |  |  |
| `atl_nom_1b_header_sensingStopTime` | `atl_nom_1b_header_sensing_stop_time` | tidied |  |  |
| `atl_nom_1b_header_stateVectorSource` | `atl_nom_1b_header_state_vector_source` | tidied |  |  |
| `atl_nom_1b_header_stateVectorTime` | `atl_nom_1b_header_state_vector_time` | tidied |  |  |
| `atl_nom_1b_header_subsettedProduct` | `atl_nom_1b_header_subsetted_product` | tidied |  |  |
| `atl_nom_1b_header_xPosition` | `atl_nom_1b_header_x_position` | tidied |  |  |
| `atl_nom_1b_header_xVelocity` | `atl_nom_1b_header_x_velocity` | tidied |  |  |
| `atl_nom_1b_header_yPosition` | `atl_nom_1b_header_y_position` | tidied |  |  |
| `atl_nom_1b_header_yVelocity` | `atl_nom_1b_header_y_velocity` | tidied |  |  |
| `atl_nom_1b_header_zPosition` | `atl_nom_1b_header_z_position` | tidied |  |  |
| `atl_nom_1b_header_zVelocity` | `atl_nom_1b_header_z_velocity` | tidied |  |  |
| `atl_nom_1b_hot_pixel_flag_crosspolar` | `atl_nom_1b_flag_for_hot_pixels_in_the_crosspolar_channel` | long_name | Flag for hot pixels in the crosspolar channel | 1 |
| `atl_nom_1b_hot_pixel_flag_mie` | `atl_nom_1b_flag_for_hot_pixels_in_the_mie_channel` | long_name | Flag for hot pixels in the mie channel | 1 |
| `atl_nom_1b_hot_pixel_flag_rayleigh` | `atl_nom_1b_flag_for_hot_pixels_in_the_rayleigh_channel` | long_name | Flag for hot pixels in the rayleigh channel | 1 |
| `atl_nom_1b_hot_pixel_level_mie` | `atl_nom_1b_level_for_hot_pixels_in_the_mie_channel` | long_name | Level for hot pixels in the mie channel | 1 |
| `atl_nom_1b_hot_pixel_level_rayleigh` | `atl_nom_1b_level_for_hot_pixels_in_the_rayleigh_channel` | long_name | Level for hot pixels in the rayleigh channel | 1 |
| `atl_nom_1b_mie_attenuated_backscatter` | `atl_nom_1b_attenuated_mie_copolar_backscatter_signal` | long_name | Attenuated Mie copolar backscatter signal | m-1 sr-1 |
| `atl_nom_1b_mie_averaged_spectral_crosstalk` | `atl_nom_1b_mie_channel_averaged_spectral_cross_talk` | long_name | Mie channel averaged spectral cross-talk | 1 |
| `atl_nom_1b_mie_background_signal` | `atl_nom_1b_mie_co_polar_channel_background_signal` | long_name | Mie co-polar channel background signal | BU |
| `atl_nom_1b_mie_lidar_constant_error` | `atl_nom_1b_mie_copolar_lidar_constant_error` | long_name | Mie copolar lidar constant error | BU m3 sr |
| `atl_nom_1b_mie_normalised_signal` | `atl_nom_1b_mie_co_polar_channel_energy_normalised_signal` | long_name | Mie co-polar channel energy normalised signal | BU |
| `atl_nom_1b_mie_offset` | `atl_nom_1b_mie_co_polar_channel_signal_offset` | long_name | Mie co-polar channel signal offset | BU |
| `atl_nom_1b_mie_offset_variation` | `atl_nom_1b_mie_co_polar_channel_offset_variation` | long_name | Mie co-polar channel offset variation | BU |
| `atl_nom_1b_mie_raw_signal` | `atl_nom_1b_mie_co_polar_channel_raw_signal` | long_name | Mie co-polar channel raw signal | BU |
| `atl_nom_1b_mie_relative_backscatter` | `atl_nom_1b_mie_co_polar_channel_relative_backscatter` | long_name | Mie co-polar channel relative backscatter | 1 |
| `atl_nom_1b_mie_spectral_crosstalk_reference_temperature` | `atl_nom_1b_mie_spectral_cross_talk_reference_temperature` | long_name | Mie spectral cross-talk reference temperature | K |
| `atl_nom_1b_rayleigh_attenuated_backscatter` | `atl_nom_1b_attenuated_rayleigh_backscatter_signal` | long_name | Attenuated Rayleigh backscatter signal | m-1 sr-1 |
| `atl_nom_1b_rayleigh_averaged_spectral_crosstalk` | `atl_nom_1b_rayleigh_channel_averaged_spectral_cross_talk` | long_name | Rayleigh channel averaged spectral cross-talk | 1 |
| `atl_nom_1b_rayleigh_background_signal` | `atl_nom_1b_rayleigh_channel_background_signal` | long_name | Rayleigh channel background signal | BU |
| `atl_nom_1b_rayleigh_normalised_signal` | `atl_nom_1b_rayleigh_channel_energy_normalised_signal` | long_name | Rayleigh channel energy normalised signal | BU |
| `atl_nom_1b_rayleigh_offset` | `atl_nom_1b_rayleigh_channel_signal_offset` | long_name | Rayleigh channel signal offset | BU |
| `atl_nom_1b_rayleigh_offset_variation` | `atl_nom_1b_rayleigh_channel_offset_variation` | long_name | Rayleigh channel offset variation | BU |
| `atl_nom_1b_rayleigh_raw_signal` | `atl_nom_1b_rayleigh_channel_raw_signal` | long_name | Rayleigh channel raw signal | BU |
| `atl_nom_1b_rayleigh_raw_spectral_crosstalk` | `atl_nom_1b_rayleigh_channel_raw_spectral_cross_talk` | long_name | Rayleigh channel raw spectral cross-talk | 1 |
| `atl_nom_1b_rayleigh_relative_backscatter` | `atl_nom_1b_rayleigh_channel_relative_backscatter` | long_name | Rayleigh channel relative backscatter | 1 |
| `atl_nom_1b_rayleigh_spectral_crosstalk_STRAP_evaluations` | `atl_nom_1b_rayleigh_spectral_crosstalk_strap_evaluations` | tidied | Spectral cross-talk on Rayleigh channel estimated via STRAP  | 1 |
| `atl_nom_1b_rayleigh_spectral_crosstalk_STRAP_evaluations_error` | `atl_nom_1b_rayleigh_spectral_crosstalk_strap_evaluations_error` | tidied | Error associated with the Rayleigh spectral crosstalk STRAP  | 1 |
| `atl_nom_1b_state_vector_quality_status` | `atl_nom_1b_state_vector_quality` | long_name | State vector quality |  |
| `atl_nom_1b_time_synchronisation_status` | `atl_nom_1b_instrument_time_synchronisation_status` | long_name | Instrument time synchronisation status |  |
| `atl_nom_1b_valid_STRAP_rayleigh_spectral_crosstalk_segment_flag` | `atl_nom_1b_valid_strap_rayleigh_spectral_crosstalk_segment_flag` | tidied | Validity flag for the Rayleigh spectral crosstalk STRAP esti | 1 |
| `atl_tc_2a_aerosol_classification_prob` | `atl_tc_2a_aerosol_classification_prob` | clash | Aerosol classification probabilities | 10-2 |
| `atl_tc_2a_aerosol_classification_prob_low_resolution` | `atl_tc_2a_aerosol_classification_prob_low_resolution` | clash | Aerosol classification probabilities (Low hor. resolution) | 10-2 |
| `atl_tc_2a_aerosol_classification_prob_medium_resolution` | `atl_tc_2a_aerosol_classification_prob_medium_resolution` | clash | Aerosol classification probabilities (Medium hor. resolution | 10-2 |
| `atl_tc_2a_classification` | `atl_tc_2a_combined_cloud_aerosol_target_classification` | long_name | Combined cloud/aerosol target classification | 1 |
| `atl_tc_2a_extended_data_quality_status` | `atl_tc_2a_extended_quality_status` | long_name | Extended Quality Status | 1 |
| `atl_tc_2a_header_ANXLongitude` | `atl_tc_2a_header_anx_longitude` | tidied |  |  |
| `atl_tc_2a_header_ANXTime` | `atl_tc_2a_header_anx_time` | tidied |  |  |
| `atl_tc_2a_header_File_Class` | `atl_tc_2a_header_File_Class` | clash |  |  |
| `atl_tc_2a_header_File_Description` | `atl_tc_2a_header_file_description` | tidied |  |  |
| `atl_tc_2a_header_File_Name` | `atl_tc_2a_header_file_name` | tidied |  |  |
| `atl_tc_2a_header_File_Type` | `atl_tc_2a_header_file_type` | tidied |  |  |
| `atl_tc_2a_header_File_Version` | `atl_tc_2a_header_file_version` | tidied |  |  |
| `atl_tc_2a_header_Mission` | `atl_tc_2a_header_mission` | tidied |  |  |
| `atl_tc_2a_header_Notes` | `atl_tc_2a_header_notes` | tidied |  |  |
| `atl_tc_2a_header_Source_Creation_Date` | `atl_tc_2a_header_source_creation_date` | tidied |  |  |
| `atl_tc_2a_header_Source_Creator` | `atl_tc_2a_header_source_creator` | tidied |  |  |
| `atl_tc_2a_header_Source_Creator_Version` | `atl_tc_2a_header_source_creator_version` | tidied |  |  |
| `atl_tc_2a_header_Source_System` | `atl_tc_2a_header_source_system` | tidied |  |  |
| `atl_tc_2a_header_Validity_Period_Validity_Start` | `atl_tc_2a_header_validity_period_validity_start` | tidied |  |  |
| `atl_tc_2a_header_Validity_Period_Validity_Stop` | `atl_tc_2a_header_validity_period_validity_stop` | tidied |  |  |
| `atl_tc_2a_header_acquisitionStation` | `atl_tc_2a_header_acquisition_station` | tidied |  |  |
| `atl_tc_2a_header_degradedProductQualityFlag` | `atl_tc_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `atl_tc_2a_header_executableMajorVersion` | `atl_tc_2a_header_executable_major_version` | tidied |  |  |
| `atl_tc_2a_header_executableMinorVersion` | `atl_tc_2a_header_executable_minor_version` | tidied |  |  |
| `atl_tc_2a_header_fileCategory` | `atl_tc_2a_header_file_category` | tidied |  |  |
| `atl_tc_2a_header_fileClass` | `atl_tc_2a_header_fileClass` | clash |  |  |
| `atl_tc_2a_header_formatMajorVersion` | `atl_tc_2a_header_format_major_version` | tidied |  |  |
| `atl_tc_2a_header_formatMinorVersion` | `atl_tc_2a_header_format_minor_version` | tidied |  |  |
| `atl_tc_2a_header_frameID` | `atl_tc_2a_header_frame_id` | tidied |  |  |
| `atl_tc_2a_header_frameStartCoordinates_geographicLatitude` | `atl_tc_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `atl_tc_2a_header_frameStartCoordinates_geographicLongitude` | `atl_tc_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `atl_tc_2a_header_frameStartMargin` | `atl_tc_2a_header_frame_start_margin` | tidied |  |  |
| `atl_tc_2a_header_frameStartTime` | `atl_tc_2a_header_frame_start_time` | tidied |  |  |
| `atl_tc_2a_header_frameStopCoordinates_geographicLatitude` | `atl_tc_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `atl_tc_2a_header_frameStopCoordinates_geographicLongitude` | `atl_tc_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `atl_tc_2a_header_frameStopMargin` | `atl_tc_2a_header_frame_stop_margin` | tidied |  |  |
| `atl_tc_2a_header_frameStopTime` | `atl_tc_2a_header_frame_stop_time` | tidied |  |  |
| `atl_tc_2a_header_meanAnomaly` | `atl_tc_2a_header_mean_anomaly` | tidied |  |  |
| `atl_tc_2a_header_missionID` | `atl_tc_2a_header_mission_id` | tidied |  |  |
| `atl_tc_2a_header_orbitEccentricity` | `atl_tc_2a_header_orbit_eccentricity` | tidied |  |  |
| `atl_tc_2a_header_orbitInclination` | `atl_tc_2a_header_orbit_inclination` | tidied |  |  |
| `atl_tc_2a_header_orbitNumber` | `atl_tc_2a_header_orbit_number` | tidied |  |  |
| `atl_tc_2a_header_orbitSemiMajorAxis` | `atl_tc_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `atl_tc_2a_header_originalProductName` | `atl_tc_2a_header_original_product_name` | tidied |  |  |
| `atl_tc_2a_header_perigeeArgument` | `atl_tc_2a_header_perigee_argument` | tidied |  |  |
| `atl_tc_2a_header_processingCentre` | `atl_tc_2a_header_processing_centre` | tidied |  |  |
| `atl_tc_2a_header_processingStartTime` | `atl_tc_2a_header_processing_start_time` | tidied |  |  |
| `atl_tc_2a_header_processingStopTime` | `atl_tc_2a_header_processing_stop_time` | tidied |  |  |
| `atl_tc_2a_header_processorMajorVersion` | `atl_tc_2a_header_processor_major_version` | tidied |  |  |
| `atl_tc_2a_header_processorMinorVersion` | `atl_tc_2a_header_processor_minor_version` | tidied |  |  |
| `atl_tc_2a_header_processorName` | `atl_tc_2a_header_processor_name` | tidied |  |  |
| `atl_tc_2a_header_productLevel` | `atl_tc_2a_header_product_level` | tidied |  |  |
| `atl_tc_2a_header_productName` | `atl_tc_2a_header_product_name` | tidied |  |  |
| `atl_tc_2a_header_productType` | `atl_tc_2a_header_product_type` | tidied |  |  |
| `atl_tc_2a_header_rightAscension` | `atl_tc_2a_header_right_ascension` | tidied |  |  |
| `atl_tc_2a_header_sensingStartTime` | `atl_tc_2a_header_sensing_start_time` | tidied |  |  |
| `atl_tc_2a_header_sensingStopTime` | `atl_tc_2a_header_sensing_stop_time` | tidied |  |  |
| `atl_tc_2a_header_stateVectorSource` | `atl_tc_2a_header_state_vector_source` | tidied |  |  |
| `atl_tc_2a_header_stateVectorTime` | `atl_tc_2a_header_state_vector_time` | tidied |  |  |
| `atl_tc_2a_header_subsettedProduct` | `atl_tc_2a_header_subsetted_product` | tidied |  |  |
| `atl_tc_2a_header_xPosition` | `atl_tc_2a_header_x_position` | tidied |  |  |
| `atl_tc_2a_header_xVelocity` | `atl_tc_2a_header_x_velocity` | tidied |  |  |
| `atl_tc_2a_header_yPosition` | `atl_tc_2a_header_y_position` | tidied |  |  |
| `atl_tc_2a_header_yVelocity` | `atl_tc_2a_header_y_velocity` | tidied |  |  |
| `atl_tc_2a_header_zPosition` | `atl_tc_2a_header_z_position` | tidied |  |  |
| `atl_tc_2a_header_zVelocity` | `atl_tc_2a_header_z_velocity` | tidied |  |  |
| `atl_tc_2a_mie_detection_status` | `atl_tc_2a_mie_channel_detection_status` | long_name | Mie channel detection status | 1 |
| `atl_tc_2a_pressure` | `atl_tc_2a_pressure` | kept |  | Pa |
| `atl_tc_2a_range` | `atl_tc_2a_range_from_lidar` | long_name | Range from lidar | m |
| `atl_tc_2a_rayleigh_detection_status` | `atl_tc_2a_ray_channel_detection_status` | long_name | Ray channel detection status | 1 |
| `atl_tc_2a_size_JSG_height` | `atl_tc_2a_size_jsg_height` | tidied |  |  |
| `aux_jsg_1d_altitude` | `aux_jsg_1d_geodetic_altitude` | long_name | Geodetic altitude (WGS84) | m |
| `aux_jsg_1d_boundary_latitude` | `aux_jsg_1d_geodetic_latitude_of_along_track_cell_boundary` | long_name | Geodetic latitude of along-track cell boundary (WGS84) | degree_north |
| `aux_jsg_1d_boundary_longitude` | `aux_jsg_1d_geodetic_longitude_of_along_track_cell_boundary` | long_name | Geodetic longitude of along-track cell boundary (WGS84) | degree_east |
| `aux_jsg_1d_geoid_offset` | `aux_jsg_1d_height_of_geoid_egm96_over_the_ellipsoid_wgs84` | long_name | Height of geoid EGM96 over the ellipsoid WGS84 (in m). | m |
| `aux_jsg_1d_header_ANXLongitude` | `aux_jsg_1d_header_anx_longitude` | tidied |  |  |
| `aux_jsg_1d_header_ANXTime` | `aux_jsg_1d_header_anx_time` | tidied |  |  |
| `aux_jsg_1d_header_File_Class` | `aux_jsg_1d_header_File_Class` | clash |  |  |
| `aux_jsg_1d_header_File_Description` | `aux_jsg_1d_header_file_description` | tidied |  |  |
| `aux_jsg_1d_header_File_Name` | `aux_jsg_1d_header_file_name` | tidied |  |  |
| `aux_jsg_1d_header_File_Type` | `aux_jsg_1d_header_file_type` | tidied |  |  |
| `aux_jsg_1d_header_File_Version` | `aux_jsg_1d_header_file_version` | tidied |  |  |
| `aux_jsg_1d_header_Mission` | `aux_jsg_1d_header_mission` | tidied |  |  |
| `aux_jsg_1d_header_Notes` | `aux_jsg_1d_header_notes` | tidied |  |  |
| `aux_jsg_1d_header_Source_Creation_Date` | `aux_jsg_1d_header_source_creation_date` | tidied |  |  |
| `aux_jsg_1d_header_Source_Creator` | `aux_jsg_1d_header_source_creator` | tidied |  |  |
| `aux_jsg_1d_header_Source_Creator_Version` | `aux_jsg_1d_header_source_creator_version` | tidied |  |  |
| `aux_jsg_1d_header_Source_System` | `aux_jsg_1d_header_source_system` | tidied |  |  |
| `aux_jsg_1d_header_Validity_Period_Validity_Start` | `aux_jsg_1d_header_validity_period_validity_start` | tidied |  |  |
| `aux_jsg_1d_header_Validity_Period_Validity_Stop` | `aux_jsg_1d_header_validity_period_validity_stop` | tidied |  |  |
| `aux_jsg_1d_header_acquisitionStation` | `aux_jsg_1d_header_acquisition_station` | tidied |  |  |
| `aux_jsg_1d_header_degradedProductQualityFlag` | `aux_jsg_1d_header_degraded_product_quality_flag` | tidied |  |  |
| `aux_jsg_1d_header_executableMajorVersion` | `aux_jsg_1d_header_executable_major_version` | tidied |  |  |
| `aux_jsg_1d_header_executableMinorVersion` | `aux_jsg_1d_header_executable_minor_version` | tidied |  |  |
| `aux_jsg_1d_header_fileCategory` | `aux_jsg_1d_header_file_category` | tidied |  |  |
| `aux_jsg_1d_header_fileClass` | `aux_jsg_1d_header_fileClass` | clash |  |  |
| `aux_jsg_1d_header_formatMajorVersion` | `aux_jsg_1d_header_format_major_version` | tidied |  |  |
| `aux_jsg_1d_header_formatMinorVersion` | `aux_jsg_1d_header_format_minor_version` | tidied |  |  |
| `aux_jsg_1d_header_frameID` | `aux_jsg_1d_header_frame_id` | tidied |  |  |
| `aux_jsg_1d_header_frameStartCoordinates_geographicLatitude` | `aux_jsg_1d_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `aux_jsg_1d_header_frameStartCoordinates_geographicLongitude` | `aux_jsg_1d_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `aux_jsg_1d_header_frameStartMargin` | `aux_jsg_1d_header_frame_start_margin` | tidied |  |  |
| `aux_jsg_1d_header_frameStartTime` | `aux_jsg_1d_header_frame_start_time` | tidied |  |  |
| `aux_jsg_1d_header_frameStopCoordinates_geographicLatitude` | `aux_jsg_1d_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `aux_jsg_1d_header_frameStopCoordinates_geographicLongitude` | `aux_jsg_1d_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `aux_jsg_1d_header_frameStopMargin` | `aux_jsg_1d_header_frame_stop_margin` | tidied |  |  |
| `aux_jsg_1d_header_frameStopTime` | `aux_jsg_1d_header_frame_stop_time` | tidied |  |  |
| `aux_jsg_1d_header_meanAnomaly` | `aux_jsg_1d_header_mean_anomaly` | tidied |  |  |
| `aux_jsg_1d_header_missionID` | `aux_jsg_1d_header_mission_id` | tidied |  |  |
| `aux_jsg_1d_header_orbitEccentricity` | `aux_jsg_1d_header_orbit_eccentricity` | tidied |  |  |
| `aux_jsg_1d_header_orbitInclination` | `aux_jsg_1d_header_orbit_inclination` | tidied |  |  |
| `aux_jsg_1d_header_orbitNumber` | `aux_jsg_1d_header_orbit_number` | tidied |  |  |
| `aux_jsg_1d_header_orbitSemiMajorAxis` | `aux_jsg_1d_header_orbit_semi_major_axis` | tidied |  |  |
| `aux_jsg_1d_header_originalProductName` | `aux_jsg_1d_header_original_product_name` | tidied |  |  |
| `aux_jsg_1d_header_perigeeArgument` | `aux_jsg_1d_header_perigee_argument` | tidied |  |  |
| `aux_jsg_1d_header_processingCentre` | `aux_jsg_1d_header_processing_centre` | tidied |  |  |
| `aux_jsg_1d_header_processingStartTime` | `aux_jsg_1d_header_processing_start_time` | tidied |  |  |
| `aux_jsg_1d_header_processingStopTime` | `aux_jsg_1d_header_processing_stop_time` | tidied |  |  |
| `aux_jsg_1d_header_processorMajorVersion` | `aux_jsg_1d_header_processor_major_version` | tidied |  |  |
| `aux_jsg_1d_header_processorMinorVersion` | `aux_jsg_1d_header_processor_minor_version` | tidied |  |  |
| `aux_jsg_1d_header_processorName` | `aux_jsg_1d_header_processor_name` | tidied |  |  |
| `aux_jsg_1d_header_productLevel` | `aux_jsg_1d_header_product_level` | tidied |  |  |
| `aux_jsg_1d_header_productName` | `aux_jsg_1d_header_product_name` | tidied |  |  |
| `aux_jsg_1d_header_productType` | `aux_jsg_1d_header_product_type` | tidied |  |  |
| `aux_jsg_1d_header_rightAscension` | `aux_jsg_1d_header_right_ascension` | tidied |  |  |
| `aux_jsg_1d_header_sensingStartTime` | `aux_jsg_1d_header_sensing_start_time` | tidied |  |  |
| `aux_jsg_1d_header_sensingStopTime` | `aux_jsg_1d_header_sensing_stop_time` | tidied |  |  |
| `aux_jsg_1d_header_stateVectorSource` | `aux_jsg_1d_header_state_vector_source` | tidied |  |  |
| `aux_jsg_1d_header_stateVectorTime` | `aux_jsg_1d_header_state_vector_time` | tidied |  |  |
| `aux_jsg_1d_header_subsettedProduct` | `aux_jsg_1d_header_subsetted_product` | tidied |  |  |
| `aux_jsg_1d_header_xPosition` | `aux_jsg_1d_header_x_position` | tidied |  |  |
| `aux_jsg_1d_header_xVelocity` | `aux_jsg_1d_header_x_velocity` | tidied |  |  |
| `aux_jsg_1d_header_yPosition` | `aux_jsg_1d_header_y_position` | tidied |  |  |
| `aux_jsg_1d_header_yVelocity` | `aux_jsg_1d_header_y_velocity` | tidied |  |  |
| `aux_jsg_1d_header_zPosition` | `aux_jsg_1d_header_z_position` | tidied |  |  |
| `aux_jsg_1d_header_zVelocity` | `aux_jsg_1d_header_z_velocity` | tidied |  |  |
| `aux_jsg_1d_land_flag` | `aux_jsg_1d_land_1_water_0_for_the_pixel_centre` | long_name | Land = 1, Water = 0 for the pixel centre | 1 |
| `aux_jsg_1d_lidar_profile_count` | `aux_jsg_1d_total_number_of_lidar_profiles_in_this_pixel` | long_name | Total number of lidar profiles in this pixel (including non  | 1 |
| `aux_jsg_1d_lidar_radar_displacement_across_track` | `aux_jsg_1d_distance_between_the_radar_and_lidar_profiles` | long_name | Distance between the radar and lidar profiles | m |
| `aux_jsg_1d_radar_profile_count` | `aux_jsg_1d_number_of_radar_profiles_in_this_pixel` | long_name | Number of radar profiles in this pixel | 1 |
| `aux_jsg_1d_sensor_altitude` | `aux_jsg_1d_spacecraft_altitude_on_the_along_track_grid_point` | long_name | Spacecraft altitude on the along-track grid point | m |
| `aux_jsg_1d_sensor_azimuth_angle` | `aux_jsg_1d_viewing_azimuth` | long_name | Viewing azimuth (topocentric CS) | deg |
| `aux_jsg_1d_sensor_elevation_angle` | `aux_jsg_1d_viewing_elevation` | long_name | Viewing elevation (topocentric CS) | deg |
| `aux_jsg_1d_solar_azimuth_angle` | `aux_jsg_1d_solar_azimuth` | long_name | Solar azimuth (topocentric CS) | deg |
| `aux_jsg_1d_solar_elevation_angle` | `aux_jsg_1d_solar_elevation` | long_name | Solar elevation (topocentric CS) | deg |
| `cpr_cd_2a_along_track_integration_length` | `cpr_cd_2a_along_track_length_of_the_integration_window` | long_name | Along-track length of the integration window | m |
| `cpr_cd_2a_doppler_velocity_and_spectrum_width_integrated_error` | `cpr_cd_2a_integrated_doppler_velocity_and_spectrum_width_error` | long_name | Integrated Doppler velocity and spectrum width error | dB |
| `cpr_cd_2a_doppler_velocity_and_spectrum_width_integrated_status` | `cpr_cd_2a_integrated_doppler_velocity_and_spectrum_width_status` | long_name | Integrated Doppler velocity and spectrum width status |  |
| `cpr_cd_2a_doppler_velocity_integrated` | `cpr_cd_2a_integrated_doppler_velocity` | long_name | Integrated Doppler velocity | m s-1 |
| `cpr_cd_2a_doppler_velocity_integrated_error` | `cpr_cd_2a_integrated_doppler_velocity_error` | long_name | Integrated Doppler velocity error | m s-1 |
| `cpr_cd_2a_geoid_offset` | `cpr_cd_2a_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `cpr_cd_2a_header_ANXLongitude` | `cpr_cd_2a_header_anx_longitude` | tidied |  |  |
| `cpr_cd_2a_header_ANXTime` | `cpr_cd_2a_header_anx_time` | tidied |  |  |
| `cpr_cd_2a_header_File_Class` | `cpr_cd_2a_header_File_Class` | clash |  |  |
| `cpr_cd_2a_header_File_Description` | `cpr_cd_2a_header_file_description` | tidied |  |  |
| `cpr_cd_2a_header_File_Name` | `cpr_cd_2a_header_file_name` | tidied |  |  |
| `cpr_cd_2a_header_File_Type` | `cpr_cd_2a_header_file_type` | tidied |  |  |
| `cpr_cd_2a_header_File_Version` | `cpr_cd_2a_header_file_version` | tidied |  |  |
| `cpr_cd_2a_header_Mission` | `cpr_cd_2a_header_mission` | tidied |  |  |
| `cpr_cd_2a_header_Notes` | `cpr_cd_2a_header_notes` | tidied |  |  |
| `cpr_cd_2a_header_Source_Creation_Date` | `cpr_cd_2a_header_source_creation_date` | tidied |  |  |
| `cpr_cd_2a_header_Source_Creator` | `cpr_cd_2a_header_source_creator` | tidied |  |  |
| `cpr_cd_2a_header_Source_Creator_Version` | `cpr_cd_2a_header_source_creator_version` | tidied |  |  |
| `cpr_cd_2a_header_Source_System` | `cpr_cd_2a_header_source_system` | tidied |  |  |
| `cpr_cd_2a_header_Validity_Period_Validity_Start` | `cpr_cd_2a_header_validity_period_validity_start` | tidied |  |  |
| `cpr_cd_2a_header_Validity_Period_Validity_Stop` | `cpr_cd_2a_header_validity_period_validity_stop` | tidied |  |  |
| `cpr_cd_2a_header_acquisitionStation` | `cpr_cd_2a_header_acquisition_station` | tidied |  |  |
| `cpr_cd_2a_header_degradedProductQualityFlag` | `cpr_cd_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `cpr_cd_2a_header_executableMajorVersion` | `cpr_cd_2a_header_executable_major_version` | tidied |  |  |
| `cpr_cd_2a_header_executableMinorVersion` | `cpr_cd_2a_header_executable_minor_version` | tidied |  |  |
| `cpr_cd_2a_header_fileCategory` | `cpr_cd_2a_header_file_category` | tidied |  |  |
| `cpr_cd_2a_header_fileClass` | `cpr_cd_2a_header_fileClass` | clash |  |  |
| `cpr_cd_2a_header_formatMajorVersion` | `cpr_cd_2a_header_format_major_version` | tidied |  |  |
| `cpr_cd_2a_header_formatMinorVersion` | `cpr_cd_2a_header_format_minor_version` | tidied |  |  |
| `cpr_cd_2a_header_frameID` | `cpr_cd_2a_header_frame_id` | tidied |  |  |
| `cpr_cd_2a_header_frameStartCoordinates_geographicLatitude` | `cpr_cd_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_cd_2a_header_frameStartCoordinates_geographicLongitude` | `cpr_cd_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_cd_2a_header_frameStartMargin` | `cpr_cd_2a_header_frame_start_margin` | tidied |  |  |
| `cpr_cd_2a_header_frameStartTime` | `cpr_cd_2a_header_frame_start_time` | tidied |  |  |
| `cpr_cd_2a_header_frameStopCoordinates_geographicLatitude` | `cpr_cd_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_cd_2a_header_frameStopCoordinates_geographicLongitude` | `cpr_cd_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_cd_2a_header_frameStopMargin` | `cpr_cd_2a_header_frame_stop_margin` | tidied |  |  |
| `cpr_cd_2a_header_frameStopTime` | `cpr_cd_2a_header_frame_stop_time` | tidied |  |  |
| `cpr_cd_2a_header_meanAnomaly` | `cpr_cd_2a_header_mean_anomaly` | tidied |  |  |
| `cpr_cd_2a_header_missionID` | `cpr_cd_2a_header_mission_id` | tidied |  |  |
| `cpr_cd_2a_header_orbitEccentricity` | `cpr_cd_2a_header_orbit_eccentricity` | tidied |  |  |
| `cpr_cd_2a_header_orbitInclination` | `cpr_cd_2a_header_orbit_inclination` | tidied |  |  |
| `cpr_cd_2a_header_orbitNumber` | `cpr_cd_2a_header_orbit_number` | tidied |  |  |
| `cpr_cd_2a_header_orbitSemiMajorAxis` | `cpr_cd_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `cpr_cd_2a_header_originalProductName` | `cpr_cd_2a_header_original_product_name` | tidied |  |  |
| `cpr_cd_2a_header_perigeeArgument` | `cpr_cd_2a_header_perigee_argument` | tidied |  |  |
| `cpr_cd_2a_header_processingCentre` | `cpr_cd_2a_header_processing_centre` | tidied |  |  |
| `cpr_cd_2a_header_processingStartTime` | `cpr_cd_2a_header_processing_start_time` | tidied |  |  |
| `cpr_cd_2a_header_processingStopTime` | `cpr_cd_2a_header_processing_stop_time` | tidied |  |  |
| `cpr_cd_2a_header_processorMajorVersion` | `cpr_cd_2a_header_processor_major_version` | tidied |  |  |
| `cpr_cd_2a_header_processorMinorVersion` | `cpr_cd_2a_header_processor_minor_version` | tidied |  |  |
| `cpr_cd_2a_header_processorName` | `cpr_cd_2a_header_processor_name` | tidied |  |  |
| `cpr_cd_2a_header_productLevel` | `cpr_cd_2a_header_product_level` | tidied |  |  |
| `cpr_cd_2a_header_productName` | `cpr_cd_2a_header_product_name` | tidied |  |  |
| `cpr_cd_2a_header_productType` | `cpr_cd_2a_header_product_type` | tidied |  |  |
| `cpr_cd_2a_header_rightAscension` | `cpr_cd_2a_header_right_ascension` | tidied |  |  |
| `cpr_cd_2a_header_sensingStartTime` | `cpr_cd_2a_header_sensing_start_time` | tidied |  |  |
| `cpr_cd_2a_header_sensingStopTime` | `cpr_cd_2a_header_sensing_stop_time` | tidied |  |  |
| `cpr_cd_2a_header_stateVectorSource` | `cpr_cd_2a_header_state_vector_source` | tidied |  |  |
| `cpr_cd_2a_header_stateVectorTime` | `cpr_cd_2a_header_state_vector_time` | tidied |  |  |
| `cpr_cd_2a_header_subsettedProduct` | `cpr_cd_2a_header_subsetted_product` | tidied |  |  |
| `cpr_cd_2a_header_xPosition` | `cpr_cd_2a_header_x_position` | tidied |  |  |
| `cpr_cd_2a_header_xVelocity` | `cpr_cd_2a_header_x_velocity` | tidied |  |  |
| `cpr_cd_2a_header_yPosition` | `cpr_cd_2a_header_y_position` | tidied |  |  |
| `cpr_cd_2a_header_yVelocity` | `cpr_cd_2a_header_y_velocity` | tidied |  |  |
| `cpr_cd_2a_header_zPosition` | `cpr_cd_2a_header_z_position` | tidied |  |  |
| `cpr_cd_2a_header_zVelocity` | `cpr_cd_2a_header_z_velocity` | tidied |  |  |
| `cpr_cd_2a_quality_status` | `cpr_cd_2a_corrected_doppler_velocity_quality_status` | long_name | Corrected Doppler velocity quality status |  |
| `cpr_cd_2a_sedimentation_velocity_best_estimate` | `cpr_cd_2a_best_estimate_of_sedimentation_velocity` | long_name | Best estimate of sedimentation velocity | m s-1 |
| `cpr_cd_2a_sedimentation_velocity_best_estimate_error` | `cpr_cd_2a_best_estimate_sedimentation_velocity_error` | long_name | Best estimate sedimentation velocity error | m s-1 |
| `cpr_cd_2a_sedimentation_velocity_best_estimate_integration_length` | `cpr_cd_2a_best_estimate_sedimentation_velocity_integration_length` | long_name | Best estimate sedimentation velocity integration length | km |
| `cpr_cd_2a_sedimentation_velocity_best_estimate_status` | `cpr_cd_2a_best_estimate_sedimentation_velocity_status` | long_name | Best estimate sedimentation velocity status |  |
| `cpr_cd_2a_sedimentation_velocity_best_estimate_variability` | `cpr_cd_2a_best_estimate_sedimentation_velocity_variability` | long_name | Best estimate sedimentation velocity variability | m s-1 |
| `cpr_cd_2a_spectrum_width_integrated` | `cpr_cd_2a_integrated_spectrum_width` | long_name | Integrated spectrum width | m s-1 |
| `cpr_cd_2a_spectrum_width_integrated_error` | `cpr_cd_2a_integrated_spectrum_width_error` | long_name | Integrated spectrum width error | m s-1 |
| `cpr_cd_2a_surface_doppler_velocity` | `cpr_cd_2a_measured_doppler_velocity_at_the_ground` | long_name | Measured Doppler velocity at the ground | m s-1 |
| `cpr_cd_2a_surface_elevation` | `cpr_cd_2a_surface_elevation_above_wgs84_ellipsoid` | long_name | Surface elevation above WGS84 ellipsoid | m |
| `cpr_cd_2a_vertical_integration_length` | `cpr_cd_2a_vertical_extent_of_the_integration_window` | long_name | Vertical extent of the integration window | m |
| `cpr_cld_2a_attenuation` | `cpr_cld_2a_2_way_attenuation` | long_name | 2-way attenuation | dB |
| `cpr_cld_2a_characteristic_diameter` | `cpr_cld_2a_retrieved_characteristic_diameter` | long_name | Retrieved characteristic diameter | m |
| `cpr_cld_2a_chi_square_normalized` | `cpr_cld_2a_normalized_chi_square` | long_name | Normalized Chi square | 1 |
| `cpr_cld_2a_doppler_velocity_classification` | `cpr_cld_2a_c_tc_doppler_velocity_classification` | long_name | C-TC Doppler velocity classification |  |
| `cpr_cld_2a_doppler_velocity_forward` | `cpr_cld_2a_forward_doppler_velocity` | long_name | Forward Doppler velocity | m s-1 |
| `cpr_cld_2a_drizzle_mass_flux` | `cpr_cld_2a_drizzle_mass_flux_at_cloud_base` | long_name | Drizzle mass flux at cloud base | kg m-2 s-1 |
| `cpr_cld_2a_geoid_offset` | `cpr_cld_2a_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `cpr_cld_2a_header_ANXLongitude` | `cpr_cld_2a_header_anx_longitude` | tidied |  |  |
| `cpr_cld_2a_header_ANXTime` | `cpr_cld_2a_header_anx_time` | tidied |  |  |
| `cpr_cld_2a_header_File_Class` | `cpr_cld_2a_header_File_Class` | clash |  |  |
| `cpr_cld_2a_header_File_Description` | `cpr_cld_2a_header_file_description` | tidied |  |  |
| `cpr_cld_2a_header_File_Name` | `cpr_cld_2a_header_file_name` | tidied |  |  |
| `cpr_cld_2a_header_File_Type` | `cpr_cld_2a_header_file_type` | tidied |  |  |
| `cpr_cld_2a_header_File_Version` | `cpr_cld_2a_header_file_version` | tidied |  |  |
| `cpr_cld_2a_header_Mission` | `cpr_cld_2a_header_mission` | tidied |  |  |
| `cpr_cld_2a_header_Notes` | `cpr_cld_2a_header_notes` | tidied |  |  |
| `cpr_cld_2a_header_Source_Creation_Date` | `cpr_cld_2a_header_source_creation_date` | tidied |  |  |
| `cpr_cld_2a_header_Source_Creator` | `cpr_cld_2a_header_source_creator` | tidied |  |  |
| `cpr_cld_2a_header_Source_Creator_Version` | `cpr_cld_2a_header_source_creator_version` | tidied |  |  |
| `cpr_cld_2a_header_Source_System` | `cpr_cld_2a_header_source_system` | tidied |  |  |
| `cpr_cld_2a_header_Validity_Period_Validity_Start` | `cpr_cld_2a_header_validity_period_validity_start` | tidied |  |  |
| `cpr_cld_2a_header_Validity_Period_Validity_Stop` | `cpr_cld_2a_header_validity_period_validity_stop` | tidied |  |  |
| `cpr_cld_2a_header_acquisitionStation` | `cpr_cld_2a_header_acquisition_station` | tidied |  |  |
| `cpr_cld_2a_header_degradedProductQualityFlag` | `cpr_cld_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `cpr_cld_2a_header_executableMajorVersion` | `cpr_cld_2a_header_executable_major_version` | tidied |  |  |
| `cpr_cld_2a_header_executableMinorVersion` | `cpr_cld_2a_header_executable_minor_version` | tidied |  |  |
| `cpr_cld_2a_header_fileCategory` | `cpr_cld_2a_header_file_category` | tidied |  |  |
| `cpr_cld_2a_header_fileClass` | `cpr_cld_2a_header_fileClass` | clash |  |  |
| `cpr_cld_2a_header_formatMajorVersion` | `cpr_cld_2a_header_format_major_version` | tidied |  |  |
| `cpr_cld_2a_header_formatMinorVersion` | `cpr_cld_2a_header_format_minor_version` | tidied |  |  |
| `cpr_cld_2a_header_frameID` | `cpr_cld_2a_header_frame_id` | tidied |  |  |
| `cpr_cld_2a_header_frameStartCoordinates_geographicLatitude` | `cpr_cld_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_cld_2a_header_frameStartCoordinates_geographicLongitude` | `cpr_cld_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_cld_2a_header_frameStartMargin` | `cpr_cld_2a_header_frame_start_margin` | tidied |  |  |
| `cpr_cld_2a_header_frameStartTime` | `cpr_cld_2a_header_frame_start_time` | tidied |  |  |
| `cpr_cld_2a_header_frameStopCoordinates_geographicLatitude` | `cpr_cld_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_cld_2a_header_frameStopCoordinates_geographicLongitude` | `cpr_cld_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_cld_2a_header_frameStopMargin` | `cpr_cld_2a_header_frame_stop_margin` | tidied |  |  |
| `cpr_cld_2a_header_frameStopTime` | `cpr_cld_2a_header_frame_stop_time` | tidied |  |  |
| `cpr_cld_2a_header_meanAnomaly` | `cpr_cld_2a_header_mean_anomaly` | tidied |  |  |
| `cpr_cld_2a_header_missionID` | `cpr_cld_2a_header_mission_id` | tidied |  |  |
| `cpr_cld_2a_header_orbitEccentricity` | `cpr_cld_2a_header_orbit_eccentricity` | tidied |  |  |
| `cpr_cld_2a_header_orbitInclination` | `cpr_cld_2a_header_orbit_inclination` | tidied |  |  |
| `cpr_cld_2a_header_orbitNumber` | `cpr_cld_2a_header_orbit_number` | tidied |  |  |
| `cpr_cld_2a_header_orbitSemiMajorAxis` | `cpr_cld_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `cpr_cld_2a_header_originalProductName` | `cpr_cld_2a_header_original_product_name` | tidied |  |  |
| `cpr_cld_2a_header_perigeeArgument` | `cpr_cld_2a_header_perigee_argument` | tidied |  |  |
| `cpr_cld_2a_header_processingCentre` | `cpr_cld_2a_header_processing_centre` | tidied |  |  |
| `cpr_cld_2a_header_processingStartTime` | `cpr_cld_2a_header_processing_start_time` | tidied |  |  |
| `cpr_cld_2a_header_processingStopTime` | `cpr_cld_2a_header_processing_stop_time` | tidied |  |  |
| `cpr_cld_2a_header_processorMajorVersion` | `cpr_cld_2a_header_processor_major_version` | tidied |  |  |
| `cpr_cld_2a_header_processorMinorVersion` | `cpr_cld_2a_header_processor_minor_version` | tidied |  |  |
| `cpr_cld_2a_header_processorName` | `cpr_cld_2a_header_processor_name` | tidied |  |  |
| `cpr_cld_2a_header_productLevel` | `cpr_cld_2a_header_product_level` | tidied |  |  |
| `cpr_cld_2a_header_productName` | `cpr_cld_2a_header_product_name` | tidied |  |  |
| `cpr_cld_2a_header_productType` | `cpr_cld_2a_header_product_type` | tidied |  |  |
| `cpr_cld_2a_header_rightAscension` | `cpr_cld_2a_header_right_ascension` | tidied |  |  |
| `cpr_cld_2a_header_sensingStartTime` | `cpr_cld_2a_header_sensing_start_time` | tidied |  |  |
| `cpr_cld_2a_header_sensingStopTime` | `cpr_cld_2a_header_sensing_stop_time` | tidied |  |  |
| `cpr_cld_2a_header_stateVectorSource` | `cpr_cld_2a_header_state_vector_source` | tidied |  |  |
| `cpr_cld_2a_header_stateVectorTime` | `cpr_cld_2a_header_state_vector_time` | tidied |  |  |
| `cpr_cld_2a_header_subsettedProduct` | `cpr_cld_2a_header_subsetted_product` | tidied |  |  |
| `cpr_cld_2a_header_xPosition` | `cpr_cld_2a_header_x_position` | tidied |  |  |
| `cpr_cld_2a_header_xVelocity` | `cpr_cld_2a_header_x_velocity` | tidied |  |  |
| `cpr_cld_2a_header_yPosition` | `cpr_cld_2a_header_y_position` | tidied |  |  |
| `cpr_cld_2a_header_yVelocity` | `cpr_cld_2a_header_y_velocity` | tidied |  |  |
| `cpr_cld_2a_header_zPosition` | `cpr_cld_2a_header_z_position` | tidied |  |  |
| `cpr_cld_2a_header_zVelocity` | `cpr_cld_2a_header_z_velocity` | tidied |  |  |
| `cpr_cld_2a_ice_water_path_error` | `cpr_cld_2a_standard_deviation_of_the_ice_water_path` | long_name | Standard deviation of the ice water path | kg m-2 |
| `cpr_cld_2a_liquid_effective_radius` | `cpr_cld_2a_liquid_cloud_effective_radius` | long_name | Liquid cloud effective radius | m |
| `cpr_cld_2a_liquid_water_content` | `cpr_cld_2a_liquid_cloud_water_content` | long_name | Liquid cloud water content | kg m-3 |
| `cpr_cld_2a_liquid_water_path` | `cpr_cld_2a_liquid_cloud_water_path` | long_name | Liquid cloud water path | kg m-2 |
| `cpr_cld_2a_maximum_dimension_L` | `cpr_cld_2a_maximum_dimension_l` | tidied | Physical dimension (actual maximum size) of retrieved ice/sn | m |
| `cpr_cld_2a_maximum_dimension_L_lower_error_bound` | `cpr_cld_2a_maximum_dimension_l_lower_error_bound` | tidied | Lower bound of the actual maximum size of retrieved particle | m |
| `cpr_cld_2a_maximum_dimension_L_upper_error_bound` | `cpr_cld_2a_maximum_dimension_l_upper_error_bound` | tidied | Upper bound of the actual maximum size of retrieved particle | m |
| `cpr_cld_2a_number_concentration_parameter_N0_star` | `cpr_cld_2a_number_concentration_parameter_n0` | long_name | Number concentration parameter N0* | m-4 |
| `cpr_cld_2a_number_concentration_parameter_N0_star_log_error` | `cpr_cld_2a_1_sigma_error_of_the_n0_decimal_logarithm` | long_name | 1-sigma error of the N0* decimal logarithm | 1 |
| `cpr_cld_2a_path_integrated_attenuation_forward` | `cpr_cld_2a_forward_path_integrated_attenuation` | long_name | Forward path integrated attenuation | dB |
| `cpr_cld_2a_rain_water_path_error` | `cpr_cld_2a_standard_deviation_of_the_rain_water_path` | long_name | Standard deviation of the rain water path | kg m-2 |
| `cpr_cld_2a_reflectivity_factor_forward` | `cpr_cld_2a_forward_reflectivity` | long_name | Forward reflectivity | dBZ |
| `cpr_cld_2a_retrieval_classification` | `cpr_cld_2a_retrieval_type` | long_name | Retrieval type |  |
| `cpr_cld_2a_surface_elevation` | `cpr_cld_2a_surface_elevation_above_wgs84_ellipsoid` | long_name | Surface elevation above WGS84 ellipsoid | m |
| `cpr_cld_2a_water_content` | `cpr_cld_2a_retrieved_water_content` | long_name | Retrieved water content | kg m-3 |
| `cpr_cld_2a_water_content_prior` | `cpr_cld_2a_a_priori_water_content` | long_name | A priori water content | kg m-3 |
| `cpr_fmr_2a_brightness_temperature` | `cpr_fmr_2a_brightness_temperature_at_94_g_hz` | long_name | Brightness temperature at 94GHz | K |
| `cpr_fmr_2a_detection_status` | `cpr_fmr_2a_cpr_detection_status` | long_name | CPR detection status |  |
| `cpr_fmr_2a_geoid_offset` | `cpr_fmr_2a_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `cpr_fmr_2a_header_ANXLongitude` | `cpr_fmr_2a_header_anx_longitude` | tidied |  |  |
| `cpr_fmr_2a_header_ANXTime` | `cpr_fmr_2a_header_anx_time` | tidied |  |  |
| `cpr_fmr_2a_header_File_Class` | `cpr_fmr_2a_header_File_Class` | clash |  |  |
| `cpr_fmr_2a_header_File_Description` | `cpr_fmr_2a_header_file_description` | tidied |  |  |
| `cpr_fmr_2a_header_File_Name` | `cpr_fmr_2a_header_file_name` | tidied |  |  |
| `cpr_fmr_2a_header_File_Type` | `cpr_fmr_2a_header_file_type` | tidied |  |  |
| `cpr_fmr_2a_header_File_Version` | `cpr_fmr_2a_header_file_version` | tidied |  |  |
| `cpr_fmr_2a_header_Mission` | `cpr_fmr_2a_header_mission` | tidied |  |  |
| `cpr_fmr_2a_header_Notes` | `cpr_fmr_2a_header_notes` | tidied |  |  |
| `cpr_fmr_2a_header_Source_Creation_Date` | `cpr_fmr_2a_header_source_creation_date` | tidied |  |  |
| `cpr_fmr_2a_header_Source_Creator` | `cpr_fmr_2a_header_source_creator` | tidied |  |  |
| `cpr_fmr_2a_header_Source_Creator_Version` | `cpr_fmr_2a_header_source_creator_version` | tidied |  |  |
| `cpr_fmr_2a_header_Source_System` | `cpr_fmr_2a_header_source_system` | tidied |  |  |
| `cpr_fmr_2a_header_Validity_Period_Validity_Start` | `cpr_fmr_2a_header_validity_period_validity_start` | tidied |  |  |
| `cpr_fmr_2a_header_Validity_Period_Validity_Stop` | `cpr_fmr_2a_header_validity_period_validity_stop` | tidied |  |  |
| `cpr_fmr_2a_header_acquisitionStation` | `cpr_fmr_2a_header_acquisition_station` | tidied |  |  |
| `cpr_fmr_2a_header_degradedProductQualityFlag` | `cpr_fmr_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `cpr_fmr_2a_header_executableMajorVersion` | `cpr_fmr_2a_header_executable_major_version` | tidied |  |  |
| `cpr_fmr_2a_header_executableMinorVersion` | `cpr_fmr_2a_header_executable_minor_version` | tidied |  |  |
| `cpr_fmr_2a_header_fileCategory` | `cpr_fmr_2a_header_file_category` | tidied |  |  |
| `cpr_fmr_2a_header_fileClass` | `cpr_fmr_2a_header_fileClass` | clash |  |  |
| `cpr_fmr_2a_header_formatMajorVersion` | `cpr_fmr_2a_header_format_major_version` | tidied |  |  |
| `cpr_fmr_2a_header_formatMinorVersion` | `cpr_fmr_2a_header_format_minor_version` | tidied |  |  |
| `cpr_fmr_2a_header_frameID` | `cpr_fmr_2a_header_frame_id` | tidied |  |  |
| `cpr_fmr_2a_header_frameStartCoordinates_geographicLatitude` | `cpr_fmr_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_fmr_2a_header_frameStartCoordinates_geographicLongitude` | `cpr_fmr_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_fmr_2a_header_frameStartMargin` | `cpr_fmr_2a_header_frame_start_margin` | tidied |  |  |
| `cpr_fmr_2a_header_frameStartTime` | `cpr_fmr_2a_header_frame_start_time` | tidied |  |  |
| `cpr_fmr_2a_header_frameStopCoordinates_geographicLatitude` | `cpr_fmr_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_fmr_2a_header_frameStopCoordinates_geographicLongitude` | `cpr_fmr_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_fmr_2a_header_frameStopMargin` | `cpr_fmr_2a_header_frame_stop_margin` | tidied |  |  |
| `cpr_fmr_2a_header_frameStopTime` | `cpr_fmr_2a_header_frame_stop_time` | tidied |  |  |
| `cpr_fmr_2a_header_meanAnomaly` | `cpr_fmr_2a_header_mean_anomaly` | tidied |  |  |
| `cpr_fmr_2a_header_missionID` | `cpr_fmr_2a_header_mission_id` | tidied |  |  |
| `cpr_fmr_2a_header_orbitEccentricity` | `cpr_fmr_2a_header_orbit_eccentricity` | tidied |  |  |
| `cpr_fmr_2a_header_orbitInclination` | `cpr_fmr_2a_header_orbit_inclination` | tidied |  |  |
| `cpr_fmr_2a_header_orbitNumber` | `cpr_fmr_2a_header_orbit_number` | tidied |  |  |
| `cpr_fmr_2a_header_orbitSemiMajorAxis` | `cpr_fmr_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `cpr_fmr_2a_header_originalProductName` | `cpr_fmr_2a_header_original_product_name` | tidied |  |  |
| `cpr_fmr_2a_header_perigeeArgument` | `cpr_fmr_2a_header_perigee_argument` | tidied |  |  |
| `cpr_fmr_2a_header_processingCentre` | `cpr_fmr_2a_header_processing_centre` | tidied |  |  |
| `cpr_fmr_2a_header_processingStartTime` | `cpr_fmr_2a_header_processing_start_time` | tidied |  |  |
| `cpr_fmr_2a_header_processingStopTime` | `cpr_fmr_2a_header_processing_stop_time` | tidied |  |  |
| `cpr_fmr_2a_header_processorMajorVersion` | `cpr_fmr_2a_header_processor_major_version` | tidied |  |  |
| `cpr_fmr_2a_header_processorMinorVersion` | `cpr_fmr_2a_header_processor_minor_version` | tidied |  |  |
| `cpr_fmr_2a_header_processorName` | `cpr_fmr_2a_header_processor_name` | tidied |  |  |
| `cpr_fmr_2a_header_productLevel` | `cpr_fmr_2a_header_product_level` | tidied |  |  |
| `cpr_fmr_2a_header_productName` | `cpr_fmr_2a_header_product_name` | tidied |  |  |
| `cpr_fmr_2a_header_productType` | `cpr_fmr_2a_header_product_type` | tidied |  |  |
| `cpr_fmr_2a_header_rightAscension` | `cpr_fmr_2a_header_right_ascension` | tidied |  |  |
| `cpr_fmr_2a_header_sensingStartTime` | `cpr_fmr_2a_header_sensing_start_time` | tidied |  |  |
| `cpr_fmr_2a_header_sensingStopTime` | `cpr_fmr_2a_header_sensing_stop_time` | tidied |  |  |
| `cpr_fmr_2a_header_stateVectorSource` | `cpr_fmr_2a_header_state_vector_source` | tidied |  |  |
| `cpr_fmr_2a_header_stateVectorTime` | `cpr_fmr_2a_header_state_vector_time` | tidied |  |  |
| `cpr_fmr_2a_header_subsettedProduct` | `cpr_fmr_2a_header_subsetted_product` | tidied |  |  |
| `cpr_fmr_2a_header_xPosition` | `cpr_fmr_2a_header_x_position` | tidied |  |  |
| `cpr_fmr_2a_header_xVelocity` | `cpr_fmr_2a_header_x_velocity` | tidied |  |  |
| `cpr_fmr_2a_header_yPosition` | `cpr_fmr_2a_header_y_position` | tidied |  |  |
| `cpr_fmr_2a_header_yVelocity` | `cpr_fmr_2a_header_y_velocity` | tidied |  |  |
| `cpr_fmr_2a_header_zPosition` | `cpr_fmr_2a_header_z_position` | tidied |  |  |
| `cpr_fmr_2a_header_zVelocity` | `cpr_fmr_2a_header_z_velocity` | tidied |  |  |
| `cpr_fmr_2a_normalized_surface_radar_cross_section_clear_sky` | `cpr_fmr_2a_normalized_clear_sky_surface_radar_cross_section` | long_name | Normalized clear sky surface radar cross section | dB |
| `cpr_fmr_2a_profile_class` | `cpr_fmr_2a_profile_identification_status` | long_name | Profile identification status |  |
| `cpr_fmr_2a_quality_status` | `cpr_fmr_2a_feature_mask_and_reflectivity_quality_status` | long_name | Feature mask and reflectivity quality status |  |
| `cpr_fmr_2a_reflectivity_corrected` | `cpr_fmr_2a_reflectivity_corrected_for_gaseous_attenuation` | long_name | Reflectivity corrected for gaseous attenuation | dBZ |
| `cpr_fmr_2a_reflectivity_masked` | `cpr_fmr_2a_reflectivity_after_noise_and_surface_clutter_removal` | long_name | Reflectivity after noise and surface clutter removal | dBZ |
| `cpr_fmr_2a_reflectivity_no_attenuation_correction` | `cpr_fmr_2a_attenuated_reflectivity_factor` | long_name | Attenuated reflectivity factor | dBZ |
| `cpr_fmr_2a_significant_detection_classification_flag` | `cpr_fmr_2a_significant_detection_flag` | long_name | Significant detection flag |  |
| `cpr_fmr_2a_surface_elevation` | `cpr_fmr_2a_surface_elevation_above_wgs84_ellipsoid` | long_name | Surface elevation above WGS84 ellipsoid | m |
| `cpr_fmr_2a_total_gaseous_attenuation` | `cpr_fmr_2a_total_two_way_column_attenuation_due_to_gases` | long_name | Total two-way column attenuation due to gases | dB |
| `cpr_nom_1b_binHeight` | `cpr_nom_1b_bin_height` | long_name | bin height | m |
| `cpr_nom_1b_binStatusFlag` | `cpr_nom_1b_bin_status_flag` | long_name | bin status flag | unitless |
| `cpr_nom_1b_covarianceCoeff` | `cpr_nom_1b_covariance_coefficient` | long_name | covariance coefficient | unitless |
| `cpr_nom_1b_dopplerStatusFlag` | `cpr_nom_1b_doppler_status_flag` | long_name | doppler status flag | unitless |
| `cpr_nom_1b_dopplerVelocity` | `cpr_nom_1b_doppler_velocity` | long_name | doppler velocity | m/s |
| `cpr_nom_1b_dopplerVelocityAtSurfaceBin` | `cpr_nom_1b_doppler_velocity_at_surface_bin` | long_name | doppler velocity at surface bin | m/s |
| `cpr_nom_1b_header_ANXLongitude` | `cpr_nom_1b_header_anx_longitude` | tidied |  |  |
| `cpr_nom_1b_header_ANXTime` | `cpr_nom_1b_header_anx_time` | tidied |  |  |
| `cpr_nom_1b_header_File_Class` | `cpr_nom_1b_header_File_Class` | clash |  |  |
| `cpr_nom_1b_header_File_Description` | `cpr_nom_1b_header_file_description` | tidied |  |  |
| `cpr_nom_1b_header_File_Name` | `cpr_nom_1b_header_file_name` | tidied |  |  |
| `cpr_nom_1b_header_File_Type` | `cpr_nom_1b_header_file_type` | tidied |  |  |
| `cpr_nom_1b_header_File_Version` | `cpr_nom_1b_header_file_version` | tidied |  |  |
| `cpr_nom_1b_header_Mission` | `cpr_nom_1b_header_mission` | tidied |  |  |
| `cpr_nom_1b_header_Notes` | `cpr_nom_1b_header_notes` | tidied |  |  |
| `cpr_nom_1b_header_Source_Creation_Date` | `cpr_nom_1b_header_source_creation_date` | tidied |  |  |
| `cpr_nom_1b_header_Source_Creator` | `cpr_nom_1b_header_source_creator` | tidied |  |  |
| `cpr_nom_1b_header_Source_Creator_Version` | `cpr_nom_1b_header_source_creator_version` | tidied |  |  |
| `cpr_nom_1b_header_Source_System` | `cpr_nom_1b_header_source_system` | tidied |  |  |
| `cpr_nom_1b_header_Validity_Period_Validity_Start` | `cpr_nom_1b_header_validity_period_validity_start` | tidied |  |  |
| `cpr_nom_1b_header_Validity_Period_Validity_Stop` | `cpr_nom_1b_header_validity_period_validity_stop` | tidied |  |  |
| `cpr_nom_1b_header_acquisitionStation` | `cpr_nom_1b_header_acquisition_station` | tidied |  |  |
| `cpr_nom_1b_header_degradedProductQualityFlag` | `cpr_nom_1b_header_degraded_product_quality_flag` | tidied |  |  |
| `cpr_nom_1b_header_executableMajorVersion` | `cpr_nom_1b_header_executable_major_version` | tidied |  |  |
| `cpr_nom_1b_header_executableMinorVersion` | `cpr_nom_1b_header_executable_minor_version` | tidied |  |  |
| `cpr_nom_1b_header_fileCategory` | `cpr_nom_1b_header_file_category` | tidied |  |  |
| `cpr_nom_1b_header_fileClass` | `cpr_nom_1b_header_fileClass` | clash |  |  |
| `cpr_nom_1b_header_formatMajorVersion` | `cpr_nom_1b_header_format_major_version` | tidied |  |  |
| `cpr_nom_1b_header_formatMinorVersion` | `cpr_nom_1b_header_format_minor_version` | tidied |  |  |
| `cpr_nom_1b_header_frameID` | `cpr_nom_1b_header_frame_id` | tidied |  |  |
| `cpr_nom_1b_header_frameStartCoordinates_geographicLatitude` | `cpr_nom_1b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_nom_1b_header_frameStartCoordinates_geographicLongitude` | `cpr_nom_1b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_nom_1b_header_frameStartMargin` | `cpr_nom_1b_header_frame_start_margin` | tidied |  |  |
| `cpr_nom_1b_header_frameStartMarginCount` | `cpr_nom_1b_header_frame_start_margin_count` | tidied |  |  |
| `cpr_nom_1b_header_frameStartTime` | `cpr_nom_1b_header_frame_start_time` | tidied |  |  |
| `cpr_nom_1b_header_frameStopCoordinates_geographicLatitude` | `cpr_nom_1b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_nom_1b_header_frameStopCoordinates_geographicLongitude` | `cpr_nom_1b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_nom_1b_header_frameStopMargin` | `cpr_nom_1b_header_frame_stop_margin` | tidied |  |  |
| `cpr_nom_1b_header_frameStopMarginCount` | `cpr_nom_1b_header_frame_stop_margin_count` | tidied |  |  |
| `cpr_nom_1b_header_frameStopTime` | `cpr_nom_1b_header_frame_stop_time` | tidied |  |  |
| `cpr_nom_1b_header_meanAnomaly` | `cpr_nom_1b_header_mean_anomaly` | tidied |  |  |
| `cpr_nom_1b_header_missionID` | `cpr_nom_1b_header_mission_id` | tidied |  |  |
| `cpr_nom_1b_header_orbitEccentricity` | `cpr_nom_1b_header_orbit_eccentricity` | tidied |  |  |
| `cpr_nom_1b_header_orbitInclination` | `cpr_nom_1b_header_orbit_inclination` | tidied |  |  |
| `cpr_nom_1b_header_orbitNumber` | `cpr_nom_1b_header_orbit_number` | tidied |  |  |
| `cpr_nom_1b_header_orbitSemiMajorAxis` | `cpr_nom_1b_header_orbit_semi_major_axis` | tidied |  |  |
| `cpr_nom_1b_header_originalProductName` | `cpr_nom_1b_header_original_product_name` | tidied |  |  |
| `cpr_nom_1b_header_perigeeArgument` | `cpr_nom_1b_header_perigee_argument` | tidied |  |  |
| `cpr_nom_1b_header_processingCentre` | `cpr_nom_1b_header_processing_centre` | tidied |  |  |
| `cpr_nom_1b_header_processingStartTime` | `cpr_nom_1b_header_processing_start_time` | tidied |  |  |
| `cpr_nom_1b_header_processingStopTime` | `cpr_nom_1b_header_processing_stop_time` | tidied |  |  |
| `cpr_nom_1b_header_processorMajorVersion` | `cpr_nom_1b_header_processor_major_version` | tidied |  |  |
| `cpr_nom_1b_header_processorMinorVersion` | `cpr_nom_1b_header_processor_minor_version` | tidied |  |  |
| `cpr_nom_1b_header_processorName` | `cpr_nom_1b_header_processor_name` | tidied |  |  |
| `cpr_nom_1b_header_productLevel` | `cpr_nom_1b_header_product_level` | tidied |  |  |
| `cpr_nom_1b_header_productName` | `cpr_nom_1b_header_product_name` | tidied |  |  |
| `cpr_nom_1b_header_productType` | `cpr_nom_1b_header_product_type` | tidied |  |  |
| `cpr_nom_1b_header_rightAscension` | `cpr_nom_1b_header_right_ascension` | tidied |  |  |
| `cpr_nom_1b_header_sensingStartTime` | `cpr_nom_1b_header_sensing_start_time` | tidied |  |  |
| `cpr_nom_1b_header_sensingStopTime` | `cpr_nom_1b_header_sensing_stop_time` | tidied |  |  |
| `cpr_nom_1b_header_stateVectorSource` | `cpr_nom_1b_header_state_vector_source` | tidied |  |  |
| `cpr_nom_1b_header_stateVectorTime` | `cpr_nom_1b_header_state_vector_time` | tidied |  |  |
| `cpr_nom_1b_header_subsettedProduct` | `cpr_nom_1b_header_subsetted_product` | tidied |  |  |
| `cpr_nom_1b_header_xPosition` | `cpr_nom_1b_header_x_position` | tidied |  |  |
| `cpr_nom_1b_header_xVelocity` | `cpr_nom_1b_header_x_velocity` | tidied |  |  |
| `cpr_nom_1b_header_yPosition` | `cpr_nom_1b_header_y_position` | tidied |  |  |
| `cpr_nom_1b_header_yVelocity` | `cpr_nom_1b_header_y_velocity` | tidied |  |  |
| `cpr_nom_1b_header_zPosition` | `cpr_nom_1b_header_z_position` | tidied |  |  |
| `cpr_nom_1b_header_zVelocity` | `cpr_nom_1b_header_z_velocity` | tidied |  |  |
| `cpr_nom_1b_integrationNumberDoppler` | `cpr_nom_1b_ray_status_pulse_pair_number` | long_name | ray status pulse-pair number | unitless |
| `cpr_nom_1b_integrationNumberEcho` | `cpr_nom_1b_ray_status_transmission_pulses` | long_name | ray status transmission pulses | unitless |
| `cpr_nom_1b_navigationLandWaterFlg` | `cpr_nom_1b_navigation_land_water_flag` | long_name | navigation land water flag | unitless |
| `cpr_nom_1b_noiseFloorPower` | `cpr_nom_1b_noise_floor_power` | long_name | noise floor power | W |
| `cpr_nom_1b_operationalMode` | `cpr_nom_1b_operational_mode` | long_name | operational mode | unitless |
| `cpr_nom_1b_pitchAngle` | `cpr_nom_1b_pitch_angle` | long_name | pitch angle | degree |
| `cpr_nom_1b_processingFrameNo` | `cpr_nom_1b_processing_frame_number` | long_name | processing frame number | unitless |
| `cpr_nom_1b_profileTime` | `cpr_nom_1b_profile_time` | long_name | profile time | seconds since 2000-1-1 00:00:00.0 0:00 |
| `cpr_nom_1b_pulseShapeWarnFlag` | `cpr_nom_1b_pulse_shape_warn_flag` | long_name | pulse shape warn flag | unitless |
| `cpr_nom_1b_pulseWidth` | `cpr_nom_1b_pulse_width` | long_name | pulse width | us |
| `cpr_nom_1b_radarCoefficient` | `cpr_nom_1b_radar_coefficient` | long_name | radar coefficient | 1/m3 |
| `cpr_nom_1b_radarReflectivityFactor` | `cpr_nom_1b_radar_reflectivity_factor` | long_name | radar reflectivity factor | mm6/m3 |
| `cpr_nom_1b_rangeBinMaxNumber` | `cpr_nom_1b_bin_number` | long_name | bin number | unitless |
| `cpr_nom_1b_rangeBinValidNumber` | `cpr_nom_1b_valid_bin_number` | long_name | valid bin number | unitless |
| `cpr_nom_1b_rangeToFirstBin` | `cpr_nom_1b_range_to_first_bin` | long_name | range to first bin | m |
| `cpr_nom_1b_rangeToIntercept` | `cpr_nom_1b_range_to_intercept` | long_name | range to intercept | m |
| `cpr_nom_1b_rayHeaderCalVers` | `cpr_nom_1b_ray_header_calculation_version` | long_name | ray header calculation version | unitless |
| `cpr_nom_1b_rayHeaderLambda` | `cpr_nom_1b_ray_header_lambda` | long_name | ray header lambda | m |
| `cpr_nom_1b_rayHeaderRangeBinSize` | `cpr_nom_1b_ray_header_range_bin_size` | long_name | ray header range bin size | m |
| `cpr_nom_1b_rayHeaderSpatAvg` | `cpr_nom_1b_ray_header_spatial_average` | long_name | ray header spatial average | m |
| `cpr_nom_1b_rayNumber` | `cpr_nom_1b_ray_number` | long_name | ray number | unitless |
| `cpr_nom_1b_rayQualityFlag` | `cpr_nom_1b_ray_quality_flag` | long_name | ray quality flag | unitless |
| `cpr_nom_1b_rayStatusFlag` | `cpr_nom_1b_ray_status_flag` | long_name | ray status flag | unitless |
| `cpr_nom_1b_rayStatusPrf` | `cpr_nom_1b_ray_status_prf` | long_name | ray status prf | Hz |
| `cpr_nom_1b_receivedEchoPower` | `cpr_nom_1b_received_echo_power` | long_name | received echo power | W |
| `cpr_nom_1b_rollAngle` | `cpr_nom_1b_roll_angle` | long_name | roll angle | degree |
| `cpr_nom_1b_satelliteVelocityContaminationInLOS` | `cpr_nom_1b_satellite_velocity_contamination_in_los` | tidied | satellite velocity contamination in line-of-sight | m/s |
| `cpr_nom_1b_satelliteVelocityX` | `cpr_nom_1b_satellite_velocity_x` | long_name | satellite velocity X | m/s |
| `cpr_nom_1b_satelliteVelocityY` | `cpr_nom_1b_satellite_velocity_y` | long_name | satellite velocity Y | m/s |
| `cpr_nom_1b_satelliteVelocityZ` | `cpr_nom_1b_satellite_velocity_z` | long_name | satellite velocity Z | m/s |
| `cpr_nom_1b_sigmaZero` | `cpr_nom_1b_sigma_zero` | long_name | sigma zero | dB |
| `cpr_nom_1b_solarAzimuthAngle` | `cpr_nom_1b_solar_azimuth_angle` | long_name | solar azimuth angle | degree |
| `cpr_nom_1b_solarElevationAngle` | `cpr_nom_1b_solar_elevation_angle` | long_name | solar elevation angle | degree |
| `cpr_nom_1b_spectrumWidth` | `cpr_nom_1b_spectrum_width` | long_name | spectrum width | m/s |
| `cpr_nom_1b_subOperationalMode` | `cpr_nom_1b_sub_operational_mode` | long_name | sub operational mode | unitless |
| `cpr_nom_1b_surfaceBinFraction` | `cpr_nom_1b_surface_bin_fraction` | long_name | surface bin fraction | unitless |
| `cpr_nom_1b_surfaceBinNumber` | `cpr_nom_1b_surface_bin_number` | long_name | surface bin number | unitless |
| `cpr_nom_1b_surfaceElevation` | `cpr_nom_1b_surface_elevation` | long_name | surface elevation (WGS84) | m |
| `cpr_nom_1b_surfaceEstimationFlag` | `cpr_nom_1b_surface_estimation_flag` | long_name | surface estimation flag | unitless |
| `cpr_nom_1b_timeFlag` | `cpr_nom_1b_flag` | long_name | time flag | unitless |
| `cpr_nom_1b_transmitPower` | `cpr_nom_1b_transmit_power` | long_name | transmit power | W |
| `cpr_nom_1b_transmitPowerAvg` | `cpr_nom_1b_transmit_power_average` | long_name | transmit power average | W |
| `cpr_nom_1b_txRxStatusFlag` | `cpr_nom_1b_txrx_status_flag` | long_name | txrx status flag | unitless |
| `cpr_nom_1b_xPosition` | `cpr_nom_1b_satellite_position_x` | long_name | satellite position X | m |
| `cpr_nom_1b_yPosition` | `cpr_nom_1b_satellite_position_y` | long_name | satellite position Y | m |
| `cpr_nom_1b_yawAngle` | `cpr_nom_1b_yaw_angle` | long_name | yaw angle | degree |
| `cpr_nom_1b_zPosition` | `cpr_nom_1b_satellite_position_z` | long_name | satellite position Z | m |
| `cpr_tc_2a_detection_status` | `cpr_tc_2a_cpr_detection_status` | long_name | CPR detection status |  |
| `cpr_tc_2a_geoid_offset` | `cpr_tc_2a_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `cpr_tc_2a_header_ANXLongitude` | `cpr_tc_2a_header_anx_longitude` | tidied |  |  |
| `cpr_tc_2a_header_ANXTime` | `cpr_tc_2a_header_anx_time` | tidied |  |  |
| `cpr_tc_2a_header_File_Class` | `cpr_tc_2a_header_File_Class` | clash |  |  |
| `cpr_tc_2a_header_File_Description` | `cpr_tc_2a_header_file_description` | tidied |  |  |
| `cpr_tc_2a_header_File_Name` | `cpr_tc_2a_header_file_name` | tidied |  |  |
| `cpr_tc_2a_header_File_Type` | `cpr_tc_2a_header_file_type` | tidied |  |  |
| `cpr_tc_2a_header_File_Version` | `cpr_tc_2a_header_file_version` | tidied |  |  |
| `cpr_tc_2a_header_Mission` | `cpr_tc_2a_header_mission` | tidied |  |  |
| `cpr_tc_2a_header_Notes` | `cpr_tc_2a_header_notes` | tidied |  |  |
| `cpr_tc_2a_header_Source_Creation_Date` | `cpr_tc_2a_header_source_creation_date` | tidied |  |  |
| `cpr_tc_2a_header_Source_Creator` | `cpr_tc_2a_header_source_creator` | tidied |  |  |
| `cpr_tc_2a_header_Source_Creator_Version` | `cpr_tc_2a_header_source_creator_version` | tidied |  |  |
| `cpr_tc_2a_header_Source_System` | `cpr_tc_2a_header_source_system` | tidied |  |  |
| `cpr_tc_2a_header_Validity_Period_Validity_Start` | `cpr_tc_2a_header_validity_period_validity_start` | tidied |  |  |
| `cpr_tc_2a_header_Validity_Period_Validity_Stop` | `cpr_tc_2a_header_validity_period_validity_stop` | tidied |  |  |
| `cpr_tc_2a_header_acquisitionStation` | `cpr_tc_2a_header_acquisition_station` | tidied |  |  |
| `cpr_tc_2a_header_degradedProductQualityFlag` | `cpr_tc_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `cpr_tc_2a_header_executableMajorVersion` | `cpr_tc_2a_header_executable_major_version` | tidied |  |  |
| `cpr_tc_2a_header_executableMinorVersion` | `cpr_tc_2a_header_executable_minor_version` | tidied |  |  |
| `cpr_tc_2a_header_fileCategory` | `cpr_tc_2a_header_file_category` | tidied |  |  |
| `cpr_tc_2a_header_fileClass` | `cpr_tc_2a_header_fileClass` | clash |  |  |
| `cpr_tc_2a_header_formatMajorVersion` | `cpr_tc_2a_header_format_major_version` | tidied |  |  |
| `cpr_tc_2a_header_formatMinorVersion` | `cpr_tc_2a_header_format_minor_version` | tidied |  |  |
| `cpr_tc_2a_header_frameID` | `cpr_tc_2a_header_frame_id` | tidied |  |  |
| `cpr_tc_2a_header_frameStartCoordinates_geographicLatitude` | `cpr_tc_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_tc_2a_header_frameStartCoordinates_geographicLongitude` | `cpr_tc_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_tc_2a_header_frameStartMargin` | `cpr_tc_2a_header_frame_start_margin` | tidied |  |  |
| `cpr_tc_2a_header_frameStartTime` | `cpr_tc_2a_header_frame_start_time` | tidied |  |  |
| `cpr_tc_2a_header_frameStopCoordinates_geographicLatitude` | `cpr_tc_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `cpr_tc_2a_header_frameStopCoordinates_geographicLongitude` | `cpr_tc_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `cpr_tc_2a_header_frameStopMargin` | `cpr_tc_2a_header_frame_stop_margin` | tidied |  |  |
| `cpr_tc_2a_header_frameStopTime` | `cpr_tc_2a_header_frame_stop_time` | tidied |  |  |
| `cpr_tc_2a_header_meanAnomaly` | `cpr_tc_2a_header_mean_anomaly` | tidied |  |  |
| `cpr_tc_2a_header_missionID` | `cpr_tc_2a_header_mission_id` | tidied |  |  |
| `cpr_tc_2a_header_orbitEccentricity` | `cpr_tc_2a_header_orbit_eccentricity` | tidied |  |  |
| `cpr_tc_2a_header_orbitInclination` | `cpr_tc_2a_header_orbit_inclination` | tidied |  |  |
| `cpr_tc_2a_header_orbitNumber` | `cpr_tc_2a_header_orbit_number` | tidied |  |  |
| `cpr_tc_2a_header_orbitSemiMajorAxis` | `cpr_tc_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `cpr_tc_2a_header_originalProductName` | `cpr_tc_2a_header_original_product_name` | tidied |  |  |
| `cpr_tc_2a_header_perigeeArgument` | `cpr_tc_2a_header_perigee_argument` | tidied |  |  |
| `cpr_tc_2a_header_processingCentre` | `cpr_tc_2a_header_processing_centre` | tidied |  |  |
| `cpr_tc_2a_header_processingStartTime` | `cpr_tc_2a_header_processing_start_time` | tidied |  |  |
| `cpr_tc_2a_header_processingStopTime` | `cpr_tc_2a_header_processing_stop_time` | tidied |  |  |
| `cpr_tc_2a_header_processorMajorVersion` | `cpr_tc_2a_header_processor_major_version` | tidied |  |  |
| `cpr_tc_2a_header_processorMinorVersion` | `cpr_tc_2a_header_processor_minor_version` | tidied |  |  |
| `cpr_tc_2a_header_processorName` | `cpr_tc_2a_header_processor_name` | tidied |  |  |
| `cpr_tc_2a_header_productLevel` | `cpr_tc_2a_header_product_level` | tidied |  |  |
| `cpr_tc_2a_header_productName` | `cpr_tc_2a_header_product_name` | tidied |  |  |
| `cpr_tc_2a_header_productType` | `cpr_tc_2a_header_product_type` | tidied |  |  |
| `cpr_tc_2a_header_rightAscension` | `cpr_tc_2a_header_right_ascension` | tidied |  |  |
| `cpr_tc_2a_header_sensingStartTime` | `cpr_tc_2a_header_sensing_start_time` | tidied |  |  |
| `cpr_tc_2a_header_sensingStopTime` | `cpr_tc_2a_header_sensing_stop_time` | tidied |  |  |
| `cpr_tc_2a_header_stateVectorSource` | `cpr_tc_2a_header_state_vector_source` | tidied |  |  |
| `cpr_tc_2a_header_stateVectorTime` | `cpr_tc_2a_header_state_vector_time` | tidied |  |  |
| `cpr_tc_2a_header_subsettedProduct` | `cpr_tc_2a_header_subsetted_product` | tidied |  |  |
| `cpr_tc_2a_header_xPosition` | `cpr_tc_2a_header_x_position` | tidied |  |  |
| `cpr_tc_2a_header_xVelocity` | `cpr_tc_2a_header_x_velocity` | tidied |  |  |
| `cpr_tc_2a_header_yPosition` | `cpr_tc_2a_header_y_position` | tidied |  |  |
| `cpr_tc_2a_header_yVelocity` | `cpr_tc_2a_header_y_velocity` | tidied |  |  |
| `cpr_tc_2a_header_zPosition` | `cpr_tc_2a_header_z_position` | tidied |  |  |
| `cpr_tc_2a_header_zVelocity` | `cpr_tc_2a_header_z_velocity` | tidied |  |  |
| `cpr_tc_2a_pressure` | `cpr_tc_2a_pressure` | kept | Pressure | Pa |
| `cpr_tc_2a_quality_status` | `cpr_tc_2a_target_classification_quality_flag` | long_name | Target classification quality flag |  |
| `cpr_tc_2a_size_CPR_height` | `cpr_tc_2a_size_cpr_height` | tidied |  |  |
| `cpr_tc_2a_surface_elevation` | `cpr_tc_2a_surface_elevation_above_wgs84_ellipsoid` | long_name | Surface elevation above WGS84 ellipsoid | m |
| `msi_aot_2a_aerosol_optical_thickness_670nm` | `msi_aot_2a_aerosol_optical_thickness_at_670nm` | long_name | Aerosol optical thickness at 670nm | 1 |
| `msi_aot_2a_aerosol_optical_thickness_865nm` | `msi_aot_2a_aerosol_optical_thickness_at_865nm` | long_name | Aerosol optical thickness at 865nm | 1 |
| `msi_aot_2a_angstrom_parameter_355nm_670nm` | `msi_aot_2a_angstrom_parameter_355nm_670nm` | clash | Angstrom parameter (355nm,670nm) | 1 |
| `msi_aot_2a_angstrom_parameter_670nm_865nm` | `msi_aot_2a_angstrom_parameter_670nm_865nm` | clash | Angstrom parameter (670nm,865nm) | 1 |
| `msi_aot_2a_geoid_offset` | `msi_aot_2a_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `msi_aot_2a_header_ANXLongitude` | `msi_aot_2a_header_anx_longitude` | tidied |  |  |
| `msi_aot_2a_header_ANXTime` | `msi_aot_2a_header_anx_time` | tidied |  |  |
| `msi_aot_2a_header_File_Class` | `msi_aot_2a_header_File_Class` | clash |  |  |
| `msi_aot_2a_header_File_Description` | `msi_aot_2a_header_file_description` | tidied |  |  |
| `msi_aot_2a_header_File_Name` | `msi_aot_2a_header_file_name` | tidied |  |  |
| `msi_aot_2a_header_File_Type` | `msi_aot_2a_header_file_type` | tidied |  |  |
| `msi_aot_2a_header_File_Version` | `msi_aot_2a_header_file_version` | tidied |  |  |
| `msi_aot_2a_header_Mission` | `msi_aot_2a_header_mission` | tidied |  |  |
| `msi_aot_2a_header_Notes` | `msi_aot_2a_header_notes` | tidied |  |  |
| `msi_aot_2a_header_Source_Creation_Date` | `msi_aot_2a_header_source_creation_date` | tidied |  |  |
| `msi_aot_2a_header_Source_Creator` | `msi_aot_2a_header_source_creator` | tidied |  |  |
| `msi_aot_2a_header_Source_Creator_Version` | `msi_aot_2a_header_source_creator_version` | tidied |  |  |
| `msi_aot_2a_header_Source_System` | `msi_aot_2a_header_source_system` | tidied |  |  |
| `msi_aot_2a_header_Validity_Period_Validity_Start` | `msi_aot_2a_header_validity_period_validity_start` | tidied |  |  |
| `msi_aot_2a_header_Validity_Period_Validity_Stop` | `msi_aot_2a_header_validity_period_validity_stop` | tidied |  |  |
| `msi_aot_2a_header_acquisitionStation` | `msi_aot_2a_header_acquisition_station` | tidied |  |  |
| `msi_aot_2a_header_degradedProductQualityFlag` | `msi_aot_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `msi_aot_2a_header_executableMajorVersion` | `msi_aot_2a_header_executable_major_version` | tidied |  |  |
| `msi_aot_2a_header_executableMinorVersion` | `msi_aot_2a_header_executable_minor_version` | tidied |  |  |
| `msi_aot_2a_header_fileCategory` | `msi_aot_2a_header_file_category` | tidied |  |  |
| `msi_aot_2a_header_fileClass` | `msi_aot_2a_header_fileClass` | clash |  |  |
| `msi_aot_2a_header_formatMajorVersion` | `msi_aot_2a_header_format_major_version` | tidied |  |  |
| `msi_aot_2a_header_formatMinorVersion` | `msi_aot_2a_header_format_minor_version` | tidied |  |  |
| `msi_aot_2a_header_frameID` | `msi_aot_2a_header_frame_id` | tidied |  |  |
| `msi_aot_2a_header_frameStartCoordinates_geographicLatitude` | `msi_aot_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `msi_aot_2a_header_frameStartCoordinates_geographicLongitude` | `msi_aot_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `msi_aot_2a_header_frameStartMargin` | `msi_aot_2a_header_frame_start_margin` | tidied |  |  |
| `msi_aot_2a_header_frameStartTime` | `msi_aot_2a_header_frame_start_time` | tidied |  |  |
| `msi_aot_2a_header_frameStopCoordinates_geographicLatitude` | `msi_aot_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `msi_aot_2a_header_frameStopCoordinates_geographicLongitude` | `msi_aot_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `msi_aot_2a_header_frameStopMargin` | `msi_aot_2a_header_frame_stop_margin` | tidied |  |  |
| `msi_aot_2a_header_frameStopTime` | `msi_aot_2a_header_frame_stop_time` | tidied |  |  |
| `msi_aot_2a_header_meanAnomaly` | `msi_aot_2a_header_mean_anomaly` | tidied |  |  |
| `msi_aot_2a_header_missionID` | `msi_aot_2a_header_mission_id` | tidied |  |  |
| `msi_aot_2a_header_orbitEccentricity` | `msi_aot_2a_header_orbit_eccentricity` | tidied |  |  |
| `msi_aot_2a_header_orbitInclination` | `msi_aot_2a_header_orbit_inclination` | tidied |  |  |
| `msi_aot_2a_header_orbitNumber` | `msi_aot_2a_header_orbit_number` | tidied |  |  |
| `msi_aot_2a_header_orbitSemiMajorAxis` | `msi_aot_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `msi_aot_2a_header_originalProductName` | `msi_aot_2a_header_original_product_name` | tidied |  |  |
| `msi_aot_2a_header_perigeeArgument` | `msi_aot_2a_header_perigee_argument` | tidied |  |  |
| `msi_aot_2a_header_processingCentre` | `msi_aot_2a_header_processing_centre` | tidied |  |  |
| `msi_aot_2a_header_processingStartTime` | `msi_aot_2a_header_processing_start_time` | tidied |  |  |
| `msi_aot_2a_header_processingStopTime` | `msi_aot_2a_header_processing_stop_time` | tidied |  |  |
| `msi_aot_2a_header_processorMajorVersion` | `msi_aot_2a_header_processor_major_version` | tidied |  |  |
| `msi_aot_2a_header_processorMinorVersion` | `msi_aot_2a_header_processor_minor_version` | tidied |  |  |
| `msi_aot_2a_header_processorName` | `msi_aot_2a_header_processor_name` | tidied |  |  |
| `msi_aot_2a_header_productLevel` | `msi_aot_2a_header_product_level` | tidied |  |  |
| `msi_aot_2a_header_productName` | `msi_aot_2a_header_product_name` | tidied |  |  |
| `msi_aot_2a_header_productType` | `msi_aot_2a_header_product_type` | tidied |  |  |
| `msi_aot_2a_header_rightAscension` | `msi_aot_2a_header_right_ascension` | tidied |  |  |
| `msi_aot_2a_header_sensingStartTime` | `msi_aot_2a_header_sensing_start_time` | tidied |  |  |
| `msi_aot_2a_header_sensingStopTime` | `msi_aot_2a_header_sensing_stop_time` | tidied |  |  |
| `msi_aot_2a_header_stateVectorSource` | `msi_aot_2a_header_state_vector_source` | tidied |  |  |
| `msi_aot_2a_header_stateVectorTime` | `msi_aot_2a_header_state_vector_time` | tidied |  |  |
| `msi_aot_2a_header_subsettedProduct` | `msi_aot_2a_header_subsetted_product` | tidied |  |  |
| `msi_aot_2a_header_xPosition` | `msi_aot_2a_header_x_position` | tidied |  |  |
| `msi_aot_2a_header_xVelocity` | `msi_aot_2a_header_x_velocity` | tidied |  |  |
| `msi_aot_2a_header_yPosition` | `msi_aot_2a_header_y_position` | tidied |  |  |
| `msi_aot_2a_header_yVelocity` | `msi_aot_2a_header_y_velocity` | tidied |  |  |
| `msi_aot_2a_header_zPosition` | `msi_aot_2a_header_z_position` | tidied |  |  |
| `msi_aot_2a_header_zVelocity` | `msi_aot_2a_header_z_velocity` | tidied |  |  |
| `msi_cm_2a_cloud_mask_quality_status` | `msi_cm_2a_quality_status_for_cloud_mask` | long_name | Quality status for cloud mask |  |
| `msi_cm_2a_cloud_phase_quality_status` | `msi_cm_2a_quality_status_for_cloud_phase` | long_name | Quality status for cloud phase |  |
| `msi_cm_2a_cloud_type_quality_status` | `msi_cm_2a_quality_status_for_cloud_type` | long_name | Quality status for cloud type |  |
| `msi_cm_2a_geoid_offset` | `msi_cm_2a_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `msi_cm_2a_header_ANXLongitude` | `msi_cm_2a_header_anx_longitude` | tidied |  |  |
| `msi_cm_2a_header_ANXTime` | `msi_cm_2a_header_anx_time` | tidied |  |  |
| `msi_cm_2a_header_File_Class` | `msi_cm_2a_header_File_Class` | clash |  |  |
| `msi_cm_2a_header_File_Description` | `msi_cm_2a_header_file_description` | tidied |  |  |
| `msi_cm_2a_header_File_Name` | `msi_cm_2a_header_file_name` | tidied |  |  |
| `msi_cm_2a_header_File_Type` | `msi_cm_2a_header_file_type` | tidied |  |  |
| `msi_cm_2a_header_File_Version` | `msi_cm_2a_header_file_version` | tidied |  |  |
| `msi_cm_2a_header_Mission` | `msi_cm_2a_header_mission` | tidied |  |  |
| `msi_cm_2a_header_Notes` | `msi_cm_2a_header_notes` | tidied |  |  |
| `msi_cm_2a_header_Source_Creation_Date` | `msi_cm_2a_header_source_creation_date` | tidied |  |  |
| `msi_cm_2a_header_Source_Creator` | `msi_cm_2a_header_source_creator` | tidied |  |  |
| `msi_cm_2a_header_Source_Creator_Version` | `msi_cm_2a_header_source_creator_version` | tidied |  |  |
| `msi_cm_2a_header_Source_System` | `msi_cm_2a_header_source_system` | tidied |  |  |
| `msi_cm_2a_header_Validity_Period_Validity_Start` | `msi_cm_2a_header_validity_period_validity_start` | tidied |  |  |
| `msi_cm_2a_header_Validity_Period_Validity_Stop` | `msi_cm_2a_header_validity_period_validity_stop` | tidied |  |  |
| `msi_cm_2a_header_acquisitionStation` | `msi_cm_2a_header_acquisition_station` | tidied |  |  |
| `msi_cm_2a_header_degradedProductQualityFlag` | `msi_cm_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `msi_cm_2a_header_executableMajorVersion` | `msi_cm_2a_header_executable_major_version` | tidied |  |  |
| `msi_cm_2a_header_executableMinorVersion` | `msi_cm_2a_header_executable_minor_version` | tidied |  |  |
| `msi_cm_2a_header_fileCategory` | `msi_cm_2a_header_file_category` | tidied |  |  |
| `msi_cm_2a_header_fileClass` | `msi_cm_2a_header_fileClass` | clash |  |  |
| `msi_cm_2a_header_formatMajorVersion` | `msi_cm_2a_header_format_major_version` | tidied |  |  |
| `msi_cm_2a_header_formatMinorVersion` | `msi_cm_2a_header_format_minor_version` | tidied |  |  |
| `msi_cm_2a_header_frameID` | `msi_cm_2a_header_frame_id` | tidied |  |  |
| `msi_cm_2a_header_frameStartCoordinates_geographicLatitude` | `msi_cm_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `msi_cm_2a_header_frameStartCoordinates_geographicLongitude` | `msi_cm_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `msi_cm_2a_header_frameStartMargin` | `msi_cm_2a_header_frame_start_margin` | tidied |  |  |
| `msi_cm_2a_header_frameStartTime` | `msi_cm_2a_header_frame_start_time` | tidied |  |  |
| `msi_cm_2a_header_frameStopCoordinates_geographicLatitude` | `msi_cm_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `msi_cm_2a_header_frameStopCoordinates_geographicLongitude` | `msi_cm_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `msi_cm_2a_header_frameStopMargin` | `msi_cm_2a_header_frame_stop_margin` | tidied |  |  |
| `msi_cm_2a_header_frameStopTime` | `msi_cm_2a_header_frame_stop_time` | tidied |  |  |
| `msi_cm_2a_header_meanAnomaly` | `msi_cm_2a_header_mean_anomaly` | tidied |  |  |
| `msi_cm_2a_header_missionID` | `msi_cm_2a_header_mission_id` | tidied |  |  |
| `msi_cm_2a_header_orbitEccentricity` | `msi_cm_2a_header_orbit_eccentricity` | tidied |  |  |
| `msi_cm_2a_header_orbitInclination` | `msi_cm_2a_header_orbit_inclination` | tidied |  |  |
| `msi_cm_2a_header_orbitNumber` | `msi_cm_2a_header_orbit_number` | tidied |  |  |
| `msi_cm_2a_header_orbitSemiMajorAxis` | `msi_cm_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `msi_cm_2a_header_originalProductName` | `msi_cm_2a_header_original_product_name` | tidied |  |  |
| `msi_cm_2a_header_perigeeArgument` | `msi_cm_2a_header_perigee_argument` | tidied |  |  |
| `msi_cm_2a_header_processingCentre` | `msi_cm_2a_header_processing_centre` | tidied |  |  |
| `msi_cm_2a_header_processingStartTime` | `msi_cm_2a_header_processing_start_time` | tidied |  |  |
| `msi_cm_2a_header_processingStopTime` | `msi_cm_2a_header_processing_stop_time` | tidied |  |  |
| `msi_cm_2a_header_processorMajorVersion` | `msi_cm_2a_header_processor_major_version` | tidied |  |  |
| `msi_cm_2a_header_processorMinorVersion` | `msi_cm_2a_header_processor_minor_version` | tidied |  |  |
| `msi_cm_2a_header_processorName` | `msi_cm_2a_header_processor_name` | tidied |  |  |
| `msi_cm_2a_header_productLevel` | `msi_cm_2a_header_product_level` | tidied |  |  |
| `msi_cm_2a_header_productName` | `msi_cm_2a_header_product_name` | tidied |  |  |
| `msi_cm_2a_header_productType` | `msi_cm_2a_header_product_type` | tidied |  |  |
| `msi_cm_2a_header_rightAscension` | `msi_cm_2a_header_right_ascension` | tidied |  |  |
| `msi_cm_2a_header_sensingStartTime` | `msi_cm_2a_header_sensing_start_time` | tidied |  |  |
| `msi_cm_2a_header_sensingStopTime` | `msi_cm_2a_header_sensing_stop_time` | tidied |  |  |
| `msi_cm_2a_header_stateVectorSource` | `msi_cm_2a_header_state_vector_source` | tidied |  |  |
| `msi_cm_2a_header_stateVectorTime` | `msi_cm_2a_header_state_vector_time` | tidied |  |  |
| `msi_cm_2a_header_subsettedProduct` | `msi_cm_2a_header_subsetted_product` | tidied |  |  |
| `msi_cm_2a_header_xPosition` | `msi_cm_2a_header_x_position` | tidied |  |  |
| `msi_cm_2a_header_xVelocity` | `msi_cm_2a_header_x_velocity` | tidied |  |  |
| `msi_cm_2a_header_yPosition` | `msi_cm_2a_header_y_position` | tidied |  |  |
| `msi_cm_2a_header_yVelocity` | `msi_cm_2a_header_y_velocity` | tidied |  |  |
| `msi_cm_2a_header_zPosition` | `msi_cm_2a_header_z_position` | tidied |  |  |
| `msi_cm_2a_header_zVelocity` | `msi_cm_2a_header_z_velocity` | tidied |  |  |
| `msi_cop_2a_cloud_effective_radius_error` | `msi_cop_2a_error_of_effective_radius` | long_name | Error of effective radius | m |
| `msi_cop_2a_cloud_optical_thickness_error` | `msi_cop_2a_error_of_cloud_optical_thickness` | long_name | Error of cloud optical thickness | 1 |
| `msi_cop_2a_cloud_top_height_error` | `msi_cop_2a_error_of_cloud_top_height` | long_name | Error of cloud top height | m |
| `msi_cop_2a_cloud_top_pressure_error` | `msi_cop_2a_error_of_cloud_top_pressure` | long_name | Error of cloud top pressure | Pa |
| `msi_cop_2a_cloud_top_temperature_error` | `msi_cop_2a_error_of_cloud_top_temperature` | long_name | Error of cloud top temperature | K |
| `msi_cop_2a_cloud_water_path_error` | `msi_cop_2a_error_of_cloud_water_path` | long_name | Error of cloud water path | kg m-2 |
| `msi_cop_2a_geoid_offset` | `msi_cop_2a_height_of_the_geoid_over_the_ellipsoid` | long_name | Height of the geoid over the ellipsoid | m |
| `msi_cop_2a_header_ANXLongitude` | `msi_cop_2a_header_anx_longitude` | tidied |  |  |
| `msi_cop_2a_header_ANXTime` | `msi_cop_2a_header_anx_time` | tidied |  |  |
| `msi_cop_2a_header_File_Class` | `msi_cop_2a_header_File_Class` | clash |  |  |
| `msi_cop_2a_header_File_Description` | `msi_cop_2a_header_file_description` | tidied |  |  |
| `msi_cop_2a_header_File_Name` | `msi_cop_2a_header_file_name` | tidied |  |  |
| `msi_cop_2a_header_File_Type` | `msi_cop_2a_header_file_type` | tidied |  |  |
| `msi_cop_2a_header_File_Version` | `msi_cop_2a_header_file_version` | tidied |  |  |
| `msi_cop_2a_header_Mission` | `msi_cop_2a_header_mission` | tidied |  |  |
| `msi_cop_2a_header_Notes` | `msi_cop_2a_header_notes` | tidied |  |  |
| `msi_cop_2a_header_Source_Creation_Date` | `msi_cop_2a_header_source_creation_date` | tidied |  |  |
| `msi_cop_2a_header_Source_Creator` | `msi_cop_2a_header_source_creator` | tidied |  |  |
| `msi_cop_2a_header_Source_Creator_Version` | `msi_cop_2a_header_source_creator_version` | tidied |  |  |
| `msi_cop_2a_header_Source_System` | `msi_cop_2a_header_source_system` | tidied |  |  |
| `msi_cop_2a_header_Validity_Period_Validity_Start` | `msi_cop_2a_header_validity_period_validity_start` | tidied |  |  |
| `msi_cop_2a_header_Validity_Period_Validity_Stop` | `msi_cop_2a_header_validity_period_validity_stop` | tidied |  |  |
| `msi_cop_2a_header_acquisitionStation` | `msi_cop_2a_header_acquisition_station` | tidied |  |  |
| `msi_cop_2a_header_degradedProductQualityFlag` | `msi_cop_2a_header_degraded_product_quality_flag` | tidied |  |  |
| `msi_cop_2a_header_executableMajorVersion` | `msi_cop_2a_header_executable_major_version` | tidied |  |  |
| `msi_cop_2a_header_executableMinorVersion` | `msi_cop_2a_header_executable_minor_version` | tidied |  |  |
| `msi_cop_2a_header_fileCategory` | `msi_cop_2a_header_file_category` | tidied |  |  |
| `msi_cop_2a_header_fileClass` | `msi_cop_2a_header_fileClass` | clash |  |  |
| `msi_cop_2a_header_formatMajorVersion` | `msi_cop_2a_header_format_major_version` | tidied |  |  |
| `msi_cop_2a_header_formatMinorVersion` | `msi_cop_2a_header_format_minor_version` | tidied |  |  |
| `msi_cop_2a_header_frameID` | `msi_cop_2a_header_frame_id` | tidied |  |  |
| `msi_cop_2a_header_frameStartCoordinates_geographicLatitude` | `msi_cop_2a_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `msi_cop_2a_header_frameStartCoordinates_geographicLongitude` | `msi_cop_2a_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `msi_cop_2a_header_frameStartMargin` | `msi_cop_2a_header_frame_start_margin` | tidied |  |  |
| `msi_cop_2a_header_frameStartTime` | `msi_cop_2a_header_frame_start_time` | tidied |  |  |
| `msi_cop_2a_header_frameStopCoordinates_geographicLatitude` | `msi_cop_2a_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `msi_cop_2a_header_frameStopCoordinates_geographicLongitude` | `msi_cop_2a_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `msi_cop_2a_header_frameStopMargin` | `msi_cop_2a_header_frame_stop_margin` | tidied |  |  |
| `msi_cop_2a_header_frameStopTime` | `msi_cop_2a_header_frame_stop_time` | tidied |  |  |
| `msi_cop_2a_header_meanAnomaly` | `msi_cop_2a_header_mean_anomaly` | tidied |  |  |
| `msi_cop_2a_header_missionID` | `msi_cop_2a_header_mission_id` | tidied |  |  |
| `msi_cop_2a_header_orbitEccentricity` | `msi_cop_2a_header_orbit_eccentricity` | tidied |  |  |
| `msi_cop_2a_header_orbitInclination` | `msi_cop_2a_header_orbit_inclination` | tidied |  |  |
| `msi_cop_2a_header_orbitNumber` | `msi_cop_2a_header_orbit_number` | tidied |  |  |
| `msi_cop_2a_header_orbitSemiMajorAxis` | `msi_cop_2a_header_orbit_semi_major_axis` | tidied |  |  |
| `msi_cop_2a_header_originalProductName` | `msi_cop_2a_header_original_product_name` | tidied |  |  |
| `msi_cop_2a_header_perigeeArgument` | `msi_cop_2a_header_perigee_argument` | tidied |  |  |
| `msi_cop_2a_header_processingCentre` | `msi_cop_2a_header_processing_centre` | tidied |  |  |
| `msi_cop_2a_header_processingStartTime` | `msi_cop_2a_header_processing_start_time` | tidied |  |  |
| `msi_cop_2a_header_processingStopTime` | `msi_cop_2a_header_processing_stop_time` | tidied |  |  |
| `msi_cop_2a_header_processorMajorVersion` | `msi_cop_2a_header_processor_major_version` | tidied |  |  |
| `msi_cop_2a_header_processorMinorVersion` | `msi_cop_2a_header_processor_minor_version` | tidied |  |  |
| `msi_cop_2a_header_processorName` | `msi_cop_2a_header_processor_name` | tidied |  |  |
| `msi_cop_2a_header_productLevel` | `msi_cop_2a_header_product_level` | tidied |  |  |
| `msi_cop_2a_header_productName` | `msi_cop_2a_header_product_name` | tidied |  |  |
| `msi_cop_2a_header_productType` | `msi_cop_2a_header_product_type` | tidied |  |  |
| `msi_cop_2a_header_rightAscension` | `msi_cop_2a_header_right_ascension` | tidied |  |  |
| `msi_cop_2a_header_sensingStartTime` | `msi_cop_2a_header_sensing_start_time` | tidied |  |  |
| `msi_cop_2a_header_sensingStopTime` | `msi_cop_2a_header_sensing_stop_time` | tidied |  |  |
| `msi_cop_2a_header_stateVectorSource` | `msi_cop_2a_header_state_vector_source` | tidied |  |  |
| `msi_cop_2a_header_stateVectorTime` | `msi_cop_2a_header_state_vector_time` | tidied |  |  |
| `msi_cop_2a_header_subsettedProduct` | `msi_cop_2a_header_subsetted_product` | tidied |  |  |
| `msi_cop_2a_header_xPosition` | `msi_cop_2a_header_x_position` | tidied |  |  |
| `msi_cop_2a_header_xVelocity` | `msi_cop_2a_header_x_velocity` | tidied |  |  |
| `msi_cop_2a_header_yPosition` | `msi_cop_2a_header_y_position` | tidied |  |  |
| `msi_cop_2a_header_yVelocity` | `msi_cop_2a_header_y_velocity` | tidied |  |  |
| `msi_cop_2a_header_zPosition` | `msi_cop_2a_header_z_position` | tidied |  |  |
| `msi_cop_2a_header_zVelocity` | `msi_cop_2a_header_z_velocity` | tidied |  |  |
| `msi_nom_1b_TIR_bench_temperature_drift` | `msi_nom_1b_tir_sens_d_t_bench` | long_name | TIR sens dT bench | K |
| `msi_nom_1b_TIR_bench_temperature_drift_high_flag` | `msi_nom_1b_tir_sens_warn_bench` | long_name | TIR sens warn bench |  |
| `msi_nom_1b_TIR_bench_temperature_sensitivity_correction_flag` | `msi_nom_1b_tir_sens_correction_bench` | long_name | TIR sens correction bench |  |
| `msi_nom_1b_TIR_cover_temperature_drift` | `msi_nom_1b_tir_sens_d_t_case` | long_name | TIR sens dT case | K |
| `msi_nom_1b_TIR_cover_temperature_drift_high_flag` | `msi_nom_1b_tir_sens_warn_case` | long_name | TIR sens warn case |  |
| `msi_nom_1b_TIR_cover_temperature_sensitivity_correction_flag` | `msi_nom_1b_tir_sens_correction_case` | long_name | TIR sens correction case |  |
| `msi_nom_1b_TIR_detector_bias_voltage_VFID_drift` | `msi_nom_1b_tir_detector_bias_voltage_vfid_drift` | clash | solar elevation VNS | V |
| `msi_nom_1b_TIR_detector_bias_voltage_VFID_drift_high_flag` | `msi_nom_1b_tir_sens_warn_vfid` | long_name | TIR sens warn VFID |  |
| `msi_nom_1b_TIR_detector_bias_voltage_VFID_sensitivity_correction_flag` | `msi_nom_1b_tir_sens_correction_vfid` | long_name | TIR sens correction VFID |  |
| `msi_nom_1b_TIR_detector_bias_voltage_VSKIM_drift` | `msi_nom_1b_tir_sens_d_v_vskim` | long_name | TIR sens dV VSKIM | V |
| `msi_nom_1b_TIR_detector_bias_voltage_VSKIM_drift_high_flag` | `msi_nom_1b_tir_sens_warn_vskim` | long_name | TIR sens warn VSKIM |  |
| `msi_nom_1b_TIR_detector_bias_voltage_VSKIM_sensitivity_correction_flag` | `msi_nom_1b_tir_sens_correction_vskim` | long_name | TIR sens correction VSKIM |  |
| `msi_nom_1b_TIR_detector_temperature_drift` | `msi_nom_1b_tir_sens_d_t_tir_detector` | long_name | TIR sens dT TIR detector | K |
| `msi_nom_1b_TIR_detector_temperature_drift_high_flag` | `msi_nom_1b_tir_sens_warn_detector` | long_name | TIR sens warn detector |  |
| `msi_nom_1b_TIR_detector_temperature_sensitivity_correction_flag` | `msi_nom_1b_tir_sens_correction_detector` | long_name | TIR sens correction detector |  |
| `msi_nom_1b_TIR_relay_lens_temperature_drift` | `msi_nom_1b_tir_relay_lens_temperature_drift` | clash | solar elevation VNS | K |
| `msi_nom_1b_TIR_relay_lens_temperature_drift_high_flag` | `msi_nom_1b_tir_sens_warn_relay` | long_name | TIR sens warn relay |  |
| `msi_nom_1b_TIR_relay_lens_temperature_sensitivity_correction_flag` | `msi_nom_1b_tir_sens_correction_relay` | long_name | TIR sens correction relay |  |
| `msi_nom_1b_VNS_data_memory_error_flag` | `msi_nom_1b_vns_edac` | long_name | VNS EDAC |  |
| `msi_nom_1b_VNS_solar_azimuth_angle` | `msi_nom_1b_solar_azimuth_vns` | long_name | solar azimuth VNS | degree |
| `msi_nom_1b_VNS_solar_elevation_angle` | `msi_nom_1b_vns_solar_elevation_angle` | clash | solar elevation VNS | degree |
| `msi_nom_1b_ancillary_data_memory_error_flag` | `msi_nom_1b_ancillary_data_edac` | long_name | Ancillary data EDAC |  |
| `msi_nom_1b_detector_latchup_flag` | `msi_nom_1b_detector_latchup` | long_name | detector latchup |  |
| `msi_nom_1b_detector_power_status` | `msi_nom_1b_detector_power` | long_name | detector power |  |
| `msi_nom_1b_flat_field_data_memory_error_flag` | `msi_nom_1b_science_data_ff_edac` | long_name | Science data FF_EDAC |  |
| `msi_nom_1b_header_ANXLongitude` | `msi_nom_1b_header_anx_longitude` | tidied |  |  |
| `msi_nom_1b_header_ANXTime` | `msi_nom_1b_header_anx_time` | tidied |  |  |
| `msi_nom_1b_header_File_Class` | `msi_nom_1b_header_File_Class` | clash |  |  |
| `msi_nom_1b_header_File_Description` | `msi_nom_1b_header_file_description` | tidied |  |  |
| `msi_nom_1b_header_File_Name` | `msi_nom_1b_header_file_name` | tidied |  |  |
| `msi_nom_1b_header_File_Type` | `msi_nom_1b_header_file_type` | tidied |  |  |
| `msi_nom_1b_header_File_Version` | `msi_nom_1b_header_file_version` | tidied |  |  |
| `msi_nom_1b_header_Mission` | `msi_nom_1b_header_mission` | tidied |  |  |
| `msi_nom_1b_header_Notes` | `msi_nom_1b_header_notes` | tidied |  |  |
| `msi_nom_1b_header_Source_Creation_Date` | `msi_nom_1b_header_source_creation_date` | tidied |  |  |
| `msi_nom_1b_header_Source_Creator` | `msi_nom_1b_header_source_creator` | tidied |  |  |
| `msi_nom_1b_header_Source_Creator_Version` | `msi_nom_1b_header_source_creator_version` | tidied |  |  |
| `msi_nom_1b_header_Source_System` | `msi_nom_1b_header_source_system` | tidied |  |  |
| `msi_nom_1b_header_Validity_Period_Validity_Start` | `msi_nom_1b_header_validity_period_validity_start` | tidied |  |  |
| `msi_nom_1b_header_Validity_Period_Validity_Stop` | `msi_nom_1b_header_validity_period_validity_stop` | tidied |  |  |
| `msi_nom_1b_header_acquisitionStation` | `msi_nom_1b_header_acquisition_station` | tidied |  |  |
| `msi_nom_1b_header_degradedProductQualityFlag` | `msi_nom_1b_header_degraded_product_quality_flag` | tidied |  |  |
| `msi_nom_1b_header_executableMajorVersion` | `msi_nom_1b_header_executable_major_version` | tidied |  |  |
| `msi_nom_1b_header_executableMinorVersion` | `msi_nom_1b_header_executable_minor_version` | tidied |  |  |
| `msi_nom_1b_header_fileCategory` | `msi_nom_1b_header_file_category` | tidied |  |  |
| `msi_nom_1b_header_fileClass` | `msi_nom_1b_header_fileClass` | clash |  |  |
| `msi_nom_1b_header_formatMajorVersion` | `msi_nom_1b_header_format_major_version` | tidied |  |  |
| `msi_nom_1b_header_formatMinorVersion` | `msi_nom_1b_header_format_minor_version` | tidied |  |  |
| `msi_nom_1b_header_frameID` | `msi_nom_1b_header_frame_id` | tidied |  |  |
| `msi_nom_1b_header_frameStartCoordinates_geographicLatitude` | `msi_nom_1b_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `msi_nom_1b_header_frameStartCoordinates_geographicLongitude` | `msi_nom_1b_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `msi_nom_1b_header_frameStartMargin` | `msi_nom_1b_header_frame_start_margin` | tidied |  |  |
| `msi_nom_1b_header_frameStartTime` | `msi_nom_1b_header_frame_start_time` | tidied |  |  |
| `msi_nom_1b_header_frameStopCoordinates_geographicLatitude` | `msi_nom_1b_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `msi_nom_1b_header_frameStopCoordinates_geographicLongitude` | `msi_nom_1b_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `msi_nom_1b_header_frameStopMargin` | `msi_nom_1b_header_frame_stop_margin` | tidied |  |  |
| `msi_nom_1b_header_frameStopTime` | `msi_nom_1b_header_frame_stop_time` | tidied |  |  |
| `msi_nom_1b_header_meanAnomaly` | `msi_nom_1b_header_mean_anomaly` | tidied |  |  |
| `msi_nom_1b_header_missionID` | `msi_nom_1b_header_mission_id` | tidied |  |  |
| `msi_nom_1b_header_orbitEccentricity` | `msi_nom_1b_header_orbit_eccentricity` | tidied |  |  |
| `msi_nom_1b_header_orbitInclination` | `msi_nom_1b_header_orbit_inclination` | tidied |  |  |
| `msi_nom_1b_header_orbitNumber` | `msi_nom_1b_header_orbit_number` | tidied |  |  |
| `msi_nom_1b_header_orbitSemiMajorAxis` | `msi_nom_1b_header_orbit_semi_major_axis` | tidied |  |  |
| `msi_nom_1b_header_originalProductName` | `msi_nom_1b_header_original_product_name` | tidied |  |  |
| `msi_nom_1b_header_perigeeArgument` | `msi_nom_1b_header_perigee_argument` | tidied |  |  |
| `msi_nom_1b_header_processingCentre` | `msi_nom_1b_header_processing_centre` | tidied |  |  |
| `msi_nom_1b_header_processingStartTime` | `msi_nom_1b_header_processing_start_time` | tidied |  |  |
| `msi_nom_1b_header_processingStopTime` | `msi_nom_1b_header_processing_stop_time` | tidied |  |  |
| `msi_nom_1b_header_processorMajorVersion` | `msi_nom_1b_header_processor_major_version` | tidied |  |  |
| `msi_nom_1b_header_processorMinorVersion` | `msi_nom_1b_header_processor_minor_version` | tidied |  |  |
| `msi_nom_1b_header_processorName` | `msi_nom_1b_header_processor_name` | tidied |  |  |
| `msi_nom_1b_header_productLevel` | `msi_nom_1b_header_product_level` | tidied |  |  |
| `msi_nom_1b_header_productName` | `msi_nom_1b_header_product_name` | tidied |  |  |
| `msi_nom_1b_header_productType` | `msi_nom_1b_header_product_type` | tidied |  |  |
| `msi_nom_1b_header_rightAscension` | `msi_nom_1b_header_right_ascension` | tidied |  |  |
| `msi_nom_1b_header_sensingStartTime` | `msi_nom_1b_header_sensing_start_time` | tidied |  |  |
| `msi_nom_1b_header_sensingStopTime` | `msi_nom_1b_header_sensing_stop_time` | tidied |  |  |
| `msi_nom_1b_header_stateVectorSource` | `msi_nom_1b_header_state_vector_source` | tidied |  |  |
| `msi_nom_1b_header_stateVectorTime` | `msi_nom_1b_header_state_vector_time` | tidied |  |  |
| `msi_nom_1b_header_subsettedProduct` | `msi_nom_1b_header_subsetted_product` | tidied |  |  |
| `msi_nom_1b_header_xPosition` | `msi_nom_1b_header_x_position` | tidied |  |  |
| `msi_nom_1b_header_xVelocity` | `msi_nom_1b_header_x_velocity` | tidied |  |  |
| `msi_nom_1b_header_yPosition` | `msi_nom_1b_header_y_position` | tidied |  |  |
| `msi_nom_1b_header_yVelocity` | `msi_nom_1b_header_y_velocity` | tidied |  |  |
| `msi_nom_1b_header_zPosition` | `msi_nom_1b_header_z_position` | tidied |  |  |
| `msi_nom_1b_header_zVelocity` | `msi_nom_1b_header_z_velocity` | tidied |  |  |
| `msi_nom_1b_housekeeping_data_within_limits_status` | `msi_nom_1b_fdir_flags` | long_name | FDIR flags |  |
| `msi_nom_1b_instrument_control_unit_analog_to_digital_converter_latchup_flag` | `msi_nom_1b_icu_adc_latchup` | long_name | ICU ADC latchup |  |
| `msi_nom_1b_line_quality_status` | `msi_nom_1b_line_quality` | long_name | Line quality |  |
| `msi_nom_1b_pixel_quality_status` | `msi_nom_1b_pixel_quality` | long_name | Pixel quality |  |
| `msi_nom_1b_pixel_values` | `msi_nom_1b_radiance_and_brightness_temperature` | long_name | Radiance (VNS bands) and brightness temperature (TIR bands) | W m-2 sr-1 um-1 (VNS bands) K (TIR bands) |
| `msi_nom_1b_science_data_memory_error_flag` | `msi_nom_1b_science_data_edac` | long_name | Science data EDAC |  |
| `msi_nom_1b_science_packet_transmission_status` | `msi_nom_1b_isp_transmission_status` | long_name | ISP transmission status |  |
| `msi_nom_1b_size_VNS_band` | `msi_nom_1b_size_vns_band` | tidied |  |  |
| `msi_nom_1b_state_vector_quality_status` | `msi_nom_1b_state_vector_quality` | long_name | State vector quality |  |
| `msi_nom_1b_sunglint_flag` | `msi_nom_1b_sun_glint` | long_name | Sun glint |  |
| `msi_nom_1b_sunglint_reflectance` | `msi_nom_1b_sun_glint_reflectance` | long_name | Sun glint reflectance | 1 |
| `msi_nom_1b_time_synchronisation_status` | `msi_nom_1b_synchronisation_status` | long_name | Time synchronisation status |  |
| `msi_rgr_1c_header_ANXLongitude` | `msi_rgr_1c_header_anx_longitude` | tidied |  |  |
| `msi_rgr_1c_header_ANXTime` | `msi_rgr_1c_header_anx_time` | tidied |  |  |
| `msi_rgr_1c_header_File_Class` | `msi_rgr_1c_header_File_Class` | clash |  |  |
| `msi_rgr_1c_header_File_Description` | `msi_rgr_1c_header_file_description` | tidied |  |  |
| `msi_rgr_1c_header_File_Name` | `msi_rgr_1c_header_file_name` | tidied |  |  |
| `msi_rgr_1c_header_File_Type` | `msi_rgr_1c_header_file_type` | tidied |  |  |
| `msi_rgr_1c_header_File_Version` | `msi_rgr_1c_header_file_version` | tidied |  |  |
| `msi_rgr_1c_header_Mission` | `msi_rgr_1c_header_mission` | tidied |  |  |
| `msi_rgr_1c_header_Notes` | `msi_rgr_1c_header_notes` | tidied |  |  |
| `msi_rgr_1c_header_Source_Creation_Date` | `msi_rgr_1c_header_source_creation_date` | tidied |  |  |
| `msi_rgr_1c_header_Source_Creator` | `msi_rgr_1c_header_source_creator` | tidied |  |  |
| `msi_rgr_1c_header_Source_Creator_Version` | `msi_rgr_1c_header_source_creator_version` | tidied |  |  |
| `msi_rgr_1c_header_Source_System` | `msi_rgr_1c_header_source_system` | tidied |  |  |
| `msi_rgr_1c_header_Validity_Period_Validity_Start` | `msi_rgr_1c_header_validity_period_validity_start` | tidied |  |  |
| `msi_rgr_1c_header_Validity_Period_Validity_Stop` | `msi_rgr_1c_header_validity_period_validity_stop` | tidied |  |  |
| `msi_rgr_1c_header_acquisitionStation` | `msi_rgr_1c_header_acquisition_station` | tidied |  |  |
| `msi_rgr_1c_header_degradedProductQualityFlag` | `msi_rgr_1c_header_degraded_product_quality_flag` | tidied |  |  |
| `msi_rgr_1c_header_executableMajorVersion` | `msi_rgr_1c_header_executable_major_version` | tidied |  |  |
| `msi_rgr_1c_header_executableMinorVersion` | `msi_rgr_1c_header_executable_minor_version` | tidied |  |  |
| `msi_rgr_1c_header_fileCategory` | `msi_rgr_1c_header_file_category` | tidied |  |  |
| `msi_rgr_1c_header_fileClass` | `msi_rgr_1c_header_fileClass` | clash |  |  |
| `msi_rgr_1c_header_formatMajorVersion` | `msi_rgr_1c_header_format_major_version` | tidied |  |  |
| `msi_rgr_1c_header_formatMinorVersion` | `msi_rgr_1c_header_format_minor_version` | tidied |  |  |
| `msi_rgr_1c_header_frameID` | `msi_rgr_1c_header_frame_id` | tidied |  |  |
| `msi_rgr_1c_header_frameStartCoordinates_geographicLatitude` | `msi_rgr_1c_header_frame_start_coordinates_geographic_latitude` | tidied |  |  |
| `msi_rgr_1c_header_frameStartCoordinates_geographicLongitude` | `msi_rgr_1c_header_frame_start_coordinates_geographic_longitude` | tidied |  |  |
| `msi_rgr_1c_header_frameStartMargin` | `msi_rgr_1c_header_frame_start_margin` | tidied |  |  |
| `msi_rgr_1c_header_frameStartTime` | `msi_rgr_1c_header_frame_start_time` | tidied |  |  |
| `msi_rgr_1c_header_frameStopCoordinates_geographicLatitude` | `msi_rgr_1c_header_frame_stop_coordinates_geographic_latitude` | tidied |  |  |
| `msi_rgr_1c_header_frameStopCoordinates_geographicLongitude` | `msi_rgr_1c_header_frame_stop_coordinates_geographic_longitude` | tidied |  |  |
| `msi_rgr_1c_header_frameStopMargin` | `msi_rgr_1c_header_frame_stop_margin` | tidied |  |  |
| `msi_rgr_1c_header_frameStopTime` | `msi_rgr_1c_header_frame_stop_time` | tidied |  |  |
| `msi_rgr_1c_header_meanAnomaly` | `msi_rgr_1c_header_mean_anomaly` | tidied |  |  |
| `msi_rgr_1c_header_missionID` | `msi_rgr_1c_header_mission_id` | tidied |  |  |
| `msi_rgr_1c_header_orbitEccentricity` | `msi_rgr_1c_header_orbit_eccentricity` | tidied |  |  |
| `msi_rgr_1c_header_orbitInclination` | `msi_rgr_1c_header_orbit_inclination` | tidied |  |  |
| `msi_rgr_1c_header_orbitNumber` | `msi_rgr_1c_header_orbit_number` | tidied |  |  |
| `msi_rgr_1c_header_orbitSemiMajorAxis` | `msi_rgr_1c_header_orbit_semi_major_axis` | tidied |  |  |
| `msi_rgr_1c_header_originalProductName` | `msi_rgr_1c_header_original_product_name` | tidied |  |  |
| `msi_rgr_1c_header_perigeeArgument` | `msi_rgr_1c_header_perigee_argument` | tidied |  |  |
| `msi_rgr_1c_header_processingCentre` | `msi_rgr_1c_header_processing_centre` | tidied |  |  |
| `msi_rgr_1c_header_processingStartTime` | `msi_rgr_1c_header_processing_start_time` | tidied |  |  |
| `msi_rgr_1c_header_processingStopTime` | `msi_rgr_1c_header_processing_stop_time` | tidied |  |  |
| `msi_rgr_1c_header_processorMajorVersion` | `msi_rgr_1c_header_processor_major_version` | tidied |  |  |
| `msi_rgr_1c_header_processorMinorVersion` | `msi_rgr_1c_header_processor_minor_version` | tidied |  |  |
| `msi_rgr_1c_header_processorName` | `msi_rgr_1c_header_processor_name` | tidied |  |  |
| `msi_rgr_1c_header_productLevel` | `msi_rgr_1c_header_product_level` | tidied |  |  |
| `msi_rgr_1c_header_productName` | `msi_rgr_1c_header_product_name` | tidied |  |  |
| `msi_rgr_1c_header_productType` | `msi_rgr_1c_header_product_type` | tidied |  |  |
| `msi_rgr_1c_header_rightAscension` | `msi_rgr_1c_header_right_ascension` | tidied |  |  |
| `msi_rgr_1c_header_sensingStartTime` | `msi_rgr_1c_header_sensing_start_time` | tidied |  |  |
| `msi_rgr_1c_header_sensingStopTime` | `msi_rgr_1c_header_sensing_stop_time` | tidied |  |  |
| `msi_rgr_1c_header_stateVectorSource` | `msi_rgr_1c_header_state_vector_source` | tidied |  |  |
| `msi_rgr_1c_header_stateVectorTime` | `msi_rgr_1c_header_state_vector_time` | tidied |  |  |
| `msi_rgr_1c_header_subsettedProduct` | `msi_rgr_1c_header_subsetted_product` | tidied |  |  |
| `msi_rgr_1c_header_xPosition` | `msi_rgr_1c_header_x_position` | tidied |  |  |
| `msi_rgr_1c_header_xVelocity` | `msi_rgr_1c_header_x_velocity` | tidied |  |  |
| `msi_rgr_1c_header_yPosition` | `msi_rgr_1c_header_y_position` | tidied |  |  |
| `msi_rgr_1c_header_yVelocity` | `msi_rgr_1c_header_y_velocity` | tidied |  |  |
| `msi_rgr_1c_header_zPosition` | `msi_rgr_1c_header_z_position` | tidied |  |  |
| `msi_rgr_1c_header_zVelocity` | `msi_rgr_1c_header_z_velocity` | tidied |  |  |
| `msi_rgr_1c_line_quality_status` | `msi_rgr_1c_line_quality` | long_name | Line quality |  |
| `msi_rgr_1c_pixel_quality_status` | `msi_rgr_1c_pixel_quality` | long_name | Pixel quality |  |
| `msi_rgr_1c_pixel_values` | `msi_rgr_1c_radiance_and_brightness_temperature` | long_name | Radiance (VNS bands) and brightness temperature (TIR bands) | W m-2 sr-1 um-1 (VNS bands) K (TIR bands) |
| `msi_rgr_1c_size_VNS_band` | `msi_rgr_1c_size_vns_band` | tidied |  |  |
| `msi_rgr_1c_state_vector_quality_status` | `msi_rgr_1c_state_vector_quality` | long_name | State vector quality |  |
| `msi_rgr_1c_sunglint_flag` | `msi_rgr_1c_sun_glint` | long_name | Sun glint |  |
| `msi_rgr_1c_sunglint_reflectance` | `msi_rgr_1c_sun_glint_reflectance` | long_name | Sun glint reflectance | 1 |
| `msi_rgr_1c_time_synchronisation_status` | `msi_rgr_1c_synchronisation_status` | long_name | Time synchronisation status |  |

## bkr/polar/jpss_atms (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `1` | `channel_01_brightness_temperature` | channel |  | K |
| `10` | `channel_10_brightness_temperature` | channel |  | K |
| `11` | `channel_11_brightness_temperature` | channel |  | K |
| `12` | `channel_12_brightness_temperature` | channel |  | K |
| `13` | `channel_13_brightness_temperature` | channel |  | K |
| `14` | `channel_14_brightness_temperature` | channel |  | K |
| `15` | `channel_15_brightness_temperature` | channel |  | K |
| `16` | `channel_16_brightness_temperature` | channel |  | K |
| `17` | `channel_17_brightness_temperature` | channel |  | K |
| `18` | `channel_18_brightness_temperature` | channel |  | K |
| `19` | `channel_19_brightness_temperature` | channel |  | K |
| `2` | `channel_02_brightness_temperature` | channel |  | K |
| `20` | `channel_20_brightness_temperature` | channel |  | K |
| `21` | `channel_21_brightness_temperature` | channel |  | K |
| `22` | `channel_22_brightness_temperature` | channel |  | K |
| `3` | `channel_03_brightness_temperature` | channel |  | K |
| `4` | `channel_04_brightness_temperature` | channel |  | K |
| `5` | `channel_05_brightness_temperature` | channel |  | K |
| `6` | `channel_06_brightness_temperature` | channel |  | K |
| `7` | `channel_07_brightness_temperature` | channel |  | K |
| `8` | `channel_08_brightness_temperature` | channel |  | K |
| `9` | `channel_09_brightness_temperature` | channel |  | K |

## bkr/polar/jpss_atms_sfr_n20 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `AT` | `brightness_temperature` | long_name | Brightness temperature | Kelvin |
| `BeamTime` | `seconds_since_1958_01_01_00_00_00_utc` | long_name | seconds since 1958/01/01 00:00:00 UTC |  |
| `De` | `effective_diameter` | long_name | Effective diameter | mm |
| `FLAGS` | `flags_of_retrieval_bit0_non_covergence_bit0_too_small` | long_name | Flags of retrieval: bit0-non_covergence, bit0-too_small | 1 |
| `IWP` | `ice_water_path` | long_name | Ice water path | g/kg |
| `LZ_angle` | `local_zenith_angle` | long_name | Local zenith angle | degrees |
| `Orbit_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `PROB` | `snow_probability` | long_name | Snow probability | 1 |
| `RAzi_angle` | `relative_azimuth_angle` | long_name | Relative azimuth angle | degrees |
| `SA_angle` | `satellite_azimuth_angle` | long_name | Satellite azimuth angle | degrees |
| `SD` | `snow_detection_0_no_snow_1_snow` | long_name | Snow Detection: 0-no_snow, 1-snow | 1 |
| `SFR` | `snow_water_equivalent_instantaneous_snow_rate` | long_name | Snow water equivalent instantaneous snow rate | mm/hr |
| `SZ_angle` | `solar_zenith_angle` | long_name | Solar zenith angle | degrees |
| `Sat_height` | `satellite_height` | long_name | Satellite height | km |
| `Sat_range` | `satellite_range` | long_name | Satellite range | km |
| `ScanTime_dom` | `calendar_day` | long_name | Calendar day |  |
| `ScanTime_hour` | `hour_of_the_day` | long_name | hour of the day |  |
| `ScanTime_minute` | `minute_of_the_hour` | long_name | minute of the hour |  |
| `ScanTime_month` | `calendar_month` | long_name | Calendar month | months |
| `ScanTime_second` | `second_of_the_minute` | long_name | second of the minute |  |
| `ScanTime_year` | `calendar_year` | long_name | Calendar Year | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_land_2_coast` | long_name | Type of Surface: 0-ocean, 1-land, 2-coast | 1 |
| `dqf` | `data_qulity_flag_0_good_1_bad_2_undetermined` | long_name | Data qulity flag: 0-good, 1-bad, 2-undetermined | 1 |

## bkr/polar/jpss_atms_sfr_n21 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `AT` | `brightness_temperature` | long_name | Brightness temperature | Kelvin |
| `BeamTime` | `seconds_since_1958_01_01_00_00_00_utc` | long_name | seconds since 1958/01/01 00:00:00 UTC |  |
| `De` | `effective_diameter` | long_name | Effective diameter | mm |
| `FLAGS` | `flags_of_retrieval_bit0_non_covergence_bit0_too_small` | long_name | Flags of retrieval: bit0-non_covergence, bit0-too_small | 1 |
| `IWP` | `ice_water_path` | long_name | Ice water path | g/kg |
| `LZ_angle` | `local_zenith_angle` | long_name | Local zenith angle | degrees |
| `Orbit_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `PROB` | `snow_probability` | long_name | Snow probability | 1 |
| `RAzi_angle` | `relative_azimuth_angle` | long_name | Relative azimuth angle | degrees |
| `SA_angle` | `satellite_azimuth_angle` | long_name | Satellite azimuth angle | degrees |
| `SD` | `snow_detection_0_no_snow_1_snow` | long_name | Snow Detection: 0-no_snow, 1-snow | 1 |
| `SFR` | `snow_water_equivalent_instantaneous_snow_rate` | long_name | Snow water equivalent instantaneous snow rate | mm/hr |
| `SZ_angle` | `solar_zenith_angle` | long_name | Solar zenith angle | degrees |
| `Sat_height` | `satellite_height` | long_name | Satellite height | km |
| `Sat_range` | `satellite_range` | long_name | Satellite range | km |
| `ScanTime_dom` | `calendar_day` | long_name | Calendar day |  |
| `ScanTime_hour` | `hour_of_the_day` | long_name | hour of the day |  |
| `ScanTime_minute` | `minute_of_the_hour` | long_name | minute of the hour |  |
| `ScanTime_month` | `calendar_month` | long_name | Calendar month | months |
| `ScanTime_second` | `second_of_the_minute` | long_name | second of the minute |  |
| `ScanTime_year` | `calendar_year` | long_name | Calendar Year | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_land_2_coast` | long_name | Type of Surface: 0-ocean, 1-land, 2-coast | 1 |
| `dqf` | `data_qulity_flag_0_good_1_bad_2_undetermined` | long_name | Data qulity flag: 0-good, 1-bad, 2-undetermined | 1 |

## bkr/polar/jpss_atms_sfr_snpp (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `AT` | `brightness_temperature` | long_name | Brightness temperature | Kelvin |
| `BeamTime` | `seconds_since_1958_01_01_00_00_00_utc` | long_name | seconds since 1958/01/01 00:00:00 UTC |  |
| `De` | `effective_diameter` | long_name | Effective diameter | mm |
| `FLAGS` | `flags_of_retrieval_bit0_non_covergence_bit0_too_small` | long_name | Flags of retrieval: bit0-non_covergence, bit0-too_small | 1 |
| `IWP` | `ice_water_path` | long_name | Ice water path | g/kg |
| `LZ_angle` | `local_zenith_angle` | long_name | Local zenith angle | degrees |
| `Orbit_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `PROB` | `snow_probability` | long_name | Snow probability | 1 |
| `RAzi_angle` | `relative_azimuth_angle` | long_name | Relative azimuth angle | degrees |
| `SA_angle` | `satellite_azimuth_angle` | long_name | Satellite azimuth angle | degrees |
| `SD` | `snow_detection_0_no_snow_1_snow` | long_name | Snow Detection: 0-no_snow, 1-snow | 1 |
| `SFR` | `snow_water_equivalent_instantaneous_snow_rate` | long_name | Snow water equivalent instantaneous snow rate | mm/hr |
| `SZ_angle` | `solar_zenith_angle` | long_name | Solar zenith angle | degrees |
| `Sat_height` | `satellite_height` | long_name | Satellite height | km |
| `Sat_range` | `satellite_range` | long_name | Satellite range | km |
| `ScanTime_dom` | `calendar_day` | long_name | Calendar day |  |
| `ScanTime_hour` | `hour_of_the_day` | long_name | hour of the day |  |
| `ScanTime_minute` | `minute_of_the_hour` | long_name | minute of the hour |  |
| `ScanTime_month` | `calendar_month` | long_name | Calendar month | months |
| `ScanTime_second` | `second_of_the_minute` | long_name | second of the minute |  |
| `ScanTime_year` | `calendar_year` | long_name | Calendar Year | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_land_2_coast` | long_name | Type of Surface: 0-ocean, 1-land, 2-coast | 1 |
| `dqf` | `data_qulity_flag_0_good_1_bad_2_undetermined` | long_name | Data qulity flag: 0-good, 1-bad, 2-undetermined | 1 |

## bkr/polar/jpss_mirs_img_n20 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `Atm_type` | `type_of_atmosphere_currently_missing` | long_name | Type of Atmosphere:currently missing | 1 |
| `BT` | `channel_temperature` | long_name | Channel Temperature (K) | Kelvin |
| `CLW` | `cloud_liquid_water` | long_name | Cloud liquid Water (mm) | mm |
| `ChanSel` | `channels_selection_used_in_retrieval` | long_name | Channels Selection Used in Retrieval | 1 |
| `ChiSqr` | `convergence_rate_3_good_10_bad` | long_name | Convergence rate: <3-good,>10-bad | 1 |
| `CldBase` | `cloud_base_pressure` | long_name | Cloud Base Pressure | millibars |
| `CldThick` | `cloud_thickness` | long_name | Cloud Thickness | millibars |
| `CldTop` | `cloud_top_pressure` | long_name | Cloud Top Pressure | millibars |
| `Emis` | `channel_emissivity` | long_name | Channel Emissivity | 1 |
| `Freq` | `central_frequency` | long_name | Central Frequency | GHz |
| `GWP` | `graupel_water_path` | long_name | Graupel Water Path (mm) | mm |
| `IWP` | `ice_water_path` | long_name | Ice Water Path (mm) | mm |
| `LWP` | `liquid_water_path` | long_name | Liquid Water Path (mm) | mm |
| `LZ_angle` | `local_zenith_angle_degree` | long_name | Local Zenith Angle degree | degrees |
| `Orb_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `Polo` | `Polo` | kept | Polarizations(Horizonal or Vertical) | 1 |
| `PrecipType` | `precipitation_type` | long_name | Precipitation Type (Frozen/Liquid) | 1 |
| `Prob_SF` | `probability_of_falling_snow` | long_name | Probability of falling snow (%) | percent |
| `Qc` | `quality_flag_0_good_1_usable_with_problem_2_bad` | long_name | Quality Flag: 0-good, 1-usable with problem, 2-bad | 1 |
| `RAzi_angle` | `relative_azimuth_angle_0_360_degree` | long_name | Relative Azimuth Angle 0-360 degree | degrees |
| `RFlag` | `rain_flag` | long_name | Rain Flag | 1 |
| `RR` | `rain_rate` | long_name | Rain Rate (mm/hr) | mm/hr |
| `RWP` | `rain_water_path` | long_name | Rain Water Path (mm) | mm |
| `SFR` | `snow_fall_rate_in_mm_hr` | long_name | Snow Fall Rate in mm/hr | mm/hr |
| `SIce` | `sea_ice_concentration` | long_name | Sea Ice Concentration (%) | percent |
| `SIce_FY` | `first_year_sea_ice_concentration` | long_name | First-Year Sea Ice Concentration (%) | percent |
| `SIce_MY` | `multi_year_sea_ice_concentration` | long_name | Multi-Year Sea Ice Concentration (%) | percent |
| `SWE` | `snow_water_equivalent` | long_name | Snow Water Equivalent (cm) | cm |
| `SWP` | `snow_water_path` | long_name | Snow Water Path | mm |
| `SZ_angle` | `solar_zenith_angle_degree` | long_name | Solar Zenith Angle (-90,90) degree | degrees |
| `ScanTime_UTC` | `number_of_seconds_since_00_00_00_utc` | long_name | Number of seconds since 00:00:00 UTC |  |
| `ScanTime_dom` | `calendar_day_of_the_month_1_31` | long_name | Calendar day of the month 1-31 |  |
| `ScanTime_doy` | `julian_day_1_366` | long_name | julian day 1-366 |  |
| `ScanTime_hour` | `hour_of_the_day_0_23` | long_name | hour of the day 0-23 |  |
| `ScanTime_minute` | `minute_of_the_hour_0_59` | long_name | minute of the hour 0-59 |  |
| `ScanTime_month` | `calendar_month_1_12` | long_name | Calendar month 1-12 | months |
| `ScanTime_second` | `second_of_the_minute_0_59` | long_name | second of the minute 0-59 |  |
| `ScanTime_year` | `calendar_year_20_xx` | long_name | Calendar Year 20XX | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_sea_ice_2_land_3_snow` | long_name | Type of Surface:0-ocean,1-sea ice,2-land,3-snow | 1 |
| `Snow` | `snow_cover` | long_name | Snow Cover | 1 |
| `SnowGS` | `snow_grain_size` | long_name | Snow Grain Size (mm) | mm |
| `SurfM` | `surface_moisture` | long_name | Surface Moisture | 1 |
| `SurfP` | `surface_pressure` | long_name | Surface Pressure (mb) | millibars |
| `TPW` | `total_precipitable_water` | long_name | Total Precipitable Water (mm) | mm |
| `TSkin` | `skin_temperature` | long_name | Skin Temperature (K) | Kelvin |
| `WindDir` | `wind_direction` | abbreviation | Wind Direction | 1 |
| `WindSp` | `wind_speed` | long_name | Wind Speed | m/s |
| `WindU` | `u_direction_wind_speed` | long_name | U-direction Wind Speed | m/s |
| `WindV` | `v_direction_wind_speed` | long_name | V-direction Wind Speed | m/s |
| `YM` | `un_corrected_channel_temperature` | long_name | Un-Corrected Channel Temperature (K) | Kelvin |

## bkr/polar/jpss_mirs_img_n21 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `Atm_type` | `type_of_atmosphere_currently_missing` | long_name | Type of Atmosphere:currently missing | 1 |
| `BT` | `channel_temperature` | long_name | Channel Temperature (K) | Kelvin |
| `CLW` | `cloud_liquid_water` | long_name | Cloud liquid Water (mm) | mm |
| `ChanSel` | `channels_selection_used_in_retrieval` | long_name | Channels Selection Used in Retrieval | 1 |
| `ChiSqr` | `convergence_rate_3_good_10_bad` | long_name | Convergence rate: <3-good,>10-bad | 1 |
| `CldBase` | `cloud_base_pressure` | long_name | Cloud Base Pressure | millibars |
| `CldThick` | `cloud_thickness` | long_name | Cloud Thickness | millibars |
| `CldTop` | `cloud_top_pressure` | long_name | Cloud Top Pressure | millibars |
| `Emis` | `channel_emissivity` | long_name | Channel Emissivity | 1 |
| `Freq` | `central_frequency` | long_name | Central Frequency | GHz |
| `GWP` | `graupel_water_path` | long_name | Graupel Water Path (mm) | mm |
| `IWP` | `ice_water_path` | long_name | Ice Water Path (mm) | mm |
| `LWP` | `liquid_water_path` | long_name | Liquid Water Path (mm) | mm |
| `LZ_angle` | `local_zenith_angle_degree` | long_name | Local Zenith Angle degree | degrees |
| `Orb_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `Polo` | `Polo` | kept | Polarizations(Horizonal or Vertical) | 1 |
| `PrecipType` | `precipitation_type` | long_name | Precipitation Type (Frozen/Liquid) | 1 |
| `Prob_SF` | `probability_of_falling_snow` | long_name | Probability of falling snow (%) | percent |
| `Qc` | `quality_flag_0_good_1_usable_with_problem_2_bad` | long_name | Quality Flag: 0-good, 1-usable with problem, 2-bad | 1 |
| `RAzi_angle` | `relative_azimuth_angle_0_360_degree` | long_name | Relative Azimuth Angle 0-360 degree | degrees |
| `RFlag` | `rain_flag` | long_name | Rain Flag | 1 |
| `RR` | `rain_rate` | long_name | Rain Rate (mm/hr) | mm/hr |
| `RWP` | `rain_water_path` | long_name | Rain Water Path (mm) | mm |
| `SFR` | `snow_fall_rate_in_mm_hr` | long_name | Snow Fall Rate in mm/hr | mm/hr |
| `SIce` | `sea_ice_concentration` | long_name | Sea Ice Concentration (%) | percent |
| `SIce_FY` | `first_year_sea_ice_concentration` | long_name | First-Year Sea Ice Concentration (%) | percent |
| `SIce_MY` | `multi_year_sea_ice_concentration` | long_name | Multi-Year Sea Ice Concentration (%) | percent |
| `SWE` | `snow_water_equivalent` | long_name | Snow Water Equivalent (cm) | cm |
| `SWP` | `snow_water_path` | long_name | Snow Water Path | mm |
| `SZ_angle` | `solar_zenith_angle_degree` | long_name | Solar Zenith Angle (-90,90) degree | degrees |
| `ScanTime_UTC` | `number_of_seconds_since_00_00_00_utc` | long_name | Number of seconds since 00:00:00 UTC |  |
| `ScanTime_dom` | `calendar_day_of_the_month_1_31` | long_name | Calendar day of the month 1-31 |  |
| `ScanTime_doy` | `julian_day_1_366` | long_name | julian day 1-366 |  |
| `ScanTime_hour` | `hour_of_the_day_0_23` | long_name | hour of the day 0-23 |  |
| `ScanTime_minute` | `minute_of_the_hour_0_59` | long_name | minute of the hour 0-59 |  |
| `ScanTime_month` | `calendar_month_1_12` | long_name | Calendar month 1-12 | months |
| `ScanTime_second` | `second_of_the_minute_0_59` | long_name | second of the minute 0-59 |  |
| `ScanTime_year` | `calendar_year_20_xx` | long_name | Calendar Year 20XX | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_sea_ice_2_land_3_snow` | long_name | Type of Surface:0-ocean,1-sea ice,2-land,3-snow | 1 |
| `Snow` | `snow_cover` | long_name | Snow Cover | 1 |
| `SnowGS` | `snow_grain_size` | long_name | Snow Grain Size (mm) | mm |
| `SurfM` | `surface_moisture` | long_name | Surface Moisture | 1 |
| `SurfP` | `surface_pressure` | long_name | Surface Pressure (mb) | millibars |
| `TPW` | `total_precipitable_water` | long_name | Total Precipitable Water (mm) | mm |
| `TSkin` | `skin_temperature` | long_name | Skin Temperature (K) | Kelvin |
| `WindDir` | `wind_direction` | abbreviation | Wind Direction | 1 |
| `WindSp` | `wind_speed` | long_name | Wind Speed | m/s |
| `WindU` | `u_direction_wind_speed` | long_name | U-direction Wind Speed | m/s |
| `WindV` | `v_direction_wind_speed` | long_name | V-direction Wind Speed | m/s |
| `YM` | `un_corrected_channel_temperature` | long_name | Un-Corrected Channel Temperature (K) | Kelvin |

## bkr/polar/jpss_mirs_img_snpp (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `Atm_type` | `type_of_atmosphere_currently_missing` | long_name | Type of Atmosphere:currently missing | 1 |
| `BT` | `channel_temperature` | long_name | Channel Temperature (K) | Kelvin |
| `CLW` | `cloud_liquid_water` | long_name | Cloud liquid Water (mm) | mm |
| `ChanSel` | `channels_selection_used_in_retrieval` | long_name | Channels Selection Used in Retrieval | 1 |
| `ChiSqr` | `convergence_rate_3_good_10_bad` | long_name | Convergence rate: <3-good,>10-bad | 1 |
| `CldBase` | `cloud_base_pressure` | long_name | Cloud Base Pressure | millibars |
| `CldThick` | `cloud_thickness` | long_name | Cloud Thickness | millibars |
| `CldTop` | `cloud_top_pressure` | long_name | Cloud Top Pressure | millibars |
| `Emis` | `channel_emissivity` | long_name | Channel Emissivity | 1 |
| `Freq` | `central_frequency` | long_name | Central Frequency | GHz |
| `GWP` | `graupel_water_path` | long_name | Graupel Water Path (mm) | mm |
| `IWP` | `ice_water_path` | long_name | Ice Water Path (mm) | mm |
| `LWP` | `liquid_water_path` | long_name | Liquid Water Path (mm) | mm |
| `LZ_angle` | `local_zenith_angle_degree` | long_name | Local Zenith Angle degree | degrees |
| `Orb_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `Polo` | `Polo` | kept | Polarizations(Horizonal or Vertical) | 1 |
| `PrecipType` | `precipitation_type` | long_name | Precipitation Type (Frozen/Liquid) | 1 |
| `Prob_SF` | `probability_of_falling_snow` | long_name | Probability of falling snow (%) | percent |
| `Qc` | `quality_flag_0_good_1_usable_with_problem_2_bad` | long_name | Quality Flag: 0-good, 1-usable with problem, 2-bad | 1 |
| `RAzi_angle` | `relative_azimuth_angle_0_360_degree` | long_name | Relative Azimuth Angle 0-360 degree | degrees |
| `RFlag` | `rain_flag` | long_name | Rain Flag | 1 |
| `RR` | `rain_rate` | long_name | Rain Rate (mm/hr) | mm/hr |
| `RWP` | `rain_water_path` | long_name | Rain Water Path (mm) | mm |
| `SFR` | `snow_fall_rate_in_mm_hr` | long_name | Snow Fall Rate in mm/hr | mm/hr |
| `SIce` | `sea_ice_concentration` | long_name | Sea Ice Concentration (%) | percent |
| `SIce_FY` | `first_year_sea_ice_concentration` | long_name | First-Year Sea Ice Concentration (%) | percent |
| `SIce_MY` | `multi_year_sea_ice_concentration` | long_name | Multi-Year Sea Ice Concentration (%) | percent |
| `SWE` | `snow_water_equivalent` | long_name | Snow Water Equivalent (cm) | cm |
| `SWP` | `snow_water_path` | long_name | Snow Water Path | mm |
| `SZ_angle` | `solar_zenith_angle_degree` | long_name | Solar Zenith Angle (-90,90) degree | degrees |
| `ScanTime_UTC` | `number_of_seconds_since_00_00_00_utc` | long_name | Number of seconds since 00:00:00 UTC |  |
| `ScanTime_dom` | `calendar_day_of_the_month_1_31` | long_name | Calendar day of the month 1-31 |  |
| `ScanTime_doy` | `julian_day_1_366` | long_name | julian day 1-366 |  |
| `ScanTime_hour` | `hour_of_the_day_0_23` | long_name | hour of the day 0-23 |  |
| `ScanTime_minute` | `minute_of_the_hour_0_59` | long_name | minute of the hour 0-59 |  |
| `ScanTime_month` | `calendar_month_1_12` | long_name | Calendar month 1-12 | months |
| `ScanTime_second` | `second_of_the_minute_0_59` | long_name | second of the minute 0-59 |  |
| `ScanTime_year` | `calendar_year_20_xx` | long_name | Calendar Year 20XX | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_sea_ice_2_land_3_snow` | long_name | Type of Surface:0-ocean,1-sea ice,2-land,3-snow | 1 |
| `Snow` | `snow_cover` | long_name | Snow Cover | 1 |
| `SnowGS` | `snow_grain_size` | long_name | Snow Grain Size (mm) | mm |
| `SurfM` | `surface_moisture` | long_name | Surface Moisture | 1 |
| `SurfP` | `surface_pressure` | long_name | Surface Pressure (mb) | millibars |
| `TPW` | `total_precipitable_water` | long_name | Total Precipitable Water (mm) | mm |
| `TSkin` | `skin_temperature` | long_name | Skin Temperature (K) | Kelvin |
| `WindDir` | `wind_direction` | abbreviation | Wind Direction | 1 |
| `WindSp` | `wind_speed` | long_name | Wind Speed | m/s |
| `WindU` | `u_direction_wind_speed` | long_name | U-direction Wind Speed | m/s |
| `WindV` | `v_direction_wind_speed` | long_name | V-direction Wind Speed | m/s |
| `YM` | `un_corrected_channel_temperature` | long_name | Un-Corrected Channel Temperature (K) | Kelvin |

## bkr/polar/jpss_mirs_snd_n20 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `Atm_type` | `type_of_atmosphere_0_simple_scene_1_retrieved_scene` | long_name | Type of Atmosphere:0-simple scene, 1-retrieved scene | 1 |
| `ChiSqr` | `convergence_rate_3_good_10_bad` | long_name | Convergence rate: <3-good,>10-bad | 1 |
| `Freq` | `central_frequency` | long_name | Central Frequency | GHz |
| `LZ_angle` | `local_zenith_angle_degree` | long_name | Local zenith angle: (-59,59) degree | degrees |
| `Orb_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `PClw` | `cloud_liquid_water_profile_in_mm` | long_name | Cloud liquid water profile in mm | mm |
| `PGraupel` | `graupel_mass_profile_in_mm` | long_name | Graupel mass profile in mm | mm |
| `PIce` | `ice_mass_profile_in_mm` | long_name | Ice mass profile in mm | mm |
| `PRain` | `rain_mass_profile_in_mm` | long_name | Rain mass profile in mm | mm |
| `PSnow` | `snow_mass_profile_in_mm` | long_name | Snow mass profile in mm | mm |
| `PTemp` | `temperature_profile_in_k` | long_name | Temperature profile in K | Kelvin |
| `PVapor` | `water_vapor_profile_in_g_kg` | long_name | Water vapor profile in g/kg | g/kg |
| `Player` | `pressure_for_each_layer_in_mb` | long_name | Pressure for each layer in mb | millibars |
| `Plevel` | `pressure_for_each_level_in_mb` | long_name | Pressure for each level in mb | millibars |
| `Polo` | `Polo` | kept | Polarizations(Horizonal or Vertical) | 1 |
| `Qc` | `quality_flag_0_good_1_usable_with_problem_2_bad` | long_name | Quality Flag: 0-good,1-usable with problem ,2-bad | 1 |
| `RAzi_angle` | `relative_azimuth_angle_0_360_degree` | long_name | Relative azimuth angle 0-360 degree | degrees |
| `SZ_angle` | `solar_zenith_angle_degree` | long_name | Solar zenith angle (-90,90) degree | degrees |
| `ScanTime_UTC` | `number_of_seconds_since_00_00_00_utc` | long_name | Number of seconds since 00:00:00 UTC |  |
| `ScanTime_dom` | `calendar_day_of_the_month_1_31` | long_name | Calendar day of the month 1-31 |  |
| `ScanTime_doy` | `julian_day_1_366` | long_name | julian day 1-366 |  |
| `ScanTime_hour` | `hour_of_the_day_0_23` | long_name | hour of the day 0-23 |  |
| `ScanTime_minute` | `minute_of_the_hour_0_59` | long_name | minute of the hour 0-59 |  |
| `ScanTime_month` | `calendar_month_1_12` | long_name | Calendar month 1-12 | months |
| `ScanTime_second` | `second_of_the_minute_0_59` | long_name | second of the minute 0-59 |  |
| `ScanTime_year` | `calendar_year_20_xx` | long_name | Calendar Year 20XX | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_sea_ice_2_land_3_snow` | long_name | Type of Surface:0-ocean,1-sea-ice,2-land,3-snow | 1 |
| `SurfP` | `surface_pressure_in_mb` | long_name | Surface pressure in mb | millibars |

## bkr/polar/jpss_mirs_snd_n21 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `Atm_type` | `type_of_atmosphere_0_simple_scene_1_retrieved_scene` | long_name | Type of Atmosphere:0-simple scene, 1-retrieved scene | 1 |
| `ChiSqr` | `convergence_rate_3_good_10_bad` | long_name | Convergence rate: <3-good,>10-bad | 1 |
| `Freq` | `central_frequency` | long_name | Central Frequency | GHz |
| `LZ_angle` | `local_zenith_angle_degree` | long_name | Local zenith angle: (-59,59) degree | degrees |
| `Orb_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `PClw` | `cloud_liquid_water_profile_in_mm` | long_name | Cloud liquid water profile in mm | mm |
| `PGraupel` | `graupel_mass_profile_in_mm` | long_name | Graupel mass profile in mm | mm |
| `PIce` | `ice_mass_profile_in_mm` | long_name | Ice mass profile in mm | mm |
| `PRain` | `rain_mass_profile_in_mm` | long_name | Rain mass profile in mm | mm |
| `PSnow` | `snow_mass_profile_in_mm` | long_name | Snow mass profile in mm | mm |
| `PTemp` | `temperature_profile_in_k` | long_name | Temperature profile in K | Kelvin |
| `PVapor` | `water_vapor_profile_in_g_kg` | long_name | Water vapor profile in g/kg | g/kg |
| `Player` | `pressure_for_each_layer_in_mb` | long_name | Pressure for each layer in mb | millibars |
| `Plevel` | `pressure_for_each_level_in_mb` | long_name | Pressure for each level in mb | millibars |
| `Polo` | `Polo` | kept | Polarizations(Horizonal or Vertical) | 1 |
| `Qc` | `quality_flag_0_good_1_usable_with_problem_2_bad` | long_name | Quality Flag: 0-good,1-usable with problem ,2-bad | 1 |
| `RAzi_angle` | `relative_azimuth_angle_0_360_degree` | long_name | Relative azimuth angle 0-360 degree | degrees |
| `SZ_angle` | `solar_zenith_angle_degree` | long_name | Solar zenith angle (-90,90) degree | degrees |
| `ScanTime_UTC` | `number_of_seconds_since_00_00_00_utc` | long_name | Number of seconds since 00:00:00 UTC |  |
| `ScanTime_dom` | `calendar_day_of_the_month_1_31` | long_name | Calendar day of the month 1-31 |  |
| `ScanTime_doy` | `julian_day_1_366` | long_name | julian day 1-366 |  |
| `ScanTime_hour` | `hour_of_the_day_0_23` | long_name | hour of the day 0-23 |  |
| `ScanTime_minute` | `minute_of_the_hour_0_59` | long_name | minute of the hour 0-59 |  |
| `ScanTime_month` | `calendar_month_1_12` | long_name | Calendar month 1-12 | months |
| `ScanTime_second` | `second_of_the_minute_0_59` | long_name | second of the minute 0-59 |  |
| `ScanTime_year` | `calendar_year_20_xx` | long_name | Calendar Year 20XX | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_sea_ice_2_land_3_snow` | long_name | Type of Surface:0-ocean,1-sea-ice,2-land,3-snow | 1 |
| `SurfP` | `surface_pressure_in_mb` | long_name | Surface pressure in mb | millibars |

## bkr/polar/jpss_mirs_snd_snpp (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `Atm_type` | `type_of_atmosphere_0_simple_scene_1_retrieved_scene` | long_name | Type of Atmosphere:0-simple scene, 1-retrieved scene | 1 |
| `ChiSqr` | `convergence_rate_3_good_10_bad` | long_name | Convergence rate: <3-good,>10-bad | 1 |
| `Freq` | `central_frequency` | long_name | Central Frequency | GHz |
| `LZ_angle` | `local_zenith_angle_degree` | long_name | Local zenith angle: (-59,59) degree | degrees |
| `Orb_mode` | `orbital_mode_0_ascending_1_descending_2_transitioning` | long_name | Orbital Mode: 0-ascending,1-descending,2-transitioning | 1 |
| `PClw` | `cloud_liquid_water_profile_in_mm` | long_name | Cloud liquid water profile in mm | mm |
| `PGraupel` | `graupel_mass_profile_in_mm` | long_name | Graupel mass profile in mm | mm |
| `PIce` | `ice_mass_profile_in_mm` | long_name | Ice mass profile in mm | mm |
| `PRain` | `rain_mass_profile_in_mm` | long_name | Rain mass profile in mm | mm |
| `PSnow` | `snow_mass_profile_in_mm` | long_name | Snow mass profile in mm | mm |
| `PTemp` | `temperature_profile_in_k` | long_name | Temperature profile in K | Kelvin |
| `PVapor` | `water_vapor_profile_in_g_kg` | long_name | Water vapor profile in g/kg | g/kg |
| `Player` | `pressure_for_each_layer_in_mb` | long_name | Pressure for each layer in mb | millibars |
| `Plevel` | `pressure_for_each_level_in_mb` | long_name | Pressure for each level in mb | millibars |
| `Polo` | `Polo` | kept | Polarizations(Horizonal or Vertical) | 1 |
| `Qc` | `quality_flag_0_good_1_usable_with_problem_2_bad` | long_name | Quality Flag: 0-good,1-usable with problem ,2-bad | 1 |
| `RAzi_angle` | `relative_azimuth_angle_0_360_degree` | long_name | Relative azimuth angle 0-360 degree | degrees |
| `SZ_angle` | `solar_zenith_angle_degree` | long_name | Solar zenith angle (-90,90) degree | degrees |
| `ScanTime_UTC` | `number_of_seconds_since_00_00_00_utc` | long_name | Number of seconds since 00:00:00 UTC |  |
| `ScanTime_dom` | `calendar_day_of_the_month_1_31` | long_name | Calendar day of the month 1-31 |  |
| `ScanTime_doy` | `julian_day_1_366` | long_name | julian day 1-366 |  |
| `ScanTime_hour` | `hour_of_the_day_0_23` | long_name | hour of the day 0-23 |  |
| `ScanTime_minute` | `minute_of_the_hour_0_59` | long_name | minute of the hour 0-59 |  |
| `ScanTime_month` | `calendar_month_1_12` | long_name | Calendar month 1-12 | months |
| `ScanTime_second` | `second_of_the_minute_0_59` | long_name | second of the minute 0-59 |  |
| `ScanTime_year` | `calendar_year_20_xx` | long_name | Calendar Year 20XX | years |
| `Sfc_type` | `type_of_surface_0_ocean_1_sea_ice_2_land_3_snow` | long_name | Type of Surface:0-ocean,1-sea-ice,2-land,3-snow | 1 |
| `SurfP` | `surface_pressure_in_mb` | long_name | Surface pressure in mb | millibars |

## bkr/polar/jpss_viirs_mband_n20 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `M01_quality_flags` | `m01_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M01 |  |
| `M01_radiance` | `spectral_radiance_viirs_band_m01` | long_name | Spectral radiance, VIIRS band M01 | W m-2 sr-1 um-1 |
| `M01_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m01` | long_name | Top of atmosphere reflectance, VIIRS band M01 | 1 |
| `M02_quality_flags` | `m02_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M02 |  |
| `M02_radiance` | `spectral_radiance_viirs_band_m02` | long_name | Spectral radiance, VIIRS band M02 | W m-2 sr-1 um-1 |
| `M02_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m02` | long_name | Top of atmosphere reflectance, VIIRS band M02 | 1 |
| `M03_quality_flags` | `m03_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M03 |  |
| `M03_radiance` | `spectral_radiance_viirs_band_m03` | long_name | Spectral radiance, VIIRS band M03 | W m-2 sr-1 um-1 |
| `M03_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m03` | long_name | Top of atmosphere reflectance, VIIRS band M03 | 1 |
| `M04_quality_flags` | `m04_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M04 |  |
| `M04_radiance` | `spectral_radiance_viirs_band_m04` | long_name | Spectral radiance, VIIRS band M04 | W m-2 sr-1 um-1 |
| `M04_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m04` | long_name | Top of atmosphere reflectance, VIIRS band M04 | 1 |
| `M05_quality_flags` | `m05_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M05 |  |
| `M05_radiance` | `spectral_radiance_viirs_band_m05` | long_name | Spectral radiance, VIIRS band M05 | W m-2 sr-1 um-1 |
| `M05_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m05` | long_name | Top of atmosphere reflectance, VIIRS band M05 | 1 |
| `M06_quality_flags` | `m06_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M06 |  |
| `M06_radiance` | `spectral_radiance_viirs_band_m06` | long_name | Spectral radiance, VIIRS band M06 | W m-2 sr-1 um-1 |
| `M06_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m06` | long_name | Top of atmosphere reflectance, VIIRS band M06 | 1 |
| `M07_quality_flags` | `m07_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M07 |  |
| `M07_radiance` | `spectral_radiance_viirs_band_m07` | long_name | Spectral radiance, VIIRS band M07 | W m-2 sr-1 um-1 |
| `M07_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m07` | long_name | Top of atmosphere reflectance, VIIRS band M07 | 1 |
| `M08_quality_flags` | `m08_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M08 |  |
| `M08_radiance` | `spectral_radiance_viirs_band_m08` | long_name | Spectral radiance, VIIRS band M08 | W m-2 sr-1 um-1 |
| `M08_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m08` | long_name | Top of atmosphere reflectance, VIIRS band M08 | 1 |
| `M09_quality_flags` | `m09_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M09 |  |
| `M09_radiance` | `spectral_radiance_viirs_band_m09` | long_name | Spectral radiance, VIIRS band M09 | W m-2 sr-1 um-1 |
| `M09_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m09` | long_name | Top of atmosphere reflectance, VIIRS band M09 | 1 |
| `M10_quality_flags` | `m10_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M10 |  |
| `M10_radiance` | `spectral_radiance_viirs_band_m10` | long_name | Spectral radiance, VIIRS band M10 | W m-2 sr-1 um-1 |
| `M10_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m10` | long_name | Top of atmosphere reflectance, VIIRS band M10 | 1 |
| `M11_quality_flags` | `m11_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M11 |  |
| `M11_radiance` | `spectral_radiance_viirs_band_m11` | long_name | Spectral radiance, VIIRS band M11 | W m-2 sr-1 um-1 |
| `M11_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m11` | long_name | Top of atmosphere reflectance, VIIRS band M11 | 1 |
| `M12_brightness_temperature` | `brightness_temperature_viirs_band_m12` | long_name | Brightness temperature, VIIRS band M12 | K |
| `M12_quality_flags` | `m12_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M12 |  |
| `M12_radiance` | `spectral_radiance_viirs_band_m12` | long_name | Spectral radiance, VIIRS band M12 | W m-2 sr-1 um-1 |
| `M13_brightness_temperature` | `brightness_temperature_viirs_band_m13` | long_name | Brightness temperature, VIIRS band M13 | K |
| `M13_quality_flags` | `m13_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M13 |  |
| `M13_radiance` | `spectral_radiance_viirs_band_m13` | long_name | Spectral radiance, VIIRS band M13 | W m-2 sr-1 um-1 |
| `M14_brightness_temperature` | `brightness_temperature_viirs_band_m14` | long_name | Brightness temperature, VIIRS band M14 | K |
| `M14_quality_flags` | `m14_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M14 |  |
| `M14_radiance` | `spectral_radiance_viirs_band_m14` | long_name | Spectral radiance, VIIRS band M14 | W m-2 sr-1 um-1 |
| `M15_brightness_temperature` | `brightness_temperature_viirs_band_m15` | long_name | Brightness temperature, VIIRS band M15 | K |
| `M15_quality_flags` | `m15_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M15 |  |
| `M15_radiance` | `spectral_radiance_viirs_band_m15` | long_name | Spectral radiance, VIIRS band M15 | W m-2 sr-1 um-1 |
| `M16_brightness_temperature` | `brightness_temperature_viirs_band_m16` | long_name | Brightness temperature, VIIRS band M16 | K |
| `M16_quality_flags` | `m16_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M16 |  |
| `M16_radiance` | `spectral_radiance_viirs_band_m16` | long_name | Spectral radiance, VIIRS band M16 | W m-2 sr-1 um-1 |
| `scan_start_offset` | `start_of_each_scan_seconds_after_start_time` | long_name | Start of each scan, seconds after start_time | s |
| `terrain_height` | `terrain_height_above_the_ellipsoid` | long_name | Terrain height above the ellipsoid | m |

## bkr/polar/jpss_viirs_mband_n21 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `M01_quality_flags` | `m01_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M01 |  |
| `M01_radiance` | `spectral_radiance_viirs_band_m01` | long_name | Spectral radiance, VIIRS band M01 | W m-2 sr-1 um-1 |
| `M01_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m01` | long_name | Top of atmosphere reflectance, VIIRS band M01 | 1 |
| `M02_quality_flags` | `m02_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M02 |  |
| `M02_radiance` | `spectral_radiance_viirs_band_m02` | long_name | Spectral radiance, VIIRS band M02 | W m-2 sr-1 um-1 |
| `M02_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m02` | long_name | Top of atmosphere reflectance, VIIRS band M02 | 1 |
| `M03_quality_flags` | `m03_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M03 |  |
| `M03_radiance` | `spectral_radiance_viirs_band_m03` | long_name | Spectral radiance, VIIRS band M03 | W m-2 sr-1 um-1 |
| `M03_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m03` | long_name | Top of atmosphere reflectance, VIIRS band M03 | 1 |
| `M04_quality_flags` | `m04_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M04 |  |
| `M04_radiance` | `spectral_radiance_viirs_band_m04` | long_name | Spectral radiance, VIIRS band M04 | W m-2 sr-1 um-1 |
| `M04_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m04` | long_name | Top of atmosphere reflectance, VIIRS band M04 | 1 |
| `M05_quality_flags` | `m05_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M05 |  |
| `M05_radiance` | `spectral_radiance_viirs_band_m05` | long_name | Spectral radiance, VIIRS band M05 | W m-2 sr-1 um-1 |
| `M05_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m05` | long_name | Top of atmosphere reflectance, VIIRS band M05 | 1 |
| `M06_quality_flags` | `m06_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M06 |  |
| `M06_radiance` | `spectral_radiance_viirs_band_m06` | long_name | Spectral radiance, VIIRS band M06 | W m-2 sr-1 um-1 |
| `M06_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m06` | long_name | Top of atmosphere reflectance, VIIRS band M06 | 1 |
| `M07_quality_flags` | `m07_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M07 |  |
| `M07_radiance` | `spectral_radiance_viirs_band_m07` | long_name | Spectral radiance, VIIRS band M07 | W m-2 sr-1 um-1 |
| `M07_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m07` | long_name | Top of atmosphere reflectance, VIIRS band M07 | 1 |
| `M08_quality_flags` | `m08_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M08 |  |
| `M08_radiance` | `spectral_radiance_viirs_band_m08` | long_name | Spectral radiance, VIIRS band M08 | W m-2 sr-1 um-1 |
| `M08_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m08` | long_name | Top of atmosphere reflectance, VIIRS band M08 | 1 |
| `M09_quality_flags` | `m09_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M09 |  |
| `M09_radiance` | `spectral_radiance_viirs_band_m09` | long_name | Spectral radiance, VIIRS band M09 | W m-2 sr-1 um-1 |
| `M09_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m09` | long_name | Top of atmosphere reflectance, VIIRS band M09 | 1 |
| `M10_quality_flags` | `m10_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M10 |  |
| `M10_radiance` | `spectral_radiance_viirs_band_m10` | long_name | Spectral radiance, VIIRS band M10 | W m-2 sr-1 um-1 |
| `M10_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m10` | long_name | Top of atmosphere reflectance, VIIRS band M10 | 1 |
| `M11_quality_flags` | `m11_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M11 |  |
| `M11_radiance` | `spectral_radiance_viirs_band_m11` | long_name | Spectral radiance, VIIRS band M11 | W m-2 sr-1 um-1 |
| `M11_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m11` | long_name | Top of atmosphere reflectance, VIIRS band M11 | 1 |
| `M12_brightness_temperature` | `brightness_temperature_viirs_band_m12` | long_name | Brightness temperature, VIIRS band M12 | K |
| `M12_quality_flags` | `m12_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M12 |  |
| `M12_radiance` | `spectral_radiance_viirs_band_m12` | long_name | Spectral radiance, VIIRS band M12 | W m-2 sr-1 um-1 |
| `M13_brightness_temperature` | `brightness_temperature_viirs_band_m13` | long_name | Brightness temperature, VIIRS band M13 | K |
| `M13_quality_flags` | `m13_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M13 |  |
| `M13_radiance` | `spectral_radiance_viirs_band_m13` | long_name | Spectral radiance, VIIRS band M13 | W m-2 sr-1 um-1 |
| `M14_brightness_temperature` | `brightness_temperature_viirs_band_m14` | long_name | Brightness temperature, VIIRS band M14 | K |
| `M14_quality_flags` | `m14_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M14 |  |
| `M14_radiance` | `spectral_radiance_viirs_band_m14` | long_name | Spectral radiance, VIIRS band M14 | W m-2 sr-1 um-1 |
| `M15_brightness_temperature` | `brightness_temperature_viirs_band_m15` | long_name | Brightness temperature, VIIRS band M15 | K |
| `M15_quality_flags` | `m15_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M15 |  |
| `M15_radiance` | `spectral_radiance_viirs_band_m15` | long_name | Spectral radiance, VIIRS band M15 | W m-2 sr-1 um-1 |
| `M16_brightness_temperature` | `brightness_temperature_viirs_band_m16` | long_name | Brightness temperature, VIIRS band M16 | K |
| `M16_quality_flags` | `m16_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M16 |  |
| `M16_radiance` | `spectral_radiance_viirs_band_m16` | long_name | Spectral radiance, VIIRS band M16 | W m-2 sr-1 um-1 |
| `scan_start_offset` | `start_of_each_scan_seconds_after_start_time` | long_name | Start of each scan, seconds after start_time | s |
| `terrain_height` | `terrain_height_above_the_ellipsoid` | long_name | Terrain height above the ellipsoid | m |

## bkr/polar/jpss_viirs_mband_snpp (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `M01_quality_flags` | `m01_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M01 |  |
| `M01_radiance` | `spectral_radiance_viirs_band_m01` | long_name | Spectral radiance, VIIRS band M01 | W m-2 sr-1 um-1 |
| `M01_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m01` | long_name | Top of atmosphere reflectance, VIIRS band M01 | 1 |
| `M02_quality_flags` | `m02_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M02 |  |
| `M02_radiance` | `spectral_radiance_viirs_band_m02` | long_name | Spectral radiance, VIIRS band M02 | W m-2 sr-1 um-1 |
| `M02_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m02` | long_name | Top of atmosphere reflectance, VIIRS band M02 | 1 |
| `M03_quality_flags` | `m03_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M03 |  |
| `M03_radiance` | `spectral_radiance_viirs_band_m03` | long_name | Spectral radiance, VIIRS band M03 | W m-2 sr-1 um-1 |
| `M03_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m03` | long_name | Top of atmosphere reflectance, VIIRS band M03 | 1 |
| `M04_quality_flags` | `m04_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M04 |  |
| `M04_radiance` | `spectral_radiance_viirs_band_m04` | long_name | Spectral radiance, VIIRS band M04 | W m-2 sr-1 um-1 |
| `M04_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m04` | long_name | Top of atmosphere reflectance, VIIRS band M04 | 1 |
| `M05_quality_flags` | `m05_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M05 |  |
| `M05_radiance` | `spectral_radiance_viirs_band_m05` | long_name | Spectral radiance, VIIRS band M05 | W m-2 sr-1 um-1 |
| `M05_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m05` | long_name | Top of atmosphere reflectance, VIIRS band M05 | 1 |
| `M06_quality_flags` | `m06_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M06 |  |
| `M06_radiance` | `spectral_radiance_viirs_band_m06` | long_name | Spectral radiance, VIIRS band M06 | W m-2 sr-1 um-1 |
| `M06_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m06` | long_name | Top of atmosphere reflectance, VIIRS band M06 | 1 |
| `M07_quality_flags` | `m07_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M07 |  |
| `M07_radiance` | `spectral_radiance_viirs_band_m07` | long_name | Spectral radiance, VIIRS band M07 | W m-2 sr-1 um-1 |
| `M07_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m07` | long_name | Top of atmosphere reflectance, VIIRS band M07 | 1 |
| `M08_quality_flags` | `m08_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M08 |  |
| `M08_radiance` | `spectral_radiance_viirs_band_m08` | long_name | Spectral radiance, VIIRS band M08 | W m-2 sr-1 um-1 |
| `M08_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m08` | long_name | Top of atmosphere reflectance, VIIRS band M08 | 1 |
| `M09_quality_flags` | `m09_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M09 |  |
| `M09_radiance` | `spectral_radiance_viirs_band_m09` | long_name | Spectral radiance, VIIRS band M09 | W m-2 sr-1 um-1 |
| `M09_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m09` | long_name | Top of atmosphere reflectance, VIIRS band M09 | 1 |
| `M10_quality_flags` | `m10_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M10 |  |
| `M10_radiance` | `spectral_radiance_viirs_band_m10` | long_name | Spectral radiance, VIIRS band M10 | W m-2 sr-1 um-1 |
| `M10_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m10` | long_name | Top of atmosphere reflectance, VIIRS band M10 | 1 |
| `M11_quality_flags` | `m11_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M11 |  |
| `M11_radiance` | `spectral_radiance_viirs_band_m11` | long_name | Spectral radiance, VIIRS band M11 | W m-2 sr-1 um-1 |
| `M11_reflectance` | `top_of_atmosphere_reflectance_viirs_band_m11` | long_name | Top of atmosphere reflectance, VIIRS band M11 | 1 |
| `M12_brightness_temperature` | `brightness_temperature_viirs_band_m12` | long_name | Brightness temperature, VIIRS band M12 | K |
| `M12_quality_flags` | `m12_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M12 |  |
| `M12_radiance` | `spectral_radiance_viirs_band_m12` | long_name | Spectral radiance, VIIRS band M12 | W m-2 sr-1 um-1 |
| `M13_brightness_temperature` | `brightness_temperature_viirs_band_m13` | long_name | Brightness temperature, VIIRS band M13 | K |
| `M13_quality_flags` | `m13_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M13 |  |
| `M13_radiance` | `spectral_radiance_viirs_band_m13` | long_name | Spectral radiance, VIIRS band M13 | W m-2 sr-1 um-1 |
| `M14_brightness_temperature` | `brightness_temperature_viirs_band_m14` | long_name | Brightness temperature, VIIRS band M14 | K |
| `M14_quality_flags` | `m14_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M14 |  |
| `M14_radiance` | `spectral_radiance_viirs_band_m14` | long_name | Spectral radiance, VIIRS band M14 | W m-2 sr-1 um-1 |
| `M15_brightness_temperature` | `brightness_temperature_viirs_band_m15` | long_name | Brightness temperature, VIIRS band M15 | K |
| `M15_quality_flags` | `m15_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M15 |  |
| `M15_radiance` | `spectral_radiance_viirs_band_m15` | long_name | Spectral radiance, VIIRS band M15 | W m-2 sr-1 um-1 |
| `M16_brightness_temperature` | `brightness_temperature_viirs_band_m16` | long_name | Brightness temperature, VIIRS band M16 | K |
| `M16_quality_flags` | `m16_quality_flags` | tidied | QF1_VIIRSMBANDSDR quality flags, VIIRS band M16 |  |
| `M16_radiance` | `spectral_radiance_viirs_band_m16` | long_name | Spectral radiance, VIIRS band M16 | W m-2 sr-1 um-1 |
| `scan_start_offset` | `start_of_each_scan_seconds_after_start_time` | long_name | Start of each scan, seconds after start_time | s |
| `terrain_height` | `terrain_height_above_the_ellipsoid` | long_name | Terrain height above the ellipsoid | m |

## bkr/polar/metop_amsua (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `alpha` | `angstrom_exponent` | abbreviation | temperature radiance constant c for each channel | K/K |
| `beta` | `temperature_radiance_constant_b_for_each_channel` | long_name | temperature radiance constant b for each channel | K |
| `channel_1` | `scene_radiance_for_channel_1` | long_name | Scene Radiance for Channel 1 | mW m-2 sr-1 (cm-1)-1 |
| `channel_10` | `scene_radiance_for_channel_10` | long_name | Scene Radiance for Channel 10 | mW m-2 sr-1 (cm-1)-1 |
| `channel_11` | `scene_radiance_for_channel_11` | long_name | Scene Radiance for Channel 11 | mW m-2 sr-1 (cm-1)-1 |
| `channel_12` | `scene_radiance_for_channel_12` | long_name | Scene Radiance for Channel 12 | mW m-2 sr-1 (cm-1)-1 |
| `channel_13` | `scene_radiance_for_channel_13` | long_name | Scene Radiance for Channel 13 | mW m-2 sr-1 (cm-1)-1 |
| `channel_14` | `scene_radiance_for_channel_14` | long_name | Scene Radiance for Channel 14 | mW m-2 sr-1 (cm-1)-1 |
| `channel_15` | `scene_radiance_for_channel_15` | long_name | Scene Radiance for Channel 15 | mW m-2 sr-1 (cm-1)-1 |
| `channel_2` | `scene_radiance_for_channel_2` | long_name | Scene Radiance for Channel 2 | mW m-2 sr-1 (cm-1)-1 |
| `channel_3` | `scene_radiance_for_channel_3` | long_name | Scene Radiance for Channel 3 | mW m-2 sr-1 (cm-1)-1 |
| `channel_4` | `scene_radiance_for_channel_4` | long_name | Scene Radiance for Channel 4 | mW m-2 sr-1 (cm-1)-1 |
| `channel_5` | `scene_radiance_for_channel_5` | long_name | Scene Radiance for Channel 5 | mW m-2 sr-1 (cm-1)-1 |
| `channel_6` | `scene_radiance_for_channel_6` | long_name | Scene Radiance for Channel 6 | mW m-2 sr-1 (cm-1)-1 |
| `channel_7` | `scene_radiance_for_channel_7` | long_name | Scene Radiance for Channel 7 | mW m-2 sr-1 (cm-1)-1 |
| `channel_8` | `scene_radiance_for_channel_8` | long_name | Scene Radiance for Channel 8 | mW m-2 sr-1 (cm-1)-1 |
| `channel_9` | `scene_radiance_for_channel_9` | long_name | Scene Radiance for Channel 9 | mW m-2 sr-1 (cm-1)-1 |
| `degraded_ins_MDR` | `degraded_ins_mdr` | tidied | Quality of MDR has been degraded due to an instrument degrad | 1 |
| `degraded_proc_MDR` | `degraded_proc_mdr` | tidied | Quality of MDR has been degraded due to a processing degrada | 1 |
| `satellite_azimuth` | `angular_relation_for_the_earth_view_satellite_azimuth` | long_name | angular relation for the earth view: satellite azimuth | degrees |
| `satellite_zenith` | `angular_relation_for_the_earth_view_satellite_zenith` | long_name | angular relation for the earth view: satellite zenith | degrees |
| `solar_azimuth` | `angular_relation_for_the_earth_view_solar_azimuth` | long_name | angular relation for the earth view: solar azimuth | degrees |
| `solar_zenith` | `angular_relation_for_the_earth_view_solar_zenith` | long_name | angular relation for the earth view: solar zenith | degrees |
| `surface_properties` | `surface_property` | long_name | Surface property | 1 |
| `terrain_elevation` | `average_terrain_elevation` | long_name | average terrain elevation | m |
| `wnc` | `temperature_radiance_central_wavenumber_for_each_channel` | long_name | temperature radiance central wavenumber for each channel | cm-1 |

## bkr/polar/metop_avhrr (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `1` | `channel_1_reflectance_0_63um` | channel |  | % |
| `2` | `channel_2_reflectance_0_86um` | channel |  | % |
| `3a` | `channel_3a_reflectance_1_6um` | channel |  | % |
| `3b` | `channel_3b_brightness_temperature_3_7um` | channel |  | K |
| `4` | `channel_4_brightness_temperature_10_8um` | channel |  | K |
| `5` | `channel_5_brightness_temperature_12um` | channel |  | K |

## bkr/polar/metop_gome (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `integration_time` | `integration_time_for_each_pixel` | long_name |  | s |
| `scan_direction_type` | `scan_direction_for_each_measurement` | long_name |  |  |
| `scan_subindex` | `relative_index_of_this_measurement_within_a_scan` | long_name |  |  |
| `solar_azimuth_angle_toa` | `solar_azimuth_angle_at_top_of_atmosphere` | long_name |  | degree |
| `solar_zenith_angle_toa` | `solar_zenith_angle_at_top_of_atmosphere` | long_name |  | degree |
| `viewing_azimuth_angle_toa` | `viewing_azimuth_angle_at_top_of_atmosphere` | long_name |  | degree |
| `viewing_zenith_angle_toa` | `viewing_zenith_angle_at_top_of_atmosphere` | long_name |  | degree |
| `wavelength_photon_radiance` | `measured_radiances` | long_name |  | count/s/cm2/sr/nm |

## bkr/polar/metop_iasi (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `scan_subindex` | `relative_index_of_this_measurement_within_an_mdr` | long_name |  |  |
| `solar_azimuth_angle` | `solar_azimuth_angle_at_the_surface` | long_name |  | degree |
| `solar_zenith_angle` | `solar_zenith_angle_at_the_surface` | long_name |  | degree |
| `wavenumber_radiance` | `measured_radiances` | long_name |  | W/m^2.sr.m^-1 |

## bkr/polar/metop_mhs (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `channel_1` | `scene_radiance_for_channel_1` | long_name | Scene Radiance for Channel 1 | mW m-2 sr-1 (cm-1)-1 |
| `channel_2` | `scene_radiance_for_channel_2` | long_name | Scene Radiance for Channel 2 | mW m-2 sr-1 (cm-1)-1 |
| `channel_3` | `scene_radiance_for_channel_3` | long_name | Scene Radiance for Channel 3 | mW m-2 sr-1 (cm-1)-1 |
| `channel_4` | `scene_radiance_for_channel_4` | long_name | Scene Radiance for Channel 4 | mW m-2 sr-1 (cm-1)-1 |
| `channel_5` | `scene_radiance_for_channel_5` | long_name | Scene Radiance for Channel 5 | mW m-2 sr-1 (cm-1)-1 |
| `degraded_ins_MDR` | `degraded_ins_mdr` | tidied | Quality of MDR has been degraded due to an instrument degrad | 1 |
| `degraded_proc_MDR` | `degraded_proc_mdr` | tidied | Quality of MDR has been degraded due to a processing degrada | 1 |
| `satellite_azimuth` | `angular_relation_for_the_earth_view_satellite_azimuth` | long_name | angular relation for the earth view: satellite azimuth | degrees |
| `satellite_zenith` | `angular_relation_for_the_earth_view_satellite_zenith` | long_name | angular relation for the earth view: satellite zenith | degrees |
| `solar_azimuth` | `angular_relation_for_the_earth_view_solar_azimuth` | long_name | angular relation for the earth view: solar azimuth | degrees |
| `solar_zenith` | `angular_relation_for_the_earth_view_solar_zenith` | long_name | angular relation for the earth view: solar zenith | degrees |
| `surface_properties` | `surface_property` | long_name | Surface property | 1 |
| `terrain_elevation` | `average_terrain_elevation` | long_name | average terrain elevation | m |

## bkr/polar/metop_sg_mws (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `L1B_quality_flag` | `l1_b_quality_flag` | tidied |  |  |

## bkr/polar/metop_sg_ro (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `bangle` | `bending_angle` | abbreviation |  |  |
| `impact` | `impact_parameter` | abbreviation |  |  |

## bkr/polar/pace_l2 (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `harp2_cloud_gpc_cloud_top_pressure` | `harp2_cloud_gpc_air_pressure_at_the_cloud_top` | long_name | Air pressure at the cloud top | Pa |
| `harp2_cloud_gpc_cloud_top_temperature` | `harp2_cloud_gpc_air_temperature_at_the_cloud_top` | long_name | Air temperature at the cloud top | K |
| `harp2_cloud_gpc_nadir_view_time` | `harp2_cloud_gpc_bin_was_viewed_at_nadir_view` | long_name | Time bin was viewed at nadir view | seconds from UTC midnight |
| `spexone_l1c_aolp` | `spexone_l1c_angle_of_linear_polarization` | long_name | Angle of linear polarization | degrees |
| `spexone_l1c_aolp_stdev` | `spexone_l1c_standard_deviation_of_aolp_in_bin` | long_name | Standard deviation of AOLP in bin | degrees |
| `spexone_l1c_dolp` | `spexone_l1c_degree_of_linear_polarization` | long_name | Degree of linear polarization | 1 |
| `spexone_l1c_dolp_stdev` | `spexone_l1c_standard_deviation_of_dolp_in_bin` | long_name | Standard deviation of DOLP in bin | 1 |
| `spexone_l1c_height_stdev` | `spexone_l1c_standard_deviation_of_terrain_altitude_within_bin` | long_name | Standard deviation of terrain altitude within bin | meters |
| `spexone_l1c_i` | `spexone_l1c_i_stokes_vector_component` | long_name | I Stokes vector component | W m^-2 sr^-1 um^-1 |
| `spexone_l1c_i_polsample` | `spexone_l1c_i_stokes_vector_component_at_polarimeter_spectral_sampling` | long_name | I Stokes vector component at polarimeter spectral sampling | W m^-2 sr^-1 um^-1 |
| `spexone_l1c_i_polsample_stdev` | `spexone_l1c_standard_deviation_of_i_polsample_in_bin` | long_name | standard deviation of i_polsample in bin | W m^-2 sr^-1 um^-1 |
| `spexone_l1c_i_stdev` | `spexone_l1c_standard_deviation_of_i_in_bin` | long_name | Standard deviation of I in bin | W m^-2 sr^-1 um^-1 |
| `spexone_l1c_intensity_bandpass` | `spexone_l1c_intensity_field_bandpass_at_each_view` | long_name | Intensity field bandpass at each view | nm |
| `spexone_l1c_intensity_f0` | `spexone_l1c_intensity_band_solar_irradiance` | long_name | Intensity band solar irradiance | W m^-2 um^-1 |
| `spexone_l1c_intensity_wavelength` | `spexone_l1c_intensity_field_center_wavelengths_at_each_view` | long_name | Intensity field center wavelengths at each view | nm |
| `spexone_l1c_nadir_view_time` | `spexone_l1c_bin_was_viewed_at_nadir_view` | long_name | Time bin was viewed at nadir view | seconds since 2026-10-29 |
| `spexone_l1c_polarization_bandpass` | `spexone_l1c_polarization_field_bandpasses_at_each_view` | long_name | Polarization field bandpasses at each view | nm |
| `spexone_l1c_polarization_f0` | `spexone_l1c_polarization_band_solar_irradiance` | long_name | Polarization band solar irradiance | W m^-2 um^-1 |
| `spexone_l1c_polarization_wavelength` | `spexone_l1c_polarization_field_wavelengths_at_each_view` | long_name | Polarization field wavelengths at each view | nm |
| `spexone_l1c_q` | `spexone_l1c_q_stokes_vector_component` | long_name | Q Stokes vector component | W m^-2 sr^-1 um^-1 |
| `spexone_l1c_q_over_i` | `spexone_l1c_q_over_i_stokes_vector_component` | long_name | Q over I (little q) Stokes vector component | 1 |
| `spexone_l1c_q_over_i_stdev` | `spexone_l1c_standard_deviation_of_q_over_i_in_bin` | long_name | Standard deviation of q_over_i in bin | 1 |
| `spexone_l1c_q_stdev` | `spexone_l1c_standard_deviation_of_q_in_bin` | long_name | Standard deviation of Q in bin | W m^-2 sr^-1 um^-1 |
| `spexone_l1c_qc` | `spexone_l1c_specific_cloud_liquid_water_content` | override | Quality indicator |  |
| `spexone_l1c_qc_polsample` | `spexone_l1c_quality_indicator` | long_name | Quality indicator |  |
| `spexone_l1c_scattering_angle` | `spexone_l1c_scattering_angle_at_bin_locations` | long_name | Scattering angle at bin locations | degrees |
| `spexone_l1c_sensor_azimuth_angle` | `spexone_l1c_sensor_azimuth_angle_at_bin_locations` | long_name | Sensor azimuth angle at bin locations | degrees |
| `spexone_l1c_sensor_view_angle` | `spexone_l1c_along_track_view_angles_for_sensor` | long_name | Along-track view angles for sensor | degrees |
| `spexone_l1c_sensor_zenith_angle` | `spexone_l1c_sensor_zenith_angle_at_bin_locations` | long_name | Sensor zenith angle at bin locations | degrees |
| `spexone_l1c_solar_azimuth_angle` | `spexone_l1c_solar_azimuth_angle_at_bin_locations` | long_name | Solar azimuth angle at bin locations | degrees |
| `spexone_l1c_solar_zenith_angle` | `spexone_l1c_solar_zenith_angle_at_bin_locations` | long_name | Solar zenith angle at bin locations | degrees |
| `spexone_l1c_u` | `spexone_l1c_u_stokes_vector_component` | long_name | U Stokes vector component | W m^-2 sr^-1 um^-1 |
| `spexone_l1c_u_over_i` | `spexone_l1c_u_over_i_stokes_vector_component` | long_name | U over I (little u) Stokes vector component | 1 |
| `spexone_l1c_u_over_i_stdev` | `spexone_l1c_standard_deviation_of_u_over_i_in_bin` | long_name | Standard deviation of u_over_i in bin | 1 |
| `spexone_l1c_u_stdev` | `spexone_l1c_standard_deviation_of_u_in_bin` | long_name | Standard deviation of U in bin | W m^-2 sr^-1 um^-1 |
| `spexone_l1c_view_time_offsets` | `spexone_l1c_offsets_of_views_from_nadir_view` | long_name | Time offsets of views from nadir view | seconds |
| `spexone_mapol_ocean_Rrs_angular` | `spexone_mapol_ocean_angular_remote_sensing_reflectance` | long_name | Angular remote sensing reflectance (before BRDF correction)  | sr^-1 |
| `spexone_mapol_ocean_Rrs_angular_mean` | `spexone_mapol_ocean_angular_mean_of_rrs_angular` | long_name | Angular mean of Rrs_angular  | sr^-1 |
| `spexone_mapol_ocean_Rrs_angular_std` | `spexone_mapol_ocean_angular_standard_deviation_of_rrs_angular` | long_name | Angular standard deviation of Rrs_angular | sr^-1 |
| `spexone_mapol_ocean_Rrs_nadir` | `spexone_mapol_ocean_nadir_adjusted_angular_remote_sensing_reflectance` | long_name | Nadir adjusted angular remote sensing reflectance (after BRD | sr^-1 |
| `spexone_mapol_ocean_Rrs_nadir_mean` | `spexone_mapol_ocean_angular_mean_of_rrs_nadir` | long_name | Angular mean of Rrs_nadir | sr^-1 |
| `spexone_mapol_ocean_Rrs_nadir_std` | `spexone_mapol_ocean_angular_standard_deviation_of_rrs_nadir` | long_name | Angular standard deviation of Rrs_nadir | sr^-1 |
| `spexone_mapol_ocean_aerosol_depol_ratio` | `spexone_mapol_ocean_aerosol_depolarization_ratio` | long_name |  Aerosol depolarization ratio |  |
| `spexone_mapol_ocean_alh` | `spexone_mapol_ocean_aerosol_layer_height` | long_name | Aerosol layer height | km |
| `spexone_mapol_ocean_angstrom_440_670` | `spexone_mapol_ocean_aerosol_angstrom_exponent_440_to_670_nm` | long_name | Aerosol Angstrom exponent, 440 to 670 nm  |  |
| `spexone_mapol_ocean_aot` | `spexone_mapol_ocean_aot` | clash | Aerosol optical thickness |  |
| `spexone_mapol_ocean_aot_coarse` | `spexone_mapol_ocean_aot_coarse` | clash | Aerosol optical thickness (coarse mode) |  |
| `spexone_mapol_ocean_aot_fine` | `spexone_mapol_ocean_aot_fine` | clash | Aerosol optical thickness (fine mode) |  |
| `spexone_mapol_ocean_chi2` | `spexone_mapol_ocean_cost_function_value` | long_name | Cost function value |  |
| `spexone_mapol_ocean_chi2_first_guess` | `spexone_mapol_ocean_cost_function_value_of_the_first_guess` | long_name | Cost function value of the first guess |  |
| `spexone_mapol_ocean_chla` | `spexone_mapol_ocean_chlorophyll_a_concentration` | long_name | Chlorophyll-a concentration | mg m^-3 |
| `spexone_mapol_ocean_fmf` | `spexone_mapol_ocean_aerosol_fine_mode_fraction` | long_name | Aerosol fine mode fraction ( aot_fine/aot) |  |
| `spexone_mapol_ocean_fvf` | `spexone_mapol_ocean_aerosol_fine_mode_volume_fraction` | long_name | Aerosol fine mode volume fraction (vd_fine/vd)  |  |
| `spexone_mapol_ocean_intensity_wavelength` | `spexone_mapol_ocean_intensity_field_center_wavelengths_at_each_view` | long_name | Intensity field center wavelengths at each view | nm |
| `spexone_mapol_ocean_mask_dolp` | `spexone_mapol_ocean_adaptive_data_mask_for_do_lp` | long_name | Adaptive data mask for DoLP (0:clear, 1: masked) |  |
| `spexone_mapol_ocean_mask_ref` | `spexone_mapol_ocean_adaptive_data_mask_for_reflectance` | long_name | Adaptive data mask for reflectance (0:clear, 1: masked) |  |
| `spexone_mapol_ocean_mi` | `spexone_mapol_ocean_mi` | clash | Spectral aerosol refractive index (mode average, imaginary p |  |
| `spexone_mapol_ocean_mi_coarse` | `spexone_mapol_ocean_mi_coarse` | clash | Spectral aerosol refractive index (coarse mode, imaginary pa |  |
| `spexone_mapol_ocean_mi_fine` | `spexone_mapol_ocean_mi_fine` | clash | Spectral aerosol refractive index (fine mode, imaginary part |  |
| `spexone_mapol_ocean_mr` | `spexone_mapol_ocean_mr` | clash | Spectral aerosol refractive index (mode average, real part) |  |
| `spexone_mapol_ocean_mr_coarse` | `spexone_mapol_ocean_mr_coarse` | clash | Spectral aerosol refractive index (coarse mode, real part) |  |
| `spexone_mapol_ocean_mr_fine` | `spexone_mapol_ocean_mr_fine` | clash | Spectral aerosol refractive index (fine mode, real part) |  |
| `spexone_mapol_ocean_nfev` | `spexone_mapol_ocean_number_of_forward_calculations` | long_name | Number of forward calculations |  |
| `spexone_mapol_ocean_njev` | `spexone_mapol_ocean_number_of_jacobian_calculations` | long_name | Number of Jacobian calculations |  |
| `spexone_mapol_ocean_nv_dolp` | `spexone_mapol_ocean_number_of_angles_for_do_lp` | long_name | Number of angles for DoLP |  |
| `spexone_mapol_ocean_nv_ref` | `spexone_mapol_ocean_number_of_angles_for_reflectance` | long_name | Number of angles for reflectance |  |
| `spexone_mapol_ocean_ozone` | `spexone_mapol_ocean_ancillary_ozone_column_density` | long_name | Ancillary ozone column density | Dobson |
| `spexone_mapol_ocean_quality_flag` | `spexone_mapol_ocean_quality_flag_0_best_others_with_lower_quality` | long_name | Quality flag, 0: best, others with lower quality |  |
| `spexone_mapol_ocean_reff_coarse` | `spexone_mapol_ocean_reff_coarse` | clash | Aerosol effective radius  (coarse mode) | um |
| `spexone_mapol_ocean_reff_fine` | `spexone_mapol_ocean_reff_fine` | clash | Aerosol effective radius  (fine mode) | um |
| `spexone_mapol_ocean_sensor_view_angle` | `spexone_mapol_ocean_along_track_view_angles_of_sensor` | long_name | Along-track view angles of sensor | degrees |
| `spexone_mapol_ocean_sph` | `spexone_mapol_ocean_sph` | clash | Aerosol spherical fraction (volume density weighted) |  |
| `spexone_mapol_ocean_sph_coarse` | `spexone_mapol_ocean_sph_coarse` | clash | Aerosol spherical fraction (coarse mode) |  |
| `spexone_mapol_ocean_sph_fine` | `spexone_mapol_ocean_sph_fine` | clash | Aerosol spherical fraction (fine mode) |  |
| `spexone_mapol_ocean_ssa` | `spexone_mapol_ocean_ssa` | clash | Aerosol single scattering albedo (total) |  |
| `spexone_mapol_ocean_ssa_coarse` | `spexone_mapol_ocean_ssa_coarse` | clash | Aerosol single scattering albedo (coarse mode) |  |
| `spexone_mapol_ocean_ssa_fine` | `spexone_mapol_ocean_ssa_fine` | clash | Aerosol single scattering albedo (fine mode) |  |
| `spexone_mapol_ocean_surface_pressure` | `spexone_mapol_ocean_ancillary_surface_pressure` | long_name+units | Ancillary surface pressure | mb -> Pa |
| `spexone_mapol_ocean_timing` | `spexone_mapol_ocean_retrieval_time_per_pixel` | long_name | Retrieval time per pixel |  |
| `spexone_mapol_ocean_vd` | `spexone_mapol_ocean_vd` | clash | Aerosol volume density (total) | um^3 um^-2 |
| `spexone_mapol_ocean_vd_coarse` | `spexone_mapol_ocean_vd_coarse` | clash | Aerosol volume density (coarse mode) | um^3 um^-2 |
| `spexone_mapol_ocean_vd_fine` | `spexone_mapol_ocean_vd_fine` | clash | Aerosol volume density (fine mode) | um^3 um^-2 |
| `spexone_mapol_ocean_vd_mode1` | `spexone_mapol_ocean_vd_mode1` | clash | Aerosol volume density (submode 1) | um^3 um^-2 |
| `spexone_mapol_ocean_vd_mode2` | `spexone_mapol_ocean_vd_mode2` | clash | Aerosol volume density (submode 2) | um^3 um^-2 |
| `spexone_mapol_ocean_vd_mode3` | `spexone_mapol_ocean_vd_mode3` | clash | Aerosol volume density (submode 3) | um^3 um^-2 |
| `spexone_mapol_ocean_vd_mode4` | `spexone_mapol_ocean_vd_mode4` | clash | Aerosol volume density (submode 4) | um^3 um^-2 |
| `spexone_mapol_ocean_vd_mode5` | `spexone_mapol_ocean_vd_mode5` | clash | Aerosol volume density (submode 5) | um^3 um^-2 |
| `spexone_mapol_ocean_veff_coarse` | `spexone_mapol_ocean_veff_coarse` | clash | Aerosol effective variance  (coarse mode) |  |
| `spexone_mapol_ocean_veff_fine` | `spexone_mapol_ocean_veff_fine` | clash | Aerosol effective variance  (fine mode) |  |
| `spexone_mapol_ocean_wind_speed` | `spexone_mapol_ocean_ocean_surface_wind_speed` | long_name | Ocean surface wind speed | m/s |
| `spexone_rtap_land_ae_440_670_first_guess` | `spexone_rtap_land_first_guess_of_angstrom_exponent_at_440_670` | long_name | First guess of angstrom exponent at 440_670 |  |
| `spexone_rtap_land_alh` | `spexone_rtap_land_alh` | clash | Aerosol layer height | km |
| `spexone_rtap_land_alh_coarse` | `spexone_rtap_land_alh_coarse` | clash | Aerosol layer height (coarse mode) | km |
| `spexone_rtap_land_alh_fine` | `spexone_rtap_land_alh_fine` | clash | Aerosol layer height (fine mode) | km |
| `spexone_rtap_land_alh_mode1` | `spexone_rtap_land_alh_mode1` | clash | Aerosol layer height (mode 1) | km |
| `spexone_rtap_land_alh_mode1_uncertainty` | `spexone_rtap_land_alh_mode1_uncertainty` | clash | Uncertainty of aerosol layer height (mode 1) | km |
| `spexone_rtap_land_alh_mode2` | `spexone_rtap_land_alh_mode2` | clash | Aerosol layer height (mode 2) | km |
| `spexone_rtap_land_alh_mode2_uncertainty` | `spexone_rtap_land_alh_mode2_uncertainty` | clash | Uncertainty of aerosol layer height (mode 2) | km |
| `spexone_rtap_land_alh_mode3` | `spexone_rtap_land_alh_mode3` | clash | Aerosol layer height (mode 3) | km |
| `spexone_rtap_land_alh_mode3_uncertainty` | `spexone_rtap_land_alh_mode3_uncertainty` | clash | Uncertainty of aerosol layer height (mode 3) | km |
| `spexone_rtap_land_alh_mode4` | `spexone_rtap_land_alh_mode4` | clash | Aerosol layer height (mode 4) | km |
| `spexone_rtap_land_alh_mode4_uncertainty` | `spexone_rtap_land_alh_mode4_uncertainty` | clash | Uncertainty of aerosol layer height (mode 4) | km |
| `spexone_rtap_land_angstrom_440_670` | `spexone_rtap_land_aerosol_angstrom_exponent_440_to_670_nm` | long_name | Aerosol Angstrom exponent, 440 to 670 nm |  |
| `spexone_rtap_land_angstrom_440_870` | `spexone_rtap_land_aerosol_angstrom_exponent_440_to_870_nm` | long_name | Aerosol Angstrom exponent, 440 to 870 nm |  |
| `spexone_rtap_land_angstrom_490_670` | `spexone_rtap_land_aerosol_angstrom_exponent_490_to_670_nm` | long_name | Aerosol Angstrom exponent, 490 to 670 nm |  |
| `spexone_rtap_land_aot` | `spexone_rtap_land_aot` | clash | Aerosol optical thickness |  |
| `spexone_rtap_land_aot550` | `spexone_rtap_land_aerosol_optical_thickness_at_550_nm` | long_name | Aerosol optical thickness at 550 nm |  |
| `spexone_rtap_land_aot550_uncertainty` | `spexone_rtap_land_uncertainty_of_aerosol_optical_thickness_at_550_nm` | long_name | Uncertainty of aerosol optical thickness at 550 nm |  |
| `spexone_rtap_land_aot_coarse` | `spexone_rtap_land_aot_coarse` | clash | Aerosol optical thickness (coarse mode) |  |
| `spexone_rtap_land_aot_coarse_uncertainty` | `spexone_rtap_land_aot_coarse_uncertainty` | clash | Uncertainty of aerosol optical thickness (coarse mode) |  |
| `spexone_rtap_land_aot_fine` | `spexone_rtap_land_aot_fine` | clash | Aerosol optical thickness (fine mode) |  |
| `spexone_rtap_land_aot_fine_uncertainty` | `spexone_rtap_land_aot_fine_uncertainty` | clash | Uncertainty of aerosol optical thickness (fine mode) |  |
| `spexone_rtap_land_aot_mode1` | `spexone_rtap_land_aot_mode1` | clash | Aerosol optical thickness (mode 1) |  |
| `spexone_rtap_land_aot_mode1_uncertainty` | `spexone_rtap_land_aot_mode1_uncertainty` | clash | Uncertainty of aerosol optical thickness (mode 1) |  |
| `spexone_rtap_land_aot_mode2` | `spexone_rtap_land_aot_mode2` | clash | Aerosol optical thickness (mode 2) |  |
| `spexone_rtap_land_aot_mode2_uncertainty` | `spexone_rtap_land_aot_mode2_uncertainty` | clash | Uncertainty of aerosol optical thickness (mode 2) |  |
| `spexone_rtap_land_aot_mode3` | `spexone_rtap_land_aot_mode3` | clash | Aerosol optical thickness (mode 3) |  |
| `spexone_rtap_land_aot_mode3_uncertainty` | `spexone_rtap_land_aot_mode3_uncertainty` | clash | Uncertainty of aerosol optical thickness (mode 3) |  |
| `spexone_rtap_land_aot_mode4` | `spexone_rtap_land_aot_mode4` | clash | Aerosol optical thickness (mode 4) |  |
| `spexone_rtap_land_aot_mode4_uncertainty` | `spexone_rtap_land_aot_mode4_uncertainty` | clash | Uncertainty of aerosol optical thickness (mode 4) |  |
| `spexone_rtap_land_aot_uncertainty` | `spexone_rtap_land_aot_uncertainty` | clash | Uncertainty of aerosol optical thickness |  |
| `spexone_rtap_land_chi2` | `spexone_rtap_land_cost_function_value` | long_name | Cost function value |  |
| `spexone_rtap_land_chi2_first_guess` | `spexone_rtap_land_cost_function_value_of_the_first_guess` | long_name | Cost function value of the first guess |  |
| `spexone_rtap_land_chla` | `spexone_rtap_land_chlorophyll_a_concentration` | long_name | Chlorophyll-a concentration | mg m^-3 |
| `spexone_rtap_land_chla_first_guess` | `spexone_rtap_land_first_guess_of_chlorophyll_a_concentration` | long_name | First guess of Chlorophyll-a concentration | mg m^-3 |
| `spexone_rtap_land_chla_uncertainty` | `spexone_rtap_land_uncertainty_of_chlorophyll_a_concentration` | long_name | Uncertainty of Chlorophyll-a concentration | mg m^-3 |
| `spexone_rtap_land_cloud_fraction` | `spexone_rtap_land_the_cloud_fraction` | long_name | The cloud fraction |  |
| `spexone_rtap_land_cloud_phase` | `spexone_rtap_land_the_cloud_phase` | long_name | The cloud phase |  |
| `spexone_rtap_land_column_number_mode1` | `spexone_rtap_land_column_number_mode1` | clash | Aerosol column number (mode 1) | m^-2 |
| `spexone_rtap_land_column_number_mode1_uncertainty` | `spexone_rtap_land_column_number_mode1_uncertainty` | clash | Uncertainty of aerosol column number (mode 1) | m^-2 |
| `spexone_rtap_land_column_number_mode2` | `spexone_rtap_land_column_number_mode2` | clash | Aerosol column number (mode 2) | m^-2 |
| `spexone_rtap_land_column_number_mode2_uncertainty` | `spexone_rtap_land_column_number_mode2_uncertainty` | clash | Uncertainty of aerosol column number (mode 2) | m^-2 |
| `spexone_rtap_land_column_number_mode3` | `spexone_rtap_land_column_number_mode3` | clash | Aerosol column number (mode 3) | m^-2 |
| `spexone_rtap_land_column_number_mode3_uncertainty` | `spexone_rtap_land_column_number_mode3_uncertainty` | clash | Uncertainty of aerosol column number (mode 3) | m^-2 |
| `spexone_rtap_land_column_number_mode4` | `spexone_rtap_land_column_number_mode4` | clash | Aerosol column number (mode 4) | m^-2 |
| `spexone_rtap_land_column_number_mode4_uncertainty` | `spexone_rtap_land_column_number_mode4_uncertainty` | clash | Uncertainty of aerosol column number (mode 4) | m^-2 |
| `spexone_rtap_land_f_isotropic` | `spexone_rtap_land_surface_isotropic_scaling_parameter_for_each_band` | long_name | Surface isotropic scaling parameter for each band |  |
| `spexone_rtap_land_fmf` | `spexone_rtap_land_aerosol_fine_mode_fraction` | long_name | Aerosol fine mode fraction (aot_fine/aot) |  |
| `spexone_rtap_land_fracday` | `spexone_rtap_land_the_fraction_in_a_day` | long_name | The fraction in a day |  |
| `spexone_rtap_land_fvf` | `spexone_rtap_land_aerosol_fine_mode_volume_fraction` | long_name | Aerosol fine mode volume fraction (vd_fine/vd) |  |
| `spexone_rtap_land_height_std` | `spexone_rtap_land_height_standard_deviation` | long_name | Height standard deviation | meter |
| `spexone_rtap_land_iteration` | `spexone_rtap_land_number_of_iterations` | long_name | Number of iterations |  |
| `spexone_rtap_land_lidar_depol_ratio` | `spexone_rtap_land_aerosol_lidar_depolarization_ratio` | long_name | Diagnostic aerosol lidar depolarization ratio |  |
| `spexone_rtap_land_lidar_p11_pi` | `spexone_rtap_land_phase_function_p11_in_backscatter_direction` | long_name | Phase function p11 in backscatter direction |  |
| `spexone_rtap_land_lidar_ratio` | `spexone_rtap_land_aerosol_lidar_ratio` | long_name | Diagnostic aerosol lidar ratio |  |
| `spexone_rtap_land_mi` | `spexone_rtap_land_mi` | clash | Aerosol refractive index (volume density weighted, imaginary |  |
| `spexone_rtap_land_mi_coarse` | `spexone_rtap_land_mi_coarse` | clash | Aerosol refractive index (coarse mode, imaginary part) |  |
| `spexone_rtap_land_mi_fine` | `spexone_rtap_land_mi_fine` | clash | Aerosol refractive index (fine mode, imaginary part) |  |
| `spexone_rtap_land_mi_mode1` | `spexone_rtap_land_mi_mode1` | clash | Aerosol refractive index (mode 1, imaginary part) |  |
| `spexone_rtap_land_mi_mode1_uncertainty` | `spexone_rtap_land_mi_mode1_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 1, imaginary p |  |
| `spexone_rtap_land_mi_mode2` | `spexone_rtap_land_mi_mode2` | clash | Aerosol refractive index (mode 2, imaginary part) |  |
| `spexone_rtap_land_mi_mode2_uncertainty` | `spexone_rtap_land_mi_mode2_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 2, imaginary p |  |
| `spexone_rtap_land_mi_mode3` | `spexone_rtap_land_mi_mode3` | clash | Aerosol refractive index (mode 3, imaginary part) |  |
| `spexone_rtap_land_mi_mode3_uncertainty` | `spexone_rtap_land_mi_mode3_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 3, imaginary p |  |
| `spexone_rtap_land_mi_mode4` | `spexone_rtap_land_mi_mode4` | clash | Aerosol refractive index (mode 4, imaginary part) |  |
| `spexone_rtap_land_mi_mode4_uncertainty` | `spexone_rtap_land_mi_mode4_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 4, imaginary p |  |
| `spexone_rtap_land_mr` | `spexone_rtap_land_mr` | clash | Aerosol refractive index (volume density weighted, real part |  |
| `spexone_rtap_land_mr_coarse` | `spexone_rtap_land_mr_coarse` | clash | Aerosol refractive index (coarse mode, real part) |  |
| `spexone_rtap_land_mr_fine` | `spexone_rtap_land_mr_fine` | clash | Aerosol refractive index (fine mode, real part) |  |
| `spexone_rtap_land_mr_mode1` | `spexone_rtap_land_mr_mode1` | clash | Aerosol refractive index (mode 1, real part) |  |
| `spexone_rtap_land_mr_mode1_uncertainty` | `spexone_rtap_land_mr_mode1_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 1, real part) |  |
| `spexone_rtap_land_mr_mode2` | `spexone_rtap_land_mr_mode2` | clash | Aerosol refractive index (mode 2, real part) |  |
| `spexone_rtap_land_mr_mode2_uncertainty` | `spexone_rtap_land_mr_mode2_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 2, real part) |  |
| `spexone_rtap_land_mr_mode3` | `spexone_rtap_land_mr_mode3` | clash | Aerosol refractive index (mode 3, real part) |  |
| `spexone_rtap_land_mr_mode3_uncertainty` | `spexone_rtap_land_mr_mode3_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 3, real part) |  |
| `spexone_rtap_land_mr_mode4` | `spexone_rtap_land_mr_mode4` | clash | Aerosol refractive index (mode 4, real part) |  |
| `spexone_rtap_land_mr_mode4_uncertainty` | `spexone_rtap_land_mr_mode4_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 4, real part) |  |
| `spexone_rtap_land_no2` | `spexone_rtap_land_no2_column_density` | long_name | NO2 column density | molecules cm^-2 |
| `spexone_rtap_land_nview` | `spexone_rtap_land_number_of_viewing_angles` | long_name | Number of viewing angles |  |
| `spexone_rtap_land_ozone` | `spexone_rtap_land_ozone_column_density` | long_name | Ozone column density | Dobson |
| `spexone_rtap_land_quality_flag` | `spexone_rtap_land_quality_of_retrieved_pixels` | long_name | Quality of retrieved pixels (0: good) |  |
| `spexone_rtap_land_reff_coarse` | `spexone_rtap_land_reff_coarse` | clash | Aerosol effective radius (coarse mode) | micron |
| `spexone_rtap_land_reff_coarse_first_guess` | `spexone_rtap_land_first_guess_of_coarse_mode_effective_radius` | long_name | First guess of coarse mode effective radius |  |
| `spexone_rtap_land_reff_coarse_uncertainty` | `spexone_rtap_land_reff_coarse_uncertainty` | clash | Uncertainty of aerosol effective radius (coarse mode) | micron |
| `spexone_rtap_land_reff_fine` | `spexone_rtap_land_reff_fine` | clash | Aerosol effective radius (fine mode) | micron |
| `spexone_rtap_land_reff_fine_first_guess` | `spexone_rtap_land_first_guess_of_fine_mode_effective_radius` | long_name | First guess of fine mode effective radius |  |
| `spexone_rtap_land_reff_fine_uncertainty` | `spexone_rtap_land_reff_fine_uncertainty` | clash | Uncertainty of aerosol effective radius (fine mode) | micron |
| `spexone_rtap_land_reff_mode1` | `spexone_rtap_land_reff_mode1` | clash | Aerosol effective radius (mode 1) | micron |
| `spexone_rtap_land_reff_mode1_uncertainty` | `spexone_rtap_land_reff_mode1_uncertainty` | clash | Uncertainty of aerosol effective radius (mode 1) | micron |
| `spexone_rtap_land_reff_mode2` | `spexone_rtap_land_reff_mode2` | clash | Aerosol effective radius (mode 2) | micron |
| `spexone_rtap_land_reff_mode2_uncertainty` | `spexone_rtap_land_reff_mode2_uncertainty` | clash | Uncertainty of aerosol effective radius (mode 2) | micron |
| `spexone_rtap_land_reff_mode3` | `spexone_rtap_land_reff_mode3` | clash | Aerosol effective radius (mode 3) | micron |
| `spexone_rtap_land_reff_mode3_uncertainty` | `spexone_rtap_land_reff_mode3_uncertainty` | clash | Uncertainty of aerosol effective radius (mode 3) | micron |
| `spexone_rtap_land_reff_mode4` | `spexone_rtap_land_reff_mode4` | clash | Aerosol effective radius (mode 4) | micron |
| `spexone_rtap_land_reff_mode4_uncertainty` | `spexone_rtap_land_reff_mode4_uncertainty` | clash | Uncertainty of aerosol effective radius (mode 4) | micron |
| `spexone_rtap_land_refr_base_mi_mode1_compnt1` | `spexone_rtap_land_refr_base_mi_mode1_compnt1` | clash | Refractive index component 1 (mode 1, imaginary part) |  |
| `spexone_rtap_land_refr_base_mi_mode1_compnt2` | `spexone_rtap_land_refr_base_mi_mode1_compnt2` | clash | Refractive index component 2 (mode 1, imaginary part) |  |
| `spexone_rtap_land_refr_base_mi_mode2_compnt1` | `spexone_rtap_land_refr_base_mi_mode2_compnt1` | clash | Refractive index component 1 (mode 2, imaginary part) |  |
| `spexone_rtap_land_refr_base_mi_mode2_compnt2` | `spexone_rtap_land_refr_base_mi_mode2_compnt2` | clash | Refractive index component 2 (mode 2, imaginary part) |  |
| `spexone_rtap_land_refr_base_mi_mode2_compnt3` | `spexone_rtap_land_refr_base_mi_mode2_compnt3` | clash | Refractive index component 3 (mode 2, imaginary part) |  |
| `spexone_rtap_land_refr_base_mi_mode3_compnt1` | `spexone_rtap_land_refr_base_mi_mode3_compnt1` | clash | Refractive index component 1 (mode 3, imaginary part) |  |
| `spexone_rtap_land_refr_base_mi_mode3_compnt2` | `spexone_rtap_land_refr_base_mi_mode3_compnt2` | clash | Refractive index component 2 (mode 3, imaginary part) |  |
| `spexone_rtap_land_refr_base_mi_mode4_compnt1` | `spexone_rtap_land_refr_base_mi_mode4_compnt1` | clash | Refractive index component 1 (mode 4, imaginary part) |  |
| `spexone_rtap_land_refr_base_mr_mode1_compnt1` | `spexone_rtap_land_refr_base_mr_mode1_compnt1` | clash | Refractive index component 1 (mode 1, real part) |  |
| `spexone_rtap_land_refr_base_mr_mode1_compnt2` | `spexone_rtap_land_refr_base_mr_mode1_compnt2` | clash | Refractive index component 2 (mode 1, real part) |  |
| `spexone_rtap_land_refr_base_mr_mode2_compnt1` | `spexone_rtap_land_refr_base_mr_mode2_compnt1` | clash | Refractive index component 1 (mode 2, real part) |  |
| `spexone_rtap_land_refr_base_mr_mode2_compnt2` | `spexone_rtap_land_refr_base_mr_mode2_compnt2` | clash | Refractive index component 2 (mode 2, real part) |  |
| `spexone_rtap_land_refr_base_mr_mode2_compnt3` | `spexone_rtap_land_refr_base_mr_mode2_compnt3` | clash | Refractive index component 3 (mode 2, real part) |  |
| `spexone_rtap_land_refr_base_mr_mode3_compnt1` | `spexone_rtap_land_refr_base_mr_mode3_compnt1` | clash | Refractive index component 1 (mode 3, real part) |  |
| `spexone_rtap_land_refr_base_mr_mode3_compnt2` | `spexone_rtap_land_refr_base_mr_mode3_compnt2` | clash | Refractive index component 2 (mode 3, real part) |  |
| `spexone_rtap_land_refr_base_mr_mode4_compnt1` | `spexone_rtap_land_refr_base_mr_mode4_compnt1` | clash | Refractive index component 1 (mode 4, real part) |  |
| `spexone_rtap_land_sensor_zenith_angle` | `spexone_rtap_land_viewing_zenith_angle` | long_name | Viewing zenith angle | degree |
| `spexone_rtap_land_sph` | `spexone_rtap_land_sph` | clash | Aerosol spherical fraction (volume density weighted) |  |
| `spexone_rtap_land_sph_coarse` | `spexone_rtap_land_sph_coarse` | clash | Aerosol spherical fraction (coarse mode) |  |
| `spexone_rtap_land_sph_fine` | `spexone_rtap_land_sph_fine` | clash | Aerosol spherical fraction (fine mode) |  |
| `spexone_rtap_land_sphere_frac_mode1` | `spexone_rtap_land_sphere_frac_mode1` | clash | Aerosol spherical fraction (mode 1) |  |
| `spexone_rtap_land_sphere_frac_mode1_uncertainty` | `spexone_rtap_land_sphere_frac_mode1_uncertainty` | clash | Uncertainty of aerosol spherical fraction (mode 1) |  |
| `spexone_rtap_land_sphere_frac_mode2` | `spexone_rtap_land_sphere_frac_mode2` | clash | Aerosol spherical fraction (mode 2) |  |
| `spexone_rtap_land_sphere_frac_mode2_uncertainty` | `spexone_rtap_land_sphere_frac_mode2_uncertainty` | clash | Uncertainty of aerosol spherical fraction (mode 2) |  |
| `spexone_rtap_land_sphere_frac_mode3` | `spexone_rtap_land_sphere_frac_mode3` | clash | Aerosol spherical fraction (mode 3) |  |
| `spexone_rtap_land_sphere_frac_mode3_uncertainty` | `spexone_rtap_land_sphere_frac_mode3_uncertainty` | clash | Uncertainty of aerosol spherical fraction (mode 3) |  |
| `spexone_rtap_land_sphere_frac_mode4` | `spexone_rtap_land_sphere_frac_mode4` | clash | Aerosol spherical fraction (mode 4) |  |
| `spexone_rtap_land_sphere_frac_mode4_uncertainty` | `spexone_rtap_land_sphere_frac_mode4_uncertainty` | clash | Uncertainty of aerosol spherical fraction (mode 4) |  |
| `spexone_rtap_land_ssa` | `spexone_rtap_land_ssa` | clash | Aerosol single scattering albedo |  |
| `spexone_rtap_land_ssa_coarse` | `spexone_rtap_land_ssa_coarse` | clash | Aerosol single scattering albedo (coarse mode) |  |
| `spexone_rtap_land_ssa_fine` | `spexone_rtap_land_ssa_fine` | clash | Aerosol single scattering albedo (fine mode) |  |
| `spexone_rtap_land_ssa_mode1` | `spexone_rtap_land_ssa_mode1` | clash | Aerosol single scattering albedo (mode 1) |  |
| `spexone_rtap_land_ssa_mode2` | `spexone_rtap_land_ssa_mode2` | clash | Aerosol single scattering albedo (mode 2) |  |
| `spexone_rtap_land_ssa_mode3` | `spexone_rtap_land_ssa_mode3` | clash | Aerosol single scattering albedo (mode 3) |  |
| `spexone_rtap_land_ssa_mode4` | `spexone_rtap_land_ssa_mode4` | clash | Aerosol single scattering albedo (mode 4) |  |
| `spexone_rtap_land_ssa_uncertainty` | `spexone_rtap_land_uncertainty_of_aerosol_single_scattering_albedo` | long_name | Uncertainty of aerosol single scattering albedo |  |
| `spexone_rtap_land_timing` | `spexone_rtap_land_retrieval_time_per_pixel` | long_name | Retrieval time per pixel | seconds |
| `spexone_rtap_land_vd` | `spexone_rtap_land_vd` | clash | Aerosol volume density (total) | um^3 um^-2 |
| `spexone_rtap_land_vd_bc` | `spexone_rtap_land_component_bc_volume_density` | long_name | Component bc volume density | um^3 um^-2 |
| `spexone_rtap_land_vd_brc` | `spexone_rtap_land_component_brc_volume_density` | long_name | Component brc volume density | um^3 um^-2 |
| `spexone_rtap_land_vd_coarse` | `spexone_rtap_land_vd_coarse` | clash | Aerosol volume density (coarse mode) | um^3 um^-2 |
| `spexone_rtap_land_vd_dust` | `spexone_rtap_land_component_dust_volume_density` | long_name | Component dust volume density | um^3 um^-2 |
| `spexone_rtap_land_vd_fine` | `spexone_rtap_land_vd_fine` | clash | Aerosol volume density (fine mode) | um^3 um^-2 |
| `spexone_rtap_land_vd_mode1` | `spexone_rtap_land_vd_mode1` | clash | Aerosol volume density (mode 1) | um^3 um^-2 |
| `spexone_rtap_land_vd_mode2` | `spexone_rtap_land_vd_mode2` | clash | Aerosol volume density (mode 2) | um^3 um^-2 |
| `spexone_rtap_land_vd_mode3` | `spexone_rtap_land_vd_mode3` | clash | Aerosol volume density (mode 3) | um^3 um^-2 |
| `spexone_rtap_land_vd_mode4` | `spexone_rtap_land_vd_mode4` | clash | Aerosol volume density (mode 4) | um^3 um^-2 |
| `spexone_rtap_land_vd_nonabs_coarse` | `spexone_rtap_land_component_nonabs_coarse_volume_density` | long_name | Component nonabs_coarse volume density | um^3 um^-2 |
| `spexone_rtap_land_vd_nonabs_fine` | `spexone_rtap_land_component_nonabs_fine_volume_density` | long_name | Component nonabs_fine volume density | um^3 um^-2 |
| `spexone_rtap_land_vd_quartz` | `spexone_rtap_land_component_quartz_volume_density` | long_name | Component quartz volume density | um^3 um^-2 |
| `spexone_rtap_land_vd_water_coarse` | `spexone_rtap_land_component_water_coarse_volume_density` | long_name | Component water_coarse volume density | um^3 um^-2 |
| `spexone_rtap_land_vd_water_fine` | `spexone_rtap_land_component_water_fine_volume_density` | long_name | Component water_fine volume density | um^3 um^-2 |
| `spexone_rtap_land_veff_coarse` | `spexone_rtap_land_veff_coarse` | clash | Aerosol effective variance (coarse mode) |  |
| `spexone_rtap_land_veff_fine` | `spexone_rtap_land_veff_fine` | clash | Aerosol effective variance (fine mode) |  |
| `spexone_rtap_land_veff_mode1` | `spexone_rtap_land_veff_mode1` | clash | Aerosol effective variance (mode 1) |  |
| `spexone_rtap_land_veff_mode1_uncertainty` | `spexone_rtap_land_veff_mode1_uncertainty` | clash | Uncertainty of aerosol effective variance (mode 1) |  |
| `spexone_rtap_land_veff_mode2` | `spexone_rtap_land_veff_mode2` | clash | Aerosol effective variance (mode 2) |  |
| `spexone_rtap_land_veff_mode2_uncertainty` | `spexone_rtap_land_veff_mode2_uncertainty` | clash | Uncertainty of aerosol effective variance (mode 2) |  |
| `spexone_rtap_land_veff_mode3` | `spexone_rtap_land_veff_mode3` | clash | Aerosol effective variance (mode 3) |  |
| `spexone_rtap_land_veff_mode3_uncertainty` | `spexone_rtap_land_veff_mode3_uncertainty` | clash | Uncertainty of aerosol effective variance (mode 3) |  |
| `spexone_rtap_land_veff_mode4` | `spexone_rtap_land_veff_mode4` | clash | Aerosol effective variance (mode 4) |  |
| `spexone_rtap_land_veff_mode4_uncertainty` | `spexone_rtap_land_veff_mode4_uncertainty` | clash | Uncertainty of aerosol effective variance (mode 4) |  |
| `spexone_rtap_land_waterfrac_inland` | `spexone_rtap_land_water_fraction_for_pixel_over_land` | long_name | Water fraction for pixel over land |  |
| `spexone_rtap_land_wind_speed_first_guess` | `spexone_rtap_land_first_guess_of_ocean_wind_speed` | long_name | First guess of ocean wind speed | m/s |
| `spexone_rtap_land_wind_speed_uncertainty` | `spexone_rtap_land_uncertainty_of_wind_speed` | long_name | Uncertainty of wind speed | m/s |
| `spexone_rtap_land_windspeed_inland` | `spexone_rtap_land_water_wind_speed_for_pixel_over_land` | long_name | Water wind speed for pixel over land |  |
| `spexone_rtap_ocean_ae_440_670_first_guess` | `spexone_rtap_ocean_first_guess_of_angstrom_exponent_at_440_670` | long_name | First guess of angstrom exponent at 440_670 |  |
| `spexone_rtap_ocean_alh` | `spexone_rtap_ocean_alh` | clash | Aerosol layer height | km |
| `spexone_rtap_ocean_alh_coarse` | `spexone_rtap_ocean_alh_coarse` | clash | Aerosol layer height (coarse mode) | km |
| `spexone_rtap_ocean_alh_fine` | `spexone_rtap_ocean_alh_fine` | clash | Aerosol layer height (fine mode) | km |
| `spexone_rtap_ocean_alh_mode1` | `spexone_rtap_ocean_alh_mode1` | clash | Aerosol layer height (mode 1) | km |
| `spexone_rtap_ocean_alh_mode1_uncertainty` | `spexone_rtap_ocean_alh_mode1_uncertainty` | clash | Uncertainty of aerosol layer height (mode 1) | km |
| `spexone_rtap_ocean_alh_mode2` | `spexone_rtap_ocean_alh_mode2` | clash | Aerosol layer height (mode 2) | km |
| `spexone_rtap_ocean_alh_mode2_uncertainty` | `spexone_rtap_ocean_alh_mode2_uncertainty` | clash | Uncertainty of aerosol layer height (mode 2) | km |
| `spexone_rtap_ocean_alh_mode3` | `spexone_rtap_ocean_alh_mode3` | clash | Aerosol layer height (mode 3) | km |
| `spexone_rtap_ocean_alh_mode3_uncertainty` | `spexone_rtap_ocean_alh_mode3_uncertainty` | clash | Uncertainty of aerosol layer height (mode 3) | km |
| `spexone_rtap_ocean_alh_mode4` | `spexone_rtap_ocean_alh_mode4` | clash | Aerosol layer height (mode 4) | km |
| `spexone_rtap_ocean_alh_mode4_uncertainty` | `spexone_rtap_ocean_alh_mode4_uncertainty` | clash | Uncertainty of aerosol layer height (mode 4) | km |
| `spexone_rtap_ocean_angstrom_440_670` | `spexone_rtap_ocean_aerosol_angstrom_exponent_440_to_670_nm` | long_name | Aerosol Angstrom exponent, 440 to 670 nm |  |
| `spexone_rtap_ocean_angstrom_440_870` | `spexone_rtap_ocean_aerosol_angstrom_exponent_440_to_870_nm` | long_name | Aerosol Angstrom exponent, 440 to 870 nm |  |
| `spexone_rtap_ocean_angstrom_490_670` | `spexone_rtap_ocean_aerosol_angstrom_exponent_490_to_670_nm` | long_name | Aerosol Angstrom exponent, 490 to 670 nm |  |
| `spexone_rtap_ocean_aot` | `spexone_rtap_ocean_aot` | clash | Aerosol optical thickness |  |
| `spexone_rtap_ocean_aot550` | `spexone_rtap_ocean_aerosol_optical_thickness_at_550_nm` | long_name | Aerosol optical thickness at 550 nm |  |
| `spexone_rtap_ocean_aot550_uncertainty` | `spexone_rtap_ocean_uncertainty_of_aerosol_optical_thickness_at_550_nm` | long_name | Uncertainty of aerosol optical thickness at 550 nm |  |
| `spexone_rtap_ocean_aot_coarse` | `spexone_rtap_ocean_aot_coarse` | clash | Aerosol optical thickness (coarse mode) |  |
| `spexone_rtap_ocean_aot_coarse_uncertainty` | `spexone_rtap_ocean_aot_coarse_uncertainty` | clash | Uncertainty of aerosol optical thickness (coarse mode) |  |
| `spexone_rtap_ocean_aot_fine` | `spexone_rtap_ocean_aot_fine` | clash | Aerosol optical thickness (fine mode) |  |
| `spexone_rtap_ocean_aot_fine_uncertainty` | `spexone_rtap_ocean_aot_fine_uncertainty` | clash | Uncertainty of aerosol optical thickness (fine mode) |  |
| `spexone_rtap_ocean_aot_mode1` | `spexone_rtap_ocean_aot_mode1` | clash | Aerosol optical thickness (mode 1) |  |
| `spexone_rtap_ocean_aot_mode1_uncertainty` | `spexone_rtap_ocean_aot_mode1_uncertainty` | clash | Uncertainty of aerosol optical thickness (mode 1) |  |
| `spexone_rtap_ocean_aot_mode2` | `spexone_rtap_ocean_aot_mode2` | clash | Aerosol optical thickness (mode 2) |  |
| `spexone_rtap_ocean_aot_mode2_uncertainty` | `spexone_rtap_ocean_aot_mode2_uncertainty` | clash | Uncertainty of aerosol optical thickness (mode 2) |  |
| `spexone_rtap_ocean_aot_mode3` | `spexone_rtap_ocean_aot_mode3` | clash | Aerosol optical thickness (mode 3) |  |
| `spexone_rtap_ocean_aot_mode3_uncertainty` | `spexone_rtap_ocean_aot_mode3_uncertainty` | clash | Uncertainty of aerosol optical thickness (mode 3) |  |
| `spexone_rtap_ocean_aot_mode4` | `spexone_rtap_ocean_aot_mode4` | clash | Aerosol optical thickness (mode 4) |  |
| `spexone_rtap_ocean_aot_mode4_uncertainty` | `spexone_rtap_ocean_aot_mode4_uncertainty` | clash | Uncertainty of aerosol optical thickness (mode 4) |  |
| `spexone_rtap_ocean_aot_uncertainty` | `spexone_rtap_ocean_aot_uncertainty` | clash | Uncertainty of aerosol optical thickness |  |
| `spexone_rtap_ocean_chi2` | `spexone_rtap_ocean_cost_function_value` | long_name | Cost function value |  |
| `spexone_rtap_ocean_chi2_first_guess` | `spexone_rtap_ocean_cost_function_value_of_the_first_guess` | long_name | Cost function value of the first guess |  |
| `spexone_rtap_ocean_chla` | `spexone_rtap_ocean_chlorophyll_a_concentration` | long_name | Chlorophyll-a concentration | mg m^-3 |
| `spexone_rtap_ocean_chla_first_guess` | `spexone_rtap_ocean_first_guess_of_chlorophyll_a_concentration` | long_name | First guess of Chlorophyll-a concentration | mg m^-3 |
| `spexone_rtap_ocean_chla_uncertainty` | `spexone_rtap_ocean_uncertainty_of_chlorophyll_a_concentration` | long_name | Uncertainty of Chlorophyll-a concentration | mg m^-3 |
| `spexone_rtap_ocean_cloud_fraction` | `spexone_rtap_ocean_the_cloud_fraction` | long_name | The cloud fraction |  |
| `spexone_rtap_ocean_cloud_phase` | `spexone_rtap_ocean_the_cloud_phase` | long_name | The cloud phase |  |
| `spexone_rtap_ocean_column_number_mode1` | `spexone_rtap_ocean_column_number_mode1` | clash | Aerosol column number (mode 1) | m^-2 |
| `spexone_rtap_ocean_column_number_mode1_uncertainty` | `spexone_rtap_ocean_column_number_mode1_uncertainty` | clash | Uncertainty of aerosol column number (mode 1) | m^-2 |
| `spexone_rtap_ocean_column_number_mode2` | `spexone_rtap_ocean_column_number_mode2` | clash | Aerosol column number (mode 2) | m^-2 |
| `spexone_rtap_ocean_column_number_mode2_uncertainty` | `spexone_rtap_ocean_column_number_mode2_uncertainty` | clash | Uncertainty of aerosol column number (mode 2) | m^-2 |
| `spexone_rtap_ocean_column_number_mode3` | `spexone_rtap_ocean_column_number_mode3` | clash | Aerosol column number (mode 3) | m^-2 |
| `spexone_rtap_ocean_column_number_mode3_uncertainty` | `spexone_rtap_ocean_column_number_mode3_uncertainty` | clash | Uncertainty of aerosol column number (mode 3) | m^-2 |
| `spexone_rtap_ocean_column_number_mode4` | `spexone_rtap_ocean_column_number_mode4` | clash | Aerosol column number (mode 4) | m^-2 |
| `spexone_rtap_ocean_column_number_mode4_uncertainty` | `spexone_rtap_ocean_column_number_mode4_uncertainty` | clash | Uncertainty of aerosol column number (mode 4) | m^-2 |
| `spexone_rtap_ocean_f_isotropic` | `spexone_rtap_ocean_surface_isotropic_scaling_parameter_for_each_band` | long_name | Surface isotropic scaling parameter for each band |  |
| `spexone_rtap_ocean_fmf` | `spexone_rtap_ocean_aerosol_fine_mode_fraction` | long_name | Aerosol fine mode fraction (aot_fine/aot) |  |
| `spexone_rtap_ocean_fracday` | `spexone_rtap_ocean_the_fraction_in_a_day` | long_name | The fraction in a day |  |
| `spexone_rtap_ocean_fvf` | `spexone_rtap_ocean_aerosol_fine_mode_volume_fraction` | long_name | Aerosol fine mode volume fraction (vd_fine/vd) |  |
| `spexone_rtap_ocean_height_std` | `spexone_rtap_ocean_height_standard_deviation` | long_name | Height standard deviation | meter |
| `spexone_rtap_ocean_iteration` | `spexone_rtap_ocean_number_of_iterations` | long_name | Number of iterations |  |
| `spexone_rtap_ocean_lidar_depol_ratio` | `spexone_rtap_ocean_aerosol_lidar_depolarization_ratio` | long_name | Diagnostic aerosol lidar depolarization ratio |  |
| `spexone_rtap_ocean_lidar_p11_pi` | `spexone_rtap_ocean_phase_function_p11_in_backscatter_direction` | long_name | Phase function p11 in backscatter direction |  |
| `spexone_rtap_ocean_lidar_ratio` | `spexone_rtap_ocean_aerosol_lidar_ratio` | long_name | Diagnostic aerosol lidar ratio |  |
| `spexone_rtap_ocean_mi` | `spexone_rtap_ocean_mi` | clash | Aerosol refractive index (volume density weighted, imaginary |  |
| `spexone_rtap_ocean_mi_coarse` | `spexone_rtap_ocean_mi_coarse` | clash | Aerosol refractive index (coarse mode, imaginary part) |  |
| `spexone_rtap_ocean_mi_fine` | `spexone_rtap_ocean_mi_fine` | clash | Aerosol refractive index (fine mode, imaginary part) |  |
| `spexone_rtap_ocean_mi_mode1` | `spexone_rtap_ocean_mi_mode1` | clash | Aerosol refractive index (mode 1, imaginary part) |  |
| `spexone_rtap_ocean_mi_mode1_uncertainty` | `spexone_rtap_ocean_mi_mode1_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 1, imaginary p |  |
| `spexone_rtap_ocean_mi_mode2` | `spexone_rtap_ocean_mi_mode2` | clash | Aerosol refractive index (mode 2, imaginary part) |  |
| `spexone_rtap_ocean_mi_mode2_uncertainty` | `spexone_rtap_ocean_mi_mode2_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 2, imaginary p |  |
| `spexone_rtap_ocean_mi_mode3` | `spexone_rtap_ocean_mi_mode3` | clash | Aerosol refractive index (mode 3, imaginary part) |  |
| `spexone_rtap_ocean_mi_mode3_uncertainty` | `spexone_rtap_ocean_mi_mode3_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 3, imaginary p |  |
| `spexone_rtap_ocean_mi_mode4` | `spexone_rtap_ocean_mi_mode4` | clash | Aerosol refractive index (mode 4, imaginary part) |  |
| `spexone_rtap_ocean_mi_mode4_uncertainty` | `spexone_rtap_ocean_mi_mode4_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 4, imaginary p |  |
| `spexone_rtap_ocean_mr` | `spexone_rtap_ocean_mr` | clash | Aerosol refractive index (volume density weighted, real part |  |
| `spexone_rtap_ocean_mr_coarse` | `spexone_rtap_ocean_mr_coarse` | clash | Aerosol refractive index (coarse mode, real part) |  |
| `spexone_rtap_ocean_mr_fine` | `spexone_rtap_ocean_mr_fine` | clash | Aerosol refractive index (fine mode, real part) |  |
| `spexone_rtap_ocean_mr_mode1` | `spexone_rtap_ocean_mr_mode1` | clash | Aerosol refractive index (mode 1, real part) |  |
| `spexone_rtap_ocean_mr_mode1_uncertainty` | `spexone_rtap_ocean_mr_mode1_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 1, real part) |  |
| `spexone_rtap_ocean_mr_mode2` | `spexone_rtap_ocean_mr_mode2` | clash | Aerosol refractive index (mode 2, real part) |  |
| `spexone_rtap_ocean_mr_mode2_uncertainty` | `spexone_rtap_ocean_mr_mode2_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 2, real part) |  |
| `spexone_rtap_ocean_mr_mode3` | `spexone_rtap_ocean_mr_mode3` | clash | Aerosol refractive index (mode 3, real part) |  |
| `spexone_rtap_ocean_mr_mode3_uncertainty` | `spexone_rtap_ocean_mr_mode3_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 3, real part) |  |
| `spexone_rtap_ocean_mr_mode4` | `spexone_rtap_ocean_mr_mode4` | clash | Aerosol refractive index (mode 4, real part) |  |
| `spexone_rtap_ocean_mr_mode4_uncertainty` | `spexone_rtap_ocean_mr_mode4_uncertainty` | clash | Uncertainty of aerosol refractive index (mode 4, real part) |  |
| `spexone_rtap_ocean_no2` | `spexone_rtap_ocean_no2_column_density` | long_name | NO2 column density | molecules cm^-2 |
| `spexone_rtap_ocean_nview` | `spexone_rtap_ocean_number_of_viewing_angles` | long_name | Number of viewing angles |  |
| `spexone_rtap_ocean_ozone` | `spexone_rtap_ocean_ozone_column_density` | long_name | Ozone column density | Dobson |
| `spexone_rtap_ocean_quality_flag` | `spexone_rtap_ocean_quality_of_retrieved_pixels` | long_name | Quality of retrieved pixels (0: good) |  |
| `spexone_rtap_ocean_reff_coarse` | `spexone_rtap_ocean_reff_coarse` | clash | Aerosol effective radius (coarse mode) | micron |
| `spexone_rtap_ocean_reff_coarse_first_guess` | `spexone_rtap_ocean_first_guess_of_coarse_mode_effective_radius` | long_name | First guess of coarse mode effective radius |  |
| `spexone_rtap_ocean_reff_coarse_uncertainty` | `spexone_rtap_ocean_reff_coarse_uncertainty` | clash | Uncertainty of aerosol effective radius (coarse mode) | micron |
| `spexone_rtap_ocean_reff_fine` | `spexone_rtap_ocean_reff_fine` | clash | Aerosol effective radius (fine mode) | micron |
| `spexone_rtap_ocean_reff_fine_first_guess` | `spexone_rtap_ocean_first_guess_of_fine_mode_effective_radius` | long_name | First guess of fine mode effective radius |  |
| `spexone_rtap_ocean_reff_fine_uncertainty` | `spexone_rtap_ocean_reff_fine_uncertainty` | clash | Uncertainty of aerosol effective radius (fine mode) | micron |
| `spexone_rtap_ocean_reff_mode1` | `spexone_rtap_ocean_reff_mode1` | clash | Aerosol effective radius (mode 1) | micron |
| `spexone_rtap_ocean_reff_mode1_uncertainty` | `spexone_rtap_ocean_reff_mode1_uncertainty` | clash | Uncertainty of aerosol effective radius (mode 1) | micron |
| `spexone_rtap_ocean_reff_mode2` | `spexone_rtap_ocean_reff_mode2` | clash | Aerosol effective radius (mode 2) | micron |
| `spexone_rtap_ocean_reff_mode2_uncertainty` | `spexone_rtap_ocean_reff_mode2_uncertainty` | clash | Uncertainty of aerosol effective radius (mode 2) | micron |
| `spexone_rtap_ocean_reff_mode3` | `spexone_rtap_ocean_reff_mode3` | clash | Aerosol effective radius (mode 3) | micron |
| `spexone_rtap_ocean_reff_mode3_uncertainty` | `spexone_rtap_ocean_reff_mode3_uncertainty` | clash | Uncertainty of aerosol effective radius (mode 3) | micron |
| `spexone_rtap_ocean_reff_mode4` | `spexone_rtap_ocean_reff_mode4` | clash | Aerosol effective radius (mode 4) | micron |
| `spexone_rtap_ocean_reff_mode4_uncertainty` | `spexone_rtap_ocean_reff_mode4_uncertainty` | clash | Uncertainty of aerosol effective radius (mode 4) | micron |
| `spexone_rtap_ocean_refr_base_mi_mode1_compnt1` | `spexone_rtap_ocean_refr_base_mi_mode1_compnt1` | clash | Refractive index component 1 (mode 1, imaginary part) |  |
| `spexone_rtap_ocean_refr_base_mi_mode1_compnt2` | `spexone_rtap_ocean_refr_base_mi_mode1_compnt2` | clash | Refractive index component 2 (mode 1, imaginary part) |  |
| `spexone_rtap_ocean_refr_base_mi_mode2_compnt1` | `spexone_rtap_ocean_refr_base_mi_mode2_compnt1` | clash | Refractive index component 1 (mode 2, imaginary part) |  |
| `spexone_rtap_ocean_refr_base_mi_mode2_compnt2` | `spexone_rtap_ocean_refr_base_mi_mode2_compnt2` | clash | Refractive index component 2 (mode 2, imaginary part) |  |
| `spexone_rtap_ocean_refr_base_mi_mode2_compnt3` | `spexone_rtap_ocean_refr_base_mi_mode2_compnt3` | clash | Refractive index component 3 (mode 2, imaginary part) |  |
| `spexone_rtap_ocean_refr_base_mi_mode3_compnt1` | `spexone_rtap_ocean_refr_base_mi_mode3_compnt1` | clash | Refractive index component 1 (mode 3, imaginary part) |  |
| `spexone_rtap_ocean_refr_base_mi_mode3_compnt2` | `spexone_rtap_ocean_refr_base_mi_mode3_compnt2` | clash | Refractive index component 2 (mode 3, imaginary part) |  |
| `spexone_rtap_ocean_refr_base_mi_mode4_compnt1` | `spexone_rtap_ocean_refr_base_mi_mode4_compnt1` | clash | Refractive index component 1 (mode 4, imaginary part) |  |
| `spexone_rtap_ocean_refr_base_mr_mode1_compnt1` | `spexone_rtap_ocean_refr_base_mr_mode1_compnt1` | clash | Refractive index component 1 (mode 1, real part) |  |
| `spexone_rtap_ocean_refr_base_mr_mode1_compnt2` | `spexone_rtap_ocean_refr_base_mr_mode1_compnt2` | clash | Refractive index component 2 (mode 1, real part) |  |
| `spexone_rtap_ocean_refr_base_mr_mode2_compnt1` | `spexone_rtap_ocean_refr_base_mr_mode2_compnt1` | clash | Refractive index component 1 (mode 2, real part) |  |
| `spexone_rtap_ocean_refr_base_mr_mode2_compnt2` | `spexone_rtap_ocean_refr_base_mr_mode2_compnt2` | clash | Refractive index component 2 (mode 2, real part) |  |
| `spexone_rtap_ocean_refr_base_mr_mode2_compnt3` | `spexone_rtap_ocean_refr_base_mr_mode2_compnt3` | clash | Refractive index component 3 (mode 2, real part) |  |
| `spexone_rtap_ocean_refr_base_mr_mode3_compnt1` | `spexone_rtap_ocean_refr_base_mr_mode3_compnt1` | clash | Refractive index component 1 (mode 3, real part) |  |
| `spexone_rtap_ocean_refr_base_mr_mode3_compnt2` | `spexone_rtap_ocean_refr_base_mr_mode3_compnt2` | clash | Refractive index component 2 (mode 3, real part) |  |
| `spexone_rtap_ocean_refr_base_mr_mode4_compnt1` | `spexone_rtap_ocean_refr_base_mr_mode4_compnt1` | clash | Refractive index component 1 (mode 4, real part) |  |
| `spexone_rtap_ocean_sensor_zenith_angle` | `spexone_rtap_ocean_viewing_zenith_angle` | long_name | Viewing zenith angle | degree |
| `spexone_rtap_ocean_sph` | `spexone_rtap_ocean_sph` | clash | Aerosol spherical fraction (volume density weighted) |  |
| `spexone_rtap_ocean_sph_coarse` | `spexone_rtap_ocean_sph_coarse` | clash | Aerosol spherical fraction (coarse mode) |  |
| `spexone_rtap_ocean_sph_fine` | `spexone_rtap_ocean_sph_fine` | clash | Aerosol spherical fraction (fine mode) |  |
| `spexone_rtap_ocean_sphere_frac_mode1` | `spexone_rtap_ocean_sphere_frac_mode1` | clash | Aerosol spherical fraction (mode 1) |  |
| `spexone_rtap_ocean_sphere_frac_mode1_uncertainty` | `spexone_rtap_ocean_sphere_frac_mode1_uncertainty` | clash | Uncertainty of aerosol spherical fraction (mode 1) |  |
| `spexone_rtap_ocean_sphere_frac_mode2` | `spexone_rtap_ocean_sphere_frac_mode2` | clash | Aerosol spherical fraction (mode 2) |  |
| `spexone_rtap_ocean_sphere_frac_mode2_uncertainty` | `spexone_rtap_ocean_sphere_frac_mode2_uncertainty` | clash | Uncertainty of aerosol spherical fraction (mode 2) |  |
| `spexone_rtap_ocean_sphere_frac_mode3` | `spexone_rtap_ocean_sphere_frac_mode3` | clash | Aerosol spherical fraction (mode 3) |  |
| `spexone_rtap_ocean_sphere_frac_mode3_uncertainty` | `spexone_rtap_ocean_sphere_frac_mode3_uncertainty` | clash | Uncertainty of aerosol spherical fraction (mode 3) |  |
| `spexone_rtap_ocean_sphere_frac_mode4` | `spexone_rtap_ocean_sphere_frac_mode4` | clash | Aerosol spherical fraction (mode 4) |  |
| `spexone_rtap_ocean_sphere_frac_mode4_uncertainty` | `spexone_rtap_ocean_sphere_frac_mode4_uncertainty` | clash | Uncertainty of aerosol spherical fraction (mode 4) |  |
| `spexone_rtap_ocean_ssa` | `spexone_rtap_ocean_ssa` | clash | Aerosol single scattering albedo |  |
| `spexone_rtap_ocean_ssa_coarse` | `spexone_rtap_ocean_ssa_coarse` | clash | Aerosol single scattering albedo (coarse mode) |  |
| `spexone_rtap_ocean_ssa_fine` | `spexone_rtap_ocean_ssa_fine` | clash | Aerosol single scattering albedo (fine mode) |  |
| `spexone_rtap_ocean_ssa_mode1` | `spexone_rtap_ocean_ssa_mode1` | clash | Aerosol single scattering albedo (mode 1) |  |
| `spexone_rtap_ocean_ssa_mode2` | `spexone_rtap_ocean_ssa_mode2` | clash | Aerosol single scattering albedo (mode 2) |  |
| `spexone_rtap_ocean_ssa_mode3` | `spexone_rtap_ocean_ssa_mode3` | clash | Aerosol single scattering albedo (mode 3) |  |
| `spexone_rtap_ocean_ssa_mode4` | `spexone_rtap_ocean_ssa_mode4` | clash | Aerosol single scattering albedo (mode 4) |  |
| `spexone_rtap_ocean_ssa_uncertainty` | `spexone_rtap_ocean_uncertainty_of_aerosol_single_scattering_albedo` | long_name | Uncertainty of aerosol single scattering albedo |  |
| `spexone_rtap_ocean_timing` | `spexone_rtap_ocean_retrieval_time_per_pixel` | long_name | Retrieval time per pixel | seconds |
| `spexone_rtap_ocean_vd` | `spexone_rtap_ocean_vd` | clash | Aerosol volume density (total) | um^3 um^-2 |
| `spexone_rtap_ocean_vd_bc` | `spexone_rtap_ocean_component_bc_volume_density` | long_name | Component bc volume density | um^3 um^-2 |
| `spexone_rtap_ocean_vd_brc` | `spexone_rtap_ocean_component_brc_volume_density` | long_name | Component brc volume density | um^3 um^-2 |
| `spexone_rtap_ocean_vd_coarse` | `spexone_rtap_ocean_vd_coarse` | clash | Aerosol volume density (coarse mode) | um^3 um^-2 |
| `spexone_rtap_ocean_vd_dust` | `spexone_rtap_ocean_component_dust_volume_density` | long_name | Component dust volume density | um^3 um^-2 |
| `spexone_rtap_ocean_vd_fine` | `spexone_rtap_ocean_vd_fine` | clash | Aerosol volume density (fine mode) | um^3 um^-2 |
| `spexone_rtap_ocean_vd_mode1` | `spexone_rtap_ocean_vd_mode1` | clash | Aerosol volume density (mode 1) | um^3 um^-2 |
| `spexone_rtap_ocean_vd_mode2` | `spexone_rtap_ocean_vd_mode2` | clash | Aerosol volume density (mode 2) | um^3 um^-2 |
| `spexone_rtap_ocean_vd_mode3` | `spexone_rtap_ocean_vd_mode3` | clash | Aerosol volume density (mode 3) | um^3 um^-2 |
| `spexone_rtap_ocean_vd_mode4` | `spexone_rtap_ocean_vd_mode4` | clash | Aerosol volume density (mode 4) | um^3 um^-2 |
| `spexone_rtap_ocean_vd_nonabs_coarse` | `spexone_rtap_ocean_component_nonabs_coarse_volume_density` | long_name | Component nonabs_coarse volume density | um^3 um^-2 |
| `spexone_rtap_ocean_vd_nonabs_fine` | `spexone_rtap_ocean_component_nonabs_fine_volume_density` | long_name | Component nonabs_fine volume density | um^3 um^-2 |
| `spexone_rtap_ocean_vd_quartz` | `spexone_rtap_ocean_component_quartz_volume_density` | long_name | Component quartz volume density | um^3 um^-2 |
| `spexone_rtap_ocean_vd_water_coarse` | `spexone_rtap_ocean_component_water_coarse_volume_density` | long_name | Component water_coarse volume density | um^3 um^-2 |
| `spexone_rtap_ocean_vd_water_fine` | `spexone_rtap_ocean_component_water_fine_volume_density` | long_name | Component water_fine volume density | um^3 um^-2 |
| `spexone_rtap_ocean_veff_coarse` | `spexone_rtap_ocean_veff_coarse` | clash | Aerosol effective variance (coarse mode) |  |
| `spexone_rtap_ocean_veff_fine` | `spexone_rtap_ocean_veff_fine` | clash | Aerosol effective variance (fine mode) |  |
| `spexone_rtap_ocean_veff_mode1` | `spexone_rtap_ocean_veff_mode1` | clash | Aerosol effective variance (mode 1) |  |
| `spexone_rtap_ocean_veff_mode1_uncertainty` | `spexone_rtap_ocean_veff_mode1_uncertainty` | clash | Uncertainty of aerosol effective variance (mode 1) |  |
| `spexone_rtap_ocean_veff_mode2` | `spexone_rtap_ocean_veff_mode2` | clash | Aerosol effective variance (mode 2) |  |
| `spexone_rtap_ocean_veff_mode2_uncertainty` | `spexone_rtap_ocean_veff_mode2_uncertainty` | clash | Uncertainty of aerosol effective variance (mode 2) |  |
| `spexone_rtap_ocean_veff_mode3` | `spexone_rtap_ocean_veff_mode3` | clash | Aerosol effective variance (mode 3) |  |
| `spexone_rtap_ocean_veff_mode3_uncertainty` | `spexone_rtap_ocean_veff_mode3_uncertainty` | clash | Uncertainty of aerosol effective variance (mode 3) |  |
| `spexone_rtap_ocean_veff_mode4` | `spexone_rtap_ocean_veff_mode4` | clash | Aerosol effective variance (mode 4) |  |
| `spexone_rtap_ocean_veff_mode4_uncertainty` | `spexone_rtap_ocean_veff_mode4_uncertainty` | clash | Uncertainty of aerosol effective variance (mode 4) |  |
| `spexone_rtap_ocean_waterfrac_inland` | `spexone_rtap_ocean_water_fraction_for_pixel_over_land` | long_name | Water fraction for pixel over land |  |
| `spexone_rtap_ocean_wind_speed_first_guess` | `spexone_rtap_ocean_first_guess_of_ocean_wind_speed` | long_name | First guess of ocean wind speed | m/s |
| `spexone_rtap_ocean_wind_speed_uncertainty` | `spexone_rtap_ocean_uncertainty_of_wind_speed` | long_name | Uncertainty of wind speed | m/s |
| `spexone_rtap_ocean_windspeed_inland` | `spexone_rtap_ocean_water_wind_speed_for_pixel_over_land` | long_name | Water wind speed for pixel over land |  |

## bkr/polar/pace_virtual (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `harp2_l1b_attitude_angle` | `harp2_l1b_attitude_angles` | long_name | Attitude angles (roll, pitch, yaw) | degrees |
| `harp2_l1b_attitude_quaternion` | `harp2_l1b_spacecraft_quaternion` | long_name | Spacecraft quaternion (J2000 to spacecraft) | unitless |
| `harp2_l1b_dolp` | `harp2_l1b_degree_of_linear_polarization` | long_name | Degree of Linear Polarization | 1 |
| `harp2_l1b_frame_id` | `harp2_l1b_frame_index_number` | long_name | Frame index number |  |
| `harp2_l1b_i` | `harp2_l1b_calibrated_total_intensity` | long_name | Calibrated total intensity | W m^-2 sr^-1 um^-1 |
| `harp2_l1b_intensity_bandpass` | `harp2_l1b_intensity_field_bandpass_at_each_view` | long_name | Intensity field bandpass at each view | nm |
| `harp2_l1b_intensity_f0` | `harp2_l1b_intensity_band_solar_irradiance` | long_name | Intensity band solar irradiance | W m^-2 um^-1 |
| `harp2_l1b_intensity_wavelength` | `harp2_l1b_intensity_field_center_wavelengths_at_each_view` | long_name | Intensity field center wavelengths at each view | nm |
| `harp2_l1b_jd` | `harp2_l1b_julian_day` | long_name | Julian Day |  |
| `harp2_l1b_lines_per_frame` | `harp2_l1b_number_of_lines_per_image_frame` | long_name | Number of lines per image frame |  |
| `harp2_l1b_orbit_altitude` | `harp2_l1b_spacecraft_altitude_amsl` | long_name | Spacecraft altitude amsl. | m |
| `harp2_l1b_orbit_latitude` | `harp2_l1b_spacecraft_latitude` | long_name | Spacecraft latitude | degree N |
| `harp2_l1b_orbit_longitude` | `harp2_l1b_spacecraft_longitude` | long_name | Spacecraft longitude | degree E |
| `harp2_l1b_orbit_position` | `harp2_l1b_spacecraft_position_vector_in_ecef_frame` | long_name | Spacecraft position vector in ECEF frame | m |
| `harp2_l1b_orbit_velocity` | `harp2_l1b_spacecraft_velocity_vector_in_ecef_frame` | long_name | Spacecraft velocity vector in ECEF frame | m/s |
| `harp2_l1b_q` | `harp2_l1b_calibrated_polarized_intensity_q_in_view_merdian_plane` | long_name | Calibrated polarized intensity Q in view-merdian plane | W m^-2 sr^-1 um^-1 |
| `harp2_l1b_qflag` | `harp2_l1b_quality_indicator` | long_name | Quality indicator | none |
| `harp2_l1b_rows` | `harp2_l1b_rows` | kept | granule length along swath_lines; rows past 1294 are not ref |  |
| `harp2_l1b_seconds_of_day` | `harp2_l1b_elapsed_seconds_of_the_day` | long_name | Elapsed seconds of the day |  |
| `harp2_l1b_sensor_azimuth_angle` | `harp2_l1b_sensor_azimuth_to_north` | long_name | Sensor azimuth to north | degrees |
| `harp2_l1b_sensor_view_angle` | `harp2_l1b_along_track_view_angles_for_sensor` | long_name | Along-track view angles for sensor | degrees |
| `harp2_l1b_solar_azimuth_angle` | `harp2_l1b_solar_azimuth_to_north` | long_name | Solar azimuth to north | degrees |
| `harp2_l1b_surface_altitude` | `harp2_l1b_surface_altitude_above_earth_ellipsoid` | long_name | Surface altitude above Earth ellipsoid | meters |
| `harp2_l1b_u` | `harp2_l1b_calibrated_polarized_intensity_u_in_view_merdian_plane` | long_name | Calibrated polarized intensity U in view-merdian plane | W m^-2 sr^-1 um^-1 |
| `harp2_l1b_view_iflag` | `harp2_l1b_intensity_quality_flag_for_each_view` | long_name | Intensity quality flag for each view | 1=good; 0=bad |
| `harp2_l1b_view_pflag` | `harp2_l1b_polarization_quality_flag_for_each_view` | long_name | Polarization quality flag for each view | 1=good; 0=bad |
| `harp2_l1b_view_polmatrix` | `harp2_l1b_polarization_characterization_matrix_of_each_view` | long_name | Polarization characterization matrix of each view | none |
| `harp2_l1c_aolp` | `harp2_l1c_angle_of_linear_polarization` | long_name | Angle of linear polarization | degrees |
| `harp2_l1c_aolp_stdev` | `harp2_l1c_standard_deviation_of_aolp_in_bin` | long_name | Standard deviation of AOLP in bin | degrees |
| `harp2_l1c_dolp` | `harp2_l1c_degree_of_linear_polarization` | long_name | Degree of Linear Polarization | 1 |
| `harp2_l1c_dolp_stdev` | `harp2_l1c_standard_deviation_of_dolp_in_bin` | long_name | Standard deviation of DOLP in bin | 1 |
| `harp2_l1c_height_stdev` | `harp2_l1c_standard_deviation_of_terrain_altitude_within_bin` | long_name | Standard deviation of terrain altitude within bin | meters |
| `harp2_l1c_i` | `harp2_l1c_i_stokes_vector_component` | long_name | I Stokes vector component | W m^-2 sr^-1 um^-1 |
| `harp2_l1c_i_stdev` | `harp2_l1c_standard_deviation_of_i_in_bin` | long_name | Standard deviation of I in bin | W m^-2 sr^-1 um^-1 |
| `harp2_l1c_intensity_bandpass` | `harp2_l1c_intensity_field_bandpass_at_each_view` | long_name | Intensity field bandpass at each view | nm |
| `harp2_l1c_intensity_f0` | `harp2_l1c_intensity_band_solar_irradiance` | long_name | Intensity band solar irradiance | W m^-2 um^-1 |
| `harp2_l1c_intensity_wavelength` | `harp2_l1c_intensity_field_center_wavelengths_at_each_view` | long_name | Intensity field center wavelengths at each view | nm |
| `harp2_l1c_nadir_view_time` | `harp2_l1c_bin_was_viewed_at_nadir_view` | long_name | Time bin was viewed at nadir view | seconds from UTC midnight |
| `harp2_l1c_polarization_bandpass` | `harp2_l1c_polarization_field_bandpasses_at_each_view` | long_name | Polarization field bandpasses at each view | nm |
| `harp2_l1c_polarization_f0` | `harp2_l1c_polarization_band_solar_irradiance` | long_name | Polarization band solar irradiance | W m^-2 um^-1 |
| `harp2_l1c_polarization_wavelength` | `harp2_l1c_polarization_field_center_wavelengths_at_each_view` | long_name | Polarization field center wavelengths at each view | nm |
| `harp2_l1c_q` | `harp2_l1c_q_stokes_vector_component` | long_name | Q Stokes vector component | W m^-2 sr^-1 um^-1 |
| `harp2_l1c_q_stdev` | `harp2_l1c_standard_deviation_of_q_in_bin` | long_name | Standard deviation of Q in bin | W m^-2 sr^-1 um^-1 |
| `harp2_l1c_qc` | `harp2_l1c_specific_cloud_liquid_water_content` | override | quality indicator |  |
| `harp2_l1c_rows` | `harp2_l1c_rows` | kept | granule length along bins_along_track; rows past 395 are not |  |
| `harp2_l1c_scattering_angle` | `harp2_l1c_scattering_angle_at_bin_locations` | long_name | Scattering angle at bin locations | degrees |
| `harp2_l1c_sensor_azimuth_angle` | `harp2_l1c_view_azimuth_angle_at_bin_locations` | long_name | View azimuth angle at bin locations | degrees |
| `harp2_l1c_sensor_view_angle` | `harp2_l1c_along_track_view_angles_for_sensor` | long_name | Along-track view angles for sensor | degrees |
| `harp2_l1c_sensor_zenith_angle` | `harp2_l1c_sensor_zenith_angle_at_bin_locations` | long_name | Sensor zenith angle at bin locations | degrees |
| `harp2_l1c_solar_azimuth_angle` | `harp2_l1c_solar_azimuth_angle_at_bin_locations` | long_name | Solar azimuth angle at bin locations | degrees |
| `harp2_l1c_solar_zenith_angle` | `harp2_l1c_solar_zenith_angle_at_bin_locations` | long_name | Solar zenith angle at bin locations | degrees |
| `harp2_l1c_u` | `harp2_l1c_u_stokes_vector_component` | long_name | U Stokes vector component | W m^-2 sr^-1 um^-1 |
| `harp2_l1c_u_stdev` | `harp2_l1c_standard_deviation_of_u_in_bin` | long_name | Standard deviation of U in bin | W m^-2 sr^-1 um^-1 |
| `harp2_l1c_view_time_offsets` | `harp2_l1c_offsets_of_views_from_nadir_view` | long_name | Time offsets of views from nadir view | seconds |
| `harp2_mapol_land_aerosol_depol_ratio` | `harp2_mapol_land_aerosol_depolarization_ratio` | long_name |  Aerosol depolarization ratio |  |
| `harp2_mapol_land_alh` | `harp2_mapol_land_aerosol_layer_height` | long_name | Aerosol layer height | km |
| `harp2_mapol_land_angstrom_440_670` | `harp2_mapol_land_aerosol_angstrom_exponent_440_to_670_nm` | long_name | Aerosol Angstrom exponent, 440 to 670 nm  |  |
| `harp2_mapol_land_angstrom_440_870` | `harp2_mapol_land_aerosol_angstrom_exponent_440_to_870_nm` | long_name | Aerosol Angstrom exponent, 440 to 870 nm  |  |
| `harp2_mapol_land_aot` | `harp2_mapol_land_aot` | clash | Aerosol optical thickness |  |
| `harp2_mapol_land_aot_coarse` | `harp2_mapol_land_aot_coarse` | clash | Aerosol optical thickness (coarse mode) |  |
| `harp2_mapol_land_aot_fine` | `harp2_mapol_land_aot_fine` | clash | Aerosol optical thickness (fine mode) |  |
| `harp2_mapol_land_chi2` | `harp2_mapol_land_cost_function_value` | long_name | Cost function value |  |
| `harp2_mapol_land_chi2_first_guess` | `harp2_mapol_land_cost_function_value_of_the_first_guess` | long_name | Cost function value of the first guess |  |
| `harp2_mapol_land_fmf` | `harp2_mapol_land_aerosol_fine_mode_fraction` | long_name | Aerosol fine mode fraction ( aot_fine/aot) |  |
| `harp2_mapol_land_fvf` | `harp2_mapol_land_aerosol_fine_mode_volume_fraction` | long_name | Aerosol fine mode volume fraction (vd_fine/vd)  |  |
| `harp2_mapol_land_intensity_wavelength` | `harp2_mapol_land_intensity_field_center_wavelengths_at_each_view` | long_name | Intensity field center wavelengths at each view | nm |
| `harp2_mapol_land_land_fiso` | `harp2_mapol_land_land_surface_isotropic_scaling_parameter_for_each_band` | long_name | Land surface isotropic scaling parameter for each band |  |
| `harp2_mapol_land_land_ndvi` | `harp2_mapol_land_land_normalized_difference_vegetation_index_based_on_rhos` | long_name | Land normalized difference vegetation index based on rhos |  |
| `harp2_mapol_land_mask_dolp` | `harp2_mapol_land_adaptive_data_mask_for_do_lp` | long_name | Adaptive data mask for DoLP (0:clear, 1: masked) |  |
| `harp2_mapol_land_mask_ref` | `harp2_mapol_land_adaptive_data_mask_for_reflectance` | long_name | Adaptive data mask for reflectance (0:clear, 1: masked) |  |
| `harp2_mapol_land_mi` | `harp2_mapol_land_mi` | clash | Spectral aerosol refractive index (mode average, imaginary p |  |
| `harp2_mapol_land_mi_coarse` | `harp2_mapol_land_mi_coarse` | clash | Spectral aerosol refractive index (coarse mode, imaginary pa |  |
| `harp2_mapol_land_mi_fine` | `harp2_mapol_land_mi_fine` | clash | Spectral aerosol refractive index (fine mode, imaginary part |  |
| `harp2_mapol_land_mr` | `harp2_mapol_land_mr` | clash | Spectral aerosol refractive index (mode average, real part) |  |
| `harp2_mapol_land_mr_coarse` | `harp2_mapol_land_mr_coarse` | clash | Spectral aerosol refractive index (coarse mode, real part) |  |
| `harp2_mapol_land_mr_fine` | `harp2_mapol_land_mr_fine` | clash | Spectral aerosol refractive index (fine mode, real part) |  |
| `harp2_mapol_land_nfev` | `harp2_mapol_land_number_of_forward_calculations` | long_name | Number of forward calculations |  |
| `harp2_mapol_land_njev` | `harp2_mapol_land_number_of_jacobian_calculations` | long_name | Number of Jacobian calculations |  |
| `harp2_mapol_land_nv_dolp` | `harp2_mapol_land_number_of_angles_for_do_lp` | long_name | Number of angles for DoLP |  |
| `harp2_mapol_land_nv_ref` | `harp2_mapol_land_number_of_angles_for_reflectance` | long_name | Number of angles for reflectance |  |
| `harp2_mapol_land_ozone` | `harp2_mapol_land_ancillary_ozone_column_density` | long_name | Ancillary ozone column density | Dobson |
| `harp2_mapol_land_quality_flag` | `harp2_mapol_land_quality_flag_0_best_others_with_lower_quality` | long_name | Quality flag, 0: best, others with lower quality |  |
| `harp2_mapol_land_reff_coarse` | `harp2_mapol_land_reff_coarse` | clash | Aerosol effective radius  (coarse mode) | um |
| `harp2_mapol_land_reff_fine` | `harp2_mapol_land_reff_fine` | clash | Aerosol effective radius  (fine mode) | um |
| `harp2_mapol_land_rhos_angular` | `harp2_mapol_land_angular_land_surface_reflectance` | long_name | Angular land surface reflectance (before BRDF correction)  | 1 |
| `harp2_mapol_land_rhos_angular_mean` | `harp2_mapol_land_angular_mean_of_rhos_angular` | long_name | Angular mean of rhos_angular  |  |
| `harp2_mapol_land_rhos_angular_std` | `harp2_mapol_land_angular_standard_deviation_of_rhos_angular` | long_name | Angular standard deviation of rhos_angular |  |
| `harp2_mapol_land_rhos_nadir` | `harp2_mapol_land_nadir_adjusted_angular_land_surface_reflectance` | long_name | Nadir adjusted angular land surface reflectance (after BRDF  |  |
| `harp2_mapol_land_rhos_nadir_mean` | `harp2_mapol_land_angular_mean_of_rhos_nadir` | long_name | Angular mean of rhos_nadir |  |
| `harp2_mapol_land_rhos_nadir_std` | `harp2_mapol_land_angular_standard_deviation_of_rhos_nadir` | long_name | Angular standard deviation of rhos_nadir |  |
| `harp2_mapol_land_rows` | `harp2_mapol_land_rows` | kept | granule length along number_of_lines; rows past 395 are not  |  |
| `harp2_mapol_land_sensor_view_angle` | `harp2_mapol_land_along_track_view_angles_of_sensor` | long_name | Along-track view angles of sensor | degrees |
| `harp2_mapol_land_sph` | `harp2_mapol_land_sph` | clash | Aerosol spherical fraction (volume density weighted) |  |
| `harp2_mapol_land_sph_coarse` | `harp2_mapol_land_sph_coarse` | clash | Aerosol spherical fraction (coarse mode) |  |
| `harp2_mapol_land_sph_fine` | `harp2_mapol_land_sph_fine` | clash | Aerosol spherical fraction (fine mode) |  |
| `harp2_mapol_land_ssa` | `harp2_mapol_land_ssa` | clash | Aerosol single scattering albedo (total) |  |
| `harp2_mapol_land_ssa_coarse` | `harp2_mapol_land_ssa_coarse` | clash | Aerosol single scattering albedo (coarse mode) |  |
| `harp2_mapol_land_ssa_fine` | `harp2_mapol_land_ssa_fine` | clash | Aerosol single scattering albedo (fine mode) |  |
| `harp2_mapol_land_surface_pressure` | `harp2_mapol_land_ancillary_surface_pressure` | long_name+units | Ancillary surface pressure | mb -> Pa |
| `harp2_mapol_land_timing` | `harp2_mapol_land_retrieval_time_per_pixel` | long_name | Retrieval time per pixel |  |
| `harp2_mapol_land_vd` | `harp2_mapol_land_vd` | clash | Aerosol volume density (total) | um^3 um^-2 |
| `harp2_mapol_land_vd_coarse` | `harp2_mapol_land_vd_coarse` | clash | Aerosol volume density (coarse mode) | um^3 um^-2 |
| `harp2_mapol_land_vd_fine` | `harp2_mapol_land_vd_fine` | clash | Aerosol volume density (fine mode) | um^3 um^-2 |
| `harp2_mapol_land_vd_mode1` | `harp2_mapol_land_vd_mode1` | clash | Aerosol volume density (submode 1) | um^3 um^-2 |
| `harp2_mapol_land_vd_mode2` | `harp2_mapol_land_vd_mode2` | clash | Aerosol volume density (submode 2) | um^3 um^-2 |
| `harp2_mapol_land_vd_mode3` | `harp2_mapol_land_vd_mode3` | clash | Aerosol volume density (submode 3) | um^3 um^-2 |
| `harp2_mapol_land_vd_mode4` | `harp2_mapol_land_vd_mode4` | clash | Aerosol volume density (submode 4) | um^3 um^-2 |
| `harp2_mapol_land_vd_mode5` | `harp2_mapol_land_vd_mode5` | clash | Aerosol volume density (submode 5) | um^3 um^-2 |
| `harp2_mapol_land_veff_coarse` | `harp2_mapol_land_veff_coarse` | clash | Aerosol effective variance  (coarse mode) |  |
| `harp2_mapol_land_veff_fine` | `harp2_mapol_land_veff_fine` | clash | Aerosol effective variance  (fine mode) |  |
| `harp2_mapol_ocean_Rrs_angular` | `harp2_mapol_ocean_angular_remote_sensing_reflectance` | long_name | Angular remote sensing reflectance (before BRDF correction)  | sr^-1 |
| `harp2_mapol_ocean_Rrs_angular_mean` | `harp2_mapol_ocean_angular_mean_of_rrs_angular` | long_name | Angular mean of Rrs_angular  | sr^-1 |
| `harp2_mapol_ocean_Rrs_angular_std` | `harp2_mapol_ocean_angular_standard_deviation_of_rrs_angular` | long_name | Angular standard deviation of Rrs_angular | sr^-1 |
| `harp2_mapol_ocean_Rrs_nadir` | `harp2_mapol_ocean_nadir_adjusted_angular_remote_sensing_reflectance` | long_name | Nadir adjusted angular remote sensing reflectance (after BRD | sr^-1 |
| `harp2_mapol_ocean_Rrs_nadir_mean` | `harp2_mapol_ocean_angular_mean_of_rrs_nadir` | long_name | Angular mean of Rrs_nadir | sr^-1 |
| `harp2_mapol_ocean_Rrs_nadir_std` | `harp2_mapol_ocean_angular_standard_deviation_of_rrs_nadir` | long_name | Angular standard deviation of Rrs_nadir | sr^-1 |
| `harp2_mapol_ocean_aerosol_depol_ratio` | `harp2_mapol_ocean_aerosol_depolarization_ratio` | long_name |  Aerosol depolarization ratio |  |
| `harp2_mapol_ocean_alh` | `harp2_mapol_ocean_aerosol_layer_height` | long_name | Aerosol layer height | km |
| `harp2_mapol_ocean_angstrom_440_670` | `harp2_mapol_ocean_aerosol_angstrom_exponent_440_to_670_nm` | long_name | Aerosol Angstrom exponent, 440 to 670 nm  |  |
| `harp2_mapol_ocean_angstrom_440_870` | `harp2_mapol_ocean_aerosol_angstrom_exponent_440_to_870_nm` | long_name | Aerosol Angstrom exponent, 440 to 870 nm  |  |
| `harp2_mapol_ocean_aot` | `harp2_mapol_ocean_aot` | clash | Aerosol optical thickness |  |
| `harp2_mapol_ocean_aot_coarse` | `harp2_mapol_ocean_aot_coarse` | clash | Aerosol optical thickness (coarse mode) |  |
| `harp2_mapol_ocean_aot_fine` | `harp2_mapol_ocean_aot_fine` | clash | Aerosol optical thickness (fine mode) |  |
| `harp2_mapol_ocean_chi2` | `harp2_mapol_ocean_cost_function_value` | long_name | Cost function value |  |
| `harp2_mapol_ocean_chi2_first_guess` | `harp2_mapol_ocean_cost_function_value_of_the_first_guess` | long_name | Cost function value of the first guess |  |
| `harp2_mapol_ocean_chla` | `harp2_mapol_ocean_chlorophyll_a_concentration` | long_name | Chlorophyll-a concentration | mg m^-3 |
| `harp2_mapol_ocean_fmf` | `harp2_mapol_ocean_aerosol_fine_mode_fraction` | long_name | Aerosol fine mode fraction ( aot_fine/aot) |  |
| `harp2_mapol_ocean_fvf` | `harp2_mapol_ocean_aerosol_fine_mode_volume_fraction` | long_name | Aerosol fine mode volume fraction (vd_fine/vd)  |  |
| `harp2_mapol_ocean_intensity_wavelength` | `harp2_mapol_ocean_intensity_field_center_wavelengths_at_each_view` | long_name | Intensity field center wavelengths at each view | nm |
| `harp2_mapol_ocean_mask_dolp` | `harp2_mapol_ocean_adaptive_data_mask_for_do_lp` | long_name | Adaptive data mask for DoLP (0:clear, 1: masked) |  |
| `harp2_mapol_ocean_mask_ref` | `harp2_mapol_ocean_adaptive_data_mask_for_reflectance` | long_name | Adaptive data mask for reflectance (0:clear, 1: masked) |  |
| `harp2_mapol_ocean_mi` | `harp2_mapol_ocean_mi` | clash | Spectral aerosol refractive index (mode average, imaginary p |  |
| `harp2_mapol_ocean_mi_coarse` | `harp2_mapol_ocean_mi_coarse` | clash | Spectral aerosol refractive index (coarse mode, imaginary pa |  |
| `harp2_mapol_ocean_mi_fine` | `harp2_mapol_ocean_mi_fine` | clash | Spectral aerosol refractive index (fine mode, imaginary part |  |
| `harp2_mapol_ocean_mr` | `harp2_mapol_ocean_mr` | clash | Spectral aerosol refractive index (mode average, real part) |  |
| `harp2_mapol_ocean_mr_coarse` | `harp2_mapol_ocean_mr_coarse` | clash | Spectral aerosol refractive index (coarse mode, real part) |  |
| `harp2_mapol_ocean_mr_fine` | `harp2_mapol_ocean_mr_fine` | clash | Spectral aerosol refractive index (fine mode, real part) |  |
| `harp2_mapol_ocean_nfev` | `harp2_mapol_ocean_number_of_forward_calculations` | long_name | Number of forward calculations |  |
| `harp2_mapol_ocean_njev` | `harp2_mapol_ocean_number_of_jacobian_calculations` | long_name | Number of Jacobian calculations |  |
| `harp2_mapol_ocean_nv_dolp` | `harp2_mapol_ocean_number_of_angles_for_do_lp` | long_name | Number of angles for DoLP |  |
| `harp2_mapol_ocean_nv_ref` | `harp2_mapol_ocean_number_of_angles_for_reflectance` | long_name | Number of angles for reflectance |  |
| `harp2_mapol_ocean_ozone` | `harp2_mapol_ocean_ancillary_ozone_column_density` | long_name | Ancillary ozone column density | Dobson |
| `harp2_mapol_ocean_quality_flag` | `harp2_mapol_ocean_quality_flag_0_best_others_with_lower_quality` | long_name | Quality flag, 0: best, others with lower quality |  |
| `harp2_mapol_ocean_reff_coarse` | `harp2_mapol_ocean_reff_coarse` | clash | Aerosol effective radius  (coarse mode) | um |
| `harp2_mapol_ocean_reff_fine` | `harp2_mapol_ocean_reff_fine` | clash | Aerosol effective radius  (fine mode) | um |
| `harp2_mapol_ocean_rows` | `harp2_mapol_ocean_rows` | kept | granule length along number_of_lines; rows past 395 are not  |  |
| `harp2_mapol_ocean_sensor_view_angle` | `harp2_mapol_ocean_along_track_view_angles_of_sensor` | long_name | Along-track view angles of sensor | degrees |
| `harp2_mapol_ocean_sph` | `harp2_mapol_ocean_sph` | clash | Aerosol spherical fraction (volume density weighted) |  |
| `harp2_mapol_ocean_sph_coarse` | `harp2_mapol_ocean_sph_coarse` | clash | Aerosol spherical fraction (coarse mode) |  |
| `harp2_mapol_ocean_sph_fine` | `harp2_mapol_ocean_sph_fine` | clash | Aerosol spherical fraction (fine mode) |  |
| `harp2_mapol_ocean_ssa` | `harp2_mapol_ocean_ssa` | clash | Aerosol single scattering albedo (total) |  |
| `harp2_mapol_ocean_ssa_coarse` | `harp2_mapol_ocean_ssa_coarse` | clash | Aerosol single scattering albedo (coarse mode) |  |
| `harp2_mapol_ocean_ssa_fine` | `harp2_mapol_ocean_ssa_fine` | clash | Aerosol single scattering albedo (fine mode) |  |
| `harp2_mapol_ocean_surface_pressure` | `harp2_mapol_ocean_ancillary_surface_pressure` | long_name+units | Ancillary surface pressure | mb -> Pa |
| `harp2_mapol_ocean_timing` | `harp2_mapol_ocean_retrieval_time_per_pixel` | long_name | Retrieval time per pixel |  |
| `harp2_mapol_ocean_vd` | `harp2_mapol_ocean_vd` | clash | Aerosol volume density (total) | um^3 um^-2 |
| `harp2_mapol_ocean_vd_coarse` | `harp2_mapol_ocean_vd_coarse` | clash | Aerosol volume density (coarse mode) | um^3 um^-2 |
| `harp2_mapol_ocean_vd_fine` | `harp2_mapol_ocean_vd_fine` | clash | Aerosol volume density (fine mode) | um^3 um^-2 |
| `harp2_mapol_ocean_vd_mode1` | `harp2_mapol_ocean_vd_mode1` | clash | Aerosol volume density (submode 1) | um^3 um^-2 |
| `harp2_mapol_ocean_vd_mode2` | `harp2_mapol_ocean_vd_mode2` | clash | Aerosol volume density (submode 2) | um^3 um^-2 |
| `harp2_mapol_ocean_vd_mode3` | `harp2_mapol_ocean_vd_mode3` | clash | Aerosol volume density (submode 3) | um^3 um^-2 |
| `harp2_mapol_ocean_vd_mode4` | `harp2_mapol_ocean_vd_mode4` | clash | Aerosol volume density (submode 4) | um^3 um^-2 |
| `harp2_mapol_ocean_vd_mode5` | `harp2_mapol_ocean_vd_mode5` | clash | Aerosol volume density (submode 5) | um^3 um^-2 |
| `harp2_mapol_ocean_veff_coarse` | `harp2_mapol_ocean_veff_coarse` | clash | Aerosol effective variance  (coarse mode) |  |
| `harp2_mapol_ocean_veff_fine` | `harp2_mapol_ocean_veff_fine` | clash | Aerosol effective variance  (fine mode) |  |
| `harp2_mapol_ocean_wind_speed` | `harp2_mapol_ocean_ocean_surface_wind_speed` | long_name | Ocean surface wind speed | m/s |
| `spexone_l1b_aolp` | `spexone_l1b_angle_of_linear_polarization` | long_name | Angle of linear polarization | degrees |
| `spexone_l1b_aolp_stdev` | `spexone_l1b_standard_deviation_of_aolp_in_bin` | long_name | Standard deviation of AOLP in bin | degrees |
| `spexone_l1b_att_quat` | `spexone_l1b_attitude_quaternions` | long_name | Attitude quaternions (J2000 to spacecraft) | seconds |
| `spexone_l1b_dolp` | `spexone_l1b_degree_of_linear_polarization` | long_name | Degree of linear polarization | 1 |
| `spexone_l1b_dolp_stdev` | `spexone_l1b_standard_deviation_of_dolp_in_bin` | long_name | Standard deviation of DOLP in bin | 1 |
| `spexone_l1b_i` | `spexone_l1b_i_stokes_vector_component` | long_name | I Stokes vector component | W m^-2 sr^-1 um^-1 |
| `spexone_l1b_i_polsample` | `spexone_l1b_i_stokes_vector_component_at_polarimeter_spectral_sampling` | long_name | I Stokes vector component at polarimeter spectral sampling | W m^-2 sr^-1 um^-1 |
| `spexone_l1b_i_polsample_stdev` | `spexone_l1b_standard_deviation_of_i_polsample_in_bin` | long_name | standard deviation of i_polsample in bin | W m^-2 sr^-1 um^-1 |
| `spexone_l1b_i_stdev` | `spexone_l1b_standard_deviation_of_i_in_bin` | long_name | Standard deviation of I in bin | W m^-2 sr^-1 um^-1 |
| `spexone_l1b_icu_time_sec` | `spexone_l1b_icu_time_sec` | clash | ICU time stamp (seconds) | seconds since 1958-01-01 00:00:00 TAI |
| `spexone_l1b_icu_time_subsec` | `spexone_l1b_icu_time_subsec` | clash | ICU time stamp (sub-seconds) | 1/65536 s |
| `spexone_l1b_intensity_bandpass` | `spexone_l1b_intensity_field_bandpass_at_each_view` | long_name | Intensity field bandpass at each view | nm |
| `spexone_l1b_intensity_f0` | `spexone_l1b_intensity_band_solar_irradiance` | long_name | Intensity band solar irradiance | W m^-2 um^-1 |
| `spexone_l1b_intensity_wavelength` | `spexone_l1b_intensity_field_center_wavelengths_at_each_view` | long_name | Intensity field center wavelengths at each view | nm |
| `spexone_l1b_orb_pos` | `spexone_l1b_orbit_position_vectors` | long_name | Orbit position vectors (ECR) | meters |
| `spexone_l1b_orb_vel` | `spexone_l1b_orbit_velocity_vectors` | long_name | Orbit velocity vectors (ECR) | meters/second |
| `spexone_l1b_polarization_bandpass` | `spexone_l1b_polarization_field_bandpasses_at_each_view` | long_name | Polarization field bandpasses at each view | nm |
| `spexone_l1b_polarization_f0` | `spexone_l1b_polarization_band_solar_irradiance` | long_name | Polarization band solar irradiance | W m^-2 um^-1 |
| `spexone_l1b_polarization_wavelength` | `spexone_l1b_polarization_field_wavelengths_at_each_view` | long_name | Polarization field wavelengths at each view | nm |
| `spexone_l1b_q_over_i` | `spexone_l1b_q_over_i_stokes_vector_component` | long_name | Q over I (little q) Stokes vector component | 1 |
| `spexone_l1b_q_over_i_stdev` | `spexone_l1b_standard_deviation_of_q_over_i_in_bin` | long_name | Standard deviation of q_over_i in bin | 1 |
| `spexone_l1b_qc` | `spexone_l1b_specific_cloud_liquid_water_content` | override | Quality indicator | 1 |
| `spexone_l1b_rows` | `spexone_l1b_rows` | kept | granule length along bins_along_track; rows past 8140 are no |  |
| `spexone_l1b_sensor_azimuth` | `spexone_l1b_sensor_azimuth_angle` | long_name | Sensor azimuth angle | degrees |
| `spexone_l1b_sensor_zenith` | `spexone_l1b_sensor_zenith_angle` | long_name | Sensor zenith angle | degrees |
| `spexone_l1b_solar_azimuth` | `spexone_l1b_solar_azimuth_angle_at_bin_locations` | long_name | Solar azimuth angle at bin locations | degrees |
| `spexone_l1b_solar_zenith` | `spexone_l1b_solar_zenith_angle_at_bin_locations` | long_name | Solar zenith angle at bin locations | degrees |
| `spexone_l1b_u_over_i` | `spexone_l1b_u_over_i_stokes_vector_component` | long_name | U over I (little u) Stokes vector component | 1 |
| `spexone_l1b_u_over_i_stdev` | `spexone_l1b_standard_deviation_of_u_over_i_in_bin` | long_name | Standard deviation of u_over_i in bin | 1 |

## bkr/precipradar/norway_radar (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `block_percent` | `geometrical_blocking` | long_name | Geometrical blocking | percent |
| `classification` | `precipitation_phase` | long_name | Precipitation phase | flag: 3=snow, 2=sleet, 1=rain, 0=None |
| `clutter_probability` | `probability_of_clutter` | long_name | Probability of clutter | percent |
| `equivalent_reflectivity_factor` | `radar_reflectivity` | long_name | Radar Reflectivity | dBZ |
| `is_blocked` | `blocked_data` | long_name | Blocked data | flag: 1=on, 0=off |
| `is_convective` | `convective_stratiform` | long_name | Convective / Stratiform | flag: 1=convective, 0=stratiform |
| `is_groundclutter` | `ground_clutter` | long_name | Ground clutter | flag: 1=on, 0=off |
| `is_highele` | `high_elevation` | long_name | High elevation | flag: 1=on, 0=off |
| `is_lowele` | `low_elevation` | long_name | Low elevation | flag: 1=on, 0=off |
| `is_nodata` | `no_data` | long_name | No data | flag: 1=on, 0=off |
| `is_otherclutter` | `other_clutter` | long_name | Other clutter | flag: 1=on, 0=off |
| `is_seaclutter` | `sea_clutter` | long_name | Sea clutter | flag: 1=on, 0=off |
| `lwe_precipitation_rate` | `radar_precipitation_rate` | long_name | Radar Precipitation Rate | mm/h |

## bkr/precipradar/opera_dbz (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `dbz` | `base_reflectivity` | eccodes |  |  |

## bkr/precipradar/uk_radar (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `rainfall_rate` | `radar_rainfall_rate_composite` | long_name | radar rainfall rate composite | mm h-1 |

## bkr/silam-aerosol/silam_global_surface_aerosol (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `CO` | `carbon_monoxide` | override | Volume mixing ratio CO_gas | ug/m3 (EU) |
| `NO` | `volume_mixing_ratio_no_gas` | long_name | Volume mixing ratio NO_gas | ug/m3 (EU) |
| `NO2` | `nitrogen_dioxide` | override | Volume mixing ratio NO2_gas | ug/m3 (EU) |
| `O3` | `ozone` | override | Volume mixing ratio O3_gas | ug/m3 (EU) |
| `PM10` | `concentration_in_air_pm10` | long_name | Concentration in air PM10 | ug/m3 |
| `PM25` | `concentration_in_air_pm2_5` | long_name | Concentration in air PM2_5 | ug/m3 |
| `SO2` | `sulphur_dioxide` | override | Volume mixing ratio SO2_gas | ug/m3 (EU) |
| `airdens` | `air_density_kg_m3` | long_name | Air density [kg/m3] | kg/m3 |

## bkr/silam-dust/silam_aerosol (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `CO` | `carbon_monoxide` | override | Concentration in air CO_gas | ug/m3 |
| `NO` | `concentration_in_air_no_gas` | long_name | Concentration in air NO_gas | ug/m3 |
| `NO2` | `nitrogen_dioxide` | override | Concentration in air NO2_gas | ug/m3 |
| `O3` | `ozone` | override | Concentration in air O3_gas | ug/m3 |
| `PM10` | `concentration_in_air_pm10` | long_name | Concentration in air PM10 | ug/m3 |
| `PM25` | `concentration_in_air_pm2_5` | long_name | Concentration in air PM2_5 | ug/m3 |
| `SO2` | `sulphur_dioxide` | override | Concentration in air SO2_gas | ug/m3 |
| `airdens` | `air_density_kg_m3` | long_name | Air density [kg/m3] | kg/m3 |

## bkr/weatherreal/WeatherReal (1 store(s))

| old | new | rule | long_name | units |
|---|---|---|---|---|
| `c` | `cloud_cover` | abbreviation |  |  |
| `msl` | `mean_sea_level_pressure` | abbreviation |  |  |
| `ra1` | `precipitation_1h` | abbreviation |  |  |
| `ra12` | `precipitation_12h` | abbreviation |  |  |
| `ra24` | `precipitation_24h` | abbreviation |  |  |
| `ra3` | `precipitation_3h` | abbreviation |  |  |
| `ra6` | `precipitation_6h` | abbreviation |  |  |
| `sp` | `surface_pressure` | abbreviation |  |  |
| `td` | `dewpoint_temperature` | abbreviation |  |  |
| `wd` | `wind_direction` | abbreviation |  |  |
| `ws` | `wind_speed` | abbreviation |  |  |

