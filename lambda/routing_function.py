from weather_router import isochronal_weather_router, polar, point_validity, visualize
import numpy as np
import pandas as pd
import json
import xarray as xr
import fsspec
import zarr
import dask
import hvplot
from bokeh.resources import INLINE

def get_weather_data(event):
    year = event['year']
    month = event['month']
    day = event['day']
    step =  event['step']
    max_days =  event['max_days']
    extent = event['extent']
    u10 = xr.open_zarr(f's3://era5-pds/zarr/{year}/{str(month).zfill(2)}/data/eastward_wind_at_10_metres.zarr')
    if day+max_days > 30:
        if not month == 12:
            u10_2 = xr.open_zarr(f's3://era5-pds/zarr/{year}/{str(month+1).zfill(2)}/data/eastward_wind_at_10_metres.zarr')
        else:
            u10_2 = xr.open_zarr(f's3://era5-pds/zarr/{year+1}/{str(1).zfill(2)}/data/eastward_wind_at_10_metres.zarr')
        u10 = xr.concat([u10, u10_2], dim = 'time0')
    v10 = xr.open_zarr(f's3://era5-pds/zarr/{year}/{str(month).zfill(2)}/data/northward_wind_at_10_metres.zarr')
    if day+max_days > 30:
        if not month == 12:
            v10_2 = xr.open_zarr(f's3://era5-pds/zarr/{year}/{str(month+1).zfill(2)}/data/northward_wind_at_10_metres.zarr')
        else:  
            v10_2 = xr.open_zarr(f's3://era5-pds/zarr/{year+1}/{str(month+1).zfill(2)}/data/northward_wind_at_10_metres.zarr')
        v10 = xr.concat([v10, v10_2], dim = 'time0')
    ds = xr.merge([v10,u10])
    ds.coords['lon'] = ((ds.coords['lon'] + 180) % 360) - 180
    ds = ds.sortby(ds.lon)
    ds = ds.rename({'time0':'time'})
    lat1,lon1,lat2,lon2 = extent
    ds = ds.sel(lat = slice(max([lat1, lat2]),min([lat1, lat2]))).sel(lon = slice(min([lon1, lon2]),max([lon1, lon2])))
    ds = ds.sel(time = ds.time.values[:max_days*24:step])
    u10 = ds.eastward_wind_at_10_metres
    v10 = ds.northward_wind_at_10_metres
    tws = np.sqrt(v10**2 + u10**2)
    tws = tws*1.94384
    twd = np.mod(180+np.rad2deg(np.arctan2(u10, v10)),360)
    ds = tws.to_dataset(name = 'tws')
    ds['twd'] = twd
    ds['u10'] = u10*1.94384
    ds['v10'] = v10*1.94384
    ds['wind_angle'] = np.deg2rad((270 - (ds.twd)) % 360)
    del ds.lat.attrs['units']
    ds.lon.attrs['long_name'] = 'longitude'
    ds = ds.interpolate_na(dim = 'time0', method = 'linear')
    return ds.load()



def handler(event, context):
    object_key = event["Records"][0]["s3"]["object"]["key"]
    fs = fsspec.filesystem('s3')
    print(f"s3://lambdaroutingstack-weatherroutebucket7b183c04-pyxlno4db9s1/{object_key}")
    with fs.open(f"s3://lambdaroutingstack-weatherroutebucket7b183c04-pyxlno4db9s1/{object_key}", 'rb') as f:
        input_data = json.load(f)
    ds = get_weather_data(input_data)
    def getWindAt(t, lat, lon):
        tws_sel = ds.tws.sel(time = t, method = 'nearest')
        tws_sel = tws_sel.sel(lat = lat, lon = lon, method = 'nearest')
        twd_sel = ds.twd.sel(time = t, method = 'nearest')
        twd_sel = twd_sel.sel(lat = lat, lon = lon, method = 'nearest')
        return (np.float32(twd_sel.values), np.float32(tws_sel.values))
    point_valid = point_validity.land_sea_mask(input_data['extent']).point_validity_arr
    df = pd.DataFrame(input_data['polar'])
    df.columns = df.columns.astype(float)
    df.index = df.index.astype(float)
    polar_class = polar.Polar(df = df)
    weatherrouter = isochronal_weather_router.weather_router(polar_class, 
                            getWindAt, 
                            ds.time.values,
                            input_data['step'],
                            input_data['start'],
                            input_data['finish'],
                            spread = input_data['spread'],
                            wake_lim = input_data['wake_lim'],
                            rounding = input_data['rounding'],
                            point_validity = point_valid)
    weatherrouter.route()
    route_df = weatherrouter.get_fastest_route()
    vis = visualize.visualize(ds, input_data['start'], input_data['finish'], route_df)
    plot = vis.return_plot()
    with fs.open(f"s3://lambdaroutingstack-weatherroutebucket7b183c04-pyxlno4db9s1/results/{input_data['name']}.csv", 'w') as f:
        route_df.to_csv(f)
    with fs.open(f"s3://lambdaroutingstack-weatherroutebucket7b183c04-pyxlno4db9s1/results/{input_data['name']}.html", 'w') as f:
        hvplot.save(plot, f, resources=INLINE)
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': '{} request completed successfully \n'.format(input_data['name'])
    }