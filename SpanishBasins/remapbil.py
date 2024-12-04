# HINDCAST and FORECAST interpolation for ECMWF data
# ecmwf_s51_stmonth11_

# Load libraries
import xarray as xr
import numpy as np
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get paths from environment variables
hindcast_input_dir = os.getenv("HINDCAST_INPUT_DIR")
hindcast_output_dir = os.getenv("HINDCAST_OUTPUT_DIR")
forecast_input_dir = os.getenv("FORECAST_INPUT_DIR")
forecast_output_dir = os.getenv("FORECAST_OUTPUT_DIR")
era5_file = os.getenv("ERA5_GRID_FILE")

if not all([hindcast_input_dir, hindcast_output_dir, forecast_input_dir, forecast_output_dir, era5_file]):
    raise ValueError("Some required environment variables are missing. Please check your .env file.")

# Spatial variables to process
spatial_vars = ["tprate"]

# Load the ERA5 grid (0.25-degree resolution)
era5 = xr.open_dataset(era5_file)
era5_reduced = era5.isel(valid_time=0)  # Remove the first dimension
new_latitudes = np.array(era5_reduced['latitude'])
new_longitudes = np.array(era5_reduced['longitude'])

def interpolate_hindcast(input_dir, output_dir, new_latitudes, new_longitudes):
    """Interpolate hindcast data to 0.25-degree grid."""
    print("Processing HINDCAST data...")
    
    # Open hindcast GRIB file
    hindcast_file = os.path.join(input_dir, 'hindcast_file_name.grib')
    ds = xr.open_dataset(hindcast_file, engine="cfgrib")

    interpolated_vars = {}
    for var in spatial_vars:
        print(f"Processing variable: {var}")
        interpolated_blocks = []
        
        # Process by time blocks
        for time_idx in range(len(ds['time'])):
            print(f" - Interpolating year {str(np.array(ds['time'][time_idx]))[:4]} ({time_idx + 1}/{len(ds['time'])})")
            block = ds[var].isel(time=time_idx)  # Select time block
            block_interp = block.interp(latitude=new_latitudes, longitude=new_longitudes, method="linear")
            interpolated_blocks.append(block_interp)
        
        # Concatenate interpolated time blocks
        interpolated_vars[var] = xr.concat(interpolated_blocks, dim='time')
        print(f"Variable {var} interpolated completely.")

    # Create a new dataset with interpolated variables
    new_ds = xr.Dataset(
        interpolated_vars,
        coords={
            'number': ds['number'],
            'time': ds['time'],
            'step': ds['step'],
            'latitude': new_latitudes,
            'longitude': new_longitudes
        }
    )

    # Save the interpolated hindcast to NetCDF
    output_file = os.path.join(output_dir, "hindcast_output_file_name.nc")
    new_ds.to_netcdf(output_file)
    print(f"Hindcast NetCDF file created: {output_file}")

def interpolate_forecast(input_dir, output_dir, new_latitudes, new_longitudes):
    """Interpolate forecast data to 0.25-degree grid."""
    print("Processing FORECAST data...")
    
    # Open forecast GRIB file
    forecast_file = os.path.join(input_dir, 'forecast_file_name.grib')
    ds = xr.open_dataset(forecast_file, engine="cfgrib")

    interpolated_vars = {}
    for var in spatial_vars:
        print(f"Interpolating variable: {var}")
        interpolated_vars[var] = ds[var].interp(
            coords={'latitude': new_latitudes, 'longitude': new_longitudes},
            method="linear"
        )
        print(f"Variable {var} interpolated.")

    # Create a new dataset with interpolated variables
    new_ds = xr.Dataset(
        interpolated_vars,
        coords={
            'number': ds['number'],
            'step': ds['step'],
            'latitude': new_latitudes,
            'longitude': new_longitudes
        }
    )

    # Save the interpolated forecast to NetCDF
    output_file = os.path.join(output_dir, "forecast_output_file_name.nc")
    new_ds.to_netcdf(output_file)
    print(f"Forecast NetCDF file created: {output_file}")

# Execute hindcast and forecast interpolation
os.makedirs(hindcast_output_dir, exist_ok=True)  # Ensure output directories exist
os.makedirs(forecast_output_dir, exist_ok=True)

interpolate_hindcast(hindcast_input_dir, hindcast_output_dir, new_latitudes, new_longitudes)
interpolate_forecast(forecast_input_dir, forecast_output_dir, new_latitudes, new_longitudes)
