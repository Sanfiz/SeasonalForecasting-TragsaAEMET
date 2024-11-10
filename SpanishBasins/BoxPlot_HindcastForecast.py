"""
This script processes forecast and hindcast data from the ECMWF SEAS5 model
to calculate and visualise precipitation anomalies for Spanish basins.

STEP1. Define main characteristics of the input data
   - Sets up the basic configuration for the ECMWF SEAS5 model, including forecast months, system type, and initialization details.
   - Iterates over some forecast year (2022, 2023, 2024)

STEP2. Load Hindcast and Forecast Data
   - Opens hindcast and forecast files in GRIB format and sets up the time and coordinate system.


STEP3. Make some computations in the data
    3.1 Convert Precipitation Units from m/s to l/m² 

    3.2 Calculate Winter Precipitation Mean an extended winter period (from November to March).

    3.3 Reshape Hindcast Dimensions to make them compatible for anomaly calculations.

STEP4. Compute Precipitation Anomalies for each basin by comparing hindcast and forecast values.

STEP5. Compute and Save Statistics
   - Calculates percentiles and central tendency statistics for each anomaly period and saves them in a CSV file.

STEP6. Visualise Results
   - Generates a boxplot for precipitation anomalies and a table with calculated statistics, saving the visualization as an image.

The script is designed to process each basin individually, iterating over the configured years and applying the workflow described above.
"""


import os
import sys
from dotenv import load_dotenv
import pandas as pd
import xarray as xr
import numpy as np
from dateutil.relativedelta import relativedelta
import matplotlib
from matplotlib import pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import warnings
warnings.filterwarnings('ignore')


##########################################################
#STEP1. Define main characteristics of the input data

# define model
institution = 'ECWMF'
name = 'SEAS5'
startmonth = 11
year=[2023,2024]
origin_labels = {'institution': institution, 'name': name}
model = 'ecmwf'
system = '51'
fcmonth = 2


# Here we save the configuration
config = dict(
    list_vars = ['total_precipitation'],
    fcy = year,
    hcstarty = 1993,
    hcendy = 2016,
    start_month = startmonth,
    origin = model,
    system = system,
    isLagged = False if model in ['ecmwf', 'meteo_france', 'dwd', 'cmcc', 'eccc'] else True
)


# Loop through each forecast year
for forecast_year in config['fcy']:
    print(f"Processing forecast year: {forecast_year}")

    ####################################################################
    # STEP2. Load Hindcast and Forecast Data

    # paths grib of 1º horizontal resolution
    HINDDIR="/MASIVO/cly/Seasonal_Verification/1-Sf_variables/data"
    FOREDIR="/MASIVO/cly/Forecast/1-Default_forecast/grib-data"


    # Open climatology
    hcst_bname = '{origin}_s{system}_stmonth{start_month:02d}_hindcast{hcstarty}-{hcendy}_monthly'.format(**config)
    hcst_fname = f'{HINDDIR}/{hcst_bname}.grib'
    st_dim_name = 'time' if not config.get('isLagged',False) else 'indexing_time'
    hcst = xr.open_dataset(hcst_fname,engine='cfgrib', backend_kwargs=dict(time_dims=('forecastMonth', st_dim_name)))
    hcst = hcst.chunk({'forecastMonth':1, 'latitude':'auto', 'longitude':'auto'})
    hcst = hcst.rename({'latitude':'lat','longitude':'lon', st_dim_name:'start_date'})
    start_month = pd.to_datetime(hcst.start_date.values[0]).month
    hcst = hcst.assign_coords({'start_month':start_month})
    vt = xr.DataArray(dims=('start_date','forecastMonth'), coords={'forecastMonth':hcst.forecastMonth,'start_date':hcst.start_date})
    vt.data = [[pd.to_datetime(std)+relativedelta(months=fcmonth-1) for fcmonth in vt.forecastMonth.values] for std in vt.start_date.values]
    hcst = hcst.assign_coords(valid_time=vt)


    # Open forecast
    fcst_bname = f"{config['origin']}_s{config['system']}_stmonth{config['start_month']:02d}_forecast{forecast_year}_monthly"
    fcst_fname = f'{FOREDIR}/{fcst_bname}.grib'
    print(f"Forecast file name for year {forecast_year}: {forecast_filename}")
    st_dim_name = 'time' if not config.get('isLagged',False) else 'indexing_time'
    fcst = xr.open_dataset(fcst_fname,engine='cfgrib', backend_kwargs=dict(time_dims=('forecastMonth', st_dim_name)))
    fcst = fcst.chunk({'forecastMonth':1, 'latitude':'auto', 'longitude':'auto'})
    fcst = fcst.rename({'latitude':'lat','longitude':'lon', st_dim_name:'start_date'})
    start_month = pd.to_datetime(fcst.start_date.values).month
    fcst = fcst.assign_coords({'start_month':start_month})
    vt = xr.DataArray(dims=('forecastMonth',), coords={'forecastMonth': fcst.forecastMonth})
    vt.data = [pd.to_datetime(fcst.start_date.values)+relativedelta(months=fcmonth-1) for fcmonth in fcst.forecastMonth.values]
    fcst = fcst.assign_coords(valid_time=vt)

    ####################################################################
    # STEP3. Make some computations in the data

    def convert_to_monthly_precipitation(data, month, leap_year=False):
        """
        This function converts precipitation units from m/s to l/m^2.
        :data: matrix of precipitation
        :month: forecast month
        :leap_year: boolean indicating if the year is a leap year
        :return: matrix of precipitation in l/m^2
        """ 
        seconds_per_day = 86400
        days_in_month = {1: 31, 2: 29 if leap_year else 28, 3: 31, 4: 30, 5: 31, 6: 30, 
                        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
        solution = data * 1000 * seconds_per_day * days_in_month[month]                     
        return solution


    hcst['tprate'] = xr.concat(
        [convert_to_monthly_precipitation(hcst['tprate'].sel(forecastMonth=m), m + 11 if m == 1 else m + 1) 
        for m in range(1, 6)], dim="forecastMonth")

    fcst['tprate'] = xr.concat(
        [convert_to_monthly_precipitation(fcst['tprate'].sel(forecastMonth=m), m + 11 if m == 1 else m + 1) 
        for m in range(1, 6)], dim="forecastMonth")

    # hcst-Dimensions: (number: 25, forecastMonth: 6, start_date: 24, lat: 46, lon: 91)
    # fcst-Dimensions: (number: 51, forecastMonth: 6, lat: 180, lon: 360)


    # 3.1 Winter average over the course of 6 months (NDJFM?)
    winter_hcst = hcst['tprate'].mean(dim='forecastMonth') 
    winter_fcst = fcst['tprate'].mean(dim='forecastMonth')

    # winter_hcst-Dimensions: (number: 25, start_date: 24, lat: 46, lon: 91)
    # winter_fcst-Dimensions: (number: 51, lat: 180, lon: 360)

    # 3.2 Merge the first two colummns of the hindcast
    winter_hcst_stacked = winter_hcst.stack(new_dim=("number", "start_date")).T

    # winter_hcst_stacked-Dimensions: (new_dim: 600, lon: 91, lat: 46)


    # path csv
    path_to_csv_files = '/sclim/cly/basins/results-basins'

    # Loop over the basins
    for i in range(1, 26):

        file_name = f"grid_points_within_{i}.csv"
        file_path = os.path.join(path_to_csv_files, file_name)
        print(f"Processing file: {file_path}")

        df = pd.read_csv(file_path)
        
        ####################################################################
        # STEP4. Compute Precipitation Anomalies

        winter_hcst_stacked = winter_hcst.stack(new_dim=("number", "start_date")).T

        # winter_hcst_stacked-Dimensions: (new_dim: 600, lon: 91, lat: 46)

        if 'x_grid' in df.columns and 'y_grid' in df.columns:

            points = list(zip(df['x_grid'], df['y_grid']))
            
            # Extract values in our lats lons
            hcst_values = []
            fcst_values = []
            for x, y in points:
                hcst_basin = winter_hcst_stacked.isel(lat=y, lon=x).values
                hcst_values.append(hcst_basin)
                fcst_basin = winter_fcst.isel(lat=y, lon=x).values
                fcst_values.append(fcst_basin)

            hcst_values = np.array(hcst_values)
            fcst_values = np.array(fcst_values) 

            # Calculate the mean of the hindcast in dimension 600
            hindcast_mean = hcst_values.mean(axis=1, keepdims=True)  # Shape: (points in basin, 1)

            # Relative anomaly
            hindcast_anomaly = (hcst_values - hindcast_mean) / hindcast_mean * 100
            forecast_anomaly = (fcst_values - hindcast_mean) / hindcast_mean * 100

            # Print the shapes for verification
            print("Hindcast relative shape:", hindcast_anomaly.shape)
            print("Forecast relative shape:", forecast_anomaly.shape)

        # Compute the mean for the basin points
        hindcast_anomaly_basinmean=hindcast_anomaly.mean(axis=0)
        forecast_anomaly_basinmean=forecast_anomaly.mean(axis=0)

        ####################################################################
        # STEP5. Compute and Save Statistics

        # Compute statistics for hindcast and forecast anomalies
        hindcast_stats = {
            "95th Percentile": np.percentile(hindcast_anomaly_basinmean, 95),
            "75th Percentile (Q3)": np.percentile(hindcast_anomaly_basinmean, 75),
            "Median (Q2)": np.percentile(hindcast_anomaly_basinmean, 50),
            "25th Percentile (Q1)": np.percentile(hindcast_anomaly_basinmean, 25),
            "5th Percentile": np.percentile(hindcast_anomaly_basinmean, 5)
        }

        forecast_stats = {
            "95th Percentile": np.percentile(forecast_anomaly_basinmean, 95),
            "75th Percentile (Q3)": np.percentile(forecast_anomaly_basinmean, 75),
            "Median (Q2)": np.percentile(forecast_anomaly_basinmean, 50),
            "25th Percentile (Q1)": np.percentile(forecast_anomaly_basinmean, 25),
            "5th Percentile": np.percentile(forecast_anomaly_basinmean, 5)
        }

        # Combine statistics into a DataFrame for easy display in the table
        stats_df = pd.DataFrame({
            f"Reference 1993-2016": hindcast_stats,
            f"Forecast {forecast_year}/{forecast_year + 1}": forecast_stats})

        # Round to two decimal places for display
        stats_df = stats_df.round(2)

        # Save the statistics to a CSV file
        output_results = '/sclim/cly/basins/results-basins/'
        output_csv = f'{output_results}HindcastForecast_stats_basin_{i}_ECWMF_SEAS5_NDJFM_{forecast_year}.csv'
        stats_df.to_csv(output_csv, index_label="Statistic")
        print(f"Statistics saved at {output_csv}")

        ####################################################################
        #STEP6. Visualise Results
        # Create a figure with subplots: one for the boxplot and one for the statistics table
        fig, (ax_box, ax_table) = plt.subplots(1, 2, figsize=(14, 6), gridspec_kw={"width_ratios": [2, 1]})
        fig.subplots_adjust(top=0.8, wspace=0.5)  # Increase space between subplots
        fig.suptitle(f"Precipitation Anomaly\nBasin: {df['basin_name'][0]}\nPeriod: Extended Winter (NDJFM). Model: ECWMF SEAS5", fontsize=14)

        # Customize boxplot
        ax_box.boxplot([hindcast_anomaly_basinmean, forecast_anomaly_basinmean], 
                    labels=[f"Reference\n1993-2016", f"Forecast\n{forecast_year}/{forecast_year +1}"], 
                    widths=0.4,
                    patch_artist=True,
                    boxprops=dict(facecolor="lightblue", color="darkblue"),
                    medianprops=dict(color="orange", linewidth=1.5),
                    whiskerprops=dict(color="darkblue"),
                    capprops=dict(color="darkblue"),
                    flierprops=dict(marker="o", color="darkblue", markersize=5))
        ax_box.set_ylabel("Precipitation Anomaly (%)", fontsize=12)
        ax_box.set_xlabel("Period", fontsize=12)

        # Table displaying statistics next to the boxplot
        ax_table.axis("off")  # Turn off axis
        table = ax_table.table(cellText=stats_df.values, 
                            colLabels=[f'Reference\n1993-2016', f'Forecast\n{forecast_year}/{forecast_year + 1}'], 
                            rowLabels=stats_df.index, 
                            cellLoc="center", 
                            loc="center",
                            colColours=["#cfe2f3", "#ffdfba"])  # Column colors

        # Customize table appearance
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.5, 1.5)  
        table.auto_set_column_width(col=list(range(len(stats_df.columns))))

        # Adjust table header font
        for key, cell in table.get_celld().items():
            if key[0] == 0:  # Header row
                cell.set_fontsize(12)
                cell.set_text_props(weight="bold")
                cell.set_height(0.1)

        # Save the plot with the table
        output_results = '/sclim/cly/basins/results-basins/'
        output_file = f'HindcastForecast_basin_{i}_ECWMF_SEAS5_NDJFM_{forecast_year}.png'
        plt.savefig(f"{output_results}{output_file}", dpi=300, bbox_inches="tight")
        plt.close()

        print(f"Plot saved at {output_results}{output_file}")
