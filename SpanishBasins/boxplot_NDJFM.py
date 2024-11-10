

"""

STEP1. Load the Forecast and Climatology Data

STEP2 

STEP3: 
        
STEP4: 
        

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

# define model

institution = 'ECWMF'
name = 'SEAS5'
startmonth = 11
year=2024
origin_labels = {'institution': institution, 'name': name}
model = 'ecmwf'
system = '51'
aggr = '3m'
fcmonth = 2


endmonth = (startmonth+fcmonth)-1 if (startmonth+fcmonth)<=12 else (startmonth+fcmonth)-1-12


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



# define paths
HINDDIR="/MASIVO/cly/Seasonal_Verification/1-Sf_variables/data"
FOREDIR="/MASIVO/cly/Forecast/1-Default_forecast/grib-data"


# Open climatology and forecast 


hcst_bname = '{origin}_s{system}_stmonth{start_month:02d}_hindcast{hcstarty}-{hcendy}_monthly'.format(**config)
hcst_fname = f'{HINDDIR}/{hcst_bname}.grib'

# forecast
fcst_bname = '{origin}_s{system}_stmonth{start_month:02d}_forecast{fcy}_monthly'.format(**config)
fcst_fname = f'{FOREDIR}/{fcst_bname}.grib'




# %% [markdown]
# ## Read hindcast

# We calculate the monthly and 3-months anomalies for the hindcast data.

#%%
print("Read hindcast")

# For the re-shaping of time coordinates in xarray.Dataset we need to select the right one 
#  -> burst mode ensembles (e.g. ECMWF SEAS5) use "time". This is the default option in this notebook
#  -> lagged start ensembles (e.g. MetOffice GloSea6) use "indexing_time" (see CDS documentation about nominal start date)
st_dim_name = 'time' if not config.get('isLagged',False) else 'indexing_time'

# Reading hindcast data from file
hcst = xr.open_dataset(hcst_fname,engine='cfgrib', backend_kwargs=dict(time_dims=('forecastMonth', st_dim_name)))
# We use dask.array with chunks on leadtime, latitude and longitude coordinate
hcst = hcst.chunk({'forecastMonth':1, 'latitude':'auto', 'longitude':'auto'})
# Reanme coordinates to match those of observations
hcst = hcst.rename({'latitude':'lat','longitude':'lon', st_dim_name:'start_date'})

# Add start_month to the xr.Dataset
start_month = pd.to_datetime(hcst.start_date.values[0]).month
hcst = hcst.assign_coords({'start_month':start_month})
# Add valid_time to the xr.Dataset
vt = xr.DataArray(dims=('start_date','forecastMonth'), coords={'forecastMonth':hcst.forecastMonth,'start_date':hcst.start_date})
vt.data = [[pd.to_datetime(std)+relativedelta(months=fcmonth-1) for fcmonth in vt.forecastMonth.values] for std in vt.start_date.values]
hcst = hcst.assign_coords(valid_time=vt)

#if aggr=='3m':
# Calculate 3-month aggregations
hcst_3 = hcst.rolling(forecastMonth=3).mean()
# rolling() assigns the label to the end of the N month period, so the first N-1 elements have NaN and can be dropped
hcst_3 = hcst_3.where(hcst.forecastMonth>=3,drop=True)
#elif aggr=='5m':
# Calculate 5-month aggregations
#hcst = hcst.rolling(forecastMonth=5).mean()
# rolling() assigns the label to the end of the N month period, so the first N-1 elements have NaN and can be dropped
#hcst = hcst.where(hcst.forecastMonth>=5,drop=True)




#%%
print("Read forecast")

# For the re-shaping of time coordinates in xarray.Dataset we need to select the right one 
#  -> burst mode ensembles (e.g. ECMWF SEAS5) use "time". This is the default option in this notebook
#  -> lagged start ensembles (e.g. MetOffice GloSea6) use "indexing_time" (see CDS documentation about nominal start date)
st_dim_name = 'time' if not config.get('isLagged',False) else 'indexing_time'

# Reading hindcast data from file
fcst = xr.open_dataset(fcst_fname,engine='cfgrib', backend_kwargs=dict(time_dims=('forecastMonth', st_dim_name)))
# We use dask.array with chunks on leadtime, latitude and longitude coordinate
fcst = fcst.chunk({'forecastMonth':1, 'latitude':'auto', 'longitude':'auto'})
# Reanme coordinates to match those of observations
fcst = fcst.rename({'latitude':'lat','longitude':'lon', st_dim_name:'start_date'})

# Add start_month to the xr.Dataset
start_month = pd.to_datetime(fcst.start_date.values).month
fcst = fcst.assign_coords({'start_month':start_month})
# Add valid_time to the xr.Dataset
vt = xr.DataArray(dims=('forecastMonth',), coords={'forecastMonth': fcst.forecastMonth})
vt.data = [pd.to_datetime(fcst.start_date.values)+relativedelta(months=fcmonth-1) for fcmonth in fcst.forecastMonth.values]
fcst = fcst.assign_coords(valid_time=vt)

#if aggr=='3m':
# Calculate 3-month aggregations
fcst_3 = fcst.rolling(forecastMonth=3).mean()
# rolling() assigns the label to the end of the N month period, so the first N-1 elements have NaN and can be dropped
fcst_3 = fcst_3.where(hcst.forecastMonth>=3,drop=True)
#elif aggr=='5m':
# Calculate 5-month aggregations
#fcst = fcst.rolling(forecastMonth=5).mean()
# rolling() assigns the label to the end of the N month period, so the first N-1 elements have NaN and can be dropped
#fcst = fcst.where(hcst.forecastMonth>=5,drop=True)


hindcast_data3 = hcst_3['tprate']
forecast_data3 = fcst_3['tprate']

years = hindcast_data3['start_date'].dt.year.values

#%%


# Definir segundos por día
seconds_per_day = 86400

# Crear un diccionario con el número de días en cada mes
days_in_month = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 
                 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}

# Crear una función para convertir de m/s a l/m^2 por mes usando días específicos
def convert_to_monthly_precipitation(data, month):
    return data * 1000 * seconds_per_day * days_in_month[month]

# Aplicar la conversión para el invierno extendido: de Noviembre a marzo - NDJFM
fcst['tprate'] = xr.concat(
    [convert_to_monthly_precipitation(fcst['tprate'].sel(forecastMonth=m), m + 11 if m == 1 else m + 1) 
     for m in range(2, 5)], dim="forecastMonth")

hcst['tprate'] = xr.concat(
    [convert_to_monthly_precipitation(hcst['tprate'].sel(forecastMonth=m), m + 11 if m == 1 else m + 1) 
     for m in range(2, 5)], dim="forecastMonth")


# Ajustar el cálculo para el invierno extendido (noviembre a marzo) - en realidad este paso no hace falta porque la cosa que igual
fcst['tprate'] = xr.concat(
    [convert_to_monthly_precipitation(fcst['tprate'].sel(forecastMonth=m), (m + 10) % 12 + 1)
     for m in range(2, 5)], dim="forecastMonth")

hcst['tprate'] = xr.concat(
    [convert_to_monthly_precipitation(hcst['tprate'].sel(forecastMonth=m), (m + 10) % 12 + 1)
     for m in range(1, 6)], dim="forecastMonth")


# Continuar con el cálculo de la media invernal
winter_fcst = fcst['tprate'].mean(dim='forecastMonth')
winter_hcst = hcst['tprate'].mean(dim='forecastMonth')


#  calcular la media de los miembros del ensemble del forecast y hindcast
ensmean_winter_fcst = winter_fcst.mean(dim='number').squeeze() # el squeeze no creo que haga falta
ensmean_winter_hcst = winter_hcst.mean(dim='number').squeeze()



# calcular la anomalia relativa
# For precipitation, we use the relative anomaly (expressed as a precentage) 
#anomtprate = (ensmean_winter_fcst - ensmean_winter_hcst)/ensmean_winter_hcst*100.
 



# Compute the relative anomaly: (Forecast - Hindcast) / Hindcast * 100
winter_fcst_expanded = winter_fcst.expand_dims(start_date=winter_hcst['start_date'])
relative_anomalies_ensemble = (winter_fcst_expanded - winter_hcst) / winter_hcst * 100

# Verifica la estructura de las anomalías relativas
print(relative_anomalies_ensemble)



# path csv
path_to_csv_files = '/sclim/cly/cly/SRS/cuencas/results'

# Prueba con i = 2
i = 2
file_name = f"grid_points_within_{i}.csv"
file_path = os.path.join(path_to_csv_files, file_name)
print(f"Processing file: {file_path}")

# Verifica si el archivo existe
if os.path.exists(file_path):
    # Carga el archivo CSV
    df = pd.read_csv(file_path)
    
    # Verifica si el archivo contiene las columnas necesarias
    if 'x_grid' in df.columns and 'y_grid' in df.columns:
        # Obtiene los puntos (x_grid, y_grid) de la cuenca
        points = list(zip(df['x_grid'], df['y_grid']))
        
        # Extrae los valores de anomalía para los puntos seleccionados usando indexación avanzada
        anomaly_values = []
        for x, y in points:
            # Extrae el valor de anomalía para cada combinación de miembro y año
            # usando selección con `isel` para coordenadas basadas en índices
            anomaly = relative_anomalies_ensemble.isel(lat=y, lon=x).values
            print(f"Point (y={y}, x={x}) extracted with anomaly values for each year and member")
            
            # Añade el valor de anomalía a la lista de valores
            anomaly_values.append(anomaly)
        
        # Convierte anomaly_values a un array de numpy para facilitar el cálculo de la media
        anomaly_values = np.array(anomaly_values)
        
        # Calcula la media de los valores de anomalía para los puntos seleccionados
        # `axis=0` calcula la media sobre los puntos, pero mantiene las dimensiones de (start_date, number)
        mean_anomaly = np.mean(anomaly_values, axis=0)
        
        # mean_anomaly es un array de (24 años, 25 miembros)
        print(f"Mean anomaly over points for file {file_name}: {mean_anomaly.shape}")
        print(mean_anomaly)  # Puedes verificar los valores de la media de la anomalía


mean_anomaly_transposed = mean_anomaly.T  # Ahora la forma es (25, 24)



# Generar el boxplot con posiciones y ancho de cajas definidos
plt.figure(figsize=(12, 6))
plt.boxplot(mean_anomaly_transposed, positions=np.arange(len(years)), widths=0.6, patch_artist=True)

# Etiquetas de los años en el eje x con rotación para facilitar la lectura
plt.xticks(ticks=np.arange(len(years)), labels=[str(year) for year in years], rotation=45)

# Configuración de la gráfica
plt.title("Relative Precipitation Anomalies for Basin i=2 (1993-2016)\n"
          "Forecast for NDJFM 2024. ECWMF SEAS5")
plt.xlabel("Year")
plt.ylabel("Relative Precipitation Anomaly (%)")

# Guardar la gráfica
output_results = '/sclim/cly/cly/SRS/cuencas/results/'
output_file = 'precip_anom_basin_2_ECWMF_SEAS5_DJF_stmonth11_year2023.png'
plt.savefig(f"{output_results}{output_file}", dpi=300, bbox_inches="tight")

print(f"Boxplot guardado en {output_results}{output_file}")



# Verifica la forma de mean_anomaly y realiza la transposición si es necesario
# Si mean_anomaly ya tiene la forma correcta, no es necesario transponer
if mean_anomaly.shape[0] == 24 and mean_anomaly.shape[1] == 25:
    mean_anomaly_transposed = mean_anomaly.T  # Transponer para obtener (25, 24)
else:
    mean_anomaly_transposed = mean_anomaly

# Confirmamos que ahora el número de columnas coincide con los años
assert mean_anomaly_transposed.shape[1] == len(years), "Mismatch between years and anomaly data dimensions."

# Crear un diccionario para almacenar las estadísticas para cada año
stats_per_year = {}

# Calcular estadísticas para cada año y almacenarlas en el diccionario
for i, year_data in enumerate(mean_anomaly_transposed.T):  # Itera sobre columnas para cada año
    stats_per_year[years[i]] = {
        "percentil_95": np.percentile(year_data, 95),
        "q3": np.percentile(year_data, 75),
        "median": np.percentile(year_data, 50),
        "q1": np.percentile(year_data, 25),
        "percentil_5": np.percentile(year_data, 5)
    }

# Convertir el diccionario a un DataFrame para una visualización y manipulación más sencilla
stats_df = pd.DataFrame(stats_per_year).T  # Transponer para tener años como filas

# Guardar los resultados en un archivo CSV
output_results = '/sclim/cly/cly/SRS/cuencas/results/'
output_file = 'precipitation_anomaly_stats_basin_2_ECWMF_SEAS5_DJF.csv'
stats_df.to_csv(f"{output_results}{output_file}", index_label="Year")

print(f"Estadísticas guardadas en {output_results}{output_file}")
