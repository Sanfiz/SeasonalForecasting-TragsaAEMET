import os
import sys
from dotenv import load_dotenv
import cdsapi
import pandas as pd
import xarray as xr
import numpy as np
import fiona
import geopandas as gpd
import xskillscore as xs
from dateutil.relativedelta import relativedelta
from shapely.geometry import Point, box
import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
from scipy.spatial import cKDTree


#########################
#paths
shapefile_path = '/sclim/cly/basins/data-basins'
masivo_path =  '/MASIVO/cly'
results_path = '/sclim/cly/basins/results-basins'

#######################################


shapefile_basins = 'DemarcacionesHidrograficasPHC2015_2021.shp'
shapefile = os.path.join(shapefile_path, shapefile_basins)
basins = gpd.read_file(shapefile)

#grib_data = '/MASIVO/cly/Seasonal_Verification/1-Sf_variables/data/ecmwf_s51_stmonth09_hindcast1993-2016_monthly.grib'
#grib_data = '/MASIVO/cly/Forecast/1-Default_forecast/grib-data/ecmwf_s51_stmonth05_forecast2024_monthly.grib'
grib_data='/MASIVO/cly/forecast_ecmwf/tprate_1deg_ecmwf_s51_20241101.grib'
hcst_fname = os.path.join(masivo_path, grib_data)
grib_data = xr.open_dataset(hcst_fname, engine='cfgrib')
lats = grib_data.latitude.values  # Extraer latitud y longitud del archivo grib



data=grib_data
# Modificar las longitudes para que estén en el rango de -180 a 180
data = data.assign_coords(longitude=((data.longitude + 180) % 360 - 180))

# Ordenar las longitudes para que vayan de -180 a 180 en orden ascendente
data = data.sortby('longitude')

# Verificar las dimensiones y el rango de las longitudes
print(data.dims)         # Debería ser (number: 51, step: 6, latitude: 180, longitude: 360)
print(data.longitude)    # Debería estar en el rango de -180 a 180

lons = data.longitude.values


################################################################################
lon_grid, lat_grid = np.meshgrid(lons, lats)
grid_points = np.array([lon_grid.ravel(), lat_grid.ravel()]).T
tree = cKDTree(grid_points)  # Construir un KDTree para búsqueda rápida de vecinos cercanos

################################################################################

# Iterar sobre cada cuenca en el shapefile
for i, basin in basins.iterrows():

    basin_name = basin['nameText'] if 'nameText' in basin else f"Basin_{i + 1}"
    print(f'Se ha escogido la cuenca: {basin_name}')

    ############# CALCULOS ###################
    # Calcular el centroide de la cuenca y el punto de malla más cercano
    basin_centroid = basin.geometry.centroid
    distance, idx = tree.query([basin_centroid.x, basin_centroid.y])
    nearest_lon, nearest_lat = grid_points[idx]

    print(f"El centro de la cuenca es => Longitude: {basin_centroid.x}, Latitude: {basin_centroid.y}")
    print(f"El punto de grid más cercano es - Longitude: {nearest_lon}, Latitude: {nearest_lat}, Distance: {distance} degrees")

    ############# PUNTOS DEL GRID EN LA CUENCA ###################
    # Crear una lista para almacenar los puntos dentro de la cuenca
    points_within_basin = []

    # Iterar a través de cada punto en la malla para encontrar los puntos dentro de la geometría de la cuenca
    for lat_idx, lat in enumerate(lats):
        for lon_idx, lon in enumerate(lons):
            point = Point(lon, lat)

            # Comprobar si el punto está dentro de la geometría de la cuenca
            if basin.geometry.contains(point):
                points_within_basin.append({
                    "x_grid": lat_idx,
                    "y_grid": lon_idx,
                    "latitude": lat,
                    "longitude": lon,
                    "basin_name": basin_name
                })

    # Si no se encontraron puntos dentro de la cuenca, agregar el punto de malla más cercano al centroide
    if not points_within_basin:
        print(f"No se encontraron puntos dentro de la cuenca {basin_name}. Añadiendo el punto de malla más cercano al centroide.")
        points_within_basin.append({
            "x_grid": np.where(lats == nearest_lat)[0][0],  # Índice de latitud más cercano
            "y_grid": np.where(lons == nearest_lon)[0][0],  # Índice de longitud más cercano
            "latitude": nearest_lat,
            "longitude": nearest_lon,
            "basin_name": basin_name
        })

    # Añadir el nombre de la cuenca a cada punto encontrado dentro de la cuenca
    for point in points_within_basin:
        point["basin_name"] = basin_name

    # Crear un DataFrame con los puntos de la cuenca
    df = pd.DataFrame(points_within_basin)
    output_csv = os.path.join(results_path, f'grid_points_within_{basin.name+1}_worldwide.csv')
    df.to_csv(output_csv, index=False)
    print(df.columns)
    df_without_basin_name = df.drop(columns=['basin_name'])

    ############# Grafica y Tabla ###################
    fig, (ax_map, ax_table) = plt.subplots(1, 2, figsize=(18, 12), gridspec_kw={'width_ratios': [2, 1]})
    fig.subplots_adjust(wspace=0.05)

    # Crear la gráfica de la cuenca
    basins.plot(ax=ax_map, color='lightblue', edgecolor='black', alpha=0.5)  # Pintar las demás cuencas en gris
    gpd.GeoDataFrame(geometry=[basin.geometry]).plot(ax=ax_map, color='none', edgecolor='red', linewidth=2)  # Cuenca seleccionada en rojo
    ax_map.set_title(f"Basin {i + 1}: " + str(basin['nameTxtInt']) if 'nameTxtInt' in basin else f"Basin {i + 1}")
    ax_map.set_xlabel("Grid Lon")
    ax_map.set_ylabel("Grid Lat")

    # Definir los límites de latitud y longitud

    if 19 <= i <= 25:
        ax_map.set_xlim(-20, 10)
        ax_map.set_ylim(-10, 31)
    else:
        ax_map.set_xlim(-20, 5)
        ax_map.set_ylim(35, 45)

    # Ajustar las líneas de la cuadrícula y reducir el tamaño de fuente
    ax_map.set_xticks(np.arange(-19.5, 5.5, 1))  # Desplazamiento de 0.5 para representar bordes
    ax_map.set_yticks(np.arange(26.5, 45.5, 1))
    ax_map.tick_params(axis='both', labelsize=8)  # Reducir el tamaño de las etiquetas de lat/lon
    ax_map.grid(color='gray', linestyle='--', linewidth=0.5)

    # Intentar graficar usando el nombre de columna real
    if 'longitude' in df_without_basin_name.columns and 'latitude' in df_without_basin_name.columns:
        # Graficar los puntos de la cuadrícula dentro de la cuenca
        ax_map.scatter(df_without_basin_name['longitude'], df_without_basin_name['latitude'], color='blue', s=10, alpha=0.6, label="Grid Points Inside Basin")
    else:
        print(f"La cuenca {basin_name} no tiene columnas 'longitude' y 'latitude' en el DataFrame. Pasando a la siguiente cuenca.")
        continue  # Pasar a la siguiente iteración del bucle for sin hacer nada

    # Añadir la información del punto de malla más cercano al centroide
    ax_map.text(0.05, 0.95, f" Nearest Centroid Grid Lon: {nearest_lon:.2f}\n Nearest Centroid Grid Lat: {nearest_lat:.2f}", 
                transform=ax_map.transAxes, fontsize=10, verticalalignment='top', bbox=dict(facecolor='white', alpha=0.7))

    # Mostrar la tabla de datos del DataFrame
    #ax_table.axis('off')  # Ocultar el eje de la tabla
    #table = ax_table.table(cellText=df_without_basin_name.head(10).values, colLabels=df_without_basin_name.columns, cellLoc='center', loc='center')
    #table.auto_set_font_size(False)
    #table.set_fontsize(14)  # Aumentar el tamaño de fuente de la tabla
    #table.scale(1, 1.5)  # Aumentar el espaciado entre filas para más legibilidad
    #table.auto_set_column_width(col=list(range(len(df.columns))))  # Ajustar ancho de columnas

    # Guardar la figura combinada
    output_image = os.path.join(results_path, f'basin_{i + 1}_with_table_worldwide.png')
    plt.savefig(output_image, dpi=fig.dpi, bbox_inches='tight', pad_inches=0.5)
    print(f'Figure with table saved at {output_image}')
    plt.close()
