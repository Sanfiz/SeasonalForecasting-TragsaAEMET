import os
import sys
from dotenv import load_dotenv
import xarray as xr
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from scipy.spatial import cKDTree
import pandas as pd

# Paths
shapefile_path = '/sclim/cly/basins/data-basins'
masivo_path = '/MASIVO/cly'
results_path = '/sclim/cly/basins/results-basins'

# Load shapefile for basins
shapefile_basins = 'DemarcacionesHidrograficasPHC2015_2021.shp'
shapefile = os.path.join(shapefile_path, shapefile_basins)
basins = gpd.read_file(shapefile)

# List of GRIB files with different resolutions and corresponding resolution names
grib_files = [
    {
        "path": "/MASIVO/cly/Forecast/1-Default_forecast/grib-data/ecmwf_s51_stmonth11_forecast2024_monthly.grib",
        "resolution": "1deg"
    },
    {
        "path": "/MASIVO/cly/Forecast/1-Default_forecast/grib025/ecmwf_s51_stmonth11_forecast2023_monthly_025.grib",
        "resolution": "025deg"
    }
]

# Define basin groups
groups = {
    "Vertiente Atlántico Norte y Cantábrico": [1, 3, 4, 17],
    "Vertiente Atlántico Sur": [8, 9, 15],
    "Vertiente Mediterránea": [0, 2, 10, 11, 12, 13, 14, 16],
    "Cuencas Interiores": [5, 6, 7],
    "Islas Canarias": [18, 19, 20, 21, 22, 23, 24]
}

# Group configurations for each area (vertiente) 
configurations = {
    "Vertiente Atlántico Norte y Cantábrico": {"xlim": (-9.5, 4.5), "ylim": (34.5, 44.5), "color": 'lightblue'},
    "Vertiente Atlántico Sur": {"xlim": (-9.5, 4.5), "ylim": (34.5, 44.5), "color": 'lightblue'},
    "Vertiente Mediterránea": {"xlim": (-9.5, 4.5), "ylim": (34.5, 44.5), "color": 'lightblue'},
    "Cuencas Interiores": {"xlim": (-9.5, 4.5), "ylim": (34.5, 44.5), "color": 'lightblue'},
    "Islas Canarias": {"xlim": (-20, -10), "ylim": (26, 30), "color": 'lightblue'}
}

# Process each GRIB file
for grib_info in grib_files:
    grib_data = xr.open_dataset(grib_info["path"], engine="cfgrib")
    
    # Extract latitude and longitude from the GRIB file
    lats = grib_data.latitude.values
    lons = grib_data.longitude.values
    
    # Set up a KDTree for the grid points
    lon_grid, lat_grid = np.meshgrid(lons, lats)
    grid_points = np.array([lon_grid.ravel(), lat_grid.ravel()]).T
    tree = cKDTree(grid_points)

    # Iterate over each basin in the shapefile
    for i, basin in basins.iterrows():
        basin_name = basin['nameText'] if 'nameText' in basin else f"Basin_{i}"
        print(f'Processing basin: {basin_name}')

        # Calculate the basin's centroid and the nearest grid point
        basin_centroid = basin.geometry.centroid
        _, idx = tree.query([basin_centroid.x, basin_centroid.y])
        nearest_lon, nearest_lat = grid_points[idx]

        # Find points within the basin
        points_within_basin = []
        for lat_idx, lat in enumerate(lats):
            for lon_idx, lon in enumerate(lons):
                point = Point(lon, lat)
                if basin.geometry.contains(point):
                    points_within_basin.append({
                        "x_grid": lat_idx,
                        "y_grid": lon_idx,
                        "latitude": lat,
                        "longitude": lon,
                        "basin_name": basin_name
                    })

        if not points_within_basin:
            points_within_basin.append({
                "x_grid": np.where(lats == nearest_lat)[0][0],
                "y_grid": np.where(lons == nearest_lon)[0][0],
                "latitude": nearest_lat,
                "longitude": nearest_lon,
                "basin_name": basin_name
            })

        # Create DataFrame
        df = pd.DataFrame(points_within_basin)

        # Plotting
        fig, ax_map = plt.subplots(figsize=(12, 8))

        # Determine group and configure plot
        for group, indices in groups.items():
            if i in indices:
                config = configurations[group]
                ax_map.set_xlim(config["xlim"])
                ax_map.set_ylim(config["ylim"])
                ax_map.tick_params(axis='both', labelsize=8)
                ax_map.grid(color='gray', linestyle='--', linewidth=0.5)
                ax_map.set_title(f"{group}\n {basin_name}", fontsize=16, fontweight='bold', loc='center')

                # Plot all basins in light gray
                basins.plot(ax=ax_map, color='lightgrey', edgecolor='black', alpha=0.5)

                # Plot group basins in color
                group_basins = basins[basins.index.isin(indices)]
                group_basins.plot(ax=ax_map, color=config["color"], edgecolor='black', alpha=0.7)

                # Highlight current basin
                gpd.GeoDataFrame(geometry=[basin.geometry]).plot(ax=ax_map, color="mistyrose", edgecolor='red', linewidth=3)
                break

        ax_map.set_xlabel("Grid Lon", fontsize=14)
        ax_map.set_ylabel("Grid Lat", fontsize=14)

        # Plot grid points within the basin
        if 'longitude' in df.columns and 'latitude' in df.columns:
            ax_map.scatter(df['longitude'], df['latitude'], color='black', s=40, alpha=0.6)

        # Save each plot as PNG with resolution info
        output_image = os.path.join(results_path, f'basin_{i}_{grib_info["resolution"]}.png')
        plt.tight_layout(pad=0)
        plt.savefig(output_image, dpi=fig.dpi, bbox_inches='tight', pad_inches=0.2)
        print(f'Saved plot at {output_image}')
        plt.close()

    # Close the GRIB file
    grib_data.close()
