# Project Documentation

---

### 1. Datasets

1. **Climate Data Store (CDS)**  
   - Provided by the European Centre for Medium-Range Weather Forecasts (ECMWF).  
   - Includes seasonal forecasting and hindcast models.  
   - Access to the data: [Copernicus CDS website](https://cds.climate.copernicus.eu/).

2. **Spanish River Basin Districts**  
   - Provided by the Ministry for the Ecological Transition and the Demographic Challenge (MITECO).  
   - Includes shapefiles for Spanish river basin districts.  
   - Download here: [MITECO website](https://www.miteco.gob.es/en/cartografia-y-sig/ide/descargas/agua/demarcaciones-hidrograficas-phc-2015-2021.html).

---

### 2. Scripts


- **`remapbil.py`**  
  - Interpolates horizontal data to decrease resolution from 1º to 0.25º over the target region using CDO tools.

- **`BoxPlot_HindcastForecast.py`**  
  - Processes seasonal forecast and hindcast data to calculate and visualize precipitation anomalies for Spanish river basins during the extended winter season.

- **`boxplot_NDJFM.py`**  
  - Generates boxplots specific to Spanish river basin districts for the November–March (NDJFM) season, highlighting seasonal precipitation trends.

- **`subplot_basins.py`**  
  - Creates subplot visualizations to compare the Spanish river basin districts based on precipitation and other hydrological indicators.

---

### 3. References

This project was developed based on the previous work:

1. **ECMWF Jupyter Notebook**  
   - Learn more: [ECMWF Seasonal Forecast Verification](https://ecmwf-projects.github.io/copernicus-training-c3s/sf-verification.html).

2. **GitHub Repository by Martín Senande Rivera**  
   - Original repository: [mSenande](https://github.com/mSenande/).
