# ERA5 Wind Speed Downloader and Processor

This Python script automates the process of downloading hourly wind data from the [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu/) for a list of geographical coordinates, computes the wind speed, and saves the results as CSV files.
To work it is need an active registration to COPERNICUS WEBSITE, and valid API KEY

---

## 📦 Features

- Reads a CSV file containing a list of coordinates (`latitude`, `longitude`).
- Downloads ERA5 reanalysis data for:
  - `100m_u_component_of_wind`
  - `100m_v_component_of_wind`
- Computes the wind speed using:  
  \[
  \text{wind\_speed} = \sqrt{u^2 + v^2}
  \]
- Outputs:
  - One NetCDF file per coordinate.
  - One CSV time series file per coordinate with columns:
    - `U100`, `V100`, `WIND_SPEED`, `ITERATION`

---

## 🗂 Folder Structure


