#%%
import os
import xarray as xr
import pandas as pd
import numpy as np
import cdsapi

# Load your coordinates
coords = pd.read_csv("coordinates.csv")
import matplotlib.pyplot as plt

# Plot the first 100 coordinates
plt.figure(figsize=(8, 8))
plt.scatter(coords.head(100)['longitude'], coords.head(100)['latitude'], c='blue', marker='o')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('First 100' ' Coordinates')
#%%
print(len(coords))
#%%
# Create output folder if it doesn't exist
output_folder = r"C:\Users\2692812C\OneDrive - University of Glasgow\Desktop\Forecasting\Data\WindSpeedDataCoordinates"
os.makedirs(output_folder, exist_ok=True)

c = cdsapi.Client()

# Loop over the first 100 coordinates
for idx, row in coords.iloc[101:1000].iterrows():
    lat = row['latitude']
    lon = row['longitude']

    # Format file name based on coordinates
    nc_filename = f"era5_wind_jan2023_lat{lat:.3f}_lon{lon:.3f}.nc"
    nc_filepath = os.path.join(output_folder, nc_filename)

    # Download data for this point (small area)
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': [
                '100m_u_component_of_wind', '100m_v_component_of_wind',
            ],
            'year': '2023',
            'month': '01',
            'day': [f"{d:02d}" for d in range(1, 32)],
            'time': [f"{h:02d}:00" for h in range(24)],
            'format': 'netcdf',
            'area': [lat + 0.05, lon - 0.05, lat - 0.05, lon + 0.05],  # North, West, South, East
        },
        nc_filepath
    )

    # Open the ERA5 NetCDF file and check the data
    ds = xr.open_dataset(nc_filepath)

    # Extract u100 and v100 as DataFrames
    u100 = ds['u100'].isel(latitude=0, longitude=0).to_pandas()
    v100 = ds['v100'].isel(latitude=0, longitude=0).to_pandas()

    # Compute wind speed
    wind_speed = np.sqrt(u100**2 + v100**2)

    # Combine into a DataFrame, including the iteration number
    df = pd.DataFrame({
        'ITERATION': idx,
        'U100': u100.values,
        'V100': v100.values,
        'WIND_SPEED': wind_speed.values
    }, index=u100.index)

    # Save to CSV with coordinate-based name
    csv_filename = f"wind_timeseries_lat_{idx}_{lat:.3f}_lon{lon:.3f}.csv"
    csv_filepath = os.path.join(output_folder, csv_filename)
    df.to_csv(csv_filepath)

    print(f"Saved: {csv_filename} (iteration {idx})")