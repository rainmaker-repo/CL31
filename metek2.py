import os
import glob
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

def plot_metek_two_fields(folder_path, start_date, end_date, colorbar_ranges=None, colormaps=None, alpha=0.5):
    """
    Overlay METEK radar data from multiple NetCDF files on a single figure with two subplots:
      - Top subplot: Liquid Water Content (LWC)
      - Bottom subplot: Reflectivity (Zea)
    Data from each file is overlaid (with transparency) and restricted to a specified time range.
    
    Args:
        folder_path (str): Path to the folder containing .nc files.
        start_date (str or datetime-like): Start of the desired time range.
        end_date (str or datetime-like): End of the desired time range.
        colorbar_ranges (dict): Dictionary of {field: (min, max)} for colorbar limits.
        colormaps (dict): Dictionary of {field: cmap_name} for colormaps.
        alpha (float): Transparency for each file's plot overlay (0 to 1).
    
    Returns:
        None
    """
    try:
        # Convert time range to pandas Timestamps
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Gather all NetCDF files in the folder
        nc_files = sorted(glob.glob(os.path.join(folder_path, "*.nc")))
        if not nc_files:
            raise FileNotFoundError("No .nc files found in folder: " + folder_path)
        
        # Define the two fields: LWC (top) and Reflectivity (Zea) (bottom)
        fields = [
            ('LWC', 'Liquid Water Content (LWC) [g/m³]'),
            ('Zea', 'Reflectivity (Zea) [dBZ]')
        ]
        
        # Create a figure with 2 subplots (stacked vertically)
        fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 8))
        axes = axes.flatten()  # axes[0] for LWC, axes[1] for Reflectivity
        
        # To store the first mesh objects for adding colorbars later
        first_mesh = [None] * len(fields)
        
        # Loop over each file in the folder
        for file_path in nc_files:
            ds = xr.open_dataset(file_path)
            
            # Ensure 'range' is a coordinate
            if 'range' not in ds.coords and 'range' in ds:
                ds = ds.assign_coords(range=ds['range'])
            
            # Convert the 'time' coordinate to datetime and slice the dataset
            ds['time'] = pd.to_datetime(ds['time'].values)
            ds_sel = ds.sel(time=slice(start_date, end_date))
            
            # Skip files with no data in the specified time range
            if ds_sel['time'].size == 0:
                ds.close()
                continue
            
            # Convert time values for plotting (matplotlib date numbers) and get range values
            time_vals = mdates.date2num(ds_sel['time'].values)
            range_vals = ds_sel['range'].values
            
            # Loop over the two fields and overlay the data on the corresponding subplot
            for i, (field, title) in enumerate(fields):
                if field in ds_sel:
                    data = ds_sel[field].values
                    
                    # Apply custom colorbar limits if provided
                    if colorbar_ranges and field in colorbar_ranges:
                        vmin, vmax = colorbar_ranges[field]
                        data = np.where((data >= vmin) & (data <= vmax), data, np.nan)
                    else:
                        vmin, vmax = np.nanmin(data), np.nanmax(data)
                    
                    # Choose colormap (default to 'viridis' if not provided)
                    cmap = colormaps[field] if colormaps and field in colormaps else 'viridis'
                    
                    # Plot using pcolormesh (data transposed so time is x and range is y)
                    mesh = axes[i].pcolormesh(time_vals, range_vals, data.T, shading='auto',
                                               cmap=cmap, vmin=vmin, vmax=vmax, alpha=alpha)
                    
                    # Store the first mesh handle for the colorbar
                    if first_mesh[i] is None:
                        first_mesh[i] = mesh
                    
                    # Set axis labels and title (only once)
                    if not axes[i].get_title():
                        axes[i].set_title(title)
                        axes[i].set_xlabel('Time (UTC)')
                        axes[i].set_ylabel('Range (m)')
                        axes[i].xaxis_date()
                        axes[i].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            
            ds.close()
        
        # Add a colorbar to each subplot using the stored mesh handle
        for i, mesh in enumerate(first_mesh):
            if mesh is not None:
                plt.colorbar(mesh, ax=axes[i])
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        print("An error occurred:", e)

def save_lwc_profiles_df(folder_path, profile_times, profile_save_path):
    """
    Merge data from multiple NetCDF files in the specified folder and, for each profile time,
    extract the Liquid Water Content (LWC) vs. altitude (using the 'range' coordinate). The output
    is saved as a tab-delimited text file in a pandas DataFrame format with columns: time, alt, lwc.
    
    Args:
        folder_path (str): Path to the folder containing .nc files.
        profile_times (list of str or datetime-like): List of times at which to extract the profile.
        profile_save_path (str): Full path for the output text file.
    
    Returns:
        None
    """
    try:
        # Gather all NetCDF files in the folder
        nc_files = sorted(glob.glob(os.path.join(folder_path, "*.nc")))
        if not nc_files:
            raise FileNotFoundError("No .nc files found in folder: " + folder_path)
        
        # Merge datasets using open_mfdataset (assumes compatible time coordinates)
        ds = xr.open_mfdataset(nc_files, combine='by_coords')
        ds['time'] = pd.to_datetime(ds['time'].values)
        
        rows = []
        for time_str in profile_times:
            # Convert profile time to Timestamp
            profile_time = pd.to_datetime(time_str)
            # Select the nearest time sample for the given profile time
            ds_sel = ds.sel(time=profile_time, method='nearest')
            
            if 'LWC' not in ds_sel:
                print(f"LWC variable not found for time {profile_time}. Skipping.")
                continue
            
            # Extract the LWC profile and corresponding altitude values
            lwc_profile = ds_sel['LWC'].values  # Assumed to be 1D (over 'range')
            altitudes = ds_sel['range'].values
            
            # For each altitude point, add a row with the selected time, altitude, and LWC value
            for alt, lwc in zip(altitudes, lwc_profile):
                rows.append({'time': profile_time, 'alt': alt, 'lwc': lwc})
        
        # Create a DataFrame and save it as a tab-delimited text file
        df = pd.DataFrame(rows)
        df.to_csv(profile_save_path, sep='\t', index=False)
        
        ds.close()
        print(f"LWC profiles saved to: {profile_save_path}")
        
    except Exception as e:
        print("An error occurred while saving profiles:", e)

# Example custom colorbar ranges and colormaps for plotting:
custom_colorbar_ranges = {
    'Zea': (4.0, 35.0),
    'LWC': (0.0, 5.0)
}

custom_colormaps = {
    'Zea': 'jet',
    'LWC': 'cividis'
}

# Folder containing the individual .nc files
folder_path = r"C:\Users\Todd McKinney\Desktop\SLW_PAPER\working\20250110"
start_time = "2025-01-10 16:00:00"
end_time   = "2025-01-10 20:30:00"
profile_save_path = r"C:\Users\Todd McKinney\Desktop\SLW_PAPER\working\LWC_profiles.txt"

# List of profile times (e.g., four example times)
profile_times = [
    "2025-01-10 16:44:00",
    "2025-01-10 18:15:00",
    "2025-01-10 19:05:00",
    "2025-01-10 20:02:00"
]

# First, create the two-subplot plot for visualization
plot_metek_two_fields(folder_path, start_time, end_time,
                      colorbar_ranges=custom_colorbar_ranges,
                      colormaps=custom_colormaps,
                      alpha=1)

# Then, save the LWC vs. altitude profiles for the specified times to a text file in a DataFrame-like format
save_lwc_profiles_df(folder_path, profile_times, profile_save_path)
