import os
import glob
import xarray as xr
import pandas as pd

def merge_metek_nc_files(folder_path, output_file, start_date, end_date):
    """
    Merge multiple NetCDF (.nc) files from a specified folder into a single file,
    filtered to a specific date/time range.

    Args:
        folder_path (str): Path to the folder containing the .nc files.
        output_file (str): Full path for the output merged NetCDF file.
        start_date (str or datetime-like): Start of the desired date/time range.
        end_date (str or datetime-like): End of the desired date/time range.

    Returns:
        None: The merged and filtered dataset is saved to 'output_file'.
    """
    try:
        # Convert start_date and end_date to pandas.Timestamp objects
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        # Create a file pattern for .nc files in the folder
        file_pattern = os.path.join(folder_path, "*.nc")
        files = sorted(glob.glob(file_pattern))
        
        if not files:
            raise ValueError(f"No .nc files found in folder: {folder_path}")
        
        # Open and merge all NetCDF files along common coordinates (e.g., time)
        ds = xr.open_mfdataset(files, combine='by_coords')
        
        # Ensure the time coordinate is in datetime format
        ds['time'] = pd.to_datetime(ds['time'].values)
        
        # Filter the dataset based on the specified time range
        merged_ds = ds.sel(time=slice(start_date, end_date))
        
        # Save the merged dataset to the output NetCDF file
        merged_ds.to_netcdf(output_file)
        
        print(f"Merged dataset saved to: {output_file}")
        
        # Close datasets
        ds.close()
        merged_ds.close()
    
    except Exception as e:
        print(f"An error occurred while merging the .nc files: {e}")

# Example usage:
folder_path = r"C:\Users\Todd McKinney\Desktop\SLW_PAPER\working\CL31\metek\20250110"
output_file = r"C:\Users\Todd McKinney\Desktop\SLW_PAPER\working\CL31\merged_output.nc"
start_date = "2025-01-10T16:00:00"
end_date = "2025-01-10T21:00:00"

merge_metek_nc_files(folder_path, output_file, start_date, end_date)
