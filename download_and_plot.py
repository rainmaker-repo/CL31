import boto3
import os
import sys
import netCDF4 as nc
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.colors as colors
import numpy as np
from datetime import datetime
import time
import logging
import warnings
from matplotlib.dates import DateFormatter

import matplotlib
matplotlib.use('Agg')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/cl31/cl31_plotter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=UserWarning)

def upload_to_s3(local_file, bucket_name, s3_path):
    """Upload a file to S3"""
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(
            local_file, 
            bucket_name, 
            s3_path,
            ExtraArgs={'ContentType': 'image/png'}
        )
        logger.info(f"Successfully uploaded {local_file} to s3://{bucket_name}/{s3_path}")
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise

def get_latest_file():
    """Get the latest .nc file from S3"""
    s3_client = boto3.client('s3')
    bucket_name = 'in-situ-592as8'
    prefix = 'CL31/'

    folders = []
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    
    logger.info("Found folders: %s", [p.get('Prefix') for p in response.get('CommonPrefixes', [])])
    
    for prefix in response.get('CommonPrefixes', []):
        folder_name = prefix.get('Prefix')
        if 'CL31_45' in folder_name:
            date_str = folder_name[-9:-1]
            folders.append((folder_name, date_str))
    
    if not folders:
        raise ValueError("No folders found matching 'CL31_45' pattern")
        
    latest_folder = max(folders, key=lambda x: x[1])[0]
    logger.info(f"Latest folder: {latest_folder}")

    files = []
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=latest_folder)
    
    logger.info("Files in latest folder: %s", [obj['Key'] for obj in response.get('Contents', [])])
    
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.nc'):
            filename = obj['Key']
            try:
                time_part = filename.split('_')[-1].replace('.nc', '')
                start_hour = int(time_part.split('-')[0])
                files.append((obj['Key'], start_hour))
                logger.info(f"Found file: {filename} with start hour {start_hour}")
            except (ValueError, IndexError) as e:
                logger.warning(f"Skipping file {filename}: {str(e)}")
                continue

    if not files:
        raise ValueError(f"No valid .nc files found in folder {latest_folder}")
        
    latest_file = max(files, key=lambda x: x[1])[0]
    
    local_file = os.path.basename(latest_file)
    logger.info(f"Downloading {latest_file} to {local_file}")
    s3_client.download_file(bucket_name, latest_file, local_file)
    logger.info("Download complete!")
    return local_file, latest_file, bucket_name

def plot_cbh_vs_time(df, save_image_path, lat, lon, site_name):
    fig, ax1 = plt.subplots(figsize=(10, 6))
    
    ax1.scatter(df['time'], df['cbh_1'], label='1st Cloud Base', color='black', marker='x')
    ax1.scatter(df['time'], df['cbh_2'], label='2nd Cloud Base', color='blue', marker='x')
    ax1.scatter(df['time'], df['cbh_3'], label='3rd Cloud Base', color='red', marker='x')
    ax1.set_ylim(0, 6000)


    ax1.set_ylabel('Cloud Base Height (m - AGL)')
    ax1.legend(loc='upper right')
    ax1.grid()
    
    ax1.set_xlabel('Time (UTC - M HR:MIN)')
    date_format = DateFormatter('%d %H:%M')
    ax1.xaxis.set_major_formatter(date_format)
    ax1.tick_params(axis='x',rotation=45)
    

    ax2 = ax1.twinx()
    ax2.spines["left"].set_position(("axes", -0.15))
    ax2.yaxis.set_label_position("left")
    ax2.yaxis.set_ticks_position("left")
    
    def meters_to_feet(meters):
        return meters * 3.28084

    ax2.set_ylim(meters_to_feet(0), meters_to_feet(6000))
    ax2.set_ylabel('Cloud Base Height (feet - AGL)')
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(500 * 3.28084))

    plt.title(f'Cloud Base Heights vs. Time (Lat: {lat}, Lon: {lon}), {site_name}')
    plt.tight_layout()
    plt.savefig(save_image_path, dpi=300)
    plt.close('all')
    logger.info(f"CBH plot saved to {save_image_path}")

def plot_diagnostics_subplots(df, save_image_path, lat, lon, site_name):
    diagnostics_vars = ['laser_temperature', 'pulse_energy', 'backscatter_sum']
    num_vars = len(diagnostics_vars)
    num_cols = 3
    num_rows = (num_vars + num_cols - 1) // num_cols
    
    fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, 10))
    axes = axes.flatten()

    for i, var in enumerate(diagnostics_vars):
        axes[i].plot(df['time'], df[var], label=var, color='black')
        axes[i].set_title(var.replace('_', ' ').title())
        axes[i].set_xlabel('Time (UTC)')
        axes[i].set_ylabel(var.replace('_', ' ').title())
        axes[i].tick_params(axis='x', rotation=45)

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.title(f'Diagnostic Plots (Lat: {lat}, Lon: {lon}), {site_name}')
    plt.tight_layout()
    plt.savefig(save_image_path)
    plt.close('all')
    logger.info(f"Diagnostics subplot figure saved to {save_image_path}")

def plot_backscatter_contour_log(dataset, save_image_path, lat, lon, site_name, vmin=1e-9, vmax=1e-1, num_bins=50):
    time = dataset.variables['time'][:]
    levels = dataset.variables['level'][:] * 10
    backscatter = dataset.variables['backscatter'][:, :]

    time_datetime = pd.to_datetime(time, unit='s')
    levels_bins = np.logspace(np.log10(vmin), np.log10(vmax), num_bins)

    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    contour = ax1.contourf(time_datetime, levels, np.transpose(backscatter), 
                           cmap='gist_ncar_r', norm=colors.LogNorm(vmin=vmin, vmax=vmax), levels=levels_bins)
    cbar = plt.colorbar(contour)
    
    cbar.set_ticks([1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1])
    cbar.set_ticklabels(['1e-9', '1e-8', '1e-7', '1e-6', '1e-5', '1e-4', '1e-3', '1e-2', '1e-1'])
    
    cbar.set_label('Backscatter (km⁻¹ sr⁻¹)', fontsize=12)

    ax1.set_xlabel('Time (UTC)')
    ax1.set_ylabel('Height (m - AGL)')
    ax1.set_ylim(0, 6000)
    

    ax1.set_xlabel('Time (UTC - M HR:MIN)')
    date_format = DateFormatter('%d %H:%M')
    ax1.xaxis.set_major_formatter(date_format)
    ax1.tick_params(axis='x',rotation=45)
    

    
    

    ax2 = ax1.twinx()
    ax2.spines["left"].set_position(("axes", -0.15))
    ax2.yaxis.set_label_position("left")
    ax2.yaxis.set_ticks_position("left")

    def meters_to_feet(meters):
        return meters * 3.28084

    ax2.set_ylim(meters_to_feet(0), meters_to_feet(6000))
    ax2.set_ylabel('Height (feet - AGL)')

    plt.title(f'Backscatter Contour Plot (Lat: {lat}, Lon: {lon}), {site_name}')
    plt.tight_layout()
    plt.savefig(save_image_path)
    plt.close('all')
    logger.info(f"Backscatter contour plot (log scale) saved to {save_image_path}")

def process_cl31_child(child_prefix, bucket_name='in-situ-592as8'):
    """Process a single CL31 child directory"""
    try:
        s3_client = boto3.client('s3')
        logger.info(f"Processing CL31 child directory: {child_prefix}")

        # Ensure 'recent' folder exists directly under the site directory
        site_path = child_prefix.rstrip('/')  # e.g., 'CL31/Lexington'
        recent_folder = f"{site_path}/recent/"
        
        try:
            s3_client.head_object(Bucket=bucket_name, Key=recent_folder)
        except:
            s3_client.put_object(Bucket=bucket_name, Key=recent_folder)
            logger.info(f"Created recent folder: {recent_folder}")

        # Get all subfolders within this child
        subfolders = []
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=child_prefix, Delimiter='/')
        
        for prefix in response.get('CommonPrefixes', []):
            subfolder = prefix.get('Prefix')
            if subfolder[-9:-1].isdigit() and len(subfolder[-9:-1]) == 8:
                date_str = subfolder[-9:-1]
                subfolders.append((subfolder, date_str))
        
        if not subfolders:
            logger.warning(f"No valid date-formatted subfolders found in {child_prefix}")
            return
            
        # Get the latest subfolder
        latest_subfolder = max(subfolders, key=lambda x: x[1])[0]
        logger.info(f"Latest subfolder for {child_prefix}: {latest_subfolder}")

        # Find the latest file in the subfolder
        files = []
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=latest_subfolder)
        
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.nc'):
                filename = obj['Key']
                try:
                    time_part = filename.split('_')[-1].replace('.nc', '')
                    start_hour = int(time_part.split('-')[0])
                    files.append((obj['Key'], start_hour))
                    logger.info(f"Found file: {filename} with start hour {start_hour}")
                except (ValueError, IndexError) as e:
                    logger.warning(f"Skipping file {filename}: {str(e)}")
                    continue

        if not files:
            logger.warning(f"No valid .nc files found in {latest_subfolder}")
            return
            
        latest_file = max(files, key=lambda x: x[1])[0]
        
        # Create unique temporary filename using child_prefix
        safe_prefix = child_prefix.replace('/', '_').replace('\\', '_')
        local_file = f"/tmp/{safe_prefix}{os.path.basename(latest_file)}"
        logger.info(f"Downloading {latest_file} to {local_file}")
        s3_client.download_file(bucket_name, latest_file, local_file)
        
        # Process the file
        process_single_file(local_file, latest_file, bucket_name)
        
        # Cleanup
        os.remove(local_file)
        logger.info(f"Completed processing for {child_prefix}")
        
    except Exception as e:
        logger.error(f"Error processing {child_prefix}: {e}", exc_info=True)

def process_single_file(local_file, s3_path, bucket_name):
    """Process a single netCDF file and generate/upload plots"""
    try:
        plots_dir = '/tmp/cl31_plots'
        os.makedirs(plots_dir, exist_ok=True)
        
        # Create unique plot paths for regular upload
        safe_prefix = s3_path.replace('/', '_').replace('\\', '_')
        plot_paths = {
            'cbh_plot.png': f'{plots_dir}/{safe_prefix}_cbh_plot.png',
            'diagnostic_plot.png': f'{plots_dir}/{safe_prefix}_diagnostic_plot.png',
            'backscatter_contour.png': f'{plots_dir}/{safe_prefix}_backscatter_contour.png'
        }
        
        dataset = nc.Dataset(local_file, 'r')
        
        time_var = dataset.variables['time'][:]
        time_datetime = pd.to_datetime(time_var, unit='s')
        
        df_non_height = pd.DataFrame({
            'time': time_datetime,
            'backscatter_sum': dataset.variables['backscatter_sum'][:],
            'cbh_1': dataset.variables['cbh_1'][:],
            'cbh_2': dataset.variables['cbh_2'][:],
            'cbh_3': dataset.variables['cbh_3'][:],
            'laser_temperature': dataset.variables['laser_temperature'][:],
            'pulse_energy': dataset.variables['pulse_energy'][:]
        })
        
        parts = os.path.basename(local_file).split('_')
        lat = parts[3]
        lon = parts[4]
        
        s3_base_path = s3_path[:-3]
        
        # Extract site name from s3_path (e.g., from 'CL31/Lexington/20240318/file.nc' get 'Lexington')
        site_name = s3_path.split('/')[1]
        
        # Generate plots with site_name parameter
        plot_cbh_vs_time(df_non_height, plot_paths['cbh_plot.png'], lat, lon, site_name)
        plot_diagnostics_subplots(df_non_height, plot_paths['diagnostic_plot.png'], lat, lon, site_name)
        plot_backscatter_contour_log(dataset, plot_paths['backscatter_contour.png'], lat, lon, site_name)
        
        # Upload plots to both locations
        s3_client = boto3.client('s3')
        
        # Get the site path (e.g., 'CL31/Lexington' or 'CL31/CliffB')
        site_parts = s3_path.split('/')[:3]
        site_path = '/'.join(site_parts[:-1])  # Excludes the date folder
        
        for plot_name, local_plot_path in plot_paths.items():
            # Upload to date-specific folder with unique name
            dated_plot_path = f"{s3_path[:-3]}_{plot_name}"
            upload_to_s3(local_plot_path, bucket_name, dated_plot_path)
            
            # Upload to recent folder with simple name
            recent_plot_path = f"{site_path}/recent/{plot_name}"
            upload_to_s3(local_plot_path, bucket_name, recent_plot_path)
            
            os.remove(local_plot_path)
        
        dataset.close()
        
    except Exception as e:
        logger.error(f"Error processing file {local_file}: {e}", exc_info=True)
        raise

# Run the processing logic in a sequential loop
if __name__ == "__main__":
    try:
        # Get all direct children of CL31/ (this replaces the looping logic)
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket='in-situ-592as8',
            Prefix='CL31/',
            Delimiter='/'
        )
        
        cl31_children = [p.get('Prefix') for p in response.get('CommonPrefixes', [])]
        logger.info(f"Found CL31 children: {cl31_children}")
        
        # Process each child sequentially
        for child in cl31_children:
            process_cl31_child(child)
            
    except Exception as e:
        logger.error(f"Error in script execution: {e}", exc_info=True)
