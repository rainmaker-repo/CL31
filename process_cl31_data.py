import subprocess
import netCDF4 as nc
import pandas as pd
import time
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.colors as colors
import numpy as np
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError
from botocore.config import Config
import os

# Configure the S3 client with retry and timeout settings
config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    },
    connect_timeout=5,
    read_timeout=5
)
s3 = boto3.client('s3', config=config, region_name='us-east-1')

# Function to configure routing to ensure Wi-Fi is used for internet


# Function to upload a file to S3
def upload_to_s3(local_file, bucket_name, s3_file_path):
    print(f"[{datetime.now()}] Preparing to upload {local_file} to S3 bucket {bucket_name} at path {s3_file_path}")
    
    
    try:
        print(f"[{datetime.now()}] Uploading {local_file} using Wi-Fi...")
        s3.upload_file(local_file, bucket_name, s3_file_path)
        print(f"[{datetime.now()}] File {local_file} uploaded to bucket {bucket_name} as {s3_file_path}.")
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"Error occurred during upload: {e}")

# Function to find the latest .dat file in the folder structure
def find_latest_dat_file(base_folder):
    print(f"[{datetime.now()}] Searching for latest .dat file in {base_folder}")
    latest_file = None
    latest_time = None

    for root, dirs, files in os.walk(base_folder):
        for file in files:
            if file.endswith('.dat'):
                file_path = os.path.join(root, file)
                file_time = os.path.getmtime(file_path)
                print(f"[{datetime.now()}] Found file: {file_path} with timestamp {file_time}")

                if latest_time is None or file_time > latest_time:
                    latest_file = file_path
                    latest_time = file_time

    if latest_file:
        print(f"[{datetime.now()}] Latest .dat file found: {latest_file}")
    else:
        print(f"[{datetime.now()}] No .dat files found.")
    
    return latest_file

# Function to generate the S3 folder path based on lat, lon, and date
def generate_s3_folder(lat, lon):
    current_date = datetime.utcnow().strftime('%Y%m%d')
    folder_path = f"CL31/Butter/CL31_{lat}_{lon}_{current_date}"
    print(f"[{datetime.now()}] Generated S3 folder path: {folder_path}")
    return folder_path

# Function to process the .dat file, convert it to NetCDF, and separate non-height data
def process_cl2nc_and_separate_data(input_dat_file, output_folder, bucket_name, s3_folder):
    print(f"[{datetime.now()}] Processing .dat file: {input_dat_file}")
    file_name = os.path.basename(input_dat_file).replace('.dat', '')
    backscatter_path = os.path.join(output_folder, f'{file_name}.nc')

    try:
        print(f"[{datetime.now()}] Running cl2nc to convert {input_dat_file} to {backscatter_path}")
        result = subprocess.run(['cl2nc', input_dat_file, backscatter_path], check=True, capture_output=True, text=True, timeout=300)
        print(f"[{datetime.now()}] Successfully converted {input_dat_file} to {backscatter_path}")
        
        # Upload the NetCDF file to S3
        upload_to_s3(backscatter_path, bucket_name, f'{s3_folder}/{file_name}.nc')
        
    except subprocess.CalledProcessError as e:
        print(f"Error during conversion: {e}")
        print(f"Error output:\n{e.stderr}")
        return
    except subprocess.TimeoutExpired as e:
        print(f"[{datetime.now()}] cl2nc process took too long and timed out: {e}")
        return
    

    return ()



# Infinite loop to process files every minute
if __name__ == "__main__":
    base_folder = '/home/cl31c/CL31/raw'
    output_folder = '/home/cl31c/CL31/pro'
 
    latitude = "45.4945"  # Replace with actual lat
    longitude = "-119.0206"  # Replace with actual lon
    bucket_name = "in-situ-592as8"
    
    while True:
        latest_dat_file = find_latest_dat_file(base_folder)

        if latest_dat_file:
            print(f"[{datetime.now()}] Processing .dat file: {latest_dat_file}")
            s3_folder = generate_s3_folder(latitude, longitude)
            
            df = process_cl2nc_and_separate_data(latest_dat_file, output_folder, bucket_name, s3_folder)

        else:
            print(f"[{datetime.now()}] No .dat files found.")
        
        print(f"[{datetime.now()}] Waiting for 1 minute before checking again.")
        time.sleep(300)
