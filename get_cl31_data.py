import socket
import os
from datetime import datetime
import sys

# Define the IP and port of the CL31 ceilometer
CEILOMETER_IP = '192.168.127.254'
CEILOMETER_PORT = 4001

# Define the start and end markers for the packet
START_MARKER = b'\x01'
END_MARKER = b'\x04'

def get_file_time_range():
    # Get the current time in UTC
    now_utc = datetime.utcnow()

    # Determine the starting time range (00-06Z, 06-12Z, etc.)
    hour = now_utc.hour
    if 0 <= hour < 6:
        start_hour = 0
        end_hour = 6
    elif 6 <= hour < 12:
        start_hour = 6
        end_hour = 12
    elif 12 <= hour < 18:
        start_hour = 12
        end_hour = 18
    else:
        start_hour = 18
        end_hour = 24

    # Format the time range and date for the filename
    date_str = now_utc.strftime('%Y%m%d')
    return f'{date_str}_{start_hour:02d}-{end_hour:02d}Z', date_str

# Function to create a new file path based on the current time range
def create_dat_file_path(base_folder, lat, lon):
    time_range, date_str = get_file_time_range()

    # Create a folder based on the date if it doesn't exist
    folder_name = f'CL31_{lat}_{lon}_{date_str}'
    folder_path = os.path.join(base_folder, folder_name)

    # Ensure the folder exists
    os.makedirs(folder_path, exist_ok=True)

    # Construct the filename with time range
    filename = f'CL31_{lat}_{lon}_{time_range}.dat'
    
    # Return the full path to the file
    save_path = os.path.join(folder_path, filename)
    return save_path
    

def capture_ceilometer_data(base_folder, lat, lon):
    try:
        # Create the initial file path
        dat_file_path = create_dat_file_path(base_folder, lat, lon)
        
        # Create a socket connection to the ceilometer
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"Attempting to connect to Ceilometer at {CEILOMETER_IP}:{CEILOMETER_PORT}")
            s.connect((CEILOMETER_IP, CEILOMETER_PORT))
            print(f"Connected to Ceilometer at {CEILOMETER_IP}:{CEILOMETER_PORT}")

            # Open the .dat file in append mode to store the data stream in ASCII
            with open(dat_file_path, 'ab') as f:
                print(f"Saving data to {dat_file_path}")

                buffer = b''  # Buffer to collect data for a complete packet

                while True:
                    # Receive data in chunks
                    data = s.recv(4096)
                    if not data:
                        print("No data received. Connection closed by the ceilometer.")
                        break

                    # Check if it's time to start a new file
                    current_file_path = create_dat_file_path(base_folder, lat, lon)
                    if current_file_path != dat_file_path:
                        f.close()
                        dat_file_path = current_file_path
                        print(f"Starting a new file: {dat_file_path}")
                        f = open(dat_file_path, 'ab')

                    # Append received data to buffer
                    buffer += data

                    # Look for the end marker (`\x04`)
                    while END_MARKER in buffer:
                        end_idx = buffer.index(END_MARKER) + 1  # Include the `\x04` in the packet

                        # Extract the complete packet up to the end marker `\x04`
                        complete_packet = buffer[:end_idx]

                        # Check for the start of the packet using `\x01`
                        if START_MARKER in complete_packet:
                            start_idx = complete_packet.index(START_MARKER)

                            # Capture the current timestamp
                            current_time = datetime.utcnow().strftime('-%Y-%m-%d %H:%M:%S\n').encode('ascii')

                            # Write the timestamp on a new line before the packet
                            f.write(b"\n")  # Ensure the new line before the timestamp
                            f.write(current_time)  # Write the timestamp
                            print(current_time.decode('ascii'), end='')

                            # Then, write the packet starting from the start marker
                            packet_data = complete_packet[start_idx:]
                            f.write(packet_data)
                            f.write(b"\n")  # Ensure a new line after the packet
                            print(packet_data.decode('ascii', errors='replace'))

                        # Flush to ensure data is saved immediately
                        f.flush()

                        # Remove the processed part from the buffer
                        buffer = buffer[end_idx:]
                        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)  # Exit the script on error

if __name__ == "__main__":
    # Set the base folder, latitude, and longitude for the CL31
    base_folder = '/home/cl31c/CL31/raw'
    latitude = "45.4945"  # Replace with actual lat
    longitude = "-119.0206"  # Replace with actual lon
    
    # Start capturing data
    capture_ceilometer_data(base_folder, latitude, longitude)
