import os
import requests
from pathlib import Path

def download_file(url, dest_path):
    """Download a file from a URL to a destination path"""
    print(f"Downloading {url} to {dest_path}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Download complete!")

def main():
    # Create data directory if it doesn't exist
    data_dir = Path('../data')
    data_dir.mkdir(exist_ok=True)
    
    # NYC Yellow Taxi data URL (small sample from 2022-01)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
    dest_path = data_dir / "yellow_taxi_2022_01.parquet"
    
    # Download the file if it doesn't exist
    if not dest_path.exists():
        download_file(url, dest_path)
    else:
        print(f"File already exists at {dest_path}")

if __name__ == "__main__":
    main()