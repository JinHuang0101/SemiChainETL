import requests
import os

# Base URL for raw files
base_url = "https://raw.githubusercontent.com/georgetown-cset/eto-chip-explorer/main/data"
files = ["inputs.csv", "providers.csv", "provision.csv", "sequence.csv", "stages.csv"]

# Directory to save files
save_dir = "raw_data"
os.makedirs(save_dir, exist_ok=True)

# Download each file
for file in files:
    url = f"{base_url}/{file}"
    filename = os.path.join(save_dir, file)
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {file} to {filename}")
    else:
        print(f"Failed to download {file}. Status: {response.status_code}")