import requests
import pandas as pd
from datetime import datetime
import os

# --- 1. Configuration ---
# Define the single, permanent file where all data will be stored.
# CORRECTED to use the filename you specified.
DESTINATION_FILE = 'louisiana-county.csv' 
# Define the parameters for the API call.
LOCATION = "Louisiana"
AREA = "Parish" # Note: The API uses "Parish", but we are saving to "county" as requested.

# --- 2. Fetch New Data from API ---
url = f"https://entergy.datacapable.com/datacapable/v1/entergy/Entergy{LOCATION}/{AREA}"
print(f"Fetching new data from: {url}")

try:
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
    exit()

# --- 3. Process the Newly Fetched Data ---
new_data_df = pd.DataFrame(data)

if new_data_df.empty:
    print("No new outage data was returned from the API. Halting script.")
    exit()

new_data_df["time_pulled"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
new_data_df["utility"] = "Entergy"

if "customersAffected" in new_data_df.columns and "customersServed" in new_data_df.columns:
    new_data_df["percentage_without_power"] = (
        (new_data_df["customersAffected"].astype(float) / new_data_df["customersServed"].astype(float) * 100)
        .round(2).astype(str) + "%"
    )
else:
    new_data_df["percentage_without_power"] = "N/A"
    print("Warning: 'customersAffected' or 'customersServed' columns not found.")

# Reorder columns for consistency in the final CSV file.
final_columns = [
    'time_pulled',
    'areaName',
    'customersAffected',
    'customersServed',
    'percentage_without_power',
    'utility'
]
new_data_df = new_data_df.reindex(columns=final_columns)

# --- 4. Append New Data to the History File ---
os.makedirs(os.path.dirname(DESTINATION_FILE), exist_ok=True)

file_exists = os.path.exists(DESTINATION_FILE)

print(f"File '{DESTINATION_FILE}' exists: {file_exists}")
print(f"Appending {len(new_data_df)} new records.")

new_data_df.to_csv(
    DESTINATION_FILE,
    mode='a',
    header=not file_exists,
    index=False
)

print(f"Successfully appended data to {DESTINATION_FILE}")
