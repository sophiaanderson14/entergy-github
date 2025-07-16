import requests
import pandas as pd
from datetime import datetime
import os

# --- 1. Configuration ---
# Define the single, permanent file where all data will be stored.
DESTINATION_FILE = 'data/louisiana_outage_history.csv'
# Define the parameters for the API call.
LOCATION = "Louisiana"
AREA = "Parish"

# --- 2. Fetch New Data from API ---
url = f"https://entergy.datacapable.com/datacapable/v1/entergy/Entergy{LOCATION}/{AREA}"
print(f"Fetching new data from: {url}")

try:
    # Make the web request with a timeout for safety.
    r = requests.get(url, timeout=15)
    # Check if the request was successful (e.g., no 404 or 500 errors).
    r.raise_for_status()
    # Convert the response to JSON format.
    data = r.json()
except requests.exceptions.RequestException as e:
    # If the request fails, print an error and stop the script.
    print(f"Error fetching data: {e}")
    exit() # exit() stops the script.

# --- 3. Process the Newly Fetched Data ---
# Convert the new data into a pandas DataFrame.
new_data_df = pd.DataFrame(data)

# If the API returned no data, print a message and stop the script.
if new_data_df.empty:
    print("No new outage data was returned from the API. Halting script.")
    exit()

# Add a precise timestamp column. Formatting to a full timestamp is crucial
# since the script runs every 10 minutes.
new_data_df["time_pulled"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
new_data_df["utility"] = "Entergy"

# Calculate the percentage without power, handling potential missing columns.
if "customersAffected" in new_data_df.columns and "customersServed" in new_data_df.columns:
    new_data_df["percentage_without_power"] = (
        (new_data_df["customersAffected"].astype(float) / new_data_df["customersServed"].astype(float) * 100)
        .round(2).astype(str) + "%"
    )
else:
    new_data_df["percentage_without_power"] = "N/A"
    print("Warning: 'customersAffected' or 'customersServed' columns not found.")

# Reorder columns for consistency in the final CSV file.
# NOTE: Adjust this list to match the exact order you want.
final_columns = [
    'time_pulled',
    'areaName',
    'customersAffected',
    'customersServed',
    'percentage_without_power',
    'utility'
    # Add other columns from the API that you want to keep, in your desired order.
]
# Filter the DataFrame to only include and order the columns we want.
# We use .reindex() to avoid errors if a column is missing.
new_data_df = new_data_df.reindex(columns=final_columns)


# --- 4. Append New Data to the History File ---
# This is the new, efficient method. We no longer read the old file.

# First, ensure the 'data/' directory exists.
os.makedirs(os.path.dirname(DESTINATION_FILE), exist_ok=True)

# Check if the file already exists to determine if we need to write the header.
file_exists = os.path.exists(DESTINATION_FILE)

print(f"File '{DESTINATION_FILE}' exists: {file_exists}")
print(f"Appending {len(new_data_df)} new records.")

# Use mode='a' to append. Write the header only if the file is new.
new_data_df.to_csv(
    DESTINATION_FILE,
    mode='a',
    header=not file_exists, # Write header only if file does not exist
    index=False
)

print(f"Successfully appended data to {DESTINATION_FILE}")
