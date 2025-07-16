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
    exit() # exit() is used to stop a script when not inside a function.

# --- 3. Process the Newly Fetched Data ---
# Convert the new data into a pandas DataFrame.
new_data_df = pd.DataFrame(data)

# If the API returned no data, print a message and stop the script.
if new_data_df.empty:
    print("No new outage data was returned from the API. Halting script.")
    exit()

# Add the new columns to the new data.
new_data_df["utility"] = "Entergy"
new_data_df["time pulled"] = datetime.now() # We will format this later.

if "customersAffected" in new_data_df.columns and "customersServed" in new_data_df.columns:
    new_data_df["percentageWithoutPower"] = (
        (new_data_df["customersAffected"].astype(float) / new_data_df["customersServed"].astype(float) * 100)
        .round(2).astype(str) + "%"
    )
else:
    new_data_df["percentageWithoutPower"] = "N/A"
    print("Warning: 'customersAffected' or 'customersServed' columns not found.")

# --- 4. Load Historical Data and Combine ---
try:
    # Try to read the existing historical file.
    history_df = pd.read_csv(DESTINATION_FILE)
    # If successful, append the new data to the historical data.
    combined_df = pd.concat([history_df, new_data_df], ignore_index=True)
    print(f"Successfully loaded {len(history_df)} records and appended {len(new_data_df)} new records.")
except FileNotFoundError:
    # If the file doesn't exist (first run), the new data is our starting point.
    print(f"'{DESTINATION_FILE}' not found. Creating a new history file.")
    combined_df = new_data_df

# --- 5. Final Formatting ---
# This is the guaranteed formatting step. It runs on the ENTIRE combined DataFrame.
print("Standardizing 'time pulled' column to MM-DD-YYYY format...")
combined_df['time pulled'] = pd.to_datetime(combined_df['time pulled']).dt.strftime('%m-%d-%Y')

filename = f"{location.lower()}-{area}.csv"
df.to_csv(filename, mode='a', index=False, header=False)

# --- 6. Save the Final Result ---
# Ensure the 'data/' directory exists before saving.
os.makedirs(os.path.dirname(DESTINATION_FILE), exist_ok=True)

# Save the final, combined DataFrame back to the single destination file.
combined_df.to_csv(DESTINATION_FILE, index=False)
print(f"Successfully saved {len(combined_df)} total records to {DESTINATION_FILE}")
