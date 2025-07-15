import requests
import pandas as pd
from datetime import datetime
import os

# --- Best Practice: Define your single, permanent destination file at the top ---
DESTINATION_FILE = 'data/louisiana_outage_history.csv'

def run_update():
    """
    Fetches new Entergy data, combines it with historical data,
    and saves the result to a CSV file.
    """
    # --- Step 1: Fetch the new data ---
    location = "Louisiana"
    area = "Parish"
    url = f"https://entergy.datacapable.com/datacapable/v1/entergy/Entergy{location}/{area}"
    print(f"Fetching new data from: {url}")
    
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return # Stop the function if fetching fails

    new_data_df = pd.DataFrame(data)

    if new_data_df.empty:
        print("No new outage data returned from API.")
        return # Stop the function if there's no data

    # --- Data Enrichment for the new data ---
    new_data_df["utility"] = "Entergy"
    new_data_df["time pulled"] = datetime.now()
    
    if "customersAffected" in new_data_df.columns and "customersServed" in new_data_df.columns:
        new_data_df["percentageWithoutPower"] = (
            (new_data_df["customersAffected"].astype(float) / new_data_df["customersServed"].astype(float) * 100)
            .round(2).astype(str) + "%"
        )
    else:
        new_data_df["percentageWithoutPower"] = "N/A"
        print("Warning: 'customersAffected' or 'customersServed' columns not found.")

    # --- Step 2: Load historical data and append new data ---
    try:
        history_df = pd.read_csv(DESTINATION_FILE)
        combined_df = pd.concat([history_df, new_data_df], ignore_index=True)
        print(f"Successfully loaded {len(history_df)} records and appended {len(new_data_df)} new records.")
    except FileNotFoundError:
        print(f"'{DESTINATION_FILE}' not found. Creating a new history file.")
        combined_df = new_data_df
        
    # --- Step 3: GUARANTEED FORMATTING STEP ---
    print("Standardizing 'time pulled' column to MM-DD-YYYY format...")
    combined_df['time pulled'] = pd.to_datetime(combined_df['time pulled']).dt.strftime('%m-%d-%Y')

    # --- Step 4: Save the updated, combined data ---
    os.makedirs(os.path.dirname(DESTINATION_FILE), exist_ok=True)
    combined_df.to_csv(DESTINATION_FILE, index=False)
    print(f"Successfully saved {len(combined_df)} total records to {DESTINATION_FILE}")


# This standard Python construct makes the script runnable.
if __name__ == "__main__":
    run_update()
