import requests
import pandas as pd
from datetime import datetime
import os

# --- Best Practice: Define your single, permanent destination file at the top ---
DESTINATION_FILE = 'data/louisiana_outage_history.csv'

def fetch_current_entergy_data(location, area):
    """
    Fetches the latest outage data from the Entergy API.
    This function is clean and focused only on getting new data.
    """
    url = f"https://entergy.datacapable.com/datacapable/v1/entergy/Entergy{location}/{area}"
    print(f"Fetching new data from: {url}")
    
    # ROBUSTNESS: Handle potential network errors or bad responses from the API.
    try:
        r = requests.get(url, timeout=15) # Add a timeout
        r.raise_for_status() # This will raise an error for bad responses (404, 500, etc.)
        data = r.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None # Return None to signal that the fetch failed

    # Convert into pandas dataframe
    new_outages_df = pd.DataFrame(data)

    # If the API returns no data, exit gracefully.
    if new_outages_df.empty:
        print("No new outage data returned from API.")
        return None

    # --- Data Enrichment ---
    new_outages_df["utility"] = "Entergy"
    # Add the current time. We will format it correctly later.
    new_outages_df["time pulled"] = datetime.now()
    
    # Calculate percentage without power
    if "customersAffected" in new_outages_df.columns and "customersServed" in new_outages_df.columns:
        new_outages_df["percentageWithoutPower"] = (
            (new_outages_df["customersAffected"].astype(float) / new_outages_df["customersServed"].astype(float) * 100)
            .round(2).astype(str) + "%"
        )
    else:
        # If columns are missing, just assign "N/A" and print a warning. Don't crash.
        new_outages_df["percentageWithoutPower"] = "N/A"
        print("Warning: 'customersAffected' or 'customersServed' columns not found.")
    
    return new_outages_df


def main():
    """
    Main execution function to run the entire process.
    """
    # --- Step 1: Fetch the new data ---
    location = "Louisiana"
    area = "Parish"
    new_data_df = fetch_current_entergy_data(location, area)

    # If fetching failed, stop the script.
    if new_data_df is None:
        print("Halting script because no new data was fetched.")
        return

    # --- Step 2: Load historical data and append new data ---
    try:
        # Load the single, permanent history file
        history_df = pd.read_csv(DESTINATION_FILE)
        # Append the new data to the old data
        combined_df = pd.concat([history_df, new_data_df], ignore_index=True)
        print(f"Successfully loaded {len(history_df)} records and appended {len(new_data_df)} new records.")
    except FileNotFoundError:
        # This runs only if the history file doesn't exist (first run)
        print(f"'{DESTINATION_FILE}' not found. Creating a new history file.")
        combined_df = new_data_df
        
    # --- Step 3: GUARANTEED FORMATTING STEP ---
    # Apply formatting to the ENTIRE combined DataFrame as the last step before saving.
    print("Standardizing 'time pulled' column to MM-DD-YYYY format...")
    combined_df['time pulled'] = pd.to_datetime(combined_df['time pulled']).dt.strftime('%m-%d-%Y')

    # --- Step 4: Save the updated, combined data ---
    # Ensure the 'data/' directory exists.
    os.makedirs(os.path.dirname(DESTINATION_FILE), exist_ok=True)
    
    # Save the result back to our single destination file.
    combined_df.to_csv(DESTINATION_FILE, index=False)
    print(f"Successfully saved {len(combined_df)} total records to {DESTINATION_FILE}")


# This standard Python construct makes the script runnable.
if __name__ == "__main__":
    main()
