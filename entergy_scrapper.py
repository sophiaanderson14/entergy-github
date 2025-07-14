import requests
import pandas as pd
from datetime import datetime
import os

# --- Best Practice: Define constants at the top ---
# The single, permanent destination file for historical data.
DESTINATION_FILE = 'data/entergy_outage_history.csv'

def fetch_current_entergy_data(location, area):
    """
    Fetches the latest outage data from the Entergy API for a specific location.
    
    This function is responsible ONLY for getting and formatting the *new* data.
    """
    url = f"https://entergy.datacapable.com/datacapable/v1/entergy/Entergy{location}/{area}"
    print(f"Fetching new data from: {url}")
    
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

    new_outages_df = pd.DataFrame(data)

    if new_outages_df.empty:
        print("No new outage data returned from API.")
        return None

    # --- Your data enrichment logic ---
    new_outages_df["utility"] = "Entergy"
    
    # Format the 'time pulled' column as MM-DD-YYYY
    new_outages_df["time pulled"] = datetime.now().strftime('%m-%d-%Y')
    
    # Calculate percentage without power
    if "customersAffected" in new_outages_df.columns and "customersServed" in new_outages_df.columns:
        new_outages_df["percentageWithoutPower"] = (
            (new_outages_df["customersAffected"].astype(float) / new_outages_df["customersServed"].astype(float) * 100)
            .round(2).astype(str) + "%"
        )
    else:
        new_outages_df["percentageWithoutPower"] = "N/A"
        print("Warning: 'customersAffected' or 'customersServed' columns not found.")
    
    return new_outages_df


def main():
    """
    Main execution function to run the data collection and saving process.
    """
    # --- Step 1: Fetch the new data ---
    location = "Louisiana"  # <-- Corrected location
    area = "County"         # <-- Changed to "Parish" as is standard for Louisiana
    new_data_df = fetch_current_entergy_data(location, area)

    # If fetching failed or returned no data, stop the script.
    if new_data_df is None:
        print("Halting script because no new data was fetched.")
        return

    # --- Step 2: Load existing data (if it exists) ---
    try:
        history_df = pd.read_csv(DESTINATION_FILE)
        print(f"Successfully loaded {len(history_df)} historical records from {DESTINATION_FILE}")
        
        # --- Step 3: Combine historical and new data ---
        combined_df = pd.concat([history_df, new_data_df], ignore_index=True)
        print(f"Appended {len(new_data_df)} new records.")

    except FileNotFoundError:
        print(f"'{DESTINATION_FILE}' not found. This must be the first run.")
        print("Creating a new history file with the newly fetched data.")
        combined_df = new_data_df

    # --- Step 4: Save the updated data ---
    os.makedirs(os.path.dirname(DESTINATION_FILE), exist_ok=True)
    
    combined_df.to_csv(DESTINATION_FILE, index=False)
    print(f"Successfully saved {len(combined_df)} total records to {DESTINATION_FILE}")


if __name__ == "__main__":
    main()
