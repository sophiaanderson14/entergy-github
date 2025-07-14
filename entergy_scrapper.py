import requests
import pandas as pd
from datetime import datetime
import math
from time import sleep
import os

def current_entergy(location,area):
    #opening base URL
    url = "https://entergy.datacapable.com/datacapable/v1/entergy/Entergy{}/{}".format(location,area)
    print(url)
    #get current time
    now = datetime.now()
    #go to webpage
    r = requests.get(url)
    #convert into json
    data = r.json()
    #convert into pandas dataframe
    entergy = pd.DataFrame(data)
    #label utility as entergy
    entergy["utility"] = "Entergy"
    #add current time to a column
    entergy["time pulled"] = now
    # ... previous code ...
    entergy["utility"] = "Entergy"
    entergy["time pulled"] = now


    if "customersAffected" in entergy.columns and "customersServed" in entergy.columns:
        entergy["percentageWithoutPower"] = (
            (entergy["customersAffected"].astype(float) / entergy["customersServed"].astype(float) * 100)
            .round(2).astype(str) + "%"
        )
    else:
        entergy["percentageWithoutPower"] = ""  # Or use "N/A"
        raise ValueError(
        "Column 'percentageWithoutPower' not found in DataFrame. Available columns: " + ', '.join(entergy.columns)
    )

    df = pd.read_csv('louisiana-county.csv')
    df['time pulled'] = pd.to_datetime(df['time pulled']).dt.strftime('%m-%d-%Y')
    os.makedirs(os.path.dirname('louisiana-county.csv'), exist_ok=True)
    filename = f"{location.lower()}-{area}.csv"
    entergy.to_csv(filename, index=False)
    print(f"Saved updated data to {filename}")
    return entergy
