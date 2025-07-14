import requests
import pandas as pd
from datetime import datetime
import math
from time import sleep

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
  # Convert decimal to percent, round, and add percent sign, if column exists
    if "percentageWithoutPower" in entergy.columns:
        entergy["percentageWithoutPower"] = (
            (entergy["percentageWithoutPower"].astype(float) * 100).round(2).astype(str) + "%"
        )
    else:
        raise ValueError(
        "Column 'percentageWithoutPower' not found in DataFrame. Available columns: " + ', '.join(entergy.columns)
    )
    filename = f"{location.lower()}-{area}.csv"
    entergy.to_csv(filename, index=False)
    print(f"Saved updated data to {filename}")
    return entergy
