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
    filename = f"{location.lower()}-{area}.csv"
    df.to_csv(filename, mode='a', index=False, header=False)
    print(path)
    entergy.to_csv(path)
    return entergy
