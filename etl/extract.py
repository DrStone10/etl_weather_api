from prefect import task
import pandas as pd
import requests

@task()
def extract_data(url, headers, params):

    # extracting the api:
    r = requests.get(url, headers=headers, params=params)
    data = r.json()
    df = pd.json_normalize(data)

    return df