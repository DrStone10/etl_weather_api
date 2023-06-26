from colorama import Fore, Style
from prefect import task
import pandas as pd
import requests

@task(log_prints=True)
def extract_data(url, headers, params):

    # extracting the api:
    r = requests.get(url, headers=headers, params=params)
    data = r.json()
    df = pd.json_normalize(data)

    print(f"{Fore.CYAN} Extract phase done. {Style.RESET_ALL}")

    return df