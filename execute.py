from prefect import flow
import pandas as pd
import yaml

# importing the extract, transform, load functions:
from etl import *

@flow(name='api_etl', log_prints=True)
def main():

    # reading the config file:
    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # loop over all cities:
    for city in config['cities']:

        # api settings:
        params = {
            'access_key': config['Key'],
            'query' : city
        }
        headers = {
            'Authorization': 'Bearer {token}'.format(token=config['Key']),
            'Content-Type': 'application/json'
        }

        # etl process:
        raw_data = extract_data(config['url'], headers, params)
        data = transform_data(raw_data)
        load_data(data, config['database'])

        print(f'Data Loaded Seccessfully for {city}')

if __name__ == '__main__':
    main()