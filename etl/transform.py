from prefect import task
import pandas as pd

@task()
def transform_data(dataframe):

    # getting only useful columns:
    raw = dataframe[['location.country', 'location.name', 'location.localtime', 'current.temperature', 'current.wind_speed', 'current.wind_degree', 'current.wind_dir', 'current.pressure', 'current.humidity', 'current.cloudcover', 'current.visibility', 'current.is_day']]
    df = raw.copy()

    # data cleaning:
    df['current.is_day'] = df['current.is_day'].replace({'yes':1, 'no':0}).astype(bool)
    df['location.localtime'] = pd.to_datetime(df['location.localtime'])
    df.rename(columns={'location.country':'country', 'location.name':'city', 'location.localtime':'localtime', 'current.temperature':'temperature', 'current.wind_degree':'wind_degree', 'current.wind_speed':'wind_speed', 'current.wind_dir':'wind_direction', 'current.pressure':'pressure', 'current.humidity':'humidity', 'current.cloudcover':'cloudcover', 'current.visibility':'visibility', 'current.is_day':'is_day'}, inplace=True)
    
    return df