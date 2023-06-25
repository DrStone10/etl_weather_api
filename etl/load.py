from sqlalchemy import create_engine
from prefect import task
import pandas as pd

@task()
def load_data(dataframe, database):
    engine = create_engine(database)
    dataframe.to_sql(con=engine, name='weather', if_exists='append', index=False)