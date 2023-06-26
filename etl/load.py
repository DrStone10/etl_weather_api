from colorama import Fore, Style
from sqlalchemy import create_engine
from prefect import task
import pandas as pd

@task(log_prints=True)
def load_data(dataframe, database, name):
    engine = create_engine(database)
    dataframe.to_sql(con=engine, name=name, if_exists='append', index=False)