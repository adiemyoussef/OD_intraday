import pytz
import schedule
from datetime import datetime, time as time_obj
import argparse
from polygon import RESTClient
import datetime
import glob
import sys
import time
import math
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import logging
from prefect import flow, task
from prefect.logging import get_run_logger
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s: %(lineno)s} %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])

client = RESTClient("sOqWsfC0sRZpjEpi7ppjWsCamGkvjpHw")

def get_eastern_time():
    eastern = pytz.timezone('America/New_York')
    naive_timestamp = datetime.datetime.now()
    eastern_timestamp = naive_timestamp.astimezone(eastern)
    return eastern_timestamp.strftime("%Y-%m-%d %H:%M:%S")

def chunker(df, size):
    """
    Objectif: (......)
    :param df: The dataframe we want to chunk
    :param size: The size of the chunk we want to insert
    :return:
    """
    return (df[pos:pos + size] for pos in range(0, len(df), size))
@task(name = "Creating engine")
def create_db_engine():
    logging.info(f"Opening Connection ...")
    try:
        # Create the database engine
        engine = create_engine(
            "mysql+mysqlconnector://doadmin:AVNS_lRx29D8jydyJ2lmQhan@db-mysql-nyc3-94751-do-user-12097851-0.b.db.ondigitalocean.com:25060/landing",
            echo=False, poolclass=NullPool
        )
        if engine:
            print(f"Engine Created: {engine}")
            return engine
    except (Exception) as e:
        logging.exception(str(e))
        return None
@task(name = "Getting run id")
def get_run_id(engine):
    if engine:
        try:
            query = "SELECT MAX(id) FROM landing.poly_options_data"
            run_id = pd.read_sql(query, con=engine).values[0][0] or 0
            return run_id + 1
        except Exception as e:
            logging.exception("Failed to fetch run_id: " + str(e))
            return 0
    return 0

@task(name = "Inserting to table")
def insert_with_progress(engine, db_name: str, dataframe: pd.DataFrame, dbTable: str):
    """
    Objectif: Fonction qui permet de faire l'insertion d'un dataframe dans une table avec une barre de progression
    :param dataframe: Le Dataframe à inserer doit etre de la meme forme que la table dans lq BD
    :param dbTable: La table ou on veut inserer le dataframe
    """

    chunksize = math.ceil(len(dataframe) / 50)

    with tqdm(total=len(dataframe)) as pbar:
        for i, cdf in enumerate(chunker(dataframe, chunksize)):
            replace = "replace" if i == 0 else "append"
            inserted = cdf.to_sql(dbTable, schema=db_name, con=engine, index=False, if_exists='append')
            pbar.update(chunksize)

@task(name = "Reading table")
def read_table(engine,query:str, *args, param = None):
    """
    Objectif:

    Get data from mysql using a generic query, with or without parameters (param)

    :return:
    data (type: pd.DataFrame)
    """

    logging.info("Reading table...")
    data = pd.read_sql(query, con=engine, params=param)


    return data

# Function to get the current time in Eastern Time


@task(name= ' Fetch options chain')
def fetch_options_data(engine, run_id):
    prefect_logger = get_run_logger()

    prefect_logger.info("Current time is within the scheduled start and end times.")
    logging.info("Current time is within the scheduled start and end times.")

    run_id += 1  # Increment the run ID

    options_chain = []
    # Start the timer
    start_time = time.time()
    for o in client.list_snapshot_options_chain(
        "I:SPX",
        params={
            "limit" : 250
        },
    ):
        options_chain.append(o)

    end_time = time.time()

    # Calculate the total time taken
    time_taken = end_time - start_time
    prefect_logger.info(f"Time taken to fetch options chain: {time_taken} seconds")
    logging.info(f"Time taken to fetch options chain: {time_taken} seconds")


    df_options_chain = pd.DataFrame(options_chain)
    df_options_chain.sort_values('open_interest',ascending= False,inplace=True)
    df_cleaned = df_options_chain.dropna(subset=['implied_volatility'])

    df_cleaned = df_cleaned.copy()
    df_cleaned['id'] = df_cleaned.index


    df_expanded = pd.json_normalize(df_cleaned['details'])
    greeks_df = df_cleaned['greeks'].apply(pd.Series)
    # Add back the identifier to df_expanded
    df_expanded['id'] = df_cleaned['id'].values
    greeks_df['id'] = df_cleaned['id'].values
    # Merge 'implied_volatility' and 'open_interest' using the identifier
    df_expanded = pd.merge(df_expanded, df_cleaned[['id', 'implied_volatility', 'open_interest']], on='id')
    df_expanded = pd.merge(df_expanded, greeks_df,on='id')
    # Extract 'option_symbol' from the 'ticker' column
    df_expanded['option_symbol'] = df_expanded['ticker'].str.extract(r':([A-Za-z]+)\d')


    # Select and arrange the required columns
    df = df_expanded[['option_symbol', 'contract_type', 'expiration_date', 'strike_price', 'implied_volatility', 'open_interest','delta', 'gamma', 'theta','vega']]
    df_final = df.copy()
    df_final['contract_type'] = df_final['contract_type'].replace({'call': 'C', 'put': 'P'})

    # Add timestamp and run ID as the first two columns
    # Usage example
    timestamp = get_eastern_time()
    logging.info(f'Timestamp: {timestamp}')
    df_final.insert(0, 'time_stamp', timestamp)
    df_final.insert(0, 'id', run_id)

    # Adjustment to be removed once Real-time data is available
    df_final["time_stamp"] = pd.to_datetime(df_final['time_stamp'])

    df_final.sort_values(['expiration_date','open_interest'],ascending= [True,False],inplace=True)


    start_time = time.time()
    insert_with_progress(engine,"landing", df_final,"poly_options_data")
    prefect_logger.info(f"Time taken to fetch options chain: {time.time() - start_time} seconds")
    logging.info(f"Time taken to insert options chain: {time.time() - start_time} seconds")


@flow(name="Gather options chains")
def poly_flow():
    engine = create_db_engine()
    run_id = get_run_id(engine)
    fetch_options_data(engine, run_id)

if __name__ == "__main__":
    poly_flow()





