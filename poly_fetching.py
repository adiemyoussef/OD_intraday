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
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s: %(lineno)s} %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
client = RESTClient("sOqWsfC0sRZpjEpi7ppjWsCamGkvjpHw")


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

def chunker(df, size):
    """
    Objectif: (......)
    :param df: The dataframe we want to chunk
    :param size: The size of the chunk we want to insert
    :return:
    """
    return (df[pos:pos + size] for pos in range(0, len(df), size))

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
            # tqdm._instances.clear()

def read_table(engine,query:str, *args, param = None):
    """
    Objectif:

    Get data from mysql using a generic query, with or without parameters (param)

    :return:
    data (type: pd.DataFrame)
    """

    logging.info("Reading table...")
    data = pd.read_sql(query, con=engine, params=param)

    #print(self.data)

    return data

# Function to get the current time in Eastern Time
def get_eastern_time():
    eastern = pytz.timezone('America/New_York')
    naive_timestamp = datetime.datetime.now()
    eastern_timestamp = naive_timestamp.astimezone(eastern)
    return eastern_timestamp.strftime("%Y-%m-%d %H:%M:%S")

def fetch_options_data(engine,start_time,end_time, run_id):

    # Only run on weekdays
    if datetime.datetime.now(pytz.timezone('America/New_York')).weekday() < 5:  # Monday to Friday are 0-4
        current_time = datetime.datetime.now(pytz.timezone('America/New_York')).time()
        if start_time.time() <= current_time <= end_time.time():

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
            df_final["time_stamp"] = df_final["time_stamp"] - pd.Timedelta(minutes=15)

            df_final.sort_values(['expiration_date','open_interest'],ascending= [True,False],inplace=True)

            # Save the DataFrame to a CSV file
            time_stamp_for_file = time.strftime("%Y%m%d-%H%M%S")
            #df_final.to_csv(f'poly_options_data_{time_stamp_for_file}.csv', index=False)

            start_time = time.time()
            insert_with_progress(engine,"landing", df_final,"poly_options_data")
            end_time = time.time()

            # Calculate the total time taken
            time_taken = end_time - start_time
            logging.info(f"Time taken to insert options chain: {time_taken} seconds")

        else:
            logging.info("Current time is outside the scheduled start and end times.")


def main():


    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep", type=int, default=1, help="Sleep time in seconds between checks.")
    parser.add_argument("--frequency", type=int, default=5, help="Frequency in minutes to fetch data.")
    parser.add_argument("--start", type=str, default="04:00", help="Start time (HH:MM) in Eastern Time.")
    parser.add_argument("--end", type=str, default="23:59", help="End time (HH:MM) in Eastern Time.")
    args = parser.parse_args()

    engine = create_db_engine()
    run_id = get_run_id(engine)
    # Create Eastern timezone object
    eastern = pytz.timezone('America/New_York')
    today = datetime.datetime.now(pytz.timezone('America/New_York')).date()
    args.start_time = eastern.localize(datetime.datetime.combine(today, datetime.datetime.strptime(args.start, "%H:%M").time()))
    args.end_time = eastern.localize(datetime.datetime.combine(today, datetime.datetime.strptime(args.end, "%H:%M").time()))

    schedule.every(args.frequency).minutes.do(lambda: fetch_options_data(engine,args.start_time, args.end_time,run_id))
    logging.info("Scheduler started.")
    while True:
        schedule.run_pending()
        time.sleep(args.sleep)


if __name__ == "__main__":
    main()





