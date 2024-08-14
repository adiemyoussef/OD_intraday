import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime
from typing import List, Optional
from charting.generate_gifs import *
from utilities.db_utils import *
from config.config import *


db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME, logger)
db.connect()
print(f'{db.get_status()}')

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fetch_data(session_date: str) -> pd.DataFrame:
    # query = f"""
    # SELECT * FROM intraday.intraday_books_test_posn
    # WHERE effective_date = '{session_date}'
    # and effective_datetime
    # """

    query =f"""
    SELECT * FROM intraday.intraday_books_test_posn
    WHERE effective_date = '2024-08-14'
    and effective_datetime < '2024-08-14 10:10:00'
    and strike_price between 5200 and 5400
    and expiration_date_original = '2024-08-14'
    """

    return db.execute_query(query)


@task
def process_data(df: pd.DataFrame, session_date: str, position_type: str, participant: str,
                 strike_ranges: List[int], expiration: str, webhook_url: str) -> bool:
    expiration_input = pd.to_datetime(expiration).strftime('%Y-%m-%d')
    daily_data = df.copy()
    daily_data['expiration_date_original'] = pd.to_datetime(daily_data['expiration_date_original']).dt.strftime(
        '%Y-%m-%d')
    daily_data = daily_data[daily_data['expiration_date_original'] == expiration_input]

    # generate_and_send_gif(data, session_date, participant, position_type, strike_input, expiration, webhook_url)
    #
    success = generate_and_send_gif(daily_data, session_date, participant, position_type,
                                    strike_ranges, expiration, webhook_url)
    return success


@flow(name="Gifs")
def intraday_flow(
        session_date: Optional[str] = None,
        strike_ranges: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'nonprocust',
        position_type: str = 'P',
        webhook_url: str = 'https://discord.com/api/webhooks/1251013946111164436/VN55yOK-ntil-PnZn1gzWHzKwzDklwIh6fVspA_I8MCCaUnG-hsRsrP1t_WsreGHIty'
):
    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_ranges is None:
        strike_ranges = [5300, 5400]
    if expiration is None:
        expiration = session_date

    df = fetch_data(session_date)
    success = process_data(df, session_date, position_type, participant,
                           strike_ranges, expiration, webhook_url)

    if success:
        print(f"Successfully processed intraday data for {session_date}")
    else:
        print(f"Failed to process intraday data for {session_date}")


if __name__ == "__main__":
    intraday_flow()