import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime
from typing import List, Optional
from charting.generate_gifs import *
from utilities.db_utils import *
from config.config import *
from pydantic import BaseModel

db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME, logger)
db.connect()
print(f'{db.get_status()}')

class FlowParameters(BaseModel):
    strike_range_start: int
    strike_range_end: int
    session_date: datetime
    participants: List[str]
    position_type: str
    expiration: Optional[datetime] = None
    webhook_url: str = 'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fetch_data(session_date: str) -> pd.DataFrame:
    # query = f"""
    # SELECT * FROM intraday.intraday_books_test_posn
    # WHERE effective_date = '{session_date}'
    # """

    query =f"""
    SELECT * FROM intraday.intraday_books_test_posn
    WHERE effective_date = '2024-08-16'
    -- and effective_datetime < '2024-08-15 18:10:00'
    and strike_price between 5300 and 5700
    and expiration_date = '2024-08-16 16:00:00'
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


# @flow(name="Gifs")
# def gif_flow(params: FlowParameters):
#     session_date = params.session_date.strftime('%Y-%m-%d')
#     strike_ranges = [params.strike_range_start, params.strike_range_end]
#     expiration = params.expiration.strftime('%Y-%m-%d') if params.expiration else session_date
#
#     df = fetch_data(session_date)
#
#     for participant in params.participants:
#         success = process_data(df, session_date, params.position_type, participant,
#                                strike_ranges, expiration, params.webhook_url)
#         if success:
#             print(f"Successfully processed intraday data for {participant} on {session_date}")
#         else:
#             print(f"Failed to process intraday data for {participant} on {session_date}")
#
#
# if __name__ == "__main__":
#     params = FlowParameters(
#         strike_range_start=5350,
#         strike_range_end=5600,
#         session_date=datetime.now(),
#         participants=['total_customers'],
#         position_type='P',
#         expiration=None
#     )
#     gif_flow(params)

@flow(name="Gifs")
def gif_flow(
        session_date: Optional[str] = None,
        strike_ranges: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'total_customers',
        position_type: str = 'Net',
        webhook_url: str = 'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'

        # 'https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ' #'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'

):
    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_ranges is None:
        strike_ranges = [5300, 5700]
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
    gif_flow()