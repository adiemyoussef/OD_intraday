import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash, Task
from datetime import timedelta, datetime, date
from typing import List, Optional
#from charting.generate_gifs import generate_gif, send_to_discord
from charting.generate_gifs import generate_gif, send_to_discord,generate_video
from utilities.db_utils import DatabaseUtilities
from config.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
import os
from datetime import datetime, time

DEV_CHANNEL ='https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
#https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ

default_date = date.today() - timedelta(days=0)
LIST_PART = ['total_customers', 'broker', 'firm', 'retail', 'institution']


db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
db.connect()
print(f'{db.get_status()}')

def get_next_expiration_date(current_date: date) -> date:
    # This function should return the next expiration date (usually the next Friday)
    # For simplicity, let's assume it's always the next day

    #TODO: Use NEXT trading day, not next day
    return current_date + timedelta(days=1)

@task
def parse_strike_range(strike_range: str) -> List[int]:
    values = list(map(int, strike_range.split(',')))
    return [min(values), max(values)]

@task(cache_key_fn=None, cache_expiration=timedelta(hours=0, minutes=1))
def fetch_data(session_date: str, strike_range: List[int], expiration: str, start_video:str) -> [pd.DataFrame]:

    start_of_video = f'{session_date} {start_video}'

    metrics_query =f"""
    SELECT * FROM intraday.intraday_books
    WHERE effective_date = '{session_date}'
    and effective_datetime >= '{start_of_video}'
    and strike_price between {strike_range[0]} and {strike_range[1]}
    and expiration_date_original = '{expiration}'
    """

    candlesticks_query = f"""
    SELECT * FROM optionsdepth_stage.charts_candlestick
    where effective_date = '{session_date}'
    and
    ticker = 'SPX'
    """

    last_price_query = f"""
    SELECT close FROM optionsdepth_stage.charts_candlestick 
    WHERE id = (SELECT MAX(id) FROM optionsdepth_stage.charts_candlestick WHERE ticker = 'SPX')
    """


    metrics = db.execute_query(metrics_query)
    candlesticks = db.execute_query(candlesticks_query)
    last_price = db.execute_query(last_price_query)


    if candlesticks.empty:
        print("No candlesticks Available")

    else:
        candlesticks['effective_datetime'] = (
            pd.to_datetime(candlesticks['effective_datetime'], utc=True)  # Set timezone to UTC
            .dt.tz_convert('America/New_York')  # Convert to Eastern Time
            .dt.tz_localize(None)  # Remove timezone information (make naive)
        )
        candlesticks.drop_duplicates(keep='first', inplace=False)


    return metrics, candlesticks, last_price

@task
def process_data(metric: pd.DataFrame,candlesticks: pd.DataFrame, session_date: str, participant: str,
                 strike_range: List[int], expiration: str, position_types: List[str]) -> List[str]:
    gif_paths = []
    for position_type in position_types:
        gif_path = generate_gif(
            metric,candlesticks, session_date, participant, position_type,
            strike_range, expiration,
            output_gif=f'animated_chart_{position_type}.gif'
        )
        if gif_path:
            gif_paths.append(gif_path)
        else:
            print(f"Failed to generate GIF for {position_type}")
    return gif_paths

@task
def generate_video_task(data: pd.DataFrame, candlesticks: pd.DataFrame, session_date: str, participant: str,
                        strike_range: List[int], expiration: str, position_type: list, last_price:float, metric:str = 'positioning') -> str:

    videos_paths =[]

    for pos_type in position_type:

        video_path = generate_video(
            data, candlesticks, session_date, participant, pos_type,
            strike_range, expiration, metric, last_price,
            output_video=f'{pos_type}_{expiration}_animated_chart.mp4'
        )
        videos_paths.append(video_path)

    return videos_paths

@task
def send_discord_message(file_paths: List[str], as_of_time_stamp:str, session_date: str, participant: str,
                         strike_range: List[int], expiration: str, position_types: List[str], metric: str,
                         webhook_url: str) -> bool:
    participant_mapping = {
        'mm': 'Market Makers',
        'broker': 'Brokers and Dealers',
        'firm': 'Firms',
        'nonprocus': 'Non-Professional Customers',
        'procust': 'Professional Customers',
        'total_customers': 'Total Customers'
    }
    participant_text = participant_mapping.get(participant, 'Unknown Participant')

    title = f"📊 {session_date} Intraday Recap"
    description = (
        f"Detailed analysis of {participant_text} {metric} for the {session_date} session.\n"
        f"This chart provides insights into market movements and positioning within the specified strike range.\n"
        ""
    )
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fields = [
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "📈 Analysis Type", "value": f"Intraday {metric} Movement", "inline": True},
        {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
        {"name": "", "value":"", "inline": True},
        {"name": "👥 Participant(s)", "value": participant_text, "inline": True},
        {"name": "🎯 Strike Range", "value": f"{strike_range[0]} - {strike_range[1]}", "inline": True},
        {"name": "📅 Expiration(s)", "value": expiration, "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "💡 Interpretation", "value": (
            "• Green bars indicate Calls positioning\n"
            "• Red bars indicate Puts positioning\n"
            "• Blue bars indicate Net positioning\n"
            "\n"
            "• ■ represents current position magnitude\n"
            "• ● represents the start of day position magnitude\n"
            "• ✖ represents the prior update position magnitude\n"
            "\n"
        ), "inline": False},
    ]
    footer_text = f"Generated on {current_time} | By OptionsDepth Inc."

    success = send_to_discord(
        webhook_url,
        file_paths,
        content="🚀 New options chart analysis is ready! Check out the latest market insights below.",
        title=title,
        description=description,
        fields=fields,
        footer_text=footer_text
    )

    # Clean up the gif files after sending
    for path in file_paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            print(f"Warning: Could not delete file {path}. It may have already been deleted.")

    return success

@flow(name="0DTE gifs")
def zero_dte_flow(
    session_date: Optional[date] = default_date,
    strike_range: Optional[List[int]] = None,
    expiration: Optional[str] = None,
    participant: str = 'total_customers',
    position_types: Optional[List[str]] = None,
    webhook_url: str = 'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'
                        #DEV_CHANNEL
    ):
    if strike_range:
        strike_range = parse_strike_range(strike_range)


    position_types = ['Net','C','P']
    expiration = str(session_date)


    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = [5500, 5700]
    if expiration is None:
        expiration = session_date
    if position_types is None:
        position_types = ['C', 'P', 'Net']
    elif 'All' in position_types:
        position_types = ['C', 'P', 'Net']



    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")

    # Fetch data
    metrics, candlesticks, last_price = fetch_data(session_date, strike_range, expiration, start_time)

    as_of_time_stamp = str(metrics["effective_datetime"].max())
    last_price = last_price.values[0][0]
    # Process data and generate GIFs
    #gif_paths = process_data(metrics, candlesticks, session_date, participant, strike_range, expiration, position_types)

    videos_paths = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types, last_price ,'positioning')
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range, expiration, position_types,'positioning', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")

@flow(name="1DTE gifs")
def one_dte_flow(
    session_date: Optional[date] = default_date,
    strike_range: Optional[List[int]] = None,
    expiration: Optional[str] = None,
    participant: str = 'total_customers',
    position_types: Optional[List[str]] = None,
    webhook_url: str = 'https://discord.com/api/webhooks/1275269470151245938/qNZXtA_ySwcJJJf6bS_myYqU-uDd71zHV--XJBR7xb6uVhs7ccjKE59_c8y9AMZ86OC_'
                    #DEV_CHANNEL
):
    # session_date = '2024-08-21'
    # participant = 'firm'
    #expiration = '2024-08-27'

    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = [5300, 5750]
    # if strike_range:
    #     strike_range = parse_strike_range(strike_range)
    if position_types is None:
        position_types = ['Net','C','P']
    if expiration is None:
        #TODO: Next business day not day
        expiration = str(get_next_expiration_date(session_date))
    elif 'All' in position_types:
        position_types = ['C', 'P', 'Net']



    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")

    # Fetch data
    data, candlesticks, last_price = fetch_data(session_date, strike_range, expiration, start_time)

    as_of_time_stamp = str(data["effective_datetime"].max())
    last_price = last_price.values[0][0]
    # Process data and generate GIFs
    #gif_paths = process_data(data, candlesticks, session_date, participant, strike_range, expiration, position_types)

    videos_paths = generate_video_task(data, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types,last_price ,'positioning')
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range, expiration, position_types,'positioning', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")


# @flow(name="DEX gifs")
# def DEX_flow(
#     session_date: Optional[date] = default_date,
#     strike_range: Optional[List[int]] = None,
#     expiration: Optional[str] = None,
#     participant: str = 'total_customers',
#     position_types: Optional[List[str]] = None,
#     webhook_url: str = DEV_CHANNEL
# ):
#     pass

@flow(name="GEX gifs")
def GEX_flow(
        session_date: Optional[date] = None,
        strike_range: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'mm',
        position_types: Optional[List[str]] = None,
        webhook_url: str = 'https://discord.com/api/webhooks/1277599354932428842/c2Ix3cPdLzI0fzxDdoGRye8nyKPLZj0dqmIxOiRQP2DYFx7YbgphUe8rAsWqkZUKiD0f'
                            #DEV_CHANNEL
):
    expiration= '2024-08-26'

    if session_date is None:
        #TODO: the latest effective_date of the book
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = [5500, 5750]
    if position_types is None:
        position_types = ['Net']

    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")
    # Fetch data
    metrics, candlesticks,last_price = fetch_data(session_date, strike_range, expiration, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())

    last_price = last_price.values[0][0]

    videos_paths = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types, last_price,"GEX")
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range, expiration, position_types,'GEX', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")
    pass

if __name__ == "__main__":
    #zero_dte_flow()
    one_dte_flow()
    #GEX_flow()