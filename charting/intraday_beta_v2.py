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


default_date = date.today() - timedelta(days=0)
LIST_PART = ['total_customers', 'broker', 'firm', 'retail', 'institution']


db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
db.connect()
print(f'{db.get_status()}')

@task
def parse_strike_range(strike_range: str) -> List[int]:
    values = list(map(int, strike_range.split(',')))
    return [min(values), max(values)]


@task(cache_key_fn=None, cache_expiration=timedelta(hours=0, minutes=1))
def fetch_data(session_date: str, strike_range: List[int], expiration: str) -> [pd.DataFrame]:

    #TODO: effective_datetime adaptive


    metrics_query =f"""
    SELECT * FROM intraday.intraday_books_test_posn
    WHERE effective_date = '{session_date}'
    and effective_datetime >= '2024-08-19 17:00:00'
    and strike_price between {strike_range[0]} and {strike_range[1]}
    and expiration_date_original = '{expiration}'
    """

    candlesticks_query = f"""
    SELECT * FROM optionsdepth_stage.charts_candlestick
    where effective_date = '{session_date}'
    and
    ticker = 'SPX'
    """


    metrics = db.execute_query(metrics_query)
    candlesticks = db.execute_query(candlesticks_query)

    if candlesticks.empty:
        print("No candlesticks Available")

    else:
        candlesticks['effective_datetime'] = (
            pd.to_datetime(candlesticks['effective_datetime'], utc=True)  # Set timezone to UTC
            .dt.tz_convert('America/New_York')  # Convert to Eastern Time
            .dt.tz_localize(None)  # Remove timezone information (make naive)
        )
        candlesticks.drop_duplicates(keep='first', inplace=False)


    return metrics, candlesticks

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
def generate_video_task(metric: pd.DataFrame, candlesticks: pd.DataFrame, session_date: str, participant: str,
                        strike_range: List[int], expiration: str, position_type: list) -> str:

    videos_paths =[]

    for pos_type in position_type:

        video_path = generate_video(
            metric, candlesticks, session_date, participant, pos_type,
            strike_range, expiration,
            output_video=f'{pos_type}_{expiration}_animated_chart.mp4'
        )
        videos_paths.append(video_path)

    return videos_paths

@task
def send_discord_message(file_paths: List[str], as_of_time_stamp:str, session_date: str, participant: str,
                         strike_range: List[int], expiration: str, position_types: List[str],
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
        f"Detailed analysis of {participant_text} positions for the {session_date} session.\n"
        f"This chart provides insights into market movements and positioning within the specified strike range.\n"
        ""
    )
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fields = [
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "📈 Analysis Type", "value": "Intraday Positional Movement", "inline": True},
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

@flow(name="Intraday GIF Generation")
# def gif_flow(
#     session_date: Optional[str] = None,
#     strike_range: Optional[List[int]] = None,
#     expiration: Optional[str] = None,
#     participant: str = 'total_customers',
#     position_types: Optional[List[str]] = None,
#     webhook_url: str = 'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'
# ):
def gif_flow(
    session_date: Optional[date] = default_date,
    #strike_range: Optional[str] = None,
    strike_range: Optional[List[int]] = None,
    expiration: Optional[str] = None,
    participant: str = 'total_customers',
    position_types: Optional[List[str]] = None,
    webhook_url: str = #'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'
                       'https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
):
    if strike_range:
        strike_range = parse_strike_range(strike_range)

    position_types = ['Net'] #, 'C', 'P']
    position_types = ['Net','C','P']
    expiration = '2024-08-20'


    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = [5450, 5650]
    if expiration is None:
        expiration = session_date
    if position_types is None:
        position_types = ['C', 'P', 'Net']
    elif 'All' in position_types:
        position_types = ['C', 'P', 'Net']

    # Fetch data
    metrics, candlesticks = fetch_data(session_date, strike_range, expiration)

    as_of_time_stamp = str(metrics["effective_datetime"].max())
    # Process data and generate GIFs
    #gif_paths = process_data(metrics, candlesticks, session_date, participant, strike_range, expiration, position_types)

    videos_paths = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration, position_types)
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range, expiration, position_types, webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")

if __name__ == "__main__":
    gif_flow()