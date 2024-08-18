import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash, Task
from datetime import timedelta, datetime
from typing import List, Optional
from charting.generate_gifs import generate_gif, send_to_discord
from utilities.db_utils import DatabaseUtilities
from config.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
import os

db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
db.connect()
print(f'{db.get_status()}')

@task
def parse_strike_range(strike_range: str) -> List[int]:
    values = list(map(int, strike_range.split(',')))
    return [min(values), max(values)]


@task(cache_key_fn=None, cache_expiration=timedelta(hours=0, minutes=1))
def fetch_data(session_date: str, strike_range: List[int], expiration: str) -> [pd.DataFrame]:

    session_date = '2024-08-16'
    expiration = '2024-08-16 16:00:00'

    metrics_query =f"""
    SELECT * FROM intraday.intraday_books -- _test_posn
    WHERE effective_date = '{session_date}'
    and effective_datetime > '2024-08-16 09:30:00'
    -- and effective_datetime < '2024-08-16 12:00:00'
    and strike_price between {strike_range[0]} and {strike_range[1]}
    and expiration_date = '{expiration}'
    """

    candlesticks_query = f"""
    SELECT * FROM optionsdepth_stage.charts_candlestick
    where effective_date = '{session_date}'
    and
    ticker = 'SPX'
    """


    metrics = db.execute_query(metrics_query)
    candlesticks = db.execute_query(candlesticks_query)


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
def send_discord_message(gif_paths: List[str], session_date: str, participant: str,
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
        {"name": "⏰ As of:", "value": current_time, "inline": True},
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
        gif_paths,
        content="🚀 New options chart analysis is ready! Check out the latest market insights below.",
        title=title,
        description=description,
        fields=fields,
        footer_text=footer_text
    )

    # Clean up the gif files after sending
    for path in gif_paths:
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
    session_date: Optional[str] = None,
    strike_range: Optional[str] = None,
    expiration: Optional[str] = None,
    participant: str = 'total_customers',
    position_types: Optional[List[str]] = None,
    webhook_url: str = 'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'
):
    if strike_range:
        strike_range = parse_strike_range(strike_range)


    # position_types = ['Net', 'C', 'P']
    position_types = ['C'] #,'P']
    expiration = '2024-08-16'

    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT
        strike_range = [5300, 5700]
    if expiration is None:
        expiration = session_date
    if position_types is None:
        position_types = ['C', 'P', 'Net']
    elif 'All' in position_types:
        position_types = ['C', 'P', 'Net']

    # Fetch data
    metrics, candlesticks = fetch_data(session_date, strike_range, expiration)

    # Process data and generate GIFs
    gif_paths = process_data(metrics, candlesticks, session_date, participant, strike_range, expiration, position_types)

    # Send Discord message
    success = send_discord_message(gif_paths, session_date, participant, strike_range, expiration, position_types, webhook_url)

    if success:
        print(f"Successfully processed and sent intraday data for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")

if __name__ == "__main__":
    gif_flow()