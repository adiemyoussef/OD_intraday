import json
import math
import uuid

import pandas as pd
import pytz
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash, Task
from prefect import task, flow, get_run_logger, get_client
from datetime import timedelta, datetime, date
from typing import List, Optional
# from charting.generate_gifs import generate_gif, send_to_discord
from charting.generate_gifs import generate_gif, generate_frame_new, send_to_discord, generate_video,send_to_discord_new

from utilities.misc_utils import *
from utilities.db_utils import *
from config.config import *
import os

from datetime import datetime, time
import numpy as np
import plotly.graph_objects as go

import concurrent.futures
import time as time_module
from functools import partial

import tempfile

import subprocess

default_date = date.today()
STRIKE_RANGE = [5500, 5900]
DEBUG_MODE = False  # Set to False for production

db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
db.connect()
print(f'MySQL status:{db.get_status()}')

prod_pg_data = PostGreData(
    host=POSGRE_PROD_DB_HOST,
    port=POSGRE_PROD_DB_PORT,
    user=POSGRE_PROD_DB_USER,
    password=POSGRE_PROD_DB_PASSWORD,
    database=POSGRE_PROD_DB_NAME
)
prod_pg_data.connect()
print(f'Prod PG status:{prod_pg_data.get_status()}')

do_space = DigitalOceanSpaces(
    region_name='nyc3',
    endpoint_url=INTRADAYBOT_ENDPOINT,
    access_key=INTRADAYBOT_ACCESSKEY,
    secret_key=INTRADAYBOT_SECRETKEY
)
do_space.connect()
print(f'Space status:{do_space.get_status()}')


def get_webhook_url(flow_name):
    if DEBUG_MODE:
        return WEBHOOK_URLS['dev']
    return WEBHOOK_URLS.get(flow_name, WEBHOOK_URLS['dev'])


@task
def parse_strike_range(strike_range: str) -> List[int]:
    values = list(map(int, strike_range.split(',')))
    return [min(values), max(values)]


@task(cache_key_fn=None, cache_expiration=timedelta(hours=0, minutes=1))
def fetch_data(session_date: str, effective_datetime: str, strike_range: List[int], expiration: str,
               start_time: str = '07:00:00') -> [pd.DataFrame]:
    prefect_logger = get_run_logger()
    start_of_video = f'{session_date} {start_time}'

    metrics_query = f"""
    SELECT * FROM intraday.intraday_books
    WHERE effective_date = '{session_date}'
    """

    if effective_datetime is not None:
        metrics_query += f" AND effective_datetime = '{effective_datetime}'"

    if start_time is not None:
        metrics_query += f" AND effective_datetime >= '{start_of_video}'"

    if strike_range is not None:
        metrics_query += f" AND strike_price BETWEEN {strike_range[0]} AND {strike_range[1]}"

    if expiration is not None:
        metrics_query += f" AND expiration_date_original = '{expiration}'"

    candlesticks_query = f"""
    SELECT * FROM public.charts_candlestick
    where effective_date = '{session_date}'
    and
    ticker = 'SPX'
    """

    last_price_query = f"""
    SELECT close FROM public.charts_candlestick 
    WHERE id = (SELECT MAX(id) FROM public.charts_candlestick WHERE ticker = 'SPX')
    """

    prefect_logger.info(f'{prod_pg_data.get_status()}')

    metrics = db.execute_query(metrics_query)
    candlesticks = prod_pg_data.execute_query(candlesticks_query)
    last_price = prod_pg_data.execute_query(last_price_query)

    unique_effectivedatetime = candlesticks['effective_datetime'].unique()
    prefect_logger.info(f'Candlesticks distinct effective_datetimes: {unique_effectivedatetime}')

    if candlesticks.empty:
        prefect_logger.info("No candlesticks Available")

    else:
        # candlesticks['effective_datetime'] = (
        #     pd.to_datetime(candlesticks['effective_datetime'], utc=False)  # Set timezone to UTC
        #     .dt.tz_convert('America/New_York')  # Convert to Eastern Time
        #     .dt.tz_localize(None)  # Remove timezone information (make naive)
        # )
        candlesticks.drop_duplicates(keep='first', inplace=False)

    unique_effectivedatetime = candlesticks['effective_datetime'].unique()
    prefect_logger.info(f'Candlesticks distinct effective_datetimes: {unique_effectivedatetime}')

    return metrics, candlesticks, last_price


@task(cache_key_fn=None, cache_expiration=timedelta(hours=0, minutes=1))
def fetch_data_depthview(session_date: str, strike_range: List[int], expiration: str = None) -> [pd.DataFrame]:
    metrics_query = f"""
    SELECT * FROM intraday.intraday_books
    WHERE effective_date = '{session_date}'
    AND effective_datetime >= (SELECT MAX(effective_datetime) FROM intraday.intraday_books where effective_date = '{session_date}')
    AND strike_price BETWEEN {strike_range[0]} AND {strike_range[1]}
    """

    if expiration is not None:
        metrics_query += f"AND expiration_date_original = '{expiration}'"

    metrics = db.execute_query(metrics_query)

    return metrics


@task
def process_data(metric: pd.DataFrame, candlesticks: pd.DataFrame, session_date: str, participant: str,
                 strike_range: List[int], expiration: str, position_types: List[str]) -> List[str]:
    gif_paths = []
    for position_type in position_types:
        gif_path = generate_gif(
            metric, candlesticks, session_date, participant, position_type,
            strike_range, expiration,
            output_gif=f'animated_chart_{position_type}.gif'
        )
        if gif_path:
            gif_paths.append(gif_path)
        else:
            print(f"Failed to generate GIF for {position_type}")
    return gif_paths


@task
def generate_video_task_(data: pd.DataFrame, candlesticks: pd.DataFrame, session_date: str, participant: str,
                         strike_range: List[int], expiration: str, position_type: list, last_price: float,
                         metric: str = 'positioning', img_path='config/images/logo_dark.png'):
    videos_paths = []
    last_frame_paths = []

    for pos_type in position_type:
        print(f'generate_video_task_ for {pos_type}')

        video_path, last_frame_path = generate_video(
            data, candlesticks, session_date, participant, pos_type,
            strike_range, expiration, metric, last_price, img_path=img_path,
            output_video=f'{pos_type}_{expiration}_animated_chart.mp4'
        )
        videos_paths.append(video_path)
        last_frame_paths.append(last_frame_path)

    # Combined paths with last_frame_paths first, followed by videos_paths
    paths = last_frame_paths + videos_paths

    return paths


def generate_video_wrapper(pos_type, data, candlesticks, session_date, participant,
                           strike_range, expiration, metric, last_price, img_path):
    start_time = time_module.time()

    # def generate_video(data, candlesticks, session_date, participant_input, position_type_input, strike_input,
    #                    expiration_input,
    #                    metric, last_price,
    #                    img_path='config/images/logo_dark.png',
    #                    output_video='None.mp4')

    video_path, last_frame_path = generate_video(
        data, candlesticks, session_date, participant, pos_type,
        strike_range, expiration, metric, last_price, img_path=img_path,
        output_video=f'{pos_type}_{expiration}_animated_chart.mp4'
    )
    end_time = time_module.time()
    duration = end_time - start_time
    print(f"Video generation for {pos_type} completed in {duration:.2f} seconds")
    return video_path, last_frame_path


@task
def generate_video_task(data: pd.DataFrame, candlesticks: pd.DataFrame, session_date: str, participant: str,
                        strike_range: List[int], expiration: str, position_type: list, last_price: float,
                        metric: str = 'positioning', img_path='config/images/logo_dark.png'):
    videos_paths = []
    last_frame_paths = []

    # Create a partial function with all arguments except pos_type
    generate_video_partial = partial(
        generate_video_wrapper,
        data=data,
        candlesticks=candlesticks,
        session_date=session_date,
        participant=participant,
        strike_range=strike_range,
        expiration=expiration,
        metric=metric,
        last_price=last_price,
        img_path=img_path
    )

    # Use ThreadPoolExecutor to parallelize the video generation
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each position type
        future_to_pos_type = {executor.submit(generate_video_partial, pos_type): pos_type for pos_type in position_type}

        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_pos_type):
            pos_type = future_to_pos_type[future]
            try:
                video_path, last_frame_path = future.result()
                videos_paths.append(video_path)
                last_frame_paths.append(last_frame_path)
            except Exception as exc:
                print(f"Video generation for {pos_type} generated an exception: {exc}")
                breakpoint()

    # Combined paths with last_frame_paths first, followed by videos_paths
    paths = last_frame_paths + videos_paths

    return paths


# def generate_frame(data, candlesticks, timestamp, participant, strike_range, expiration, position_type, metric,
#                    last_price, img_path):
#     # Your existing frame generation code here
#     # ...
#
#     # Instead of saving to a local directory, save to a temporary file
#     with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmpfile:
#         fig.write_image(tmpfile.name, scale=3)
#
#     return tmpfile.name


@task
def test_generate_video_task_old(data, candlesticks, session_date, participant, strike_range, expiration,
                                 position_types,
                                 last_price, metric='positioning', img_path='config/images/logo_dark.png',
                                 space_name=INTRADAYBOT_SPACENAME, webhook_url=None):
    """
    Generate and process video frames for multiple position types, create videos, and optionally send to Discord.

    This function processes options data for specified position types (e.g., 'Net', 'C', 'P'),
    generates video frames, creates videos, and stores them in DigitalOcean Spaces. It also
    keeps track of the last processed timestamp to avoid redundant processing in subsequent runs.

    Args:
        data (pd.DataFrame): The options data to process.
        candlesticks (pd.DataFrame): Candlestick data for the underlying asset.
        session_date (str): The date of the trading session.
        participant (str): The market participant type (e.g., 'mm', 'total_customers').
        strike_range (list): The range of strike prices to consider.
        expiration (str): The expiration date for the options.
        position_types (list): List of position types to process (e.g., ['Net', 'C', 'P']).
        last_price (float): The last price of the underlying asset.
        metric (str): The metric to compute (default: 'positioning').
        img_path (str): Path to the background image for frames.
        space_name (str): Name of the DigitalOcean Space to use.
        webhook_url (str): Discord webhook URL for sending results (optional).

    Returns:
        dict: A dictionary containing video and latest frame URLs for each position type.
    """

    # Set default position types if not provided
    if not position_types:
        position_types = ['Net', 'C', 'P']

    results = {}

    for pos in position_types:
        print(f"Processing: {session_date}_{participant}_{strike_range}_{expiration}_{pos}_{metric}")

        # Create a unique hash for this combination of parameters
        param_hash = hash(f"{session_date}_{participant}_{strike_range}_{expiration}_{pos}_{metric}")
        last_timestamp_key = f"last_timestamp_{param_hash}"
        frames_prefix = f"frames_{param_hash}/"

        # Retrieve the last processed timestamp from DigitalOcean Spaces
        last_timestamp = None
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_name = temp_file.name
            if do_space.download_from_spaces(space_name, last_timestamp_key, temp_file_name):
                with open(temp_file_name, 'r') as f:
                    last_timestamp = pd.to_datetime(f.read().strip())
        os.remove(temp_file_name)

        # Filter data to process only new timestamps
        if last_timestamp:
            new_data = data[data['effective_datetime'] > last_timestamp]
        else:
            new_data = data

        # Generate new frames for the filtered data
        new_frames = []
        for timestamp in new_data['effective_datetime'].unique():
            fig, frame_path = generate_frame_new(new_data, candlesticks, timestamp, participant, strike_range,
                                                 expiration,
                                                 pos, metric, last_price, img_path)
            spaces_key = f"{frames_prefix}{timestamp.strftime('%Y%m%d%H%M%S')}.png"
            do_space.upload_to_spaces(frame_path, space_name, spaces_key)
            new_frames.append(spaces_key)
            os.remove(frame_path)  # Remove the temporary file

        # Retrieve all frames (existing + new) from DigitalOcean Spaces
        all_frames = sorted(do_space.list_objects(space_name, prefix=frames_prefix))

        # Generate video from all frames
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download all frames to a temporary directory
            for spaces_key in all_frames:
                local_file = os.path.join(tmpdir, os.path.basename(spaces_key))
                do_space.download_from_spaces(space_name, spaces_key, local_file)

            # Use ffmpeg to create a video from the frames
            output_video = f'video_{param_hash}.mp4'
            ffmpeg_command = f'ffmpeg -framerate 3 -pattern_type glob -i "{tmpdir}/*.png" -c:v libx264 -pix_fmt yuv420p {output_video}'
            subprocess.run(ffmpeg_command, shell=True, check=True)

            # Upload the generated video to DigitalOcean Spaces
            video_key = f"videos/{output_video}"
            do_space.upload_to_spaces(output_video, space_name, video_key)
            os.remove(output_video)

        # Create URLs for the video and latest frame
        video_url = f"https://{space_name}.nyc3.digitaloceanspaces.com/{video_key}"
        latest_frame_key = all_frames[-1] if all_frames else None
        latest_frame_url = f"https://nyc3.digitaloceanspaces.com/{latest_frame_key}" if latest_frame_key else None

        # Store results for this position type
        results[pos] = {
            'video_url': video_url,
            'latest_frame_url': latest_frame_url
        }

        # Send results to Discord if a webhook URL is provided
        if webhook_url:
            as_of_time_stamp = str(data["effective_datetime"].max())
            success = send_discord_message([video_url, latest_frame_url], as_of_time_stamp, session_date, participant,
                                           strike_range, expiration, [pos], metric, webhook_url)

            # Update the last processed timestamp if Discord message was sent successfully
            if success:
                latest_timestamp = data['effective_datetime'].max()
                with open('last_timestamp.txt', 'w') as f:
                    f.write(str(latest_timestamp))
                do_space.upload_to_spaces('last_timestamp.txt', space_name, last_timestamp_key)
                print(f"Updated last processed timestamp to {latest_timestamp} for {pos}")
            else:
                print(f"Failed to send to Discord. Last timestamp not updated for {pos}")
        else:
            print("No webhook URL provided. Video not sent to Discord.")

    return results


@task
def test_generate_video_task(data, candlesticks, session_date, participant, strike_range, expiration, position_types,
                             last_price, metric='positioning', img_path='config/images/logo_dark.png',
                             space_name=INTRADAYBOT_SPACENAME):
    """
    Generate and process video frames for multiple position types, create videos, and optionally send to Discord.
    This version uses DigitalOcean Spaces for persistent tracking of generated frames.

    Args:
        [All arguments remain the same as in the previous version]

    Returns:
        dict: A dictionary containing video and latest frame URLs for each position type.
    """

    if not position_types:
        position_types = ['Net', 'C', 'P']

    results = {}

    for pos in position_types:
        print(f"Processing: {session_date}_{participant}_{strike_range}_{expiration}_{pos}_{metric}")

        # Create a unique identifier for this combination of parameters
        combo_id = f"{session_date}_{participant}_{'-'.join(map(str, strike_range))}_{expiration}_{pos}_{metric}"
        print(f"Generated {combo_id} for {session_date}_{participant}_{strike_range}_{expiration}_{pos}_{metric}")
        # Keys for storing metadata and frames in DigitalOcean Spaces
        metadata_key = f"metadata/{combo_id}.json"
        frames_prefix = f"frames/{combo_id}/"

        # Retrieve or initialize metadata
        metadata = do_space.get_or_initialize_metadata(space_name, metadata_key)

        # # Filter data to process only new timestamps
        # last_timestamp = pd.to_datetime(metadata['last_timestamp']) if metadata['last_timestamp'] else None
        # if last_timestamp:
        #     new_data = data[data['effective_datetime'] > last_timestamp]
        # else:
        #     new_data = data
        #
        # unprocessed_effectivedatetimes = new_data['effective_datetime'].unique()
        # print(f'Unique time_stamps to process: {unprocessed_effectivedatetimes}')

        # Filter data to process only new timestamps, but include necessary historical data
        last_timestamp = pd.to_datetime(metadata['last_timestamp']) if metadata['last_timestamp'] else None
        current_day_start = pd.Timestamp(session_date).floor('D')
        day_data = data[data['effective_datetime'] >= current_day_start]

        print(f"Last time_stamp: {last_timestamp}")
        if not day_data.empty:
            first_datetime = day_data['effective_datetime'].min()
            start_of_day_data = data[data['effective_datetime'] == first_datetime]
        else:
            start_of_day_data = pd.DataFrame()  # Empty DataFrame if no data for the day

        if last_timestamp:
            # Include the last processed data point
            last_processed_data = data[data['effective_datetime'] == last_timestamp]

            # Include all new data points
            new_data = data[data['effective_datetime'] > last_timestamp]

            # Combine all necessary data
            data_to_process = pd.concat([last_processed_data, start_of_day_data, new_data]).drop_duplicates()
        else:
            new_data = data
            data_to_process = pd.concat([start_of_day_data, new_data]).drop_duplicates()

        unprocessed_effectivedatetimes = new_data['effective_datetime'].unique()
        effectivedatetimes_datasent = data_to_process['effective_datetime'].unique()
        print(f'Unique time_stamps to process: {unprocessed_effectivedatetimes}')
        print(f'Unique time_stamps to process: {effectivedatetimes_datasent}')
        # Generate new frames for the filtered data
        new_frames = []
        for timestamp in new_data['effective_datetime'].unique():
            print(f'Processing: {timestamp}')
            fig, frame_path = generate_frame_new(data_to_process, candlesticks, timestamp, participant, strike_range,
                                                 expiration,
                                                 pos, metric, last_price, img_path)
            frame_key = f"{frames_prefix}{timestamp.strftime('%Y%m%d%H%M%S')}.png"
            do_space.upload_to_spaces(frame_path, space_name, frame_key)
            new_frames.append(frame_key)
            metadata['frames'].append(frame_key)
            os.remove(frame_path)  # Remove the temporary file

        # Update metadata with new frames and last timestamp
        if new_frames:
            metadata['last_timestamp'] = str(new_data['effective_datetime'].max())
            do_space.update_metadata(space_name, metadata_key, metadata)

        # Generate video from all frames
        video_url, latest_frame_url = generate_video_from_frames(do_space, space_name, metadata['frames'], combo_id)

        # Store results for this position type
        results[pos] = {
            'video_url': video_url,
            'latest_frame_url': latest_frame_url
        }

    return results


def generate_video_from_frames(do_space, space_name, frame_keys, combo_id):
    """Generate video from frames and return video and latest frame URLs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download all frames to a temporary directory
        for frame_key in frame_keys:
            local_file = os.path.join(tmpdir, os.path.basename(frame_key))
            do_space.download_from_spaces(space_name, frame_key, local_file)

        # Use ffmpeg to create a video from the frames
        output_video = f'video_{combo_id}.mp4'
        ffmpeg_command = f'ffmpeg -framerate 3 -pattern_type glob -i "{tmpdir}/*.png" -c:v libx264 -pix_fmt yuv420p {output_video}'
        #subprocess.run(ffmpeg_command, shell=True, check=True)

        result = subprocess.run(ffmpeg_command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"FFmpeg command failed. Output:\n{result.stdout}\n{result.stderr}")
            breakpoint()

        # Upload the generated video to DigitalOcean Spaces
        video_key = f"videos/{output_video}"
        do_space.upload_to_spaces(output_video, space_name, video_key)
        os.remove(output_video)

    video_url = f"https://nyc3.digitaloceanspaces.com/{video_key}"
    latest_frame_url = f"https://nyc3.digitaloceanspaces.com/{frame_keys[-1]}" if frame_keys else None

    return video_url, latest_frame_url


@task
def send_discord_message(file_paths: List[str], as_of_time_stamp: str, session_date: str, participant: str,
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
        {"name": "", "value": "", "inline": True},
        {"name": "👥 Participant(s)", "value": participant_text, "inline": True},
        {"name": "🎯 Strike Range", "value": f"{strike_range[0]} - {strike_range[1]}", "inline": True},
        {"name": "📅 Expiration(s)", "value": expiration, "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
    ]
    footer_text = f"Generated on {current_time} | By OptionsDepth.com"

    success = send_to_discord_new(
        webhook_url,
        file_paths,
        content="",  # "🚀 New options chart analysis is ready! Check out the latest market insights below.",
        title=title,
        description=description,
        fields=fields,
        footer_text=footer_text,
        do_spaces=do_space
    )

    # # Clean up the gif files after sending
    # for path in file_paths:
    #     try:
    #         print(f'Suppose to remove: {path}')
    #         #os.remove(path)
    #     except FileNotFoundError:
    #         print(f"Warning: Could not delete file {path}. It may have already been deleted.")

    return success


@flow(name="0DTE gifs")
def zero_dte_flow(
        session_date: Optional[date] = default_date,
        strike_range: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'total_customers',
        position_types: Optional[List[str]] = DEFAULT_POS_TYPES,
        webhook_url: str = None
):
    webhook_url = webhook_url or get_webhook_url('zero_dte')

    expiration = str(session_date)

    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        strike_range = get_strike_range(prod_pg_data,session_date)
        #strike_range = [5580, 5900]
    if expiration is None:
        expiration = session_date
    if position_types is None:
        position_types = DEFAULT_POS_TYPES
    elif 'All' in position_types:
        position_types = DEFAULT_POS_TYPES

    current_time = datetime.now().time()

    if current_time < time(12, 0):
        start_time = START_TIME_PRE_MARKET
    elif time(12, 0) <= current_time < time(23, 0):
        start_time = START_TIME_MARKET
    else:  # 7:00 PM or later
        start_time = START_TIME_PRE_MARKET

    print(f"Start time set to: {start_time}")

    # Fetch data
    metrics, candlesticks, last_price = fetch_data(session_date, None, strike_range, expiration, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())
    last_price = last_price.values[0][0]
    # Process data and generate GIFs
    paths_to_send = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                        position_types, last_price, 'positioning', POS_0DTE)
    print(f"Video and frames generated at: {paths_to_send}")

    # Send Discord message with Videos
    video_success = send_discord_message(paths_to_send, as_of_time_stamp, session_date, participant, strike_range,
                                         expiration, position_types, 'positioning', webhook_url)

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
        position_types: Optional[List[str]] = DEFAULT_POS_TYPES,
        webhook_url: str = None
):
    # Set default values if not provided
    webhook_url = webhook_url or get_webhook_url('one_dte')
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        strike_range = get_strike_range(prod_pg_data,session_date, range_value = 0.025, range_type = 'percent')
        #strike_range = [5580, 5900]
    if position_types is None:
        position_types = DEFAULT_POS_TYPES
    elif 'All' in position_types:
        position_types = DEFAULT_POS_TYPES
    if expiration is None:
        expiration = get_trading_day(session_date, n=1, direction='next', return_format='str')

    current_time = datetime.now().time()
    if current_time < time(12, 0):
        start_time = START_TIME_PRE_MARKET
    elif time(12, 0) <= current_time < time(23, 0):
        start_time = START_TIME_MARKET
    else:  # 7:00 PM or later
        start_time = START_TIME_PRE_MARKET

    print(f"Start time set to: {start_time}")

    # Fetch data
    data, candlesticks, last_price = fetch_data(session_date, None, strike_range, expiration, start_time)

    as_of_time_stamp = str(data["effective_datetime"].max())
    last_price = last_price.values[0][0]
    videos_paths = generate_video_task(data, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types, last_price, 'positioning', POS_UPCOMING_EXP)
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range,
                                         expiration, position_types, 'positioning', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")


@flow(name="GEX gifs")
def GEX_flow(
        session_date: Optional[date] = None,
        strike_range: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'mm',
        position_types: Optional[List[str]] = DEFAULT_POS_TYPES,
        webhook_url: str = None
):
    # Default values
    webhook_url = webhook_url or get_webhook_url('gex')
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        strike_range = get_strike_range(prod_pg_data,session_date, range_value = 0.025, range_type = 'percent')
        #strike_range = [5580, 5900]
    if position_types is None:
        position_types = DEFAULT_POS_TYPES
    if expiration is None:
        expiration = session_date

    current_time = datetime.now().time()
    if current_time < time(12, 0):
        start_time = START_TIME_PRE_MARKET
    elif time(12, 0) <= current_time < time(23, 0):
        start_time = START_TIME_MARKET
    else:  # 7:00 PM or later
        start_time = START_TIME_PRE_MARKET

    print(f"Start time set to: {start_time}")

    # Fetch data
    metrics, candlesticks, last_price = fetch_data(session_date, None, strike_range, expiration, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())
    last_price = last_price.values[0][0]
    videos_paths = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types, last_price, "GEX", GEX_0DTE)
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range,
                                         expiration, position_types, 'GEX', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")
    pass


# -------------------DEV TEST---------------------#
@flow(name="TEST - 0DTE gifs")
def test_zero_dte_flow(
        session_date: Optional[date] = default_date,
        strike_range: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'total_customers',
        position_types: Optional[List[str]] = DEFAULT_POS_TYPES,
        webhook_url: str = None
):
    webhook_url = webhook_url or get_webhook_url('test-0dte')

    expiration = str(session_date)

    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        strike_range = get_strike_range(prod_pg_data,session_date)
        #strike_range = [5580, 5900]
    if expiration is None:
        expiration = session_date
    if position_types is None:
        position_types = DEFAULT_POS_TYPES
    elif 'All' in position_types:
        position_types = DEFAULT_POS_TYPES

    current_time = datetime.now().time()

    if current_time < time(12, 0):
        start_time = START_TIME_PRE_MARKET
    elif time(12, 0) <= current_time < time(23, 0):
        start_time = START_TIME_MARKET
    else:  # 7:00 PM or later
        start_time = START_TIME_PRE_MARKET

    print(f"Start time set to: {start_time}")

    # Fetch data
    metrics, candlesticks, last_price = fetch_data(session_date, None, strike_range, expiration, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())
    last_price = last_price.values[0][0]
    # Process data and generate GIFs
    # paths_to_send = test_generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
    #                                    position_types, last_price ,'positioning', POS_0DTE)
    # print(f"Video and frames generated at: {paths_to_send}")

    # Generate video with new frames
    video_url = test_generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                         position_types, last_price, metric='positioning', img_path=POS_0DTE,
                                         space_name=INTRADAYBOT_SPACENAME)

    # Send Discord message with Video URL
    video_success = send_discord_message(video_url, as_of_time_stamp, session_date, participant, strike_range,
                                         expiration, position_types, 'positioning', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")

@flow(name="TEST - 1DTE gifs")
def test_one_dte_flow(
        session_date: Optional[date] = default_date,
        strike_range: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'total_customers',
        position_types: Optional[List[str]] = DEFAULT_POS_TYPES,
        webhook_url: str = None
):
    webhook_url = webhook_url or get_webhook_url('test-1dte')

    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        strike_range = get_strike_range(prod_pg_data,session_date)
        #strike_range = [5580, 5900]
    if expiration is None:
        expiration = get_trading_day(session_date, n=1, direction='next', return_format='str')
    if position_types is None:
        position_types = DEFAULT_POS_TYPES
    elif 'All' in position_types:
        position_types = DEFAULT_POS_TYPES

    current_time = datetime.now().time()

    if current_time < time(12, 0):
        start_time = START_TIME_PRE_MARKET
    elif time(12, 0) <= current_time < time(23, 0):
        start_time = START_TIME_MARKET
    else:  # 7:00 PM or later
        start_time = START_TIME_PRE_MARKET

    print(f"Start time set to: {start_time}")

    # Fetch data
    metrics, candlesticks, last_price = fetch_data(session_date, None, strike_range, expiration, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())
    last_price = last_price.values[0][0]
    # Process data and generate GIFs
    # paths_to_send = test_generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
    #                                    position_types, last_price ,'positioning', POS_0DTE)
    # print(f"Video and frames generated at: {paths_to_send}")

    # Generate video with new frames
    video_url = test_generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                         position_types, last_price, metric='positioning', img_path=POS_UPCOMING_EXP,
                                         space_name=INTRADAYBOT_SPACENAME)

    # Send Discord message with Video URL
    video_success = send_discord_message(video_url, as_of_time_stamp, session_date, participant, strike_range,
                                         expiration, position_types, 'positioning', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")

@flow(name="TEST - GEX gifs")
def test_GEX_flow(
        session_date: Optional[date] = None,
        strike_range: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'mm',
        position_types: Optional[List[str]] = DEFAULT_POS_TYPES,
        webhook_url: str = None
):

    #Default values
    webhook_url = webhook_url or get_webhook_url('test-gex-0dte')
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        strike_range = get_strike_range(prod_pg_data,session_date, range_value = 0.025, range_type = 'percent')
        #strike_range = [5580, 5900]
    if position_types is None:
        position_types = DEFAULT_POS_TYPES
    if expiration is None:
        expiration = session_date

    current_time = datetime.now().time()
    if current_time < time(12, 0):
        start_time = START_TIME_PRE_MARKET
    elif time(12, 0) <= current_time < time(23, 0):
        start_time = START_TIME_MARKET
    else:  # 7:00 PM or later
        start_time = START_TIME_PRE_MARKET

    print(f"Start time set to: {start_time}")


    # Fetch data
    metrics, candlesticks,last_price = fetch_data(session_date,None, strike_range, expiration, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())
    last_price = last_price.values[0][0]
    # videos_paths = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
    #                                    position_types, last_price,"GEX", GEX_0DTE)
    # Generate video with new frames
    video_url = test_generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                         position_types, last_price, metric='GEX', img_path=GEX_0DTE,
                                          space_name=INTRADAYBOT_SPACENAME)

    # Send Discord message with Video URL
    video_success = send_discord_message(video_url, as_of_time_stamp, session_date, participant, strike_range,
                                         expiration, position_types, 'GEX', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")
# ------------------ DEPTHVIEW ------------------#


# ---------------- HEATMAP GIF -------------------- #

def generate_heatmap_gif(
        session_date: Optional[date] = default_date,
        webhook_url: str = 'https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
):
    pass


# ------------ STATIC CHARTS ----------------------#

def plot_options_data(df: pd.DataFrame,
                      aggregation: str,
                      strike_input: list,
                      expiration_input: str,
                      participant: str = "total_customers",
                      view: str = 'both'):
    """
    Plots the options data based on specified aggregation and view.

    Parameters:
        df (pd.DataFrame): DataFrame containing options data with columns for 'expiration', 'type', 'strike', and 'net_position'.
        aggregation (str): 'strike' or 'expiration' to set the aggregation level.
        view (str): 'net', 'call', 'put', 'all', or 'both' to determine which data to show.
        strike_input (tuple): (min_strike, max_strike) to filter options by strike range.
        expiration_input (list): List of specific expirations to include.
    """
    x_axis_title = 'position'
    img = load_logo()

    print('-------------------------')
    print('----Ploting Function-----')
    print(f'strikes received by function: {strike_input} {type(strike_input)}')
    print(f'expirations received by function: {expiration_input} {type(expiration_input)}')

    print('-------------------------')

    # --------------------------- Strikes ---------------------------
    if strike_input == "all":
        subtitle_strike = "All Strikes"

    # - list --> range
    if isinstance(strike_input, list):
        print("------------- range strikes -------------")

        df = df[(df['strike_price'] >= min(strike_input)) & (df['strike_price'] <= max(strike_input))]
        subtitle_strike = f"For range: {min(strike_input)} to {max(strike_input)} strikes"

    # - tuple --> list
    elif isinstance(strike_input, tuple):
        print("------------- list strikes -------------")
        df = df[df['strike_price'].isin(strike_input)]
        subtitle_strike = f"For the following strikes: {', '.join([str(item) for item in strike_input])}"

    # - string --> single
    if isinstance(strike_input, int):
        print("------------- single strike -------------")
        df = df[df['strike_price'] == strike_input]
        subtitle_strike = f"Strike: {strike_input} uniquely"

    # - string "all" --> everything # keep dataframe the same. define title string
    if expiration_input == "all":
        subtitle_expiration = "All Expirations"

    # ---------------------------------- Expirations------------------------
    # - list --> range
    if isinstance(expiration_input, list):
        print("------------- range expirations -------------")

        print(type(expiration_input[0]))  # string TODO fix to datetime
        df = df[df['expiration_date_original'].between(min(expiration_input), max(expiration_input))]
        subtitle_expiration = f"For {min(expiration_input).strftime('%Y-%m-%d')} to {max(expiration_input).strftime('%Y-%m-%d')} expirations range"

    # - tuple --> list
    elif isinstance(expiration_input, tuple):
        print("------------- list expirations -------------")
        df = df[df['expiration_date_original'].isin(expiration_input)]
        subtitle_expiration = f"For {', '.join([date.strftime('%Y-%m-%d') for date in expiration_input])} specific expirations"

    # - string --> single
    if isinstance(expiration_input, str):
        print("------------- single expiration -------------")
        date_format = '%Y-%m-%d'
        expiration_input_date_obj = datetime.strptime(expiration_input, date_format).date()
        df = df[df['expiration_date_original'] == expiration_input_date_obj]
        subtitle_expiration = f"For the {expiration_input} expiration"

    # -------------------------------------- plotting --------------------------------------
    # Aggregate data
    if aggregation == 'strike':
        grouped = df.groupby(['strike_price', 'call_put_flag']).agg({f'{participant}_posn': 'sum'}).unstack(
            fill_value=0)
        grouped.columns = grouped.columns.droplevel()
        orientation = 'h'
        yaxis_title = "Strike Value"
        xaxis_title = "Net Position"
        y_parameters = dict(
            showgrid=True,  # Show major gridlines
            gridcolor='#95a0ab',  # Color of major gridlines
            gridwidth=1,  # Width of major gridlines
            dtick=5,  # Major tick every 500
            tick0=0,  # Start ticks from 0
            showticklabels=True
        )
        x_parameters = None

    elif aggregation == "expiration":
        breakpoint()

    else:
        breakpoint()
    # Create figure
    fig = go.Figure()

    if view in ['call', 'put', 'all', 'both']:
        for col in ['C', 'P']:
            if view == 'all' or view == 'both' or view.lower() == col.lower():
                fig.add_trace(go.Bar(
                    name='Calls' if col == 'C' else 'Puts',
                    x=grouped.index if orientation == 'v' else grouped[col],
                    y=grouped[col] if orientation == 'v' else grouped.index,
                    orientation=orientation,
                    marker_color='SeaGreen' if col == 'C' else 'Red',
                    text=grouped[col],  # Text to display (values on the x-axis)
                    textposition='inside',
                    textfont=dict(  # Customize the font of the text
                        size=20,  # Font size
                    )
                    # Position the text outside the bar
                ))

    if view in ['net']:
        grouped['Net'] = grouped['C'] + grouped['P']
        fig.add_trace(go.Bar(
            name='Net',
            x=grouped.index if orientation == 'v' else grouped['Net'],
            y=grouped['Net'] if orientation == 'v' else grouped.index,
            orientation=orientation,
            marker_color='rgb(17,73,124)'
        ))

    fig.update_layout(
        title=f"SPX: Net Customer Options Positioning <br><sup>{subtitle_strike}<br>{subtitle_expiration}</sup>",
    )
    # ------------------

    if y_parameters:
        fig.update_layout(yaxis=y_parameters)

    if x_parameters:
        fig.update_layout(xaxis=x_parameters)

    # Update layout
    fig.update_layout(
        title=dict(
            # text=(f"<b>Breakdown By Strike</b><br>"
            #       f"<sup>SPX: {participant_mapping.get(participant, 'Unknown Participant')} {position_type} {metric_to_compute} as of {timestamp}</sup><br>"
            #       f"<sup>{subtitle_strike} | {subtitle_expiration}</sup>"),
            text=(f"<b>Breakdown By Strike</b><br>"
                  f"<sup>SPX:  TOTO </sup><br>"
                  f"<sup>{subtitle_strike} | {subtitle_expiration}</sup>"),
            font=dict(family="Arial", size=24, color="black"),
            x=0.0,
            y=0.98,
            xanchor='left',
            yanchor='top'
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            groupclick="toggleitem"
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(family="Arial", color='black', size=12),
        xaxis=dict(
            title=f"{x_axis_title}",
            showgrid=True,
            gridcolor='lightgrey',
            zeroline=True,
            zerolinecolor='black',
            zerolinewidth=2
        ),
        yaxis=dict(
            title="Strike Price",
            showgrid=True,
            gridcolor='lightgrey',
            dtick=10
        ),
        width=1000,
        height=1200,
        margin=dict(l=50, r=50, t=100, b=50),
        images=[dict(
            source=img,
            xref="paper", yref="paper",
            x=0.15, y=0.6,
            sizex=0.75, sizey=0.75,
            sizing="contain",
            opacity=0.3,  # Increased opacity for better visibility
            layer="below"
        )] if img else []
    )

    # Add copyright sign at bottom left
    fig.add_annotation(
        text="© OptionsDepth",
        xref="paper", yref="paper",
        x=-0.05, y=-0.05,
        showarrow=False,
        font=dict(size=10, color="gray")
    )

    # Add "Powered by OptionsDepth.com" at bottom right
    fig.add_annotation(
        text="Powered by OptionsDepth.com",
        xref="paper", yref="paper",
        x=0.99, y=-0.05,
        showarrow=False,
        font=dict(size=10, color="gray"),
        xanchor="right"
    )

    fig.update_layout(
        width=1920,  # Full HD width
        height=1080,  # Full HD height
        font=dict(size=16)  # Increase font size for better readability

    )

    return fig


def generate_and_send_options_charts(df_metrics: pd.DataFrame = None,
                                     strike_range: list = None,
                                     expiration: str = None,
                                     session_date: str = '2024-09-13',
                                     participant: str = "total_customers",
                                     output_dir: str = 'temp_charts',
                                     webhook_url: str = DEV_CHANNEL, ):
    expiration = '2024-09-16'

    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        # TODO: +/- 200 pts from SPOT Open
        strike_range = STRIKE_RANGE
    if expiration is None:
        # TODO: Next business day not day
        expiration = '2024-09-24'  # str(get_next_expiration_date(session_date))

    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")

    # Fetch data
    data, candlesticks, last_price = fetch_data(session_date, '2024-09-13 12:00:00', strike_range, expiration,
                                                start_time)

    as_of_time_stamp = str(data["effective_datetime"].max())
    last_price = last_price.values[0][0]

    # Create a unique temporary directory to store charts
    temp_dir = os.path.join(output_dir, f'charts_{uuid.uuid4().hex}')
    os.makedirs(temp_dir, exist_ok=True)

    # Generate Calls and Puts chart
    fig_calls_puts = plot_options_data(data, 'strike', strike_range, expiration, participant, view='both')
    # fig_calls_puts.show()

    # Generate Net chart
    fig_net = plot_options_data(data, 'strike', strike_range, expiration, participant, view='net')
    # fig_net.show()

    # Save the charts as image files
    calls_puts_path = os.path.join(temp_dir, f'calls_puts_{session_date}.png')
    net_path = os.path.join(temp_dir, f'net_{session_date}.png')

    fig_calls_puts.write_image(calls_puts_path, scale=2)
    fig_net.write_image(net_path, scale=2)

    print(f"Charts for {session_date} saved successfully!")
    print(f"Calls and Puts chart: {calls_puts_path}")
    print(f"Net chart: {net_path}")

    # Prepare the embed
    title = f"📊 {session_date} Options Positioning"
    fields = [
        {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
        {"name": "👥 Participant:", "value": participant, "inline": True}
    ]
    footer_text = f"Generated on {as_of_time_stamp} | By OptionsDepth.com"

    file_paths = [calls_puts_path, net_path]

    if not isinstance(file_paths, list):
        file_paths = [file_paths]

    # First Request: Send the embedded message
    embed = {
        "title": title or "Options Chart Analysis",
        # "description": "Here's the latest options chart analysis.",
        "color": 3447003,  # A nice blue color
        "fields": fields or [],
        "footer": {"text": footer_text or "Generated by Options Analysis Bot"},
        "timestamp": datetime.utcnow().isoformat()
    }

    # Prepare the payload with the embed
    payload = {
        # "embeds": [embed],
        # "content": content or ""  # Ensure content is initialized
    }

    # Second Request: Send the GIFs separately
    files = {}
    for i, file_path in enumerate(file_paths):
        with open(file_path, 'rb') as f:
            file_content = f.read()
        files[f"file{i}"] = (os.path.basename(file_path), file_content, "image/gif")

    # Send the second request (GIFs only)
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200 or response.status_code == 204:
        print("GIFs sent successfully to Discord!")
    else:
        print(f"Failed to send GIFs. Status code: {response.status_code}")
        print(f"Response content: {response.content}")

    return response.status_code == 200


if __name__ == "__main__":
    test_zero_dte_flow()
    test_one_dte_flow()
    test_GEX_flow()
    # zero_dte_flow()
    # one_dte_flow()
    # GEX_flow()
    # plot_depthview(webhook_url=WebhookUrl.DEFAULT)
    # generate_heatmap_gif()
    # all_GEX_flow()
    # i=0
    # while i < 20:
    #     i+=1
    #     generate_and_send_options_charts()