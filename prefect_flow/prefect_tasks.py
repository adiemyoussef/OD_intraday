import asyncio
import os
from concurrent.futures import ThreadPoolExecutor,as_completed

import anyio
import pandas as pd
from aio_pika import Message, exceptions as aio_pika_exceptions
import zipfile
import time as time_module
from prefect import task, flow, get_run_logger,get_client
from prefect.tasks import task_input_hash
from prefect.deployments import run_deployment
from prefect_dask import DaskTaskRunner

from datetime import datetime, timedelta, time  # This imports the time class from datetime
from datetime import time as datetime_time
from zoneinfo import ZoneInfo
from config.config import *
import dill
import multiprocessing as mp
from dask import delayed, compute
import dask
import requests
from polygon import RESTClient
import io
from typing import List
from datetime import datetime
import plotly.graph_objects as go
import json
import boto3
from botocore.client import Config
from io import BytesIO

# Import your utility classes
from utilities.sftp_utils import *
from utilities.db_utils import *
from utilities.rabbitmq_utils import *
from utilities.misc_utils import *
from utilities.customized_logger import DailyRotatingFileHandler
from utilities.logging_config import *
from charting.intraday_beta_v2 import *
from heatmaps_simulation.heatmap_task import *

# Setup
mp.set_start_method("fork", force=True)
dill.settings['recurse'] = True
client = RESTClient("sOqWsfC0sRZpjEpi7ppjWsCamGkvjpHw")

DEBUG_MODE = False
LOG_LEVEL = logging.DEBUG

# Get the appropriate logger
logger = get_logger(debug_mode=False)

# -------- Initializing the Classes -------#
db_utils = DatabaseUtilities(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, logger=logger)
pg_data = PostGreData(
    host=POSGRE_DB_HOST,
    port=POSGRE_DB_PORT,
    user=POSGRE_DB_USER,
    password=POSGRE_DB_PASSWORD,
    database=POSGRE_DB_NAME
)
rabbitmq_utils = RabbitMQUtilities(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, logger=logger)
sftp_utils = SFTPUtility(SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD, logger=logger)

logger.info(f"Initializing db status: {db_utils.get_status()}")
logger.info(f'Postgre Status -- > {pg_data.get_status()}')
logger.info(f"Initializing RabbitMQ status: {rabbitmq_utils.get_status()}")


# -------------------------------------------#

async def get_deployment_id(deployment_name):
    async with get_client() as client:
        deployments = await client.read_deployments()
        for deployment in deployments:
            if deployment.name == deployment_name:
                return deployment.id
    return None

async def trigger_deployments(deployment_names):
    prefect_logger = get_run_logger()
    async with get_client() as client:
        for deployment_name in deployment_names:
            try:
                deployment_id = await get_deployment_id(deployment_name)
                if deployment_id:
                    flow_run = await client.create_flow_run_from_deployment(deployment_id=deployment_id)
                    prefect_logger.info(f"Deployment '{deployment_name}' triggered successfully with flow run ID {flow_run.id}")
                else:
                    prefect_logger.error(f"Deployment '{deployment_name}' not found")
            except Exception as e:
                prefect_logger.error(f"Failed to trigger deployment '{deployment_name}': {str(e)}")



def process_greek(greek_name, poly_data, book):
    latest_greek = poly_data.sort_values('time_stamp', ascending=False).groupby('contract_id').first().reset_index()
    latest_greek = latest_greek[['contract_id', greek_name, 'time_stamp']]
    book = pd.merge(book, latest_greek, on='contract_id', how='left', suffixes=('', '_update'))

    update_col = f"{greek_name}_update"
    if update_col in book.columns:
        # Get the unique update times for contracts that have been updated
        updated_contracts = book[book[update_col].notnull()]
        unique_update_times = updated_contracts['time_stamp_update'].unique()

        print(f"\nUnique update times for {greek_name}:")
        for time in sorted(unique_update_times):
            print(time)

        print(f"\nNumber of unique update times: {len(unique_update_times)}")

        # Update the greek values and clean up
        book[greek_name] = book[update_col].combine_first(book[greek_name])
        book.drop(columns=[update_col, "time_stamp_update"], inplace=True)
    else:
        print(f"\nWarning: {update_col} not found in merged DataFrame. No updates performed.")

    return book


def prepare_to_fetch_historical_poly(current_time=None, shift_previous_minutes=0, shift_current_minutes=0):
    """
    Prepare parameters for the poly data fetch query based on the current time.

    :param current_time: Optional. The current time to use for calculations.
                         If None, uses the actual current time in New York timezone.
    :param shift_previous_minutes: Optional. Number of minutes to shift the previous datetime.
                                   Positive values shift forward, negative values shift backward. Default is 0.
    :param shift_current_minutes: Optional. Number of minutes to shift the current datetime.
                                  Positive values shift forward, negative values shift backward. Default is 0.
    :return: Tuple of (previous_date, current_date, previous_datetime, current_datetime)
    """
    nyse = mcal.get_calendar('NYSE')

    if current_time is None:
        current_time = datetime.now(ZoneInfo("America/New_York"))

    current_date = current_time.date()

    # Find the previous business day
    prev_5_days = current_date - timedelta(days=5)
    business_days = nyse.valid_days(start_date=prev_5_days, end_date=current_date)
    previous_date = business_days[-2].date()  # Second to last business day

    # Determine the current datetime (rounded to nearest 10 minutes)
    current_datetime = current_time.replace(minute=(current_time.minute // 10) * 10, second=0, microsecond=0)

    # Apply the shift to current datetime
    current_datetime += timedelta(minutes=shift_current_minutes)

    # Set the previous datetime to the same time on the previous business day and apply its shift
    previous_datetime = datetime.combine(previous_date, current_datetime.time())
    previous_datetime += timedelta(minutes=shift_previous_minutes)

    # Format dates and datetimes for the query
    previous_date_str = previous_date.strftime('%Y-%m-%d')
    current_date_str = current_date.strftime('%Y-%m-%d')
    previous_datetime_str = previous_datetime.strftime('%Y-%m-%d %H:%M:%S')
    current_datetime_str = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

    logger.debug(f"Prepared poly fetch parameters: "
                 f"Previous Date: {previous_date_str}, "
                 f"Current Date: {current_date_str}, "
                 f"Previous Datetime: {previous_datetime_str}, "
                 f"Current Datetime: {current_datetime_str}, "
                 f"Shift Previous Minutes: {shift_previous_minutes}, "
                 f"Shift Current Minutes: {shift_current_minutes}")

    return previous_date_str, current_date_str, previous_datetime_str, current_datetime_str


def filter_and_log_nan_values(final_book: pd.DataFrame) -> pd.DataFrame:
    """

    :param final_book:
    :return:
    """
    nan_rows = final_book[final_book.isna().any(axis=1)]
    nan_contracts = nan_rows['option_symbol'].unique()
    logger.debug(f"Filtered out {len(nan_rows)} rows with NaN values")
    logger.debug(f"Unique contracts filtered: {nan_contracts}")
    logger.debug(f"MM positions of filtered contracts: \n{nan_rows.groupby('option_symbol')['mm_posn'].sum()}")

    return final_book.dropna()


def ensure_all_connections_are_open():
    max_attempts = 5
    attempt = 0
    while attempt < max_attempts:
        db_status = db_utils.get_status()
        rabbitmq_status = rabbitmq_utils.get_status()

        if db_status['status'] == 'Connected' and rabbitmq_status['status'] == 'Connected':
            logger.debug("Both database and RabbitMQ connections are established.")
            return True

        logger.warning(
            f"Attempt {attempt + 1}: Connections not ready. DB: {db_status}, RabbitMQ: {rabbitmq_status}")
        time.sleep(5)  # Wait for 5 seconds before retrying
        attempt += 1

    logger.error("Failed to establish connections after maximum attempts.")
    return False


# ------------------ UPDATED HEATMAP ------------------#
@task
def save_heatmap_to_storage(heatmap_fig, timestamp):
    filename = f"heatmap_{timestamp.strftime('%Y%m%d_%H%M%S')}.png"

    # Convert Plotly figure to PNG
    img_bytes = heatmap_fig.to_image(format="png")

    # Configure the client
    session = boto3.session.Session()
    client = session.client('s3',
                            region_name='nyc3',  # Replace with your region
                            endpoint_url='https://heatmaps-gifs.nyc3.digitaloceanspaces.com',
                            # Replace with your endpoint
                            # TODO
                            aws_access_key_id='DO00PQZRAYM3PA449PL2',
                            # TODO
                            aws_secret_access_key='b3e3lewfKXkAkaE3ewfqgVOxwv83lmqdM98x93IhXok')

    # Upload to DO Spaces
    client.put_object(Bucket='heatmaps',
                      Key=filename,
                      Body=img_bytes,
                      ACL='private',
                      ContentType='image/png')

    return filename


@task
def retrieve_heatmap_from_storage(filename):
    # Configure the client (same as above)
    session = boto3.session.Session()
    client = session.client('s3',
                            region_name='nyc3',
                            endpoint_url='https://heatmaps-gifs.nyc3.digitaloceanspaces.com',
                            aws_access_key_id='DO00PQZRAYM3PA449PL2',
                            aws_secret_access_key='b3e3lewfKXkAkaE3ewfqgVOxwv83lmqdM98x93IhXok')

    # Download from DO Spaces
    response = client.get_object(Bucket='heatmaps', Key=filename)
    img_bytes = response['Body'].read()

    return BytesIO(img_bytes)


@task
def send_heatmap_discord(gamma_chart: go.Figure, as_of_time_stamp: str, session_date: str,
                         y_min: int, y_max: int, webhook_url: str) -> bool:
    title = f"📊 {session_date} Intraday Gamma Heatmap"
    current_time = datetime.utcnow()
    # Define the Eastern Time zone
    eastern_tz = pytz.timezone('America/New_York')
    # Convert UTC time to Eastern Time
    eastern_time = current_time.replace(tzinfo=pytz.utc).astimezone(eastern_tz)
    # Format the time in a friendly way
    friendly_time = eastern_time.strftime("%B %d, %Y at %I:%M %p %Z")
    fields = [
        # {"name": "📈 Analysis Type", "value": "Intraday Gamma Heatmap", "inline": True},
        {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
    ]
    footer_text = f"Generated on {friendly_time} | By OptionsDepth.com"

    # Prepare the embed
    embed = {
        "title": title,
        "color": 3447003,  # A nice blue color
        "fields": fields,
        "footer": {"text": footer_text},
        "image": {"url": "attachment://heatmap.png"}  # Reference the attached image
    }

    # Convert Plotly figure to image bytes
    img_bytes = gamma_chart.to_image(format="png", scale=3)

    # Prepare the payload
    payload = {
        "embeds": [embed]
    }

    # Prepare the files dictionary
    files = {
        "payload_json": (None, json.dumps(payload), "application/json"),
        "file": ("heatmap.png", img_bytes, "image/png")
    }

    # Send the request
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200 or response.status_code == 204:
        print(f"Heatmap for {session_date} sent successfully to Discord!")
        return True
    else:
        print(f"Failed to send heatmap. Status code: {response.status_code}")
        print(f"Response content: {response.content}")
        return False


@task(name="Send latest gamma heatmap to discord", task_run_name="Latest gamma heatmap to discord")
def intraday_gamma_heatmap(db, effective_datetime: str, effective_date: str):
    prefect_logger = get_run_logger()

    raw_gamma_data = fetch_gamma_data(db, effective_date, effective_datetime)
    prefect_logger.info("Fetched raw_gamma_data")
    processed_gamma_data = process_gamma_data(raw_gamma_data)
    cd_formatted_datetime = et_to_utc(effective_datetime)
    prefect_logger.info("Fetched all Data")
    # Fetch candlestick data (assuming you still need this)
    cd_query = f"""
    SELECT * FROM optionsdepth_stage.charts_candlestick
    WHERE ticker = 'SPX' 
    AND 
    effective_date = '{effective_date}'
    AND 
    effective_datetime <= '{cd_formatted_datetime}'
    AND
    effective_datetime > '{effective_date} 09:20:00'
    """

    candlesticks = db.execute_query(cd_query)

    if candlesticks.empty:
        spx_candlesticks = None
    else:
        prefect_logger.info(f"Fetched candlesticks")
        candlesticks_resampled = resample_and_convert_timezone(candlesticks)
        spx_candlesticks = candlesticks_resampled.set_index('effective_datetime', drop=False)

    # Filter data for current effective_datetime
    current_data = processed_gamma_data.copy()  # [processed_gamma_data['effective_datetime'] == effective_datetime]

    df_gamma = current_data.pivot_table(index='sim_datetime', columns='price', values='value')  # , aggfunc='first')

    # For minima_df and maxima_df, use the same index and columns as df_gamma
    minima_df = current_data.pivot_table(index='sim_datetime', columns='price', values='minima')  # , aggfunc='first')
    maxima_df = current_data.pivot_table(index='sim_datetime', columns='price', values='maxima')  # , aggfunc='first')

    # Fill NaN values in minima_df and maxima_df
    minima_df = minima_df.reindex_like(df_gamma).fillna(np.nan)
    maxima_df = maxima_df.reindex_like(df_gamma).fillna(np.nan)

    # Generate and send heatmap
    # gamma_chart = plot_gamma(df_heatmap=df_gamma, minima_df=minima_df, maxima_df=maxima_df,
    #                          effective_datetime=effective_datetime, spx=spx_candlesticks, y_min=5440, y_max=5735)

    gamma_chart = plot_gamma_test(df_gamma, minima_df, maxima_df, effective_datetime, spx_candlesticks,
                                  y_min=5550, y_max=5850,
                                  save_fig=False, fig_show=False,
                                  fig_path=None, show_projection_line=False)

    gamma_chart.update_layout(
        width=1920,  # Full HD width
        height=1080,  # Full HD height
        font=dict(size=16)  # Increase font size for better readability

    )

    # Call the new function
    success = send_heatmap_discord(
        gamma_chart=gamma_chart,
        as_of_time_stamp=effective_datetime,
        session_date=effective_date,
        y_min=5550,
        y_max=5850,
        webhook_url=HEATMAP_CHANNEL  # Make sure to define this
    )

    if success:
        print(f"Heatmap for {effective_datetime} has been processed and sent to Discord.")
    else:
        print(f"Failed to send heatmap for {effective_datetime} to Discord.")

    print(f"Heatmap for {effective_datetime} has been processed and saved.")


@task
def send_charm_heatmap_discord(charm_chart: go.Figure, as_of_time_stamp: str, session_date: str,
                               y_min: int, y_max: int, webhook_url: str) -> bool:
    title = f"📊 {session_date} Intraday Charm Heatmap"  # as of {as_of_time_stamp}"
    # description = (
    #     f"Detailed analysis of SPX Gamma for the {session_date} session.\n"
    #     f"This heatmap provides insights into market makers gamma exposure within the specified price range.\n"
    # )
    current_time = datetime.utcnow()
    # Define the Eastern Time zone
    eastern_tz = pytz.timezone('America/New_York')
    # Convert UTC time to Eastern Time
    eastern_time = current_time.replace(tzinfo=pytz.utc).astimezone(eastern_tz)
    # Format the time in a friendly way
    friendly_time = eastern_time.strftime("%B %d, %Y at %I:%M %p %Z")
    fields = [
        # {"name": "📈 Analysis Type", "value": "Intraday Gamma Heatmap", "inline": True},
        {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
    ]
    footer_text = f"Generated on {friendly_time} | By OptionsDepth.com"

    # Prepare the embed
    embed = {
        "title": title,
        # "description": description,
        "color": 3447003,  # A nice blue color
        "fields": fields,
        "footer": {"text": footer_text},
        # "timestamp": current_time,
        "image": {"url": "attachment://heatmap.png"}  # Reference the attached image
    }

    # Convert Plotly figure to image bytes
    img_bytes = charm_chart.to_image(format="png", scale=3)

    # Prepare the payload
    payload = {
        # "content": "🚀[UPDATE]: New Gamma Heatmap analysis is ready!",
        "embeds": [embed]
    }

    # Prepare the files dictionary
    files = {
        "payload_json": (None, json.dumps(payload), "application/json"),
        "file": ("heatmap.png", img_bytes, "image/png")
    }

    # Send the request
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200 or response.status_code == 204:
        print(f"Heatmap for {session_date} sent successfully to Discord!")
        return True
    else:
        print(f"Failed to send heatmap. Status code: {response.status_code}")
        print(f"Response content: {response.content}")
        return False


@task(name="Send latest gamma heatmap to discord", task_run_name="Latest gamma heatmap to discord")
def intraday_charm_heatmap(db, effective_datetime: str, effective_date: str):
    prefect_logger = get_run_logger()

    raw_charm_data = fetch_charm_data(db, effective_date, effective_datetime)
    prefect_logger.info("Fetched raw_gamma_data")
    processed_charm_data = process_charm_data(raw_charm_data)
    cd_formatted_datetime = et_to_utc(effective_datetime)
    prefect_logger.info("Fetched all Data")
    # Fetch candlestick data (assuming you still need this)
    cd_query = f"""
    SELECT * FROM optionsdepth_stage.charts_candlestick
    WHERE ticker = 'SPX' 
    AND 
    effective_date = '{effective_date}'
    AND 
    effective_datetime <= '{cd_formatted_datetime}'
    AND
    effective_datetime > '{effective_date} 09:20:00'
    """

    candlesticks = db.execute_query(cd_query)

    if candlesticks.empty:
        spx_candlesticks = None
    else:
        prefect_logger.info(f"Fetched candlesticks")
        candlesticks_resampled = resample_and_convert_timezone(candlesticks)
        spx_candlesticks = candlesticks_resampled.set_index('effective_datetime', drop=False)

    # Filter data for current effective_datetime
    current_data = processed_charm_data.copy()  # [processed_gamma_data['effective_datetime'] == effective_datetime]

    df_charm = current_data.pivot_table(index='sim_datetime', columns='price', values='value')  # , aggfunc='first')

    # Generate and send heatmap
    charm_chart = plot_charm(df=df_charm,
                             effective_datetime=effective_datetime,
                             spx=spx_candlesticks)

    charm_chart.update_layout(
        width=1920,  # Full HD width
        height=1080,  # Full HD height
        font=dict(size=16)  # Increase font size for better readability

    )

    # Call the new function
    success = send_charm_heatmap_discord(
        charm_chart=charm_chart,
        as_of_time_stamp=effective_datetime,
        session_date=effective_date,
        y_min=5550,
        y_max=5700,
        webhook_url=CHARM_HEATMAP_CHANNEL  # Make sure to define this
    )

    if success:
        print(f"Heatmap for {effective_datetime} has been processed and sent to Discord.")
    else:
        print(f"Failed to send heatmap for {effective_datetime} to Discord.")

    print(f"Heatmap for {effective_datetime} has been processed and saved.")


# -----------------DEPTHVIEW-----------------#
@task(name="Send latest depthview to discord", task_run_name="Latest depthview to discord")
def intraday_depthview():
    pass


@task(name="Send latest depthflow to discord", task_run_name="Latest depthview to discord")
def intraday_depthflow():
    pass


# ----------------- TASKS -------------------#
@task(name="fetch_historical_poly_data", task_run_name="Fetching historical greeks")
def fetch_historical_poly_data(previous_date, current_date, previous_datetime, current_datetime):
    start_time = time_module.time()
    query = f"""
    SELECT option_symbol, contract_type, strike_price, expiration_date, time_stamp,
           implied_volatility, delta, gamma, vega
    FROM landing.poly_options_data
    WHERE
        date_only BETWEEN'{previous_date}' AND '{current_date}'
        AND time_stamp BETWEEN '{previous_datetime}' AND '{current_datetime}'
    """

    poly_data = db_utils.execute_query(query)
    logger.info(f'Fetched {len(poly_data)} rows in {time_module.time() - start_time} sec.')

    return poly_data


@task(retries=3, retry_delay_seconds=60, name="process_last_message_in_queue",
      task_run_name="Processing last message in queue...")
async def process_last_message_in_queue(rabbitmq_utils: RabbitMQUtilities, expected_file_override=None):
    logger.info("Processing last message in queue...")

    try:
        rabbitmq_utils.connect()

        if expected_file_override:
            expected_file_name = expected_file_override
            logger.info(f"[OVERRIDE]: Getting {expected_file_name}")

            return expected_file_name
        else:
            expected_file_name = determine_expected_file_name()

        logger.info(f"Expected file name: {expected_file_name}")

        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < RABBITMQ_MAX_RUNTIME:
            try:
                messages = rabbitmq_utils.fetch_all_messages_in_queue(RABBITMQ_CBOE_QUEUE)
                message_count = len(messages)
                logger.debug(f"Messages in queue: {message_count}")

                if messages:
                    for method_frame, properties, body in reversed(messages):  # Process from newest to oldest
                        msg_decoded = body.decode('utf-8')
                        msg_body = json.loads(msg_decoded)
                        file_name = msg_body.get("filename")

                        if file_name == expected_file_name:
                            logger.debug(f"[FOUND]: Expected file {expected_file_name}")
                            return (method_frame, properties, body), msg_body
                        else:
                            rabbitmq_utils.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                            logger.debug(
                                f"[NOT FOUND]: Expected file {expected_file_name} not found. Retrying in {PROCESS_MESSAGE_QUEUE_RETRY_DELAY} seconds...")
                else:
                    logger.debug(f"Queue is empty. Retrying in {PROCESS_MESSAGE_QUEUE_RETRY_DELAY} seconds...")

                time.sleep(PROCESS_MESSAGE_QUEUE_RETRY_DELAY)

            except Exception as e:
                logger.error(f"Error occurred while checking messages: {e}")
                time.sleep(PROCESS_MESSAGE_QUEUE_RETRY_DELAY)

        logger.debug(f"Maximum runtime reached. Expected file {expected_file_name} not found.")
        return None, None

    except Exception as e:
        logger.error(f"Error in process_last_message: {e}")
        raise

    finally:
        # Don't close the connection here
        pass


@task(name="Get initial book",
      task_run_name="Getting initial book...",
      description="Updates the book of financial options contracts with the latest Greek values.",
      retries=2,
      retry_delay_seconds=60,
      timeout_seconds=60,
      # cache_key_fn=task_input_hash,
      # cache_expiration=timedelta(hours=12),

      )
def get_initial_book(get_unrevised_book: Callable):
    prefect_logger = get_run_logger()
    try:
        current_time = datetime.now(ZoneInfo("America/New_York"))
        current_date = current_time.date()
        limit2am = current_time.replace(hour=0, minute=15, second=0, microsecond=0).time()
        limit7pm = current_time.replace(hour=23, minute=0, second=0, microsecond=0).time()

        # TODO: Verify that the current_date is a business date

        if limit2am <= current_time.time() < limit7pm:  # Between 2 AM and 7 PM
            prefect_logger.info(f"Operating during Revised book hours")
            query = f"""
            SELECT * FROM intraday.new_daily_book_format 
            WHERE effective_date = '{current_date}' AND revised = 'Y'
            """
            session_book = db_utils.execute_query(query)

            prefect_logger.info(f"Got Revised book of {session_book['effective_date']}")

            if session_book.empty:
                query = f"""
                SELECT * FROM intraday.new_daily_book_format 
                WHERE effective_date = '{current_date}' AND revised = 'N'
                """
                session_book = db_utils.execute_query(query)
                prefect_logger.info("Getting Unrevised book")
                if session_book.empty:
                    send_notification(f"No session_book found for {current_date}. Using unrevised session_book.")
                    return get_unrevised_book()
                else:
                    # send_notification(f"Unrevised session_book found for {current_date}. Proceeding with caution.")
                    return session_book
            else:
                logger.debug(f"Revised session_book loaded for date: {current_date}")
                # TODO: verify it makes sense
                return session_book

        else:  # Between 8 PM and 2 AM

            # TODO:
            # if it's sunday, get revised book since it runs Friday-Saturday
            # else;
            logger.debug("Using unrevised session_book due to time of day.")
            return get_unrevised_book()

    except Exception as e:
        logger.error(f"Error getting initial session_book: {e}")
        raise


# This function will be implemented later

@task
def get_unrevised_book():
    logger.info(f"Operating during Unrevised book hours")
    logger.info(f"Operating during Unrevised book hours")

    unrevised_book = pd.read_pickle("unrevised_book_20240730.pkl")
    unrevised_book.drop(columns=['effective_datetime'], inplace=True)

    return unrevised_book

    # #TODO: Correct this
    # current_time = datetime.now(ZoneInfo("America/New_York"))
    # current_date = current_time.date()
    #
    # query = f"""
    # SELECT * FROM intraday.new_daily_book_format
    # WHERE effective_date = '{current_date}' AND revised = 'N'
    # """
    #
    # try:
    #     unrevised_book = db_utils.execute_query(query)
    #
    #     if unrevised_book.empty:
    #         #TODO; send notification
    #         pass

    #         #unrevised_book = generate_unrevised_book(current_date)
    #         #TODO: verify it makes sens
    #         #unrevised_book
    #     else:
    #         logger.debug(f"Revised session_book loaded for date: {current_date}")
    #         #TODO: verify it makes sense
    #         return unrevised_book
    #
    # except Exception as e:
    #     logger.error(f"Error getting initial session_book: {e}")
    #     raise


@task(retries=2)
def get_file_from_sftp(msg_body, override_path=None):
    prefect_logger = get_run_logger()
    logger.debug("get_file_from_sftp")
    file_path = msg_body["path"]
    if override_path:
        file_path = override_path
        logger.info(f'[OVERRIDE]: Getting {file_path}')

    else:
        file_path = msg_body["path"]
        logger.info(f'[NO OVERRIDE]: Getting {file_path}')
    try:

        # sftp_utils.ensure_connection()  # Ensure connection before SFTP operation

        start = time_module.time()
        sftp_utils.connect()
        prefect_logger.info(f'It took {time_module.time() - start} sec. to connect')

        read_time = time_module.time()
        file_info = sftp_utils.get_file_info(file_path)
        file_data = sftp_utils.read_file(file_path)
        prefect_logger.info(f'It took {time_module.time() - read_time} sec. to read to file')
        logger.debug(f'[verify_and_process_message]: {file_info} {file_data}')

        return file_info, file_data, file_info['mtime']

    except Exception as e:
        logger.error(f"Error verifying and processing file {file_path}: {str(e)}")
        raise
    finally:
        sftp_utils.disconnect()


@task(retries=2)
async def read_and_verify_sftp_file(file_info, file_data):
    try:
        with zipfile.ZipFile(file_data, 'r') as z:
            csv_file = z.namelist()[0]
            with z.open(csv_file) as f:
                chunk_size = 10000
                chunks = []
                for chunk in pd.read_csv(f, chunksize=chunk_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)

        logger.debug(f"File read and verified successfully: {file_info['file_name']}")

        if 'call_or_put' in df.columns:
            df = df.rename(columns={'call_or_put': 'call_put_flag'})
        if 'underlying' in df.columns:
            df = df.rename(columns={'underlying': 'ticker'})

        df['expiration_date'] = pd.to_datetime(df['expiration_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        if OPTION_SYMBOLS_TO_PROCESS:
            df = df[df['option_symbol'].isin(OPTION_SYMBOLS_TO_PROCESS)]

        # TODO: Test with this
        df = verify_data(df)

        # Convert

        return df
    except Exception as e:
        logger.error(f"Failed to read or verify file: {e}")
        raise


@task(retries=2)
async def build_latest_book(initial_book, intraday_data):
    logger.debug('Entered update_book_intraday')
    if 'id' in initial_book.columns:
        initial_book = initial_book.drop(columns=['id'])

    # Ensure consistent data types --- It's an additional layer of protection
    initial_book['strike_price'] = initial_book['strike_price'].astype(float)
    intraday_data['strike_price'] = intraday_data['strike_price'].astype(float)
    initial_book['expiration_date_original'] = pd.to_datetime(initial_book['expiration_date_original'])
    intraday_data['expiration_date'] = pd.to_datetime(intraday_data['expiration_date'])

    processed_intraday = intraday_data.groupby(
        ['ticker', 'option_symbol', 'call_put_flag', 'expiration_date', 'strike_price']).agg({
        'mm_buy_vol': 'sum',
        'mm_sell_vol': 'sum',
        'firm_open_buy_vol': 'sum',
        'firm_close_buy_vol': 'sum',
        'firm_open_sell_vol': 'sum',
        'firm_close_sell_vol': 'sum',
        'bd_open_buy_vol': 'sum',
        'bd_close_buy_vol': 'sum',
        'bd_open_sell_vol': 'sum',
        'bd_close_sell_vol': 'sum',
        'cust_lt_100_open_buy_vol': 'sum',
        'cust_lt_100_close_buy_vol': 'sum',
        'cust_lt_100_open_sell_vol': 'sum',
        'cust_lt_100_close_sell_vol': 'sum',
        'cust_100_199_open_buy_vol': 'sum',
        'cust_100_199_close_buy_vol': 'sum',
        'cust_100_199_open_sell_vol': 'sum',
        'cust_100_199_close_sell_vol': 'sum',
        'cust_gt_199_open_buy_vol': 'sum',
        'cust_gt_199_close_buy_vol': 'sum',
        'cust_gt_199_open_sell_vol': 'sum',
        'cust_gt_199_close_sell_vol': 'sum',
        'procust_lt_100_open_buy_vol': 'sum',
        'procust_lt_100_close_buy_vol': 'sum',
        'procust_lt_100_open_sell_vol': 'sum',
        'procust_lt_100_close_sell_vol': 'sum',
        'procust_100_199_open_buy_vol': 'sum',
        'procust_100_199_close_buy_vol': 'sum',
        'procust_100_199_open_sell_vol': 'sum',
        'procust_100_199_close_sell_vol': 'sum',
        'procust_gt_199_open_buy_vol': 'sum',
        'procust_gt_199_close_buy_vol': 'sum',
        'procust_gt_199_open_sell_vol': 'sum',
        'procust_gt_199_close_sell_vol': 'sum',
        'trade_datetime': 'max'
    }).reset_index()

    processed_intraday['mm_posn'] = processed_intraday['mm_buy_vol'] - processed_intraday['mm_sell_vol']
    processed_intraday['firm_posn'] = (
            processed_intraday['firm_open_buy_vol'] + processed_intraday['firm_close_buy_vol'] -
            processed_intraday['firm_open_sell_vol'] - processed_intraday['firm_close_sell_vol'])
    processed_intraday['broker_posn'] = (
            processed_intraday['bd_open_buy_vol'] + processed_intraday['bd_close_buy_vol'] -
            processed_intraday['bd_open_sell_vol'] - processed_intraday['bd_close_sell_vol'])
    processed_intraday['nonprocust_posn'] = (
            (processed_intraday['cust_lt_100_open_buy_vol'] + processed_intraday['cust_lt_100_close_buy_vol'] +
             processed_intraday['cust_100_199_open_buy_vol'] + processed_intraday['cust_100_199_close_buy_vol'] +
             processed_intraday['cust_gt_199_open_buy_vol'] + processed_intraday['cust_gt_199_close_buy_vol']) -
            (processed_intraday['cust_lt_100_open_sell_vol'] + processed_intraday['cust_lt_100_close_sell_vol'] +
             processed_intraday['cust_100_199_open_sell_vol'] + processed_intraday['cust_100_199_close_sell_vol'] +
             processed_intraday['cust_gt_199_open_sell_vol'] + processed_intraday['cust_gt_199_close_sell_vol']))
    processed_intraday['procust_posn'] = ((processed_intraday['procust_lt_100_open_buy_vol'] + processed_intraday[
        'procust_lt_100_close_buy_vol'] +
                                           processed_intraday['procust_100_199_open_buy_vol'] + processed_intraday[
                                               'procust_100_199_close_buy_vol'] +
                                           processed_intraday['procust_gt_199_open_buy_vol'] + processed_intraday[
                                               'procust_gt_199_close_buy_vol']) -
                                          (processed_intraday['procust_lt_100_open_sell_vol'] + processed_intraday[
                                              'procust_lt_100_close_sell_vol'] +
                                           processed_intraday['procust_100_199_open_sell_vol'] + processed_intraday[
                                               'procust_100_199_close_sell_vol'] +
                                           processed_intraday['procust_gt_199_open_sell_vol'] + processed_intraday[
                                               'procust_gt_199_close_sell_vol']))

    processed_intraday['total_customers_posn'] = (
            processed_intraday['firm_posn'] + processed_intraday['broker_posn'] +
            processed_intraday['nonprocust_posn'] + processed_intraday['procust_posn'])

    # -------------------- DATA CONVERSION AND VALIDATION ----------------#
    # Data Manipulation muste be done:

    # Convert strike_price in initial_book from Decimal to float
    initial_book['strike_price'] = initial_book['strike_price'].astype(float)

    # Convert strike_price in processed_intraday from np.float64 to float (this might not be necessary, but it ensures consistency)
    processed_intraday['strike_price'] = processed_intraday['strike_price'].astype(float)

    # Convert expiration_date_original in initial_book from datetime.date to datetime64[ns]
    initial_book['expiration_date_original'] = pd.to_datetime(initial_book['expiration_date_original'])

    # Convert expiration_date in processed_intraday from string to datetime64[ns]
    processed_intraday['expiration_date'] = pd.to_datetime(processed_intraday['expiration_date'])

    # ------------------------------------------------------------#

    initial_duplicates = analyze_duplicates(initial_book, "initial_book", INITAL_BOOK_KEY, logger)
    intraday_duplicates = analyze_duplicates(processed_intraday, "processed_intraday", INTRADAY_KEY, logger)

    merged = pd.merge(initial_book, processed_intraday,
                      left_on=['ticker', 'option_symbol', 'call_put_flag', 'strike_price',
                               'expiration_date_original'],
                      right_on=['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date'],
                      how='outer', suffixes=('', '_intraday'))

    position_columns = ['mm_posn', 'firm_posn', 'broker_posn', 'nonprocust_posn', 'procust_posn',
                        'total_customers_posn']

    for col in position_columns:
        merged[col] = merged[col].fillna(0) + merged[f'{col}_intraday'].fillna(0)
        merged.loc[merged[col].isna(), col] = merged[f'{col}_intraday']
    merged['expiration_date_original'] = merged['expiration_date_original'].fillna(
        merged['expiration_date_intraday'])

    merged['revised'] = 'Y'
    merged['time_stamp'] = None

    merged['trade_datetime'] = pd.to_datetime(merged['trade_datetime'], errors='coerce')
    max_trade_datetime = merged['trade_datetime'].max()
    if pd.notnull(max_trade_datetime):
        effective_date = max_trade_datetime.date()
        effective_datetime = max_trade_datetime
    else:
        current_datetime = datetime.now()
        effective_date = current_datetime.date()
        effective_datetime = current_datetime

    merged['effective_date'] = effective_date
    merged['effective_datetime'] = effective_datetime

    # TODO: Not TRUE for Unrevised
    as_of_date = initial_book['as_of_date'].max()
    merged['as_of_date'] = as_of_date

    start_of_day_columns = [col for col in initial_book.columns if col != 'id']
    effective_date_index = start_of_day_columns.index('effective_date')
    start_of_day_columns.insert(effective_date_index + 1, 'effective_datetime')
    merged = merged[start_of_day_columns]

    merged['expiration_date'] = merged.apply(get_expiration_datetime, axis=1)
    merged['expiration_date'] = pd.to_datetime(merged['expiration_date'])
    position_columns = ['mm_posn', 'firm_posn', 'broker_posn', 'nonprocust_posn', 'procust_posn',
                        'total_customers_posn']
    merged = merged.loc[(merged[position_columns] != 0).any(axis=1)]
    merged = merged.sort_values(['expiration_date_original', 'mm_posn'], ascending=[True, False])

    merged_duplicates = analyze_duplicates(merged, "merged", MERGED_BOOK_KEY, logger)

    logger.info('Finished update_book_intraday')

    return merged


@task(
    name="update_book_with_latest_greeks",
    description="Updates the book of financial options contracts with the latest Greek values.",
    tags=["finance", "options", "greeks"],
    retries=3,
    retry_delay_seconds=5,
    timeout_seconds=600,
    log_prints=True
)
def update_book_with_latest_greeks(book: pd.DataFrame, poly_historical_data: pd.DataFrame) -> pd.DataFrame:
    """
    We fetch the live poly data
    We merge it with the latest book
    We log how many have been merged and how many haven't been merged.

    For the unmerged contracts, we look for them in our poly_historical_data and update them
    We return the latest book updated with its latest greeks.

    :param book: DataFrame containing the current book of contracts
    :param poly_historical_data: DataFrame containing historical data for the contracts
    :return: Updated DataFrame with the latest Greek values
    """
    custom_logger = get_logger()
    prefect_logger = get_run_logger()

    custom_logger.info('Entered update_book_with_latest_greeks')
    prefect_logger.info('Entered update_book_with_latest_greeks')

    logger.debug('Entered update_book_with_latest_greeks')

    try:
        latest_poly = get_latest_poly(client)
    except Exception as e:
        error_message = f"Failed to fetch latest poly data: {str(e)}. Proceeding with historical data only."
        custom_logger.error(error_message)
        prefect_logger.error(error_message)
        latest_poly = pd.DataFrame()  # Create an empty DataFrame to use in the merge step



    # Create a key for faster merge
    book['strike_price'] = book['strike_price'].astype(int)
    book['contract_id'] = book['option_symbol'] + '_' + \
                          book['call_put_flag'] + '_' + \
                          book['strike_price'].astype(str) + '_' + \
                          pd.to_datetime(book['expiration_date_original']).dt.strftime('%Y-%m-%d')

    # latest_poly.rename(columns={'implied_volatility': 'iv'}, inplace=True)
    # latest_poly['contract_id'] = latest_poly['option_symbol'] + '_' + \
    #                              latest_poly['contract_type'] + '_' + \
    #                              latest_poly['strike_price'].astype(str) + '_' + \
    #                              latest_poly['expiration_date']

    # # Merge latest book with the latest poly data
    # merged_book = pd.merge(book, latest_poly, on='contract_id', how='left', suffixes=('', '_update'))

    if not latest_poly.empty:
        prefect_logger.info('Latest Poly Data is not empty ')
        latest_poly.rename(columns={'implied_volatility': 'iv'}, inplace=True)
        latest_poly['contract_id'] = latest_poly['option_symbol'] + '_' + \
                                     latest_poly['contract_type'] + '_' + \
                                     latest_poly['strike_price'].astype(str) + '_' + \
                                     latest_poly['expiration_date']

        # Merge latest book with the latest poly data
        merged_book = pd.merge(book, latest_poly, on='contract_id', how='left', suffixes=('', '_update'))

    else:
        # If latest_poly is empty, just use the book as is
        prefect_logger.info('!!!!! Latest Poly Data is empty !!!!!!')
        merged_book = book.copy()
        for col in ['iv_update', 'delta_update', 'gamma_update', 'vega_update']:
            merged_book[col] = np.nan

    # Log merge results
    total_contracts = len(book)
    merged_contracts = merged_book['iv_update'].notna().sum()
    unmerged_contracts = total_contracts - merged_contracts

    # TODO: Jusqu'ici, pas de probleme... But after the historical merge, we lose some contracts that are in merged_book

    historical_updates = 0
    if unmerged_contracts > 0:

        if poly_historical_data.empty:
            error_message = "poly_historical_data is empty. Skipping historical data updates."
            custom_logger.warning(error_message)
            prefect_logger.warning(error_message)
            print("EMPTYYYYYYYYY")
            # send_discord_message(error_message)  # Assuming you have this function defined

        else:
            # Isolating the contracts that haven't been updated with the latest poly fetch
            mask = merged_book['iv_update'].isna()
            unmerged_contracts_df = merged_book[mask]

            # Creating a key for merging purposes
            poly_historical_data.rename(columns={'implied_volatility': 'iv'}, inplace=True)
            poly_historical_data['contract_id'] = poly_historical_data['option_symbol'] + '_' + \
                                                  poly_historical_data['contract_type'] + '_' + \
                                                  poly_historical_data['strike_price'].astype(str) + '_' + \
                                                  poly_historical_data['expiration_date']

            # Get the latest historical data for unmerged contracts.
            latest_historical = poly_historical_data.sort_values('time_stamp', ascending=False).groupby(
                'contract_id').first().reset_index()

            # Merge unmerged contracts with latest historical data
            updated_unmerged = pd.merge(unmerged_contracts_df,
                                        latest_historical,
                                        # latest_historical[['contract_id', 'iv', 'delta', 'gamma', 'vega']],
                                        on='contract_id', how='left', suffixes=('', '_historical'))

            # Update the merged_book with historical data
            # merged_book.update(updated_unmerged)
            # Instead of using update, we'll use a more controlled approach
            for greek in ['iv', 'delta', 'gamma', 'vega']:
                historical_column = f'{greek}_historical'
                merged_book.loc[mask, greek] = updated_unmerged[historical_column].combine_first(
                    merged_book.loc[mask, greek])

            historical_updates = updated_unmerged['iv_historical'].notna().sum()

    # ------- INVESTIGATION----------#

    # After all updates, identify contracts that weren't updated
    not_updated_mask = merged_book[['iv_update', 'delta_update', 'gamma_update', 'vega_update']].isna().all(axis=1)

    not_updated_contracts = merged_book[not_updated_mask]

    # Store not updated contracts in a CSV file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"not_updated_contracts_{timestamp}.csv"
    not_updated_contracts.to_csv(csv_filename, index=False)

    prefect_logger.info(f"Contracts not updated have been saved to {csv_filename}")

    # Calculate final statistics
    contracts_with_greeks = merged_book[['iv', 'delta', 'gamma', 'vega']].notna().all(axis=1).sum()
    contracts_without_greeks = total_contracts - contracts_with_greeks
    contracts_not_updated = not_updated_contracts.shape[0]

    # Get mm_posn for contracts without greeks
    contracts_without_greeks_df = merged_book[merged_book[['iv', 'delta', 'gamma', 'vega']].isna().any(axis=1)]
    mm_posn_sum = contracts_without_greeks_df['mm_posn'].sum()

    # Calculate final statistics
    # Clean up the merged book
    for greek in ['iv', 'delta', 'gamma', 'vega']:
        update_column = f'{greek}_update'
        historical_column = f'{greek}_historical'

        merged_book[greek] = merged_book[update_column].combine_first(merged_book[greek])

        # Drop temporary columns
        merged_book.drop(columns=[update_column, historical_column], errors='ignore', inplace=True)

    # Log the number of contracts at this point
    prefect_logger.info(f"Number of contracts after Greek updates: {len(merged_book)}")

    # Calculate final statistics
    contracts_with_greeks = merged_book[['iv', 'delta', 'gamma', 'vega']].notna().all(axis=1).sum()
    # contracts_with_greeks = merged_book['iv'].notna().sum()
    contracts_without_greeks = total_contracts - contracts_with_greeks
    contracts_not_updated = total_contracts - merged_contracts - historical_updates

    # Get mm_posn for contracts without greeks
    contracts_without_greeks_df = merged_book[merged_book['iv'].isna()]
    mm_posn_sum = contracts_without_greeks_df['mm_posn'].sum()

    # Prepare log message
    log_data = f"""
    #---------- Greeks Updates Summary ----------#
    Total contracts: {total_contracts}
    Contracts updated with latest market data: {merged_contracts}
    Contracts updated with historical data: {historical_updates}
    Contracts not updated (kept initial greeks): {contracts_not_updated}
    Contracts without greeks after all updates: {contracts_without_greeks}
    Sum of mm_posn for contracts without greeks: {mm_posn_sum}
    #-------------------------------------------------#
    """

    # Log using both loggers
    logger.info(log_data)
    prefect_logger.info(log_data)

    # Filter and keep the rows without NaN values in greeks
    mask = ~merged_book[['iv', 'delta', 'gamma', 'vega']].isna().any(axis=1)
    merged_book = merged_book[mask]

    columns_to_keep = book.columns[:-1]
    merged_book = merged_book[columns_to_keep]
    merged_book.loc[:, 'time_stamp'] = get_eastern_time()

    return merged_book


def compare_dataframes(posn_only, final_book_clean_insert):
    prefect_logger = get_run_logger()
    prefect_logger.info(
        f"Row counts: posn_only: {len(posn_only)}, final_book_clean_insert: {len(final_book_clean_insert)}")

    posn_columns = [col for col in posn_only.columns if '_posn' in col]

    prefect_logger.info("\nData types comparison:")
    for col in posn_columns:
        prefect_logger.info(
            f"{col}: posn_only: {posn_only[col].dtype}, final_book_clean_insert: {final_book_clean_insert[col].dtype}")

    prefect_logger.info("\nNaN value counts:")
    for col in posn_columns:
        prefect_logger.info(
            f"{col}: posn_only: {posn_only[col].isna().sum()}, final_book_clean_insert: {final_book_clean_insert[col].isna().sum()}")

    prefect_logger.info("\nValue differences:")
    for col in posn_columns:
        diff = (posn_only[col] != final_book_clean_insert[col]).sum()
        prefect_logger.info(f"{col}: {diff} differences")

    prefect_logger.info("\nSample of differences:")
    for col in posn_columns:
        mask = (posn_only[col] != final_book_clean_insert[col])
        if mask.any():
            diff_df = pd.DataFrame({
                'posn_only': posn_only.loc[mask, col],
                'final_book_clean_insert': final_book_clean_insert.loc[mask, col]
            })
            prefect_logger.info(f"\n{col}:")
            prefect_logger.info(diff_df.head())


# ----------------- FLOWS ------------------#
@flow(name="Post-Processing Flow 1")
def post_processing_flow_1():
    logger = get_run_logger()
    logger.info("Post-Processing Flow 1 is running")
    # Add any post-processing logic here


@flow(name="Post-Processing Flow 2")
def post_processing_flow_2():
    logger = get_run_logger()
    logger.info("Post-Processing Flow 2 is running")
    # Add any post-processing logic here


@flow(
    name="Intraday Flow",
    description="""
    This flow processes intraday financial data for options trading.
    It performs the following main steps:
    1. Loads the initial book of contracts
    2. Fetches historical poly data
    3. Processes the latest message from the queue
    4. Retrieves and verifies SFTP file data
    5. Builds the latest book with updated contract information
    6. Updates the book with the latest Greeks values
    7. Filters out invalid entries and logs the results
    8. Acknowledges the processed message in RabbitMQ

    The flow handles various edge cases and exceptions, ensuring robust
    data processing and connection management throughout its execution.
    """,
    version="1.0.0",
    retries=3,
    retry_delay_seconds=300,  # 5 minutes
    timeout_seconds=3600,  # 1 hour
    # validate_parameters=True
)
def Intraday_Flow():
    prefect_logger = get_run_logger()

    flow_start_time = time_module.time()
    current_time = datetime.now(ZoneInfo("America/New_York")).time()

    expected_file_override = None  # '/subscriptions/order_000059435/item_000068201/Cboe_OpenClose_2024-08-15_15_00_1.csv.zip'

    db_utils.connect()

    try:

        # TODO: initial_price, last_price = get_prices()

        # parallel_subflows = [zero_dte_flow(), one_dte_flow()]
        # await asyncio.gather(*parallel_subflows)

        initial_book = get_initial_book(get_unrevised_book)
        book_date_loaded = initial_book["effective_date"].unique()
        prefect_logger.info(f"Initial Book of {book_date_loaded} loaded")

        previous_date, current_date, previous_datetime, current_datetime = prepare_to_fetch_historical_poly(
            shift_previous_minutes=60 * 8, shift_current_minutes=0)

        # TODO: Will be faster on postgre
        # Fetch poly data: Slow
        poly_historical_data = fetch_historical_poly_data(previous_date, current_date, previous_datetime,
                                                          current_datetime)

        # --------- Connections --------#
        rabbitmq_utils.connect()
        connections_ready = ensure_all_connections_are_open()
        if not connections_ready:
            logger.error("Unable to establish necessary connections. Aborting flow.")
            return

        logger.debug(f"[process_intraday_data] db status: {db_utils.get_status()}")
        logger.debug(f"[process_intraday_data] rabbitmq status: {rabbitmq_utils.get_status()}")

        message, msg_body = process_last_message_in_queue(rabbitmq_utils)
        message_frame, message_properties, _ = message

        if message is None and msg_body is None:
            logger.debug("No messages to process in the queue. Ending the flow.")
            return

        if message is None:
            logger.debug("No message to process in process_intraday_data Flow")
            return

        logger.debug(f"Process_last_message returned: {msg_body}")

        try:
            # sftp_utils.ensure_connection()  # Ensure connection before SFTP operation
            file_info, file_data, file_last_modified = get_file_from_sftp(msg_body, expected_file_override)
            if file_info:
                logger.debug(f"FILE INFO ---> {file_info}")
                df = read_and_verify_sftp_file(file_info, file_data)
                if df is not None and not df.empty:
                    logger.debug(f"Successfully processed file: {file_info['file_name']}")
                    logger.debug(f"DataFrame shape: {df.shape}")
                    latest_book = build_latest_book(initial_book, df)

                    # ------- Posn Only --------#
                    posn_only = latest_book.iloc[:, :-4]
                    posn_only.loc[:, 'time_stamp'] = get_eastern_time()
                    # Log the number of rows
                    # TODO: UNCOMMENT
                    db_utils.insert_progress('intraday', 'intraday_books_test_posn', posn_only)

                    latest_book["strike_price"].astype(int)

                    final_book = update_book_with_latest_greeks(latest_book, poly_historical_data)

                    # -------------- INVESRTIGATION ---------------#
                    # Assuming final_book and latest_book are your dataframes
                    # Merge the dataframes

                    merged_book = pd.merge(latest_book, final_book,
                                           on=['option_symbol', 'strike_price', 'expiration_date', 'call_put_flag'],
                                           suffixes=('_latest', '_final'))

                    # Calculate the difference in total_customers_posn
                    merged_book['total_customers_posn_diff'] = merged_book['total_customers_posn_final'] - merged_book[
                        'total_customers_posn_latest']

                    # Filter rows where there's a difference
                    diff_rows = merged_book[merged_book['total_customers_posn_diff'] != 0]

                    # Display the differences
                    prefect_logger.info("Rows with differences in total_customers_posn:")
                    prefect_logger.info(diff_rows[['option_symbol', 'strike_price', 'expiration_date', 'call_put_flag',
                                                   'total_customers_posn_latest', 'total_customers_posn_final',
                                                   'total_customers_posn_diff']])

                    # Calculate some statistics
                    prefect_logger.info("\nStatistics of differences:")
                    prefect_logger.info(diff_rows['total_customers_posn_diff'].describe())

                    # Check for missing rows
                    latest_book_rows = set(
                        latest_book[['option_symbol', 'strike_price', 'expiration_date', 'call_put_flag']].apply(tuple,
                                                                                                                 axis=1))
                    final_book_rows = set(
                        final_book[['option_symbol', 'strike_price', 'expiration_date', 'call_put_flag']].apply(tuple,
                                                                                                                axis=1))

                    missing_in_final = latest_book_rows - final_book_rows
                    missing_in_latest = final_book_rows - latest_book_rows

                    prefect_logger.info(f"\nRows in latest_book but missing in final_book: {len(missing_in_final)}")
                    prefect_logger.info(f"Rows in final_book but missing in latest_book: {len(missing_in_latest)}")

                    # The contracts that have a position but that don't have greeks
                    if missing_in_final:
                        missing_in_final_df = pd.DataFrame(list(missing_in_final),
                                                           columns=['option_symbol', 'strike_price', 'expiration_date',
                                                                    'call_put_flag'])

                        # Convert expiration_date to datetime if it's not already
                        missing_in_final_df['expiration_date'] = pd.to_datetime(missing_in_final_df['expiration_date'])

                        # Merge with latest_book to get the total_customers_posn
                        missing_in_final_df = pd.merge(
                            missing_in_final_df,
                            latest_book[['option_symbol', 'strike_price', 'expiration_date', 'call_put_flag',
                                         'total_customers_posn']],
                            on=['option_symbol', 'strike_price', 'expiration_date', 'call_put_flag'],
                            how='left'
                        )

                        # Sort by expiration_date and total_customers_posn
                        missing_in_final_df = missing_in_final_df.sort_values(
                            ['expiration_date', 'total_customers_posn'], ascending=[True, False])

                        prefect_logger.info(
                            "\nSample of rows missing in final_book (ordered by expiration_date and total_customers_posn):")
                        prefect_logger.info(missing_in_final_df.head())

                        # Save to CSV
                        missing_in_final_df.to_csv('missing_in_final_book.csv', index=False)
                        prefect_logger.info(
                            "Full list of missing rows in final_book saved to 'missing_in_final_book.csv'")

                        # Count contracts for each distinct expiration_date
                        expiration_counts = missing_in_final_df['expiration_date'].value_counts().sort_index()
                        prefect_logger.info("\nNumber of contracts for each distinct expiration_date:")
                        prefect_logger.info(expiration_counts)

                        # List strikes and total_customers_posn for each distinct expiration date
                        prefect_logger.info("-----------------------------------------------")
                        prefect_logger.info(
                            "\nList of strikes and total_customers_posn for each distinct expiration date:")
                        for date in missing_in_final_df['expiration_date'].unique():
                            date_df = missing_in_final_df[missing_in_final_df['expiration_date'] == date]
                            strikes = date_df['strike_price'].unique()
                            strikes.sort()
                            prefect_logger.info(f"Expiration Date: {date.date()}")
                            prefect_logger.info(f"Strikes: {', '.join(map(str, strikes))}")
                            prefect_logger.info(f"Number of strikes: {len(strikes)}")
                            prefect_logger.info("Top 5 contracts by total_customers_posn:")
                            prefect_logger.info(date_df.nlargest(5, 'total_customers_posn')[
                                                    ['strike_price', 'call_put_flag', 'total_customers_posn']])
                            prefect_logger.info("-------------------------")

                        # Calculate and print total_customers_posn statistics
                        total_posn = missing_in_final_df['total_customers_posn'].sum()
                        max_posn = missing_in_final_df['total_customers_posn'].max()
                        prefect_logger.info(f"\nTotal total_customers_posn for all missing contracts: {total_posn}")
                        prefect_logger.info(f"Maximum total_customers_posn among missing contracts: {max_posn}")
                        prefect_logger.info("\nTop 10 missing contracts by total_customers_posn:")
                        prefect_logger.info(missing_in_final_df.nlargest(10, 'total_customers_posn')[
                                                ['option_symbol', 'strike_price', 'expiration_date', 'call_put_flag',
                                                 'total_customers_posn']])

                    else:
                        prefect_logger.info("No missing rows in final_book")

                    if missing_in_latest:
                        prefect_logger.info("\nSample of rows missing in latest_book:")
                        prefect_logger.info(final_book[
                                                final_book[['symbol', 'strike_price', 'expiration_date',
                                                            'call_put_flag']].apply(
                                                    tuple, axis=1).isin(list(missing_in_latest)[:5])])

                    # Check for NaN values
                    prefect_logger.info("\nNaN values in latest_book:")
                    prefect_logger.info(latest_book['total_customers_posn'].isna().sum())

                    prefect_logger.info("\nNaN values in final_book:")
                    prefect_logger.info(final_book['total_customers_posn'].isna().sum())

                    # Check for infinity values
                    prefect_logger.info("\nInfinity values in latest_book:")
                    prefect_logger.info(np.isinf(latest_book['total_customers_posn']).sum())

                    prefect_logger.info("\nInfinity values in final_book:")
                    prefect_logger.info(np.isinf(final_book['total_customers_posn']).sum())
                    # -------------------------------------------------#

                    # Filter out NaN values and log
                    filtered_final_book = filter_and_log_nan_values(final_book)
                    logger.debug(f"Len of filtered_final_book: {len(final_book)}")

                    # Identify columns that contain '_posn' in their names
                    posn_columns = [col for col in filtered_final_book.columns if '_posn' in col]

                    # Convert these columns to integer type
                    final_book_clean_insert = filtered_final_book.copy()
                    # Convert these columns to integer type
                    final_book_clean_insert[posn_columns] = final_book_clean_insert[posn_columns].apply(
                        lambda x: x.astype(int))

                    # Check if all values are indeed integers
                    all_integers = all(final_book_clean_insert[col].dtype == 'int64' for col in posn_columns)
                    logger.debug(f"\nAll '_posn' columns converted to integers: {all_integers}")

                    # Print total number of NaN values filled
                    total_nan_filled = sum(final_book_clean_insert[col].isna().sum() for col in posn_columns)
                    logger.info(f"\nTotal number of NaN values filled across all '_posn' columns: {total_nan_filled}")

                    # compare_dataframes(posn_only, final_book_clean_insert)

                    db_utils.insert_progress('intraday', 'intraday_books', final_book_clean_insert)
                    pg_data.insert_progress('intraday', 'intraday_books', final_book_clean_insert)

                    # Get the current price (you'll need to implement this function)
                    # current_price = get_current_price()

                    # TODO: if it's the 1800 file: export as unrevised initial book for the next effective datge

                    rabbitmq_utils.ensure_connection()
                    logger.info(f'RabbitMQ Status: {rabbitmq_utils.get_status()}')

                    for attempt in range(RABBITMQ_MAX_ACK_RETRIES):
                        try:
                            rabbitmq_utils.safe_ack(message_frame.delivery_tag, msg_body)
                            logger.debug(f"Message acknowledged for file: {file_info['file_name']}")
                            break
                        except Exception as e:
                            logger.error(
                                f"Error acknowledging message {file_info['file_name']} (attempt {attempt + 1}/{RABBITMQ_MAX_ACK_RETRIES}): {e}")
                            if attempt == RABBITMQ_MAX_ACK_RETRIES - 1:
                                logger.error(
                                    f"Failed to acknowledge message {file_info['file_name']} after all retries.")
                            time.sleep(10)  # Wait a bit before retrying

                    logger.info(f"Data flow finished in {time_module.time() - flow_start_time} sec.")

                    # ------- Send Charts -------- #
                    effective_datetime = str(final_book_clean_insert["effective_datetime"].unique()[0])

                    if current_time < datetime_time(16, 0):
                        prefect_logger.info("It's before 4 PM ET. Proceeding with heatmap generation.")
                        heatmap_generation_flow(final_book_clean_insert, effective_datetime=effective_datetime)

                        # TODO: modify the other params to remove this and start at the same time as the book generation
                        if current_time > datetime_time(7, 0):
                            intraday_gamma_heatmap(db, effective_datetime, current_date)
                            intraday_charm_heatmap(db, effective_datetime, current_date)
                            # prefect_logger.info("Triggering gif flows...")
                            # run_deployment(name="Trigger Gif Flows/Trigger Gif Flows")
                            # prefect_logger.info("Gif flows triggered.")




                    else:
                        prefect_logger.info("It's past 4 PM ET. Skipping heatmap generation.")
                        # Generate 1DTE heatmap
                        # heatmap_generation_flow(final_book_clean_insert,)
                        # generate_charts(final_book_clean_insert,effective_datetime=effective_datetime)

                    prefect_logger.info(f"Finished flow in {time_module.time() - flow_start_time} sec.")

                    # prefect_logger.info("Triggering gif flows...")
                    # run_deployment(name="Trigger Gif Flows/Trigger Gif Flows")
                    # prefect_logger.info("Gif flows triggered.")



                else:
                    logger.warning(f"DataFrame is empty or None for file: {file_info['file_name']}")
                    rabbitmq_utils.safe_nack(message_frame.delivery_tag, requeue=True)
            else:
                logger.warning("File info is None")
                rabbitmq_utils.safe_nack(message_frame.delivery_tag, requeue=True)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            rabbitmq_utils.safe_nack(message_frame.delivery_tag, requeue=True)

    except Exception as e:
        logger.error(f"Error in process_intraday_data flow: {e}")
    finally:
        sftp_utils.disconnect()  # Disconnect at the end of the flow


@flow(name="Trigger Gif Flows")
async def trigger_gif_flows():
    prefect_logger = get_run_logger()
    try:
        prefect_logger.info("Starting to trigger gif flows...")
        deployment_names = [
            "0 DTE Flow",
            "1 DTE Flow",
            "MM GEX Flow"
        ]
        await trigger_deployments(deployment_names)
        prefect_logger.info("All gif flows have been triggered.")
    except Exception as e:
        prefect_logger.error(f"Error in trigger_gif_flows: {e}")


if __name__ == "__main__":

    Intraday_Flow()
