import asyncio
import math
from typing import List
import aiomysql
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
import os
import re
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pytz
import numpy as np
import requests
import logging
import datetime
import pytz
import time
import logging
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
from datetime import datetime, timedelta
import logging
import decimal
nyse = mcal.get_calendar('NYSE')
logger = logging.getLogger(__name__)

#------ OptionsDepth Modules ---------#
from config.config import *

# Additional utility functions for SFTP monitoring

def detect_dates(folder_path: str) -> List[str]:
    date_pattern_cboe = re.compile(r'Close_(\d{4}-\d{2}-\d{2})')
    date_pattern_optionchain = re.compile(r'intraday_optionchain_ (\d{4}-\d{2}-\d{2})')

    distinct_dates = set()
    date_pattern = date_pattern_optionchain if 'options_chain' in folder_path else date_pattern_cboe

    for filename in os.listdir(folder_path):
        match = date_pattern.search(filename)
        if match:
            distinct_dates.add(match.group(1))

    return sorted(list(distinct_dates), key=lambda x: datetime.strptime(x, '%Y-%m-%d'))

def get_sorted_files(path: str, date_str: str) -> List[str]:
    date_time_pattern = re.compile(rf'Cboe_OpenClose_{date_str}_(\d{{2}})_(\d{{2}})\.csv')

    filtered_files = [
        file for file in os.listdir(path)
        if (match := date_time_pattern.search(file)) and int(match.group(1)) < 16
    ]

    return sorted(filtered_files)

def get_eastern_time() -> str:
    eastern = pytz.timezone('America/New_York')
    return datetime.now().astimezone(eastern).strftime("%Y-%m-%d %H:%M:%S")

def list_price(latest_active_spx_price: float, steps: float, range: float) -> np.ndarray:
    lower_bound = round((1 - range) * latest_active_spx_price)
    upper_bound = round((1 + range) * latest_active_spx_price)

    lower_end = np.arange(latest_active_spx_price, lower_bound, -steps)
    upper_end = np.arange(latest_active_spx_price, upper_bound, steps)
    return np.unique(np.sort(np.concatenate((lower_end, upper_end))))

def get_expiration_datetime(row):
    if pd.isna(row['expiration_date_original']):
        return None
    base_date = pd.to_datetime(row['expiration_date_original']).strftime('%Y-%m-%d')
    if row['option_symbol'] in ['SPX', 'VIX']:
        return f"{base_date} 09:15:00"
    elif row['option_symbol'] in ['SPXW', 'VIXW']:
        return f"{base_date} 16:00:00"
    else:
        return None

def get_previous_business_day(date, calendar):
    if isinstance(date, pd.Timestamp):
        date = date.to_pydatetime()
    prev_days = calendar.valid_days(end_date=date, start_date=date - pd.Timedelta(days=5))
    return prev_days[-2].to_pydatetime()

def get_trading_day(reference_date, n=1, direction='previous', return_format='datetime'):
    """
    Find the nth trading day before or after a reference date.

    Parameters:
    reference_date (str or datetime): The reference date
    n (int): Number of trading days to move (default 1)
    direction (str): 'previous' or 'next' (default 'previous')
    return_format (str): 'string', 'datetime', or 'tz' (default 'datetime')

    Returns:
    The calculated trading day in the specified format
    """

    # Ensure reference_date is a timezone-aware pandas Timestamp in UTC
    reference_date = pd.Timestamp(reference_date).tz_localize('UTC')

    # Get NYSE calendar
    nyse = mcal.get_calendar('NYSE')

    # Determine the date range based on direction
    if direction == 'previous':
        start_date = reference_date - pd.Timedelta(days=n * 5)  # Increased range
        end_date = reference_date
    elif direction == 'next':
        start_date = reference_date
        end_date = reference_date + pd.Timedelta(days=n * 5)  # Increased range
    else:
        raise ValueError("Direction must be 'previous' or 'next'")

    # Get valid trading days
    trading_days = nyse.valid_days(start_date=start_date, end_date=end_date)

    # Find the desired trading day
    if direction == 'previous':
        mask = trading_days < reference_date
        result = trading_days[mask]
        if len(result) >= n:
            result = result[-n]
        else:
            raise ValueError(f"Not enough previous trading days within the date range. Found {len(result)}, requested {n}.")
    else:  # next
        mask = trading_days > reference_date
        result = trading_days[mask]
        if len(result) >= n:
            result = result[n - 1]
        else:
            raise ValueError(f"Not enough next trading days within the date range. Found {len(result)}, requested {n}.")

    # Format the result based on the return_format parameter
    if return_format == 'string' or return_format == 'str':
        return result.strftime('%Y-%m-%d')
    elif return_format == 'datetime':
        return result.tz_convert(None)  # Return timezone-naive Timestamp
    elif return_format == 'tz':
        return result  # Return timezone-aware Timestamp
    else:
        raise ValueError("Invalid return_format. Must be 'string', 'datetime', or 'tz'.")



def get_strike_range(db,session_date, range_value=200, range_type:str='points'):
    """
    Find the previous session's close price and return a range around it.

    Parameters:
    db (connector)    : The connector from which to exectue the query
    session_date (str): The current session date in 'YYYY-MM-DD' format.
    range_points (int): The range to add/subtract from the close price. Default is 200.

    Returns:
    list: [lower_bound, upper_bound] of the strike range.
    """
    # Convert session_date to datetime
    # session_date = pd.to_datetime(session_date)
    previous_trading_day = previous_trading_day = get_trading_day(session_date, n=1, direction='previous', return_format='str')
    # Query to find the previous session's close price
    query = f"""
    SELECT effective_date, close
    FROM optionsdepth_stage.charts_candlestick
    WHERE ticker = 'SPX'
    AND effective_date = '{previous_trading_day}'
    ORDER BY effective_datetime DESC
    LIMIT 1
    """

    # Execute the query
    result = db.execute_query(query)

    if result.empty:
        raise ValueError(f"No previous session data found for date: {session_date}")

    previous_close = float(result['close'].iloc[0])

    # Calculate the range based on range_type
    if range_type == 'points':
        lower_bound = previous_close - range_value
        upper_bound = previous_close + range_value
    elif range_type == 'percent':

        lower_bound = previous_close * (1 - range_value)
        upper_bound = previous_close * (1 + range_value)
    else:
        raise ValueError("Invalid range_type. Use 'points' or 'percent'.")


    # Round to nearest 10
    lower_bound = int(round(lower_bound, -1))
    upper_bound = int(round(upper_bound, -1))

    return [lower_bound, upper_bound]


def verify_intraday_data(df):
    missing_columns = set(INTRADAY_REQUIRED_COLUMNS) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required column(s): {missing_columns}")

    expected_types = {
        'trade_datetime': 'object',
        'ticker': 'object',
        'security_type': 'int64',
        'option_symbol': 'object',
        'expiration_date': 'object',
        'strike_price': 'float64',
        'call_put_flag': 'object',
        'days_to_expire': 'int64',
        'series_type': 'object',
        'previous_close': 'float64'
    }

    for col, expected_type in expected_types.items():
        if df[col].dtype != expected_type:
            raise ValueError(f"Column {col} has incorrect data type. Expected {expected_type}, got {df[col].dtype}")

    if not df['security_type'].isin(VALID_SECURITY_TYPES).all():
        raise ValueError("Invalid security_type values found")

    if not df['call_put_flag'].isin(VALID_CALL_PUT_FLAGS).all():
        raise ValueError("Invalid call_put_flag values found")

    if not df['series_type'].isin(VALID_SERIES_TYPES).all():
        raise ValueError("Invalid series_type values found")

    volume_qty_columns = [col for col in df.columns if
                          (col.endswith('_qty') or col.endswith('_vol')) and not col.startswith('total_')]
    for col in volume_qty_columns:
        if not ((df[col] >= 0) & df[col].apply(
                lambda x: isinstance(x, (int, np.integer)) or (isinstance(x, float) and x.is_integer()))).all():
            raise ValueError(f"Column {col} contains negative or non-integer values")

    if not (df['strike_price'] > 0).all():
        raise ValueError("strike_price should be positive")

    if not (df['days_to_expire'] >= 0).all():
        raise ValueError("days_to_expire should be non-negative")

    return df


def get_trading_window_start(effective_datetime, lookback_hours=24):

    breakpoint()

    debug_datetime = '2024-07-21 20:20:00'

    effective_datetime = pd.to_datetime(debug_datetime, utc=True)

    # Ensure effective_datetime is timezone-aware
    if effective_datetime.tzinfo is None:
        effective_datetime = effective_datetime.tz_localize('UTC')

    # Calculate the initial window start
    window_start = effective_datetime - timedelta(hours=lookback_hours)

    # Get valid trading days, including the effective_datetime
    trading_days = nyse.valid_days(start_date=window_start.date(), end_date=effective_datetime.date())
    # trading_days = pd.to_datetime(trading_days).tz_localize('UTC')

    # If window_start is not on a trading day, adjust to the last valid trading day
    if window_start.floor('D') not in trading_days:
        # Find the last trading day before or on the window_start date
        last_trading_day = trading_days[trading_days <= window_start.floor('D')]
        if not last_trading_day.empty:
            last_trading_day = last_trading_day[-1]
            # Set window_start to the end of the last trading day
            window_start = last_trading_day.replace(hour=23, minute=59, second=59)
        else:
            # If no valid trading day found, use the original window_start
            window_start = window_start

    # Ensure we don't go beyond the lookback period
    window_start = max(window_start, effective_datetime - timedelta(hours=lookback_hours))

    return window_start


def determine_expected_file_name(current_time=None):
    """
    Determine the expected file name based on the current time.

    :param current_time: Optional. The current time to use for calculations.
                         If None, uses the actual current time.
    :return: The expected file name as a string.
    """
    if current_time is None:
        current_time = datetime.now()

    nyse = mcal.get_calendar('NYSE')

    is_next_day_period = (current_time.hour > 20) or (current_time.hour == 20 and current_time.minute >= 20)

    if is_next_day_period:
        logger.info("We are in the next day period")
        next_day = current_time.date() + timedelta(days=1)
        next_5_days = next_day + timedelta(days=5)
        next_business_days = nyse.valid_days(start_date=next_day, end_date=next_5_days)
        file_date = next_business_days[0].date()
    else:
        logger.info("We are NOT in the next day period")
        file_date = current_time.date()

    # Adjust the time for the 5-6 PM and 6-7 PM periods
    if 17 <= current_time.hour < 18:
        expected_file_time = current_time.replace(hour=17, minute=0, second=0, microsecond=0)
    elif 18 <= current_time.hour < 19:
        expected_file_time = current_time.replace(hour=18, minute=0, second=0, microsecond=0)
    else:
        expected_file_time = current_time.replace(minute=(current_time.minute // 10) * 10, second=0, microsecond=0)

    expected_file_name = f"Cboe_OpenClose_{file_date.strftime('%Y-%m-%d')}_{expected_file_time.strftime('%H_%M')}_1.csv.zip"
    # Holiday
    #expected_file_name = f"Cboe_OpenClose_{file_date.strftime('%Y-%m-%d')}_{expected_file_time.strftime('%H_%M')}_2.csv.zip"

    logger.info(f"Expected file name: {expected_file_name}")
    return expected_file_name

async def get_unrevised_book(session_date, db_utils):

    try:
        previous_business_date = nyse.previous_open(session_date).date()
        logger.info(f"Getting unrevised book for previous business date: {previous_business_date}")

        # Get the revised book for the previous business date
        query = f"SELECT * FROM intraday.new_daily_book_format WHERE effective_date = '{previous_business_date}' AND revised = 'Y'"
        previous_revised_book = await db_utils.execute_query(query)

        if previous_revised_book.empty:
            logger.error(f"No revised book found for previous business date: {previous_business_date}")
            raise ValueError(f"No revised book available for {previous_business_date}")

        # Get the 19:00 session file for the previous day
        previous_day_file_query = f"""
        SELECT * FROM intraday.intraday_books 
        WHERE effective_date = '{previous_business_date}' 
        AND trade_datetime = (
            SELECT MAX(trade_datetime) 
            FROM intraday.intraday_books 
            WHERE effective_date = '{previous_business_date}'
        )
        """
        previous_day_file = await db_utils.execute_query(previous_day_file_query)

        if previous_day_file.empty:
            logger.error(f"No 19:00 session file found for previous business date: {previous_business_date}")
            raise ValueError(f"No 19:00 session file available for {previous_business_date}")

        # Combine the previous revised book with the 19:00 session file
        combined_book = pd.concat([previous_revised_book, previous_day_file]).drop_duplicates(subset=['option_symbol'],
                                                                                              keep='last')

        logger.info(
            f"Unrevised book created by combining revised book from {previous_business_date} and its 19:00 session file")
        return combined_book

    except Exception as e:
        logger.error(f"Error getting unrevised book: {e}")
        raise


async def send_webhook_to_discord(file_info, flow_start_time):
    logger = logging.getLogger(__name__)
    try:
        # Send webhook to Discord bot
        webhook_url = "http://localhost:8081/webhook"
        payload = {
            "event": "prefect_flow_completed",
            "command": "depthview",
            "data": {
                "message": f"Processing completed for file: {file_info['file_name']}",
                "total_time": f"{time.time() - flow_start_time:.2f} seconds",
            }
        }
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        logger.info("Webhook sent to Discord bot")
    except requests.RequestException as e:
        logger.error(f"Failed to send webhook to Discord bot: {e}")


def verify_data(df):
    missing_columns = set(INTRADAY_REQUIRED_COLUMNS) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required column(s): {missing_columns}")

    expected_types = {
        'trade_datetime': 'object',
        'ticker': 'object',
        'security_type': 'int64',
        'option_symbol': 'object',
        'expiration_date': 'object',
        'strike_price': 'float64',
        'call_put_flag': 'object',
        'days_to_expire': 'int64',
        'series_type': 'object',
        'previous_close': 'float64'
    }

    for col, expected_type in expected_types.items():
        if df[col].dtype != expected_type:
            raise ValueError(f"Column {col} has incorrect data type. Expected {expected_type}, got {df[col].dtype}")

    if not df['security_type'].isin(VALID_SECURITY_TYPES).all():
        raise ValueError("Invalid security_type values found")

    if not df['call_put_flag'].isin(VALID_CALL_PUT_FLAGS).all():
        raise ValueError("Invalid call_put_flag values found")

    if not df['series_type'].isin(VALID_SERIES_TYPES).all():
        raise ValueError("Invalid series_type values found")

    volume_qty_columns = [col for col in df.columns if
                          (col.endswith('_qty') or col.endswith('_vol')) and not col.startswith('total_')]
    for col in volume_qty_columns:
        if not ((df[col] >= 0) & df[col].apply(
                lambda x: isinstance(x, (int, np.integer)) or (isinstance(x, float) and x.is_integer()))).all():
            raise ValueError(f"Column {col} contains negative or non-integer values")

    if not (df['strike_price'] > 0).all():
        raise ValueError("strike_price should be positive")

    if not (df['days_to_expire'] >= 0).all():
        raise ValueError("days_to_expire should be non-negative")

    return df

def parse_message(message):
    if "heartbeat" in message.lower():
        logger.debug("Heartbeat found in the message")
        return None
    parts = message.split(', ')
    file_name = parts[0].split(': ')[1]
    file_path = parts[1].split(': ')[1]
    timestamp = parts[2].split(': ')[1]
    return {
        'file_name': file_name,
        'file_path': file_path,
        'timestamp': timestamp
    }

def get_latest_poly(client):
    """
    Fetches the latest options data from Polygon and returns it as a DataFrame.

    :param client: The Polygon client object used to fetch data.
    :return: A DataFrame containing the latest options data.
    """

    logger.info("Getting latest Poly Data...")

    options_chain = []

    # Start the timer
    start_time = time.time()

    for o in client.list_snapshot_options_chain("I:SPX", params={"limit": 250}):
        options_chain.append(o)

    end_time = time.time()
    time_taken = end_time - start_time
    logging.info(f"Time taken to fetch options chain: {time_taken} seconds")

    df_options_chain = pd.DataFrame(options_chain)
    df_options_chain.sort_values('open_interest', ascending=False, inplace=True)
    df_cleaned = df_options_chain.dropna(subset=['implied_volatility']).copy()

    df_cleaned['id'] = df_cleaned.index

    df_expanded = pd.json_normalize(df_cleaned['details'])
    greeks_df = df_cleaned['greeks'].apply(pd.Series)

    df_expanded['id'] = df_cleaned['id'].values
    greeks_df['id'] = df_cleaned['id'].values

    df_expanded = pd.merge(df_expanded, df_cleaned[['id', 'implied_volatility', 'open_interest']], on='id')
    df_expanded = pd.merge(df_expanded, greeks_df, on='id')

    df_expanded['option_symbol'] = df_expanded['ticker'].str.extract(r':([A-Za-z]+)\d')

    df = df_expanded[['option_symbol', 'contract_type', 'expiration_date', 'strike_price',
                      'implied_volatility', 'open_interest', 'delta', 'gamma', 'vega']] #theta

    df_final = df.copy()
    df_final['contract_type'] = df_final['contract_type'].replace({'call': 'C', 'put': 'P'})

    # Adjustment to be removed once Real-time data is available

    timestamp = get_eastern_time()
    logging.info(f'Timestamp: {timestamp}')
    df_final.insert(0, 'time_stamp', timestamp)
    df_final["time_stamp"] = pd.to_datetime(df_final['time_stamp'])

    df_final.sort_values(['expiration_date', 'open_interest'], ascending=[True, False], inplace=True)

    return df_final

def analyze_duplicates(df, df_name, key_columns, logger):
    total_rows = len(df)
    duplicates = df[df.duplicated(subset=key_columns, keep=False)]
    duplicate_count = len(duplicates)

    analysis = f"""
    #-------- DUPLICATE ANALYSIS: {df_name} -------#
    Total rows: {total_rows}
    Duplicate rows: {duplicate_count}
    Percentage of duplicates: {(duplicate_count / total_rows) * 100:.2f}%

    Columns: {', '.join(df.columns)}

    Key columns used for duplicate check: {', '.join(key_columns)}

    Data types of key columns:
    {df[key_columns].dtypes.to_string()}

    Sample of duplicate rows (if any):
    {duplicates.head().to_string() if not duplicates.empty else "No duplicates found"}
    #------------------------------------------#
    """

    logger.debug(analysis)
    return duplicates


def send_notification(message: str, logger):
    # Implement your notification logic here
    logger.warning(f"Notification: {message}")


def fetch_gamma_data(db, effective_date, effective_datetime):
    query = f"""
    WITH ranked_gamma AS (
        SELECT 
            id,
            ticker,
            effective_date,
            effective_datetime,
            price,
            value,
            sim_datetime,
            NULL as minima,
            NULL as maxima,
            ROW_NUMBER() OVER (PARTITION BY sim_datetime,price ORDER BY effective_datetime DESC) AS rn
        FROM intraday.intraday_gamma
        WHERE effective_datetime <= '{effective_datetime}' -- (SELECT max(effective_datetime) FROM intraday.intraday_gamma)
        and effective_date = '{effective_date}'
        and time(effective_datetime) >= '{HEATMAPS_START_TIME}'
    ),
    consumed_gamma AS (
        SELECT 
            id,
            ticker,
            effective_date,
            effective_datetime,
            price,
            value,
            sim_datetime,
            minima,
            maxima
        FROM ranked_gamma
        WHERE rn = 1
    ),
    upcoming_gamma AS (
        SELECT * 
        FROM intraday.intraday_gamma
        WHERE effective_datetime = '{effective_datetime}' -- (SELECT max(effective_datetime) FROM intraday.intraday_gamma)
        and
        effective_date = '{effective_date}'
    ),
    final_gamma as(
    SELECT * FROM consumed_gamma
    UNION ALL
    SELECT * FROM upcoming_gamma
    )
    SELECT * from final_gamma
    -- group by effective_datetime;

    -- SELECT * from final_gamma
    """

    return db.execute_query(query)

def fetch_charm_data(db, effective_date, effective_datetime):
    query = f"""
    WITH ranked_charm AS (
        SELECT 
            id,
            ticker,
            effective_date,
            effective_datetime,
            price,
            value,
            sim_datetime,
            NULL as minima,
            NULL as maxima,
            ROW_NUMBER() OVER (PARTITION BY sim_datetime,price ORDER BY effective_datetime DESC) AS rn
        FROM intraday.intraday_charm
        WHERE effective_datetime <= '{effective_datetime}' -- (SELECT max(effective_datetime) FROM intraday.intraday_charm)
        and effective_date = '{effective_date}'
        and time(effective_datetime) >= '{HEATMAPS_START_TIME}'
    ),
    consumed_charm AS (
        SELECT 
            id,
            ticker,
            effective_date,
            effective_datetime,
            price,
            value,
            sim_datetime,
            NULL as minima,
            NULL as maxima
        FROM ranked_charm
        WHERE rn = 1
    ),
    upcoming_charm AS (
        SELECT * 
        FROM intraday.intraday_charm
        WHERE effective_datetime = '{effective_datetime}' -- (SELECT max(effective_datetime) FROM intraday.intraday_charm)
        and
        effective_date = '{effective_date}'
    ),
    final_charm as(
    SELECT * FROM consumed_charm
    UNION ALL
    SELECT * FROM upcoming_charm
    )
    SELECT * from final_charm
    """

    return db.execute_query(query)


def resample_and_convert_timezone(df:pd.DataFrame, datetime_column='effective_datetime', resample_interval='5T',
                                  source_timezone='US/Eastern',target_timezone='US/Eastern'):
    """
    Resample a dataframe with 1-minute OHLCV data to a specified interval and convert timezone.

    Parameters:
    df (pandas.DataFrame): Input dataframe with OHLCV data
    datetime_column (str): Name of the datetime column (default: 'effective_datetime')
    resample_interval (str): Pandas resample rule (default: '5T' for 5 minutes)
    timezone (str): Timezone to convert to (default: 'US/Eastern')

    Returns:
    pandas.DataFrame: Resampled dataframe with converted timezone
    """

    # Ensure the datetime column is in the correct format
    df[datetime_column] = pd.to_datetime(df[datetime_column])

    # Set the datetime column as index
    df = df.set_index(datetime_column)

    # Check if the index is timezone-aware
    if df.index.tzinfo is None:
        # If timezone-naive, assume it's UTC and localize
        df.index = df.index.tz_localize(source_timezone)
        #df.index = df.index.tz_localize('UTC')

    # Convert to target timezone
    target_tz = pytz.timezone(target_timezone)
    df.index = df.index.tz_convert(target_tz)

    # Resample to the specified interval
    df_resampled = df.resample(resample_interval).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
    })

    # Reset index to make datetime a column again
    df_resampled.reset_index(inplace=True)

    # Remove timezone information after conversion if needed
    df_resampled[datetime_column] = df_resampled[datetime_column].dt.tz_localize(None)

    return df_resampled

def process_gamma_data(df):
    df['effective_datetime'] = pd.to_datetime(df['effective_datetime'])
    df['sim_datetime'] = pd.to_datetime(df['sim_datetime'])
    df['price'] = df['price'].astype(float)
    df['value'] = df['value'].astype(float)
    df['minima'] = df['minima'].astype(float)
    df['maxima'] = df['maxima'].astype(float)
    return df

def process_charm_data(df):
    df['effective_datetime'] = pd.to_datetime(df['effective_datetime'])
    df['sim_datetime'] = pd.to_datetime(df['sim_datetime'])
    df['price'] = df['price'].astype(float)
    df['value'] = df['value'].astype(float)

    return df

def et_to_utc(et_time_str):
    # Parse the input string to a datetime object
    et_time = datetime.strptime(et_time_str, '%Y-%m-%d %H:%M:%S')

    # Set the timezone to Eastern Time
    eastern = pytz.timezone('US/Eastern')
    et_time = eastern.localize(et_time)

    # Convert to UTC
    utc_time = et_time.astimezone(pytz.UTC)

    # Format the UTC time as a string
    return utc_time.strftime('%Y-%m-%d %H:%M:%S')

def round_to_nearest_tens(number):
    """
    Rounds a number to the nearest lower and upper multiples of 10.

    Args:
    number (int or float): The number to round.

    Returns:
    tuple: The nearest lower and upper multiples of 10.
    """
    lower = number - (number % 10)  # Find the nearest lower multiple of 10
    upper = lower + 10  # Find the nearest upper multiple of 10
    return (lower, upper)


def sort_file_paths(file_paths):
    def get_priority(path):
        basename = os.path.basename(path)
        is_image = basename.endswith('.png')

        if is_image and basename.startswith('last_frame_Net'):
            return (0, 0)  # Highest priority for Net images
        elif is_image and basename.startswith('last_frame_C'):
            return (0, 1)  # Second priority for Call images
        elif is_image and basename.startswith('last_frame_P'):
            return (0, 2)  # Third priority for Put images
        elif basename.startswith('Net'):
            return (1, 0)  # Fourth priority for Net videos
        elif basename.startswith('C'):
            return (1, 1)  # Fifth priority for Call videos
        elif basename.startswith('P'):
            return (1, 2)  # Sixth priority for Put videos
        else:
            return (2, 0)  # Lowest priority for any other files

    return sorted(file_paths, key=get_priority)

def safe_add(a, b):
    if isinstance(a, decimal.Decimal):
        return a + decimal.Decimal(str(b))
    elif isinstance(b, decimal.Decimal):
        return decimal.Decimal(str(a)) + b
    else:
        return a + b