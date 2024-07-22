import asyncio
import math
from typing import List
import aiomysql
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import re
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pytz
import numpy as np

import pandas_market_calendars as mcal
from datetime import datetime, timedelta
import logging

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

    expected_file_time = current_time.replace(minute=(current_time.minute // 10) * 10, second=0, microsecond=0)
    expected_file_name = f"Cboe_OpenClose_{file_date.strftime('%Y-%m-%d')}_{expected_file_time.strftime('%H_%M')}_1.csv.zip"

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

# Usage example:
# s3_utils = S3Utilities(DO_SPACES_URL, DO_SPACES_KEY, DO_SPACES_SECRET, DO_SPACES_BUCKET)
# seen_files = s3_utils.load_json(LOG_FILE_KEY)
# s3_utils.save_json(LOG_FILE_KEY, list(seen_files))