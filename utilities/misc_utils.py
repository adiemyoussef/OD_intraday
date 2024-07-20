import asyncio
import math
from typing import List
import aiomysql
import pandas as pd
from datetime import datetime
import logging
import os
import re
import json
import boto3
import requests
from botocore.exceptions import NoCredentialsError, ClientError
import pytz
import numpy as np


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

def get_previous_business_day(date, calendar):
    if isinstance(date, pd.Timestamp):
        date = date.to_pydatetime()
    prev_days = calendar.valid_days(end_date=date, start_date=date - pd.Timedelta(days=5))
    return prev_days[-2].to_pydatetime()

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

def send_discord_notification(message,webhook_hurl):
    data = {"content": message}
    response = requests.post(webhook_hurl, json=data)
    if response.status_code != 204:
        raise ValueError(f"Request to Discord returned an error {response.status_code}, the response is:\n{response.text}")


# Usage example:
# s3_utils = S3Utilities(DO_SPACES_URL, DO_SPACES_KEY, DO_SPACES_SECRET, DO_SPACES_BUCKET)
# seen_files = s3_utils.load_json(LOG_FILE_KEY)
# s3_utils.save_json(LOG_FILE_KEY, list(seen_files))