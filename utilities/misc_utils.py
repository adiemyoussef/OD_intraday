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
from botocore.exceptions import NoCredentialsError, ClientError
import pytz
import numpy as np


class AsyncDatabaseUtilities:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool = None
        self.logger = logging.getLogger(__name__)

    async def create_pool(self):
        if self.pool is None:
            try:
                self.pool = await aiomysql.create_pool(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    db=self.database,
                    charset='utf8mb4',
                    autocommit=True,
                    cursorclass=aiomysql.DictCursor,
                    minsize=1,
                    maxsize=10
                )
                self.logger.info("Database connection pool created successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create database connection pool: {e}")
                self.pool = None

    async def execute_query(self, query: str, params: tuple = None) -> List[dict]:
        if not self.pool:
            await self.create_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                return await cursor.fetchall()

    async def execute_insert(self, query: str, params: tuple) -> bool:
        if not self.pool:
            await self.create_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.execute(query, params)
                    await conn.commit()
                    return True
                except Exception as e:
                    self.logger.error(f"Error executing insert: {e}")
                    return False

    async def fetch_dataframe(self, query: str, params: tuple = None) -> pd.DataFrame:
        results = await self.execute_query(query, params)
        return pd.DataFrame(results) if results else pd.DataFrame()

    async def get_book(self, ticker: str, active_effective_date: str) -> pd.DataFrame:
        query = f"""SELECT * FROM intraday.new_daily_book_format WHERE effective_date = %s AND ticker = %s;"""
        return await self.fetch_dataframe(query, (active_effective_date, ticker))

    async def insert_progress(self, dbName: str, dbTable: str, dataframe: pd.DataFrame):
        if not self.pool:
            await self.create_pool()

        chunksize = math.ceil(len(dataframe) / 50)
        for i, cdf in enumerate(self.chunker(dataframe, chunksize)):
            try:
                async with self.pool.acquire() as conn:
                    await conn.begin()
                    await conn.execute(f"INSERT INTO {dbName}.{dbTable} VALUES ", cdf.to_dict('records'))
                    await conn.commit()
                self.logger.info(f"Inserted chunk {i + 1} ({len(cdf)} rows)")
            except Exception as e:
                self.logger.error(f"Error inserting chunk {i}: {e}")

    @staticmethod
    def chunker(df, size):
        return (df[pos:pos + size] for pos in range(0, len(df), size))


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


# Usage example:
# s3_utils = S3Utilities(DO_SPACES_URL, DO_SPACES_KEY, DO_SPACES_SECRET, DO_SPACES_BUCKET)
# seen_files = s3_utils.load_json(LOG_FILE_KEY)
# s3_utils.save_json(LOG_FILE_KEY, list(seen_files))