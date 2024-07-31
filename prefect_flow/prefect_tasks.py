import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from aio_pika import Message, exceptions as aio_pika_exceptions
import zipfile
import time as time_module
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
from prefect_dask import DaskTaskRunner
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from config.config import *
import dill
import multiprocessing as mp
from dask import delayed, compute
import dask
import requests
from polygon import RESTClient

# Import your utility classes
from utilities.sftp_utils import *
from utilities.db_utils import *
from utilities.rabbitmq_utils import *
from utilities.misc_utils import *
from utilities.customized_logger import DailyRotatingFileHandler

# Setup
mp.set_start_method("fork", force=True)
dill.settings['recurse'] = True
client = RESTClient("sOqWsfC0sRZpjEpi7ppjWsCamGkvjpHw")

def setup_custom_logger(name, log_level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # File handler with DailyRotatingFileHandler
    file_handler = DailyRotatingFileHandler(
        base_filename=f"logs/{name}",
        when="midnight",
        interval=1,
        backupCount=7
    )
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger

DEBUG_MODE = True
LOG_LEVEL = logging.DEBUG

# Setup the main logger
logger = setup_custom_logger("Prefect Flow", logging.DEBUG if DEBUG_MODE else logging.INFO)
logger.setLevel(LOG_LEVEL)  # Or any other level like logging.INFO, logging.WARNING, etc.

#-------- Initializing the Classes -------#
polygon_client = RESTClient("sOqWsfC0sRZpjEpi7ppjWsCamGkvjpHw")

db_utils = DatabaseUtilities(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, logger=logger)
logger.debug(f"Initializing db status: {db_utils.get_status()}")

rabbitmq_utils = RabbitMQUtilities(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, logger=logger)
logger.debug(f"Initializing RabbitMQ status: {rabbitmq_utils.get_status()}")

sftp_utils = SFTPUtility(SFTP_HOST,SFTP_PORT,SFTP_USERNAME,SFTP_PASSWORD, logger = logger)
#-------------------------------------------#
def send_notification(message: str):
    prefect_logger = get_run_logger()
    # Implement your notification logic here
    prefect_logger.warning(f"Notification: {message}")

def process_greek_og(greek_name, poly_data, book):
    latest_greek = poly_data.sort_values('time_stamp', ascending=False).groupby('contract_id').first().reset_index()
    latest_greek = latest_greek[['contract_id', greek_name, 'time_stamp']]
    book = pd.merge(book, latest_greek, on='contract_id', how='left', suffixes=('', '_update'))
    print(book.columns)
    update_col = f"{greek_name}_update"
    if update_col in book.columns:
        book[greek_name] = book[update_col].combine_first(book[greek_name])
        book.drop(columns=[update_col, "time_stamp_update"], inplace=True)


    return book

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
    #breakpoint()
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
    prefect_logger = get_run_logger()
    max_attempts = 5
    attempt = 0
    while attempt < max_attempts:
        db_status = db_utils.get_status()
        rabbitmq_status = rabbitmq_utils.get_status()

        if db_status['status'] == 'Connected' and rabbitmq_status['status'] == 'Connected':
            prefect_logger.debug("Both database and RabbitMQ connections are established.")
            return True

        prefect_logger.warning(
            f"Attempt {attempt + 1}: Connections not ready. DB: {db_status}, RabbitMQ: {rabbitmq_status}")
        time.sleep(5)  # Wait for 5 seconds before retrying
        attempt += 1

    prefect_logger.error("Failed to establish connections after maximum attempts.")
    return False

#------------------ TASKS ------------------#
@task(name= "fetch_historical_poly_data", task_run_name= "Fetching historical greeks")
def fetch_historical_poly_data(previous_date, current_date, previous_datetime, current_datetime):
    start_time = time.time()
    query = f"""
    SELECT option_symbol, contract_type, strike_price, expiration_date, time_stamp,
           implied_volatility, delta, gamma, vega
    FROM landing.poly_options_data
    WHERE
        date_only BETWEEN'{previous_date}' AND '{current_date}'
        AND time_stamp BETWEEN '{previous_datetime}' AND '{current_datetime}'
    """
    #breakpoint()
    poly_data  = db_utils.execute_query(query)
    logger.info(f'Fetched {len(poly_data)} rows in {time.time() - start_time} sec.')

    return poly_data
@task(retries=3, retry_delay_seconds=60, name= "process_last_message_in_queue", task_run_name= "Processing last message in queue...")
async def process_last_message_in_queue(rabbitmq_utils:RabbitMQUtilities, expected_file_override = None):
    prefect_logger = get_run_logger()
    prefect_logger.debug("process_last_message_in_queue")
    function_start_time = time.time()
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
                            logger.debug(f"[NOT FOUND]: Expected file {expected_file_name} not found. Retrying in {PROCESS_MESSAGE_QUEUE_RETRY_DELAY} seconds...")
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

@task(name="update_book_with_latest_greeks",
      task_run_name= "Getting initial book...",
      description="Updates the book of financial options contracts with the latest Greek values.",
      retries=2,
      retry_delay_seconds=60,
      timeout_seconds=60,
      cache_key_fn=task_input_hash,
      cache_expiration=timedelta(hours=1),

      )
def get_initial_book(get_unrevised_book: Callable):
    prefect_logger = get_run_logger()
    try:
        current_time = datetime.now(ZoneInfo("America/New_York"))
        current_date = current_time.date()
        limit2am = current_time.replace(hour=2, minute=0, second=0, microsecond=0).time()
        limit7pm = current_time.replace(hour=19, minute=0, second=0, microsecond=0).time()

        #TODO: Verify that the current_date is a business date

        if limit2am <= current_time.time() < limit7pm:  # Between 2 AM and 7 PM
            logger.info(f"Operating during Revised book hours")
            query = f"""
            SELECT * FROM intraday.new_daily_book_format 
            WHERE effective_date = '{current_date}' AND revised = 'Y'
            """
            session_book = db_utils.execute_query(query)

            logger.info(f"Got Revised book of {session_book['effective_date']}")

            if session_book.empty:
                query = f"""
                SELECT * FROM intraday.new_daily_book_format 
                WHERE effective_date = '{current_date}'
                """
                session_book = db_utils.execute_query(query)
                logger.info("Getting Unrevised book")
                if session_book.empty:
                    #send_notification(f"No session_book found for {current_date}. Using unrevised session_book.")
                    return get_unrevised_book()
                else:
                    #send_notification(f"Unrevised session_book found for {current_date}. Proceeding with caution.")
                    return session_book
            else:
                prefect_logger.debug(f"Revised session_book loaded for date: {current_date}")
                # TODO: verify it makes sense
                return session_book

        else:  # Between 8 PM and 2 AM

            #TODO:
            # if it's sunday, get revised book since it runs Friday-Saturday
            # else;
            prefect_logger.debug("Using unrevised session_book due to time of day.")
            return get_unrevised_book()

    except Exception as e:
        prefect_logger.error(f"Error getting initial session_book: {e}")
        raise
# This function will be implemented later

@task
def get_unrevised_book():
    prefect_logger = get_run_logger()

    logger.info(f"Operating during Unrevised book hours")
    prefect_logger.info(f"Operating during Unrevised book hours")

    unrevised_book = pd.read_pickle("unrevised_book_20240730.pkl")
    unrevised_book.drop(columns =['effective_datetime'], inplace = True)

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
    #         breakpoint()
    #         #unrevised_book = generate_unrevised_book(current_date)
    #         #TODO: verify it makes sens
    #         #unrevised_book
    #     else:
    #         prefect_logger.debug(f"Revised session_book loaded for date: {current_date}")
    #         #TODO: verify it makes sense
    #         return unrevised_book
    #
    # except Exception as e:
    #     prefect_logger.error(f"Error getting initial session_book: {e}")
    #     raise

@task(retries=2)
def get_file_from_sftp(msg_body, override_path = None):
    logger.debug("get_file_from_sftp")
    file_path = msg_body["path"]
    if override_path:
        file_path = override_path
        logger.info(f'[OVERRIDE]: Getting {file_path}')

    else:
        file_path = msg_body["path"]
        logger.info(f'[NO OVERRIDE]: Getting {file_path}')
    try:
        sftp_utils.connect()

        file_info = sftp_utils.get_file_info(file_path)
        file_data = sftp_utils.read_file(file_path)
        logger.debug(f'[verify_and_process_message]: {file_info} {file_data}')


        return file_info, file_data, file_info['mtime']

    except Exception as e:
        logger.error(f"Error verifying and processing file {file_path}: {str(e)}")
        raise
    finally:
        sftp_utils.disconnect()

@task(retries=2)
async def read_and_verify_sftp_file(file_info, file_data):
    prefect_logger = get_run_logger()
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

        #TODO: Test with this
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

        # Check for uniqueness in key columns
        #TODO: change during live hours
        initial_key = ['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']
        intraday_key = ['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']

        #A ce stade ci, on n'a pas encore ajusté le expiration_date du book

        if initial_book[initial_key].duplicated().any():
            logger.info("Non-unique keys found in initial_book")
            #breakpoint()
            #TODO: Print the duplicates before removing them
            initial_book = initial_book.drop_duplicates(subset=initial_key, keep='last')

        if not intraday_data[intraday_key].duplicated().any():
            logger.info("Non-unique keys found in intraday_data")
            #breakpoint()
            # TODO: Print the duplicates before removing them
            intraday_data = intraday_data.drop_duplicates(subset=intraday_key, keep='last')



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

        if initial_book[initial_key].duplicated().any():
            logger.info("Non-unique keys found in initial_book")
            #TODO: Print the duplicates before removing them
            initial_book = initial_book.drop_duplicates(subset=initial_key, keep='last')

        if processed_intraday[intraday_key].duplicated().any():
            logger.info("Non-unique keys found in intraday_data")
            # TODO: Print the duplicates before removing them
            processed_intraday = processed_intraday.drop_duplicates(subset=intraday_key, keep='last')

        merged = pd.merge(initial_book, processed_intraday,
                          left_on=['ticker', 'option_symbol', 'call_put_flag', 'strike_price',
                                   'expiration_date_original'],
                          right_on=['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date'],
                          how='outer', suffixes=('', '_intraday'))

        #------------#
        #TODO: print nice recap of the merge

        #------------#


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

        logger.debug('Finished update_book_intraday')

        return merged

@task(
    name="update_book_with_latest_greeks",
    description="Updates the book of financial options contracts with the latest Greek values.",
    tags=["finance", "options", "greeks"],
    retries=3,
    retry_delay_seconds=5,
    timeout_seconds= 60,
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
    prefect_logger = get_run_logger()
    logger.debug('Entered update_book_with_latest_greeks')

    latest_poly = get_latest_poly(client)

    # Create a key for faster merge
    book['strike_price'] = book['strike_price'].astype(int)
    book['contract_id'] = book['option_symbol'] + '_' + \
                          book['call_put_flag'] + '_' + \
                          book['strike_price'].astype(str) + '_' + \
                          pd.to_datetime(book['expiration_date_original']).dt.strftime('%Y-%m-%d')

    latest_poly.rename(columns={'implied_volatility': 'iv'}, inplace=True)
    latest_poly['contract_id'] = latest_poly['option_symbol'] + '_' + \
                                          latest_poly['contract_type'] + '_' + \
                                          latest_poly['strike_price'].astype(str) + '_' + \
                                          latest_poly['expiration_date']

    # Merge latest book with the latest poly data
    merged_book = pd.merge(book, latest_poly, on='contract_id', how='left', suffixes=('', '_update'))

    # Log merge results
    total_contracts = len(book)
    merged_contracts = merged_book['iv_update'].notna().sum()
    unmerged_contracts = total_contracts - merged_contracts

    if unmerged_contracts > 0:
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
                                    latest_historical[['contract_id', 'iv', 'delta', 'gamma', 'vega']],
                                    on='contract_id', how='left', suffixes=('', '_historical'))

        # Update the merged_book with historical data
        merged_book.update(updated_unmerged)

        historical_updates = updated_unmerged['iv_historical'].notna().sum()
    else:
        historical_updates = 0


    # Clean up the merged book
    for greek in ['iv', 'delta', 'gamma', 'vega']:
        update_column = f'{greek}_update'
        historical_column = f'{greek}_historical'

        if historical_column in merged_book.columns:
            merged_book[greek] = merged_book[update_column].combine_first(
                merged_book[historical_column]).combine_first(merged_book[greek])
        else:
            merged_book[greek] = merged_book[update_column].combine_first(merged_book[greek])

        # Drop temporary columns
        merged_book.drop(columns=[update_column, historical_column], errors='ignore', inplace=True)


    # Calculate final statistics
    contracts_with_greeks = merged_book['iv'].notna().sum()
    contracts_without_greeks = total_contracts - contracts_with_greeks
    contracts_not_updated = total_contracts - merged_contracts - historical_updates

    # Get mm_posn for contracts without greeks
    contracts_without_greeks_df = merged_book[merged_book['iv'].isna()]
    mm_posn_sum = contracts_without_greeks_df['mm_posn'].sum()

    # Prepare log message
    log_message = f"""
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
    logger.info(log_message)
    prefect_logger.info(log_message)

    # Filter and keep the rows without NaN values in greeks
    mask = ~merged_book[['iv', 'delta', 'gamma', 'vega']].isna().any(axis=1)
    merged_book = merged_book[mask]


    columns_to_keep = book.columns[:-1]
    merged_book = merged_book[columns_to_keep]
    merged_book.loc[:,'time_stamp'] = get_eastern_time()



    return merged_book



#----------------- FLOWS ------------------#
@flow(name="Intraday Flow")
def Intraday_Flow():

    prefect_logger = get_run_logger()
    flow_start_time = time.time()

    expected_file_override = None #'/subscriptions/order_000059435/item_000068201/Cboe_OpenClose_2024-07-30_18_00_1.csv.zip'

    db_utils.connect()
    # rabbitmq_utils.connect()

    try:
        #Fast
        initial_book = get_initial_book(get_unrevised_book)

        book_date_loaded = initial_book["effective_date"].unique()
        logger.info(f"Initial Book of {book_date_loaded} loaded")

        previous_date, current_date, previous_datetime, current_datetime = prepare_to_fetch_historical_poly(
            shift_previous_minutes=60 * 8, shift_current_minutes=0)

        # Fetch poly data: Slow
        poly_data = fetch_historical_poly_data(previous_date, current_date, previous_datetime, current_datetime)

        rabbitmq_utils.connect()

        # Ensure connections are established
        connections_ready = ensure_all_connections_are_open()
        if not connections_ready:
            prefect_logger.error("Unable to establish necessary connections. Aborting flow.")
            return

        prefect_logger.debug(f"[process_intraday_data] db status: {db_utils.get_status()}")
        prefect_logger.debug(f"[process_intraday_data] rabbitmq status: {rabbitmq_utils.get_status()}")

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
            file_info, file_data, file_last_modified = get_file_from_sftp(msg_body, expected_file_override)
            if file_info:
                logger.debug(f"FILE INFO ---> {file_info}")
                df = read_and_verify_sftp_file(file_info, file_data)
                if df is not None and not df.empty:
                    logger.debug(f"Successfully processed file: {file_info['file_name']}")
                    logger.debug(f"DataFrame shape: {df.shape}")
                    latest_book = build_latest_book(initial_book, df)


                    final_book = update_book_with_latest_greeks(latest_book, poly_data)
                    logger.debug(f"Len of final_book: {len(final_book)}")

                    # Filter out NaN values and log
                    filtered_final_book = filter_and_log_nan_values(final_book)
                    logger.debug(f"Len of filtered_final_book: {len(final_book)}")


                    #TODO: Filter out the rows where there's Nan Values and log the unique contract that have been filtered out
                    #      with also the mm_position of these contract to see which one could impact
                    #db_utils.insert_progress('intraday', 'intraday_books',filtered_final_book)

                    # TODO: if it's the 1800 file: export as unrevised initial book for the next effective datge

                    rabbitmq_utils.ensure_connection()
                    logger.info(f'RabbitMQ Status: {rabbitmq_utils.get_status()}')


                    for attempt in range(RABBITMQ_MAX_ACK_RETRIES):
                        try:
                            rabbitmq_utils.safe_ack(message_frame.delivery_tag,msg_body)
                            logger.debug(f"Message acknowledged for file: {file_info['file_name']}")
                            break
                        except Exception as e:
                            logger.error(f"Error acknowledging message {file_info['file_name']} (attempt {attempt + 1}/{RABBITMQ_MAX_ACK_RETRIES}): {e}")
                            if attempt == RABBITMQ_MAX_ACK_RETRIES - 1:
                                logger.error(f"Failed to acknowledge message {file_info['file_name']} after all retries.")
                            time.sleep(10)  # Wait a bit before retrying
                    logger.info(f"Finished flow in {time.time()-flow_start_time} sec.")


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


if __name__ == "__main__":
    """
    Synchronous Flow
    """
    Intraday_Flow()

