import asyncio

from aio_pika import Message, exceptions as aio_pika_exceptions
import zipfile
from datetime import datetime, timedelta
import time
import pandas_market_calendars as mcal
from prefect import task, flow, get_run_logger
from config.config import *
import dill
import multiprocessing as mp
from dask import delayed, compute
import dask
import requests

# Import your utility classes
from utilities.sftp_utils import *
from utilities.db_utils import *
from utilities.rabbitmq_utils import *
from utilities.misc_utils import *
from utilities.customized_logger import DailyRotatingFileHandler
# Setup
mp.set_start_method("fork", force=True)
dill.settings['recurse'] = True

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
# Setup the main logger
logger = setup_custom_logger("Prefect Flow", logging.DEBUG if DEBUG_MODE else logging.INFO)
logger.setLevel(logging.INFO)  # Or any other level like logging.INFO, logging.WARNING, etc.


# Initialize utility instances
#db_utils = AsyncDatabaseUtilities(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, logger=logger)
#rabbitmq_utils = AsyncRabbitMQUtilities(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, logger=logger)
sftp_utils = SFTPUtility(SFTP_HOST,SFTP_PORT,SFTP_USERNAME,SFTP_PASSWORD, logger = logger)

db_utils = DatabaseUtilities(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, logger=logger)

logger.info(f"Initializing db status: {db_utils.get_status()}")
def terminate_pool(pool):
    pool.terminate()
    pool.join()
    logger.debug("Multiprocessing pool terminated gracefully.")

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

def process_wrapper(args):
    return process_greek(*args)

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


def process_greek(greek_name, poly_data, book):
    latest_greek = poly_data.sort_values('time_stamp', ascending=False).groupby('contract_id').first().reset_index()
    latest_greek = latest_greek[['contract_id', greek_name, 'time_stamp']]
    book = pd.merge(book, latest_greek, on='contract_id', how='left', suffixes=('', '_update'))
    update_col = f"{greek_name}_update"
    if update_col in book.columns:
        book[greek_name] = book[update_col].combine_first(book[greek_name])
        book.drop(columns=[update_col, "time_stamp_update"], inplace=True)
    return book


@task(retries=3, retry_delay_seconds=60)
async def process_last_message(rabbitmq_utils:AsyncRabbitMQUtilities):
    logger = get_run_logger()
    function_start_time = time.time()
    try:
        await rabbitmq_utils.connect()
        expected_file_name = determine_expected_file_name()

        logger.info(f"Expected file name: {expected_file_name}")

        RETRY_DELAY = 5  # in seconds
        MAX_RUNTIME = 3600  # 1 hour in seconds

        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < MAX_RUNTIME:
            try:
                messages = await rabbitmq_utils.fetch_all_messages_in_queue(RABBITMQ_CBOE_QUEUE)
                message_count = len(messages)
                logger.info(f"Messages in queue: {message_count}")

                if messages:
                    for message in reversed(messages):  # Process from newest to oldest
                        msg_decoded = message.body.decode('utf-8')

                        msg_body = json.loads(msg_decoded)
                        file_name = msg_body.get("filename")

                        if file_name == expected_file_name:
                            logger.info(f"[FOUND]: Expected file {expected_file_name}")
                            return message, msg_body

                    logger.info(f"[NOT FOUND]: Expected file {expected_file_name} not found. Retrying in {RETRY_DELAY} seconds...")
                else:
                    logger.info(f"Queue is empty. Retrying in {RETRY_DELAY} seconds...")

                await asyncio.sleep(RETRY_DELAY)

            except Exception as e:
                logger.error(f"Error occurred while checking messages: {e}")
                await asyncio.sleep(RETRY_DELAY)

        logger.info(f"Maximum runtime reached. Expected file {expected_file_name} not found.")
        #send_discord_message()
        return None, None

    except Exception as e:
        logger.error(f"Error in process_last_message: {e}")
        raise
    finally:
        # Don't close the connection here
        pass

@task(retries=2)
async def get_initial_book():
    logger = get_run_logger()
    try:
        # latest_book_date = await db_utils.get_latest_book_date()
        # book = await db_utils.get_book("your_ticker", latest_book_date)
        # logger.info(f"Initial book loaded for date: {latest_book_date}")
        book = pd.read_csv("/Users/youssefadiem/Downloads/book_20240716.csv")
        return book
    except Exception as e:
        logger.error(f"Error getting initial book: {e}")
        raise


@task(retries=2)
async def get_file_from_sftp(msg_body):

    logger.info("Entered verify_and_process_message")
    # Your existing verify_and_process_message task
    file_path = msg_body["path"]  #: "/subscriptions/order_000059435/item_000068201
    try:

        await sftp_utils.connect()
        file_info = await sftp_utils.get_file_info(file_path)
        file_data = await sftp_utils.read_file(file_path)
        logger.info(f'[verify_and_process_message]: {file_info} {file_data}')

        return file_info, file_data, file_info['mtime']

    except Exception as e:
        logger.error(f"Error verifying and processing file {file_path}: {str(e)}")
        raise
    finally:
        await sftp_utils.disconnect()



@task(retries=2)
async def read_and_verify_sftp_file(file_info, file_data):
    logger = get_run_logger()
    try:
        with zipfile.ZipFile(file_data, 'r') as z:
            csv_file = z.namelist()[0]
            with z.open(csv_file) as f:
                chunk_size = 10000
                chunks = []
                for chunk in pd.read_csv(f, chunksize=chunk_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)

        logger.info(f"File read and verified successfully: {file_info['file_name']}")

        if 'call_or_put' in df.columns:
            df = df.rename(columns={'call_or_put': 'call_put_flag'})
        if 'underlying' in df.columns:
            df = df.rename(columns={'underlying': 'ticker'})

        df['expiration_date'] = pd.to_datetime(df['expiration_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        if OPTION_SYMBOLS_TO_PROCESS:
            df = df[df['option_symbol'].isin(OPTION_SYMBOLS_TO_PROCESS)]

        #df = verify_data(df)

        return df
    except Exception as e:
        logger.error(f"Failed to read or verify file: {e}")
        raise


@task(retries=2)
async def build_latest_book(initial_book, intraday_data):
        logger.info('Entered update_book_intraday')
        if 'id' in initial_book.columns:
            initial_book = initial_book.drop(columns=['id'])

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

        logger.info('Finished update_book_intraday')

        return merged



@task(retries=2)
async def update_book_with_latest_greeks(book: pd.DataFrame) -> pd.DataFrame:
    logger.info('Entered update_book_with_latest_greeks')


    latest_datetime = book["effective_datetime"].max()
    effective_datetime = latest_datetime


    previous_business_day = get_previous_business_day(effective_datetime, nyse)

    current_date = effective_datetime.date()
    current_datetime = effective_datetime.strftime('%Y-%m-%d %H:%M:%S')

    previous_date = previous_business_day.date()
    previous_datetime = pd.Timestamp.combine(previous_date,effective_datetime.time()).strftime('%Y-%m-%d %H:%M:%S')

    start_time = time.time()  # Start the timer

    query = f"""
    SELECT option_symbol, contract_type, strike_price, expiration_date, time_stamp,
           implied_volatility, delta, gamma, vega
    FROM landing.poly_options_data
    WHERE
        date_only >= '{previous_date}'
        AND date_only <= '{current_date}'
        AND time_stamp BETWEEN '{previous_datetime}' AND '{current_datetime}'
    ORDER BY date_only DESC, time_stamp DESC
    """

    logger.info(f"[update_book_with_latest_greeks] db status: {db_utils.get_status()}")
    test_query = f"""
    SELECT option_symbol, contract_type, strike_price, expiration_date, time_stamp,
           implied_volatility, delta, gamma, vega
    FROM landing.poly_options_data
    WHERE time_stamp > '2024-07-23 12:00:36'
    """
    # breakpoint()
    poly_data = db_utils.execute_query(test_query)


    logger.info(f'Fetched {len(poly_data)} from poly')
    #poly_data = pd.concat(utils.return_query(query))  # Assuming return_query yields DataFrames
    elapsed_time = time.time() - start_time  # End the timer
    logger.info(f"{elapsed_time:.2f} seconds to get the greeks")

    poly_data.rename(columns={'implied_volatility': 'iv'}, inplace=True)
    poly_data['contract_id'] = poly_data['option_symbol'] + '_' + \
                               poly_data['contract_type'] + '_' + \
                               poly_data['strike_price'].astype(str) + '_' + \
                               poly_data['expiration_date']

    book['strike_price'] = book['strike_price'].astype(int)
    book['contract_id'] = book['option_symbol'] + '_' + \
                          book['call_put_flag'] + '_' + \
                          book['strike_price'].astype(str) + '_' + \
                          pd.to_datetime(book['expiration_date_original']).dt.strftime('%Y-%m-%d')

    # TODO: This takes too much time....
    tasks = [delayed(process_greek)(greek_name, poly_data, book.copy()) for greek_name in ['iv', 'delta', 'gamma', 'vega']]
    results = compute(*tasks)

    final_book = results[0]
    for result in results[1:]:
        final_book.update(result)

    book.drop(columns=['contract_id'], axis=1, inplace=True)
    book['time_stamp'] = get_eastern_time()

    elapsed_time = time.time() - start_time  # End the timer
    logger.info(f"{elapsed_time:.2f} seconds to finalize the book")

    logger.info(f"[END OF update_book_with_latest_greeks] db status: {db_utils.get_status()}")
    return book


@task(retries=2)
async def generate_posn_chart():
    # Your existing generate_posn_chart task
    pass


@flow(name="Intraday flow")
async def process_intraday_data():
    logger = get_run_logger()
    flow_start_time = time.time()

    loop = asyncio.get_event_loop()
    # await initialize_db()
    db_utils.connect()
    rabbitmq_utils = AsyncRabbitMQUtilities(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, logger=logger)
    #await rabbitmq_utils.heartbeat()
    logger.info(f"[process_intraday_data] db status: {db_utils.get_status()}")
    try:

        initial_book = await get_initial_book()
        message, msg_body = await process_last_message(rabbitmq_utils)


        logger.info(f"RabbitMQ status before processing: {rabbitmq_utils.get_status()}")
        await rabbitmq_utils.connect()
        logger.info(f"RabbitMQ status before processing: {rabbitmq_utils.get_status()}")
        await rabbitmq_utils.safe_ack(message)


        if message is None and msg_body is None:
            logger.info("No messages to process in the queue. Ending the flow.")
            return

        if message is None:
            logger.info("No message to process in process_intraday_data Flow")
            return

        logger.info(f"Process_last_message returned: {msg_body}")


        try:
            file_info, file_data, file_last_modified = await get_file_from_sftp(msg_body)
            if file_info:
                logger.info(f"FILE INFO ---> {file_info}")
                df = await read_and_verify_sftp_file(file_info, file_data)
                if df is not None and not df.empty:
                    logger.info(f"Successfully processed file: {file_info['file_name']}")
                    logger.info(f"DataFrame shape: {df.shape}")
                    logger.info(f"RabbitMQ status before processing: {rabbitmq_utils.get_status()}")
                    await rabbitmq_utils.ensure_connection()
                    logger.info(f"RabbitMQ status before processing: {rabbitmq_utils.get_status()}")

                    await rabbitmq_utils.safe_ack(message)
                    latest_book = await build_latest_book(initial_book, df)
                    final_book = await update_book_with_latest_greeks(latest_book)
                    logger.info(f"[BEFORE INSERT] db status: {db_utils.get_status()}")

                    #await insert_progress('intraday', 'intraday_books', final_book, logger=logger)
                    #await db_utils.('intraday', 'intraday_books', final_book)
                    logger.info(f"RabbitMQ status before processing: {rabbitmq_utils.get_status()}")
                    await rabbitmq_utils.ensure_connection()
                    logger.info(f"RabbitMQ status before processing: {rabbitmq_utils.get_status()}")

                    MAX_RETRIES = 3
                    for attempt in range(MAX_RETRIES):
                        try:
                            await rabbitmq_utils.safe_ack(message)
                            logger.info(f"Message acknowledged for file: {file_info['file_name']}")
                            break
                        except Exception as e:
                            logger.error(f"Error acknowledging message (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                            if attempt == MAX_RETRIES - 1:
                                logger.error("Failed to acknowledge message after all retries.")
                            await asyncio.sleep(10)  # Wait a bit before retrying


                    total_time_taken = time.time() - flow_start_time
                    logger.info(f"Total time taken: {total_time_taken:.2f} seconds")

                    time_difference = datetime.now() - file_last_modified
                    time_difference_seconds = time_difference.total_seconds()
                    logger.info(
                        f"Time difference between file last modified and process end: {time_difference_seconds} seconds")

                    # Send webhook to Discord bot
                    # await send_webhook_to_discord
                    # logger.info("Webhook sent to Discord bot")
                else:
                    logger.warning(f"DataFrame is empty or None for file: {file_info['file_name']}")
                    await rabbitmq_utils.safe_nack(message, requeue=True)
            else:
                logger.warning("File info is None")
                await rabbitmq_utils.safe_nack(message, requeue=True)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            await rabbitmq_utils.safe_nack(message, requeue=True)

    except Exception as e:
        logger.error(f"Error in process_intraday_data flow: {e}")





# async def main():
#     try:
#         await process_intraday_data()
#     except Exception as e:
#         logger.error(f"An error occurred in the main execution: {e}")
#     finally:
#         # Ensure all connections are properly closed
#         await rabbitmq_utils.close()
#
#         db_utils.close()
#         logger.info(f"[END OF FLOW] db status: {db_utils.get_status()}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_intraday_data())
    loop.close()
    #asyncio.run(process_intraday_data())
