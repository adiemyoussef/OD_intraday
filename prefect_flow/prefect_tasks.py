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

# Import your utility classes
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

# Setup the NYSE calendar
nyse = mcal.get_calendar('NYSE')

DEBUG_MODE = True
# Setup the main logger
logger = setup_custom_logger("Prefect Flow", logging.DEBUG if DEBUG_MODE else logging.INFO)

logger.setLevel(logging.INFO)  # Or any other level like logging.INFO, logging.WARNING, etc.

# Initialize utility instances
db_utils = AsyncDatabaseUtilities(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, logger=logger)
rabbitmq_utils = AsyncRabbitMQUtilities(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASS, logger=logger)

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
async def process_last_message():
    logger = get_run_logger()
    function_start_time = time.time()
    try:
        await rabbitmq_utils.connect()
        queue = await rabbitmq_utils.get_queue(RABBITMQ_CBOE_QUEUE)

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

        logger.info(f"Expected file name: {expected_file_name}")

        # Check if the queue is empty
        try:
            message = await queue.get(fail=False)
            if message is None:
                logger.warning("The queue is empty. No messages to process.")
                return None, None
            else:
                # Put the message back in the queue
                await queue.put(Message(message.body, **message.info()))
        except aio_pika_exceptions.QueueEmpty:
            logger.warning("The queue is empty. No messages to process.")
            return None, None

        messages = []
        logger.info("Starting to process messages from the queue")
        try:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    logger.info(f"Processing message: {message.message_id}")
                    async with message.process():
                        messages.append(message)
                        if len(messages) >= 10:  # Limit to last 10 messages for efficiency
                            logger.info("Reached limit of 10 messages")
                            break
        except Exception as e:
            logger.error(f"Error while iterating over queue: {e}")
            raise

        logger.info(f"Processed {len(messages)} messages from the queue")

        if not messages:
            logger.warning("No messages found in the queue.")
            return None, None

        # Reverse the list to process from the most recent message
        messages.reverse()

        for message in messages:
            msg_body = message.body.decode('utf-8')
            file_info = parse_message(msg_body)
            logger.info(f"Checking message: {file_info['file_name'] if file_info else 'Invalid message'}")
            if file_info and file_info['file_name'] == expected_file_name:
                logger.info(f"Expected file found: {expected_file_name}")
                logger.info(f"Time taken to find file: {time.time() - function_start_time:.2f} seconds")
                return message, msg_body

        # If expected message not found, process the last message
        last_message = messages[0]
        last_msg_body = last_message.body.decode('utf-8')
        last_file_info = parse_message(last_msg_body)
        logger.info(f"Expected file not found. Processing last message: {last_file_info['file_name'] if last_file_info else 'Invalid message'}")
        return last_message, last_msg_body

    except Exception as e:
        logger.error(f"Error in process_last_message: {e}")
        raise
    finally:
        await rabbitmq_utils.close()

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
async def verify_and_process_message(message):
    # Your existing verify_and_process_message task
    pass


@task(retries=2)
async def read_and_verify_file(file_info, file_data):
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
        return merged



@task(retries=2)
async def update_book_with_latest_greeks(book: pd.DataFrame) -> pd.DataFrame:
    logger.info('Entered update_book_with_latest_greeks')
    latest_datetime = book["effective_datetime"].max()
    effective_datetime = latest_datetime
    previous_business_day = get_previous_business_day(effective_datetime, nyse)
    effective_date = effective_datetime.date()
    effective_time = effective_datetime.time()
    previous_date = previous_business_day.date()
    start_time = time.time()  # Start the timer

    # Define the start of the 24-hour rolling window
    rolling_window_start = effective_datetime - timedelta(days=1)

    query = f"""
    SELECT option_symbol, contract_type, strike_price, expiration_date, time_stamp,
           implied_volatility, delta, gamma, vega
    FROM landing.poly_options_data
    WHERE
        date_only >= '{previous_date}'
        AND date_only <= '{effective_date}'
        AND (
            date_only < '{effective_date}'
            OR (date_only = '{effective_date}' AND time_stamp <= '{effective_datetime}')
        )
    ORDER BY date_only DESC, time_stamp DESC
    """

    query_2 = f"""
    SELECT option_symbol, contract_type, strike_price, expiration_date, time_stamp,
           implied_volatility, delta, gamma, vega
    FROM landing.poly_options_data
    WHERE
        time_stamp >= '{rolling_window_start}'
        AND time_stamp <= '{effective_datetime}'
    ORDER BY time_stamp DESC
    """
    # breakpoint()
    poly_data = await db_utils.execute_query(query)

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

    tasks = [delayed(process_greek)(greek_name, poly_data, book.copy()) for greek_name in ['iv', 'delta', 'gamma', 'vega']]
    results = compute(*tasks)

    final_book = results[0]
    for result in results[1:]:
        final_book.update(result)

    book.drop(columns=['contract_id'], axis=1, inplace=True)
    book['time_stamp'] = get_eastern_time()

    elapsed_time = time.time() - start_time  # End the timer
    logger.info(f"{elapsed_time:.2f} seconds to finalize the book")
    return book


@task(retries=2)
async def generate_posn_chart():
    # Your existing generate_posn_chart task
    pass


@flow(name="Intraday flow")
async def process_intraday_data():
    logger = get_run_logger()
    flow_start_time = time.time()
    try:
        await db_utils.create_pool()  # Ensure database connection pool is created

        message, msg_body = await process_last_message()

        if message is None and msg_body is None:
            logger.info("No messages to process in the queue. Ending the flow.")
            return

        if message is None:
            logger.info("No message to process in process_intraday_data Flow")
            return

        initial_book = await get_initial_book()
        logger.info(f"Process_last_message returned: {msg_body}")

        try:
            file_info, file_data, file_last_modified = await verify_and_process_message(msg_body)
            if file_info:
                logger.info(f"FILE INFO ---> {file_info}")
                df = await read_and_verify_file(file_info, file_data)
                if df is not None and not df.empty:
                    logger.info(f"Successfully processed file: {file_info['file_name']}")
                    logger.info(f"DataFrame shape: {df.shape}")

                    latest_book = await build_latest_book(initial_book, df)
                    final_book = await update_book_with_latest_greeks(latest_book)
                    await db_utils.insert_progress('intraday', 'intraday_books', final_book)

                    await rabbitmq_utils.safe_ack(message)
                    logger.info(f"Message acknowledged for file: {file_info['file_name']}")

                    total_time_taken = time.time() - flow_start_time
                    logger.info(f"Total time taken: {total_time_taken:.2f} seconds")

                    time_difference = datetime.now() - file_last_modified
                    time_difference_seconds = time_difference.total_seconds()
                    logger.info(
                        f"Time difference between file last modified and process end: {time_difference_seconds} seconds")

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
    finally:
        # Ensure connections are closed
        await rabbitmq_utils.close()

async def main():
    await process_intraday_data()

if __name__ == "__main__":
    asyncio.run(main())