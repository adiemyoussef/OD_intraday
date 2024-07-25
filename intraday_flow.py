from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta, time
import time as time_module
from zoneinfo import ZoneInfo
import dill
import zipfile

# Import your utility classes
from config.config import *
from utilities.sftp_utils import *
from utilities.db_utils import *
from utilities.rabbitmq_utils import *
from utilities.misc_utils import *
from utilities.customized_logger import DailyRotatingFileHandler
from charting.position_gif import *
# Setup
mp.set_start_method("fork", force=True)
dill.settings['recurse'] = True
DEBUG_MODE = True
LOG_LEVEL = logging.INFO

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

# Setup the main logger
logger = setup_custom_logger("Intraday Flow", logging.DEBUG if DEBUG_MODE else logging.INFO)
logger.setLevel(LOG_LEVEL)  # Or any other level like logging.INFO, logging.WARNING, etc.

#-------- Initializing the Classes -------#
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

def process_greek(greek_name, poly_data, book):

    logger.info("Processing Greeks...")

    latest_greek = poly_data.sort_values('time_stamp', ascending=False).groupby('contract_id').first().reset_index()
    latest_greek = latest_greek[['contract_id', greek_name, 'time_stamp']]
    book = pd.merge(book, latest_greek, on='contract_id', how='left', suffixes=('', '_update'))
    update_col = f"{greek_name}_update"
    if update_col in book.columns:
        book[greek_name] = book[update_col].combine_first(book[greek_name])
        book.drop(columns=[update_col, "time_stamp_update"], inplace=True)


    return book

def prepare_poly_fetch(current_time=None):
    """
    Prepare parameters for the poly data fetch query based on the current time.

    :param current_time: Optional. The current time to use for calculations.
                         If None, uses the actual current time in New York timezone.
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

    # Set the previous datetime to the same time on the previous business day
    previous_datetime = datetime.combine(previous_date, current_datetime.time())

    # Format dates and datetimes for the query
    previous_date_str = previous_date.strftime('%Y-%m-%d')
    current_date_str = current_date.strftime('%Y-%m-%d')
    previous_datetime_str = previous_datetime.strftime('%Y-%m-%d %H:%M:%S')
    current_datetime_str = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

    logger.debug(f"Prepared poly fetch parameters: "
                f"Previous Date: {previous_date_str}, "
                f"Current Date: {current_date_str}, "
                f"Previous Datetime: {previous_datetime_str}, "
                f"Current Datetime: {current_datetime_str}")

    return previous_date_str, current_date_str, previous_datetime_str, current_datetime_str

def ensure_connections():
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

def filter_and_log_nan_values(final_book):
    nan_rows = final_book[final_book.isna().any(axis=1)]
    nan_contracts = nan_rows['option_symbol'].unique()
    logger.debug(f"Filtered out {len(nan_rows)} rows with NaN values")
    logger.debug(f"Unique contracts filtered: {nan_contracts}")
    logger.debug(f"MM positions of filtered contracts: \n{nan_rows.groupby('option_symbol')['mm_posn'].sum()}")

    return final_book.dropna()

@task(retries=3, retry_delay_seconds=60)
async def process_last_message_in_queue(rabbitmq_utils:RabbitMQUtilities):
    prefect_logger = get_run_logger()
    prefect_logger.debug("process_last_message_in_queue")
    function_start_time = time.time()
    try:
        rabbitmq_utils.connect()
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

@task(name= 'Get initial book',
      task_run_name= ' Getting initial book',
      retries=2,
      cache_key_fn=task_input_hash,
      cache_expiration=timedelta(hours=1))
def get_initial_book(get_unrevised_book: Callable):
    prefect_logger = get_run_logger()

    try:
        current_time = datetime.now(ZoneInfo("America/New_York"))
        current_date = current_time.date()
        limit2am = current_time.replace(hour=2, minute=0, second=0, microsecond=0).time()
        limit7pm = current_time.replace(hour=19, minute=0, second=0, microsecond=0).time()

        #TODO: Verify that the current_date is a business date

        if limit2am <= current_time.time() < limit7pm:  # Between 2 AM and 7 PM
            prefect_logger.info(f"Operating during Revised book hours")
            logger.info(f"Operating during Revised book hours")
            query = f"""
            SELECT * FROM intraday.new_daily_book_format 
            WHERE effective_date = '{current_date}' AND revised = 'Y'
            """
            session_book = db_utils.execute_query(query)

            #------- Sanity checks ------------#
            unique_dates = session_book['effective_date'].unique()
            unique_revised_values = session_book['revised'].unique()
            book_length = len(session_book)
            unique_tickers = session_book['ticker'].nunique()
            unique_option_symbols = session_book['option_symbol'].nunique()

            # Additional checks you might consider
            if len(unique_dates) != 1:
                prefect_logger.warning(f"Multiple dates found in session_book: {unique_dates}")
                logger.warning(f"Multiple dates found in session_book: {unique_dates}")

            if session_book.empty:
                # We try to get a book even if it's unrevised
                query = f"""
                SELECT * FROM intraday.new_daily_book_format 
                WHERE effective_date = '{current_date}'
                """
                session_book = db_utils.execute_query(query)
                logger.info("Getting Unrevised book")
                prefect_logger.info(f"Revised session_book loaded for date: {current_date}")

                if session_book.empty:
                    send_notification(f"No session_book found for {current_date}. \nProceeding with caution...")
                    return get_unrevised_book()

                else:
                    send_notification(f"Unrevised session_book found for {current_date}.")
                    return session_book
            else:
                prefect_logger.info(f"Revised session_book loaded for date: {current_date}")
                logger.info(f"Revised session_book loaded for date: {current_date}")

                prefect_logger.info(f"--------------------------")
                prefect_logger.info(f"Session book sanity check:")
                prefect_logger.info(f"- Unique dates: {unique_dates}")
                prefect_logger.info(f"- Unique revised values: {unique_revised_values}")
                prefect_logger.info(f"- Book length: {book_length}")
                prefect_logger.info(f"- Unique tickers: {unique_tickers}")
                prefect_logger.info(f"- Unique option symbols: {unique_option_symbols}")
                prefect_logger.info(f"--------------------------")

                logger.info(f"--------------------------")
                logger.info(f"Session book sanity check:")
                logger.info(f"- Unique dates: {unique_dates}")
                logger.info(f"- Unique revised values: {unique_revised_values}")
                logger.info(f"- Book length: {book_length}")
                logger.info(f"- Unique tickers: {unique_tickers}")
                logger.info(f"- Unique option symbols: {unique_option_symbols}")
                logger.info(f"--------------------------")


                return session_book
        else:  # Between 8 PM and 2 AM
            logger.info("Using unrevised session_book due to time of day (8PM --> 2AM).")
            prefect_logger.info("Using unrevised session_book due to time of day.")
            return get_unrevised_book()

    except Exception as e:
        prefect_logger.error(f"Error getting initial session_book: {e}")
        raise

def get_unrevised_book():
    # This function will be implemented later

    unrevised_book = pd.read_csv('/Users/youssefadiem/Downloads/intradaybook_20240723.csv')


    return unrevised_book

@task(name='Get file from sftp',
      description='One the message from the queue is detecting, we grab the approproate file from the sftp',
      retries=2)
def get_file_from_sftp(msg_body):
    logger.debug("get_file_from_sftp")
    file_path = msg_body["path"]
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

@task(name= 'Reading and verify the sftp file',
      description= 'Bla bla bla',
      retries=2)
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

        #df = verify_data(df)

        return df
    except Exception as e:
        logger.error(f"Failed to read or verify file: {e}")
        raise

@task(name='Build the latest book',
      description = 'Once a new file arrives on the sftp, we refresh our initial book with the latest positions',
      retries=2)
async def build_latest_book(initial_book, intraday_data):
        logger.debug('Entered update_book_intraday')
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

        logger.debug('Finished update_book_intraday')

        return merged

@task(name= 'Fetch greeks',
      task_run_name= 'Fetching greeks',
      description= 'Fetching greeks from database to have the latest greeks for each contract',
      retries=2)
def fetch_poly_data(previous_date, current_date, previous_datetime, current_datetime):
    prefect_logger = get_run_logger()

    prefect_logger.info('Fetching data from poly....')
    start_time = time.time()
    query = f"""
    SELECT option_symbol, contract_type, strike_price, expiration_date, time_stamp,
           implied_volatility, delta, gamma, vega
    FROM landing.poly_options_data
    WHERE
        date_only BETWEEN'{previous_date}' AND '{current_date}'
        AND time_stamp BETWEEN '{previous_datetime}' AND '{current_datetime}'
    """

    poly_data  = db_utils.execute_query(query)
    logger.info(f'Fetched {len(poly_data)} rows in {time.time() - start_time} sec.')

    return poly_data


@task(name= 'Update book with latest greeks',
      task_run_name= 'Updating the book with the latest greeks',
      description= 'Update the book with the latest greeks available for each contract',
      retries=2)
def update_book_with_latest_greeks(book: pd.DataFrame, poly_data: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Entered update_book_with_latest_greeks')

    # Create a key for faster merge
    book['strike_price'] = book['strike_price'].astype(int)
    book['contract_id'] = book['option_symbol'] + '_' + \
                          book['call_put_flag'] + '_' + \
                          book['strike_price'].astype(str) + '_' + \
                          pd.to_datetime(book['expiration_date_original']).dt.strftime('%Y-%m-%d')

    poly_data.rename(columns={'implied_volatility': 'iv'}, inplace=True)
    poly_data['contract_id'] = poly_data['option_symbol'] + '_' + \
                               poly_data['contract_type'] + '_' + \
                               poly_data['strike_price'].astype(str) + '_' + \
                               poly_data['expiration_date']


    # Filter and keep the rows without NaN values
    mask = ~book.drop(columns=['time_stamp']).isna().any(axis=1)
    book = book[mask]

    false_count = (~mask).sum()
    logger.debug("Number of rows with NaN values in greeks: {}".format(false_count))

    start_time = time.time()
    cpu_count = os.cpu_count()
    logger.info(f'Got {cpu_count} CPUs')
    try:
        with ThreadPoolExecutor(max_workers=cpu_count) as executor:
            futures = [executor.submit(process_greek, greek_name, poly_data, book.copy())
                       for greek_name in ['iv', 'delta', 'gamma', 'vega']]
            results = [future.result() for future in futures]

        final_book = results[0]
        for result in results[1:]:
            final_book.update(result)

    except Exception as e:
        logger.error(f"Error In multi-Processing): {e}")

        return pd.DataFrame()


    final_book.drop(columns=['contract_id'], axis=1, inplace=True)
    final_book['time_stamp'] = get_eastern_time()

    elapsed_time = time.time() - start_time
    logger.debug(f"{elapsed_time:.2f} seconds to update_book_with_latest_greeks")

    logger.debug(f"[END OF update_book_with_latest_greeks] db status: {db_utils.get_status()}")

    return final_book


@task(
    name="Generate and Send Intraday GIFs",
    task_run_name="generate_send_intraday_gifs_{book_date_loaded}",
    description="Generates GIFs from intraday books data and sends them to Discord for a specific date and strike range."
)
def generate_send_intraday_gifs(book_date_loaded: str, webhook_url: str, strike_ranges: list, expiration:str):


    def convert_to_date_string(date_input):
        if isinstance(date_input, (datetime, date)):
            return date_input.strftime('%Y-%m-%d')
        elif isinstance(date_input, str):
            return date_input  # Assume it's already in the correct format
        elif isinstance(date_input, (np.datetime64, np.ndarray)):
            return np.datetime_as_string(date_input, unit='D')
        else:
            raise ValueError(f"Unsupported date type: {type(date_input)}")

    if isinstance(book_date_loaded, (list, np.ndarray, tuple, set)):
        if len(book_date_loaded) == 1:
            book_date_loaded = book_date_loaded[0]
            book_date = convert_to_date_string(book_date_loaded)
        else:
            raise ValueError(f"Unexpected multiple dates in book_date_loaded: {book_date_loaded}")
    else:
        book_date = convert_to_date_string(book_date_loaded)


    query = f"""
    SELECT * FROM intraday.intraday_books
    WHERE date(time_stamp) = '{book_date}'
    """

    try:
        start_time = time.time()
        all_intraday_books = db_utils.execute_query(query)
        logger.info(
            f"Retrieved intraday books data for {book_date} in {time.time() - start_time}: {len(all_intraday_books)} entrie with {len(all_intraday_books['time_stamp'].unique())} time stamps")
        success = generate_and_send_gif(all_intraday_books, book_date, 'mm', strike_ranges, expiration,webhook_url)
        if success:
            logger.info(f"Successfully generated and sent GIFs to Discord for {book_date}")
        else:
            logger.error(f"Failed to generate or send some GIFs for {book_date}")

        return success
    except Exception as e:
        logger.error(f"Error in loading all intraday books: {e}")


#----------------- FLOWS ------------------#
@flow(name="Optimized Intraday Flow - Late Night")
def Intraday_Flow():
    prefect_logger = get_run_logger()
    flow_start_time = time.time()

    db_utils.connect()
    # rabbitmq_utils.connect()

    try:
        #Fast
        initial_book = get_initial_book(get_unrevised_book)

        book_date_loaded = initial_book["effective_date"].unique()
        logger.info(f"Initial Book of {book_date_loaded} loaded")

        previous_date, current_date, previous_datetime, current_datetime = prepare_poly_fetch()


        # Fetch poly data: Slow
        poly_data = fetch_poly_data(previous_date, current_date, previous_datetime, current_datetime)

        rabbitmq_utils.connect()

        # Ensure connections are established
        connections_ready = ensure_connections()
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
            file_info, file_data, file_last_modified = get_file_from_sftp(msg_body)
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
                    db_utils.insert_progress('intraday', 'intraday_books',filtered_final_book)

                    logger.info(f"[BEFORE GIF]: Finished flow in {time.time()-flow_start_time} sec.")

                    # Example strike ranges
                    strike_ranges = [5260, 5530]

                    # result = generate_send_intraday_gifs(
                    #     book_date_loaded=book_date_loaded,
                    #     webhook_url=WEBHOOK_URL,
                    #     strike_ranges=strike_ranges,
                    #     expiration = '2024-07-25'
                    #
                    # )
                    #
                    # if not result:
                    #     raise Exception("Failed to generate and send intraday GIFs")

                    logger.info(f"[AFTER GIF]: Finished flow in {time.time() - flow_start_time} sec.")


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

