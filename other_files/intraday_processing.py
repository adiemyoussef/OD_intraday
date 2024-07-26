from prefect import task, flow, get_run_logger
import time
from datetime import datetime, timedelta
import zipfile
import asyncssh
import io
import requests

from other_files.data_verification import *
from rabbitmq_handler import RobustRabbitMQHandler

@task
def get_initial_book():
    file_path = '/Users/youssefadiem/Downloads/book_20240716.csv'
    logger.info(f"Loading start-of-day book from {file_path}")
    return pd.read_csv(file_path)

@task(retries=3, retry_delay_seconds=60)
async def process_last_message(rabbit_handler: RobustRabbitMQHandler):
    logger = get_run_logger()
    function_start_time = time.time()
    try:
        channel = await rabbit_handler.get_channel()
        queue = await channel.get_queue(QUEUE_NAME)
        logger.info(f"Using the {queue} queue")

        current_time = datetime.now()
        is_next_day_period = (current_time.hour > 20) or (current_time.hour == 20 and current_time.minute >= 20)
        logger.info(f"Current time:{current_time}, {current_time.hour}, {current_time.minute}")

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

        start_time = time.time()
        all_messages = []
        most_recent_message = None
        most_recent_time = None

        while time.time() - start_time < PREFECT_WAIT_TIME_SFTP:
            message = await queue.get(fail=False)
            if message is None:
                break  # No more messages in the queue
            all_messages.append(message)

        logger.info(f"Found {len(all_messages)} messages in the queue")

        for message in all_messages:
            msg_body = message.body.decode('utf-8')
            file_info = parse_message(msg_body)

            if file_info:
                file_info_name_stripped = file_info['file_name'].strip('"')
                logger.info(f'Processing: {file_info_name_stripped}')

                file_time = datetime.strptime(file_info_name_stripped.split('_')[-2], '%H%M')
                if most_recent_time is None or file_time > most_recent_time:
                    most_recent_time = file_time
                    most_recent_message = (message, msg_body)

                if file_info_name_stripped == expected_file_name:
                    logger.info(f"Expected file found: {expected_file_name}")
                    logger.info(f"Time taken to find file: {time.time() - function_start_time:.2f} seconds")
                    return message, msg_body, all_messages
            else:
                logger.info("Invalid message format")

        if most_recent_message:
            logger.info(f"Expected file not found. Processing most recent file: {most_recent_message[1]}")
            return most_recent_message[0], most_recent_message[1], all_messages

        logger.warning(f"No suitable file found within {PREFECT_WAIT_TIME_SFTP} seconds.")
        return None, None, all_messages

    except aio_pika.AMQPException as e:
        logger.error(f"AMQP Error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

@task(retries=2)
async def verify_and_process_message(message):
    logger = get_run_logger()
    function_start_time = time.time()
    try:
        if not message or not isinstance(message, str):
            raise ValueError("Invalid message format")

        file_info = parse_message(message)
        if not file_info:
            return None

        if DEBUG_MODE:
            logger.info(f"DEBUG MODE: Loading file from local directory {LOCAL_DEBUG_DIR}")
            file_path = os.path.join(LOCAL_DEBUG_DIR, file_info['file_name'])
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File does not exist in local debug directory: {file_path}")

            with open(file_path, 'rb') as f:
                file_data = io.BytesIO(f.read())
            logger.info("Loaded the file from the local directory")
        else:
            async with asyncssh.connect(sftp_host, port=sftp_port, username=sftp_username, password=sftp_password, known_hosts=None) as conn:
                async with conn.start_sftp_client() as sftp_client:
                    clean_file_name = file_info['file_name'].strip('"')
                    file_path = os.path.join(SFTP_DIRECTORY, clean_file_name)
                    logger.info(f"Getting {file_path}")
                    try:
                        file_attr = await sftp_client.stat(file_path)
                        file_last_modified = datetime.fromtimestamp(file_attr.mtime)
                    except asyncssh.SFTPError as e:
                        if e.code == asyncssh.SFTPError.NO_SUCH_FILE:
                            raise FileNotFoundError(f"File does not exist: {file_path}")
                        else:
                            raise e

                    if not file_path.endswith('.zip'):
                        raise ValueError(f"Invalid file type: {file_path}")

                    logger.info("Downloading the file from the sftp")
                    file_data = io.BytesIO()
                    async with sftp_client.open(file_path, 'rb') as remote_file:
                        data = await remote_file.read()
                        file_data.write(data)
                    logger.info("Downloaded the file from the sftp")
                    file_data.seek(0)

        logger.info(f"Message verified, processed, and file loaded: {file_info}")
        logger.info(f"Time taken to verify and process message: {time.time() - function_start_time:.2f} seconds")
        return file_info, file_data, file_last_modified
    except Exception as e:
        logger.error(f"Failed to process message: {e}")
        raise

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
            df = df.rename(columns({'underlying': 'ticker'})

        df['expiration_date'] = pd.to_datetime(df['expiration_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        if OPTION_SYMBOLS_TO_PROCESS:
            df = df[df['option_symbol'].isin(OPTION_SYMBOLS_TO_PROCESS)]

        df = verify_data(df)

        return df
    except Exception as e:
        logger.error(f"Failed to read or verify file: {e}")
        raise

@task(retries=2)
async def build_latest_book(initial_book, intraday_data):
    logging.info('Entered update_book_intraday')
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
    processed_intraday['procust_posn'] = (
        (processed_intraday['procust_lt_100_open_buy_vol'] + processed_intraday['procust_lt_100_close_buy_vol'] +
         processed_intraday['procust_100_199_open_buy_vol'] + processed_intraday['procust_100_199_close_buy_vol'] +
         processed_intraday['procust_gt_199_open_buy_vol'] + processed_intraday['procust_gt_199_close_buy_vol']) -
        (processed_intraday['procust_lt_100_open_sell_vol'] + processed_intraday['procust_lt_100_close_sell_vol'] +
         processed_intraday['procust_100_199_open_sell_vol'] + processed_intraday['procust_100_199_close_sell_vol'] +
         processed_intraday['procust_gt_199_open_sell_vol'] + processed_intraday['procust_gt_199_close_sell_vol']))

    processed_intraday['total_customers_posn'] = (
        processed_intraday['firm_posn'] + processed_intraday['broker_posn'] +
        processed_intraday['nonprocust_posn'] + processed_intraday['procust_posn'])

    merged = pd.merge(initial_book, processed_intraday,
                      left_on=['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date_original'],
                      right_on=['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date'],
                      how='outer', suffixes=('', '_intraday'))

    position_columns = ['mm_posn', 'firm_posn', 'broker_posn', 'nonprocust_posn', 'procust_posn', 'total_customers_posn']

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
    position_columns = ['mm_posn', 'firm_posn', 'broker_posn', 'nonprocust_posn', 'procust_posn', 'total_customers_posn']
    merged = merged.loc[(merged[position_columns] != 0).any(axis=1)]
    merged = merged.sort_values(['expiration_date_original', 'mm_posn'], ascending=[True, False])
    return merged

@task(retries=2)
async def update_book_with_latest_greeks(book: pd.DataFrame) -> pd.DataFrame:
    logging.info('Entered update_book_with_latest_greeks')
    latest_datetime = book["effective_datetime"].max()
    effective_datetime = latest_datetime
    previous_business_day = get_previous_business_day(effective_datetime, nyse)
    effective_date = effective_datetime.date()
    effective_time = effective_datetime.time()
    previous_date = previous_business_day.date()
    start_time = time.time()

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
    poly_data = utils.return_query_2(query)

    logger.info(f'Fetched {len(poly_data)} from poly')
    elapsed_time = time.time() - start_time
    logger.info(f"{elapsed_time:.2f} seconds to get the greeks")

    poly_data.rename(columns={'implied_volatility': 'iv'}, inplace=True)
    poly_data['contract_id'] = poly_data['option_symbol'] + '_' + poly_data['contract_type'] + '_' + poly_data['strike_price'].astype(str) + '_' + poly_data['expiration_date']

    book['strike_price'] = book['strike_price'].astype(int)
    book['contract_id'] = book['option_symbol'] + '_' + book['call_put_flag'] + '_' + book['strike_price'].astype(str) + '_' + pd.to_datetime(book['expiration_date_original']).dt.strftime('%Y-%m-%d')

    from dask import delayed, compute

    tasks = [delayed(process_greek)(greek_name, poly_data, book.copy()) for greek_name in ['iv', 'delta', 'gamma', 'vega']]
    results = compute(*tasks)

    final_book = results[0]
    for result in results[1:]:
        final_book.update(result)

    book.drop(columns=['contract_id'], axis=1, inplace=True)
    book['time_stamp'] = utils.get_eastern_time()

    elapsed_time = time.time() - start_time
    logger.info(f"{elapsed_time:.2f} seconds to finalize the book")
    return book

@task(retries=2)
async def generate_posn_chart():
    figure_base64 = generate_figure()

    webhook_url = "http://localhost:8081/webhook"
    payload = {
        "event": "prefect_flow_completed",
        "command": "test",
        "data": {
            "message": f"Processing completed for file",
            "figure": figure_base64
        }
    }

    response = requests.post(webhook_url, json=payload)
    response.raise_for_status()
    logger.info("Webhook with DataFrame figure sent to Discord bot")

@flow(name="Intraday flow")
async def process_intraday_data():
    logger = get_run_logger()
    flow_start_time = time.time()

    connection_params = {
        "host": RABBITMQ_HOST,
        "port": RABBITMQ_PORT,
        "login": RABBITMQ_USER,
        "password": RABBITMQ_PASS,
        "heartbeat": 60
    }
    rabbit_handler = RobustRabbitMQHandler(connection_params)

    try:
        message, msg_body, all_messages = await process_last_message(rabbit_handler)

        if message is None:
            logger.info("No message to process")
            return

        initial_book = get_initial_book()
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
                    utils.insert_progress('intraday', 'intraday_books', final_book)

                    try:
                        await rabbit_handler.safe_ack(message)
                        logger.info(f"Message acknowledged for file: {file_info['file_name']}")
                    except Exception as e:
                        logger.error(f"Failed to acknowledge message: {e}")

                    for other_message in all_messages:
                        if other_message != message:
                            try:
                                await rabbit_handler.safe_nack(other_message, requeue=True)
                            except Exception as e:
                                logger.error(f"Failed to nack other message: {e}")

                    total_time_taken = time.time() - flow_start_time
                    logger.info(f"Total time taken: {total_time_taken:.2f} seconds")

                    time_difference = datetime.now() - file_last_modified
                    time_difference_seconds = time_difference.total_seconds()
                    logger.info(f"Time difference between file last modified and process end: {time_difference_seconds} seconds")

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
                    await rabbit_handler.safe_nack(message, requeue=True)
            else:
                logger.warning("File info is None")
                await rabbit_handler.safe_nack(message, requeue=True)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            await rabbit_handler.safe_nack(message, requeue=True)
            for other_message in all_messages:
                if other_message != message:
                    await rabbit_handler.safe_nack(other_message, requeue=True)

    except Exception as e:
        logger.error(f"Error in process_intraday_data flow: {e}")
        if 'all_messages' in locals():
            for msg in all_messages:
                try:
                    await rabbit_handler.safe_nack(msg, requeue=True)
                except Exception as e:
                    logger.error(f"Failed to nack message during error handling: {e}")
    finally:
        await rabbit_handler.close()
