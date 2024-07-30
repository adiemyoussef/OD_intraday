import os
import time
import zipfile
from datetime import datetime, timedelta
import pandas as pd
import asyncio
import logging
from dask import delayed, compute
import dask

#------- OptionsDepth Modules -------- #
from config.config import *
from utilities.sftp_utils_async import SFTPUtility
from utilities.db_utils_async import AsyncDatabaseUtilities
from utilities.misc_utils import *
#--------------------------------------#


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and set it for the handler
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(file_formatter)

# Add the handler to the logger
logger.addHandler(console_handler)


db = AsyncDatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME, logger)

def process_greek_og(greek_name, poly_data, book):
    latest_greek = poly_data.sort_values('time_stamp', ascending=False).groupby('contract_id').first().reset_index()
    latest_greek = latest_greek[['contract_id', greek_name, 'time_stamp']]
    book = pd.merge(book, latest_greek, on='contract_id', how='left', suffixes=('', '_update'),indicator=True)

    print("--------------")
    print("Merge Analysis:")
    print(f"Total rows after merge: {len(book)}")
    print(f"Rows only in initial_book: {book.get('left_only', 0)}")
    print(f"Rows only in processed_intraday: {book.get('right_only', 0)}")
    print(f"Rows in both DataFrames: {book.get('both', 0)}")

    if int(book.get('both', 0)) > 0:
        print(f"Merging {int(book.get('both', 0))} rows")
    # Calculate percentages
    total_rows = len(book)
    print("\nPercentages:")
    print(f"Rows only in initial_book: {book.get('left_only', 0) / total_rows * 100:.2f}%")
    print(f"Rows only in processed_intraday: {book.get('right_only', 0) / total_rows * 100:.2f}%")
    print(f"Rows in both DataFrames: {book.get('both', 0) / total_rows * 100:.2f}%")


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
async def get_distinct_sessions(sftp_utility: SFTPUtility, sftp_folder: str) -> list:
    """Detect distinct sessions in the SFTP folder."""
    try:
        files = await sftp_utility.list_directory(sftp_folder)
        sessions = set()
        for file_name in files:
            if file_name.startswith("Cboe_OpenClose_") and file_name.endswith(".csv.zip"):
                date_str = file_name.split("_")[2]
                sessions.add(date_str)
        return sorted(list(sessions), reverse=True)
    except Exception as e:
        logger.error(f"Error getting distinct sessions: {str(e)}")
        raise

async def get_initial_book(session_date: str):
    """Get the initial book for a session."""
    logger.info(f'Getting book for {session_date}')
    initial_book = await db.get_initial_book('SPX', session_date)
    return initial_book

async def update_book_with_latest_greeks(book: pd.DataFrame, lookback_hours=24) -> pd.DataFrame:

    """
    #TODO:
    In the case of backdate, I want to optimize this so I don't lose 50 sec. on the query everytime.
    Run it asynchronously ?
    Live: Load the query with the expected datetime and reload it only if the expected doesn't arrive on time and I need to process the newest

    :param book:
    :return:
    """

    latest_datetime = book["effective_datetime"].max()
    effective_datetime = pd.Timestamp(latest_datetime)
    effective_datetime = latest_datetime

    logging.info('Entered update_book_with_latest_greeks')


    debug_datetime = '2024-07-21 20:20:00'
    test_datetime = pd.to_datetime(debug_datetime, utc=True)
    #effective_datetime = test_datetime

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

    start_time = time.time()
    logger.info(f'Starting to Fetch greeks from poly from {previous_datetime} to {current_datetime}')

    poly_data_list = await db.execute_query(query)
    logger.info(f'Fetched {len(poly_data_list)} from poly')
    poly_data = pd.DataFrame(poly_data_list)

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

async def verify_and_process_message(sftp_utility: SFTPUtility, file_path: str):
    """Verify and process the file."""
    try:
        file_info = await sftp_utility.get_file_info(file_path)
        file_data = await sftp_utility.read_file(file_path)

        return file_info, file_data, file_info['mtime']
    except Exception as e:
        logger.error(f"Error verifying and processing file {file_path}: {str(e)}")
        raise

async def read_and_verify_file(file_info, file_data):

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

        df = verify_intraday_data(df)

        return df

    except Exception as e:
        logger.error(f"Failed to read or verify file: {e}")
        raise


async def build_latest_book(initial_book, intraday_data):
    """
    Build the latest book based on initial book and new data.
    """
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

    #-------------------- DATA CONVERSION AND VALIDATION ----------------#
    # Data Manipulation muste be done:

    # Convert strike_price in initial_book from Decimal to float
    initial_book['strike_price'] = initial_book['strike_price'].astype(float)

    # Convert strike_price in processed_intraday from np.float64 to float (this might not be necessary, but it ensures consistency)
    processed_intraday['strike_price'] = processed_intraday['strike_price'].astype(float)

    # Convert expiration_date_original in initial_book from datetime.date to datetime64[ns]
    initial_book['expiration_date_original'] = pd.to_datetime(initial_book['expiration_date_original'])

    # Convert expiration_date in processed_intraday from string to datetime64[ns]
    processed_intraday['expiration_date'] = pd.to_datetime(processed_intraday['expiration_date'])
    #------------------------------------------------------------#


    merged = pd.merge(initial_book, processed_intraday,
                      left_on=['ticker', 'option_symbol', 'call_put_flag', 'strike_price',
                               'expiration_date_original'],
                      right_on=['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date'],
                      how='outer', suffixes=('', '_intraday'),
                      indicator=True)

    merge_counts = merged['_merge'].value_counts()
    time_stamp = intraday_data["trade_datetime"].unique()
    if len(time_stamp) == 1:
        time_stamp = time_stamp[0]
    else:
        breakpoint()
    # print("--------------")
    # print(f"Current timestamp:{time_stamp}")
    # print("Merge Analysis:")
    # print(f"Total rows after merge: {len(merged)}")
    # print(f"Rows only in initial_book: {merge_counts.get('left_only', 0)}")
    # print(f"Rows only in processed_intraday: {merge_counts.get('right_only', 0)}")
    # print(f"Rows in both DataFrames: {merge_counts.get('both', 0)}")
    #
    # if int(merge_counts.get('both', 0)) > 0:
    #     print(f"Merging {int(merge_counts.get('both', 0))} rows")
    # # Calculate percentages
    # total_rows = len(merged)
    # print("\nPercentages:")
    # print(f"Rows only in initial_book: {merge_counts.get('left_only', 0) / total_rows * 100:.2f}%")
    # print(f"Rows only in processed_intraday: {merge_counts.get('right_only', 0) / total_rows * 100:.2f}%")
    # print(f"Rows in both DataFrames: {merge_counts.get('both', 0) / total_rows * 100:.2f}%")


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

def parse_file_name(file_name):
    """Parse the file_name to extract date and time information."""
    parts = file_name.split('_')
    date_str, time_str = parts[2], f"{parts[3]}:{parts[4]}"
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")

async def get_file_info(sftp_utility, sftp_folder, file_name):
    file_path = f"{sftp_folder}/{file_name}"
    file_info = await sftp_utility.get_file_info(file_path)
    return file_name, file_info['mtime']

async def get_session_files(sftp_utility: SFTPUtility, sftp_folder: str, session_date: str):
    """Get all files for a given session in chronological order."""
    start_time = time.time()

    all_files = await sftp_utility.list_directory(sftp_folder)
    session_datetime = datetime.strptime(session_date, "%Y-%m-%d")

    # Filter files for the specific session date
    session_files = [f for f in all_files if f.startswith(f"Cboe_OpenClose_{session_date}")]
    start_time = time.time()

    # Parallelize file info retrieval
    tasks = [get_file_info(sftp_utility, sftp_folder, file_name) for file_name in session_files]
    file_info_list = await asyncio.gather(*tasks)

    # Sort files based on last modified time
    sorted_files = [f[0] for f in sorted(file_info_list, key=lambda x: x[1])]

    end_time = time.time()
    logger.info(f"Time taken to process {len(session_files)} files: {end_time - start_time:.2f} seconds")

    return sorted_files


async def process_session(sftp_utility: SFTPUtility, session_date: str, sftp_folder: str):
    """Process a single session."""
    logger.info(f"Processing session for date: {session_date}")

    initial_book = await get_initial_book(session_date)

    if initial_book is None:
        logger.error(f"Cannot process session {session_date} without an initial book.")
        return

    session_files = await get_session_files(sftp_utility, sftp_folder, session_date)
    logger.info(f"session_files: {session_files}")
    for file_name in session_files[99:]:
        file_path = f"{sftp_folder}/{file_name}"
        logger.info(f"Processing file: {file_name}")

        try:
            file_info, file_data, file_last_modified = await verify_and_process_message(sftp_utility, file_path)

            file_name_ = file_info['file_name']

            if file_info:
                logger.info(f"FILE INFO ---> {file_info}")
                df = await read_and_verify_file(file_info, file_data)

                if df is not None and not df.empty:
                    start_time = time.time()
                    logger.info(f"Successfully processed file: {file_info['file_name']}")
                    logger.info(f"DataFrame shape from SFTP: {df.shape}")

                    latest_book = await build_latest_book(initial_book, df)
                    latest_book['time_stamp'] = get_eastern_time()      #Temporaire
                    columns_to_drop = ['iv', 'delta', 'gamma', 'vega']
                    df_end = latest_book.iloc[:, :-4]


                    # final_book = await update_book_with_latest_greeks(latest_book)

                    # Check for NaN values
                    nan_counts = df_end.isna().sum()
                    print("Columns with NaN values:")
                    print(nan_counts[nan_counts > 0])

                    rows_with_nan = df_end.isna().any(axis=1).sum()
                    print(f"\nTotal rows with at least one NaN: {rows_with_nan}")
                    print(f"Percentage of rows with NaN: {rows_with_nan / len(df_end) * 100:.2f}%")

                    # Drop rows with NaN values
                    final_book_clean = df_end.dropna()

                    print(f"\nOriginal DataFrame shape: {df_end.shape}")
                    print(f"Cleaned DataFrame shape: {final_book_clean.shape}")
                    print(f"Rows removed: {len(df_end) - len(final_book_clean)}")

                    # breakpoint()
                    await db.insert_progress('intraday', 'intraday_books_test_posn', final_book_clean)
                    logger.info(f'It took {time.time() - start_time} sec. to process {file_name_}')

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {str(e)}")


async def main():
    sftp_config = {
        "sftp_host": SFTP_HOST,
        "sftp_port": int(SFTP_PORT),
        "sftp_username": SFTP_USERNAME,
        "sftp_password": "Salam123+-"
    }

    breakpoint()
    sftp_folder = SFTP_DIRECTORY

    async with SFTPUtility(**sftp_config, logger=logger) as sftp:
        try:
            distinct_sessions = await get_distinct_sessions(sftp, sftp_folder)
            logger.info(f"Distinct sessions: {distinct_sessions}")

            for session_date in distinct_sessions[:-1]:
                logger.info(f"Processing: {session_date}")
                await process_session(sftp, session_date, sftp_folder)
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())