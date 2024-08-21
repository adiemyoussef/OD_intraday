import time
import sys
import pandas as pd
import pandas_market_calendars as mcal
import pytz
from OptionsDepth import Options_data
import utilities.sql_config as config
from datetime import datetime, timedelta

# Initializing class and functions
od = Options_data()
nyse = mcal.get_calendar('NYSE')
montreal_tz = pytz.timezone('America/Montreal')

def round_up_to_nearest_5min(dt):
    # Round up to the nearest 5 minutes
    new_minute = (dt.minute + 4) // 5 * 5
    rounded_time = dt.replace(second=0, microsecond=0, minute=0, hour=dt.hour) + timedelta(minutes=new_minute)
    return rounded_time

def get_latest_quote_date():
    """
    Fetches the latest quote date from a specific table.

    This function is intended to retrieve the most recent date for which quotes are available.
    It accesses a predefined table and extracts the latest date entry.

    :return: The latest quote date.
    """
    # Implement the logic to fetch the latest quote_date from the tables
    # Return the latest quote_date
    pass

def date_isin_mm_books(quote_date:str, ticker:str):
    """
    Checks if a given quote_date exists in the results.mm_books table.

    This function queries a table to check if a specific date is present for a given ticker.
    It is used to determine if data for a certain date has already been processed or not.

    :param quote_date: The date to check in the results.mm_books table.
    :param ticker: The ticker symbol associated with the quote_date.
    :return: Boolean indicating whether the date is in the table (True if present, False otherwise).
    """
    result = od.read_table(config.is_date_in_mm_books, param={"quote_date": str(quote_date), "ticker": ticker})
    # Assuming the result is a DataFrame with the count in the first cell
    count = result.iloc[0, 0]

    #TODO: Add a condition to check if the count is normal
    #      e.g: For SPX, if we have 300 lines, it's not normal.

    return count > 0

def calculate_next_trading_date(date:str):
    """
    Calculates the next trading date after a given date using the NYSE trading calendar.

    This function is useful for determining the next active trading day,
    particularly for scheduling or forecasting purposes.

    :param date: The date from which to calculate the next trading date.
    :return: The next valid trading date as per the NYSE calendar.
    """
    # Ensure the date is in the correct format
    formatted_date = pd.to_datetime(date).date()

    # Get the trading dates
    trading_dates = nyse.valid_days(start_date=formatted_date, end_date=formatted_date + pd.DateOffset(days=10))

    # Find the next trading date after the current date
    for trading_date in trading_dates:
        if trading_date.date() > formatted_date:
            return trading_date.date()

    # Return None if no trading date is found (unlikely for a short range)
    return None

def manipulation_before_insert(df_mm_book: pd.DataFrame, revised:str = True) -> pd.DataFrame:
    """
    Performs necessary data manipulations before inserting the DataFrame into a table.

    This function includes the following steps:
    1. Adding a timestamp column.
    2. Renaming the 'book_date' column to 'as_of_date'.
    3. Removing '^' characters from the 'underlying_symbol' column.
    4. Renaming 'underlying_symbol' to 'ticker'.
    5. Calculating and inserting an 'effective_date' column.
    6. Inserting 'customer_positions' column after 'net_contract' with value -1 * net_contract.

    :param df_mm_book: The DataFrame to manipulate.
    :return: The manipulated DataFrame.
    """

    try:
        # Add a Timestamp Column
        formatted_timestamp = pd.Timestamp.now(tz=montreal_tz).strftime('%Y-%m-%d %H:%M:%S')

        if revised:
            revised_book_statut = 'Y'
        else:
            revised_book_statut = 'N'

        df_mm_book.insert(0, 'revised', revised_book_statut)
        df_mm_book.insert(1, 'time_stamp', formatted_timestamp)
        # Rename 'book_date' to 'as_of_date'
        df_mm_book.rename(columns={'book_date': 'as_of_date'}, inplace=True)

        # Remove '^' from 'underlying_symbol'
        df_mm_book['underlying_symbol'] = df_mm_book['underlying_symbol'].str.replace('^', '', regex=False)

        # Rename 'underlying_symbol' to 'ticker'
        df_mm_book.rename(columns={'underlying_symbol': 'ticker'}, inplace=True)

        # Calculate 'effective_date' and insert it
        df_mm_book.insert(3, 'effective_date', df_mm_book['as_of_date'].apply(calculate_next_trading_date))


        # # Insert 'customer_positions' column after 'net_contract' with value -1 * net_contract
        # net_contract_index = df_mm_book.columns.get_loc('net_contract')
        # df_mm_book.insert(net_contract_index + 1, 'customer_positions', -1 * df_mm_book['net_contract'])

    except Exception as e:
        print(f"An error occurred during manipulation: {e}")

    return df_mm_book


def insert_to_table(df_book: pd.DataFrame, schema:str = 'results', table:str = 'mm_books'):
    """
    Inserts data into a specific table.

    This function should contain the logic to insert entries into a designated table.
    It is assumed that the data to be inserted is prepared and available.

    """
    # Logic to insert entries into the table
    try:
        print(f'Length of book to insert: {len(df_book)}')
        od.insert_with_progress(schema,df_book, table)
    except Exception as e:
        print(f"Error: \n\n{e}", file=sys.stderr)

def delete_entries(ticker:str, date:str):
    """
    Deletes specific entries from a table.

    This function should contain the logic to remove entries from a designated table.
    The criteria for deletion would depend on the implementation specifics.

    """
    flag, message, affected_rows = od.execute_query(config.delete_mm_book_entry, param={"as_of_date": date,"ticker":ticker})
    print(f'Deleted entries --->',message)

    return affected_rows


def update_mm_book_with_latest_data(date: str) -> pd.DataFrame:
    """
    This function processes options data for a given date, merging it with implied volatility, delta,
    and gamma data, and updates the original DataFrame to reflect the latest values for these metrics
    after 16:00. It ensures that missing values are forward-filled appropriately and that the final
    DataFrame contains the most recent data.

    Parameters:
        date (str): The date for which to process the options data, in the format 'YYYY-MM-DD'.

    Returns:
        pd.DataFrame: The updated DataFrame containing the latest implied_volatility_1545, delta_1545,
                      and gamma_1545 values.
    """
    try:

        start_time = '15:30:00'
        greeks_snapshot_time = '16:00'
        iv_snapshot_time = '15:45'


        # mm_book_from_query = od.read_table(config.specific_book, param={"book_date": date, "ticker": '^SPX'})
        # breakpoint()
        mm_book_from_query = od.read_table(config.new_specific_book, param={"book_date": date, "ticker": '^SPX'})
        mm_book = manipulation_before_insert(mm_book_from_query)

        # Date Manipulation to prevent further complications with the merges
        mm_book['expiration_date_original'] = pd.to_datetime(mm_book['expiration_date_original'])
        mm_book['expiration_date_original'] = mm_book['expiration_date_original'].dt.strftime('%Y-%m-%d')
        # columns_to_keep = ['time_stamp', 'ticker', 'option_symbol', 'effective_date', 'as_of_date',
        #                    'call_put_flag', 'expiration_date_original', 'expiration_date', 'strike_price',
        #                    'net_contract', 'customer_positions', 'delta_1545', 'gamma_1545', 'implied_volatility_1545']
        # mm_book = mm_book[columns_to_keep]
        greeks_date = mm_book["as_of_date"].values.max()
        query = f"""SELECT * FROM landing.poly_options_data where date(time_stamp) = '{greeks_date}' and time(time_stamp) > '{start_time}'"""
        iv_collected = od.read_table(query)


        if iv_collected.empty:
            mm_book_final = mm_book.copy()

            print(f"Implied Volatility 1545 - Min: {mm_book_final['iv'].min()}, Max: {mm_book_final['iv'].max()}")
            print(f"Delta 1545 - Min: {mm_book_final['delta'].min()}, Max: {mm_book_final['delta'].max()}")
            print(f"Gamma 1545 - Min: {mm_book_final['gamma'].min()}, Max: {mm_book_final['gamma'].max()}")

            # Check for None (NaN) values before returning
            if mm_book_final['iv'].isna().any():
                print("Error: There are None values in implied_volatility_1545 column.")
                breakpoint()
                return None
            if mm_book_final['delta'].isna().any():
                print("Error: There are None values in delta_1545 column.")
                breakpoint()
                return None
            if mm_book_final['gamma'].isna().any():
                print("Error: There are None values in gamma_1545 column.")
                breakpoint()
                return None

            return mm_book_final


        else:

            max_ids = iv_collected.groupby('time_stamp')['id'].max()

            # Then, join this back to the original dataframe to filter rows
            iv_collected = iv_collected[iv_collected.set_index('time_stamp')['id'].eq(max_ids).values]

            # Combine columns to create a unique identifier for each contract
            iv_collected['contract_id'] = iv_collected['option_symbol'] + '_' + iv_collected['contract_type'] + '_' + \
                                          iv_collected['strike_price'].astype(str) + '_' + iv_collected['expiration_date']
            pivot_df = iv_collected.pivot(index='contract_id', columns='time_stamp', values='implied_volatility')
            pivot_df_deltas = iv_collected.pivot(index='contract_id', columns='time_stamp', values='delta')
            pivot_df_gammas = iv_collected.pivot(index='contract_id', columns='time_stamp', values='gamma')

            # Resetting the index to get 'contract_id' as a column
            pivot_df.reset_index(inplace=True)
            pivot_df_deltas.reset_index(inplace=True)
            pivot_df_gammas.reset_index(inplace=True)
            # Split 'contract_id' back into the original columns
            pivot_df[['option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']] = pivot_df[
                'contract_id'].str.split('_', expand=True)
            pivot_df_deltas[['option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']] = pivot_df[
                'contract_id'].str.split('_', expand=True)
            pivot_df_gammas[['option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']] = pivot_df[
                'contract_id'].str.split('_', expand=True)



            # Convert 'strike_price' back to numeric
            pivot_df['strike_price'] = pivot_df['strike_price'].astype(float)
            pivot_df_deltas['strike_price'] = pivot_df_deltas['strike_price'].astype(float)
            pivot_df_gammas['strike_price'] = pivot_df_deltas['strike_price'].astype(float)

            # Reorder columns to move the original columns to the front
            pivot_df = pivot_df[
                ['option_symbol', 'call_put_flag', 'strike_price', 'expiration_date'] + [col for col in pivot_df.columns if
                                                                                         col not in ['option_symbol',
                                                                                                     'call_put_flag',
                                                                                                     'strike_price',
                                                                                                     'expiration_date']]]
            pivot_df_deltas = pivot_df_deltas[
                ['option_symbol', 'call_put_flag', 'strike_price', 'expiration_date'] + [col for col in
                                                                                         pivot_df_deltas.columns if
                                                                                         col not in ['option_symbol',
                                                                                                     'call_put_flag',
                                                                                                     'strike_price',
                                                                                                     'expiration_date']]]

            pivot_df_gammas = pivot_df_gammas[
                ['option_symbol', 'call_put_flag', 'strike_price', 'expiration_date'] + [col for col in
                                                                                         pivot_df_gammas.columns if
                                                                                         col not in ['option_symbol',
                                                                                                     'call_put_flag',
                                                                                                     'strike_price',
                                                                                                     'expiration_date']]]

            customer_positions_id_index = mm_book.columns.get_loc('total_customers_posn')

            # ----------- IV ---------------#

            mm_book_iv = mm_book.copy()
            mm_book_iv.drop(['delta', 'gamma','vega'], axis=1, inplace=True)
            merged_df_iv = pd.merge(mm_book_iv, pivot_df,
                                    left_on=['option_symbol', 'call_put_flag', 'expiration_date_original', 'strike_price'],
                                    right_on=['option_symbol', 'call_put_flag', 'expiration_date', 'strike_price'],
                                    how='left')

            merged_df_iv.drop(['expiration_date_x', 'expiration_date_y', 'contract_id'], axis=1, inplace=True)
            df_iv = merged_df_iv.iloc[:, customer_positions_id_index:]
            df_iv = df_iv.ffill(axis=1)
            merged_df_iv.update(df_iv)

            merged_df_iv.sort_values('expiration_date_original', ascending=True, inplace=True)

            # ----------- Delta ---------------#
            mm_book_delta = mm_book.copy()
            mm_book_delta.drop(['iv', 'gamma', 'vega'], axis=1, inplace=True)
            merged_df_deltas = pd.merge(mm_book_delta, pivot_df_deltas,
                                        left_on=['option_symbol', 'call_put_flag', 'expiration_date_original',
                                                 'strike_price'],
                                        right_on=['option_symbol', 'call_put_flag', 'expiration_date', 'strike_price'],
                                        how='left')

            merged_df_deltas.drop(['expiration_date_x', 'expiration_date_y', 'contract_id'], axis=1, inplace=True)
            df_deltas = merged_df_deltas.iloc[:, customer_positions_id_index:]
            df_deltas = df_deltas.ffill(axis=1)
            merged_df_deltas.update(df_deltas)
            merged_df_deltas.sort_values('expiration_date_original', ascending=True, inplace=True)

            # ----------- Gamma ------------#
            mm_book_gamma = mm_book.copy()
            mm_book_gamma.drop(['iv', 'delta', 'vega'], axis=1, inplace=True)
            merged_df_gammas = pd.merge(mm_book_gamma, pivot_df_gammas,
                                        left_on=['option_symbol', 'call_put_flag', 'expiration_date_original',
                                                 'strike_price'],
                                        right_on=['option_symbol', 'call_put_flag', 'expiration_date', 'strike_price'],
                                        how='left')

            merged_df_gammas.drop(['expiration_date_x', 'expiration_date_y', 'contract_id'], axis=1, inplace=True)
            df_gammas = merged_df_gammas.iloc[:, customer_positions_id_index:]
            df_gammas = df_gammas.ffill(axis=1)
            merged_df_gammas.update(df_gammas)
            merged_df_gammas.sort_values('expiration_date_original', ascending=True, inplace=True)

            # -------------------------------#
            # -------------------- UPDATING THE BOOK ---------------#
            contract_id_index = pivot_df.columns.get_loc('contract_id')
            next_col_after_contract_id = pivot_df.columns[contract_id_index + 1]
            # Extract the column names starting from the 7th column
            # datetime_columns_iv = merged_df_iv.columns[greeks_index:]
            # datetime_columns_delta = merged_df_deltas.columns[greeks_index:]
            # datetime_columns_gamma = merged_df_gammas.columns[greeks_index:]
            datetime_columns_iv = merged_df_iv.columns[merged_df_iv.columns.get_loc(next_col_after_contract_id):]
            datetime_columns_delta = merged_df_deltas.columns[merged_df_deltas.columns.get_loc(next_col_after_contract_id):]
            datetime_columns_gamma = merged_df_gammas.columns[merged_df_gammas.columns.get_loc(next_col_after_contract_id):]

            # Parse the column names to datetime objects
            datetime_objects_iv = [datetime.strptime(col, '%Y-%m-%d %H:%M:%S') for col in datetime_columns_iv]
            datetime_objects_delta = [datetime.strptime(col, '%Y-%m-%d %H:%M:%S') for col in datetime_columns_delta]
            datetime_objects_gamma = [datetime.strptime(col, '%Y-%m-%d %H:%M:%S') for col in datetime_columns_gamma]

            # Filter out datetimes that are after 16:00
            after_16_iv = [dt for dt in datetime_objects_iv if dt.time() > datetime.strptime(iv_snapshot_time, '%H:%M').time()]
            after_16_delta = [dt for dt in datetime_objects_delta if dt.time() > datetime.strptime(greeks_snapshot_time, '%H:%M').time()]
            after_16_gamma = [dt for dt in datetime_objects_gamma if dt.time() > datetime.strptime(greeks_snapshot_time, '%H:%M').time()]

            # If there are no datetimes after 16:00, handle this case
            if not after_16_iv or not after_16_delta or not after_16_gamma:
                print(f"No columns with datetime after {iv_snapshot_time} found... Proceeding with EOD greeks")
                mm_book_final = mm_book.copy()

                print(f"Implied Volatility 1545 - Min: {mm_book_final['iv'].min()}, Max: {mm_book_final['iv'].max()}")
                print(f"Delta 1545 - Min: {mm_book_final['delta'].min()}, Max: {mm_book_final['delta'].max()}")
                print(f"Gamma 1545 - Min: {mm_book_final['gamma'].min()}, Max: {mm_book_final['gamma'].max()}")

                # Check for None (NaN) values before returning
                if mm_book_final['iv'].isna().any():
                    print("Error: There are None values in implied_volatility_1545 column.")
                    breakpoint()
                    return None
                if mm_book_final['delta'].isna().any():
                    print("Error: There are None values in delta_1545 column.")
                    breakpoint()
                    return None
                if mm_book_final['gamma'].isna().any():
                    print("Error: There are None values in gamma_1545 column.")
                    breakpoint()
                    return None

                return mm_book_final
                return None
            else:
                # Find the datetime that is closest to snapshot time
                closest_after_16_iv = min(after_16_iv, key=lambda x: (
                            x - datetime.combine(x.date(), datetime.strptime(iv_snapshot_time, '%H:%M').time())).total_seconds())
                closest_after_16_delta = min(after_16_delta, key=lambda x: (
                            x - datetime.combine(x.date(), datetime.strptime(greeks_snapshot_time, '%H:%M').time())).total_seconds())
                closest_after_16_gamma = min(after_16_gamma, key=lambda x: (
                            x - datetime.combine(x.date(), datetime.strptime(greeks_snapshot_time, '%H:%M').time())).total_seconds())

                # Find the corresponding column names
                closest_column_iv = closest_after_16_iv.strftime('%Y-%m-%d %H:%M:%S')
                closest_column_delta = closest_after_16_delta.strftime('%Y-%m-%d %H:%M:%S')
                closest_column_gamma = closest_after_16_gamma.strftime('%Y-%m-%d %H:%M:%S')

                # Display the column names
                print(f"The column closest to but after 16:00 for IV is: {closest_column_iv}")
                print(f"The column closest to but after 16:00 for Delta is: {closest_column_delta}")
                print(f"The column closest to but after 16:00 for Gamma is: {closest_column_gamma}")

                mm_book_final = mm_book.copy()
                # Update the values for implied_volatility_1545, delta_1545, and gamma_1545 with the values from the closest columns after 16:00
                mm_book_final['iv'] = merged_df_iv[closest_column_iv]
                mm_book_final['delta'] = merged_df_deltas[closest_column_delta]
                mm_book_final['gamma'] = merged_df_gammas[closest_column_gamma]

                print(f"Implied Volatility 1545 - Min: {mm_book_final['iv'].min()}, Max: {mm_book_final['iv'].max()}")
                print(f"Delta 1545 - Min: {mm_book_final['delta'].min()}, Max: {mm_book_final['delta'].max()}")
                print(f"Gamma 1545 - Min: {mm_book_final['gamma'].min()}, Max: {mm_book_final['gamma'].max()}")

                # Check for None (NaN) values before returning
                if mm_book_final['iv'].isna().any():
                    print("Error: There are None values in implied_volatility_1545 column.")
                    breakpoint()
                    return None
                if mm_book_final['delta'].isna().any():
                    print("Error: There are None values in delta_1545 column.")
                    breakpoint()
                    return None
                if mm_book_final['gamma'].isna().any():
                    print("Error: There are None values in gamma_1545 column.")
                    breakpoint()
                    return None

                return mm_book_final

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def generate_book(ticker:str, date:str):
    """
    Generates a 'book' for a specific ticker and date.

    This function reads data for a given ticker and date,
    and then applies necessary manipulations before the data is ready for use.

    :param ticker: The ticker symbol for which to generate the book.
    :param date: The date for which to generate the book.
    :return: The manipulated DataFrame (book) for the given ticker and date.
    """
    # Logic to generate the book
    updated_mm_book = update_mm_book_with_latest_data(date)
    # mm_book = od.read_table(config.specific_book,param={"book_date": date,"ticker":ticker})
    # mm_book_modified = manipulation_before_insert(updated_mm_book)

    return updated_mm_book


def generate_mm_books(override=False, sleep_time=600, retry_cycles=6):
    """
    This function orchestrates the generation of daily mm_books for export to results.mm_books.

    The process includes:
        - Verifying if the OC and EOD tables are synchronized.
        - Checking if the date to insert is already in the table.
        - Optionally overriding a pre-existing entry.
        - Retrying the insert process for a specified number of cycles if needed.

    :param override: Allows overriding a pre-existing entry.
    :param sleep_time: Time to wait (in seconds) between retries.
    :param retry_cycles: The number of cycles to attempt retry.
    :return: None. The function performs operations and may raise an exception if unsuccessful.
    """
    # Temporary
    ticker = '^SPX'

    # TODO: Detect the dates that are in OC but not in mm_books in case we want to backdate.
    # TODO: Allow to insert in mm_books for back_dates
    dates = ['2024-01-03', '2024-01-04']
    print("Starting....")
    oc_dates = od.read_table('select distinct(quote_date) FROM landing.OC')
    #as_of_dates = od.read_table('select distinct(as_of_date) FROM optionsdepth_stage.charts_mmbook')
    as_of_dates = od.read_table('select distinct(as_of_date) FROM intraday.new_daily_book_format')
    # Perform a left join
    merged_df = pd.merge(oc_dates, as_of_dates, left_on='quote_date', right_on='as_of_date', how='left')
    merged_df['quote_date'] = pd.to_datetime(merged_df['quote_date'])
    filtered_df = merged_df[merged_df['quote_date'] > '2024-01-01']
    dates_to_run = filtered_df[filtered_df['as_of_date'].isna()]['quote_date'].dt.date.values
    breakpoint()
    for date in reversed(dates_to_run):
        print(f'Starting for quote_date: {date}')

        for _ in range(retry_cycles):
            if od.tables_are_synced():
                #df_book = generate_book(ticker, date)
                #breakpoint()
                quote_date = od.read_table(config.latest_quote_date).values[0][0]
                if date_isin_mm_books(quote_date, ticker.replace('^','')) and override:
                    print(f'Override entry for {date}')
                    df_book = generate_book(ticker, date)
                    #if not delete_entries(ticker.replace('^',''), date):
                    #     print("Deletion not working")
                    #     break
                    #TODO: Remove after Launch


                    #breakpoint()
                    insert_to_table(df_book, 'intraday', 'new_daily_book_format')
                    # insert_to_table(df_book, 'optionsdepth_stage', 'charts_mmbook')
                elif not date_isin_mm_books(quote_date, ticker.replace('^','')):
                    print(f'{date} not in table')
                    df_book = generate_book(ticker, date)

                    #breakpoint()
                    insert_to_table(df_book, 'intraday', 'new_daily_book_format')
                    #insert_to_table(df_book, 'optionsdepth_stage', 'charts_mmbook')

                print(f"{date} already in results.mm_books. No Override")
                break
            else:
                time.sleep(sleep_time)  # Wait for 10 minutes
        else:
            raise Exception("OC and EOD not Synchronised")

def run(override_entries:bool):
    try:
        generate_mm_books(override_entries)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)

if __name__ == "__main__":
    # import pyarrow.parquet as pq
    # import pyarrow as pa
    # parquet_file = 'poly_options.parquet'
    # chunk_size = 100000
    # query = f"""SELECT * FROM landing.poly_options_data ORDER BY date_only"""
    # chunks = []
    # # Initialize the start row
    # start_row = 0
    #
    # for chunk in od.read_table(query,chunk_size):
    #
    #     breakpoint()
    #     # Convert the chunk DataFrame to Arrow table
    #     table = pa.Table.from_pandas(chunk)
    #     # Append the Arrow table to the list of chunks
    #     chunks.append(table)
    #
    #     # Save to Parquet file incrementally
    #     with pq.ParquetWriter(parquet_file, table.schema, use_dictionary=True, compression='snappy') as writer:
    #         for table_chunk in chunks:
    #             writer.write_table(table_chunk)
    #
    #     # Clear the chunks list to free memory
    #     chunks.clear()
    #     start_row += chunk_size
    #     print(f'Processed up to row {start_row}')
    #
    # breakpoint()
    run(override_entries=True)
