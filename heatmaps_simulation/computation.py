import time
import pandas as pd
import pytz
from cupy_numba.main import compute_all
import argparse
import os
from datetime import datetime, timedelta
import numpy as np
from charting_test.intraday_plot import *
import yfinance as yf
from config.config import *
import logging
from utilities.db_utils import *

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

db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME, logger)

parser = argparse.ArgumentParser()
parser.add_argument("--mode", help="Calculation mode", choices=['delta', 'vanna'], default='delta')
parser.add_argument("--proc", help="Number of processors to use", default=32)
args = parser.parse_args()



def insert_heatmap_simulation(df, minima, maxima, ticker):

    # breakpoint()
    df_long = df.reset_index().melt(id_vars=['index'], var_name='price', value_name='value')
    df_long['minima'] = minima.reset_index().melt(id_vars=['index'], var_name='price', value_name='value').value
    df_long['maxima'] = maxima.reset_index().melt(id_vars=['index'], var_name='price', value_name='value').value
    df_long.rename(columns={'index': 'effective_datetime'}, inplace=True)

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    effective_date = df_long['effective_datetime'].iloc[0].date()

    df_long.insert(2, 'effective_date', effective_date)
    df_long.insert(1, 'ticker', ticker)

    return df_long

def time_to_secs(x):
    return x.total_seconds() / 86400

def array_tte(sim_times, expiration):
    array_tte = []
    for time in sim_times:
        tte = expiration - time
        array_tte.append(time_to_secs(tte))

    return array_tte

def simulation_times(start_time: datetime):
    """
    Generate an array of simulation times. The last value will be 15:59, replacing 16:00.

    :param start_time: Must be a datetime object, e.g., datetime(2023, 1, 11, 8, 30)
    :return: List of simulation times.
    """
    close_time = start_time.replace(second=0, microsecond=0, minute=0, hour=16)
    #close_time = start_time.replace(second=0, microsecond=0, minute=30, hour=9)
    incoming_sim_times = []

    while start_time <= close_time:
        incoming_sim_times.append(start_time)
        start_time += timedelta(minutes=HEATMAP_TIME_STEPS)

    # Replace the last time (16:00) with 15:59
    if incoming_sim_times[-1].time() == datetime(1, 1, 1, 16, 0).time():
        incoming_sim_times[-1] = incoming_sim_times[-1].replace(minute=59, hour=15)
    # else:
    #     incoming_sim_times.append(incoming_sim_times[-1].replace(minute=59, hour=15))

    return incoming_sim_times

def price_matrix(latest_price, steps, interval):
    """
    :param latest_price     : center price of the interval
    :param steps            : increment to take between each price point
    :param interval         : % up and down from the latest price (2% must be input as 0.02)
    :return:  returns a list of prices
    """
    latest_active_spx_price = latest_price
    pepe = 5 * round(latest_active_spx_price / 5)

    simulation_steps = steps
    lower_bound = round((1 - interval) * pepe)
    upper_bound = round((1 + interval) * pepe)

    lower_end = np.arange(pepe, lower_bound, -simulation_steps).tolist()
    upper_end = np.arange(pepe, upper_bound, +simulation_steps).tolist()
    list_prices = np.unique(np.sort(np.concatenate((lower_end, upper_end), dtype=float)))

    return list_prices
def book_to_list(mm_book:pd.DataFrame, sim_times):

    """
    receives the mm_book from the db, and structures it to include times to expiration
    :param mm_book: the mm_book from the db
    :return:
    """
    #breakpoint()
    mm_book['expiration_date'] = pd.to_datetime(mm_book['expiration_date'])
    mm_book['expiration_date'] = mm_book.apply(
        lambda row: row['expiration_date'].replace(hour=16, minute=0) if row['option_symbol'] == 'SPXW' else (
            row['expiration_date'].replace(hour=9, minute=15) if row['option_symbol'] == 'SPX' else row[
                'expiration_date']), axis=1)
    mm_book['tte'] = mm_book.apply(lambda row: array_tte(sim_times, row["expiration_date"]), axis=1)

    result = mm_book[['call_put_flag', 'strike_price', 'iv', 'tte', 'mm_posn']].copy()
    result['iv'].replace({'0': 0.00000000001, 0: 0.00000000001}, inplace = True)

    return result.values.tolist()

def resample_and_convert_timezone(df, datetime_column='effective_datetime', resample_interval='5T',
                                  target_timezone='US/Eastern'):
    """
    Resample a dataframe with 1-minute OHLCV data to a specified interval and convert timezone.

    Parameters:
    df (pandas.DataFrame): Input dataframe with OHLCV data
    datetime_column (str): Name of the datetime column (default: 'effective_datetime')
    resample_interval (str): Pandas resample rule (default: '5T' for 5 minutes)
    timezone (str): Timezone to convert to (default: 'US/Eastern')

    Returns:
    pandas.DataFrame: Resampled dataframe with converted timezone
    """

    # Ensure the datetime column is in the correct format
    df[datetime_column] = pd.to_datetime(df[datetime_column])

    # Set the datetime column as index
    df = df.set_index(datetime_column)

    # Check if the index is timezone-aware
    if df.index.tzinfo is None:
        # If timezone-naive, assume it's UTC and localize
        df.index = df.index.tz_localize('UTC')

    # Convert to target timezone
    target_tz = pytz.timezone(target_timezone)
    df.index = df.index.tz_convert(target_tz)

    # Resample to the specified interval
    df_resampled = df.resample(resample_interval).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
    })

    # Reset index to make datetime a column again
    df_resampled.reset_index(inplace=True)

    # Remove timezone information after conversion if needed
    df_resampled[datetime_column] = df_resampled[datetime_column].dt.tz_localize(None)

    return df_resampled


def generate_gamma_heatmap(sim_times: list, prices: list, deltas:np.ndarray, trade_date:datetime.date, hour_start:datetime.date):
    print("Generating heatmap results")
    simtime_number = []
    for (i, time) in enumerate(sim_times):
        simtime_number.append(i)


    charm = np.gradient(deltas, axis=1, edge_order=2)
    gamma = np.gradient(deltas, axis=0, edge_order=2)
    gamma_der = np.gradient(gamma, axis=0, edge_order=2)
    gamma_2der = np.gradient(gamma_der, axis=0, edge_order=2)


    df_charm = pd.DataFrame(charm.transpose(), columns=prices)
    df_gamma = pd.DataFrame(gamma.transpose(), columns=prices)
    df_gamma_der = pd.DataFrame(gamma_der.transpose(), columns=prices)
    df_gamma_2der = pd.DataFrame(gamma_2der.transpose(), columns=prices)

    hm_time_index = []
    for i in range(0, len(df_charm), 1):
        hour_string = str(hour_start)
        begin_time = datetime.strptime(hour_string, '%H:%M:%S')
        t = (begin_time + timedelta(minutes=i * 5)).time()
        full_date_time = datetime.combine(trade_date, t)
        hm_time_index.append(full_date_time)

    # Format
    df_charm.index = hm_time_index
    df_gamma.index = hm_time_index
    df_gamma_der.index = hm_time_index
    df_gamma_2der.index = hm_time_index


    return df_charm, df_gamma, df_gamma_der, df_gamma_2der



def compute_heatmap(args, type:str, df_book: pd.DataFrame, start_time:datetime, price:float, steps:float, range:float):

    num_processors = os.cpu_count()
    print("Number of processors available:", num_processors)
    args.proc = num_processors
    args.mode = type

    #TODO: Friday AM TEST
    df_book['iv'] = df_book['iv'].astype(float)

    # breakpoint()
    prices = price_matrix(price, steps, range).tolist()

    sim_times = simulation_times(start_time)

    book = book_to_list(df_book,sim_times)

    delta_array = compute_all(args,book,prices)

    df_charm, df_gamma, df_gammader, df_gamma2der = generate_gamma_heatmap(sim_times, prices, delta_array, start_time.date(), start_time.time())

    mask_maxima = df_gamma2der > 0
    mask_minima = df_gamma2der < 0

    maxima_df = df_gammader.copy()
    maxima_df[mask_maxima] = np.nan

    minima_df = df_gammader.copy()
    minima_df[mask_minima] = np.nan


    return df_charm, df_gamma, minima_df, maxima_df


def append_time(row):
    if row['option_symbol'] == 'SPX':
        return row['expiration_date_original'] + ' 09:15'
    elif row['option_symbol'] == 'SPXW':
        return row['expiration_date_original'] + ' 16:00'
    else:
        return row['expiration_date_original']  # Just in case there are other symbols


def filter_datetimes_specific_date(datetimes, target_date='2024-07-25', start_hour=7, end_hour=19):
    """
    Filter datetime strings to only include those on the target date between start_hour and end_hour.

    :param datetimes: List or array of datetime strings
    :param target_date: The specific date to filter for (default '2024-07-25')
    :param start_hour: Start hour for filtering (default 7 for 7 AM)
    :param end_hour: End hour for filtering (default 19 for 7 PM)
    :return: Filtered list of datetime strings
    """

    if not isinstance(datetimes, pd.DatetimeIndex):
        datetimes = pd.to_datetime(datetimes)

    target_date = pd.to_datetime(target_date).date()

    mask = (
            (datetimes.date == target_date) &
            (datetimes.hour >= start_hour) &
            (datetimes.hour < end_hour)
    )

    return datetimes[mask].tolist()


if __name__ == "__main__":
    spx = {"steps": HEATMAP_PRICE_STEPS, "range": HEATMAP_PRICE_RANGE}

    #------------- INPUTS ------------#



    EXPIRATIONS_TO_KEEP = 100         # Allows to filter on expirations for positionning
    # trading_view_file = "SPREADEX_SPX_5_20240731.csv"
    spx_data_raw = pd.read_csv('spx_20240806.csv')
    spx_data_raw.rename(columns={'time': 'effective_datetime'}, inplace = True)
    spx_data_raw['effective_datetime'] = pd.to_datetime(spx_data_raw['effective_datetime'], unit='s')
    spx_data_raw['effective_datetime'] = spx_data_raw['effective_datetime'].dt.tz_localize('UTC').dt.tz_convert(
        'US/Eastern')

    #----------------------------------#
    # -----------DATA READING----------#
    query = """
    SELECT * FROM intraday.intraday_books
    WHERE effective_date ='2024-08-06'
    and effective_datetime >= '2024-08-06 09:00:00'
    """

    df_books = db.execute_query(query)
    # breakpoint()
    #
    # df_book.to_pickle('20240725_books.pkl')


    #df_books = pd.read_pickle('20240725_books.pkl')
    # spx_data_raw_db = pd.read_csv('spx_20240725.csv')

    #------- TRADINGVIEW DATA-------- #

    all_datetimes = spx_data_raw['effective_datetime'].unique()


    filtered_datetimes = filter_datetimes_specific_date(all_datetimes)
    spx_data_raw_filtered = spx_data_raw[spx_data_raw['effective_datetime'].isin(filtered_datetimes)]


    spx_data = resample_and_convert_timezone(spx_data_raw)
    spx_data.set_index('effective_datetime', inplace=True)
    spx_data_chart = spx_data[spx_data.index.time >= pd.Timestamp('07:00').time()]
    target_date = pd.Timestamp('2024-08-06').date()
    start_time = pd.Timestamp('9:00').time()


    open_price = 5250

    unique_effective_datetimes = df_books['effective_datetime'].unique()
    cumulative_df = pd.DataFrame()
    for effective_datetime in unique_effective_datetimes:
        logger.info(f"Processing effective_datetime: {effective_datetime}")

        effective_datetime = pd.to_datetime(effective_datetime)
        spx_data_chart = spx_data[
            (spx_data.index.date == target_date) &
            (spx_data.index.time >= start_time) &
            (spx_data.index <= effective_datetime)
            ]


        datetime_object = pd.to_datetime(effective_datetime)

        df_subset = df_books[df_books['effective_datetime'] == effective_datetime].copy()

        start_heatmap_computations = time.time()

        #def compute_heatmap(args, type:str, df_book: pd.DataFrame, start_time:datetime, price:float, steps:float, range:float):
        df_charm, df_gamma, minima_df, maxima_df = compute_heatmap(args, type='delta', df_book=df_subset,
                                              start_time=datetime_object, price=open_price,
                                              steps=spx['steps'], range=spx['range'])

        logger.info(f'It took {time.time() - start_heatmap_computations} to generate the heatmap')

        # Update the cumulative heatmap
        #heatmap_manager.update_heatmap(df_heatmap, datetime_object)

        # Get the current cumulative heatmap for plotting
        #current_cumulative_heatmap = heatmap_manager.get_current_heatmap()


        # Filter SPX data for charting
        # spx_data_chart = spx_data[(spx_data.index <= effective_datetime) &
        #                           (spx_data.index.date == datetime_object.date())]


        # Plot the cumulative heatmap
        #plot_heatmap_cumul(cumulative_df, spx=spx_data_chart, show_fig=True)
        #df_to_plot = current_cumulative_heatmap.drop(columns=["effective_datetime"])
        #plot_heatmap(df_to_plot,effective_datetime, spx=spx_data_chart, show_fig=False)


        plot_gamma(df_heatmap=df_gamma, minima_df=minima_df, maxima_df=maxima_df, effective_datetime=effective_datetime, spx=spx_data_chart)

        logger.info(f"{effective_datetime} heatmap has been processed and plotted.")



    logger.info("All heatmaps have been processed !!!!")