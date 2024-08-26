import uuid

import pytz
from prefect import task, flow, get_run_logger
import pandas as pd
import numpy as np
from datetime import datetime
import plotly.graph_objects as go
from charting_test.intraday_plot import *
from config.config import *
# Import your utility classes
from utilities.sftp_utils import *
from utilities.db_utils import *
from utilities.rabbitmq_utils import *
from utilities.misc_utils import *
from utilities.customized_logger import DailyRotatingFileHandler
from utilities.logging_config import *
from charting.intraday_beta_v2 import *
from heatmaps_simulation.heatmap_task import *

import numpy as np
from datetime import datetime, timedelta

import os
import cv2
import numpy as np
from pathlib import Path
import uuid
from datetime import datetime


def create_video_from_frames(frame_paths, output_path, fps=5):
    """
    Create a video from a list of image frames.

    :param frame_paths: List of paths to the image frames
    :param output_path: Path where the output video will be saved
    :param fps: Frames per second for the output video
    """
    if not frame_paths:
        print("No frames to create video.")
        return

    # Read the first frame to get the frame size
    frame = cv2.imread(frame_paths[0])
    height, width, layers = frame.shape

    # Define the codec and create VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))

    for frame_path in frame_paths:
        frame = cv2.imread(frame_path)
        out.write(frame)

    # Release the VideoWriter
    out.release()
    print(f"Video created successfully: {output_path}")

def et_to_utc(et_time_str):
    # Parse the input string to a datetime object
    et_time = datetime.strptime(et_time_str, '%Y-%m-%d %H:%M:%S')

    # Set the timezone to Eastern Time
    eastern = pytz.timezone('US/Eastern')
    et_time = eastern.localize(et_time)

    # Convert to UTC
    utc_time = et_time.astimezone(pytz.UTC)

    # Format the UTC time as a string
    return utc_time.strftime('%Y-%m-%d %H:%M:%S')

def format_effective_datetime(effective_datetime):
    if isinstance(effective_datetime, np.ndarray) and effective_datetime.dtype.kind == 'M':
        # Convert numpy datetime64 to Python datetime
        py_datetime = effective_datetime[0].astype('datetime64[s]').astype(datetime)

        # If py_datetime is still an int (nanoseconds since epoch), convert it
        if isinstance(py_datetime, (int, np.integer)):
            py_datetime = datetime(1970, 1, 1) + timedelta(seconds=py_datetime.astype(int) * 1e-9)

        # Format the Python datetime
        return py_datetime.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(effective_datetime, str):
        # If it's already a string, try to parse and reformat it
        try:
            return datetime.strptime(effective_datetime, '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            # If parsing fails, return the original string
            return effective_datetime
    elif hasattr(effective_datetime, 'strftime'):
        # If it's a datetime-like object, format it directly
        return effective_datetime.strftime('%Y-%m-%d %H:%M:%S')
    else:
        raise ValueError(f"Unsupported type for effective_datetime: {type(effective_datetime)}")


# Example usage:
datetime_array = np.array(['2024-08-23T07:30:00.000000000'], dtype='datetime64[ns]')
formatted_datetime = format_effective_datetime(datetime_array)
print(formatted_datetime)  # Output: '2024-08-23 07:30:00'
def get_gamma_effective_datetime(effective_date):
    query = f"""
    SELECT distinct(effective_datetime) from intraday.intraday_gamma
    where effective_date = '{effective_date}'
    and time(effective_datetime) >= '09:50:00'
    """
    return db.execute_query(query)


def fetch_gamma_data(db, effective_date, effective_datetime):

    query = f"""
    WITH ranked_gamma AS (
        SELECT 
            id,
            ticker,
            effective_date,
            effective_datetime,
            price,
            value,
            sim_datetime,
            NULL as minima,
            NULL as maxima,
            ROW_NUMBER() OVER (PARTITION BY sim_datetime,price ORDER BY effective_datetime DESC) AS rn
        FROM intraday.intraday_gamma
        WHERE effective_datetime <= '{effective_datetime}' -- (SELECT max(effective_datetime) FROM intraday.intraday_gamma)
        and effective_date = '{effective_date}'
        and time(effective_datetime) >= '09:50:00'
    ),
    consumed_gamma AS (
        SELECT 
            id,
            ticker,
            effective_date,
            effective_datetime,
            price,
            value,
            sim_datetime,
            minima,
            maxima
        FROM ranked_gamma
        WHERE rn = 1
    ),
    upcoming_gamma AS (
        SELECT * 
        FROM intraday.intraday_gamma
        WHERE effective_datetime >= (SELECT max(effective_datetime) FROM intraday.intraday_gamma)
        and
        effective_date = '{effective_date}'
    ),
    final_gamma as(
    SELECT * FROM consumed_gamma
    UNION ALL
    SELECT * FROM upcoming_gamma
    )
    SELECT * from final_gamma
    -- group by effective_datetime;
    
    -- SELECT * from final_gamma
    """

    return db.execute_query(query)

def resample_and_convert_timezone(df:pd.DataFrame, datetime_column='effective_datetime', resample_interval='5T',
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

def process_gamma_data(df):
    df['effective_datetime'] = pd.to_datetime(df['effective_datetime'])
    df['sim_datetime'] = pd.to_datetime(df['sim_datetime'])
    df['price'] = df['price'].astype(float)
    df['value'] = df['value'].astype(float)
    df['minima'] = df['minima'].astype(float)
    df['maxima'] = df['maxima'].astype(float)
    return df

def generate_heatmap(df_gamma, minima_df, maxima_df, effective_datetime, spx_candlesticks=None):
    # Your existing plot_gamma function here
    # Make sure to adapt it to work with the new data structure
    pass


def heatmap_generation_flow(
        db,
        effective_date: str,
        steps: float = 2.5,
        range: float = 0.025,
):
    effective_datetimes = get_gamma_effective_datetime(effective_date).values

    # Create a unique temporary directory to store frames
    temp_dir = f'temp_frames_{uuid.uuid4().hex}'
    os.makedirs(temp_dir, exist_ok=True)

    frame_paths = []
    # Fetch and process gamma data
    for e_d in effective_datetimes:

        formatted_datetime = format_effective_datetime(e_d)
        print(f'Processing {formatted_datetime}')
        raw_gamma_data = fetch_gamma_data(db, effective_date, formatted_datetime)
        processed_gamma_data = process_gamma_data(raw_gamma_data)

        cd_formatted_datetime =et_to_utc(formatted_datetime)

        # Fetch candlestick data (assuming you still need this)
        cd_query = f"""
        SELECT * FROM optionsdepth_stage.charts_candlestick
        WHERE ticker = 'SPX' 
        AND 
        effective_date = '{effective_date}'
        AND 
        effective_datetime <= '{cd_formatted_datetime}'
        AND
        effective_datetime > '2024-08-26 13:50:00'
        """

        candlesticks = db.execute_query(cd_query)

        if candlesticks.empty:
            spx_candlesticks = None
        else:
            candlesticks_resampled = resample_and_convert_timezone(candlesticks)
            spx_candlesticks = candlesticks_resampled.set_index('effective_datetime', drop=False)


        effective_datetime = formatted_datetime
        # Filter data for current effective_datetime
        current_data = processed_gamma_data.copy() #[processed_gamma_data['effective_datetime'] == effective_datetime]

        # print(f'Len of current_data: {current_data.shape}')
        #
        # duplicates = current_data.duplicated(subset=['sim_datetime', 'price'], keep=False)
        # if duplicates.any():
        #     print(f"Found {duplicates.sum()} duplicate entries.")
        #
        # # Reshape data into heatmap format
        #
        # #----------TEST----------#
        # current_data = current_data.groupby(['sim_datetime', 'price']).agg({
        #     'value': 'sum',
        #     'minima': lambda x: x.dropna().iloc[0] if x.dropna().any() else np.nan,
        #     'maxima': lambda x: x.dropna().iloc[0] if x.dropna().any() else np.nan
        # }).reset_index()

        df_gamma = current_data.pivot_table(index='sim_datetime', columns='price', values='value') #, aggfunc='first')


        # For minima_df and maxima_df, use the same index and columns as df_gamma
        minima_df = current_data.pivot_table(index='sim_datetime', columns='price', values='minima') #, aggfunc='first')
        maxima_df = current_data.pivot_table(index='sim_datetime', columns='price', values='maxima') #, aggfunc='first')

        # Fill NaN values in minima_df and maxima_df
        minima_df = minima_df.reindex_like(df_gamma).fillna(np.nan)
        maxima_df = maxima_df.reindex_like(df_gamma).fillna(np.nan)

        # Generate and send heatma

        gamma_chart = plot_gamma(df_heatmap=df_gamma, minima_df=minima_df, maxima_df=maxima_df,
                                 effective_datetime=effective_datetime, spx=spx_candlesticks)

        # Save the figure as an image
        frame_path = os.path.join(temp_dir, f'frame_{len(frame_paths):03d}.png')

        gamma_chart.update_layout(
            width=1920,  # Full HD width
            height=1080,  # Full HD height
            font=dict(size=16)  # Increase font size for better readability
        )
        gamma_chart.write_image(frame_path, scale=2)

        frame_paths.append(frame_path)

        print(f"Heatmap for {effective_datetime} has been processed and saved.")


    # Generate video from saved frames
    output_video = f'heatmap_video_{effective_date}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.mp4'
    create_video_from_frames(frame_paths, output_video, fps=5)

    #send to discord both the video and the last frame
    # webhook: https://discord.com/api/webhooks/1277632021828599931/A5S1TsZmN3kJsN1ubUxkk3pmaRY3UsNp4WhQfPctvNqwiazwXakL6OV_IvD91zD7Aq6J

    # Clean up temporary files
    for file in frame_paths:
        os.remove(file)
    os.rmdir(temp_dir)

    print(f"Video generated: {output_video}")

    print(f"Video generated: {output_video}")
if __name__ == "__main__":
    db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
    db.connect()
    print(f'{db.get_status()}')
    heatmap_generation_flow(db, effective_date='2024-08-26')