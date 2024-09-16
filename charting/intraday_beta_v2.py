import json
import math
import uuid

import pandas as pd
import pytz
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash, Task
from datetime import timedelta, datetime, date
from typing import List, Optional
#from charting.generate_gifs import generate_gif, send_to_discord
from charting.generate_gifs import generate_gif, send_to_discord,generate_video
from utilities.db_utils import DatabaseUtilities
from config.config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
import os
from datetime import datetime, time
import numpy as np
import plotly.graph_objects as go
from PIL import Image
from config.config import *
from enum import Enum

DEV_CHANNEL ='https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
#https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ

default_date = date.today() - timedelta(days=0)
LIST_PART = ['total_customers', 'broker', 'firm', 'retail', 'institution']
STRIKE_RANGE = [5485, 5800]

db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
db.connect()
print(f'{db.get_status()}')




def load_logo():
    try:
        return Image.open(LOGO_dark)
    except FileNotFoundError:
        print(f"Warning: Logo file not found at {LOGO_dark}. Using placeholder.")
        return Image.new('RGBA', (100, 100), color=(73, 109, 137))


def get_next_expiration_date(current_date: date) -> date:
    # This function should return the next expiration date (usually the next Friday)
    # For simplicity, let's assume it's always the next day

    #TODO: Use NEXT trading day, not next day
    return current_date + timedelta(days=1)

@task
def parse_strike_range(strike_range: str) -> List[int]:
    values = list(map(int, strike_range.split(',')))
    return [min(values), max(values)]

@task(cache_key_fn=None, cache_expiration=timedelta(hours=0, minutes=1))
def fetch_data(session_date: str,effective_datetime:str, strike_range: List[int], expiration: str, start_time:str = '07:00:00') -> [pd.DataFrame]:

    start_of_video = f'{session_date} {start_time}'

    metrics_query = f"""
    SELECT * FROM intraday.intraday_books
    WHERE effective_date = '{session_date}'
    """

    if effective_datetime is not None:
        metrics_query += f" AND effective_datetime = '{effective_datetime}'"

    if start_time is not None:
        metrics_query += f" AND effective_datetime >= '{start_of_video}'"

    if strike_range is not None:
        metrics_query += f" AND strike_price BETWEEN {strike_range[0]} AND {strike_range[1]}"

    if expiration is not None:
        metrics_query += f" AND expiration_date_original = '{expiration}'"

    candlesticks_query = f"""
    SELECT * FROM optionsdepth_stage.charts_candlestick
    where effective_date = '{session_date}'
    and
    ticker = 'SPX'
    """

    last_price_query = f"""
    SELECT close FROM optionsdepth_stage.charts_candlestick 
    WHERE id = (SELECT MAX(id) FROM optionsdepth_stage.charts_candlestick WHERE ticker = 'SPX')
    """


    metrics = db.execute_query(metrics_query)
    candlesticks = db.execute_query(candlesticks_query)
    last_price = db.execute_query(last_price_query)


    if candlesticks.empty:
        print("No candlesticks Available")

    else:
        candlesticks['effective_datetime'] = (
            pd.to_datetime(candlesticks['effective_datetime'], utc=True)  # Set timezone to UTC
            .dt.tz_convert('America/New_York')  # Convert to Eastern Time
            .dt.tz_localize(None)  # Remove timezone information (make naive)
        )
        candlesticks.drop_duplicates(keep='first', inplace=False)


    return metrics, candlesticks, last_price


@task(cache_key_fn=None, cache_expiration=timedelta(hours=0, minutes=1))
def fetch_data_depthview(session_date: str, strike_range: List[int], expiration: str = None) -> [pd.DataFrame]:

    metrics_query = f"""
    SELECT * FROM intraday.intraday_books
    WHERE effective_date = '{session_date}'
    AND effective_datetime >= (SELECT MAX(effective_datetime) FROM intraday.intraday_books where effective_date = '{session_date}')
    AND strike_price BETWEEN {strike_range[0]} AND {strike_range[1]}
    """

    if expiration is not None:
        metrics_query += f"AND expiration_date_original = '{expiration}'"


    metrics = db.execute_query(metrics_query)

    return metrics
@task
def process_data(metric: pd.DataFrame,candlesticks: pd.DataFrame, session_date: str, participant: str,
                 strike_range: List[int], expiration: str, position_types: List[str]) -> List[str]:
    gif_paths = []
    for position_type in position_types:
        gif_path = generate_gif(
            metric,candlesticks, session_date, participant, position_type,
            strike_range, expiration,
            output_gif=f'animated_chart_{position_type}.gif'
        )
        if gif_path:
            gif_paths.append(gif_path)
        else:
            print(f"Failed to generate GIF for {position_type}")
    return gif_paths

@task
def generate_video_task(data: pd.DataFrame, candlesticks: pd.DataFrame, session_date: str, participant: str,
                        strike_range: List[int], expiration: str, position_type: list, last_price:float, metric:str = 'positioning') -> str:

    videos_paths =[]

    for pos_type in position_type:

        video_path = generate_video(
            data, candlesticks, session_date, participant, pos_type,
            strike_range, expiration, metric, last_price,
            output_video=f'{pos_type}_{expiration}_animated_chart.mp4'
        )
        videos_paths.append(video_path)

    return videos_paths

@task
def send_discord_message(file_paths: List[str], as_of_time_stamp:str, session_date: str, participant: str,
                         strike_range: List[int], expiration: str, position_types: List[str], metric: str,
                         webhook_url: str) -> bool:
    participant_mapping = {
        'mm': 'Market Makers',
        'broker': 'Brokers and Dealers',
        'firm': 'Firms',
        'nonprocus': 'Non-Professional Customers',
        'procust': 'Professional Customers',
        'total_customers': 'Total Customers'
    }
    participant_text = participant_mapping.get(participant, 'Unknown Participant')

    title = f"📊 {session_date} Intraday Recap"
    description = (
        f"Detailed analysis of {participant_text} {metric} for the {session_date} session.\n"
        f"This chart provides insights into market movements and positioning within the specified strike range.\n"
        ""
    )
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fields = [
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "📈 Analysis Type", "value": f"Intraday {metric} Movement", "inline": True},
        {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
        {"name": "", "value":"", "inline": True},
        {"name": "👥 Participant(s)", "value": participant_text, "inline": True},
        {"name": "🎯 Strike Range", "value": f"{strike_range[0]} - {strike_range[1]}", "inline": True},
        {"name": "📅 Expiration(s)", "value": expiration, "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "", "value": "", "inline": True},
        {"name": "💡 Interpretation", "value": (
            "• Green bars indicate Calls positioning\n"
            "• Red bars indicate Puts positioning\n"
            "• Blue bars indicate Net positioning\n"
            "\n"
            "• ■ represents current position magnitude\n"
            "• ● represents the start of day position magnitude\n"
            "• ✖ represents the prior update position magnitude\n"
            "\n"
        ), "inline": False},
    ]
    footer_text = f"Generated on {current_time} | By OptionsDepth Inc."

    success = send_to_discord(
        webhook_url,
        file_paths,
        content="🚀 New options chart analysis is ready! Check out the latest market insights below.",
        title=title,
        description=description,
        fields=fields,
        footer_text=footer_text
    )

    # Clean up the gif files after sending
    for path in file_paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            print(f"Warning: Could not delete file {path}. It may have already been deleted.")

    return success

@flow(name="0DTE gifs")
def zero_dte_flow(
    session_date: Optional[date] = default_date,
    strike_range: Optional[List[int]] = None,
    expiration: Optional[str] = None,
    participant: str = 'total_customers',
    position_types: Optional[List[str]] = None,
    webhook_url: str = 'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'
                        #DEV_CHANNEL
    ):
    if strike_range:
        strike_range = parse_strike_range(strike_range)


    position_types = ['Net','C','P']
    expiration = str(session_date)


    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = STRIKE_RANGE
    if expiration is None:
        expiration = session_date
    if position_types is None:
        position_types = ['C', 'P', 'Net']
    elif 'All' in position_types:
        position_types = ['C', 'P', 'Net']



    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")

    # Fetch data
    metrics, candlesticks, last_price = fetch_data(session_date, None,strike_range, expiration, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())
    last_price = last_price.values[0][0]
    # Process data and generate GIFs
    videos_paths = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types, last_price ,'positioning')
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range, expiration, position_types,'positioning', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")

@flow(name="1DTE gifs")
def one_dte_flow(
    session_date: Optional[date] = default_date,
    strike_range: Optional[List[int]] = None,
    expiration: Optional[str] = None,
    participant: str = 'total_customers',
    position_types: Optional[List[str]] = None,
    webhook_url: str = 'https://discord.com/api/webhooks/1275269470151245938/qNZXtA_ySwcJJJf6bS_myYqU-uDd71zHV--XJBR7xb6uVhs7ccjKE59_c8y9AMZ86OC_'
                       #DEV_CHANNEL
):

    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = STRIKE_RANGE

    if position_types is None:
        position_types = ['Net','C','P']
    if expiration is None:
        #TODO: Next business day not day
        expiration = str(get_next_expiration_date(session_date))
    elif 'All' in position_types:
        position_types = ['C', 'P', 'Net']

    # expiration = '2024-09-09'

    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")

    # Fetch data
    data, candlesticks, last_price = fetch_data(session_date, None,strike_range, expiration, start_time)

    as_of_time_stamp = str(data["effective_datetime"].max())
    last_price = last_price.values[0][0]
    # Process data and generate GIFs
    #gif_paths = process_data(data, candlesticks, session_date, participant, strike_range, expiration, position_types)

    videos_paths = generate_video_task(data, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types,last_price ,'positioning')
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range, expiration, position_types,'positioning', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")


# @flow(name="DEX gifs")
# def DEX_flow(
#     session_date: Optional[date] = default_date,
#     strike_range: Optional[List[int]] = None,
#     expiration: Optional[str] = None,
#     participant: str = 'total_customers',
#     position_types: Optional[List[str]] = None,
#     webhook_url: str = DEV_CHANNEL
# ):
#     pass

@flow(name="GEX gifs")
def GEX_flow(
        session_date: Optional[date] = None,
        strike_range: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'mm',
        position_types: Optional[List[str]] = None,
        webhook_url: str = 'https://discord.com/api/webhooks/1277599354932428842/c2Ix3cPdLzI0fzxDdoGRye8nyKPLZj0dqmIxOiRQP2DYFx7YbgphUe8rAsWqkZUKiD0f'
                            #DEV_CHANNEL
):


    if session_date is None:
        #TODO: the latest effective_date of the book
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = STRIKE_RANGE
    if position_types is None:
        position_types = ['Net','C','P']
        #position_types = ['Net']

    if expiration is None:
        expiration = session_date

    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")
    # Fetch data
    metrics, candlesticks,last_price = fetch_data(session_date,None, strike_range, expiration, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())

    last_price = last_price.values[0][0]

    videos_paths = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types, last_price,"GEX")
    print(f"Video generated at: {videos_paths}")

    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range, expiration, position_types,'GEX', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")
    pass

@flow(name="All expirations GEX gifs")
def all_GEX_flow(
        session_date: Optional[date] = None,
        strike_range: Optional[List[int]] = None,
        expiration: Optional[str] = None,
        participant: str = 'mm',
        position_types: Optional[List[str]] = None,
        webhook_url: str = 'https://discord.com/api/webhooks/1281238194699898900/NKdEGh7PHeucQd7xItqxHWGgg9HwHHe8IQ0VRK5qAtPkc9fTrsPz0p_VVZcneZEl6wmH'
        #DEV_CHANNEL
):


    if session_date is None:
        #TODO: the latest effective_date of the book
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = STRIKE_RANGE
    if position_types is None:
        position_types = ['Net','C','P']
    if expiration is None:
        expiration = 'all'

    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '09:30:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:30:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")
    # Fetch data
    metrics, candlesticks,last_price = fetch_data(session_date,None, strike_range, None, start_time)
    as_of_time_stamp = str(metrics["effective_datetime"].max())

    last_price = last_price.values[0][0]

    videos_paths = generate_video_task(metrics, candlesticks, session_date, participant, strike_range, expiration,
                                       position_types, last_price, "GEX")
    print(f"Video generated at: {videos_paths}")
    breakpoint()
    # Send Discord message with Videos
    video_success = send_discord_message(videos_paths, as_of_time_stamp, session_date, participant, strike_range, expiration, position_types,'GEX', webhook_url)

    if video_success:
        print(f"Successfully processed and sent intraday data (GIF and video) for {session_date}")
    else:
        print(f"Failed to process or send intraday data for {session_date}")
    pass
#------------------ DEPTHVIEW ------------------#
class WebhookUrl(Enum):
    DEFAULT = 'https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
    URL_1 = 'https://discord.com/api/webhooks/your-webhook-url-1'
    URL_2 = 'https://discord.com/api/webhooks/your-webhook-url-2'
    # Add more webhook URLs as needed

@flow(name="Depthview flow")
def plot_depthview(
    session_date: Optional[date] = default_date,
    strike_range: Optional[List[int]] = None,
    expiration: Optional[str] = None,
    participant: str = 'mm',
    position_types: Optional[List[str]] = None,
    #webhook_url: str = 'https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
    webhook_url: WebhookUrl = WebhookUrl.DEFAULT
    ):

    print(f"Using webhook URL: {webhook_url.value}")

    # Replace the original Image.open(LOGO_dark) with:
    img = load_logo()

    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")
    strike_range = STRIKE_RANGE
    metric_type = "GEX"
    position_types = "all"

    metrics,candlesticks, last_price = fetch_data(session_date, "2024-09-09 14:00:00" ,strike_range, None,None)

    # Title formatting
    if metric_type == "GEX":
        dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Market Makers' GEX</span>"
        colorscale = "RdBu"
        title_colorbar = f"{metric_type}<br>(M$/point)"


    elif metric_type == "DEX":
        dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Customer DEX</span>"
        title_colorbar = f"{metric_type}<br>(δ)"

    elif metric_type == "position":
        dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Customer Position</span>"
        title_colorbar = f"{metric_type}<br>(contracts #)"


    if position_types == "calls":
        option_types_title = "Filtered for calls only"

    elif position_types == "puts":
        option_types_title = "Filtered for puts only"

    elif position_types == "all":
        option_types_title = "All contracts, puts and calls combined"



    # Ensure that the DataFrame values are floats
    metrics["metric"] = metrics[f"{participant}_posn"] * metrics['gamma']
    metrics["metric"] = metrics["metric"].astype(float)
    metrics['strike_price'] = metrics['strike_price'].astype(float)
    metrics['expiration_date_original'] = metrics['expiration_date_original'].astype(str)
    z = metrics.pivot_table(index='strike_price', columns='expiration_date_original', values="metric").values

    if metric_type != "GEX":
        colorscale = [
            [0.0, 'red'],  # Lowest value
            [0.5, 'white'],  # Mid value at zero
            [1.0, 'green'],  # Highest value
        ]
    else:
        print("Totoooooo")
    # Calculate the mid value for the color scale (centered at zero)
    # zmin = metrics[metric_type].min()
    # zmax = metrics[metric_type].max()

    zmin = metrics["metric"].min()
    zmax = metrics["metric"].max()
    val_range = max(abs(zmin), abs(zmax))

    y_min = math.floor(metrics['strike_price'].min() / 10) * 10
    y_max = math.ceil(metrics['strike_price'].max() / 10) * 10

    # Apply symmetric log scale transformation
    def symmetric_log_scale(value, log_base=10):
        return np.sign(value) * np.log1p(np.abs(value)) / np.log(log_base)

    z_log = symmetric_log_scale(z)


    # Round the original values for display in hover text
    rounded_z = np.around(z, decimals=2)


    # Create the heatmap using Plotly
    fig = go.Figure(data=go.Heatmap(
        z=z_log,
        x=metrics['expiration_date_original'].unique(),
        y=metrics['strike_price'].unique(),
        text=np.where(np.isnan(rounded_z), '', rounded_z),
        xgap=1,  # Add small gap between columns
        ygap=1,  # Add small gap between rows
        zsmooth='best',  # Smooth the colors
        texttemplate="%{text}",
        colorscale=colorscale,
        zmin=-symmetric_log_scale(val_range),
        zmax=symmetric_log_scale(val_range),
        colorbar=dict(
            title=title_colorbar,
            tickvals=symmetric_log_scale(np.array([-val_range, 0, val_range])),
            ticktext=[-round(val_range), 0, round(val_range)]
        ),
        zmid=0,
        hovertemplate='Expiration: %{x}<br>Strike: %{y}<br>' + metric_type + ': %{text}<extra></extra>'
    ))


    fig.update_layout(
        title=dict(
            text=(
                  f"{dynamic_title}"
                  # f"<br><span style='font-size:20px;'>As of {effective_datetime}</span>"
                  f"<br><span style='font-size:20px;'>{option_types_title}</span>"

            ),
            font=dict(family="Noto Sans SemiBold", color="white"),
            y=0.96,  # Adjust to control the vertical position of the title
            x=0.0,

            pad=dict(t=10, b=10, l=40)  # Adjust padding around the title
        ),


        margin=dict(l=40, r=40, t=130, b=30),  # Adjust overall margins
        xaxis=dict(
            title='Expiration Date',
            tickangle=-45,
            tickmode='linear',
            type='category'
        ),
        yaxis=dict(
            title='Strike Price',
            tickmode='linear',
            dtick=10
        ),
        font=dict(family="Noto Sans Medium", color='white'),
        autosize=True,
        plot_bgcolor='white',
        paper_bgcolor='#053061',  # Dark blue background

    )

    # Update layout with rounded y-axis range
    fig.update_layout(
        xaxis=dict(
            showgrid=False,
            ticks="",
            showline=False,
            zeroline=False,
            constrain="domain",
        ),
        yaxis=dict(
            showgrid=False,
            ticks="outside",
            # showline=True,
            zeroline=False,
            constrain="domain",
            #range=[y_min, y_max],  # Set the rounded range
            dtick=10,  # Set tick interval to 10
        ),
        plot_bgcolor='rgba(0,0,0,0)',
    )

    # Ensure heatmap cells align perfectly with axis ticks
    fig.update_traces(
        xaxis='x',
        yaxis='y'
    )

    fig.add_layout_image(
        dict(
            source=img,
            xref="paper",
            yref="paper",
            x=1,
            y=1.15,
            xanchor="right",
            yanchor="top",
            sizex=0.175,
            sizey=0.175,
            sizing="contain",
            layer="above"
        )
    )

    fig.update_layout(
        width=1920,  # Full HD width
        height=1080,  # Full HD height
        font=dict(size=16)  # Increase font size for better readability

    )

    # fig.show()
    breakpoint()

    title = f"📊 {session_date} Intraday Depthview"
    current_time = datetime.utcnow()
    # Define the Eastern Time zone
    eastern_tz = pytz.timezone('America/New_York')
    # Convert UTC time to Eastern Time
    eastern_time = current_time.replace(tzinfo=pytz.utc).astimezone(eastern_tz)
    # Format the time in a friendly way
    friendly_time = eastern_time.strftime("%B %d, %Y at %I:%M %p %Z")
    fields = [
        # {"name": "📈 Analysis Type", "value": "Intraday Gamma Heatmap", "inline": True},
        {"name": "⏰ As of:", "value": "2024-09-05 09:30:00", "inline": True},
    ]
    footer_text = f"Generated on {friendly_time} | By OptionsDepth Inc."

    # Prepare the embed
    embed = {
        "title": title,
        "color": 3447003,  # A nice blue color
        "fields": fields,
        "footer": {"text": footer_text},
        "image": {"url": "attachment://depthview.png"}  # Reference the attached image
    }

    # Convert Plotly figure to image bytes
    img_bytes = fig.to_image(format="png", scale=3)

    # Prepare the payload
    payload = {
        "embeds": [embed]
    }

    # Prepare the files dictionary
    files = {
        "payload_json": (None, json.dumps(payload), "application/json"),
        "file": ("depthview.png", img_bytes, "image/png")
    }

    # Send the request
    response = requests.post(webhook_url.value, files=files)

    if response.status_code == 200 or response.status_code == 204:
        print(f"Heatmap for {session_date} sent successfully to Discord!")
        fig.show()
        return True
    else:
        print(f"Failed to send heatmap. Status code: {response.status_code}")
        print(f"Response content: {response.content}")
        return False


#---------------- HEATMAP GIF -------------------- #

def generate_heatmap_gif(
    session_date: Optional[date] = default_date,
    webhook_url: str = 'https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
    ):
    pass

#------------ STATIC CHARTS ----------------------#
import pandas as pd
import pandas as pd
import plotly.graph_objects as go
import requests
import json
from io import BytesIO
def plot_options_data(df:pd.DataFrame,
                      aggregation:str,
                      strike_input: list,
                      expiration_input: str,
                      participant:str = "total_customers",
                      view:str ='both'):
    """
    Plots the options data based on specified aggregation and view.

    Parameters:
        df (pd.DataFrame): DataFrame containing options data with columns for 'expiration', 'type', 'strike', and 'net_position'.
        aggregation (str): 'strike' or 'expiration' to set the aggregation level.
        view (str): 'net', 'call', 'put', 'all', or 'both' to determine which data to show.
        strike_input (tuple): (min_strike, max_strike) to filter options by strike range.
        expiration_input (list): List of specific expirations to include.
    """
    x_axis_title = 'position'
    img = load_logo()

    print('-------------------------')
    print('----Ploting Function-----')
    print(f'strikes received by function: {strike_input} {type(strike_input)}')
    print(f'expirations received by function: {expiration_input} {type(expiration_input)}')

    print('-------------------------')

    # --------------------------- Strikes ---------------------------
    if strike_input  == "all":
        subtitle_strike= "All Strikes"

    # - list --> range
    if isinstance(strike_input, list):
        print("------------- range strikes -------------")

        df = df[(df['strike_price'] >= min(strike_input)) & (df['strike_price'] <= max(strike_input))]
        subtitle_strike=f"For range: {min(strike_input)} to {max(strike_input)} strikes"

    # - tuple --> list
    elif isinstance(strike_input, tuple):
        print("------------- list strikes -------------")
        df = df[df['strike_price'].isin(strike_input)]
        subtitle_strike=f"For the following strikes: {', '.join([str(item) for item in strike_input])}"

    # - string --> single
    if isinstance(strike_input, int):
        print("------------- single strike -------------")
        df = df[df['strike_price'] == strike_input]
        subtitle_strike=f"Strike: {strike_input} uniquely"

    # - string "all" --> everything # keep dataframe the same. define title string
    if expiration_input == "all":
        subtitle_expiration = "All Expirations"

    #---------------------------------- Expirations------------------------
    # - list --> range
    if isinstance(expiration_input, list):
        print("------------- range expirations -------------")

        print(type(expiration_input[0])) #string TODO fix to datetime
        df = df[df['expiration_date_original'].between(min(expiration_input), max(expiration_input))]
        subtitle_expiration=f"For {min(expiration_input).strftime('%Y-%m-%d')} to {max(expiration_input).strftime('%Y-%m-%d')} expirations range"

    # - tuple --> list
    elif isinstance(expiration_input, tuple):
        print("------------- list expirations -------------")
        df = df[df['expiration_date_original'].isin(expiration_input)]
        subtitle_expiration=f"For {', '.join([date.strftime('%Y-%m-%d') for date in expiration_input])} specific expirations"

    # - string --> single
    if isinstance(expiration_input, str):
        print("------------- single expiration -------------")
        date_format = '%Y-%m-%d'
        expiration_input_date_obj = datetime.strptime(expiration_input, date_format).date()
        df = df[df['expiration_date_original'] == expiration_input_date_obj]
        subtitle_expiration=f"For the {expiration_input} expiration"



    #-------------------------------------- plotting --------------------------------------
    # Aggregate data
    if aggregation == 'strike':
        grouped = df.groupby(['strike_price', 'call_put_flag']).agg({f'{participant}_posn': 'sum'}).unstack(fill_value=0)
        grouped.columns = grouped.columns.droplevel()
        orientation = 'h'
        yaxis_title = "Strike Value"
        xaxis_title = "Net Position"
        y_parameters = dict(
                showgrid=True,  # Show major gridlines
                gridcolor='#95a0ab',  # Color of major gridlines
                gridwidth=1,  # Width of major gridlines
                dtick=5,  # Major tick every 500
                tick0=0,  # Start ticks from 0
                showticklabels=True
            )
        x_parameters = None

    elif aggregation == "expiration":
        breakpoint()

    else:
        breakpoint()
    # Create figure
    fig = go.Figure()

    if view in ['call', 'put', 'all', 'both']:
        for col in ['C', 'P']:
            if view == 'all' or view == 'both' or view.lower() == col.lower():
                fig.add_trace(go.Bar(
                    name='Calls' if col == 'C' else 'Puts',
                    x=grouped.index if orientation == 'v' else grouped[col],
                    y=grouped[col] if orientation == 'v' else grouped.index,
                    orientation=orientation,
                    marker_color='SeaGreen' if col == 'C' else 'Red',
                    text=grouped[col],  # Text to display (values on the x-axis)
                    textposition='inside',
                    textfont=dict(  # Customize the font of the text
                        size=20,  # Font size
                    )
                    # Position the text outside the bar
                ))

    if view in ['net']:
        grouped['Net'] = grouped['C'] + grouped['P']
        fig.add_trace(go.Bar(
            name='Net',
            x=grouped.index if orientation == 'v' else grouped['Net'],
            y=grouped['Net'] if orientation == 'v' else grouped.index,
            orientation=orientation,
            marker_color='rgb(17,73,124)'
        ))

    fig.update_layout(
        title=f"SPX: Net Customer Options Positioning <br><sup>{subtitle_strike}<br>{subtitle_expiration}</sup>",
    )
    #------------------


    if y_parameters:
        fig.update_layout(yaxis=y_parameters)

    if x_parameters:
        fig.update_layout(xaxis=x_parameters)


    # Update layout
    fig.update_layout(
        title=dict(
            # text=(f"<b>Breakdown By Strike</b><br>"
            #       f"<sup>SPX: {participant_mapping.get(participant, 'Unknown Participant')} {position_type} {metric_to_compute} as of {timestamp}</sup><br>"
            #       f"<sup>{subtitle_strike} | {subtitle_expiration}</sup>"),
            text=(f"<b>Breakdown By Strike</b><br>"
                  f"<sup>SPX:  TOTO </sup><br>"
                  f"<sup>{subtitle_strike} | {subtitle_expiration}</sup>"),
            font=dict(family="Arial", size=24, color="black"),
            x=0.0,
            y=0.98,
            xanchor='left',
            yanchor='top'
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            groupclick="toggleitem"
        ),
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(family="Arial", color='black', size=12),
        xaxis=dict(
            title=f"{x_axis_title}",
            showgrid=True,
            gridcolor='lightgrey',
            zeroline=True,
            zerolinecolor='black',
            zerolinewidth=2
        ),
        yaxis=dict(
            title="Strike Price",
            showgrid=True,
            gridcolor='lightgrey',
            dtick=10
        ),
        width=1000,
        height=1200,
        margin=dict(l=50, r=50, t=100, b=50),
        images=[dict(
            source=img,
            xref="paper", yref="paper",
            x=0.15, y=0.6,
            sizex=0.75, sizey=0.75,
            sizing="contain",
            opacity=0.3,  # Increased opacity for better visibility
            layer="below"
        )] if img else []
    )

    # Add copyright sign at bottom left
    fig.add_annotation(
        text="© OptionsDepth Inc.",
        xref="paper", yref="paper",
        x=-0.05, y=-0.05,
        showarrow=False,
        font=dict(size=10, color="gray")
    )

    # Add "Powered by OptionsDepth inc." at bottom right
    fig.add_annotation(
        text="Powered by OptionsDepth Inc.",
        xref="paper", yref="paper",
        x=0.99, y=-0.05,
        showarrow=False,
        font=dict(size=10, color="gray"),
        xanchor="right"
    )

    fig.update_layout(
        width=1920,  # Full HD width
        height=1080,  # Full HD height
        font=dict(size=16)  # Increase font size for better readability

    )


    return fig

# @flow(name="Depthview flow")
# def plot_depthview(
#     session_date: Optional[date] = default_date,
#     strike_range: Optional[List[int]] = None,
#     expiration: Optional[str] = None,
#     participant: str = 'total_customers',
#     position_types: Optional[List[str]] = None,
#     webhook_url: str = 'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'
#     ):
#
#     current_time = datetime.now().time()
#
#     if current_time < time(12, 0):  # Before 12:00 PM
#         start_time = '07:00:00'
#     elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
#         start_time = '09:00:00'
#     else:  # 7:00 PM or later
#         start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case
#
#     print(f"Start time set to: {start_time}")
#
#
#     metrics, candlesticks, last_price = fetch_data(session_date, strike_range, expiration, start_time)
#
#     #title formatting
#     if type == "GEX":
#         dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Market Makers' GEX</span>"
#         colorscale = "RdBu"
#         title_colorbar = f"{type}<br>(M$/point)"
#
#     elif type == "DEX":
#         dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Customer DEX</span>"
#         title_colorbar = f"{type}<br>(δ)"
#
#     elif type == "position":
#         dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Customer Position</span>"
#         title_colorbar = f"{type}<br>(contracts #)"
#
#
#     if position_types == "calls":
#         option_types_title = "Filtered for calls only"
#
#     elif position_types == "puts":
#         option_types_title = "Filtered for puts only"
#
#     elif position_types == "all":
#         option_types_title = "All contracts, puts and calls combined"
#
#
#
#     # Ensure that the DataFrame values are floats
#     metrics[type] = metrics[type].astype(float)
#     metrics['strike_price'] = metrics['strike_price'].astype(float)
#     metrics['expiration_date_original'] = metrics['expiration_date_original'].astype(str)
#     z = metrics.pivot(index='strike_price', columns='expiration_date_original', values=type).values
#
#     if type != "GEX":
#         colorscale = [
#             [0.0, 'red'],  # Lowest value
#             [0.5, 'white'],  # Mid value at zero
#             [1.0, 'green'],  # Highest value
#         ]
#
#     # Calculate the mid value for the color scale (centered at zero)
#     zmin = metrics[type].min()
#     zmax = metrics[type].max()
#     val_range = max(abs(zmin), abs(zmax))
#
#
#
#     # Apply symmetric log scale transformation
#     def symmetric_log_scale(value, log_base=10):
#         return np.sign(value) * np.log1p(np.abs(value)) / np.log(log_base)
#
#     z_log = symmetric_log_scale(z)
#
#
#     # Round the original values for display in hover text
#     rounded_z = np.around(z, decimals=2)
#
#
#     # Create the heatmap using Plotly
#     fig = go.Figure(data=go.Heatmap(
#         z=z_log,
#         x=metrics['expiration_date_original'].unique(),
#         y=metrics['strike_price'].unique(),
#         text=np.where(np.isnan(rounded_z), '', rounded_z),
#         texttemplate="%{text}",
#         colorscale=colorscale,
#         zmin=-symmetric_log_scale(val_range),
#         zmax=symmetric_log_scale(val_range),
#         colorbar=dict(
#             title=title_colorbar,
#             tickvals=symmetric_log_scale(np.array([-val_range, 0, val_range])),
#             ticktext=[-round(val_range), 0, round(val_range)]
#         ),
#         zmid=0,
#         hovertemplate='Expiration: %{x}<br>Strike: %{y}<br>' + type + ': %{text}<extra></extra>'
#     ))
#
#
#     fig.update_layout(
#         title=dict(
#             text=(
#                   f"{dynamic_title}"
#                   f"<br><span style='font-size:20px;'>As of {effective_datetime}</span>"
#                   f"<br><span style='font-size:20px;'>{option_types_title}</span>"
#
#             ),
#             font=dict(family="Noto Sans SemiBold", color="white"),
#             y=0.96,  # Adjust to control the vertical position of the title
#             x=0.0,
#
#             # xanchor='left',
#             # yanchor='top',
#             pad=dict(t=10, b=10, l=40)  # Adjust padding around the title
#         ),
#         # width=width,
#         # height=height,
#
#         margin=dict(l=40, r=40, t=130, b=30),  # Adjust overall margins
#         xaxis=dict(
#             title='Expiration Date',
#             tickangle=-45,
#             tickmode='linear',
#             type='category'
#         ),
#         yaxis=dict(
#             title='Strike Price',
#             tickmode='linear',
#             dtick=10
#         ),
#         font=dict(family="Noto Sans Medium", color='white'),
#         autosize=True,
#         # paper_bgcolor='white',  # Set paper background to white
#         plot_bgcolor='white',
#         paper_bgcolor='#053061',  # Dark blue background
#
#     )
#
#     fig.add_layout_image(
#         dict(
#             source=img2,
#             xref="paper",
#             yref="paper",
#             x=1,
#             y=1.11,
#             xanchor="right",
#             yanchor="top",
#             sizex=0.175,
#             sizey=0.175,
#             sizing="contain",
#             layer="above"
#         )
#     )
#
#     fig.update_layout(
#         width=1920,  # Full HD width
#         height=1080,  # Full HD height
#         font=dict(size=16)  # Increase font size for better readability
#
#     )
#
#
#     title = f"📊 {session_date} Intraday DepthView"
#
#     # Define the Eastern Time zone, Convert UTC time to Eastern Time, then Format the time in a friendly way
#     current_time = datetime.utcnow()
#     eastern_tz = pytz.timezone('America/New_York')
#     eastern_time = current_time.replace(tzinfo=pytz.utc).astimezone(eastern_tz)
#     friendly_time = eastern_time.strftime("%B %d, %Y at %I:%M %p %Z")
#     fields = [
#         {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
#     ]
#     footer_text = f"Generated on {friendly_time} | By OptionsDepth Inc."
#
#     # Prepare the embed
#     embed = {
#         "title": title,
#         "color": 3447003,
#         "fields": fields,
#         "footer": {"text": footer_text},
#         "image": {"url": "attachment://depthview.png"}  # Reference the attached image
#     }
#
#     # Convert Plotly figure to image bytes
#     img_bytes = fig.to_image(format="png", scale=3)
#
#     # Prepare the payload
#     payload = {
#         # "content": "🚀[UPDATE]: New Gamma Heatmap analysis is ready!",
#         "embeds": [embed]
#     }
#
#     # Prepare the files dictionary
#     files = {
#         "payload_json": (None, json.dumps(payload), "application/json"),
#         "file": ("heatmap.png", img_bytes, "image/png")
#     }
#
#     # Send the request
#     response = requests.post(webhook_url, files=files)
#
#     if response.status_code == 200 or response.status_code == 204:
#         print(f"Heatmap for {session_date} sent successfully to Discord!")
#         return True
#     else:
#         print(f"Failed to send heatmap. Status code: {response.status_code}")
#         print(f"Response content: {response.content}")
#         return False



def generate_and_send_options_charts(df_metrics: pd.DataFrame =None,
                                     strike_range: list = None,
                                     expiration:str = None,
                                     session_date: str = '2024-09-13',
                                     participant: str = "total_customers",
                                     output_dir: str = 'temp_charts',
                                     webhook_url: str =DEV_CHANNEL,):

    expiration = '2024-09-16'
    #description = "TOTO EST EXCITEYYYYY"
    #content = "Je suis sous beeeef"

    # Set default values if not provided
    if session_date is None:
        session_date = datetime.now().strftime('%Y-%m-%d')
    if strike_range is None:
        #TODO: +/- 200 pts from SPOT Open
        strike_range = STRIKE_RANGE
    if expiration is None:
        #TODO: Next business day not day
        expiration = str(get_next_expiration_date(session_date))


    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")

    # Fetch data
    data, candlesticks, last_price = fetch_data(session_date, '2024-09-13 12:00:00',strike_range, expiration, start_time)

    as_of_time_stamp = str(data["effective_datetime"].max())
    last_price = last_price.values[0][0]


    # Create a unique temporary directory to store charts
    temp_dir = os.path.join(output_dir, f'charts_{uuid.uuid4().hex}')
    os.makedirs(temp_dir, exist_ok=True)

    # Generate Calls and Puts chart
    fig_calls_puts = plot_options_data(data, 'strike', strike_range, expiration, participant,view='both')
    # fig_calls_puts.show()

    # Generate Net chart
    fig_net = plot_options_data(data, 'strike', strike_range, expiration, participant,view='net')
    # fig_net.show()

    # Save the charts as image files
    calls_puts_path = os.path.join(temp_dir, f'calls_puts_{session_date}.png')
    net_path = os.path.join(temp_dir, f'net_{session_date}.png')

    fig_calls_puts.write_image(calls_puts_path, scale=2)
    fig_net.write_image(net_path, scale=2)

    print(f"Charts for {session_date} saved successfully!")
    print(f"Calls and Puts chart: {calls_puts_path}")
    print(f"Net chart: {net_path}")

    # Prepare the embed
    title = f"📊 {session_date} Options Positioning"
    fields = [
        {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
        {"name": "👥 Participant:", "value": participant, "inline": True}
    ]
    footer_text = f"Generated on {as_of_time_stamp} | By OptionsDepth Inc."

    file_paths = [calls_puts_path,net_path]

    if not isinstance(file_paths, list):
        file_paths = [file_paths]

    # First Request: Send the embedded message
    embed = {
        "title": title or "Options Chart Analysis",
        #"description": "Here's the latest options chart analysis.",
        "color": 3447003,  # A nice blue color
        "fields": fields or [],
        "footer": {"text": footer_text or "Generated by Options Analysis Bot"},
        "timestamp": datetime.utcnow().isoformat()
    }

    # Prepare the payload with the embed
    payload = {
        #"embeds": [embed],
        #"content": content or ""  # Ensure content is initialized
    }

    # Send the first request (embed only)
    # response = requests.post(webhook_url, json=payload)
    #
    # print(response)
    #
    # if response.status_code == 200 or response.status_code == 204:
    #     print("Embedded message sent successfully to Discord!")
    # else:
    #     print(f"Failed to Embedded message. Status code: {response.status_code}")
    #     print(f"Response content: {response.content}")
    #     breakpoint()

    # Second Request: Send the GIFs separately
    files = {}
    for i, file_path in enumerate(file_paths):
        with open(file_path, 'rb') as f:
            file_content = f.read()
        files[f"file{i}"] = (os.path.basename(file_path), file_content, "image/gif")

    # Send the second request (GIFs only)
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200 or response.status_code == 204:
        print("GIFs sent successfully to Discord!")
    else:
        print(f"Failed to send GIFs. Status code: {response.status_code}")
        print(f"Response content: {response.content}")

    return response.status_code == 200



if __name__ == "__main__":
    #zero_dte_flow()
    #one_dte_flow()
    #GEX_flow()
    #plot_depthview(webhook_url=WebhookUrl.DEFAULT)
    #generate_heatmap_gif()
    #all_GEX_flow()
    i=0
    while i < 20:
        i+=1
        generate_and_send_options_charts()