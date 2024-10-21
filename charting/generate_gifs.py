import uuid
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from prefect import task, flow, get_run_logger,get_client
#3from imageio.plugins import ffmpeg
from plotly.subplots import make_subplots
import imageio.v2 as imageio
from datetime import date, datetime
import colorsys
from config.config import *
import requests
import json
import logging
# Load the image file
from PIL import Image
import tempfile
import base64
from io import BytesIO
import requests
import json
from datetime import datetime
import cv2
import imageio_ffmpeg
import ffmpeg
import cv2
import numpy as np
from PIL import Image
import os
import uuid
from pathlib import Path
import imageio
import subprocess
import numpy as np
import os

import os
import tempfile
import urllib.parse
import requests
import mimetypes
from typing import List, Optional
from prefect import get_run_logger
from datetime import datetime

import requests
import json
from datetime import datetime
from prefect import get_run_logger
import os
import mimetypes
import tempfile
import urllib.parse
from typing import List, Optional
import platform
from pathlib import Path
from utilities.misc_utils import *
from utilities.db_utils import *
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

## Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and set it for the handler
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(file_formatter)

# Add the handler to the logger
logger.addHandler(console_handler)

participant_mapping = {
    'mm': 'Market Makers',
    'broker': 'Brokers and Dealers',
    'firm': 'Firms',
    'nonprocus': 'Non-Professional Customers',
    'procust': 'Professional Customers',
    'total_customers': 'Total Customers'
}

def generate_frame_wrapper(args):
    timestamp, index, generate_frame_partial = args
    fig = generate_frame_partial(timestamp)
    frame_path = f'temp_frames/frame_{index:03d}.png'
    fig.write_image(frame_path)
    return index, frame_path

def process_single_strike(group, participant, metric, last_price=1):
    date = group['effective_date'].iloc[0]
    strike = group['strike_price'].iloc[0]
    results = {}

    for flag in ['C', 'P']:
        flag_group = group[group['call_put_flag'] == flag].sort_values('effective_datetime')
        if not flag_group.empty:
            if metric == "GEX":
                metric_values = flag_group[f'{participant}_posn'] * flag_group['gamma'] * float(last_price) * 100
            elif metric == "DEX":
                metric_values = flag_group[f'{participant}_posn'] * flag_group['delta']
            else:
                metric_values = flag_group[f'{participant}_posn']

            datetimes = flag_group['effective_datetime']
            highest_idx = metric_values.idxmax()
            lowest_idx = metric_values.idxmin()

            results[flag] = {
                'start_of_day': {'value': metric_values.iloc[0], 'time': datetimes.iloc[0]},
                'current': {'value': metric_values.iloc[-1], 'time': datetimes.iloc[-1]},
                'lowest': {'value': metric_values.min(), 'time': datetimes[lowest_idx]},
                'highest': {'value': metric_values.max(), 'time': datetimes[highest_idx]},
                'prior_update': {
                    'value': metric_values.iloc[-2] if len(metric_values) > 1 else metric_values.iloc[0],
                    'time': datetimes.iloc[-2] if len(datetimes) > 1 else datetimes.iloc[0]
                }
            }

    # Calculate net metric
    results['Net'] = {}
    for key in set(results.get('C', {}).keys()) | set(results.get('P', {}).keys()):
        c_value = results.get('C', {}).get(key, {}).get('value', 0)
        p_value = results.get('P', {}).get(key, {}).get('value', 0)

        c_value = 0 if math.isnan(c_value) else c_value
        p_value = 0 if math.isnan(p_value) else p_value

        net_value = c_value + p_value

        c_time = results.get('C', {}).get(key, {}).get('time')
        p_time = results.get('P', {}).get(key, {}).get('time')

        net_time = max(filter(None, [c_time, p_time])) if c_time or p_time else None

        results['Net'][key] = {
            'value': net_value,
            'time': net_time
        }

    return {'date': date, 'strike_price': strike, **results}

def generate_color_shades(base_color, num_shades=5):
    """Generate different shades of a given color."""


    # Convert hex to RGB
    rgb = tuple(int(base_color.lstrip('#')[i:i + 2], 16) for i in (0, 2, 4))

    # Convert RGB to HSV
    hsv = colorsys.rgb_to_hsv(rgb[0] / 255.0, rgb[1] / 255.0, rgb[2] / 255.0)

    # Generate shades by varying the value (brightness)
    shades = []
    for i in range(num_shades):
        v = max(0.3, hsv[2] - 0.15 * i)  # Adjust this formula to get desired shades
        rgb = colorsys.hsv_to_rgb(hsv[0], hsv[1], v)
        shades.append(f'rgb({int(rgb[0] * 255)},{int(rgb[1] * 255)},{int(rgb[2] * 255)})')

    return shades

def generate_color_scale(base_color, is_positive):
    rgb = tuple(int(base_color.lstrip('#')[i:i + 2], 16) for i in (0, 2, 4))
    hsv = colorsys.rgb_to_hsv(rgb[0] / 255.0, rgb[1] / 255.0, rgb[2] / 255.0)

    if is_positive:
        v = min(1, hsv[2] * 1.3)  # Increase brightness for positive values
    else:
        v = max(0, hsv[2] * 0.7)  # Decrease brightness for negative values

    rgb = colorsys.hsv_to_rgb(hsv[0], hsv[1], v)
    return f'rgb({int(rgb[0] * 255)},{int(rgb[1] * 255)},{int(rgb[2] * 255)})'



def generate_frame(data, candlesticks, timestamp, participant, strike_input, expiration_input, position_type,metric_to_compute,last_price,
                   full_img_path):


    if metric_to_compute == 'GEX':
        x_axis_title = "Notional Gamma (M$)"
    elif metric_to_compute == 'DEX':
        x_axis_title = "Delta Exposure"
    else:
        x_axis_title = "Position"

    #print(f'This is the full_img_path passed to generate_frame: {full_img_path}')

    try:
        img = Image.open(full_img_path)
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode()
        img_src = f"data:image/png;base64,{img_str}"

    except Exception as e:
        print(f"Error loading image: {e}")
        img_src = None


    metrics_data = data[data['effective_datetime'] <= timestamp].copy()

    # Check if candlesticks is not None and has the required column
    if candlesticks is not None and 'effective_datetime' in candlesticks.columns:
        candlesticks_data = candlesticks[candlesticks['effective_datetime'] == timestamp].copy()
    else:
        print("Candlesticks data is missing or does not contain 'effective_datetime' column. SPX Spot Price line will not be added.")
        candlesticks_data = pd.DataFrame()

    # Apply strike and expiration filters
    if strike_input != "all":
        if isinstance(strike_input, (list, tuple)):
            metrics_data = metrics_data[metrics_data['strike_price'].between(min(strike_input), max(strike_input))]
            subtitle_strike = f"For strikes: {min(strike_input)} to {max(strike_input)}"
        elif isinstance(strike_input, int):
            metrics_data = metrics_data[metrics_data['strike_price'] == strike_input]
            subtitle_strike = f"For strike: {strike_input}"
    else:
        subtitle_strike = "All Strikes"

    #--------- Expiration ------------ #
    if expiration_input != "all":
        if isinstance(expiration_input, (list, tuple)):
            metrics_data = metrics_data[metrics_data['expiration_date_original'].isin(expiration_input)]
            subtitle_expiration = f"For expirations: {', '.join(str(exp) for exp in expiration_input)}"
            breakpoint()
        elif isinstance(expiration_input, date):
            metrics_data = metrics_data[metrics_data['expiration_date_original'] == expiration_input]
            subtitle_expiration = f"For expiration: {expiration_input}"

        else:

            # Ensure expiration_input is a string in 'YYYY-MM-DD' format
            expiration_input = pd.to_datetime(expiration_input).strftime('%Y-%m-%d')
            # Convert the expiration_date_original column to datetime, then back to string in 'YYYY-MM-DD' format
            metrics_data['expiration_date_original'] = pd.to_datetime(metrics_data['expiration_date_original']).dt.strftime(
                '%Y-%m-%d')

            metrics_data = metrics_data[metrics_data['expiration_date_original'] == expiration_input]
            subtitle_expiration = f"For expiration: {expiration_input}"


    else:
        subtitle_expiration = "All Expirations"


    # Group by strike price
    grouped = metrics_data.groupby('strike_price')

    # Process each group
    results = [process_single_strike(group, participant, metric_to_compute,last_price) for _, group in grouped]

    # Convert results to DataFrame and sort by strike price
    results_df = pd.DataFrame(results).sort_values('strike_price', ascending=True)

    # Create figure
    fig = make_subplots(rows=1, cols=1)

    position_types = ['C', 'P', 'Net'] if position_type == 'All' else [position_type]

    #TODO: config_file
    colors = {
        'Net': {
            'negative': 'rgb(0,149,255)',  # light blue
            'positive': 'rgb(0,149,255)'  # light blue

        },
        'C': {
            'negative': 'rgb(0,217,51)',  # dark green
            'positive': 'rgb(0,217,51)'  # light green
        },
        'P': {
            'negative': 'rgb(204,3,0)',  # dark red
            'positive': 'rgb(204,3,0)'  # light red
        }
    }

    for pos_type in position_types:
        if pos_type in results_df.columns:
            update_period = ['current', 'start_of_day', 'prior_update']
            symbols = ['line-ns', 'circle', 'x-thin']


            for position, symbol in zip(update_period, symbols):
                def safe_extract(x, key):
                    if isinstance(x, dict) and key in x:
                        return x[key]['value'] if isinstance(x[key], dict) else x[key]
                    return None

                x_values = results_df[pos_type].apply(lambda x: safe_extract(x, position))
                valid_mask = x_values.notnull()

                if position == 'current':
                    positive_mask = x_values > 0
                    negative_mask = x_values <= 0

                    # Positive values
                    trace_positive = go.Bar(
                        x=x_values[valid_mask & positive_mask],
                        y=results_df['strike_price'][valid_mask & positive_mask],
                        name=f'{pos_type} {position.capitalize().replace("_", " ")} (Positive)',
                        orientation='h',
                        marker_color=colors[pos_type]['positive'],

                        opacity=1,
                        legendgroup=pos_type,
                        legendgrouptitle_text=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask & positive_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace_positive)

                    # Negative values
                    trace_negative = go.Bar(
                        x=x_values[valid_mask & negative_mask],
                        y=results_df['strike_price'][valid_mask & negative_mask],
                        name=f'{pos_type} {position.capitalize().replace("_", " ")} (Negative)',
                        orientation='h',
                        marker_color=colors[pos_type]['negative'],
                        opacity=1,
                        legendgroup=pos_type,
                        legendgrouptitle_text=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask & negative_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace_negative)
                else:
                    # For non-current update_period, use the same color scheme as before
                    trace = go.Scatter(
                        x=x_values[valid_mask],
                        y=results_df['strike_price'][valid_mask],
                        mode='markers',
                        name=f'{pos_type} {position.capitalize().replace("_", " ")}',
                        # marker=dict(symbol=symbol, size=10, color='black'),
                        marker=dict(
                            symbol=symbol,
                            size=10,  # Increased size for better visibility
                            color='black',
                            line=dict(width=2, color='black'),  # Add a border to make it more visible
                        ),
                        legendgroup=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace)

            # Add horizontal line for range (lowest to highest) for each strike
            for _, row in results_df.iterrows():
                strike = row['strike_price']
                data = row[pos_type]
                if isinstance(data, dict):
                    min_value = min(safe_extract(data, 'lowest'), safe_extract(data, 'highest'))
                    max_value = max(safe_extract(data, 'lowest'), safe_extract(data, 'highest'))
                    if min_value is not None and max_value is not None:
                        fig.add_shape(
                            type="line",
                            x0=min_value,
                            x1=max_value,
                            y0=strike,
                            y1=strike,
                            line=dict(
                                color=colors[pos_type]['positive'],
                                width=2.5,
                                dash="solid",  # Options: "solid", "dot", "dash", "longdash", "dashdot", "longdashdot"
                            ),
                            opacity=1,
                        )

    # Add the horizontal line for SPX Spot Price if 'close' exists in candlesticks_data
    if 'close' in candlesticks_data.columns and not candlesticks_data['close'].empty:
        spx_spot_price = candlesticks_data['close'].iloc[0]

        x_min = min(trace.x.min() for trace in fig.data if hasattr(trace, 'x') and len(trace.x) > 0)
        x_max = max(trace.x.max() for trace in fig.data if hasattr(trace, 'x') and len(trace.x) > 0)

        # Add some padding to the right of the chart
        x_padding = (x_max - x_min) * 0.25  # 10% of the x-axis range
        new_x_max = x_max + x_padding
        new_x_min = x_min - x_padding
        # Update the x-axis range
        fig.update_xaxes(range=[new_x_min, new_x_max])

        fig.add_shape(
            type="line",
            x0=x_min,
            x1=new_x_max,
            y0=spx_spot_price,
            y1=spx_spot_price,
            line=dict(color="black", width=2, dash="dot"),  # Changed to dotted line

            name="SPX Spot Price"
        )

        # Add annotation for SPX Spot Price in the padded area
        fig.add_annotation(
            x=new_x_max,
            y=spx_spot_price,
            text=f"SPX Spot<br><b>{spx_spot_price:.2f}</b>",
            #text=f"SPX Spot Price:\n{spx_spot_price:.2f}",
            showarrow=False,
            xanchor="right",
            yanchor="middle",
            bgcolor="grey",  # Changed background color to grey
            bordercolor="black",
            borderpad=6,  # Increased padding between text and border
            #borderRadius=10,  # Added rounded corners
            borderwidth=1,
            font=dict(color="white", size=16),  # Changed text color to white
        )


    # Update layout
    fig.update_layout(
        title=dict(
            text=(f"<b>Breakdown By Strike</b><br>"
                  f"<sup>SPX: {participant_mapping.get(participant, 'Unknown Participant')} {position_type} {metric_to_compute} as of {timestamp}</sup><br>"
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
            source=img_src,
            xref="paper", yref="paper",
            x=0.15, y=0.8,
            sizex=0.65, sizey=0.65,
            sizing="contain",
            opacity=0.25,  # Increased opacity for better visibility
            layer="below"
        )] if img_src else []
    )

    # Add copyright sign at bottom left
    fig.add_annotation(
        text="© OptionsDepth.com",
        xref="paper", yref="paper",
        x=-0.05, y=-0.05,
        showarrow=False,
        font=dict(size=11, color="gray")
    )

    # Add "Powered by OptionsDepth inc." at bottom right
    fig.add_annotation(
        text="Powered by OptionsDepth.com",
        xref="paper", yref="paper",
        x=0.99, y=-0.05,
        showarrow=False,
        font=dict(size=11, color="gray"),
        xanchor="right"
    )


    return fig

def generate_frame_last(data, candlesticks, timestamp, participant, strike_input, expiration_input, position_type,metric_to_compute,last_price,
                   full_img_path):


    if metric_to_compute == 'GEX':
        x_axis_title = "Notional Gamma (M$)"
    elif metric_to_compute == 'DEX':
        x_axis_title = "Delta Exposure"
    else:
        x_axis_title = "Position"

    #print(f'This is the full_img_path passed to generate_frame: {full_img_path}')

    # Improved cross-platform image loading with error handling and path information
    img_src = None
    try:
        # Get the absolute path of the script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Go up one level to the project root
        project_root = os.path.dirname(script_dir)
        # Construct the full path to the image using os.path.join for cross-platform compatibility
        full_img_path = os.path.normpath(os.path.join(project_root, 'config', 'images', os.path.basename(full_img_path)))

        print(f"Attempting to load image from: {full_img_path}")
        print(f"Current platform: {platform.system()}")

        if os.path.isfile(full_img_path):
            with Image.open(full_img_path) as img:
                buffered = BytesIO()
                img.save(buffered, format="PNG")
                img_str = base64.b64encode(buffered.getvalue()).decode()
                img_src = f"data:image/png;base64,{img_str}"
            print(f"Image loaded successfully from: {full_img_path}")
        else:
            print(f"Image file not found at: {full_img_path}")
            print(f"Current working directory: {os.getcwd()}")
            print(f"Contents of {os.path.dirname(full_img_path)}:")
            print(os.listdir(os.path.dirname(full_img_path)))

            # Check for case-sensitive file systems (mainly for macOS)
            if platform.system() == "Darwin":  # macOS
                lower_case_files = [f.lower() for f in os.listdir(os.path.dirname(full_img_path))]
                if os.path.basename(full_img_path).lower() in lower_case_files:
                    print(
                        "Note: The image file might exist with a different case. macOS is case-insensitive but case-preserving.")
    except Exception as e:
        print(f"Error loading image: {e}")
        print(f"Attempted to load from: {full_img_path}")
        print(f"Current working directory: {os.getcwd()}")


    metrics_data = data[data['effective_datetime'] <= timestamp].copy()

    # Check if candlesticks is not None and has the required column
    if candlesticks is not None and 'effective_datetime' in candlesticks.columns:
        candlesticks_data = candlesticks[candlesticks['effective_datetime'] == timestamp].copy()
    else:
        print("Candlesticks data is missing or does not contain 'effective_datetime' column. SPX Spot Price line will not be added.")
        candlesticks_data = pd.DataFrame()

    # Apply strike and expiration filters
    if strike_input != "all":
        if isinstance(strike_input, (list, tuple)):
            metrics_data = metrics_data[metrics_data['strike_price'].between(min(strike_input), max(strike_input))]
            subtitle_strike = f"For strikes: {min(strike_input)} to {max(strike_input)}"
        elif isinstance(strike_input, int):
            metrics_data = metrics_data[metrics_data['strike_price'] == strike_input]
            subtitle_strike = f"For strike: {strike_input}"
    else:
        subtitle_strike = "All Strikes"

    #--------- Expiration ------------ #
    if expiration_input != "all":
        if isinstance(expiration_input, (list, tuple)):
            metrics_data = metrics_data[metrics_data['expiration_date_original'].isin(expiration_input)]
            subtitle_expiration = f"For expirations: {', '.join(str(exp) for exp in expiration_input)}"
            breakpoint()
        elif isinstance(expiration_input, date):
            metrics_data = metrics_data[metrics_data['expiration_date_original'] == expiration_input]
            subtitle_expiration = f"For expiration: {expiration_input}"

        else:

            # Ensure expiration_input is a string in 'YYYY-MM-DD' format
            expiration_input = pd.to_datetime(expiration_input).strftime('%Y-%m-%d')
            # Convert the expiration_date_original column to datetime, then back to string in 'YYYY-MM-DD' format
            metrics_data['expiration_date_original'] = pd.to_datetime(metrics_data['expiration_date_original']).dt.strftime(
                '%Y-%m-%d')

            metrics_data = metrics_data[metrics_data['expiration_date_original'] == expiration_input]
            subtitle_expiration = f"For expiration: {expiration_input}"


    else:
        subtitle_expiration = "All Expirations"


    # Group by strike price
    grouped = metrics_data.groupby('strike_price')

    # Process each group
    results = [process_single_strike(group, participant, metric_to_compute,last_price) for _, group in grouped]

    # Convert results to DataFrame and sort by strike price
    results_df = pd.DataFrame(results).sort_values('strike_price', ascending=True)

    # Create figure
    fig = make_subplots(rows=1, cols=1)

    position_types = ['C', 'P', 'Net'] if position_type == 'All' else [position_type]

    #TODO: config_file
    colors = {
        'Net': {
            'negative': 'rgb(0,149,255)',  # light blue
            'positive': 'rgb(0,149,255)'  # light blue

        },
        'C': {
            'negative': 'rgb(0,217,51)',  # dark green
            'positive': 'rgb(0,217,51)'  # light green
        },
        'P': {
            'negative': 'rgb(204,3,0)',  # dark red
            'positive': 'rgb(204,3,0)'  # light red
        }
    }

    for pos_type in position_types:
        if pos_type in results_df.columns:
            update_period = ['current', 'start_of_day', 'prior_update']
            symbols = ['line-ns', 'circle', 'x-thin']


            for position, symbol in zip(update_period, symbols):
                def safe_extract(x, key):
                    if isinstance(x, dict) and key in x:
                        return x[key]['value'] if isinstance(x[key], dict) else x[key]
                    return None

                x_values = results_df[pos_type].apply(lambda x: safe_extract(x, position))
                valid_mask = x_values.notnull()

                if position == 'current':
                    positive_mask = x_values > 0
                    negative_mask = x_values <= 0

                    # Positive values
                    trace_positive = go.Bar(
                        x=x_values[valid_mask & positive_mask],
                        y=results_df['strike_price'][valid_mask & positive_mask],
                        name=f'{pos_type} {position.capitalize().replace("_", " ")} (Positive)',
                        orientation='h',
                        marker_color=colors[pos_type]['positive'],

                        opacity=1,
                        legendgroup=pos_type,
                        legendgrouptitle_text=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask & positive_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace_positive)

                    # Negative values
                    trace_negative = go.Bar(
                        x=x_values[valid_mask & negative_mask],
                        y=results_df['strike_price'][valid_mask & negative_mask],
                        name=f'{pos_type} {position.capitalize().replace("_", " ")} (Negative)',
                        orientation='h',
                        marker_color=colors[pos_type]['negative'],
                        opacity=1,
                        legendgroup=pos_type,
                        legendgrouptitle_text=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask & negative_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace_negative)
                else:
                    # For non-current update_period, use the same color scheme as before
                    trace = go.Scatter(
                        x=x_values[valid_mask],
                        y=results_df['strike_price'][valid_mask],
                        mode='markers',
                        name=f'{pos_type} {position.capitalize().replace("_", " ")}',
                        # marker=dict(symbol=symbol, size=10, color='black'),
                        marker=dict(
                            symbol=symbol,
                            size=10,  # Increased size for better visibility
                            color='black',
                            line=dict(width=2, color='black'),  # Add a border to make it more visible
                        ),
                        legendgroup=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace)

            # Add horizontal line for range (lowest to highest) for each strike
            for _, row in results_df.iterrows():
                strike = row['strike_price']
                data = row[pos_type]
                if isinstance(data, dict):
                    min_value = min(safe_extract(data, 'lowest'), safe_extract(data, 'highest'))
                    max_value = max(safe_extract(data, 'lowest'), safe_extract(data, 'highest'))
                    if min_value is not None and max_value is not None:
                        fig.add_shape(
                            type="line",
                            x0=min_value,
                            x1=max_value,
                            y0=strike,
                            y1=strike,
                            line=dict(
                                color=colors[pos_type]['positive'],
                                width=2.5,
                                dash="solid",  # Options: "solid", "dot", "dash", "longdash", "dashdot", "longdashdot"
                            ),
                            opacity=1,
                        )

    # Add the horizontal line for SPX Spot Price if 'close' exists in candlesticks_data
    if 'close' in candlesticks_data.columns and not candlesticks_data['close'].empty:
        spx_spot_price = candlesticks_data['close'].iloc[0]

        x_min = min(trace.x.min() for trace in fig.data if hasattr(trace, 'x') and len(trace.x) > 0)
        x_max = max(trace.x.max() for trace in fig.data if hasattr(trace, 'x') and len(trace.x) > 0)

        # Add some padding to the right of the chart
        x_padding = (x_max - x_min) * 0.33  # 10% of the x-axis range
        new_x_max = x_max + x_padding
        new_x_min = x_min - x_padding
        # Update the x-axis range
        fig.update_xaxes(range=[new_x_min, new_x_max])

        fig.add_shape(
            type="line",
            x0=x_min,
            x1=new_x_max,
            y0=spx_spot_price,
            y1=spx_spot_price,
            line=dict(color="black", width=2, dash="dot"),  # Changed to dotted line
            #line=dict(color="black", width=4, dash="dash"),
            name="SPX Spot Price"
        )

        # Add annotation for SPX Spot Price in the padded area
        fig.add_annotation(
            x=new_x_max,
            y=spx_spot_price,
            text=f"SPX Spot<br><b>{spx_spot_price:.2f}</b>",
            #text=f"SPX Spot Price:\n{spx_spot_price:.2f}",
            showarrow=False,
            xanchor="right",
            yanchor="middle",
            bgcolor="grey",  # Changed background color to grey
            bordercolor="black",
            borderpad=6,  # Increased padding between text and border
            #borderRadius=10,  # Added rounded corners
            borderwidth=1,
            font=dict(color="white", size=16),  # Changed text color to white
        )


    # Update layout
    fig.update_layout(
        title=dict(
            text=(f"<b>Breakdown By Strike</b><br>"
                  f"<sup>SPX: {participant_mapping.get(participant, 'Unknown Participant')} {position_type} {metric_to_compute} as of {timestamp}</sup><br>"
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
            source=img_src,
            xref="paper", yref="paper",
            x=0.15, y=0.8,
            sizex=0.65, sizey=0.65,
            sizing="contain",
            opacity=0.25,  # Increased opacity for better visibility
            layer="below"
        )] if img_src else []
    )

    # Add copyright sign at bottom left
    fig.add_annotation(
        text="© OptionsDepth.com",
        xref="paper", yref="paper",
        x=-0.05, y=-0.05,
        showarrow=False,
        font=dict(size=11, color="gray")
    )

    # Add "Powered by OptionsDepth inc." at bottom right
    fig.add_annotation(
        text="Powered by OptionsDepth.com",
        xref="paper", yref="paper",
        x=0.99, y=-0.05,
        showarrow=False,
        font=dict(size=11, color="gray"),
        xanchor="right"
    )

    # Instead of saving to a local directory, save to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmpfile:
        fig.write_image(tmpfile.name, scale=3)


    return fig, tmpfile.name

@task(name= 'Generate frame')
def generate_frame_new(data, candlesticks, timestamp, participant, strike_input, expiration_input, position_type, metric_to_compute, last_price, full_img_path):
    prefect_logger = get_run_logger()
    if metric_to_compute == 'GEX':
        x_axis_title = "Notional Gamma (M$)"
    elif metric_to_compute == 'DEX':
        x_axis_title = "Delta Exposure"
    else:
        x_axis_title = "Position"

    img_src = None
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        full_img_path = os.path.normpath(os.path.join(project_root, 'config', 'images', os.path.basename(full_img_path)))

        print(f"Attempting to load image from: {full_img_path}")
        print(f"Current platform: {platform.system()}")

        if os.path.isfile(full_img_path):
            with Image.open(full_img_path) as img:
                buffered = BytesIO()
                img.save(buffered, format="PNG")
                img_str = base64.b64encode(buffered.getvalue()).decode()
                img_src = f"data:image/png;base64,{img_str}"
            prefect_logger.info(f"Image loaded successfully from: {full_img_path}")
        else:
            prefect_logger.info(f"Image file not found at: {full_img_path}")
            prefect_logger.info(f"Current working directory: {os.getcwd()}")
            prefect_logger.info(f"Contents of {os.path.dirname(full_img_path)}:")
            prefect_logger.info(os.listdir(os.path.dirname(full_img_path)))

            if platform.system() == "Darwin":
                lower_case_files = [f.lower() for f in os.listdir(os.path.dirname(full_img_path))]
                if os.path.basename(full_img_path).lower() in lower_case_files:
                    prefect_logger.info("Note: The image file might exist with a different case. macOS is case-insensitive but case-preserving.")
    except Exception as e:
        prefect_logger.info(f"Error loading image: {e}")
        prefect_logger.info(f"Attempted to load from: {full_img_path}")
        prefect_logger.info(f"Current working directory: {os.getcwd()}")

    metrics_data = data[data['effective_datetime'] <= timestamp].copy()

    if candlesticks is not None and 'effective_datetime' in candlesticks.columns:
        candlesticks_data = candlesticks[candlesticks['effective_datetime'] == timestamp].copy()
    else:
        prefect_logger.info("Candlesticks data is missing or does not contain 'effective_datetime' column. SPX Spot Price line will not be added.")
        candlesticks_data = pd.DataFrame()

    if strike_input != "all":
        if isinstance(strike_input, (list, tuple)):
            metrics_data = metrics_data[metrics_data['strike_price'].between(min(strike_input), max(strike_input))]
            subtitle_strike = f"For strikes: {min(strike_input)} to {max(strike_input)}"
        elif isinstance(strike_input, int):
            metrics_data = metrics_data[metrics_data['strike_price'] == strike_input]
            subtitle_strike = f"For strike: {strike_input}"
    else:
        subtitle_strike = "All Strikes"

    if expiration_input != "all":
        if isinstance(expiration_input, (list, tuple)):
            metrics_data = metrics_data[metrics_data['expiration_date_original'].isin(expiration_input)]
            subtitle_expiration = f"For expirations: {', '.join(str(exp) for exp in expiration_input)}"
        elif isinstance(expiration_input, date):
            metrics_data = metrics_data[metrics_data['expiration_date_original'] == expiration_input]
            subtitle_expiration = f"For expiration: {expiration_input}"
        else:
            expiration_input = pd.to_datetime(expiration_input).strftime('%Y-%m-%d')
            metrics_data['expiration_date_original'] = pd.to_datetime(metrics_data['expiration_date_original']).dt.strftime('%Y-%m-%d')
            metrics_data = metrics_data[metrics_data['expiration_date_original'] == expiration_input]
            subtitle_expiration = f"For expiration: {expiration_input}"
    else:
        subtitle_expiration = "All Expirations"

    grouped = metrics_data.groupby('strike_price')
    results = [process_single_strike(group, participant, metric_to_compute, last_price) for _, group in grouped]
    results_df = pd.DataFrame(results).sort_values('strike_price', ascending=True)

    fig = make_subplots(rows=1, cols=1)

    position_types = ['C', 'P', 'Net'] if position_type == 'All' else [position_type]

    colors = {
        'Net': {
            'negative': 'rgb(0,149,255)',
            'positive': 'rgb(0,149,255)'
        },
        'C': {
            'negative': 'rgb(0,217,51)',
            'positive': 'rgb(0,217,51)'
        },
        'P': {
            'negative': 'rgb(204,3,0)',
            'positive': 'rgb(204,3,0)'
        }
    }

    def safe_extract(x, key):
        if isinstance(x, dict) and key in x:
            return x[key]['value'] if isinstance(x[key], dict) else x[key]
        return None

    x_min = float('inf')
    x_max = float('-inf')

    for pos_type in position_types:
        if pos_type in results_df.columns:
            update_period = ['current', 'start_of_day', 'prior_update']
            symbols = ['line-ns', 'circle', 'x-thin']

            for position, symbol in zip(update_period, symbols):
                x_values = results_df[pos_type].apply(lambda x: safe_extract(x, position))
                valid_mask = x_values.notnull()

                if position == 'current':
                    positive_mask = x_values > 0
                    negative_mask = x_values <= 0

                    trace_positive = go.Bar(
                        x=x_values[valid_mask & positive_mask],
                        y=results_df['strike_price'][valid_mask & positive_mask],
                        name=f'{pos_type} {position.capitalize().replace("_", " ")} (Positive)',
                        orientation='h',
                        marker_color=colors[pos_type]['positive'],
                        opacity=1,
                        legendgroup=pos_type,
                        legendgrouptitle_text=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask & positive_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace_positive)

                    trace_negative = go.Bar(
                        x=x_values[valid_mask & negative_mask],
                        y=results_df['strike_price'][valid_mask & negative_mask],
                        name=f'{pos_type} {position.capitalize().replace("_", " ")} (Negative)',
                        orientation='h',
                        marker_color=colors[pos_type]['negative'],
                        opacity=1,
                        legendgroup=pos_type,
                        legendgrouptitle_text=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask & negative_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace_negative)

                else:
                    trace = go.Scatter(
                        x=x_values[valid_mask],
                        y=results_df['strike_price'][valid_mask],
                        mode='markers',
                        name=f'{pos_type} {position.capitalize().replace("_", " ")}',
                        marker=dict(
                            symbol=symbol,
                            size=10,
                            color='black',
                            line=dict(width=2, color='black'),
                        ),
                        legendgroup=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                    fig.add_trace(trace)

            for _, row in results_df.iterrows():
                strike = row['strike_price']
                data = row[pos_type]
                if isinstance(data, dict):
                    min_value = safe_extract(data, 'lowest')
                    max_value = safe_extract(data, 'highest')
                    if min_value is not None and max_value is not None:
                        fig.add_shape(
                            type="line",
                            x0=min_value,
                            x1=max_value,
                            y0=strike,
                            y1=strike,
                            line=dict(
                                color=colors[pos_type]['positive'],
                                width=1,  # Thinner line for the wick
                                dash="solid",
                            ),
                            opacity=0.5,  # Semi-transparent
                        )
                        x_min = min(x_min, min_value)
                        x_max = max(x_max, max_value)

    # Add some padding to the x-axis range
    x_padding = (x_max - x_min) * 0.1  # 10% padding on each side
    x_min -= x_padding
    x_max += x_padding

    # Update the x-axis range
    fig.update_xaxes(range=[x_min, x_max])

    if 'close' in candlesticks_data.columns and not candlesticks_data['close'].empty:
        spx_spot_price = candlesticks_data['close'].iloc[0]

        fig.add_shape(
            type="line",
            x0=x_min,
            x1=x_max,
            y0=spx_spot_price,
            y1=spx_spot_price,
            line=dict(color="black", width=2, dash="dot"),
            name="SPX Spot Price"
        )

        fig.add_annotation(
            x=x_max,
            y=spx_spot_price,
            text=f"SPX Spot<br><b>{spx_spot_price:.2f}</b>",
            showarrow=False,
            xanchor="right",
            yanchor="middle",
            bgcolor="grey",
            bordercolor="black",
            borderpad=6,
            borderwidth=1,
            font=dict(color="white", size=16),
        )

    fig.update_layout(
        title=dict(
            text=(f"<b>Breakdown By Strike</b><br>"
                  f"<sup>SPX: {participant_mapping.get(participant, 'Unknown Participant')} {position_type} {metric_to_compute} as of {timestamp}</sup><br>"
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
            source=img_src,
            xref="paper", yref="paper",
            x=0.15, y=0.8,
            sizex=0.65, sizey=0.65,
            sizing="contain",
            opacity=0.25,
            layer="below"
        )] if img_src else []
    )

    fig.add_annotation(
        text="© OptionsDepth.com",
        xref="paper", yref="paper",
        x=-0.05, y=-0.05,
        showarrow=False,
        font=dict(size=11, color="gray")
    )

    fig.add_annotation(
        text="Powered by OptionsDepth.com",
        xref="paper", yref="paper",
        x=0.99, y=-0.05,
        showarrow=False,
        font=dict(size=11, color="gray"),
        xanchor="right"
    )

    with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmpfile:
        fig.write_image(tmpfile.name, scale=3)

    return fig, tmpfile.name

def generate_gif(data,candlesticks, session_date, participant_input, position_type_input, strike_input, expiration_input,last_price,
                 metric = "positioning",
                 img_path='config/images/logo_dark.png',
                 output_gif='animated_chart.gif'):

    # Get the project root directory
    project_root = Path(__file__).parent.parent

    # Construct the full path to the image
    full_img_path = project_root / img_path


    # Get unique timestamps
    timestamps = data['effective_datetime'].unique()

    # Create a unique temporary directory to store frames
    temp_dir = f'temp_frames_{uuid.uuid4().hex}'
    os.makedirs(temp_dir, exist_ok=True)

    # Generate frames
    frames = []

    for i, timestamp in enumerate(timestamps):
        print(f"Generating Graph for {timestamp} - {position_type_input} - {participant_input}")
        fig = generate_frame(data,candlesticks, timestamp, participant_input, strike_input, expiration_input, position_type_input,
                             full_img_path, metric, last_price)


        # Save the frame as an image
        frame_path = os.path.join(temp_dir, f'frame_{i:03d}.png')
        fig.write_image(frame_path)
        frames.append(imageio.imread(frame_path))

    last_frame_path = os.path.join(temp_dir, f'last_frame_{position_type_input}_{timestamps[-1]}.png')
    fig.write_image(last_frame_path)

    # Add the last frame one more time to pause at the end
    frames.append(imageio.imread(frame_path))

    # Create the GIF
    imageio.mimsave(output_gif, frames, fps=1.5)

    # # Clean up temporary files
    # for file in os.listdir(temp_dir):
    #     os.remove(os.path.join(temp_dir, file))
    # os.rmdir(temp_dir)
    #
    # print(f"Animation saved as {output_gif}")
    # return output_gif
    # Clean up temporary files
    for file in os.listdir(temp_dir):
        if file != 'last_frame.png':
            os.remove(os.path.join(temp_dir, file))

    print(f"Animation saved as {output_gif}")
    print(f"Last frame saved as {last_frame_path}")
    return output_gif, last_frame_path

def generate_discord_compatible_video(input_video_path, output_video_path):
    """
    Convert the input video to a Discord-compatible format.
    """
    try:
        (
            ffmpeg
            .input(input_video_path)
            .output(output_video_path, vcodec='libx264', acodec='aac', **{'b:v': '1M', 'b:a': '128k'})
            # .output(output_video_path,
            #         vcodec='libx264',
            #         acodec='aac',
            #         vf='scale=1280:-2',  # Ensure width is 1280, height auto
            #         video_bitrate='5M',
            #         audio_bitrate='384k',
            #         r=30,  # Frame rate
            #         pix_fmt='yuv420p',  # Pixel format
            #         movflags='+faststart'
            #         )
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        print(f"Successfully converted video to Discord-compatible format: {output_video_path}")
        return output_video_path
    except ffmpeg.Error as e:
        print(f"Error occurred while converting video: {e.stderr.decode()}")
        return None


def get_image_path(img_path='config/images/position_0DTE_exp.png'):
    # List of possible base directories to check
    base_dirs = [
        Path(__file__).resolve().parent.parent,  # Two levels up from the script
        Path(__file__).resolve().parent,         # One level up from the script
        Path.cwd(),                              # Current working directory
        Path.home(),                             # User's home directory
    ]

    for base_dir in base_dirs:
        full_img_path = base_dir / img_path
        print(f"Checking for image at: {full_img_path}")
        if full_img_path.exists():
            print(f"Image found at: {full_img_path}")
            return str(full_img_path)

    print("Image not found in any of the expected locations.")
    print(f"Script location: {Path(__file__).resolve()}")
    print(f"Current working directory: {os.getcwd()}")
    return None


def generate_video(data, candlesticks, session_date, participant_input, position_type_input, strike_input, expiration_input,
                   metric,last_price,
                   img_path='config/images/logo_dark.png',
                   output_video='None.mp4'):

    full_img_path = get_image_path(img_path)

    if full_img_path is None:
        print("Warning: Proceeding without background image.")

    timestamps = data['effective_datetime'].unique()

    # Create a unique temporary directory to store frames
    temp_dir = f'temp_frames_{uuid.uuid4().hex}'
    os.makedirs(temp_dir, exist_ok=True)

    # Generate frames
    frame_paths = []

    for i, timestamp in enumerate(timestamps):
        print(f"Generating Graph for {timestamp} - {position_type_input} - {participant_input}")

        fig = generate_frame(data, candlesticks, timestamp, participant_input, strike_input, expiration_input, position_type_input,
                             metric,last_price,
                             full_img_path)

        # Save the frame as an image
        frame_path = os.path.join(temp_dir, f'frame_{i:03d}.png')

        fig.write_image(frame_path,scale= 3)
        frame_paths.append(frame_path)

    # Use the frame_paths directly, no need for additional processing
    file_paths = frame_paths

    # Create the writer with the correct output path
    temp_output = f'temp_{output_video}'
    writer = imageio.get_writer(temp_output, fps=3)

    # Read and write images
    for file_path in file_paths:
        image = imageio.imread(file_path)
        writer.append_data(image)

    print('Finished writing temporary video')
    writer.close()

    # Convert to Discord-compatible format
    final_output = generate_discord_compatible_video(temp_output, output_video)


    # Save the last frame separately
    last_frame_path = os.path.join(temp_dir, f'last_frame_{position_type_input}_{timestamps[-1]}.png')
    fig.write_image(last_frame_path)


    if final_output:
        print(f"Video saved as {final_output}")
        return final_output, last_frame_path
    else:
        print("Failed to generate Discord-compatible video")
        return None

def generate_video_new(data, candlesticks, session_date, participant_input, position_type_input, strike_input, expiration_input,
                   metric,last_price,
                   img_path='config/images/logo_dark.png',
                   output_video='None.mp4'):

    full_img_path = get_image_path(img_path)

    if full_img_path is None:
        print("Warning: Proceeding without background image.")

    timestamps = data['effective_datetime'].unique()

    # Create a unique temporary directory to store frames
    temp_dir = f'temp_frames_{uuid.uuid4().hex}'
    os.makedirs(temp_dir, exist_ok=True)

    # Generate frames
    frame_paths = []

    for i, timestamp in enumerate(timestamps):
        print(f"Generating Graph for {timestamp} - {position_type_input} - {participant_input}")

        fig = generate_frame(data, candlesticks, timestamp, participant_input, strike_input, expiration_input, position_type_input,
                             metric,last_price,
                             full_img_path)

        # Save the frame as an image
        frame_path = os.path.join(temp_dir, f'frame_{i:03d}.png')

        fig.write_image(frame_path,scale= 3)
        frame_paths.append(frame_path)

    # Use the frame_paths directly, no need for additional processing
    file_paths = frame_paths

    # Create the writer with the correct output path
    temp_output = f'temp_{output_video}'
    writer = imageio.get_writer(temp_output, fps=3)

    # Read and write images
    for file_path in file_paths:
        image = imageio.imread(file_path)
        writer.append_data(image)

    print('Finished writing temporary video')
    writer.close()

    # Convert to Discord-compatible format
    final_output = generate_discord_compatible_video(temp_output, output_video)

    # # Clean up temporary files
    # for file in frame_paths:
    #     os.remove(file)
    # os.rmdir(temp_dir)
    # os.remove(temp_output)

    # Save the last frame separately
    last_frame_path = os.path.join(temp_dir, f'last_frame_{position_type_input}_{timestamps[-1]}.png')
    fig.write_image(last_frame_path)




    if final_output:
        print(f"Video saved as {final_output}")
        return final_output, last_frame_path
    else:
        print("Failed to generate Discord-compatible video")
        return None


def generate_snapshot(data, candlesticks, session_date, participant_input, position_type_input, strike_input, expiration_input,
                   metric,last_price,
                   img_path='config/images/logo_dark.png',
                   output_video='None.mp4'):


    # Get the project root directory
    project_root = Path(__file__).parent.parent

    # Construct the full path to the image
    full_img_path = project_root / img_path

    # Get unique timestamps
    timestamp = data['effective_datetime'].max()

    # Create a unique temporary directory to store frames
    temp_dir = f'snapshot_{uuid.uuid4().hex}'
    os.makedirs(temp_dir, exist_ok=True)

    # Generate frames
    frame_paths = []


    fig = generate_frame(data, candlesticks, timestamp, participant_input, strike_input, expiration_input, position_type_input,
                         metric,last_price,
                         full_img_path)

    # Save the frame as an image
    frame_path = os.path.join(temp_dir, f'frame_{metric}_{timestamp}.png')
    fig.write_image(frame_path)
    frame_paths.append(frame_path)

    # Use the frame_paths directly, no need for additional processing
    file_paths = frame_paths

    # Create the writer with the correct output path
    temp_output = f'temp_{output_video}'
    writer = imageio.get_writer(temp_output, fps=3)

    # Read and write images
    for file_path in file_paths:
        image = imageio.imread(file_path)
        writer.append_data(image)

    print('Finished writing temporary video')
    writer.close()

    # Convert to Discord-compatible format
    final_output = generate_discord_compatible_video(temp_output, output_video)

    # Clean up temporary files
    for file in frame_paths:
        os.remove(file)
    os.rmdir(temp_dir)
    os.remove(temp_output)

    if final_output:
        print(f"Video saved as {final_output}")
        return final_output
    else:
        print("Failed to generate Discord-compatible video")
        return None

# Assuming DigitalOceanSpaces and SpacesStatus are imported or defined elsewhere

def sort_file_paths(file_paths):
    def extract_number(filename):
        return int(''.join(filter(str.isdigit, filename)))

    return sorted(file_paths, key=lambda x: extract_number(os.path.basename(x)))



def send_to_discord_new(webhook_url: str, file_paths_or_urls: List[dict], content: Optional[str] = None,
                        title: Optional[str] = None, description: Optional[str] = None,
                        fields: Optional[List[dict]] = None, footer_text: Optional[str] = None,
                        do_spaces: DigitalOceanSpaces = None, space_name: str = INTRADAYBOT_SPACENAME):
    """
    Sends a message to Discord with an embedded message first, followed by files in a separate message.

    This function performs the following tasks:
    1. Sends an embedded message to a Discord channel using the provided webhook URL.
    2. Downloads files from either DigitalOcean Spaces or direct URLs.
    3. Sends the downloaded files as a separate message to the Discord channel.
    4. Cleans up temporary files after successful sending.

    The function is designed to handle potential failures at various stages:
    - If sending the embedded message fails, it logs the error and returns False.
    - If downloading a file fails, it logs the error and continues with the next file.
    - If sending the files fails, it logs the error and returns False.
    - It uses a temporary directory to manage downloaded files, which is automatically cleaned up.
    - It attempts to remove individual temporary files after sending, handling potential errors.

    Args:
        webhook_url (str): The Discord webhook URL to send messages to.
        file_paths_or_urls (List[dict]): A list of dictionaries containing file URLs or paths.
        content (Optional[str]): Additional text content for the message.
        title (Optional[str]): Title for the embedded message.
        description (Optional[str]): Description for the embedded message.
        fields (Optional[List[dict]]): Fields for the embedded message.
        footer_text (Optional[str]): Footer text for the embedded message.
        do_spaces (Optional[DigitalOceanSpaces]): DigitalOcean Spaces client for file downloads.
        space_name (str): Name of the DigitalOcean Space to use.

    Returns:
        bool: True if the message and files were sent successfully, False otherwise.
    """
    prefect_logger = get_run_logger()

    # Ensure file_paths_or_urls is a list
    if not isinstance(file_paths_or_urls, list):
        file_paths_or_urls = [file_paths_or_urls]

    # Prepare the embed for the Discord message
    embed = {
        "title": title or "Options Chart Analysis",
        "description": description or "Here's the latest options chart analysis.",
        "color": 3447003,  # A nice blue color
        "fields": fields or [],
        "footer": {"text": footer_text or "Generated by Options Analysis Bot"},
        "timestamp": datetime.utcnow().isoformat()
    }

    # Prepare the payload with the embed
    payload = {
        "embeds": [embed],
        "content": content or ""
    }


    # Send the embedded message to Discord
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        prefect_logger.info("Embedded message sent successfully to Discord!")
        print("Embedded message sent successfully to Discord!")
    except requests.exceptions.RequestException as e:
        error_message = f"Failed to send embedded message. Error: {e}"
        print(error_message)
        prefect_logger.error(error_message)
        return False

    # Process and send files
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Temporary directory created at: {tmpdir}")
        local_file_paths = []

        # Download files from URLs or DigitalOcean Spaces
        for result_dict in file_paths_or_urls:
            for position_type, urls in result_dict.items():
                for url_type, url in urls.items():
                    if url.startswith(('http://', 'https://')):
                        # Extract the object key from the URL
                        parsed_url = urllib.parse.urlparse(url)
                        object_key = parsed_url.path.lstrip('/')
                        local_path = os.path.join(tmpdir, f"{position_type}_{url_type}_{os.path.basename(object_key)}")

                        try:
                            # Download file using DigitalOcean Spaces or direct HTTP request
                            if do_spaces:
                                success = do_spaces.download_from_spaces(space_name, object_key, local_path)
                                if not success:
                                    raise Exception("Failed to download file from DigitalOcean Spaces")
                            else:
                                response = requests.get(url)
                                response.raise_for_status()
                                with open(local_path, 'wb') as f:
                                    f.write(response.content)

                            local_file_paths.append(local_path)
                            print(f"Downloaded file: {local_path}")
                            prefect_logger.info(f"Downloaded file: {local_path}")
                        except Exception as e:
                            error_message = f"Failed to download file from {url}. Error: {e}"
                            print(error_message)
                            prefect_logger.error(error_message)
                            continue

        # Sort the file paths to ensure consistent order
        sorted_file_paths = sort_file_paths(local_file_paths)

        # Prepare files for sending to Discord
        files = {}
        for i, file_path in enumerate(sorted_file_paths):
            with open(file_path, 'rb') as f:
                file_content = f.read()
            mime_type, _ = mimetypes.guess_type(file_path)
            files[f"file{i}"] = (os.path.basename(file_path), file_content, mime_type or 'application/octet-stream')

        # Send files to Discord
        try:
            response = requests.post(webhook_url, files=files)
            response.raise_for_status()
            prefect_logger.info("Files sent successfully to Discord!")
            print("Files sent successfully to Discord!")

            # Clean up temporary files
            for file_path in sorted_file_paths:
                try:
                    os.remove(file_path)
                    print(f"Removed temporary file: {file_path}")
                except FileNotFoundError:
                    print(f"File not found, skipping removal: {file_path}")
                except Exception as e:
                    print(f"Error removing file {file_path}: {e}")

            return True
        except requests.exceptions.RequestException as e:
            error_message = f"Failed to send files. Error: {e}"
            print(error_message)
            prefect_logger.error(error_message)
            return False

    print(f"Temporary directory removed: {tmpdir}")

def send_to_discord(webhook_url, file_paths, content=None, title=None, description=None, fields=None,
                    footer_text=None):
    """
    Sends a message to Discord with an embedded message first, followed by GIFs in a separate message.
    :param webhook_url: Discord webhook URL
    :param file_paths: List of file paths for GIFs
    :param content: Optional content text
    :param title: Title for the embed
    :param description: Description for the embed
    :param fields: Fields for the embed
    :param footer_text: Footer text for the embed
    :return: Status code of the second request
    """
    prefect_logger = get_run_logger()
    if not isinstance(file_paths, list):
        file_paths = [file_paths]

    # First Request: Send the embedded message
    embed = {
        "title": title or "Options Chart Analysis",
        "description": description or "Here's the latest options chart analysis.",
        "color": 3447003,  # A nice blue color
        "fields": fields or [],
        "footer": {"text": footer_text or "Generated by Options Analysis Bot"},
        "timestamp": datetime.utcnow().isoformat()
    }

    # Prepare the payload with the embed
    payload = {
        "embeds": [embed],
        "content": content or ""  # Ensure content is initialized
    }

    try:
        # response = requests.post(webhook_url, data=json.dumps(payload))#, headers=headers)
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()  # This will raise an exception for HTTP errors
        prefect_logger.info("Embedded message sent successfully to Discord!")
        print("Embedded message sent successfully to Discord!")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send embedded message. Error: {e}")
        print(f"Response content: {response.content if response else 'No response'}")
        prefect_logger.error(f"Failed to send embedded message. Error: {e}")
        prefect_logger.error(f"Response content: {response.content if response else 'No response'}")
        return False

    # # Send the first request (embed only)
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
    sorted_file_paths = sort_file_paths(file_paths)

    # Now use the sorted paths to create the files dictionary
    files = {}
    for i, file_path in enumerate(sorted_file_paths):
        with open(file_path, 'rb') as f:
            file_content = f.read()
        files[f"file{i}"] = (os.path.basename(file_path), file_content, "image/gif")

    # # Second Request: Send the GIFs separately
    # files = {}
    # for i, file_path in enumerate(file_paths):
    #     with open(file_path, 'rb') as f:
    #         file_content = f.read()
    #     files[f"file{i}"] = (os.path.basename(file_path), file_content, "image/gif")

    # Send the second request (GIFs only)
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200 or response.status_code == 204:
        print("GIFs sent successfully to Discord!")
        prefect_logger.info("GIFs sent successfully to Discord!")
    else:
        print(f"Failed to send GIFs. Status code: {response.status_code}")
        print(f"Response content: {response.content}")
        prefect_logger.error(f"Failed to send GIFs. Status code: {response.status_code}")
        prefect_logger.error(f"Response content: {response.content}")
    return response.status_code == 200

#---------------------#
def generate_and_send_gif(data, session_date, participant, position_type , strike_input, expiration,last_price,webhook_url):
    gif_path = generate_gif(
        data,
        session_date,
        participant_input = participant,
        position_type_input=position_type,
        strike_input=strike_input,
        expiration_input=expiration,
        last_price= last_price
        )



    if gif_path is None:
        print(f"Failed to generate GIF for {session_date} with strike input {strike_input}")
        return False

    participant_mapping = {
        'mm': 'Market Makers',
        'broker': 'Brokers and Dealers',
        'firm': 'Firms',
        'nonprocus': 'Non-Professional Customers',
        'procust': 'Professional Customers',
        'total_customers': 'Total Customers'
    }

    # Usage example:
    participant_text = participant_mapping.get(participant, 'Unknown Participant')

    title = f"📊{session_date} Intraday Recap."
    description = (
        f"Detailed analysis of {participant_text} positions for the {session_date} session.\n"
        f"This chart provides insights into market movements and positioning within the specified strike range."
    )
    fields = [
        {"name": "👥 Participant(s)", "value": participant_text, "inline": True},
        {"name": "🎯 Strike Range", "value": f"{strike_input[0]} - {strike_input[1]}", "inline": True},
        {"name": "📅 Expiration(s)", "value": session_date, "inline": True},
        {"name": "📈 Analysis Type", "value": "Intraday Positional Movement", "inline": True},
        {"name": "🕒 Time Range", "value": "Market Hours", "inline": True},
        {"name": "🔄 Update Frequency", "value": "10 minutes", "inline": True},
        #{"name": "📊 Data Points", "value": str(len(data)), "inline": False},
        {"name": "💡 Interpretation", "value": (
            "• Green bars indicate Calls positioning\n"
            "• Red bars indicate Puts positioning\n"
            "• Blue bars indicate Net positioning\n"
            "\n"
            "• ■ represents current position magnitude\n"
            "• ● represents the start of day position magnitude\n"
            "• ✖ represents the prior update position magnitude\n"
        ), "inline": False},
    ]
    footer_text = f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by OptionsDepth"

    #TODO: add different content depending on the time of the day
    content = "🚀 New options chart analysis is ready! Check out the latest market insights below."


    success = send_to_discord(
        webhook_url,
        gif_path,
        content={content},
        title=title,
        description=description,
        fields=fields,
        footer_text=footer_text
    )

    os.remove(gif_path)  # Clean up the gif file
    return success


def generate_and_send_video(data, session_date, participant, position_type , strike_input, expiration,metric,last_price,webhook_url):
    video_path = generate_video(
        data,
        session_date,
        participant_input = participant,
        position_type_input=position_type,
        strike_input=strike_input,
        expiration_input=expiration,
        metric=metric,
        last_price=last_price)



    if video_path is None:
        print(f"Failed to generate video for {session_date} with strike input {strike_input}")
        return False

    participant_mapping = {
        'mm': 'Market Makers',
        'broker': 'Brokers and Dealers',
        'firm': 'Firms',
        'nonprocus': 'Non-Professional Customers',
        'procust': 'Professional Customers',
        'total_customers': 'Total Customers'
    }

    # Usage example:
    participant_text = participant_mapping.get(participant, 'Unknown Participant')

    title = f"📊{session_date} Intraday Recap."
    description = (
        f"Detailed analysis of {participant_text} positions for the {session_date} session.\n"
    )
    fields = [
        {"name": "👥 Participant(s)", "value": participant_text, "inline": True},
        {"name": "🎯 Strike Range", "value": f"{strike_input[0]} - {strike_input[1]}", "inline": True},
        {"name": "📅 Expiration(s)", "value": session_date, "inline": True},
        {"name": "📈 Analysis Type", "value": "Intraday Positional Movement", "inline": True},
        {"name": "🕒 Time Range", "value": "Market Hours", "inline": True},
        {"name": "🔄 Update Frequency", "value": "10 minutes", "inline": True},
        #{"name": "📊 Data Points", "value": str(len(data)), "inline": False},
        {"name": "💡 Interpretation", "value": (
            "• Green bars indicate Calls positioning\n"
            "• Red bars indicate Puts positioning\n"
            "• Blue bars indicate Net positioning\n"
            "\n"
            "• ■ represents current position magnitude\n"
            "• ● represents the start of day position magnitude\n"
            "• ✖ represents the prior update position magnitude\n"
        ), "inline": False},
    ]
    footer_text = f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by OptionsDepth"

    #TODO: add different content depending on the time of the day
    content = "🚀 New options chart analysis is ready! Check out the latest market insights below."


    success = send_to_discord(
        webhook_url,
        video_path,
        content={content},
        title=title,
        description=description,
        fields=fields,
        footer_text=footer_text
    )

    os.remove(video_path)  # Clean up the gif file
    return success
