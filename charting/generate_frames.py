import uuid
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
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
import numpy as np
import concurrent.futures
from functools import partial
import os
import shutil
from utilities.misc_utils import *

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

def process_single_strike_original(group, participant):
    #TODO: Investigate Net


    date = group['effective_date'].iloc[0]
    strike = group['strike_price'].iloc[0]

    results = {}
    for flag in ['C', 'P']:
        flag_group = group[group['call_put_flag'] == flag].sort_values('effective_datetime')
        if not flag_group.empty:
            metric = flag_group[f'{participant}_posn']
            datetimes = flag_group['effective_datetime']

            highest_idx = metric.idxmax()
            lowest_idx = metric.idxmin()

            results[flag] = {
                'start_of_day': {'value': metric.iloc[0], 'time': datetimes.iloc[0]},
                'current': {'value': metric.iloc[-1], 'time': datetimes.iloc[-1]},
                'lowest': {'value': metric.min(), 'time': datetimes[lowest_idx]},
                'highest': {'value': metric.max(), 'time': datetimes[highest_idx]},
                'prior_update': {'value': metric.iloc[-2] if len(metric) > 1 else metric.iloc[0],
                                 'time': datetimes.iloc[-2] if len(metric) > 1 else datetimes.iloc[0]}
            }

    # Calculate net metric
    if 'C' in results and 'P' in results:
        results['Net'] = {
            key: {'value': results['C'][key]['value'] + results['P'][key]['value'],
                  'time': max(results['C'][key]['time'], results['P'][key]['time'])}
            for key in results['C']
        }

    return {'date': date, 'strike_price': strike, **results}

def process_single_strike(group, participant, metric, last_price=1):

    date = group['effective_date'].iloc[0]
    strike = group['strike_price'].iloc[0]
    results = {}

    for flag in ['C', 'P']:
        flag_group = group[group['call_put_flag'] == flag].sort_values('effective_datetime')
        if not flag_group.empty:
            if metric == "GEX":

                metric_values = flag_group[f'{participant}_posn'] * flag_group['gamma'] * float(last_price) * 100  #/ 1000000
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
    # if 'C' in results and 'P' in results:
    #     results['Net'] = {
    #         key: {
    #             'value': results['C'][key]['value'] + results['P'][key]['value'],
    #             'time': max(results['C'][key]['time'], results['P'][key]['time'])
    #         } for key in results['C']
    #     }
    import math

    # Calculate net metric
    results['Net'] = {}
    for key in set(results.get('C', {}).keys()) | set(results.get('P', {}).keys()):
        c_value = results.get('C', {}).get(key, {}).get('value', 0)
        p_value = results.get('P', {}).get(key, {}).get('value', 0)

        # Handle NaN values
        c_value = 0 if math.isnan(c_value) else c_value
        p_value = 0 if math.isnan(p_value) else p_value

        net_value = c_value + p_value

        c_time = results.get('C', {}).get(key, {}).get('time')
        p_time = results.get('P', {}).get(key, {}).get('time')

        # Use the available time, or None if both are missing
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

def generate_frame_wrapper(args):
    timestamp, index, generate_frame_partial,position_type, temp_dir = args
    fig = generate_frame_partial(timestamp=timestamp)
    #frame_path = temp_dir #f'temp_frames_{position_type}/frame_{index:03d}.png'
    # Use os.path.join to combine temp_dir with the filename
    frame_path = os.path.join(temp_dir, f'frame_{index:03d}.png')
    print(f'frame path: {frame_path}')
    fig.write_image(frame_path, scale=3)
    return index, frame_path



def parallel_frame_generation_(data, candlesticks, timestamps, participant, strike_input, expiration_input,
                              position_type, metric, last_price, full_img_path, temp_dir, max_workers=None):
    generate_frame_partial = partial(
        generate_frame,
        data=data,
        candlesticks=candlesticks,
        participant=participant,
        strike_input=strike_input,
        expiration_input=expiration_input,
        position_type=position_type,
        metric_to_compute=metric,
        last_price=last_price,
        full_img_path=full_img_path
    )

    args_list = [(timestamp, i, generate_frame_partial,position_type) for i, timestamp in enumerate(timestamps)]

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(generate_frame_wrapper, args_list))

    # Sort results to maintain order
    results.sort(key=lambda x: x[0])
    frame_paths = [path for _, path in results]

    return frame_paths

def parallel_frame_generation(data, candlesticks, timestamps, participant, strike_input, expiration_input,
                              position_type, metric, last_price, full_img_path, temp_dir, max_workers=None):


    generate_frame_partial = partial(
        generate_frame,
        data=data,
        candlesticks=candlesticks,
        participant=participant,
        strike_input=strike_input,
        expiration_input=expiration_input,
        position_type=position_type,
        metric_to_compute=metric,
        last_price=last_price,
        full_img_path=full_img_path
    )

    args_list = [(timestamp, i, generate_frame_partial, position_type,temp_dir) for i, timestamp in enumerate(timestamps)]

    frame_paths = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_args = {executor.submit(generate_frame_wrapper, args): args for args in args_list}
        for future in concurrent.futures.as_completed(future_to_args):
            args = future_to_args[future]
            try:
                index, path = future.result()
                frame_paths.append((index, path))
                print(f"Generated frame {index} for {position_type}")
            except Exception as e:
                print(f"Error generating frame for {args[0]}: {str(e)}")

    # Sort results to maintain order
    frame_paths.sort(key=lambda x: x[0])
    return [path for _, path in frame_paths]

# def generate_frame(data, candlesticks, timestamp, participant, strike_input, expiration_input, position_type,metric_to_compute,last_price,
#                    full_img_path):
def generate_frame(data, candlesticks, timestamp, participant, strike_input, expiration_input, position_type,
                   metric_to_compute, last_price, full_img_path):

    # ... rest of the function ...
    if metric_to_compute == 'GEX':
        x_axis_title = "Notional Gamma (M$)"
    elif metric_to_compute == 'DEX':
        x_axis_title = "Delta Exposure"
    else:
        x_axis_title = "Position"


    try:
        img = Image.open(full_img_path)
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode()
        img_src = f"data:image/png;base64,{img_str}"
        #print(f"Image loaded successfully from {full_img_path}")
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
            #'negative': 'rgb(0,0,130)',    # dark blue
            'negative': 'rgb(0,149,255)',  # light blue
            'positive': 'rgb(0,149,255)'  # light blue

        },
        'C': {
            # 'negative': 'rgb(50,168,82)',   # dark green
            # 'positive': 'rgb(48,199,40)',  # light green
            'negative': 'rgb(0,217,51)',  # dark green
            'positive': 'rgb(0,217,51)'  # light green
        },
        'P': {
            # 'negative': 'rgb(160,0,0)',   # dark red
            # 'positive': 'rgb(255,0,0)'   # light red
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
            x=0.15, y=0.6,
            sizex=0.75, sizey=0.75,
            sizing="contain",
            opacity=0.3,  # Increased opacity for better visibility
            layer="below"
        )] if img_src else []
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


    return fig


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
        #print(f"Generating Graph for {timestamp} - {position_type_input} - {participant_input}")
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


def generate_video_(data, candlesticks, session_date, participant_input, position_type_input, strike_input, expiration_input,
                   metric,last_price,
                   img_path='config/images/logo_dark.png',
                   output_video='None.mp4'):


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
    frame_paths = []

    frame_paths = parallel_frame_generation(
        data, candlesticks, timestamps, participant_input, strike_input, expiration_input, position_type_input,
        metric, last_price, full_img_path, temp_dir, max_workers=os.cpu_count()
    )

    print(f'!!!!!!!!! FRAME PATHS : {frame_paths} !!!!!!!!!!!!!!!!!!!')
    # for i, timestamp in enumerate(timestamps):
    #     print(f"Generating Graph for {timestamp} - {position_type_input} - {participant_input}")
    #
    #     fig = generate_frame(data, candlesticks, timestamp, participant_input, strike_input, expiration_input, position_type_input,
    #                          metric,last_price,
    #                          full_img_path)
    #
    #
    #
    #     # Save the frame as an image
    #     frame_path = os.path.join(temp_dir, f'frame_{i:03d}.png')
    #
    #     fig.write_image(frame_path,scale= 3)
    #     frame_paths.append(frame_path)

    # Use the frame_paths directly, no need for additional processing
    file_paths = frame_paths

    # Create the writer with the correct output path
    temp_output = f'temp_{output_video}'
    writer = imageio.get_writer(temp_output, fps=3)

    # Read and write images
    for file_path in file_paths:
        print(f'File path: {file_path}')
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
    # last_frame_path = os.path.join(temp_dir, f'last_frame_{position_type_input}_{timestamps[-1]}.png')
    # fig.write_image(last_frame_path)
    #
    #
    #
    #
    # if final_output:
    #     print(f"Video saved as {final_output}")
    #     return final_output, last_frame_path
    # else:
    #     print("Failed to generate Discord-compatible video")
    #     return None


def generate_video__(data, candlesticks, session_date, participant_input, position_type_input, strike_input, expiration_input,
                   metric, last_price,
                   img_path='config/images/logo_dark.png',
                   output_video='None.mp4'):

    # Get the project root directory
    project_root = Path(__file__).parent.parent

    # Construct the full path to the image
    full_img_path = project_root / img_path

    # Get unique timestamps
    timestamps = data['effective_datetime'].unique()



    # Create a unique temporary directory to store frames
    temp_dir = f'temp_frames_{position_type_input}_{uuid.uuid4().hex}'
    os.makedirs(temp_dir, exist_ok=True)

    # Generate frames in parallel
    frame_paths = parallel_frame_generation(
        data, candlesticks, timestamps, participant_input, strike_input, expiration_input, position_type_input,
        metric, last_price, full_img_path, temp_dir, max_workers=os.cpu_count()
    )

    # Create the writer with the correct output path
    temp_output = f'temp_{output_video}'
    writer = imageio.get_writer(temp_output, fps=3)

    # Read and write images
    for file_path in frame_paths:
        image = imageio.imread(file_path)
        writer.append_data(image)

    print('Finished writing temporary video')
    writer.close()

    # Convert to Discord-compatible format
    final_output = generate_discord_compatible_video(temp_output, output_video)

    # Save the last frame separately
    last_frame_path = os.path.join(temp_dir, f'last_frame_{position_type_input}_{timestamps[-1]}.png')
    frame_paths[-1].write_image(last_frame_path)

    if final_output:
        print(f"Video saved as {final_output}")
        return final_output, last_frame_path
    else:
        print("Failed to generate Discord-compatible video")
        return None, None
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

    # Send the first request (embed only)
    response = requests.post(webhook_url, json=payload)

    print(response)

    if response.status_code == 200 or response.status_code == 204:
        print("Embedded message sent successfully to Discord!")
    else:
        print(f"Failed to Embedded message. Status code: {response.status_code}")
        print(f"Response content: {response.content}")
        breakpoint()

    # Second Request: Send the GIFs separately
    sorted_file_paths = sort_file_paths(file_paths)

    # Now use the sorted paths to create the files dictionary
    files = {}
    for i, file_path in enumerate(sorted_file_paths):
        with open(file_path, 'rb') as f:
            file_content = f.read()
        files[f"file{i}"] = (os.path.basename(file_path), file_content, "image/gif")

    # files = {}
    # for i, file_path in enumerate(file_paths):
    #     with open(file_path, 'rb') as f:
    #         file_content = f.read()
    #     files[f"file{i}"] = (os.path.basename(file_path), file_content, "image/gif")

    # Send the second request (GIFs only)
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200 or response.status_code == 204:
        print("GIFs sent successfully to Discord!")
    else:
        print(f"Failed to send GIFs. Status code: {response.status_code}")
        print(f"Response content: {response.content}")

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

def generate_video(data, candlesticks, session_date, participant_input, position_type_input, strike_input, expiration_input,
                   metric, last_price,
                   img_path='config/images/logo_dark.png',
                   output_video='None.mp4'):

    # Get the project root directory
    project_root = Path(__file__).parent.parent

    # Construct the full path to the image
    full_img_path = project_root / img_path

    # Get unique timestamps
    timestamps = data['effective_datetime'].unique()

    # Create a unique temporary directory to store frames
    temp_dir = f'temp_frames_{position_type_input}_{uuid.uuid4().hex}'
    os.makedirs(temp_dir, exist_ok=True)
    print(f"Created temporary directory: {temp_dir}")

    try:
        # Generate frames in parallel
        workers = 4 #os.cpu_count()
        print(f"Entering Frame generation with {workers} workers")
        frame_paths = parallel_frame_generation(
            data, candlesticks, timestamps, participant_input, strike_input, expiration_input, position_type_input,
            metric, last_price, full_img_path, temp_dir, max_workers=workers
        )
        print(f"Generated {len(frame_paths)} frames")

        # Check if any frames were generated
        if not frame_paths:
            raise ValueError(f"No frames were generated for {position_type_input}")

        # Create the writer with the correct output path
        temp_output = f'temp_{output_video}'
        writer = imageio.get_writer(temp_output, fps=3)

        # Read and write images
        for file_path in frame_paths:
            if not os.path.exists(file_path):
                print(f"Warning: Frame file not found: {file_path}")
                continue
            image = imageio.imread(file_path)
            writer.append_data(image)

        print('Finished writing temporary video')
        writer.close()

        # Convert to Discord-compatible format
        final_output = generate_discord_compatible_video(temp_output, output_video)

        # Save the last frame separately
        last_frame_path = os.path.join(temp_dir, f'last_frame_{position_type_input}_{timestamps[-1]}.png')
        if os.path.exists(frame_paths[-1]):
            shutil.copy(frame_paths[-1], last_frame_path)
        else:
            print(f"Warning: Last frame not found: {frame_paths[-1]}")

        if final_output:
            print(f"Video saved as {final_output}")
            return final_output, last_frame_path
        else:
            print("Failed to generate Discord-compatible video")
            return None, None

    except Exception as e:
        print(f"Error in generate_video: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

    finally:
        # Clean up temporary files
        for file in os.listdir(temp_dir):
            try:
                #os.remove(os.path.join(temp_dir, file))
                print(f"Should have removed file {file}")
            except Exception as e:
                print(f"Error removing file {file}: {str(e)}")
        try:
            #os.rmdir(temp_dir)
            print(f"Should have removed directory {temp_dir}")
        except Exception as e:
            print(f"Error removing directory {temp_dir}: {str(e)}")
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
        video_path,
        content={content},
        title=title,
        description=description,
        fields=fields,
        footer_text=footer_text
    )

    os.remove(video_path)  # Clean up the gif file
    return success