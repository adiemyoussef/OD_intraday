from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
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


def generate_frame_wrapper(args):
    timestamp, index, generate_frame_partial = args
    fig = generate_frame_partial(timestamp)
    frame_path = f'temp_frames/frame_{index:03d}.png'
    fig.write_image(frame_path)
    return index, frame_path


def process_single_strike(group, participant):
    date = group['effective_date'].iloc[0]
    strike = group['strike_price'].iloc[0]

    results = {}
    for flag in ['C', 'P']:
        flag_group = group[group['call_put_flag'] == flag].sort_values('effective_datetime')
        if not flag_group.empty:
            positions = flag_group[f'{participant}_posn']
            datetimes = flag_group['effective_datetime']

            highest_idx = positions.idxmax()
            lowest_idx = positions.idxmin()

            results[flag] = {
                'start_of_day': {'value': positions.iloc[0], 'time': datetimes.iloc[0]},
                'current': {'value': positions.iloc[-1], 'time': datetimes.iloc[-1]},
                'lowest': {'value': positions.min(), 'time': datetimes[lowest_idx]},
                'highest': {'value': positions.max(), 'time': datetimes[highest_idx]},
                'prior_update': {'value': positions.iloc[-2] if len(positions) > 1 else positions.iloc[0],
                                 'time': datetimes.iloc[-2] if len(positions) > 1 else datetimes.iloc[0]}
            }

    # Calculate net positions
    if 'C' in results and 'P' in results:
        results['Net'] = {
            key: {'value': results['C'][key]['value'] + results['P'][key]['value'],
                  'time': max(results['C'][key]['time'], results['P'][key]['time'])}
            for key in results['C']
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

def generate_frame(data, timestamp, participant, strike_input, expiration_input, position_type,
                   full_img_path, color_net='#0000FF', color_call='#00FF00', color_put='#FF0000'):


    try:
        img = Image.open(full_img_path)
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode()
        img_src = f"data:image/png;base64,{img_str}"
        print(f"Image loaded successfully from {full_img_path}")
    except Exception as e:
        print(f"Error loading image: {e}")
        img_src = None



    daily_data = data[data['effective_datetime'] <= timestamp].copy()

    # Apply strike and expiration filters
    if strike_input != "all":
        if isinstance(strike_input, (list, tuple)):
            daily_data = daily_data[daily_data['strike_price'].between(min(strike_input), max(strike_input))]
            subtitle_strike = f"For strikes: {min(strike_input)} to {max(strike_input)}"
        elif isinstance(strike_input, int):
            daily_data = daily_data[daily_data['strike_price'] == strike_input]
            subtitle_strike = f"For strike: {strike_input}"
    else:
        subtitle_strike = "All Strikes"


    if expiration_input != "all":
        if isinstance(expiration_input, (list, tuple)):
            daily_data = daily_data[daily_data['expiration_date_original'].isin(expiration_input)]
            subtitle_expiration = f"For expirations: {', '.join(str(exp) for exp in expiration_input)}"
        elif isinstance(expiration_input, date):
            daily_data = daily_data[daily_data['expiration_date_original'] == expiration_input]
            subtitle_expiration = f"For expiration: {expiration_input}"
        else:

            # Ensure expiration_input is a string in 'YYYY-MM-DD' format
            expiration_input = pd.to_datetime(expiration_input).strftime('%Y-%m-%d')
            # Convert the expiration_date_original column to datetime, then back to string in 'YYYY-MM-DD' format
            daily_data['expiration_date_original'] = pd.to_datetime(daily_data['expiration_date_original']).dt.strftime(
                '%Y-%m-%d')


            daily_data = daily_data[daily_data['expiration_date_original'] == expiration_input]
            subtitle_expiration = f"For expiration: {expiration_input}"

    else:
        subtitle_expiration = "All Expirations"


    # Group by strike price
    grouped = daily_data.groupby('strike_price')

    # Process each group
    results = [process_single_strike(group, participant) for _, group in grouped]

    # Convert results to DataFrame and sort by strike price
    results_df = pd.DataFrame(results).sort_values('strike_price', ascending=True)

    # Create figure
    fig = make_subplots(rows=1, cols=1)

    position_types = ['C', 'P', 'Net'] if position_type == 'All' else [position_type]
    colors = {'Net': color_net, 'C': color_call, 'P': color_put}

    for pos_type in position_types:
        if pos_type in results_df.columns:
            shades = generate_color_shades(colors[pos_type])

            positions = ['current', 'start_of_day', 'prior_update']
            symbols = ['square', 'circle', 'x']

            for position, symbol, shade in zip(positions, symbols, shades):
                # Helper function to safely extract values
                def safe_extract(x, key):
                    if isinstance(x, dict) and key in x:
                        return x[key]['value'] if isinstance(x[key], dict) else x[key]
                    return None

                x_values = results_df[pos_type].apply(lambda x: safe_extract(x, position))
                valid_mask = x_values.notnull()

                if position == 'current':
                    trace = go.Bar(
                        x=x_values[valid_mask],
                        y=results_df['strike_price'][valid_mask],
                        name=f'{pos_type} {position.capitalize().replace("_", " ")}',
                        orientation='h',
                        marker_color=shade,
                        opacity=0.7,
                        legendgroup=pos_type,
                        legendgrouptitle_text=pos_type,
                        hovertemplate=f"<b>{position.capitalize().replace('_', ' ')}</b><br>" +
                                      "Strike: %{y}<br>" +
                                      "Position: %{x}<br>" +
                                      "Time: %{customdata}<extra></extra>",
                        customdata=results_df[pos_type][valid_mask].apply(
                            lambda x: x[position]['time'] if isinstance(x, dict) and position in x else None)
                    )
                else:
                    trace = go.Scatter(
                        x=x_values[valid_mask],
                        y=results_df['strike_price'][valid_mask],
                        mode='markers',
                        name=f'{pos_type} {position.capitalize().replace("_", " ")}',
                        marker=dict(symbol=symbol, size=8, color=shade),
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
                            line=dict(color=shade, width=2),
                            opacity=0.7,
                        )

    # Update layout
    fig.update_layout(
        title=dict(
            text=(f"<b>Breakdown By Strike</b><br>"
                  f"<sup>SPX: {participant.upper()} {position_type} Positioning as of {timestamp}</sup><br>"
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
            title="Position",
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
            opacity=0.1,  # Increased opacity for better visibility
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

def generate_gif(data, session_date,participant_input, position_type_input, strike_input, expiration_input,
                 img_path='config/images/logo_light.png', color_net='#0000FF', color_call='#00FF00', color_put='#FF0000',
                 output_gif='animated_chart.gif'):

    # Get the project root directory
    project_root = Path(__file__).parent.parent

    # Construct the full path to the image
    full_img_path = project_root / img_path

    # Get unique timestamps
    timestamps = data['effective_datetime'].unique()


    # Create a temporary directory to store frames
    if not os.path.exists('temp_frames'):
        os.makedirs('temp_frames')


    # Generate frames
    frames = []

    # breakpoint()

    for i, timestamp in enumerate(timestamps):
        print(f"Generating Graph for {timestamp}")
        fig = generate_frame(data, timestamp, participant_input, strike_input, expiration_input, position_type_input,
                             full_img_path, color_net, color_call, color_put)

        # Save the frame as an image
        frame_path = f'temp_frames/frame_{i:03d}.png'
        fig.write_image(frame_path)
        frames.append(imageio.imread(frame_path))

    frames.append(imageio.imread(frame_path))

    # Create the GIF
    imageio.mimsave(output_gif, frames, fps=5)

    # Clean up temporary files
    for file in os.listdir('temp_frames'):
        os.remove(os.path.join('temp_frames', file))
    os.rmdir('temp_frames')

    print(f"Animation saved as {output_gif}")
    return output_gif



def send_to_discord(webhook_url, file_path, content=None, title=None, description=None, fields=None,
                    footer_text=None):
    with open(file_path, 'rb') as f:
        file_content = f.read()

    # Prepare the embed
    embed = {
        "title": title or "Options Chart Analysis",
        "description": description or "Here's the latest options chart analysis.",
        "color": 3447003,  # A nice blue color, you can change this
        "timestamp": datetime.utcnow().isoformat(),
        "fields": fields or [],
        "image": {"url": "attachment://chart.gif"},
        "footer": {"text": footer_text or "Generated by Options Analysis Bot"},
        # "author": {
        #     "name": "Options Analysis Bot",
        #     "icon_url": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQmKLlOmqYS-L68xOFw8jNoYHFSWCKgK7DXjA&s"  # Replace with your bot's icon URL
        # }
    }

    # Prepare the payload
    payload = {
        "content": content,
        "embeds": [embed]
    }

    # Prepare the files
    files = {
        "file": ("chart.gif", file_content, "image/gif"),
        "payload_json": (None, json.dumps(payload))
    }

    # Send the request
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200:
        print("Message sent successfully to Discord!")
    else:
        print(f"Failed to send message. Status code: {response.status_code}")
        print(f"Response content: {response.content}")

    return response.status_code == 200

def generate_and_send_gif(data, session_date, participant, position_type , strike_input, expiration,webhook_url):
    gif_path = generate_gif(
        data,
        session_date,
        participant_input = participant,
        position_type_input=position_type,
        strike_input=strike_input,
        expiration_input=expiration,)



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
            "• ■ represents current position magnitude\n"
            "• ● represents the start of day position magnitude\n"
            "• ✖ represents the prior update position magnitude\n"
        ), "inline": False},
    ]
    footer_text = f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Options Analysis v1.0"

    success = send_to_discord(
        webhook_url,
        gif_path,
        content="🚀 New options chart analysis is ready! Check out the latest market insights below.",
        title=title,
        description=description,
        fields=fields,
        footer_text=footer_text
    )

    os.remove(gif_path)  # Clean up the gif file
    return success
