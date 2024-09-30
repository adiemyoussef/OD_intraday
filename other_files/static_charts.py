import yfinance as yf
import plotly.graph_objects as go
import plotly.io as pio
from datetime import datetime, timedelta, date
pio.renderers.default = "browser"
import numpy as np
import pandas as pd
import os
from PIL import Image
import pandas as pd
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

DEV_CHANNEL ='https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
#https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ

default_date = date.today() - timedelta(days=0)
LIST_PART = ['total_customers', 'broker', 'firm', 'retail', 'institution']


db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
db.connect()
print(f'{db.get_status()}')



def plot_options_data(df, aggregation, strike_input, expiration_input, as_of, view='net'):
    """
    Plots the options data based on specified aggregation and view.

    Parameters:
        df (pd.DataFrame): DataFrame containing options data with columns for 'expiration', 'type', 'strike', and 'net_position'.
        aggregation (str): 'strike' or 'expiration' to set the aggregation level.
        view (str): 'net', 'call', 'put', 'all', or 'both' to determine which data to show.
        strike_input (tuple): (min_strike, max_strike) to filter options by strike range.
        expiration_input (list): List of specific expirations to include.
    """

    # -------------------------------------- prepping data and parameters --------------------------------------
    df["net_contract"] = -df["net_contract"]

    print('-------------------------')
    print('----Ploting Function-----')
    print(f'strikes received by function: {strike_input} {type(strike_input)}')
    print(f'expirations received by function: {expiration_input} {type(expiration_input)}, ')

    print('-------------------------')

    # si on dinne expiration comme parametre

    # expiration scenarios

    # - string "all" --> everything # keep dataframe the same. define title string
    if expiration_input == "all":
        subtitle_expiration = "All Expirations"

    # - list --> range
    if isinstance(expiration_input, list):
        print("------------- range expirations -------------")

        print(type(expiration_input[0]))  # string TODO fix to datetime
        df = df[df['expiration_date_original'].between(min(expiration_input), max(expiration_input))]
        subtitle_expiration = f"For {min(expiration_input).strftime('%Y-%m-%d')} to {max(expiration_input).strftime('%Y-%m-%d')} expirations range"

    # - tuple --> list
    elif isinstance(expiration_input, tuple):
        print("------------- list expirations -------------")
        df = df[df['expiration_date_original'].isin(expiration_input)]
        subtitle_expiration = f"For {', '.join([date.strftime('%Y-%m-%d') for date in expiration_input])} specific expirations"

    # - string --> single
    if isinstance(expiration_input, date):
        print("------------- single expiration -------------")
        df = df[df['expiration_date_original'] == expiration_input]
        subtitle_expiration = f"For {expiration_input} unique expiration"

    # --------------------------- strikes ---------------------------

    if strike_input == "all":
        subtitle_strike = "All Strikes"

    # - list --> range
    if isinstance(strike_input, list):
        print("------------- range strikes -------------")

        df = df[(df['strike_price'] >= min(strike_input)) & (df['strike_price'] <= max(strike_input))]
        subtitle_strike = f"For range: {min(strike_input)} to {max(strike_input)} strikes"

    # - tuple --> list
    elif isinstance(strike_input, tuple):
        print("------------- list strikes -------------")
        df = df[df['strike_price'].isin(strike_input)]
        subtitle_strike = f"For the following strikes: {', '.join([str(item) for item in strike_input])}"

    # - string --> single
    if isinstance(strike_input, int):
        print("------------- single strike -------------")
        df = df[df['strike_price'] == strike_input]
        subtitle_strike = f"Strike: {strike_input} uniquely"

    # Debugging prints after filtering
    if not df.empty:
        print("Min Strike Price:", df['strike_price'].min())
        print("Max Strike Price:", df['strike_price'].max())
        print("Unique Expiration Dates:", df['expiration_date_original'].unique())
    else:
        print("No data left after filtering")

    print('DataFrame head after filtering:')

    # -------------------------------------- plotting --------------------------------------
    # Aggregate data
    if aggregation == 'strike':
        grouped = df.groupby(['strike_price', 'call_put_flag']).agg({'net_contract': 'sum'}).unstack(fill_value=0)
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
            # minor=dict(  # Configuration for minor gridlines
            #     showgrid=True,  # Enable minor gridlines
            #     gridcolor='#f0f0f0',  # Color of minor gridlines
            #     gridwidth=0.5,  # Width of minor gridlines
            #     dtick=5  # Minor tick every 250
            # ),
            showticklabels=True
        )
        x_parameters = None

        width = 1200
        height = len(grouped.index) * len(grouped.columns) * 15 + 600





    elif aggregation == 'expiration':
        grouped = df.groupby(['expiration_date', 'call_put_flag']).agg({'net_contract': 'sum'}).unstack(
            fill_value=0)
        grouped.columns = grouped.columns.droplevel()
        dates = [str(date) for date in grouped.index]
        orientation = 'v'
        yaxis_title = "Net Position"
        xaxis_title = "Expiration Date"
        y_parameters = None
        x_parameters = dict(
            type='date',  # Specify that the x-axis contains date values
            tickformat='%b %d, %Y',  # Format the tick labels
            tickvals=grouped.index,  # Set tick values to match your dates
            ticktext=[date.strftime('%b %d, %Y') for date in grouped.index],  # Custom tick labels formatted
            gridcolor='lightgrey',  # Color of vertical gridlines
            showgrid=True,  # Show vertical gridlines
        )

        width = len(grouped.index) * len(grouped.columns) * 20 + 1080 + (len(grouped.index) / 5) * 100
        height = 1080

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
    # ------------------

    fig.update_layout(
        plot_bgcolor='rgba(255, 255, 255,1)',
        paper_bgcolor='#053061',  # Dark blue background
        title=dict(
            text=(
                f"<span style='font-size:40px;'>SPX: Net Customer Options Positioning as of {as_of}</span>"
                f"<br><span style='font-size:20px; display:block; margin-top:20px; margin-bottom:0px;'>{subtitle_strike}</span>"
                f"<br><span style='font-size:20px; display:block; margin-top:10px; margin-bottom:0px;'>{subtitle_expiration}</span>"
            ),
            font=dict(family="Noto Sans SemiBold", color="white"),
            y=0.96,  # Adjust to control the vertical position of the title
            x=0.0,
            # xanchor='left',
            # yanchor='top',
            pad=dict(t=10, b=10, l=40)  # Adjust padding around the title
        ),
        width=width,
        height=height,
        margin=dict(l=40, r=40, t=190, b=30),  # Adjust overall margins
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        font=dict(family="Noto Sans Medium", color='white', size=20),
        legend=dict(
            title='Option Type',
            font=dict(size=20)
        )
    )

    if y_parameters:
        fig.update_layout(yaxis=y_parameters)

    if x_parameters:
        fig.update_layout(xaxis=x_parameters)

    fig.add_layout_image(
        dict(
            source=img,
            xref="paper",
            yref="y domain",
            x=0.5,
            y=0.5,
            yanchor="middle",
            xanchor="center",
            sizex=1,
            sizey=1,
            sizing="contain",
            opacity=0.08,
            layer="below")
    )

    # fig.show()
    return fig, width, height

def send_heatmap_discord(gamma_chart: go.Figure, as_of_time_stamp: str, session_date: str,
                         y_min: int, y_max: int, webhook_url: str) -> bool:
    title = f"📊 {session_date} Intraday Gamma Heatmap"
    description = (
        f"Detailed analysis of SPX Gamma for the {session_date} session.\n"
        f"This heatmap provides insights into market makers gamma exposure within the specified price range.\n"
    )
    current_time = datetime.utcnow().isoformat()
    fields = [
        {"name": "📈 Analysis Type", "value": "Intraday Gamma Heatmap", "inline": True},
        {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
        {"name": "🎯 Price Range", "value": f"{y_min} - {y_max}", "inline": True},
        {"name": "💡 Interpretation", "value": (
            "• Darker colors indicate higher gamma concentration\n"
            "• Light colors indicate lower gamma concentration\n"
            "• Dotted lines represent significant gamma levels\n"
        ), "inline": False},
    ]
    footer_text = f"Generated on {current_time} | By OptionsDepth Inc."

    # Prepare the embed
    embed = {
        "title": title,
        "description": description,
        "color": 3447003,  # A nice blue color
        "fields": fields,
        "footer": {"text": footer_text},
        "timestamp": current_time,
        "image": {"url": "attachment://heatmap.png"}  # Reference the attached image
    }

    # Convert Plotly figure to image bytes
    img_bytes = gamma_chart.to_image(format="png", scale=3)

    # Prepare the payload
    payload = {
        "content": "🚀[UPDATE]: New Gamma Heatmap analysis is ready!",
        "embeds": [embed]
    }

    # Prepare the files dictionary
    files = {
        "payload_json": (None, json.dumps(payload), "application/json"),
        "file": ("heatmap.png", img_bytes, "image/png")
    }

    # Send the request
    response = requests.post(webhook_url, files=files)

    if response.status_code == 200 or response.status_code == 204:
        print(f"Heatmap for {session_date} sent successfully to Discord!")
        return True
    else:
        print(f"Failed to send heatmap. Status code: {response.status_code}")
        print(f"Response content: {response.content}")
        return False
