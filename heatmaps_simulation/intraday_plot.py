from datetime import datetime, timedelta
import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import imageio.v2 as imageio
from scipy import interpolate
from PIL import Image
import warnings
import yfinance as yf
from config.config import *
from pathlib import Path
from prefect import task, flow, get_run_logger

# from prefect import task, flow, get_run_logger
with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=RuntimeWarning)


# Adjust the fps as needed.

# img_dark = Image.open(LOGO_dark)
# img2 = Image.open(LOGO_light)


# At the top of your heatmap_task.py file, add:
# Get the directory of the current script
SCRIPT_DIR = Path(__file__).parent.resolve()

# Define the path to the config directory relative to the script
CONFIG_DIR = SCRIPT_DIR.parent / 'config'

# Update the LOGO_dark path
LOGO_dark = str(CONFIG_DIR / 'images' / 'logo_dark.png')
LOGO_light = str(CONFIG_DIR / 'images' / 'logo_light.png')
# In your intraday_plot.py file, update the image loading:
from PIL import Image

def load_logo(path):
    try:
        return Image.open(path)
    except FileNotFoundError:
        print(f"Warning: Logo file not found at {path}. Using placeholder.")
        return Image.new('RGBA', (100, 100), color = (73, 109, 137))

# Replace the original Image.open(LOGO_dark) with:
img_dark = load_logo(LOGO_dark)
img_light = load_logo(LOGO_light)
def create_gif(folder_path:str, output_filename:str):
    """
    Creates a gif from a series of images that are in folder_path.
    Outputs a .gif file that is named output_filename

    Returns
    -------

    """
    file_paths = sorted(os.listdir(folder_path))
    #file_paths = [os.path.join(folder_path, file) for file in file_paths]
    file_paths = [os.path.join(folder_path, file) for file in file_paths if
                  file.endswith(('.png', '.jpg', '.jpeg'))]  # Add other image extensions if necessary

    writer = imageio.get_writer(os.path.join(folder_path, output_filename), fps=2)

    images = [imageio.imread(file_path) for file_path in file_paths]

    for image in images:
        writer.append_data(image)

    print('Finished')
    writer.close()

    # Save the images as a GIF
    # imageio.mimsave(os.path.join(folder_path, output_filename), images, duration=0.3)
    #[os.remove(file) for file in glob.glob(os.path.join(folder_path, '*.png'))]
def get_traces(result_df, key_values, key_prices, line_color):
    print("Getting traces")
    traces = []

    previous_num_points = len(result_df[key_prices].iloc[0])
    x_vals = {i: [result_df.index[0]] for i in range(previous_num_points)}
    y_vals = {i: [result_df[key_prices].iloc[0][i]] for i in range(previous_num_points)}

    def add_traces(x_vals, y_vals):
        for i, x_sequence in x_vals.items():
            if len(x_sequence) > 1:  # Only add traces if more than one point exists
                traces.append(go.Scatter(x=x_sequence, y=y_vals[i], mode='lines', showlegend=False,
                                         line=dict(shape='spline', color=line_color, dash='dot')))

    for current_date, row in result_df.iloc[1:].iterrows():
        current_num_points = len(row[key_prices])

        if current_num_points != previous_num_points:
            add_traces(x_vals, y_vals)
            x_vals = {i: [] for i in range(current_num_points)}
            y_vals = {i: [] for i in range(current_num_points)}
            previous_num_points = current_num_points

        for i in range(current_num_points):
            # Check the price difference constraint
            if x_vals[i] and abs(y_vals[i][-1] - row[key_prices][i]) > 7:
                add_traces({i: x_vals[i]}, {i: y_vals[i]})
                x_vals[i] = []
                y_vals[i] = []

            x_vals[i].append(current_date)
            y_vals[i].append(row[key_prices][i])

    # Add the remaining traces
    add_traces(x_vals, y_vals)

    print(f'Traces: {traces}')

    return traces


def plot_gamma(df_heatmap: pd.DataFrame, minima_df: pd.DataFrame, maxima_df: pd.DataFrame, effective_datetime, spx: pd.DataFrame = None,y_min=None, y_max=None, save_fig=False, fig_show = False, fig_path=None):

    #COUCOUUUUU

    # prefect_logger = get_run_logger()
    x = df_heatmap.index
    y = df_heatmap.columns.values
    z = df_heatmap.values.transpose()

    # Format the effective_datetime for the title
    if not isinstance(effective_datetime, str):
        title_stamp = effective_datetime.strftime("%Y-%m-%d %H:%M")
    else:
        title_stamp = effective_datetime  # Keep it as is if it's already a string

    # z_min = minima_df.values.transpose()
    # z_max = maxima_df.values.transpose()

    z_max = minima_df.values.transpose()
    z_min = maxima_df.values.transpose()

    times_to_show = np.arange(0, len(x), 6)
    bonbhay = [x[time] for time in times_to_show]
    x_values = [simtime.strftime("%H:%M") for simtime in bonbhay]
    y_traces = np.arange(10 * round(y[0] / 10), 10 * round(y[-1] / 10) + 10, 10)

    fig = go.Figure()

    heatmap = go.Contour(
        name="Gamma",
        showlegend=True,
        z=z,
        y=y,
        x=x,
        contours_coloring='heatmap',
        colorscale="RdBu",
        zmax=1600,
        zmin=-1600,
        line_color='#072B43',
        line_width=3,
        contours_start=0,
        contours_end=0,
        colorbar=dict(
            x=0.5,  # Centered below the plot
            y=-0.15,  # Position below the plot
            len=0.5,  # Length of the colorbar
            orientation='h',
            title='Gamma (Delta / 2.5 Points)',  # title here
            titleside='bottom',
            titlefont=dict(
                size=14,
                family='Noto Sans SemiBold')
        )

    )

    minima = go.Contour(
        name="Gamma Trough",
        showlegend=True,
        z=z_min,
        y=y,
        x=x,
        contours_coloring='heatmap',
        colorscale=[[0.0, "rgba(0,0,0,0)"],
                    [1.0, "rgba(0,0,0,0)"]],
        line_color='yellow',
        line_width=5,
        line_dash="dot",
        line_smoothing=0,
        contours_start=0,
        contours_end=0,
        showscale=False
    )
    maxima = go.Contour(
        name="Gamma Peak",
        showlegend=True,
        z=z_max,
        y=y,
        x=x,
        contours_coloring='heatmap',
        colorscale=[[0.0, "rgba(0,0,0,0)"],
                    [1.0, "rgba(0,0,0,0)"]],
        line_color='green',
        line_width=5,
        line_dash="dot",
        line_smoothing=0,
        contours_start=0,
        contours_end=0,
        showscale=False
    )

    fig.add_trace(heatmap)
    fig.add_trace(minima)
    fig.add_trace(maxima)

    fig.update_layout(

        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        plot_bgcolor='rgba(255, 255, 255,0)',
        paper_bgcolor='#021f40',  # 053061
        title=dict(
            text=f"Dealer's Gamma Exposure Map Forecast <br><sup>All expirations, session as of {title_stamp}</sup>",
            font=dict(size=40, family="Noto Sans SemiBold", color="white"),
            yref='container',
            x=0.0,
            xanchor='left',
            yanchor='top',
            # automargin=True,  # Adjust the left margin (example: 100 pixels)

            pad=dict(t=60, b=30, l=80)
        ),
        font=dict(
            family="Noto Sans Medium",
            color='white',
            size=16,
        ),
        # style of new shapes
        newshape=dict(line_color='yellow'),

        xaxis=dict(
            tickmode='array',
            tickvals=bonbhay,
            ticktext=x_values
        ),
        yaxis=dict(
            tickmode='array',
            tickvals=y_traces,
            ticktext=y_traces,
            side='right'
        )

    )

    # if not spx empty
    #----- Adding OHLC -----

    if spx is not None:
        #prefect_logger.info("Entering OHLC overlay")
        print("Entering OHLC overlay")
        candlestick = go.Candlestick(
            x=spx.index,
            open=spx['open'],
            high=spx['high'],
            low=spx['low'],
            close=spx['close'],
            name= 'SPX',


        )
        fig.add_trace(candlestick)

    fig.add_layout_image(
        dict(
            source=img_dark,
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
            layer="above")
    )
    fig.add_layout_image(
        dict(
            source=img_dark,
            xref="paper",
            yref="paper",
            x=1,
            y=1.01,
            yanchor="bottom",
            xanchor="right",

            sizex=0.175,
            sizey=0.175,
            # sizing="contain",
            # opacity=1,
            # layer="above",
        )
    )

    fig.update_xaxes(rangeslider_visible=False)
    fig.update_yaxes(range=[y_min, y_max])
    image_width = 1440  # Width in pixels
    image_height = 810  # Height in pixels
    scale_factor = 3  # Increase for better quality, especially for raster formats



    #breakpoint()
    if fig_show:
        fig.show()

    if save_fig:
        # Create a directory for saving images
        save_dir = os.path.join(os.path.expanduser("~"), "heatmap_images")
        os.makedirs(save_dir, exist_ok=True)

        # Generate the filename
        stamp = df_heatmap.index[0].strftime("%Y-%m-%d_%H-%M")
        filename = f"heatmap_{stamp}.png"

        # Full path for saving the image
        save_path = os.path.join(save_dir, filename)

        # Save the image
        fig.write_image(
            save_path,
            width=image_width,
            height=image_height,
            scale=scale_factor
        )
        print(f"Image saved to: {save_path}")

    if save_fig and fig_path:
        fig.write_image(
            fig_path,
            width=image_width,
            height=image_height,
            scale=scale_factor
        )
        print(f"Image saved to: {fig_path}")
    elif save_fig:
        print("save_fig is True but no fig_path provided. Figure not saved.")

    return fig  # Return the figure object if needed

def plot_charm_intraday(df: pd.DataFrame, effective_datetime, spx: pd.DataFrame = None, save_fig=False, fig_show=False):


    unique_times = sorted(df['sim_datetime'].unique())
    unique_prices = sorted(df['price'].unique())

    # Create empty 2D matrices for value, minima, and maxima
    z = np.full((len(unique_prices), len(unique_times)), np.nan)
    z_min = np.full((len(unique_prices), len(unique_times)), np.nan)
    z_max = np.full((len(unique_prices), len(unique_times)), np.nan)

    for _, row in df.iterrows():
        time_idx = unique_times.index(row['sim_datetime'])
        price_idx = unique_prices.index(row['price'])
        z[price_idx, time_idx] = row['value']
        z_min[price_idx, time_idx] = row['minima']
        z_max[price_idx, time_idx] = row['maxima']

    x = unique_times
    y = unique_prices

    title_stamp = effective_datetime #.strftime("%Y-%m-%d %H:%M")

    times_to_show = np.arange(0, len(x), 6)
    bonbhay = [x[time] for time in times_to_show]
    x_values = [simtime.strftime("%H:%M") for simtime in bonbhay]
    y_traces = np.arange(10 * round(y[0] / 10), 10 * round(y[-1] / 10) + 10, 10)

    fig = go.Figure()

    heatmap = go.Contour(
        name="Gamma",
        showlegend=True,
        z=z,
        y=y,
        x=x,
        contours_coloring='heatmap',
        colorscale="RdBu",
        zmax=1600,
        zmin=-1600,
        line_color='#072B43',
        line_width=3,
        contours_start=0,
        contours_end=0,
        colorbar=dict(
            x=0.5,
            y=-0.15,
            len=0.5,
            orientation='h',
            title='Gamma (Delta / 2.5 Points)',
            titleside='bottom',
            titlefont=dict(
                size=14,
                family='Noto Sans SemiBold')
        )
    )

    # minima = go.Contour(
    #     name="Gamma Trough",
    #     showlegend=True,
    #     z=z_max,
    #     y=y,
    #     x=x,
    #     contours_coloring='heatmap',
    #     colorscale=[[0.0, "rgba(0,0,0,0)"],
    #                 [1.0, "rgba(0,0,0,0)"]],
    #     line_color='yellow',
    #     line_width=5,
    #     line_dash="dot",
    #     line_smoothing=0,
    #     contours_start=0,
    #     contours_end=0,
    #     showscale=False
    # )
    #
    # maxima = go.Contour(
    #     name="Gamma Peak",
    #     showlegend=True,
    #     z=z_min,
    #     y=y,
    #     x=x,
    #     contours_coloring='heatmap',
    #     colorscale=[[0.0, "rgba(0,0,0,0)"],
    #                 [1.0, "rgba(0,0,0,0)"]],
    #     line_color='green',
    #     line_width=5,
    #     line_dash="dot",
    #     line_smoothing=0,
    #     contours_start=0,
    #     contours_end=0,
    #     showscale=False
    # )

    fig.add_trace(heatmap)
    # fig.add_trace(minima)
    # fig.add_trace(maxima)

    fig.update_layout(

        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        plot_bgcolor='rgba(255, 255, 255,0)',
        paper_bgcolor='#021f40',  # 053061
        title=dict(
            text=f"Dealer's Gamma Exposure Map Forecast <br><sup>All expirations, session as of {title_stamp}</sup>",
            font=dict(size=40, family="Noto Sans SemiBold", color="white"),
            yref='container',
            x=0.0,
            xanchor='left',
            yanchor='top',
            # automargin=True,  # Adjust the left margin (example: 100 pixels)

            pad=dict(t=60, b=30, l=80)
        ),
        font=dict(
            family="Noto Sans Medium",
            color='white',
            size=16,
        ),
        # style of new shapes
        newshape=dict(line_color='yellow'),

        xaxis=dict(
            tickmode='array',
            tickvals=bonbhay,
            ticktext=x_values
        ),
        yaxis=dict(
            tickmode='array',
            tickvals=y_traces,
            ticktext=y_traces,
            side='right'
        )

    )

    # if not spx empty
    # ----- Adding OHLC -----

    if spx is not None:
        # prefect_logger.info("Entering OHLC overlay")
        print("Entering OHLC overlay")
        candlestick = go.Candlestick(
            x=spx.index,
            open=spx['open'],
            high=spx['high'],
            low=spx['low'],
            close=spx['close'],
            name='SPX',

        )
        fig.add_trace(candlestick)

    fig.add_layout_image(
        dict(
            source=img_dark,
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
            layer="above")
    )
    fig.add_layout_image(
        dict(
            source=img_dark,
            xref="paper",
            yref="paper",
            x=1,
            y=1.01,
            yanchor="bottom",
            xanchor="right",

            sizex=0.175,
            sizey=0.175,
            # sizing="contain",
            # opacity=1,
            # layer="above",
        )
    )

    fig.update_xaxes(rangeslider_visible=False)

    image_width = 1440  # Width in pixels
    image_height = 810  # Height in pixels
    scale_factor = 3  # Increase for better quality, especially for raster formats

    if fig_show:
        fig.show()

    fig.show()

    return fig

def plot_charm(df: pd.DataFrame, effective_datetime, spx: pd.DataFrame = None, save_fig=False, fig_show=False, grid= False):
    x = df.index
    y = df.columns.values
    z = df.values.transpose()


    title_date = effective_datetime

    y_traces = np.arange(10 * round(y[0] / 10), 10 * round(y[-1] / 10) + 10, 10)

    # find number of hours in the range
    num_hours = (max(x) - min(x) + timedelta(minutes=5)).total_seconds() / 3600

    times_to_show = np.arange(0, len(x), 6)
    bonbhay = [x[time] for time in times_to_show]
    x_values = [simtime.strftime("%H:%M") for simtime in bonbhay]


    # max_val = np.percentile(z, 95)
    # min_val = np.percentile(-z, 95)
    max_val = 150
    min_val = -150
    max_val = np.max([abs(max_val), abs(min_val)])

    fig = go.Figure()


    heatmap = go.Contour(
        z=z,
        y=y,
        x=x,
        contours_coloring='heatmap',
        colorscale=[[0.0, "rgb(0, 59, 99)"],
                    [0.499, "rgb(186, 227, 255)"],
                    [0.501, "rgb(255, 236, 196)"],
                    [1.0, "rgb(255, 148, 71)"]],
        # colorscale= "Spectral",
        zmax=max_val,
        zmin=-max_val,
        line_color='#404040',
        line_width=2,
        contours_start=0,
        contours_end=0,
        colorbar=dict(
            x=0.5,  # Centered below the plot
            y=-0.2,  # Position below the plot
            len=0.5,  # Length of the colorbar
            orientation='h'  # Horizontal orientation
        )

    )

    fig.add_trace(heatmap)

    fig.update_layout(
        xaxis=dict(
            tickmode='array',
            tickvals=bonbhay,
            ticktext=x_values
        ),
        yaxis=dict(
            tickmode='array',
            tickvals=y_traces,
            ticktext=y_traces
        )
    )

    if grid == True:

        for y_ in y_traces:
            fig.add_hline(y=y_, line_width=1, line_dash='solid', line_color="rgb(222, 222, 222, 0.2)")
        for x_ in bonbhay:
            fig.add_vline(x=x_, line_width=1, line_dash="solid", line_color="rgb(222, 222, 222, 0.2)")

    if spx is not None:
        #prefect_logger.info("Entering OHLC overlay")
        print("Entering OHLC overlay")
        candlestick = go.Candlestick(
            x=spx.index,
            open=spx['open'],
            high=spx['high'],
            low=spx['low'],
            close=spx['close'],
            name= 'SPX',


        )
        fig.add_trace(candlestick)

    fig.add_layout_image(
        dict(
            source=img_dark,
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
            layer="above")
    )
    fig.add_layout_image(
        dict(
            source=img_dark,
            xref="paper",
            yref="paper",
            x=1,
            y=1.01,
            yanchor="bottom",
            xanchor="right",

            sizex=0.175,
            sizey=0.175,
            # sizing="contain",
            # opacity=1,
            # layer="above",
        )
    )

    fig.update_xaxes(rangeslider_visible=False)

    # ----------------------------------------
    # ----------------------------------------


    fig.update_layout(
        title=f"Market Makers Charm Exposure Map Forecast <br><sup>All expirations, as of {title_date}</sup>",
    )

    fig.update_layout(
        plot_bgcolor='rgba(255, 255, 255,0)',
        paper_bgcolor='#053061',  # 053061
        title=dict(
            font=dict(size=40, family="Noto Sans SemiBold", color="white"),
            yref='container',
            # automargin=True,
            # y=0.90,
            x=0.0,
            xanchor='left',
            yanchor='top',
            # automargin=True,  # Adjust the left margin (example: 100 pixels)

            pad=dict(t=60, b=30, l=80)
        ),
        font=dict(
            family="Noto Sans Medium",
            color='white',
            size=16,
        ),
        # style of new shapes
        newshape=dict(line_color='yellow'),
    )

    fig.update_layout(
        yaxis={'side': 'right'}
    )

    return fig

def plot_heatmap(df_heatmap: pd.DataFrame,effective_datetime, spx:pd.DataFrame=None,show_fig = False):


    x = df_heatmap.index
    y = df_heatmap.columns.values
    z = df_heatmap.values.transpose()


    y_traces = np.arange(10 * round(y[0] / 10), 10 * round(y[-1] / 10) + 10, 10)
    title_stamp = effective_datetime.strftime("%Y-%m-%d %H:%M")

    times_to_show = np.arange(0, len(x), 6)
    bonbhay = [x[time] for time in times_to_show]
    x_values = [simtime.strftime("%H:%M") for simtime in bonbhay]

    max_val = np.max(z)
    min_val = np.min(z)
    max_val = np.max([abs(max_val), abs(min_val)])

    # breakpoint()
    fig = go.Figure()
    # breakpoint()
    heatmap = go.Contour(
        name="Gamma",
        showlegend=True,
        z=z,
        y=y,
        x=x,
        contours_coloring='heatmap',
        colorscale="RdBu",
        zmax=1600,
        zmin=-1600,
        line_color='#072B43',
        line_width=3,
        contours_start=0,
        contours_end=0,
        colorbar=dict(
            x=0.5,  # Centered below the plot
            y=-0.15,  # Position below the plot
            len=0.5,  # Length of the colorbar
            orientation='h',
            title='Gamma (Delta / 2.5 Points)',  # title here
            titleside='bottom',
            titlefont=dict(
                size=14,
                family='Noto Sans SemiBold')
        )

    )

    fig.add_trace(heatmap)
    #fig.add_trace(minima)
    #fig.add_trace(maxima)

    fig.update_layout(

        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        plot_bgcolor='rgba(255, 255, 255,0)',
        paper_bgcolor='#021f40',  # 053061
        title=dict(
            text=f"Dealer's Gamma Exposure Map Forecast <br><sup>All expirations, session as of {title_stamp}</sup>",
            font=dict(size=40, family="Noto Sans SemiBold", color="white"),
            yref='container',
            x=0.0,
            xanchor='left',
            yanchor='top',
            # automargin=True,  # Adjust the left margin (example: 100 pixels)

            pad=dict(t=60, b=30, l=80)
        ),
        font=dict(
            family="Noto Sans Medium",
            color='white',
            size=16,
        ),
        # style of new shapes
        newshape=dict(line_color='yellow'),

        xaxis=dict(
            tickmode='array',
            tickvals=bonbhay,
            ticktext=x_values
        ),
        yaxis=dict(
            tickmode='array',
            tickvals=y_traces,
            ticktext=y_traces,
            side='right'
        )

    )


    # if not spx empty
    #----- Adding OHLC -----
    if spx is not None:
        candlestick = go.Candlestick(
            x=spx.index,
            open=spx['open'],
            high=spx['high'],
            low=spx['low'],
            close=spx['close'],
            name= 'SPX'

        )
        fig.add_trace(candlestick)
        fig.update_xaxes(rangeslider_visible=False)

    #---------------------





    fig.add_layout_image(
        dict(
            source=img_dark,
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
            layer="above")
    )
    fig.add_layout_image(
        dict(
            source=img_dark,
            xref="paper",
            yref="paper",
            x=1,
            y=1.01,
            yanchor="bottom",
            xanchor="right",

            sizex=0.175,
            sizey=0.175,
            # sizing="contain",
            # opacity=1,
            # layer="above",
        )
    )
    fig.update_xaxes(rangeslider_visible=False)
    fig.update_layout(
        title=f"Dealer's Gamma Exposure Map Evolution <br><sup>All expirations, trading session of {title_stamp}</sup>",
    )

    fig.update_layout(
        #Added
        #margin=dict(l=0, r=0, t=0, b=0),
        plot_bgcolor='rgba(255, 255, 255,1)',
        paper_bgcolor='#053061',#053061
        title=dict(
            font=dict(size=40, family="Noto Sans SemiBold", color="white"),
            yref='container',
            x=0.0,
            xanchor='left',
            yanchor='top',
            # automargin=True,  # Adjust the left margin (example: 100 pixels)

            pad=dict(t=60, b=30, l=80)
        ),
        font=dict(
            family="Noto Sans Medium",
            color='white',
            size=16,
        ),
        # style of new shapes
        newshape=dict(line_color='yellow'),
    )

    fig.update_layout(
        yaxis={'side': 'right'}
    )


    if show_fig:
        # Show the figure
        fig.show()

    #----------- SAVING GRAPH ---------------#
    y_min = 5370  # Replace with your desired minimum value
    y_max = 5630  # Replace with your desired maximum value
    fig.update_yaxes(range=[y_min, y_max])


    image_width = 1440  # Width in pixels
    image_height = 810  # Height in pixels
    scale_factor = 3  # Increase for better quality, especially for raster formats

    stamp = df_heatmap.index[0].strftime("%Y-%m-%d_%H-%M")


    fig.write_image(
        os.path.join(rf"C:\Users\ilias\PycharmProjects\OD_intraday\heatmaps_simulation\20240726_heatmaps",f"heatmap_{stamp}.png"),  # Saving as PNG for high resolution
        width=image_width,
        height=image_height,
        scale=scale_factor
    )


def plot_gamma_test(df_heatmap: pd.DataFrame, minima_df: pd.DataFrame, maxima_df: pd.DataFrame, effective_datetime,
                    spx: pd.DataFrame = None, y_min=None, y_max=None, save_fig=False, fig_show=False, fig_path=None,
                    show_projection_line=False):

    if not isinstance(effective_datetime, pd.Timestamp):
        effective_datetime = pd.to_datetime(effective_datetime)

    title_stamp = effective_datetime.strftime("%Y-%m-%d %H:%M")

    # Split data into past and future
    df_past = df_heatmap[df_heatmap.index <= effective_datetime]
    df_future = df_heatmap[df_heatmap.index >= effective_datetime]


    # Ensure y_min and y_max are set
    y_min = y_min if y_min is not None else df_heatmap.columns.min()
    y_max = y_max if y_max is not None else df_heatmap.columns.max()

    x = df_heatmap.index
    y = df_heatmap.columns.values
    z = df_heatmap.values.transpose()

    z_max = minima_df.values.transpose()
    z_min = maxima_df.values.transpose()


    fig = go.Figure()

    # Past data
    if not df_past.empty:
        if len(df_past) > 1:
            heatmap_past = go.Contour(
                name="Past Gamma",
                z=df_past.values.T,
                x=df_past.index,
                y=df_past.columns,
                contours_coloring='heatmap',
                colorscale="RdBu",
                zmin=-1600,
                zmax=1600,
                showscale=False,
                line_width=0,
                showlegend=True
            )
        else:
            # If there's only one value, use a scatter plot instead
            heatmap_past = go.Scatter(
                name="Past Gamma",
                x=df_past.index,
                y=df_past.columns,
                mode='markers',
                marker=dict(
                    size=10,
                    color=df_past.values.flatten(),
                    colorscale="RdBu",
                    cmin=-1600,
                    cmax=1600,
                    showscale=False
                ),
                showlegend=True
            )
        fig.add_trace(heatmap_past)

    # Future data
    if not df_future.empty:
        heatmap_future = go.Contour(
            name="Future Gamma",
            z=df_future.values.T,
            x=df_future.index,
            y=df_future.columns,
            contours_coloring='heatmap',
            colorscale="RdBu",
            zmin=-1600,
            zmax=1600,
            line_color='#072B43',
            line_width=3,
            contours_start=0,
            contours_end=0,
            colorbar=dict(
                # title='Gamma (Delta / 2.5 Points)',
                # titleside='top',
                # titlefont=dict(size=14, family='Noto Sans SemiBold'),
                title=dict(
                    text='Gamma (Delta / 2.5 Points)',
                    side='top',
                    font=dict(size=14, family='Noto Sans SemiBold')
                ),
                x=0.5,
                y=-0.175,
                len=0.5,
                thickness=20,
                orientation='h',
            ),

            showlegend=True
        )
        fig.add_trace(heatmap_future)

    # Add minima and maxima traces for the entire range
    minima = go.Contour(
        name="Gamma Trough",
        showlegend=True,
        z=z_min,
        y=y,
        x=x,
        contours_coloring='heatmap',
        colorscale=[[0.0, "rgba(0,0,0,0)"], [1.0, "rgba(0,0,0,0)"]],
        line_color='yellow',
        line_width=5,
        line_dash="dot",
        line_smoothing=0,
        contours_start=0,
        contours_end=0,
        showscale=False
    )
    maxima = go.Contour(
        name="Gamma Peak",
        showlegend=True,
        z=z_max,
        y=y,
        x=x,
        contours_coloring='heatmap',
        colorscale=[[0.0, "rgba(0,0,0,0)"], [1.0, "rgba(0,0,0,0)"]],
        line_color='green',
        line_width=5,
        line_dash="dot",
        line_smoothing=0,
        contours_start=0,
        contours_end=0,
        showscale=False
    )

    fig.add_trace(minima)
    fig.add_trace(maxima)

    # Add vertical line at effective_datetime
    fig.add_shape(
        type="line",
        x0=effective_datetime,
        x1=effective_datetime,
        y0=y_min,
        y1=y_max,
        line=dict(color="white", width=2, dash="dot"),
    )

    # Add projection line if requested
    if show_projection_line:
        fig.add_shape(
            type="line",
            x0=effective_datetime,
            x1=effective_datetime,
            y0=y_min,
            y1=y_max,
            line=dict(color="red", width=2, dash="solid"),
        )

        # Add arrows and text
        for text, ax in [("Realized values", -40), ("Projection", 40)]:
            fig.add_annotation(
                x=effective_datetime,
                y=y_min,
                text=text,
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor="red",
                ax=ax,
                ay=20
            )

    # Update layout
    fig.update_layout(
        title=dict(
            text=f"Dealer's Gamma Exposure Map Forecast <br><sup>All expirations, session as of {title_stamp}</sup>",
            font=dict(size=40, family="Noto Sans SemiBold", color="white"),
            yref='container',
            x=0.0,
            xanchor='left',
            yanchor='top',
            pad=dict(t=60, b=30, l=80)
        ),
        xaxis_title="Time",
        yaxis_title="Price",
        autosize=False,
        width=1440,
        height=810,
        showlegend=False,
        plot_bgcolor='rgba(255, 255, 255,0)',
        paper_bgcolor='#021f40',
        font=dict(family="Noto Sans Medium", color='white', size=16),
        newshape=dict(line_color='yellow'),
        margin=dict(b=150)  # Increase bottom margin to accommodate colorbar and title
    )

    # Set x-axis ticks
    time_ticks = pd.date_range(df_heatmap.index.min(), df_heatmap.index.max(), freq='1h')
    fig.update_xaxes(
        tickmode='array',
        tickvals=time_ticks,
        ticktext=[t.strftime('%H:%M') for t in time_ticks],
        rangeslider_visible=False
    )

    # Set y-axis ticks and range
    y_ticks = np.arange(10 * round(y_min / 10), 10 * round(y_max / 10) + 10, 10)
    fig.update_yaxes(
        tickmode='array',
        tickvals=y_ticks,
        ticktext=y_ticks,
        side='right',
        range=[y_min, y_max]
    )

    # Add OHLC if spx is provided
    if spx is not None:
        candlestick = go.Candlestick(
            x=spx.index,
            open=spx['open'],
            high=spx['high'],
            low=spx['low'],
            close=spx['close'],
            name='SPX',
            showlegend=False
        )
        fig.add_trace(candlestick)

    # Add layout images (assuming 'img_dark' is defined elsewhere)
    if 'img_dark' in globals():
        fig.add_layout_image(
            dict(
                source=img_dark,
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
                layer="above")
        )
        #TOP RIGHT
        fig.add_layout_image(
            dict(
                source=img_light,
                xref="paper",
                yref="paper",
                x=1,
                y=1.01,
                yanchor="bottom",
                xanchor="right",
                sizex=0.15,
                sizey=0.15,
            )
        )

    if fig_show:
        fig.show()

    return fig


def plot_charm_test(df: pd.DataFrame, effective_datetime, spx: pd.DataFrame = None, save_fig=False, fig_show=False, grid=False):
    if not isinstance(effective_datetime, pd.Timestamp):
        effective_datetime = pd.to_datetime(effective_datetime)

    title_date = effective_datetime.strftime("%Y-%m-%d %H:%M")

    # Split data into past and future
    df_past = df[df.index <= effective_datetime]
    df_future = df[df.index >= effective_datetime]

    y = df.columns.values
    y_traces = np.arange(10 * round(y[0] / 10), 10 * round(y[-1] / 10) + 10, 10)

    times_to_show = np.arange(0, len(df.index), 6)
    bonbhay = [df.index[time] for time in times_to_show]
    x_values = [simtime.strftime("%H:%M") for simtime in bonbhay]

    max_val = 150
    min_val = -150
    max_val = np.max([abs(max_val), abs(min_val)])

    fig = go.Figure()

    # Past data
    if not df_past.empty:
        if len(df_past) > 1:
            heatmap_past = go.Contour(
                name="Past Charm",
                z=df_past.values.T,
                x=df_past.index,
                y=df_past.columns,
                contours_coloring='heatmap',
                colorscale=[[0.0, "rgb(0, 59, 99)"],
                            [0.499, "rgb(186, 227, 255)"],
                            [0.501, "rgb(255, 236, 196)"],
                            [1.0, "rgb(255, 148, 71)"]],
                zmax=max_val,
                zmin=-max_val,
                showscale=False,
                line_width=0,
                showlegend=True
            )
        else:
            heatmap_past = go.Scatter(
                name="Past Charm",
                x=df_past.index,
                y=df_past.columns,
                mode='markers',
                marker=dict(
                    size=10,
                    color=df_past.values.flatten(),
                    colorscale=[[0.0, "rgb(0, 59, 99)"],
                                [0.499, "rgb(186, 227, 255)"],
                                [0.501, "rgb(255, 236, 196)"],
                                [1.0, "rgb(255, 148, 71)"]],
                    cmin=-max_val,
                    cmax=max_val,
                    showscale=False
                ),
                showlegend=True
            )
        fig.add_trace(heatmap_past)

    # Future data
    if not df_future.empty:
        heatmap_future = go.Contour(
            name="Future Charm",
            z=df_future.values.T,
            x=df_future.index,
            y=df_future.columns,
            contours_coloring='heatmap',
            colorscale=[[0.0, "rgb(0, 59, 99)"],
                        [0.499, "rgb(186, 227, 255)"],
                        [0.501, "rgb(255, 236, 196)"],
                        [1.0, "rgb(255, 148, 71)"]],
            zmax=max_val,
            zmin=-max_val,
            line_color='#404040',
            line_width=2,
            contours_start=0,
            contours_end=0,
            colorbar=dict(
                x=0.5,
                y=-0.2,
                len=0.5,
                orientation='h',
                title=dict(
                    text='Charm (Delta / 1 Day)',
                    side='top',
                    font=dict(size=14, family='Noto Sans SemiBold')
                ),
            ),
            showlegend=True
        )
        fig.add_trace(heatmap_future)

    # Add vertical line at effective_datetime
    fig.add_shape(
        type="line",
        x0=effective_datetime,
        x1=effective_datetime,
        y0=y.min(),
        y1=y.max(),
        line=dict(color="white", width=2, dash="dot"),
    )

    if grid:
        for y_ in y_traces:
            fig.add_hline(y=y_, line_width=1, line_dash='solid', line_color="rgb(222, 222, 222, 0.2)")
        for x_ in bonbhay:
            fig.add_vline(x=x_, line_width=1, line_dash="solid", line_color="rgb(222, 222, 222, 0.2)")

    if spx is not None:
        candlestick = go.Candlestick(
            x=spx.index,
            open=spx['open'],
            high=spx['high'],
            low=spx['low'],
            close=spx['close'],
            name='SPX',
            showlegend=False
        )
        fig.add_trace(candlestick)

    # Add layout images (assuming 'img_dark' and 'img_light' are defined elsewhere)
    if 'img_dark' in globals():
        fig.add_layout_image(
            dict(
                source=img_dark,
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
                layer="above")
        )
    if 'img_light' in globals():
        fig.add_layout_image(
            dict(
                source=img_light,
                xref="paper",
                yref="paper",
                x=1,
                y=1.01,
                yanchor="bottom",
                xanchor="right",
                sizex=0.175,
                sizey=0.175,
            )
        )

    fig.update_layout(
        title=dict(
            text=f"Market Makers Charm Exposure Map Forecast <br><sup>All expirations, as of {title_date}</sup>",
            font=dict(size=40, family="Noto Sans SemiBold", color="white"),
            yref='container',
            x=0.0,
            xanchor='left',
            yanchor='top',
            pad=dict(t=60, b=30, l=80)
        ),
        xaxis_title="Time",
        yaxis_title="Price",
        plot_bgcolor='rgba(255, 255, 255,0)',
        paper_bgcolor='#053061',
        font=dict(family="Noto Sans Medium", color='white', size=16),
        newshape=dict(line_color='yellow'),
        autosize=False,
        width=1440,
        height=810,
        showlegend=False,
        margin=dict(b=150)
    )

    fig.update_xaxes(
        tickmode='array',
        tickvals=bonbhay,
        ticktext=x_values,
        rangeslider_visible=False
    )

    fig.update_yaxes(
        tickmode='array',
        tickvals=y_traces,
        ticktext=y_traces,
        side='right'
    )

    if fig_show:
        fig.show()

    return fig


if __name__ == "__main__":
    from utilities.db_utils import *
    # db = DatabaseUtilities(DB_HOST, int(DB_PORT), DB_USER, DB_PASSWORD, DB_NAME)
    # db.connect()
    # print(f'{db.get_status()}')
    #
    # effective_date = '2024-08-22'
    # effective_datetimes_query = f"""
    # SELECT distinct(effective_datetime) from intraday.intraday_gamma
    # where effective_date = '{effective_date}'
    # """
    # list_of_ed =db.execute_query(effective_datetimes_query)#'2024-08-21 10:30:00','2024-08-21 11:30:00','2024-08-21 12:30:00','2024-08-21 13:30:00','2024-08-21 14:30:00','2024-08-21 15:00:00']
    #
    #
    # for effective_datetime in list_of_ed.values:
    #     effective_datetime = effective_datetime[0]
    #     print(effective_datetime)
    #     books_query = f"""
    #     WITH ranked_gamma AS (
    #         SELECT
    #             id,
    #             ticker,
    #             effective_date,
    #             effective_datetime,
    #             price,
    #             value,
    #             sim_datetime,
    #             NULL as minima,
    #             NULL as maxima,
    #             ROW_NUMBER() OVER (PARTITION BY sim_datetime,price ORDER BY effective_datetime DESC) AS rn
    #         FROM intraday.intraday_gamma
    #         WHERE effective_datetime <= '{effective_datetime}' -- (SELECT max(effective_datetime) FROM intraday_gamma)
    #         and effective_date = '{effective_date}'
    #     ),
    #     consumed_gamma AS (
    #         SELECT
    #             id,
    #             ticker,
    #             effective_date,
    #             effective_datetime,
    #             price,
    #             value,
    #             sim_datetime,
    #             minima,
    #             maxima
    #         FROM ranked_gamma
    #         WHERE rn = 1
    #     ),
    #     upcoming_gamma AS (
    #         SELECT *
    #         FROM intraday.intraday_gamma
    #         WHERE effective_datetime >= '{effective_datetime}' -- (SELECT max(effective_datetime) FROM intraday_gamma)
    #         and
    #         effective_date = '{effective_date}'
    #     ),
    #     final_gamma as(
    #     SELECT * FROM consumed_gamma
    #     UNION ALL
    #     SELECT * FROM upcoming_gamma
    #     )
    #     SELECT * from final_gamma;
    #     """
    #     df_book = db.execute_query(books_query)
    #
    #     #breakpoint()
    #     plot_gamma_intraday(df_book,effective_datetime)
    create_gif(folder_path=r"/Users/youssefadiem/PycharmProjects/OptionsDepth_intraday/heatmaps_simulation/temp_frames_42d6f4ea211145498a868a62cb245339", output_filename= '20240920_gamma_recap.mp4')

