import pandas as pd
import plotly.graph_objects as go
from datetime import date
import base64
from plotly.subplots import make_subplots
import imageio
import os



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
    import colorsys

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


def generate_frame(data, timestamp, participant, strike_input, expiration_input, position_type='All',
                   img_path=None, color_net='#0000FF', color_call='#00FF00', color_put='#FF0000'):
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
        margin=dict(l=50, r=50, t=100, b=50)
    )

    return fig


def generate_animated_chart(data, date, participant, strike_input, expiration_input, position_type='All',
                            img_path=None, color_net='#0000FF', color_call='#00FF00', color_put='#FF0000',
                            output_gif='animated_chart.gif'):
    # Get unique timestamps
    timestamps = data['effective_datetime'].unique()

    # Create a temporary directory to store frames
    if not os.path.exists('temp_frames'):
        os.makedirs('temp_frames')

    # Generate frames
    frames = []
    for i, timestamp in enumerate(timestamps):
        print(f"Generating Graph for {timestamp}")
        fig = generate_frame(data, timestamp, participant, strike_input, expiration_input, position_type,
                             img_path, color_net, color_call, color_put)

        # Save the frame as an image
        frame_path = f'temp_frames/frame_{i:03d}.png'
        fig.write_image(frame_path)
        frames.append(imageio.imread(frame_path))

    # Create the GIF
    imageio.mimsave(output_gif, frames, fps=5)

    # Clean up temporary files
    for file in os.listdir('temp_frames'):
        os.remove(os.path.join('temp_frames', file))
    os.rmdir('temp_frames')

    print(f"Animation saved as {output_gif}")


if __name__ == "__main__":


    #TODO: Wrap this in a function where current_date is a param, participant is a param, position_type is a param
    # return a gif that will be sent to discord

    # query = """
    # SELECT * FROM intraday.intraday_books
    # where date(time_stamp) = 'current_date'
    # """
    # df = utils.return_query(query)


    df = pd.read_pickle("/Users/iliaselbekri/PycharmProjects/OD_intraday/20240716_intraday.pkl")
    breakpoint()
    generate_animated_chart(
        df,
        '2024-07-15',
        'mm',
        strike_input=[5450, 5800],
        expiration_input='all',
        position_type='All',  # Can be 'All', 'Net', 'C', or 'P'
        img_path='path/to/watermark.png',  # Optional
        color_net='#0000FF',  # Blue for Net
        color_call='#00FF00',  # Green for Calls
        color_put='#FF0000',  # Red for Puts
        output_gif='animated_options_chart.gif'
    )