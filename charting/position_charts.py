def plot_options_data(df_metrics:pd.DataFrame, aggregation, participant = "total_customers", strike_input, expiration_input, as_of, view='both'):
    """
    Plots the options data based on specified aggregation and view.

    Parameters:
        df (pd.DataFrame): DataFrame containing options data with columns for 'expiration', 'type', 'strike', and 'net_position'.
        aggregation (str): 'strike' or 'expiration' to set the aggregation level.
        view (str): 'net', 'call', 'put', 'all', or 'both' to determine which data to show.
        strike_input (tuple): (min_strike, max_strike) to filter options by strike range.
        expiration_input (list): List of specific expirations to include.
    """


    print('-------------------------')
    print('----Ploting Function-----')
    print(f'strikes received by function: {strike_input} {type(strike_input)}')
    print(f'expirations received by function: {expiration_input} {type(expiration_input)}, ')

    print('-------------------------')

    # --------------------------- Strikes ---------------------------
    if strike_input  == "all":
        subtitle_strike= "All Strikes"

    # - list --> range
    if isinstance(strike_input, list):
        print("------------- range strikes -------------")

        df = df_metrics[(df_metrics['strike_price'] >= min(strike_input)) & (df_metrics['strike_price'] <= max(strike_input))]
        subtitle_strike=f"For range: {min(strike_input)} to {max(strike_input)} strikes"

    # - tuple --> list
    elif isinstance(strike_input, tuple):
        print("------------- list strikes -------------")
        df = df_metrics[df_metrics['strike_price'].isin(strike_input)]
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
    if isinstance(expiration_input, date):
        print("------------- single expiration -------------")
        df = df[df['expiration_date_original'] == expiration_input]
        subtitle_expiration=f"For {expiration_input} unique expiration"


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


@flow(name="Depthview flow")
def plot_depthview(
    session_date: Optional[date] = default_date,
    strike_range: Optional[List[int]] = None,
    expiration: Optional[str] = None,
    participant: str = 'total_customers',
    position_types: Optional[List[str]] = None,
    webhook_url: str = 'https://discord.com/api/webhooks/1273463250230444143/74Z8Xo4Wes7jwzdonzcLZ_tCm8hdFDYlvPfdTcftKHjkI_K8GNA1ZayQmv_ZoEuie_8_'
    ):

    current_time = datetime.now().time()

    if current_time < time(12, 0):  # Before 12:00 PM
        start_time = '07:00:00'
    elif time(12, 0) <= current_time < time(23, 0):  # Between 12:00 PM and 7:00 PM
        start_time = '09:00:00'
    else:  # 7:00 PM or later
        start_time = '07:00:00'  # You might want to adjust this for the after 7:00 PM case

    print(f"Start time set to: {start_time}")


    metrics, candlesticks, last_price = fetch_data(session_date, strike_range, expiration, start_time)

    #title formatting
    if type == "GEX":
        dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Market Makers' GEX</span>"
        colorscale = "RdBu"
        title_colorbar = f"{type}<br>(M$/point)"

    elif type == "DEX":
        dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Customer DEX</span>"
        title_colorbar = f"{type}<br>(δ)"

    elif type == "position":
        dynamic_title = f"<span style='font-size:40px;'>SPX DepthView - Net Customer Position</span>"
        title_colorbar = f"{type}<br>(contracts #)"


    if position_types == "calls":
        option_types_title = "Filtered for calls only"

    elif position_types == "puts":
        option_types_title = "Filtered for puts only"

    elif position_types == "all":
        option_types_title = "All contracts, puts and calls combined"



    # Ensure that the DataFrame values are floats
    metrics[type] = metrics[type].astype(float)
    metrics['strike_price'] = metrics['strike_price'].astype(float)
    metrics['expiration_date_original'] = metrics['expiration_date_original'].astype(str)
    z = metrics.pivot(index='strike_price', columns='expiration_date_original', values=type).values

    if type != "GEX":
        colorscale = [
            [0.0, 'red'],  # Lowest value
            [0.5, 'white'],  # Mid value at zero
            [1.0, 'green'],  # Highest value
        ]

    # Calculate the mid value for the color scale (centered at zero)
    zmin = metrics[type].min()
    zmax = metrics[type].max()
    val_range = max(abs(zmin), abs(zmax))



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
        hovertemplate='Expiration: %{x}<br>Strike: %{y}<br>' + type + ': %{text}<extra></extra>'
    ))


    fig.update_layout(
        title=dict(
            text=(
                  f"{dynamic_title}"
                  f"<br><span style='font-size:20px;'>As of {effective_datetime}</span>"
                  f"<br><span style='font-size:20px;'>{option_types_title}</span>"

            ),
            font=dict(family="Noto Sans SemiBold", color="white"),
            y=0.96,  # Adjust to control the vertical position of the title
            x=0.0,

            # xanchor='left',
            # yanchor='top',
            pad=dict(t=10, b=10, l=40)  # Adjust padding around the title
        ),
        # width=width,
        # height=height,

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
        # paper_bgcolor='white',  # Set paper background to white
        plot_bgcolor='white',
        paper_bgcolor='#053061',  # Dark blue background

    )

    fig.add_layout_image(
        dict(
            source=img2,
            xref="paper",
            yref="paper",
            x=1,
            y=1.11,
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


    title = f"📊 {session_date} Intraday DepthView"

    # Define the Eastern Time zone, Convert UTC time to Eastern Time, then Format the time in a friendly way
    current_time = datetime.utcnow()
    eastern_tz = pytz.timezone('America/New_York')
    eastern_time = current_time.replace(tzinfo=pytz.utc).astimezone(eastern_tz)
    friendly_time = eastern_time.strftime("%B %d, %Y at %I:%M %p %Z")
    fields = [
        {"name": "⏰ As of:", "value": as_of_time_stamp, "inline": True},
    ]
    footer_text = f"Generated on {friendly_time} | By OptionsDepth Inc."

    # Prepare the embed
    embed = {
        "title": title,
        "color": 3447003,
        "fields": fields,
        "footer": {"text": footer_text},
        "image": {"url": "attachment://depthview.png"}  # Reference the attached image
    }

    # Convert Plotly figure to image bytes
    img_bytes = fig.to_image(format="png", scale=3)

    # Prepare the payload
    payload = {
        # "content": "🚀[UPDATE]: New Gamma Heatmap analysis is ready!",
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

