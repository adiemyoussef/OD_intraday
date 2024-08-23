
def plot_gamma_intraday(df: pd.DataFrame, effective_datetime, spx: pd.DataFrame = None, save_fig=False, fig_show=False):
    prefect_logger = get_run_logger()

    # Pivot the dataframe to get the required format for heatmap
    df_heatmap = df.pivot(index='sim_datetime', columns='price', values='value')
    df_minima = df.pivot(index='sim_datetime', columns='price', values='minima')
    df_maxima = df.pivot(index='sim_datetime', columns='price', values='maxima')

    x = df_heatmap.index
    y = df_heatmap.columns.values
    z = df_heatmap.values.transpose()

    title_stamp = effective_datetime.strftime("%Y-%m-%d %H:%M")

    z_min = df_minima.values.transpose()
    z_max = df_maxima.values.transpose()

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
    # ----- Adding OHLC -----

    if spx is not None:
        prefect_logger.info("Entering OHLC overlay")
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
            layer="above")
    )
    fig.add_layout_image(
        dict(
            source=img,
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

    return fig

    return fig