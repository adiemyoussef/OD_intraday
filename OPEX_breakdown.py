import sys
import warnings
import logging
import pandas as pd
from OptionsDepth import Options_data
import plotly.io as pio
import plotly.graph_objects as go
from datetime import datetime, timedelta, time
import pandas_market_calendars as mcal
import numpy as np
from PIL import Image
import yfinance as yf

od = Options_data()

# TODO: relative paths - MAC & Windows
img = Image.open(r"/Users/youssefadiem/PycharmProjects/OptionsDepth/config/images/logo_light.png")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s: %(lineno)s} %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
nyse = mcal.get_calendar('NYSE')
current_date = datetime.now().date()

def get_trading_day(cal, current_date):
    market_days = nyse.valid_days(start_date=(current_date - pd.to_timedelta('4D')), end_date=(current_date + pd.to_timedelta('4D'))).normalize().date

    is_business_day = current_date in market_days

    if is_business_day:
        return current_date
    else:
        return nyse.valid_days(start_date=(current_date), end_date=(current_date + pd.to_timedelta('5D')))[0].date()

def by_strike(book, participant, exp_date=None, time_exp="PM",y_min=None, y_max=None):

    breakpoint()
    if exp_date is not None:
        if time_exp == "AM":
            specific_hour = 9
            specific_minute = 15
        elif time_exp == "PM":
            specific_hour = 16
            specific_minute = 0
        else:
            specific_hour = 16
            specific_minute = 0

        as_of_date = book['as_of_date'].values[0].strftime('%Y-%m-%d')
        exp_date = datetime.combine(exp_date, time(specific_hour, specific_minute)).strftime('%Y-%m-%d %H:%M:%S')
        book = book.loc[book['expiration_date'] == exp_date]

    breakpoint()
    # Store the original number of rows
    original_row_count = len(book)

    # Remove duplicates and store the result in a new dataframe
    book = book.drop_duplicates()

    # Calculate the number of dropped rows
    dropped_row_count = original_row_count - len(book)

    # Print the number of dropped rows
    print(f"Number of duplicate rows dropped: {dropped_row_count}")

    breakpoint()
    calls = book[book["call_put_flag"] == "C"].groupby(['strike_price']).agg({participant: 'sum'}).reset_index()
    puts = book[book["call_put_flag"] == "P"].groupby(['strike_price']).agg({participant: 'sum'}).reset_index()

    # Sorting by strike price
    calls = calls.sort_values(by='strike_price').fillna(0)
    puts = puts.sort_values(by='strike_price').fillna(0)

    breakpoint()
    fig = go.Figure()

    as_of_date = "2024-09-19"
    exp_date_str = exp_date

    fig.add_trace(go.Bar(name='Calls', x=calls[participant], y=calls['strike_price'], orientation='h', marker_color='SeaGreen', offsetgroup=1))
    fig.add_trace(go.Bar(name='Puts', x=puts[participant], y=puts['strike_price'], orientation='h', marker_color='Red', offsetgroup=2))
    fig.update_layout(
        plot_bgcolor='rgba(255, 255, 255,0)',
        barmode='group',
        title=dict(
            text=f"SPX - Net {participant}' Exposure by Strike<br><sup> Contracts expiring on {exp_date_str} {time_exp if exp_date else ''}</sup> <br>",
            font=dict(size=35, family="Lato Black", color="rgb(7,43,67)"),
            yref='container',
            automargin=True,
            y=0.95,
            x=0.5,
            xanchor='center',
            yanchor='top',
        ),
        xaxis_title=f"Net {participant}' Exposure",
        yaxis_title="Strike Price",
        yaxis=dict(range=[y_min, y_max] if y_min is not None and y_max is not None else None),
        font=dict(
            family="Lato",
            color="black"
        ),
        margin=dict(l=100, r=50, t=150, b=50)
    )
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

    fig.write_html(f'./OPEX_Charts/{participant}_Exposure_{time_exp}_Expiration.html')

    fig.show()

    breakpoint()


if __name__ == "__main__":

    # df = pd.read_csv("/Users/youssefadiem/Downloads/OPEX_bucketed.csv")
    query = """
    SELECT
        as_of_date,
        effective_date,
        call_put_flag,
        expiration_date_original,
        expiration_date,
        strike_price,
        mm_posn as 'Market Makers', -- VolSignals
        procust_posn as 'Pro Customers', -- VolSignals
        nonprocust_posn as 'Non Pro Cust.', -- VolSignals
        broker_posn as 'Broker&Dealers', -- VolSignals
        firm_posn as 'Firms', -- VolSignals
        total_customers_posn as 'Customers' -- VolSignals


        FROM intraday.new_daily_book_format
        where effective_date = '2024-09-19'
        and
        expiration_date_original = '2024-09-20'

    """

    # query = """
    # SELECT
    # as_of_date,
    # effective_datetime,
    # effective_date,
    # call_put_flag,
    # expiration_date_original,
    # expiration_date,
    # strike_price,
    # mm_posn as 'Market Makers', -- VolSignals
    # procust_posn as 'Pro Customers', -- VolSignals
    # nonprocust_posn as 'Non Pro Cust.', -- VolSignals
    # broker_posn as 'Broker&Dealers', -- VolSignals
    # firm_posn as 'Firms', -- VolSignals
    # total_customers_posn as 'Customers' -- VolSignals
    # FROM intraday.intraday_books_test_posn
    # where effective_date ='2024-08-15'
    # and effective_datetime = '2024-08-15 18:00:00'
    # and
    # expiration_date_original = '2024-08-16'
    # """
    book = od.read_table(query) #

    #book.to_csv('/Users/youssefadiem/Downloads/OPEX_sillos_20240816_OPEX.csv')
    breakpoint()
    #breakpoint()
    # list of participant columns you want to visualize
    #participant_cols = ['mm_posn', 'broker_posn', 'total_customers_posn']
    #participant_cols = 'Non Pro Cust.'
    #participant_cols = 'Pro Customers'
    #participant_cols = 'Broker&Dealers'
    #participant_cols = 'Firms'
    #participant_cols = 'BrokerDealers&Firms'
    participant_cols = 'Customers'
    #participant_cols = 'MarketMakers&BrokerDealers&Firms'
    by_strike(book, participant_cols, exp_date=datetime(2024, 9, 20), time_exp="PM", y_min=5200, y_max=5850)


    breakpoint()

    # for participant in participants:
    #     by_strike(df, participant)
