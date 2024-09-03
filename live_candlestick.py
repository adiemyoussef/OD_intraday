import time
import logging
from polygon import RESTClient
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

client = RESTClient(api_key='nsb3PIJ5l5QnK8YVXHrWrnd0_SfCPsz9')
SLEEP_TIME = 0.1  # Update every second
TIME_FRAME = 1
LOOKBACK_PERIOD = 1

def get_latest_data(timespan="second", from_time=None):
    # now = datetime.now(timezone.utc)
    # from_date = now - timedelta(minutes=1, seconds=0)  # Look back 5 minutes to ensure we get the latest data
    # to_date = now
    now = datetime.now(timezone.utc)

    if timespan == "second":
        # For second data, look back to the start of the current minute
        from_time = now.replace(second=LOOKBACK_PERIOD, microsecond=0)
    elif from_time is None:
        # For minute data, use the provided from_time or default to start of current minute
        from_time = now.replace(second=LOOKBACK_PERIOD, microsecond=0)

    to_date = now
    """
    :param from_: The start of the aggregate time window as YYYY-MM-DD, a date, Unix MS Timestamp, or a datetime.
    :param to: The end of the aggregate time window as YYYY-MM-DD, a date, Unix MS Timestamp, or a datetime.

    """
    try:
        aggs = client.get_aggs(ticker="I:SPX", from_=from_time, to=to_date, adjusted=True, sort="desc", limit=1,
                               multiplier=1, timespan=timespan)
        if aggs:
            return aggs[0]
        else:
            logging.warning(f"No data received for timespan {timespan} from {from_time} to {to_date}")
            return None
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        time.sleep(LOOKBACK_PERIOD)
        return None

def print_candlestick(open_price, high, low, close, start_time):
    print(f"\nS&P 500 Candlestick for period starting at: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC: Open: {open_price:.2f} -- High: {high:.2f} -- Low: {low:.2f} -- Close: {close:.2f}")


def track_realtime_candlestick(timeframe_minutes=1):
    print(
        f"Tracking real-time S&P 500 candlestick data with {timeframe_minutes}-minute timeframe. Press Ctrl+C to stop.")

    current_period_start = None
    open_price = high = low = close = None
    data_missing_count = 0

    while True:
        now = datetime.now(timezone.utc)

        # Check if we're in a new period
        if current_period_start is None or now >= current_period_start + timedelta(minutes=timeframe_minutes):
            # Start of a new period
            current_period_start = now.replace(minute=now.minute - (now.minute % timeframe_minutes), second=0,
                                               microsecond=0)
            open_price = high = low = close = None

        latest_data = get_latest_data("second", current_period_start)

        if latest_data:
            data_missing_count = 0
            if open_price is None:
                open_price = latest_data.open
            high = latest_data.high if high is None else max(high, latest_data.high)
            low = latest_data.low if low is None else min(low, latest_data.low)
            close = latest_data.close
            print_candlestick(open_price, high, low, close, current_period_start)
        else:
            data_missing_count += 1
            if data_missing_count > 5:
                logging.warning(f"No data received for {data_missing_count} consecutive attempts")

        time.sleep(SLEEP_TIME)
        #     minute_data = get_latest_data("second")
        #
        #     if minute_data:
        #         open_price = minute_data.open
        #         high = minute_data.high
        #         low = minute_data.low
        #         close = minute_data.close
        #         print(
        #             f"\nNew {timeframe_minutes}-minute period started at {current_period_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        #         print_candlestick(open_price, high, low, close, current_period_start)
        #     else:
        #         print(
        #             f"\nWaiting for data for new period starting at {current_period_start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        #         open_price = high = low = close = None
        # else:
        #     # Update with latest data
        #     latest_data = get_latest_data("second")
        #
        #     if latest_data and open_price is not None:
        #         high = max(high, latest_data.high)
        #         low = min(low, latest_data.low)
        #         close = latest_data.close
        #         print_candlestick(open_price, high, low, close, current_period_start)
        #     elif open_price is None:
        #         print("Waiting for initial period data...")
        #
        #     else:
        #         print("Waiting for data update...")
        #         breakpoint()
        #
        # time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    try:
        track_realtime_candlestick(TIME_FRAME)
    except KeyboardInterrupt:
        logging.info("\nStopped tracking data.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")