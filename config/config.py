import os
import pandas_market_calendars as mcal
import os
from PIL import Image
nyse = mcal.get_calendar('NYSE')


#---------------- DIGITAL OCEAN ---------------------------#
# # - MySQL
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT',25060))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_DRIVER = os.getenv('DB_DRIVER')
#
# # - Spaces configuration
DO_SPACES_URL = os.getenv('DO_SPACES_URL')
DO_SPACES_KEY = os.getenv('DO_SPACES_KEY')
DO_SPACES_SECRET = os.getenv('DO_SPACES_SECRET')
DO_SPACES_BUCKET = os.getenv('DO_SPACES_BUCKET')
LOG_FILE_KEY = os.getenv('LOG_FILE_KEY')
#
# #---------------- SFTP ---------------------------#
# #- Server
SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_PORT = int(os.getenv('SFTP_PORT',22))
SFTP_USERNAME = os.getenv('SFTP_USERNAME')
SFTP_PASSWORD = os.getenv('SFTP_PASSWORD')
SFTP_DIRECTORY = os.getenv('SFTP_DIRECTORY')


#- Monitoring
SFTP_BASE_SLEEP_TIME = 10                    # Base sleep time in seconds
SFTP_REDUCED_SLEEP_TIME = 5                  # Reduced sleep time when expecting a file
SFTP_EXPECTATION_WINDOW = 120                # Time window in seconds when expecting a file (2 minutes)
SFTP_MAX_RETRY_ATTEMPTS = 3                  # Maximum number of retry attempts for SFTP operations


#---------------- RABBITMQ ---------------------------#
# Heartbeat configurations
RABBITMQ_HOST = "64.225.63.198"                         # Replace with your RabbitMQ host if different
RABBITMQ_CBOE_QUEUE = "youssef_local"              # Your queue name
RABBITMQ_HEARTBEAT_QUEUE = 'heartbeats'             # Name of the queue that receives the hearbeats - Work around to prevent Broken Pipe Error (https://stackoverflow.com/questions/45064662/rabbitmq-broken-pipe-error-or-lost-messages)
RABBITMQ_PORT = 5672
RABBITMQ_USER = "optionsdepth"
RABBITMQ_PASS = "Salam123+"

RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD = 1000          # Alert when queue size exceeds this number
RABBITMQ_HEARTBEAT_INTERVAL = 60                    # Send heartbeat every HEARTBEAT_INTERVAL seconds to keep the connection alive
RABBITMQ_CLEAR_HEARTBEAT_INTERVAL = 3600            # Clear heartbeat queue every CLEAR_HEARTBEAT_INTERVAL seconds to prevent the message from accumulating
PROCESS_MESSAGE_QUEUE_RETRY_DELAY = 5  # in seconds
RABBITMQ_MAX_RUNTIME = 3600  # 1 hour in seconds
RABBITMQ_MAX_ACK_RETRIES = 3
# RABBITMQ_HOST = "localhost"  # Replace with your RabbitMQ host if different
# QUEUE_NAME = "youssef_local"  # Your queue name

#---------------- DISCORD ---------------------------#
# Bot
WEBHOOK_URL = 'https://discord.com/api/webhooks/1251013946111164436/VN55yOK-ntil-PnZn1gzWHzKwzDklwIh6fVspA_I8MCCaUnG-hsRsrP1t_WsreGHIity'


#-------- DATA PROCESSING -----------#
OPTION_SYMBOLS_TO_PROCESS = ['SPX', 'SPXW']
CSV_CHUNKSIZE = 100000

# Check for uniqueness in key columns
INITAL_BOOK_KEY = ['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']
INTRADAY_KEY = ['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']
MERGED_BOOK_KEY = ['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date_original']
#FINAL_BOOK_KEY =
# Required columns for intraday data
INTRADAY_REQUIRED_COLUMNS = [
    'ticker',
    'option_symbol',
    'call_put_flag',  # Note: This was 'call_or_put' in your original list
    'expiration_date',
    'strike_price',
    'mm_buy_vol',
    'mm_sell_vol',
    'firm_open_buy_vol',
    'firm_close_buy_vol',
    'firm_open_sell_vol',
    'firm_close_sell_vol',
    'bd_open_buy_vol',
    'bd_close_buy_vol',
    'bd_open_sell_vol',
    'bd_close_sell_vol',
    'cust_lt_100_open_buy_vol',
    'cust_lt_100_close_buy_vol',
    'cust_lt_100_open_sell_vol',
    'cust_lt_100_close_sell_vol',
    'cust_100_199_open_buy_vol',
    'cust_100_199_close_buy_vol',
    'cust_100_199_open_sell_vol',
    'cust_100_199_close_sell_vol',
    'cust_gt_199_open_buy_vol',
    'cust_gt_199_close_buy_vol',
    'cust_gt_199_open_sell_vol',
    'cust_gt_199_close_sell_vol',
    'procust_lt_100_open_buy_vol',
    'procust_lt_100_close_buy_vol',
    'procust_lt_100_open_sell_vol',
    'procust_lt_100_close_sell_vol',
    'procust_100_199_open_buy_vol',
    'procust_100_199_close_buy_vol',
    'procust_100_199_open_sell_vol',
    'procust_100_199_close_sell_vol',
    'procust_gt_199_open_buy_vol',
    'procust_gt_199_close_buy_vol',
    'procust_gt_199_open_sell_vol',
    'procust_gt_199_close_sell_vol'
]

# Valid security types
VALID_SECURITY_TYPES = {1, 2, 3, 4}

# Valid call/put flags
VALID_CALL_PUT_FLAGS = {'P', 'C'}

# Valid series types
VALID_SERIES_TYPES = {'S', 'N'}


#----- SIMULATIONS ------------ #
HEATMAP_TIME_STEPS = 10
HEATMAP_PRICE_STEPS = 5
HEATMAP_PRICE_RANGE = 0.015

# CONSTANTES
SPX_TICKER = 'SPX'
HAT_SPX_TICKER = '^SPX'
YAHOO_SPX_TICKER = "^GSPC"
RISK_FREE_RATE = 0.055

# ----------- CHARTING ---------------#

#GRAPH DIMENSIONS
IMAGE_WIDTH = 1440  # Width in pixels
IMAGE_HEIGTH = 810  # Height in pixels
SCALE_FACTOR = 3  # Increase for better quality, especially for raster formats

# GRAPHS - Color palettes, fonts, etc.
COLOR_SCALE = "RdBu"
COLOR_SCALE_CUSTOM = [[0.0, "rgb(0, 59, 99)"],
                      [0.499, "rgb(186, 227, 255)"],
                      [0.501, "rgb(255, 236, 196)"],
                      [1.0, "rgb(255, 148, 71)"]]

BACKGROUND_COLOR = 'white'
TEXT_COLOR = 'black'

POSITION_COULORS = ['black', 'red', 'green']
FLOW_COLORS = ['magenta', 'yellow', 'grey']

#WATERMARKS
# If your image is located relative to the script's directory, you can use:
LOGO_dark = os.path.join('..', 'config', 'images', 'logo_dark.png')
LOGO_light = os.path.join('..', 'config', 'images', 'logo_light.png')


if __name__ == "__main__":
    print("#----------- VARIABLES ------------#")


    def print_var(name, value):
        print(f"{name}: {value} (Type: {type(value).__name__})")


    # Digital Ocean
    print_var("DB_HOST", DB_HOST)
    print_var("DB_PORT", DB_PORT)
    print_var("DB_USER", DB_USER)
    print_var("DB_NAME", DB_NAME)
    print_var("DB_DRIVER", DB_DRIVER)
    print_var("DO_SPACES_URL", DO_SPACES_URL)
    print_var("DO_SPACES_KEY", DO_SPACES_KEY)
    print_var("DO_SPACES_BUCKET", DO_SPACES_BUCKET)
    print_var("LOG_FILE_KEY", LOG_FILE_KEY)

    # SFTP
    print_var("SFTP_HOST", SFTP_HOST)
    print_var("SFTP_PORT", SFTP_PORT)
    print_var("SFTP_USERNAME", SFTP_USERNAME)
    print_var("SFTP_PASSWORD", SFTP_PASSWORD)
    print_var("SFTP_DIRECTORY", SFTP_DIRECTORY)
    print_var("SFTP_BASE_SLEEP_TIME", SFTP_BASE_SLEEP_TIME)
    print_var("SFTP_REDUCED_SLEEP_TIME", SFTP_REDUCED_SLEEP_TIME)
    print_var("SFTP_EXPECTATION_WINDOW", SFTP_EXPECTATION_WINDOW)
    print_var("SFTP_MAX_RETRY_ATTEMPTS", SFTP_MAX_RETRY_ATTEMPTS)

    # RabbitMQ
    print_var("RABBITMQ_HOST", RABBITMQ_HOST)
    print_var("RABBITMQ_CBOE_QUEUE", RABBITMQ_CBOE_QUEUE)
    print_var("RABBITMQ_HEARTBEAT_QUEUE", RABBITMQ_HEARTBEAT_QUEUE)
    print_var("RABBITMQ_PORT", RABBITMQ_PORT)
    print_var("RABBITMQ_USER", RABBITMQ_USER)
    print_var("RABBITMQ_PASS", RABBITMQ_PASS)
    print_var("RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD", RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD)
    print_var("RABBITMQ_HEARTBEAT_INTERVAL", RABBITMQ_HEARTBEAT_INTERVAL)
    print_var("RABBITMQ_CLEAR_HEARTBEAT_INTERVAL", RABBITMQ_CLEAR_HEARTBEAT_INTERVAL)
    print_var("PROCESS_MESSAGE_QUEUE_RETRY_DELAY", PROCESS_MESSAGE_QUEUE_RETRY_DELAY)
    print_var("RABBITMQ_MAX_RUNTIME", RABBITMQ_MAX_RUNTIME)
    print_var("RABBITMQ_MAX_ACK_RETRIES", RABBITMQ_MAX_ACK_RETRIES)

    # Discord
    print_var("WEBHOOK_URL", WEBHOOK_URL)

    # # Data Processing
    # print_var("OPTION_SYMBOLS_TO_PROCESS", OPTION_SYMBOLS_TO_PROCESS)
    # print_var("CSV_CHUNKSIZE", CSV_CHUNKSIZE)
    # print_var("INTRADAY_REQUIRED_COLUMNS", INTRADAY_REQUIRED_COLUMNS)
    # print_var("VALID_SECURITY_TYPES", VALID_SECURITY_TYPES)
    # print_var("VALID_CALL_PUT_FLAGS", VALID_CALL_PUT_FLAGS)
    # print_var("VALID_SERIES_TYPES", VALID_SERIES_TYPES)

    # # Simulations
    # print_var("HEATMAP_TIME_STEPS", HEATMAP_TIME_STEPS)
    # print_var("HEATMAP_PRICE_STEPS", HEATMAP_PRICE_STEPS)
    # print_var("HEATMAP_PRICE_RANGE", HEATMAP_PRICE_RANGE)
    #
    # # Constants
    # print_var("SPX_TICKER", SPX_TICKER)
    # print_var("HAT_SPX_TICKER", HAT_SPX_TICKER)
    # print_var("YAHOO_SPX_TICKER", YAHOO_SPX_TICKER)
    # print_var("RISK_FREE_RATE", RISK_FREE_RATE)
    #
    # # Charting
    # print_var("IMAGE_WIDTH", IMAGE_WIDTH)
    # print_var("IMAGE_HEIGTH", IMAGE_HEIGTH)
    # print_var("SCALE_FACTOR", SCALE_FACTOR)
    # print_var("COLOR_SCALE", COLOR_SCALE)
    # print_var("COLOR_SCALE_CUSTOM", COLOR_SCALE_CUSTOM)
    # print_var("BACKGROUND_COLOR", BACKGROUND_COLOR)
    # print_var("TEXT_COLOR", TEXT_COLOR)
    # print_var("POSITION_COULORS", POSITION_COULORS)
    # print_var("FLOW_COLORS", FLOW_COLORS)
    # print_var("LOGO_dark", LOGO_dark)
    # print_var("LOGO_light", LOGO_light)

    print("#---------------------------------------#")