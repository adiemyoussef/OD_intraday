import os
from dotenv import load_dotenv
import pandas_market_calendars as mcal

nyse = mcal.get_calendar('NYSE')
# Load environment variables from a .env file if it exists
# Load environment variables from a .env file if it exists
#dotenv_path = os.path.join(os.path.dirname(__file__), 'config.env')
dotenv_path = '/Users/youssefadiem/PycharmProjects/OptionsDepth_intraday/config.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

#---------------- DIGITAL OCEAN ---------------------------#
# # - MySQL
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT'))
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
SFTP_PORT = int(os.getenv('SFTP_PORT'))
SFTP_USERNAME = os.getenv('SFTP_USERNAME')
SFTP_PASSWORD = os.getenv('SFTP_PASSWORD_OD')
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

# RABBITMQ_HOST = "localhost"  # Replace with your RabbitMQ host if different
# QUEUE_NAME = "youssef_local"  # Your queue name

#---------------- DISCORD ---------------------------#
# Bot
WEBHOOK_URL = 'https://discord.com/api/webhooks/1251013946111164436/VN55yOK-ntil-PnZn1gzWHzKwzDklwIh6fVspA_I8MCCaUnG-hsRsrP1t_WsreGHIity'


#-------- DATA PROCESSING -----------#
OPTION_SYMBOLS_TO_PROCESS = ['SPX', 'SPXW'] #, 'VIX', 'VIXW']
CSV_CHUNKSIZE = 100000


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
HEATMAP_PRICE_RANGE = 0.02
