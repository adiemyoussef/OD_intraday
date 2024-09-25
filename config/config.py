import pandas_market_calendars as mcal
import os
from prefect.blocks.system import JSON, Secret
from PIL import Image
from enum import Enum
nyse = mcal.get_calendar('NYSE')

def load_db_config():
    return JSON.load("database-credentials").value

def load_postgre_db_config():
    return JSON.load("postgre-credentials").value

def load_postgre_STAGE_db_config():
    return JSON.load("postgre-stage").value

def load_postgre_PROD_db_config():
    return JSON.load("postgre-prod").value

def load_sftp_config():
    return JSON.load("sftp-credentials").value

def load_rabbitmq_config():
    return JSON.load("rabbitmq-config").value

def load_do_spaces_config():
    return JSON.load("do-spaces-config").value

def load_discord_webhook():
    return Secret.load("discord-webhook-url").get()

def load_other_config():
    return JSON.load("other-config").value

def load_logo(path):
    try:
        return Image.open(path)
    except FileNotFoundError:
        print(f"Warning: Logo file not found at {path}. Using placeholder.")
        return Image.new('RGBA', (100, 100), color=(73, 109, 137))


# Load configurations
postgresql_config = load_postgre_db_config()
postgresql_STAGE_config = load_postgre_STAGE_db_config()
postgresql_PROD_config = load_postgre_PROD_db_config()
db_config = load_db_config()
sftp_config = load_sftp_config()
rabbitmq_config = load_rabbitmq_config()
do_spaces_config = load_do_spaces_config()
discord_webhook = load_discord_webhook()
other_config = load_other_config()

# MySQL Database Configuration
DB_HOST = db_config['host']
DB_PORT = db_config['port']
DB_USER = db_config['user']
DB_PASSWORD = db_config['password']
DB_NAME = db_config['database']
DB_DRIVER = db_config['driver']

# Posgre Test Database Configuration
POSGRE_DB_HOST = postgresql_config['host']
POSGRE_DB_PORT = postgresql_config['port']
POSGRE_DB_USER = postgresql_config['user']
POSGRE_DB_PASSWORD = postgresql_config['password']
POSGRE_DB_NAME = postgresql_config['database']
POSGRE_DB_DRIVER = postgresql_config['driver']

# Posgre STAGE Database Configuration
POSGRE_STAGE_DB_HOST = postgresql_STAGE_config['host']
POSGRE_STAGE_DB_PORT = postgresql_STAGE_config['port']
POSGRE_STAGE_DB_USER = postgresql_STAGE_config['user']
POSGRE_STAGE_DB_PASSWORD = postgresql_STAGE_config['password']
POSGRE_STAGE_DB_NAME = postgresql_STAGE_config['database']
POSGRE_STAGE_DB_DRIVER = postgresql_STAGE_config['driver']

# Posgre PROD Database Configuration
POSGRE_PROD_DB_HOST = postgresql_PROD_config['host']
POSGRE_PROD_DB_PORT = postgresql_PROD_config['port']
POSGRE_PROD_DB_USER = postgresql_PROD_config['user']
POSGRE_PROD_DB_PASSWORD = postgresql_PROD_config['password']
POSGRE_PROD_DB_NAME = postgresql_PROD_config['database']
POSGRE_PROD_DB_DRIVER = postgresql_PROD_config['driver']


# Digital Ocean Spaces Configuration
DO_SPACES_URL = do_spaces_config['url']
DO_SPACES_KEY = do_spaces_config['key']
DO_SPACES_SECRET = do_spaces_config['secret']
DO_SPACES_BUCKET = do_spaces_config['bucket']
LOG_FILE_KEY = other_config.get('log_file_key', 'seen_files_log.json')

# SFTP Configuration
SFTP_HOST = sftp_config['host']
SFTP_PORT = sftp_config['port']
SFTP_USERNAME = sftp_config['username']
SFTP_PASSWORD = sftp_config['password']
SFTP_DIRECTORY = sftp_config['directory']
SFTP_TIME_OUT = 600
# SFTP Monitoring
SFTP_BASE_SLEEP_TIME = other_config.get('sftp_base_sleep_time', 10)
SFTP_REDUCED_SLEEP_TIME = other_config.get('sftp_reduced_sleep_time', 5)
SFTP_EXPECTATION_WINDOW = other_config.get('sftp_expectation_window', 120)
SFTP_MAX_RETRY_ATTEMPTS = other_config.get('sftp_max_retry_attempts', 3)

# RabbitMQ Configuration
RABBITMQ_HOST = rabbitmq_config['host']
RABBITMQ_CBOE_QUEUE = rabbitmq_config['cboe_queue']
RABBITMQ_HEARTBEAT_QUEUE = rabbitmq_config['heartbeat_queue']
RABBITMQ_PORT = rabbitmq_config['port']
RABBITMQ_USER = rabbitmq_config['user']
RABBITMQ_PASS = rabbitmq_config['password']
RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD = rabbitmq_config['queue_size_alert_threshold']
RABBITMQ_HEARTBEAT_INTERVAL = rabbitmq_config['heartbeat_interval']
RABBITMQ_CLEAR_HEARTBEAT_INTERVAL = rabbitmq_config['clear_heartbeat_interval']
RABBITMQ_MAX_RUNTIME = rabbitmq_config['max_runtime']
RABBITMQ_MAX_ACK_RETRIES = rabbitmq_config['max_ack_retries']
PROCESS_MESSAGE_QUEUE_RETRY_DELAY = other_config.get('process_message_queue_retry_delay', 5)

#---------------------- DISCORD -------------------------#
# Discord Configuration
WEBHOOK_URL = discord_webhook
DEV_CHANNEL ='https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
HEATMAP_CHANNEL = 'https://discord.com/api/webhooks/1278125396671332393/Y02pRK5XTUKURHoSF6tIlSDzHBPUzUqDHzA8ybsat4Z-zCN8EeyXmyjir7SwMB_OQm42'
CHARM_HEATMAP_CHANNEL = 'https://discord.com/api/webhooks/1281065101805359134/pJzUD5GQufw3W9wUa4E9_GbwcZPgAsx61A6JssGiNbebUZ94SyOkWk83FurbEpxvFeb4'

WEBHOOK_URLS = {
    'zero_dte': 'https://discord.com/api/webhooks/zero_dte_webhook_url',
    'one_dte': 'https://discord.com/api/webhooks/one_dte_webhook_url',
    'gex': 'https://discord.com/api/webhooks/gex_webhook_url',
    'depthview': 'https://discord.com/api/webhooks/depthview_webhook_url',
    'options_charts': 'https://discord.com/api/webhooks/options_charts_webhook_url',
    'dev': 'https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
}

class WebhookUrl(Enum):
    DEFAULT = 'https://discord.com/api/webhooks/1274040299735486464/Tp8OSd-aX6ry1y3sxV-hmSy0J3UDhQeyXQbeLD1T9XF5zL4N5kJBBiQFFgKXNF9315xJ'
    URL_1 = 'https://discord.com/api/webhooks/your-webhook-url-1'
    URL_2 = 'https://discord.com/api/webhooks/your-webhook-url-2'
    # Add more webhook URLs as needed

START_TIME_PRE_MARKET = '15:00:00'
START_TIME_MARKET = '15:00:00'
DEFAULT_POS_TYPES = ['Net', 'C','P']

#--------------- DATA PROCESSING ---------------------#
# Data Processing
OPTION_SYMBOLS_TO_PROCESS = other_config.get('option_symbols_to_process', ['SPX', 'SPXW'])
CSV_CHUNKSIZE = other_config.get('csv_chunksize', 100000)

# Keep the rest of your configuration as is
INITAL_BOOK_KEY = ['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']
INTRADAY_KEY = ['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date']
MERGED_BOOK_KEY = ['ticker', 'option_symbol', 'call_put_flag', 'strike_price', 'expiration_date_original']

INTRADAY_REQUIRED_COLUMNS = [
    'ticker', 'option_symbol', 'call_put_flag', 'expiration_date', 'strike_price',
    'mm_buy_vol', 'mm_sell_vol',
    'firm_open_buy_vol', 'firm_close_buy_vol', 'firm_open_sell_vol', 'firm_close_sell_vol',
    'bd_open_buy_vol', 'bd_close_buy_vol', 'bd_open_sell_vol', 'bd_close_sell_vol',
    'cust_lt_100_open_buy_vol', 'cust_lt_100_close_buy_vol', 'cust_lt_100_open_sell_vol', 'cust_lt_100_close_sell_vol',
    'cust_100_199_open_buy_vol', 'cust_100_199_close_buy_vol', 'cust_100_199_open_sell_vol', 'cust_100_199_close_sell_vol',
    'cust_gt_199_open_buy_vol', 'cust_gt_199_close_buy_vol', 'cust_gt_199_open_sell_vol', 'cust_gt_199_close_sell_vol',
    'procust_lt_100_open_buy_vol', 'procust_lt_100_close_buy_vol', 'procust_lt_100_open_sell_vol', 'procust_lt_100_close_sell_vol',
    'procust_100_199_open_buy_vol', 'procust_100_199_close_buy_vol', 'procust_100_199_open_sell_vol', 'procust_100_199_close_sell_vol',
    'procust_gt_199_open_buy_vol', 'procust_gt_199_close_buy_vol', 'procust_gt_199_open_sell_vol', 'procust_gt_199_close_sell_vol'
]

VALID_SECURITY_TYPES = {1, 2, 3, 4}
VALID_CALL_PUT_FLAGS = {'P', 'C'}
VALID_SERIES_TYPES = {'S', 'N'}

# Simulations
HEATMAP_TIME_STEPS = 5
HEATMAP_PRICE_STEPS = 2.5
HEATMAP_PRICE_RANGE = 0.025

# Constants
SPX_TICKER = 'SPX'
HAT_SPX_TICKER = '^SPX'
YAHOO_SPX_TICKER = "^GSPC"
RISK_FREE_RATE = 0.055

#------------------- VISUALS ------------------- #
# Charting
IMAGE_WIDTH = 1440
IMAGE_HEIGTH = 810
SCALE_FACTOR = 3
COLOR_SCALE = "RdBu"
COLOR_SCALE_CUSTOM = [[0.0, "rgb(0, 59, 99)"],
                      [0.499, "rgb(186, 227, 255)"],
                      [0.501, "rgb(255, 236, 196)"],
                      [1.0, "rgb(255, 148, 71)"]]
BACKGROUND_COLOR = 'white'
TEXT_COLOR = 'black'
POSITION_COULORS = ['black', 'red', 'green']
FLOW_COLORS = ['magenta', 'yellow', 'grey']
BAR_POSN_COLORS = {
    'Net': {
        'negative': 'rgb(0,149,255)',  # light blue
        'positive': 'rgb(0,149,255)'  # light blue

    },
    'C': {
        'negative': 'rgb(0,217,51)',  # dark green
        'positive': 'rgb(0,217,51)'  # light green
    },
    'P': {
        'negative': 'rgb(204,3,0)',  # dark red
        'positive': 'rgb(204,3,0)'  # light red
    }
}
# Watermarks
LOGO_dark = os.path.join('..', 'config', 'images', 'logo_dark.png')
LOGO_light = os.path.join('..', 'config', 'images', 'logo_light.png')

GEX_0DTE = os.path.join('..', 'config', 'images', 'GEX_0DTE_exp.png')
GEX_upcoming_exp = os.path.join('..', 'config', 'images', 'GEX_upcoming_exp.png')

#POS_0DTE = os.path.join('..', 'config', 'images', 'position_0DTE_exp.png')
POS_0DTE = os.path.join('config', 'images', 'position_0DTE_exp.png')
POS_UPCOMING_EXP = os.path.join('..', 'config', 'images', 'position_upcoming_exp.png')



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