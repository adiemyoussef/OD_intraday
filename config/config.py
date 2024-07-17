#---------------- DIGTIAL OCEAN ---------------------------#
# - MySQL
DB_HOST = 'db-mysql-nyc3-94751-do-user-12097851-0.b.db.ondigitalocean.com'
DB_PORT = '25060'
DB_USER = 'doadmin'
DB_PASSWORD = 'AVNS_lRx29D8jydyJ2lmQhan'
DB_NAME = 'defaultdb'
DB_DRIVER = 'mysql+pymysql'


# - Spaces configuration
DO_SPACES_URL = 'https://nyc3.digitaloceanspaces.com'  # Replace 'nyc3' with your region if different
DO_SPACES_KEY = 'DO00EMRQHFB38ZQZRGA7'
DO_SPACES_SECRET = 'gHFYFlzQ1T73gAY7BKRNtgx5hlEMMAB2YZIv+YyQUps'
DO_SPACES_BUCKET = 'intraday'
LOG_FILE_KEY = 'seen_files_log.json'

#---------------- SFTP ---------------------------#
#- Server
SFTP_HOST = "sftp.datashop.livevol.com"
SFTP_PORT = 22
SFTP_USERNAME = "contact_optionsdepth_com"
SFTP_PASSWORD = "Salam123+-"
SFTP_DIRECTORY = "/subscriptions/order_000059435/item_000068201"

#- Monitoring
SFTP_BASE_SLEEP_TIME = 10                    # Base sleep time in seconds
SFTP_REDUCED_SLEEP_TIME = 5                  # Reduced sleep time when expecting a file
SFTP_EXPECTATION_WINDOW = 120                # Time window in seconds when expecting a file (2 minutes)
SFTP_MAX_RETRY_ATTEMPTS = 3                  # Maximum number of retry attempts for SFTP operations


#---------------- RABBITMQ ---------------------------#
# Heartbeat configurations
RABBITMQ_HOST = "localhost"                         # Replace with your RabbitMQ host if different
RABBITMQ_CBOE_QUEUE = "youssef_local"               # Your queue name
RABBITMQ_HEARTBEAT_QUEUE = 'heartbeats'             # Name of the queue that receives the hearbeats - Work around to prevent Broken Pipe Error (https://stackoverflow.com/questions/45064662/rabbitmq-broken-pipe-error-or-lost-messages)

RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD = 1000          # Alert when queue size exceeds this number
RABBITMQ_HEARTBEAT_INTERVAL = 60                    # Send heartbeat every HEARTBEAT_INTERVAL seconds to keep the connection alive
RABBITMQ_CLEAR_HEARTBEAT_INTERVAL = 3600            # Clear heartbeat queue every CLEAR_HEARTBEAT_INTERVAL seconds to prevent the message from accumulating


#---------------- DISCORD ---------------------------#
# Bot
WEBHOOK_URL = 'https://discord.com/api/webhooks/1251013946111164436/VN55yOK-ntil-PnZn1gzWHzKwzDklwIh6fVspA_I8MCCaUnG-hsRsrP1t_WsreGHIity'




