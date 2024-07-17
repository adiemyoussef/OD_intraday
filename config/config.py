import os
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
load_dotenv()

#---------------- DIGITAL OCEAN ---------------------------#
# - MySQL
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_DRIVER = os.getenv('DB_DRIVER')

# - Spaces configuration
DO_SPACES_URL = os.getenv('DO_SPACES_URL')
DO_SPACES_KEY = os.getenv('DO_SPACES_KEY')
DO_SPACES_SECRET = os.getenv('DO_SPACES_SECRET')
DO_SPACES_BUCKET = os.getenv('DO_SPACES_BUCKET')
LOG_FILE_KEY = os.getenv('LOG_FILE_KEY')

#---------------- SFTP ---------------------------#
#- Server
SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_PORT = int(os.getenv('SFTP_PORT'))
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
RABBITMQ_HOST = "localhost"                         # Replace with your RabbitMQ host if different
RABBITMQ_CBOE_QUEUE = "youssef_local"               # Your queue name
RABBITMQ_HEARTBEAT_QUEUE = 'heartbeats'             # Name of the queue that receives the hearbeats - Work around to prevent Broken Pipe Error (https://stackoverflow.com/questions/45064662/rabbitmq-broken-pipe-error-or-lost-messages)

RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD = 1000          # Alert when queue size exceeds this number
RABBITMQ_HEARTBEAT_INTERVAL = 60                    # Send heartbeat every HEARTBEAT_INTERVAL seconds to keep the connection alive
RABBITMQ_CLEAR_HEARTBEAT_INTERVAL = 3600            # Clear heartbeat queue every CLEAR_HEARTBEAT_INTERVAL seconds to prevent the message from accumulating


#---------------- DISCORD ---------------------------#
# Bot
WEBHOOK_URL = 'https://discord.com/api/webhooks/1251013946111164436/VN55yOK-ntil-PnZn1gzWHzKwzDklwIh6fVspA_I8MCCaUnG-hsRsrP1t_WsreGHIity'




