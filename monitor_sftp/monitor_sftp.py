import time
import json
import pika
import boto3
import asyncio
import aiohttp
import discord
import asyncssh
import traceback
import logging
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError, ClientError
import pandas_market_calendars as mcal

#------- Local modules --------#
from config.config import *
from utilities.customized_logger import DailyRotatingFileHandler


#-------------- INITIALIZATIONS ----------------#
# Set debug mode
DEBUG = False

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
handler = DailyRotatingFileHandler(base_filename="logs/sftp_monitor", when='midnight', interval=1, backupCount=7)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Add console handler for immediate feedback
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Initialize S3 client
s3_client = boto3.client(
    's3',
    endpoint_url=DO_SPACES_URL,
    aws_access_key_id=DO_SPACES_KEY,
    aws_secret_access_key=DO_SPACES_SECRET
)

# Setup the NYSE calendar
nyse = mcal.get_calendar('NYSE')


#----------------- FUNCTIONS ------------------#
async def adaptive_sleep():
    """
    Calculate adaptive sleep time based on current time and configured parameters.

    Returns:
        int: Number of seconds to sleep.
    """
    now = datetime.now()
    minutes, seconds = now.minute, now.second
    seconds_to_next_10min = 600 - ((minutes % 10) * 60 + seconds)

    if 0 <= seconds_to_next_10min <= SFTP_EXPECTATION_WINDOW:
        return SFTP_REDUCED_SLEEP_TIME
    else:
        sleep_time = seconds_to_next_10min - SFTP_EXPECTATION_WINDOW
        if sleep_time <= 0:
            sleep_time += 600
        return min(sleep_time, SFTP_BASE_SLEEP_TIME)


async def send_discord_notification(message, is_error=False):
    """
    Send a notification to Discord.

    Args:
        message (str): The message to send.
        is_error (bool): Whether the message is an error notification.
    """
    async with aiohttp.ClientSession() as session:
        webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
        embed = discord.Embed(
            title="🚨 MONITOR_SFTP ALERT 🚨" if is_error else "ℹ️ MONITOR_SFTP INFO ℹ️",
            description=message,
            color=discord.Color.red() if is_error else discord.Color.blue()
        )
        await webhook.send(embed=embed)


def send_discord_notification_sync(message, is_error=False):
    """
    Synchronous wrapper for send_discord_notification.

    Args:
        message (str): The message to send.
        is_error (bool): Whether the message is an error notification.
    """
    asyncio.run(send_discord_notification(message, is_error))


def load_seen_files():
    """
    Load the list of seen files from DigitalOcean Spaces.

    Returns:
        list: List of seen files.
    """
    try:
        response = s3_client.get_object(Bucket=DO_SPACES_BUCKET, Key=LOG_FILE_KEY)
        seen_files_log = json.loads(response['Body'].read().decode('utf-8'))
        return seen_files_log
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"State file {LOG_FILE_KEY} not found in bucket. Creating a new one.")
        create_initial_log_file()
        return []
    except (NoCredentialsError, ClientError) as e:
        logger.error(f"Error loading seen files from DigitalOcean Spaces: {e}")
        return []


def create_initial_log_file():
    """Create an initial empty log file in DigitalOcean Spaces."""
    try:
        s3_client.put_object(
            Bucket=DO_SPACES_BUCKET,
            Key=LOG_FILE_KEY,
            Body=json.dumps([]).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Created initial log file {LOG_FILE_KEY} in DigitalOcean Spaces.")
    except (NoCredentialsError, ClientError) as e:
        logger.error(f"Error creating initial log file in DigitalOcean Spaces: {e}")


def save_seen_files(seen_files):
    """
    Save the list of seen files to DigitalOcean Spaces.

    Args:
        seen_files (list): List of seen files to save.
    """
    try:
        s3_client.put_object(
            Bucket=DO_SPACES_BUCKET,
            Key=LOG_FILE_KEY,
            Body=json.dumps(seen_files).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Saved {len(seen_files)} seen files to DigitalOcean Spaces.")
    except (NoCredentialsError, ClientError) as e:
        logger.error(f"Error saving seen files to DigitalOcean Spaces: {e}")


def get_rabbitmq_connection(max_retries=5, retry_delay=5):
    """
    Establish a connection to RabbitMQ.

    Args:
        max_retries (int): Maximum number of connection attempts.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        tuple: (connection, channel) for RabbitMQ.

    Raises:
        Exception: If unable to connect after max_retries.
    """
    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            channel = connection.channel()

            for queue in [RABBITMQ_CBOE_QUEUE, RABBITMQ_HEARTBEAT_QUEUE]:
                try:
                    channel.queue_declare(queue=queue, passive=True)
                    logger.info(f"Queue '{queue}' exists.")
                except pika.exceptions.ChannelClosedByBroker:
                    logger.warning(f"Queue '{queue}' does not exist. Ensuring it's created with correct properties.")
                    channel = connection.channel()
                    utils.declare_queue(channel)

            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"Failed to connect to RabbitMQ (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    raise Exception("Failed to connect to RabbitMQ after multiple attempts")


def check_queue_status(channel):
    """
    Check the status of the RabbitMQ queue.

    Args:
        channel: RabbitMQ channel.

    Returns:
        int: Number of messages in the queue, or None if there's an error.
    """
    try:
        queue_info = channel.queue_declare(queue=RABBITMQ_CBOE_QUEUE, passive=True)
        message_count = queue_info.method.message_count
        logger.info(f"Current message count in queue '{RABBITMQ_CBOE_QUEUE}': {message_count}")
        if message_count > RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD:
            send_discord_notification_sync(
                f"Queue '{RABBITMQ_CBOE_QUEUE}' size ({message_count}) exceeds threshold ({RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD})")
        return message_count
    except Exception as e:
        logger.error(f"Error checking queue status: {str(e)}")
        return None


def publish_to_rabbitmq(channel, message):
    """
    Publish a message to RabbitMQ.

    Args:
        channel: RabbitMQ channel.
        message (str): Message to publish.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_CBOE_QUEUE,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='text/plain',
                content_encoding='utf-8'
            ),
        )
        logger.info(f"Published message to queue '{RABBITMQ_CBOE_QUEUE}': {message[:100]}...")
        return True
    except Exception as e:
        logger.error(f"Failed to publish message: {str(e)}")
        return False


async def send_heartbeat(channel):
    """
    Send periodic heartbeats to RabbitMQ.

    Args:
        channel: RabbitMQ channel.
    """
    last_clear_time = time.time()
    while True:
        try:
            heartbeat_message = json.dumps({
                "type": "heartbeat",
                "timestamp": datetime.now().isoformat()
            })
            channel.basic_publish(
                exchange='',
                routing_key=RABBITMQ_HEARTBEAT_QUEUE,
                body=heartbeat_message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.debug("Sent heartbeat to queue")

            current_time = time.time()
            if current_time - last_clear_time >= RABBITMQ_CLEAR_HEARTBEAT_INTERVAL:
                channel.queue_purge(RABBITMQ_HEARTBEAT_QUEUE)
                logger.info(f"Cleared heartbeat queue after {RABBITMQ_CLEAR_HEARTBEAT_INTERVAL} seconds")
                last_clear_time = current_time

        except Exception as e:
            logger.warning(f"Error sending heartbeat: {e}")

        await asyncio.sleep(RABBITMQ_HEARTBEAT_INTERVAL)


async def monitor_sftp(sftp_host, sftp_port, sftp_username, sftp_password, sftp_directory):
    """
    Main function to monitor SFTP server for new files.

    Args:
        sftp_host (str): SFTP server hostname.
        sftp_port (int): SFTP server port.
        sftp_username (str): SFTP username.
        sftp_password (str): SFTP password.
        sftp_directory (str): Directory to monitor on SFTP server.
    """
    logger.info("Starting SFTP monitoring...")

    seen_files = set(load_seen_files())
    logger.info(f"Loaded {len(seen_files)} log entries from state.")

    connection, channel = get_rabbitmq_connection()
    logger.info("RabbitMQ connection established.")

    heartbeat_task = asyncio.create_task(send_heartbeat(channel))

    last_successful_check = datetime.now()

    try:
        while True:
            try:
                async with asyncssh.connect(sftp_host, port=sftp_port, username=sftp_username,
                                            password=sftp_password, known_hosts=None) as conn:
                    async with conn.start_sftp_client() as sftp:
                        logger.info("SFTP connection established.")

                        while True:
                            try:
                                start_time = time.time()

                                files = await sftp.listdir(sftp_directory)
                                new_files = [f for f in files if f not in seen_files and f not in {'.', '..'}]

                                for filename in new_files:
                                    file_path = f"{sftp_directory}/{filename}"
                                    file_attr = await sftp.stat(file_path)

                                    logger.info(f'{filename} not in seen files')
                                    file_info_str = f"File: {filename}, Path: {file_path}, Timestamp: {datetime.fromtimestamp(file_attr.mtime).isoformat()}"

                                    if publish_to_rabbitmq(channel, file_info_str):
                                        seen_files.add(filename)
                                    else:
                                        logger.warning(f"Failed to publish message for file: {filename}. Retrying...")
                                        connection, channel = get_rabbitmq_connection()
                                        if publish_to_rabbitmq(channel, file_info_str):
                                            seen_files.add(filename)
                                        else:
                                            logger.error(f"Failed to publish message for file: {filename} after retry.")

                                if new_files:
                                    save_seen_files(list(seen_files))

                                last_successful_check = datetime.now()

                                sleep_time = await adaptive_sleep()
                                await asyncio.sleep(sleep_time)

                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                logger.error(f"Error during SFTP monitoring: {e}")
                                if datetime.now() - last_successful_check > timedelta(minutes=1):
                                    error_message = f"SFTP monitoring failed continuously for 5 minutes. Last error: {str(e)}"
                                    logger.critical(error_message)
                                    await send_discord_notification(error_message, is_error=True)
                                await asyncio.sleep(SFTP_BASE_SLEEP_TIME)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in SFTP connection: {e}")
                error_message = f"Failed to establish SFTP connection: {str(e)}"
                logger.critical(error_message)
                await send_discord_notification(error_message, is_error=True)
                await asyncio.sleep(SFTP_BASE_SLEEP_TIME)

    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

        if connection and connection.is_open:
            connection.close()
        logger.info("RabbitMQ connection closed and heartbeat task cancelled.")


async def debug_mode(channel):
    """
    Run the script in debug mode, sending test messages to RabbitMQ.

    Args:
        channel: RabbitMQ channel.
    """
    logger.info("Entering debug mode: sending test messages.")
    test_messages = [f"Test message {i}" for i in range(10)]
    for message in test_messages:
        if publish_to_rabbitmq(channel, message):
            logger.info(f"Successfully published: {message}")
        else:
            logger.error(f"Failed to publish: {message}")
        await asyncio.sleep(1)
    logger.info("Exiting debug mode.")


if __name__ == "__main__":
    connection = None
    try:
        connection, channel = get_rabbitmq_connection()
        if DEBUG:
            asyncio.run(debug_mode(channel))
        else:
            asyncio.run(monitor_sftp(sftp_host, sftp_port, sftp_username, sftp_password, sftp_directory))
    except KeyboardInterrupt:
        logger.info("Program interrupted by user. Shutting down...")
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()
        logger.critical(f"MONITOR_SFTP CRASH: {error_type} - {error_message}\n{error_traceback}")
        send_discord_notification_sync(f"MONITOR_SFTP CRASH: {error_type} - {error_message}\n{error_traceback}",
                                       is_error=True)
    finally:
        if connection and connection.is_open:
            connection.close()
            logger.info("RabbitMQ connection closed.")