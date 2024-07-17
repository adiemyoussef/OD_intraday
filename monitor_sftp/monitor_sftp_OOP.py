import asyncio
import traceback
import aiohttp
import asyncssh
import discord
import logging
import time
import json
import pika
from datetime import datetime, timedelta

# ------ Local Modules ------#
from utilities.db_utils import AsyncDatabaseUtilities
from utilities.s3_utils import S3Utilities
from utilities.misc_utils import get_eastern_time
from utilities.customized_logger import DailyRotatingFileHandler
from config.config import *

def setup_custom_logger(name, log_level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # File handler with DailyRotatingFileHandler
    file_handler = DailyRotatingFileHandler(
        base_filename=f"logs/{name}",
        when="midnight",
        interval=1,
        backupCount=7
    )
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger

class SFTPMonitor:
    def __init__(self, sftp_host, sftp_port, sftp_username, sftp_password, sftp_directory,
                 rabbitmq_host, rabbitmq_queue, heartbeat_queue, webhook_url,
                 s3_utils, db_utils, logger):
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_username = sftp_username
        self.sftp_password = sftp_password
        self.sftp_directory = sftp_directory
        self.rabbitmq_host = rabbitmq_host
        self.rabbitmq_queue = rabbitmq_queue
        self.heartbeat_queue = heartbeat_queue
        self.webhook_url = webhook_url
        self.s3_utils = s3_utils
        self.db_utils = db_utils
        self.logger = logger
        self.connection = None
        self.channel = None
        self.seen_files = set()

    async def adaptive_sleep(self):
        now = datetime.now()
        minutes, seconds = now.minute, now.second

        # Calculate seconds until the next minute
        seconds_to_next_minute = 60 - seconds

        # If we're between 18:00 and 20:20, use a longer sleep time
        if (now.hour == 18 and now.minute >= 0) or (now.hour == 19) or (now.hour == 20 and now.minute <= 20):
            return min(300, seconds_to_next_minute)  # Sleep for 5 minutes or until the next minute

        # Calculate minutes and seconds since the last 10-minute mark
        minutes_since_last_10 = minutes % 10
        seconds_since_last_10 = minutes_since_last_10 * 60 + seconds

        # If we're within the 1-3 minute window after a 10-minute mark
        if 60 <= seconds_since_last_10 < 180:
            return min(1, seconds_to_next_minute)  # Use a very short sleep time (1 second or less)

        # If we're approaching the next 10-minute mark (within 30 seconds)
        elif seconds_since_last_10 >= 570:  # 570 seconds = 9 minutes 30 seconds
            return min(1, seconds_to_next_minute)  # Use a very short sleep time (1 second or less)

        # For all other times
        else:
            # Calculate time to the next 10-minute mark
            seconds_to_next_10min = 600 - seconds_since_last_10
            sleep_time = max(1, min(seconds_to_next_10min - 30, SFTP_BASE_SLEEP_TIME))
            return min(sleep_time, seconds_to_next_minute)

    async def send_discord_notification(self, message, is_error=False):
        try:
            async with aiohttp.ClientSession() as session:
                webhook = discord.Webhook.from_url(self.webhook_url, session=session)
                embed = discord.Embed(
                    title="🚨 MONITOR_SFTP ALERT 🚨" if is_error else "ℹ️ MONITOR_SFTP INFO ℹ️",
                    description=message,
                    color=discord.Color.red() if is_error else discord.Color.blue()
                )
                await webhook.send(embed=embed)
        except Exception as e:
            self.logger.error(f"Failed to send Discord notification: {str(e)}")

    def get_rabbitmq_connection(self, max_retries=5, retry_delay=5):
        for attempt in range(max_retries):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host))
                channel = connection.channel()

                for queue in [self.rabbitmq_queue, self.heartbeat_queue]:
                    try:
                        channel.queue_declare(queue=queue, passive=True)
                        self.logger.info(f"Queue '{queue}' exists.")
                    except pika.exceptions.ChannelClosedByBroker:
                        self.logger.warning(f"Queue '{queue}' does not exist. Creating it.")
                        channel = connection.channel()
                        channel.queue_declare(queue=queue, durable=True)

                return connection, channel
            except pika.exceptions.AMQPConnectionError as e:
                self.logger.warning(f"Failed to connect to RabbitMQ (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise ConnectionError("Failed to connect to RabbitMQ after multiple attempts")

    def publish_to_rabbitmq(self, message):
        start_time = time.time()
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.rabbitmq_queue,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='text/plain',
                    content_encoding='utf-8'
                ),
            )
            end_time = time.time()
            self.logger.debug(f"Published message to queue '{self.rabbitmq_queue}' in {end_time - start_time:.4f} seconds")
            return True
        except Exception as e:
            end_time = time.time()
            self.logger.error(f"Failed to publish message in {end_time - start_time:.4f} seconds: {str(e)}")
            return False

    async def send_heartbeat(self):
        last_clear_time = time.time()
        while True:
            try:
                heartbeat_message = json.dumps({
                    "type": "heartbeat",
                    "timestamp": get_eastern_time()
                })
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.heartbeat_queue,
                    body=heartbeat_message,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                self.logger.debug("Sent heartbeat to queue")

                current_time = time.time()
                if current_time - last_clear_time >= RABBITMQ_CLEAR_HEARTBEAT_INTERVAL:
                    self.channel.queue_purge(self.heartbeat_queue)
                    self.logger.info(f"Cleared heartbeat queue after {RABBITMQ_CLEAR_HEARTBEAT_INTERVAL} seconds")
                    last_clear_time = current_time

            except Exception as e:
                self.logger.warning(f"Error sending heartbeat: {e}")

            await asyncio.sleep(RABBITMQ_HEARTBEAT_INTERVAL)

    async def monitor(self):
        self.logger.info("Starting SFTP monitoring...")

        start_time = time.time()
        self.seen_files = set(self.s3_utils.load_json(LOG_FILE_KEY))
        end_time = time.time()
        self.logger.debug(f"Loaded {len(self.seen_files)} log entries from state in {end_time - start_time:.4f} seconds")

        start_time = time.time()
        self.connection, self.channel = self.get_rabbitmq_connection()
        end_time = time.time()
        self.logger.debug(f"RabbitMQ connection established in {end_time - start_time:.4f} seconds")

        heartbeat_task = asyncio.create_task(self.send_heartbeat())

        last_successful_check = datetime.now()

        try:
            while True:
                try:
                    connect_start = time.time()
                    async with asyncssh.connect(self.sftp_host, port=self.sftp_port, username=self.sftp_username,
                                                password=self.sftp_password, known_hosts=None) as conn:
                        async with conn.start_sftp_client() as sftp:
                            connect_end = time.time()
                            self.logger.debug(f"SFTP connection established in {connect_end - connect_start:.4f} seconds")

                            while True:
                                try:
                                    cycle_start = time.time()
                                    listdir_start = time.time()
                                    files = await sftp.listdir(self.sftp_directory)
                                    listdir_end = time.time()
                                    self.logger.debug(f"SFTP directory listing took {listdir_end - listdir_start:.4f} seconds")

                                    new_files = [f for f in files if f not in self.seen_files and f not in {'.', '..'}]

                                    for filename in new_files:
                                        file_path = f"{self.sftp_directory}/{filename}"
                                        stat_start = time.time()
                                        file_attr = await sftp.stat(file_path)
                                        stat_end = time.time()
                                        self.logger.debug(f"File stat for {filename} took {stat_end - stat_start:.4f} seconds")

                                        file_mtime = datetime.fromtimestamp(file_attr.mtime)
                                        detection_time = datetime.now()
                                        detection_delay = (detection_time - file_mtime).total_seconds()
                                        self.logger.debug(f"File {filename} detected {detection_delay:.4f} seconds after last modification")

                                        self.logger.info(f'{filename} not in seen files')
                                        file_info = {
                                            "filename": filename,
                                            "path": file_path,
                                            "timestamp": file_mtime.isoformat(),
                                            "detection_time": detection_time.isoformat(),
                                            "detection_delay": detection_delay
                                        }
                                        file_info_str = json.dumps(file_info)

                                        publish_start = time.time()
                                        if self.publish_to_rabbitmq(file_info_str):
                                            self.seen_files.add(filename)
                                            publish_end = time.time()
                                            self.logger.debug(f"Message for {filename} published in {publish_end - publish_start:.4f} seconds")
                                        else:
                                            self.logger.warning(f"Failed to publish message for file: {filename}. Retrying...")
                                            self.connection, self.channel = self.get_rabbitmq_connection()
                                            retry_start = time.time()
                                            if self.publish_to_rabbitmq(file_info_str):
                                                self.seen_files.add(filename)
                                                retry_end = time.time()
                                                self.logger.debug(f"Message for {filename} published after retry in {retry_end - retry_start:.4f} seconds")
                                            else:
                                                self.logger.error(f"Failed to publish message for file: {filename} after retry.")

                                    if new_files:
                                        save_start = time.time()
                                        self.s3_utils.save_json(LOG_FILE_KEY, list(self.seen_files))
                                        save_end = time.time()
                                        self.logger.debug(f"Saved seen files to S3 in {save_end - save_start:.4f} seconds")

                                    last_successful_check = datetime.now()

                                    cycle_end = time.time()
                                    self.logger.debug(f"Full monitoring cycle took {cycle_end - cycle_start:.4f} seconds")

                                    sleep_time = await self.adaptive_sleep()
                                    self.logger.debug(f"Sleeping for {sleep_time} seconds")
                                    await asyncio.sleep(sleep_time)

                                except asyncio.CancelledError:
                                    raise
                                except Exception as e:
                                    self.logger.error(f"Error during SFTP monitoring: {e}")
                                    if datetime.now() - last_successful_check > timedelta(minutes=5):
                                        error_message = f"SFTP monitoring failed continuously for 5 minutes. Last error: {str(e)}"
                                        self.logger.critical(error_message)
                                        await self.send_discord_notification(error_message, is_error=True)
                                    await asyncio.sleep(SFTP_BASE_SLEEP_TIME)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"Error in SFTP connection: {e}")
                    error_message = f"Failed to establish SFTP connection: {str(e)}"
                    self.logger.critical(error_message)
                    await self.send_discord_notification(error_message, is_error=True)
                    await asyncio.sleep(SFTP_BASE_SLEEP_TIME)

        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

            if self.connection and self.connection.is_open:
                self.connection.close()
            self.logger.info("RabbitMQ connection closed and heartbeat task cancelled.")

    async def debug_mode(self):
        self.logger.info("Entering debug mode: sending test messages.")

        # Ensure RabbitMQ connection is established
        if not self.connection or not self.channel:
            self.connection, self.channel = self.get_rabbitmq_connection()

        test_messages = [f"Test message {i}" for i in range(10)]
        for message in test_messages:
            if self.publish_to_rabbitmq(message):
                self.logger.info(f"Successfully published: {message}")
            else:
                self.logger.error(f"Failed to publish: {message}")
            await asyncio.sleep(1)

        self.logger.info("Exiting debug mode.")


if __name__ == "__main__":
    DEBUG = False

    # Setup the main logger
    main_logger = setup_custom_logger("SFTPMonitor", logging.DEBUG if DEBUG else logging.INFO)

    main_logger.setLevel(logging.INFO)  # Or any other level like logging.INFO, logging.WARNING, etc.

    # Initialize utilities with the custom logger
    db_utils = AsyncDatabaseUtilities(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, logger=main_logger)
    s3_utils = S3Utilities(DO_SPACES_URL, DO_SPACES_KEY, DO_SPACES_SECRET, DO_SPACES_BUCKET, logger=main_logger)
    monitor = SFTPMonitor(
        SFTP_HOST, SFTP_PORT, SFTP_USERNAME, SFTP_PASSWORD, SFTP_DIRECTORY,
        RABBITMQ_HOST, RABBITMQ_CBOE_QUEUE, RABBITMQ_HEARTBEAT_QUEUE, WEBHOOK_URL,
        s3_utils, db_utils, main_logger
    )

    try:
        if DEBUG:
            asyncio.run(monitor.debug_mode())
        else:
            asyncio.run(monitor.monitor())
    except KeyboardInterrupt:
        main_logger.info("Program interrupted by user. Shutting down...")
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        error_traceback = traceback.format_exc()
        main_logger.critical(f"MONITOR_SFTP CRASH: {error_type} - {error_message}\n{error_traceback}")
        asyncio.run(
            monitor.send_discord_notification(f"MONITOR_SFTP CRASH: {error_type} - {error_message}\n{error_traceback}",
                                              is_error=True))
    finally:
        if monitor.connection and monitor.connection.is_open:
            monitor.connection.close()
            main_logger.info("RabbitMQ connection closed.")
