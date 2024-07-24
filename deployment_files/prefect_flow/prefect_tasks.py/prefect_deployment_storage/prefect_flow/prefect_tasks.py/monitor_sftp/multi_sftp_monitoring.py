import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import aiohttp
import asyncssh
import discord
import logging
import time
import json
import pika
from datetime import datetime, timedelta
from typing import Dict, Any

from utilities.db_utils import AsyncDatabaseUtilities
from utilities.s3_utils import S3Utilities
from utilities.sftp_utils import get_eastern_time

class SFTPMonitorConfig:
    def __init__(self, config: Dict[str, Any]):
        self.sftp_host = config['SFTP_HOST']
        self.sftp_port = config['SFTP_PORT']
        self.sftp_username = config['SFTP_USERNAME']
        self.sftp_password = config['SFTP_PASSWORD']
        self.sftp_directory = config['SFTP_DIRECTORY']
        self.rabbitmq_host = config['RABBITMQ_HOST']
        self.rabbitmq_queue = config['RABBITMQ_QUEUE']
        self.heartbeat_queue = config['RABBITMQ_HEARTBEAT_QUEUE']
        self.webhook_url = config['WEBHOOK_URL']
        self.do_spaces_url = config['DO_SPACES_URL']
        self.do_spaces_key = config['DO_SPACES_KEY']
        self.do_spaces_secret = config['DO_SPACES_SECRET']
        self.do_spaces_bucket = config['DO_SPACES_BUCKET']
        self.log_file_key = config['LOG_FILE_KEY']
        self.base_sleep_time = config['SFTP_BASE_SLEEP_TIME']
        self.reduced_sleep_time = config['SFTP_REDUCED_SLEEP_TIME']
        self.expectation_window = config['SFTP_EXPECTATION_WINDOW']
        self.max_retry_attempts = config['SFTP_MAX_RETRY_ATTEMPTS']
        self.queue_size_alert_threshold = config['RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD']
        self.heartbeat_interval = config['RABBITMQ_HEARTBEAT_INTERVAL']
        self.clear_heartbeat_interval = config['RABBITMQ_CLEAR_HEARTBEAT_INTERVAL']

class SFTPMonitor:
    def __init__(self, config: SFTPMonitorConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.s3_utils = S3Utilities(
            config.do_spaces_url, config.do_spaces_key,
            config.do_spaces_secret, config.do_spaces_bucket
        )

    def get_rabbitmq_connection(self, max_retries=5, retry_delay=5):
        for attempt in range(max_retries):
            try:
                connection = pika.BlockingConnection(pika.ConnectionParameters(self.config.rabbitmq_host))
                channel = connection.channel()
                return connection, channel
            except pika.exceptions.AMQPConnectionError as e:
                self.logger.warning(f"Failed to connect to RabbitMQ (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise Exception("Failed to connect to RabbitMQ after multiple attempts")

    def publish_to_rabbitmq(self, channel, message: str):
        try:
            channel.queue_declare(queue=self.config.rabbitmq_queue, durable=True)
            channel.basic_publish(
                exchange='',
                routing_key=self.config.rabbitmq_queue,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='text/plain',
                    content_encoding='utf-8'
                ),
            )
            self.logger.info(f"Published message to queue '{self.config.rabbitmq_queue}': {message[:100]}...")
            return True
        except Exception as e:
            self.logger.error(f"Failed to publish message to queue '{self.config.rabbitmq_queue}': {str(e)}")
            return False

    async def adaptive_sleep(self):
        now = datetime.now()
        minutes, seconds = now.minute, now.second
        seconds_to_next_10min = 600 - ((minutes % 10) * 60 + seconds)

        if 0 <= seconds_to_next_10min <= self.config.expectation_window:
            return self.config.reduced_sleep_time
        else:
            sleep_time = seconds_to_next_10min - self.config.expectation_window
            if sleep_time <= 0:
                sleep_time += 600
            return min(sleep_time, self.config.base_sleep_time)

    async def monitor_directory(self):
        seen_files = set(self.s3_utils.load_json(self.config.log_file_key))
        self.logger.info(f"Loaded {len(seen_files)} log entries for directory {self.config.sftp_directory}")

        connection, channel = self.get_rabbitmq_connection()
        last_successful_check = datetime.now()

        try:
            while True:
                try:
                    async with asyncssh.connect(
                        self.config.sftp_host, port=self.config.sftp_port,
                        username=self.config.sftp_username, password=self.config.sftp_password,
                        known_hosts=None
                    ) as conn:
                        async with conn.start_sftp_client() as sftp:
                            files = await sftp.listdir(self.config.sftp_directory)
                            new_files = [f for f in files if f not in seen_files and f not in {'.', '..'}]

                            for filename in new_files:
                                file_path = f"{self.config.sftp_directory}/{filename}"
                                file_attr = await sftp.stat(file_path)

                                self.logger.info(f'{filename} not in seen files')
                                file_info = {
                                    "filename": filename,
                                    "path": file_path,
                                    "timestamp": datetime.fromtimestamp(file_attr.mtime).isoformat()
                                }
                                file_info_str = json.dumps(file_info)

                                if self.publish_to_rabbitmq(channel, file_info_str):
                                    seen_files.add(filename)
                                else:
                                    self.logger.warning(f"Failed to publish message for file: {filename}. Retrying...")
                                    connection, channel = self.get_rabbitmq_connection()
                                    if self.publish_to_rabbitmq(channel, file_info_str):
                                        seen_files.add(filename)
                                    else:
                                        self.logger.error(f"Failed to publish message for file: {filename} after retry.")

                            if new_files:
                                self.s3_utils.save_json(self.config.log_file_key, list(seen_files))

                            last_successful_check = datetime.now()

                    await asyncio.sleep(await self.adaptive_sleep())

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger.error(f"Error monitoring directory {self.config.sftp_directory}: {e}")
                    if datetime.now() - last_successful_check > timedelta(minutes=5):
                        error_message = f"SFTP monitoring failed continuously for 5 minutes. Last error: {str(e)}"
                        self.logger.critical(error_message)
                        await self.send_discord_notification(error_message, is_error=True)
                    await asyncio.sleep(self.config.base_sleep_time)

        finally:
            if connection and connection.is_open:
                connection.close()

    async def send_discord_notification(self, message, is_error=False):
        async with aiohttp.ClientSession() as session:
            webhook = discord.Webhook.from_url(self.config.webhook_url, session=session)
            embed = discord.Embed(
                title="🚨 MONITOR_SFTP ALERT 🚨" if is_error else "ℹ️ MONITOR_SFTP INFO ℹ️",
                description=message,
                color=discord.Color.red() if is_error else discord.Color.blue()
            )
            await webhook.send(embed=embed)

    def run(self):
        self.logger.info(f"Starting SFTP monitoring for {self.config.sftp_directory}...")
        asyncio.run(self.monitor_directory())

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

if __name__ == "__main__":
    # Configuration for the first SFTP monitor
    config1 = SFTPMonitorConfig({
        'SFTP_HOST': "sftp.datashop.livevol.com",
        'SFTP_PORT': 22,
        'SFTP_USERNAME': "contact_optionsdepth_com",
        'SFTP_PASSWORD': "Salam123+-",
        'SFTP_DIRECTORY': "/subscriptions/order_000059435/item_000068201",
        'RABBITMQ_HOST': "localhost",
        'RABBITMQ_QUEUE': "youssef_local",
        'RABBITMQ_HEARTBEAT_QUEUE': 'heartbeats',
        'WEBHOOK_URL': 'https://discord.com/api/webhooks/1251013946111164436/VN55yOK-ntil-PnZn1gzWHzKwzDklwIh6fVspA_I8MCCaUnG-hsRsrP1t_WsreGHIity',
        'DO_SPACES_URL': 'https://nyc3.digitaloceanspaces.com',
        'DO_SPACES_KEY': 'DO00EMRQHFB38ZQZRGA7',
        'DO_SPACES_SECRET': 'gHFYFlzQ1T73gAY7BKRNtgx5hlEMMAB2YZIv+YyQUps',
        'DO_SPACES_BUCKET': 'intraday',
        'LOG_FILE_KEY': 'seen_files_log_1.json',
        'SFTP_BASE_SLEEP_TIME': 10,
        'SFTP_REDUCED_SLEEP_TIME': 5,
        'SFTP_EXPECTATION_WINDOW': 120,
        'SFTP_MAX_RETRY_ATTEMPTS': 3,
        'RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD': 1000,
        'RABBITMQ_HEARTBEAT_INTERVAL': 60,
        'RABBITMQ_CLEAR_HEARTBEAT_INTERVAL': 3600
    })

    # Configuration for the second SFTP monitor (with different settings)
    config2 = SFTPMonitorConfig({
        'SFTP_HOST': "another.sftp.host.com",
        'SFTP_PORT': 22,
        'SFTP_USERNAME': "another_username",
        'SFTP_PASSWORD': "another_password",
        'SFTP_DIRECTORY': "/another/directory",
        'RABBITMQ_HOST': "localhost",
        'RABBITMQ_QUEUE': "another_queue",
        'RABBITMQ_HEARTBEAT_QUEUE': 'heartbeats_2',
        'WEBHOOK_URL': 'https://discord.com/api/webhooks/another_webhook_url',
        'DO_SPACES_URL': 'https://nyc3.digitaloceanspaces.com',
        'DO_SPACES_KEY': 'DO00EMRQHFB38ZQZRGA7',
        'DO_SPACES_SECRET': 'gHFYFlzQ1T73gAY7BKRNtgx5hlEMMAB2YZIv+YyQUps',
        'DO_SPACES_BUCKET': 'intraday',
        'LOG_FILE_KEY': 'seen_files_log_2.json',
        'SFTP_BASE_SLEEP_TIME': 20,  # Different sleep time
        'SFTP_REDUCED_SLEEP_TIME': 10,
        'SFTP_EXPECTATION_WINDOW': 180,
        'SFTP_MAX_RETRY_ATTEMPTS': 5,
        'RABBITMQ_QUEUE_SIZE_ALERT_THRESHOLD': 2000,
        'RABBITMQ_HEARTBEAT_INTERVAL': 120,
        'RABBITMQ_CLEAR_HEARTBEAT_INTERVAL': 7200
    })

    # Create and run monitors
    monitor1 = SFTPMonitor(config1, setup_logger("SFTPMonitor1"))
    monitor2 = SFTPMonitor(config2, setup_logger("SFTPMonitor2"))

    # Run monitors in separate processes
    p1 = multiprocessing.Process(target=monitor1.run)
    p2 = multiprocessing.Process(target=monitor2.run)

    p1.start()
    p2.start()

    p1.join()
    p2.join()