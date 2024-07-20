import asyncio
import logging
import time
from typing import Optional, List

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aiormq import ChannelInvalidStateError

from config.config import *

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


QUEUE_NAME = 'youssef_local'
EXPECTED_FILE_NAME = "Cboe_OpenClose_2024-07-18_00_00_1.csv.zip"
MAX_EMPTY_TRIES = 5
RETRY_DELAY = 5  # in seconds
POST_ACK_DELAY = 5  # in seconds, to allow server to update message count

class AsyncRabbitMQUtilities:
    """
    A utility class for asynchronous RabbitMQ operations.
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 logger: Optional[logging.Logger] = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.logger = logger or logging.getLogger(__name__)

    async def connect(self):
        if not self.connection or self.connection.is_closed:
            try:
                self.connection = await aio_pika.connect_robust(
                    f"amqp://{self.user}:{self.password}@{self.host}:{self.port}/"
                )
                self.channel = await self.connection.channel()
                self.logger.info("Successfully connected to RabbitMQ and created a channel.")
            except aio_pika.AMQPException as e:
                self.logger.error(f"AMQP error occurred while connecting to RabbitMQ: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error occurred while connecting to RabbitMQ: {e}")
                raise

    async def close(self):
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()
        self.logger.info("RabbitMQ connection and channel closed.")

    async def get_queue(self, queue_name: str):
        if not self.channel:
            await self.connect()

        try:
            queue = await self.channel.declare_queue(queue_name, durable=True)
            self.logger.info(f"Queue '{queue_name}' retrieved or created successfully.")
            return queue
        except aio_pika.AMQPException as e:
            self.logger.error(f"AMQP error occurred while getting queue '{queue_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while getting queue '{queue_name}': {e}")
            raise

    async def safe_ack(self, message: AbstractIncomingMessage):
        try:
            await message.ack()
            self.logger.info("Message acknowledged successfully.")
        except ChannelInvalidStateError:
            self.logger.error("Failed to acknowledge message due to invalid channel state.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while acknowledging message: {e}")
            raise

    async def safe_nack(self, message: AbstractIncomingMessage, requeue: bool = True):
        try:
            await message.nack(requeue=requeue)
            self.logger.info(f"Message negative-acknowledged successfully. Requeue: {requeue}")
        except ChannelInvalidStateError:
            self.logger.error("Failed to negative-acknowledge message due to invalid channel state.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while negative-acknowledging message: {e}")
            raise

    async def get_message_count(self, queue_name: str) -> int:
        try:
            queue = await self.channel.declare_queue(queue_name, passive=True)
            return queue.declaration_result.message_count
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while getting message count: {e}")
            return -1

async def process_message(rabbit_utils: AsyncRabbitMQUtilities, message: AbstractIncomingMessage, queue_name: str, local_counter: List[int]):
    try:
        # Permet de decoder le message dans le AbstractIncomingMessage
        msg_body = message.body.decode('utf-8')
        logger.info(f"Received message: {msg_body}")

        if message.timestamp:
            message_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(message.timestamp))
            logger.info(f"Message received at: {message_time}")
        else:
            logger.info("Message timestamp not available")

        # Simulate processing time
        await asyncio.sleep(5)

        # Check if the expected file is in the message
        if EXPECTED_FILE_NAME in msg_body:
            logger.info(f"Expected file {EXPECTED_FILE_NAME} found in the message")
        else:
            logger.info(f"Expected file {EXPECTED_FILE_NAME} not found, processing most recent message")

        # Acknowledge the message
        await rabbit_utils.safe_ack(message)

        # Update the local counter and log the remaining message count
        local_counter[0] -= 1
        logger.info(f"Messages remaining in the queue: {local_counter[0]}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Requeue the message in case of an error
        await rabbit_utils.safe_nack(message, requeue=True)

async def process_all_messages(rabbit_utils: AsyncRabbitMQUtilities, messages: List[AbstractIncomingMessage], queue_name: str, local_counter: List[int]):
    tasks = []
    found_expected = False

    # Reverse the order of messages to process the newest first
    for message in reversed(messages):
        msg_body = message.body.decode('utf-8')
        if EXPECTED_FILE_NAME in msg_body:
            logger.info(f"Expected file {EXPECTED_FILE_NAME} found in the message: {msg_body}")
            found_expected = True
            tasks.append(process_message(rabbit_utils, message, queue_name, local_counter))
            break

    if not found_expected:
        logger.info(f"Expected file {EXPECTED_FILE_NAME} not found in any messages.")
        # Process messages one by one in a sequential manner from newest to oldest
        for message in reversed(messages):
            await process_message(rabbit_utils, message, queue_name, local_counter)

async def main():
    empty_tries = 0

    rabbit_utils = AsyncRabbitMQUtilities(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        user=RABBITMQ_USER,
        password=RABBITMQ_PASS,
        logger=logger
    )

    await rabbit_utils.connect()

    while empty_tries < MAX_EMPTY_TRIES:
        try:
            # queue est un Objet de type RobustQueue
            queue = await rabbit_utils.get_queue(QUEUE_NAME)
            # liste de AbstractIncomingMessage
            messages: List[AbstractIncomingMessage] = []

            # On accede aux informations du queue
            # message --> C'est un Objet IncomingMessage qui donne des informations sur le queue
            # message = await queue.get(fail=False)  # Set fail=False to manually acknowledge

            # Ramasse tous les messages dans le queue: Peut etre amelioré (asynchrone non bloquant) mais simple pour le moment
            while True:
                message = await queue.get(fail=False)  # Set fail=False to manually acknowledge

                if message is None:
                    # Ca peut etre normal: connection peut ne pas etre pret ou wtvr....
                    logger.info("No Message in the queue")
                    break
                messages.append(message)

            # Update local counter to track remaining messages
            local_counter = [len(messages)]

            # 7sec. pour 252 messages: A ce point, ca ne vide pas le queue dans RabbitMQ
            # breakpoint()

            if not messages:
                empty_tries += 1
                logger.info(f"Queue is empty. Attempt {empty_tries}/{MAX_EMPTY_TRIES}. Retrying in {RETRY_DELAY} seconds...")
                await asyncio.sleep(RETRY_DELAY)

            else:
                empty_tries = 0  # Reset the empty tries counter on a successful fetch
                await process_all_messages(rabbit_utils, messages, QUEUE_NAME, local_counter)

        except Exception as e:
            logger.error(f"Error occurred: {e}")
            break

    logger.info("Maximum empty tries reached. Exiting...")
    await rabbit_utils.close()

if __name__ == "__main__":
    asyncio.run(main())
