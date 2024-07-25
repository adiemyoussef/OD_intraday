import asyncio
import logging
import time
from typing import Optional, List

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aiormq import ChannelInvalidStateError

from config.config import *
from utilities.rabbitmq_utils import AsyncRabbitMQUtilities

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

QUEUE_NAME = 'youssef_local'
EXPECTED_FILE_NAME = "Cboe_OpenClose_2024-07-19_00_00_1.csv.zip"
MAX_EMPTY_TRIES = 5
RETRY_DELAY = 5  # in seconds
POST_ACK_DELAY = 5  # in seconds, to allow server to update message count

async def process_message(rabbit_utils: AsyncRabbitMQUtilities, message: AbstractIncomingMessage, local_counter: List[int]):
    try:
        # Decode the message body
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

async def process_all_messages(rabbit_utils: AsyncRabbitMQUtilities, messages: List[AbstractIncomingMessage], local_counter: List[int]):
    tasks = []
    found_expected = False

    # Process messages from newest to oldest
    for message in reversed(messages):
        msg_body = message.body.decode('utf-8')
        if EXPECTED_FILE_NAME in msg_body:
            logger.info(f"Expected file {EXPECTED_FILE_NAME} found in the message: {msg_body}")
            found_expected = True
            await process_message(rabbit_utils, message, local_counter)
            break

    if found_expected:
        # Remove the processed message from the list
        messages = [msg for msg in messages if msg.body.decode('utf-8') != msg_body]

    if not found_expected:
        logger.info(f"Expected file {EXPECTED_FILE_NAME} not found in any messages.")

    # Process remaining messages one by one from newest to oldest
    for message in reversed(messages):
        await process_message(rabbit_utils, message, local_counter)

async def fetch_all_messages(rabbit_utils: AsyncRabbitMQUtilities, queue_name: str) -> List[AbstractIncomingMessage]:
    messages = []
    try:
        queue = await rabbit_utils.get_queue(queue_name)
        while True:
            message = await queue.get(fail=False)
            if message is None:
                break
            messages.append(message)
    except Exception as e:
        logger.error(f"Error fetching messages: {e}")
    return messages


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
            # Fetch all messages from the queue
            messages = await fetch_all_messages(rabbit_utils, QUEUE_NAME)

            # Update local counter to track remaining messages
            local_counter = [len(messages)]

            if not messages:
                empty_tries += 1
                logger.info(f"Queue is empty. Attempt {empty_tries}/{MAX_EMPTY_TRIES}. Retrying in {RETRY_DELAY} seconds...")
                await asyncio.sleep(RETRY_DELAY)
            else:
                empty_tries = 0  # Reset the empty tries counter on a successful fetch
                await process_all_messages(rabbit_utils, messages, local_counter)

        except Exception as e:
            logger.error(f"Error occurred: {e}")
            break

    logger.info("Maximum empty tries reached. Exiting...")
    await rabbit_utils.close()

if __name__ == "__main__":
    asyncio.run(main())
