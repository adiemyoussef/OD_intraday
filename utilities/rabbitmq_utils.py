import aio_pika
from aiormq import ChannelInvalidStateError
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
import logging
from typing import Optional, Callable, List
import asyncio
import time
class AsyncRabbitMQUtilities:
    """
    A utility class for asynchronous RabbitMQ operations.

    This class provides methods for connecting to RabbitMQ, publishing messages,
    consuming messages, and managing connections and channels.
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the AsyncRabbitMQUtilities instance.

        :param host: RabbitMQ host address
        :param port: RabbitMQ port number
        :param user: RabbitMQ username
        :param password: RabbitMQ password
        :param logger: Logger instance to use for logging. If None, a new logger will be created.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.logger = logger or logging.getLogger(__name__)

    async def connect(self):
        """
        Establish a connection to RabbitMQ and create a channel.

        This method should be called before any other RabbitMQ operations.

        Exceptions:
        - aio_pika.AMQPException: If there's an error connecting to RabbitMQ.
        - Exception: For any other unexpected errors during connection.
        """
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
        """
        Close the RabbitMQ connection and channel.

        This method should be called when RabbitMQ operations are no longer needed.
        """
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()
        self.logger.info("RabbitMQ connection and channel closed.")

    async def get_queue(self, queue_name: str) -> AbstractQueue:
        """
        Get a queue by its name. If the queue doesn't exist, it will be created.

        :param queue_name: Name of the queue
        :return: AbstractQueue object

        Exceptions:
        - aio_pika.AMQPException: If there's an error getting or creating the queue.
        - Exception: For any other unexpected errors.
        """
        if not self.channel:
            await self.connect()

        try:
            queue = await self.channel.declare_queue(queue_name, durable=True)
            self.logger.debug(f"Queue '{queue_name}' retrieved or created successfully.")
            return queue
        except aio_pika.AMQPException as e:
            self.logger.error(f"AMQP error occurred while getting queue '{queue_name}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while getting queue '{queue_name}': {e}")
            raise

    async def publish_message(self, queue_name: str, message: str):
        """
        Publish a message to a specified queue.

        :param queue_name: Name of the queue to publish to
        :param message: Message to publish

        Exceptions:
        - aio_pika.AMQPException: If there's an error publishing the message.
        - Exception: For any other unexpected errors during publishing.
        """
        if not self.channel:
            await self.connect()

        try:
            queue = await self.get_queue(queue_name)
            await self.channel.default_exchange.publish(
                aio_pika.Message(body=message.encode()),
                routing_key=queue.name
            )
            self. logger.debug(f"Message published to queue '{queue_name}' successfully.")
        except aio_pika.AMQPException as e:
            self.logger.error(f"AMQP error occurred while publishing message: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while publishing message: {e}")
            raise

    async def consume_messages(self, queue_name: str, callback: Callable[[AbstractIncomingMessage], None],
                               max_messages: Optional[int] = None):
        """
        Consume messages from a specified queue.

        :param queue_name: Name of the queue to consume from
        :param callback: Async function to process each message
        :param max_messages: Maximum number of messages to consume (None for unlimited)

        Exceptions:
        - aio_pika.AMQPException: If there's an error during message consumption.
        - Exception: For any other unexpected errors during consumption.
        """
        if not self.channel:
            await self.connect()

        try:
            queue = await self.get_queue(queue_name)
            message_count = 0

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        await callback(message)
                        message_count += 1
                        self.logger.info(f"Message received: {message.body.decode()}")
                        if max_messages and message_count >= max_messages:
                            break

            self. logger.debug(f"Consumed {message_count} messages from queue '{queue_name}'.")
        except aio_pika.AMQPException as e:
            self.logger.error(f"AMQP error occurred while consuming messages: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while consuming messages: {e}")
            raise

    async def fetch_messages(self, queue_name: str, max_messages: int = 10) -> list:
        """
        Fetch a number of messages from a specified queue.

        :param queue_name: Name of the queue to fetch messages from
        :param max_messages: Maximum number of messages to fetch
        :return: List of messages

        Exceptions:
        - aio_pika.AMQPException: If there's an error during message fetching.
        - Exception: For any other unexpected errors during fetching.
        """
        if not self.channel:
            await self.connect()

        messages = []
        try:
            queue = await self.get_queue(queue_name)
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        messages.append(message.body.decode())
                        self.logger.info(f"Fetched message: {message.body.decode()}")
                        if len(messages) >= max_messages:
                            break
            self. logger.debug(f"Fetched {len(messages)} messages from queue '{queue_name}'.")
            return messages
        except aio_pika.AMQPException as e:
            self.logger.error(f"AMQP error occurred while fetching messages: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while fetching messages: {e}")
            raise
        return messages

    async def safe_ack(self, message: AbstractIncomingMessage):
        """
        Safely acknowledge a message, handling potential channel state errors.

        :param message: Message to acknowledge

        Exceptions:
        - ChannelInvalidStateError: If the channel is in an invalid state.
        - Exception: For any other unexpected errors during acknowledgment.
        """
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
        """
        Safely negative-acknowledge a message, handling potential channel state errors.

        :param message: Message to negative-acknowledge
        :param requeue: Whether to requeue the message

        Exceptions:
        - ChannelInvalidStateError: If the channel is in an invalid state.
        - Exception: For any other unexpected errors during negative acknowledgment.
        """
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
        """
        Get the message count of a specified queue.

        :param queue_name: Name of the queue
        :return: Number of messages in the queue

        Exceptions:
        - Exception: For any other unexpected errors while getting the message count.
        """
        try:
            queue = await self.channel.declare_queue(queue_name, passive=True)
            self. logger.debug(f"Message count for queue '{queue_name}' retrieved successfully.")
            return queue.declaration_result.message_count
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while getting message count: {e}")
            return -1


async def process_single_message(
    rabbit_utils: AsyncRabbitMQUtilities,
    message: AbstractIncomingMessage,
    expected_message: str,
    message_count: List[int],
    logger: Optional[logging.Logger] = None
):
    """
    Process a single RabbitMQ message.

    This function decodes the message, checks for the expected content,
    simulates processing time, and handles message acknowledgment.

    :param rabbit_utils: An instance of AsyncRabbitMQUtilities for RabbitMQ operations
    :param message: The RabbitMQ message to process
    :param expected_message: The expected content to look for in the message
    :param message_count: A list containing the count of remaining messages
    :param logger: Optional logger for logging operations
    """
    try:
        msg_body = message.body.decode('utf-8')
        logger.info(f"Received message: {msg_body}")

        if message.timestamp:
            message_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(message.timestamp))
            logger.info(f"Message received at: {message_time}")
        else:
            logger.info("Message timestamp not available")

        await asyncio.sleep(5)  # Simulate processing time

        if expected_message in msg_body:
            logger.info(f"Expected file {expected_message} found in the message")
        else:
            logger.info(f"Expected file {expected_message} not found, processing most recent message")

        await rabbit_utils.safe_ack(message)

        message_count[0] -= 1
        logger.info(f"Messages remaining in the queue: {message_count[0]}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await rabbit_utils.safe_nack(message, requeue=True)

async def process_message_batch(
    rabbit_utils: AsyncRabbitMQUtilities,
    messages: List[AbstractIncomingMessage],
    expected_message: str,
    message_count: List[int],
    logger: Optional[logging.Logger] = None
):
    """
    Process a batch of RabbitMQ messages, prioritizing the expected message.

    This function processes messages from newest to oldest, looking for the expected message.
    If found, it processes that message first, then processes the remaining messages.

    :param rabbit_utils: An instance of AsyncRabbitMQUtilities for RabbitMQ operations
    :param messages: A list of RabbitMQ messages to process
    :param expected_message: The expected content to look for in the messages
    :param message_count: A list containing the count of remaining messages
    :param logger: Optional logger for logging operations
    """
    found_expected = False

    for message in reversed(messages):
        msg_body = message.body.decode('utf-8')
        if expected_message in msg_body:
            logger.info(f"Expected file {expected_message} found in the message: {msg_body}")
            found_expected = True
            await process_single_message(rabbit_utils, message, expected_message, message_count, logger)
            messages.remove(message)
            break

    if not found_expected:
        logger.info(f"Expected file {expected_message} not found in any messages.")

    for message in reversed(messages):
        await process_single_message(rabbit_utils, message, expected_message, message_count, logger)

async def fetch_all_queue_messages(
    rabbit_utils: AsyncRabbitMQUtilities,
    queue_name: str,
    logger: Optional[logging.Logger] = None
) -> List[AbstractIncomingMessage]:
    """
    Fetch all messages from a specified RabbitMQ queue.

    This function attempts to retrieve all available messages from the given queue
    without removing them from the queue.

    :param rabbit_utils: An instance of AsyncRabbitMQUtilities for RabbitMQ operations
    :param queue_name: The name of the queue to fetch messages from
    :param logger: Optional logger for logging operations
    :return: A list of all fetched messages
    """
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



#---------- EXAMPLE -----------#
async def main():
    rabbitmq_utils = AsyncRabbitMQUtilities(host="64.225.63.198", port=5672, user="optionsdepth", password="Salam123+")
    await rabbitmq_utils.connect()
    messages = await rabbitmq_utils.fetch_messages("youssef_local", max_messages=10)
    print(f"Fetched messages: {messages}")
    await rabbitmq_utils.close()


if __name__ == "__main__":
    asyncio.run(main())
