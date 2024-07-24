import time
import aio_pika
from aiormq import ChannelInvalidStateError
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
import logging
from typing import Optional, Callable, List
from enum import Enum
class RabbitMQStatus(Enum):
    DISCONNECTED = "Disconnected"
    CONNECTED = "Connected"
    ERROR = "Error"
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
        self.status = RabbitMQStatus.DISCONNECTED
        self.last_error = None

    def get_status(self):
        return {
            "status": self.status.value,
            "last_error": str(self.last_error) if self.last_error else None
        }
    async def connect(self):
        try:
            if not self.connection or self.connection.is_closed:
                self.connection = await aio_pika.connect_robust(
                    f"amqp://{self.user}:{self.password}@{self.host}:{self.port}/"
                )
                self.channel = await self.connection.channel()
                self.logger.info("Successfully connected to RabbitMQ and created a channel.")
                self.status = RabbitMQStatus.CONNECTED
                self.last_error = None
        except Exception as e:
            self.logger.error(f"Error connecting to RabbitMQ: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            raise

    async def ensure_connection(self):
        if self.status != RabbitMQStatus.CONNECTED or not self.connection or self.connection.is_closed:
            self.logger.info("Connection is not active. Attempting to reconnect...")
            await self.connect()
    async def reconnect(self):
        self.logger.info("Attempting to reconnect to RabbitMQ...")
        try:
            if self.connection:
                await self.connection.close()
            if self.channel:
                await self.channel.close()
            await self.connect()
            self.logger.info("Successfully reconnected to RabbitMQ.")
        except Exception as e:
            self.logger.error(f"Failed to reconnect to RabbitMQ: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            raise

    async def close(self):
        try:
            if self.channel:
                await self.channel.close()
            if self.connection:
                await self.connection.close()
            self.logger.info("RabbitMQ connection and channel closed.")
            self.status = RabbitMQStatus.DISCONNECTED
        except Exception as e:
            self.logger.error(f"Error closing RabbitMQ connection: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            raise

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
            self.logger.info(f"Message published to queue '{queue_name}' successfully.")
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

            self.logger.info(f"Consumed {message_count} messages from queue '{queue_name}'.")
        except aio_pika.AMQPException as e:
            self.logger.error(f"AMQP error occurred while consuming messages: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while consuming messages: {e}")
            raise

    async def safe_ack(self, message: AbstractIncomingMessage):
        try:
            self.logger.info(f"{self.get_status()} after first ack trial")
            await self.ensure_connection()
            self.logger.info(f"{self.get_status()}")
            await message.ack()
            self.logger.info("Message acknowledged successfully.")
        except ChannelInvalidStateError as e:
            self.logger.error("Failed to acknowledge message due to invalid channel state.")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            # Instead of raising, we'll attempt to reconnect
            await self.reconnect()
            # After reconnection, try to ack again
            try:
                await message.ack()
                self.logger.info("Message acknowledged successfully after reconnection.")
            except Exception as inner_e:
                self.logger.error(f"Failed to acknowledge message after reconnection: {inner_e}")
                raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while acknowledging message: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            raise

    async def safe_nack(self, message: AbstractIncomingMessage, requeue: bool = True):
        try:
            await self.ensure_connection()
            await message.nack(requeue=requeue)
            self.logger.info(f"Message negative-acknowledged successfully. Requeue: {requeue}")
        except ChannelInvalidStateError as e:
            self.logger.error("Failed to negative-acknowledge message due to invalid channel state.")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            # Instead of raising, we'll attempt to reconnect
            await self.reconnect()
            # After reconnection, try to nack again
            try:
                await message.nack(requeue=requeue)
                self.logger.info(f"Message negative-acknowledged successfully after reconnection. Requeue: {requeue}")
            except Exception as inner_e:
                self.logger.error(f"Failed to negative-acknowledge message after reconnection: {inner_e}")
                raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while negative-acknowledging message: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
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
            self.logger.info(f"Message count for queue '{queue_name}' retrieved successfully.")
            return queue.declaration_result.message_count
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while getting message count: {e}")
            return -1

    async def fetch_all_messages_in_queue(self, queue_name: str) -> List[AbstractIncomingMessage]:
        """
        Fetch all messages from a specified RabbitMQ queue without removing them.

        :param queue_name: The name of the queue to fetch messages from
        :return: A list of all fetched messages
        """
        messages = []
        try:
            queue = await self.get_queue(queue_name)
            while True:
                message = await queue.get(fail=False)
                if message is None:
                    break
                messages.append(message)
            self.logger.info(f"Fetched {len(messages)} messages from queue '{queue_name}'.")
        except Exception as e:
            self.logger.error(f"Error fetching messages from queue '{queue_name}': {e}")
        return messages

    async def process_single_message(
        self,
        message: AbstractIncomingMessage,
        process_func: Optional[Callable[[str], None]] = None,
        processing_time: float = 5.0
    ):
        """
        Process a single RabbitMQ message.

        This method decodes the message, optionally applies a custom processing function,
        simulates processing time, and handles message acknowledgment.

        :param message: The RabbitMQ message to process
        :param process_func: Optional function to process the message content
        :param processing_time: Time in seconds to simulate processing (default: 5.0)
        """
        try:
            msg_body = message.body.decode('utf-8')
            self.logger.info(f"Received message: {msg_body}")

            if message.timestamp:
                message_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(message.timestamp))
                self.logger.info(f"Message received at: {message_time}")
            else:
                self.logger.info("Message timestamp not available")

            if process_func:
                process_func(msg_body)

            await asyncio.sleep(processing_time)  # Simulate processing time

            await self.safe_ack(message)
            self.logger.info("Message processed and acknowledged successfully.")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            await self.safe_nack(message)

    async def process_messages(
        self,
        queue_name: str,
        process_func: Optional[Callable[[str], None]] = None,
        max_messages: Optional[int] = None,
        processing_time: float = 5.0
    ):
        """
        Process messages from a specified queue.

        :param queue_name: Name of the queue to consume from
        :param process_func: Optional function to process each message
        :param max_messages: Maximum number of messages to process (None for unlimited)
        :param processing_time: Time in seconds to simulate processing for each message
        """
        if not self.channel:
            await self.connect()

        try:
            queue = await self.get_queue(queue_name)
            message_count = 0

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await self.process_single_message(message, process_func, processing_time)
                    message_count += 1
                    if max_messages and message_count >= max_messages:
                        break

            self.logger.info(f"Processed {message_count} messages from queue '{queue_name}'.")
        except Exception as e:
            self.logger.error(f"Error processing messages from queue '{queue_name}': {e}")

    async def heartbeat(self):
        while True:
            try:
                if not self.connection or self.connection.is_closed:
                    await self.reconnect()
                await asyncio.sleep(2)
            except Exception as e:
                self.logger.error(f"Heartbeat failed: {e}")
                await asyncio.sleep(5)
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

async def main():
    rabbitmq_utils = AsyncRabbitMQUtilities(host="64.225.63.198", port=5672, user="optionsdepth", password="Salam123+")
    await rabbitmq_utils.connect()
    messages = await rabbitmq_utils.fetch_messages("youssef_local", max_messages=10)
    print(f"Fetched messages: {messages}")
    await rabbitmq_utils.close()

# Run the main function in an asyncio event loop
import asyncio

if __name__ == "__main__":
    asyncio.run(main())
