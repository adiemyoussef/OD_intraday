import asyncio
from typing import Optional, List, Callable
import logging
import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aiormq.exceptions import ChannelInvalidStateError

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
            self.logger.info(f"Queue '{queue_name}' retrieved or created successfully.")
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
        """
        Safely acknowledge a message, handling potential channel state errors.

        :param message: Message to acknowledge

        Exceptions:
        - ChannelInvalidStateError: If the channel is in an invalid state.
        - Exception: For any other unexpected errors during acknowledgment.
        """
        try:
            await message.ack()
            self.logger.info(f"Message acknowledged successfully.")
        except ChannelInvalidStateError:
            self.logger.error("Failed to acknowledge message due to invalid channel state.")
            # Optionally, implement reconnection logic here
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
            # Optionally, implement reconnection logic here
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error occurred while negative-acknowledging message: {e}")
            raise
