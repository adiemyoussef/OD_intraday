import pika
import logging
from typing import Optional, Callable, List
from enum import Enum

class RabbitMQStatus(Enum):
    DISCONNECTED = "Disconnected"
    CONNECTED = "Connected"
    ERROR = "Error"

class RabbitMQUtilities:
    def __init__(self, host: str, port: int, user: str, password: str,
                 logger: Optional[logging.Logger] = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection = None
        self.channel = None
        self.logger = logger or logging.getLogger(__name__)
        self.status = RabbitMQStatus.DISCONNECTED
        self.last_error = None

    def get_status(self):
        return {
            "status": self.status.value,
            "last_error": str(self.last_error) if self.last_error else None
        }

    def connect(self):
        try:
            if not self.connection or self.connection.is_closed:
                credentials = pika.PlainCredentials(self.user, self.password)
                parameters = pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                self.logger.info("Successfully connected to RabbitMQ and created a channel.")
                self.status = RabbitMQStatus.CONNECTED
                self.last_error = None
        except Exception as e:
            self.logger.error(f"Error connecting to RabbitMQ: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            raise

    def ensure_connection(self):
        if self.status != RabbitMQStatus.CONNECTED or not self.connection or self.connection.is_closed:
            self.logger.info("Connection is not active. Attempting to reconnect...")
            self.connect()

    def reconnect(self):
        self.logger.info("Attempting to reconnect to RabbitMQ...")
        try:
            if self.connection:
                self.connection.close()
            if self.channel:
                self.channel.close()
            self.connect()
            self.logger.info("Successfully reconnected to RabbitMQ.")
        except Exception as e:
            self.logger.error(f"Failed to reconnect to RabbitMQ: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            raise

    def close(self):
        try:
            if self.channel:
                self.channel.close()
            if self.connection:
                self.connection.close()
            self.logger.info("RabbitMQ connection and channel closed.")
            self.status = RabbitMQStatus.DISCONNECTED
        except Exception as e:
            self.logger.error(f"Error closing RabbitMQ connection: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            raise

    def get_queue(self, queue_name: str):
        if not self.channel:
            self.connect()

        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.logger.debug(f"Queue '{queue_name}' retrieved or created successfully.")
            return queue_name
        except Exception as e:
            self.logger.error(f"Error occurred while getting queue '{queue_name}': {e}")
            raise

    def publish_message(self, queue_name: str, message: str):
        if not self.channel:
            self.connect()

        try:
            self.get_queue(queue_name)
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message.encode()
            )
            self.logger.info(f"Message published to queue '{queue_name}' successfully.")
        except Exception as e:
            self.logger.error(f"Error occurred while publishing message: {e}")
            raise

    def consume_messages(self, queue_name: str, callback: Callable[[pika.spec.Basic.Deliver, pika.spec.BasicProperties, bytes], None],
                         max_messages: Optional[int] = None):
        if not self.channel:
            self.connect()

        try:
            self.get_queue(queue_name)
            message_count = 0

            def internal_callback(ch, method, properties, body):
                nonlocal message_count
                callback(method, properties, body)
                message_count += 1
                self.logger.info(f"Message received: {body.decode()}")
                if max_messages and message_count >= max_messages:
                    ch.stop_consuming()

            self.channel.basic_consume(queue=queue_name, on_message_callback=internal_callback, auto_ack=True)
            self.channel.start_consuming()

            self.logger.info(f"Consumed {message_count} messages from queue '{queue_name}'.")
        except Exception as e:
            self.logger.error(f"Error occurred while consuming messages: {e}")
            raise

    def safe_ack(self, delivery_tag, message):
        try:
            self.ensure_connection()
            self.channel.basic_ack(delivery_tag=delivery_tag)
            self.logger.info(f"Message {message['filename']} acknowledged successfully.")
        except Exception as e:
            self.logger.error(f"Error occurred while acknowledging message {message['filename']}: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            self.reconnect()
            try:
                self.channel.basic_ack(delivery_tag=delivery_tag)
                self.logger.info("Message {message['filename']} acknowledged successfully after reconnection.")
            except Exception as inner_e:
                self.logger.error(f"Failed to acknowledge message {message['filename']} after reconnection: {inner_e}")
                raise

    def safe_nack(self, delivery_tag, requeue: bool = True):
        try:
            self.ensure_connection()
            self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
            self.logger.info(f"Message negative-acknowledged successfully. Requeue: {requeue}")
        except Exception as e:
            self.logger.error(f"Error occurred while negative-acknowledging message: {e}")
            self.status = RabbitMQStatus.ERROR
            self.last_error = e
            self.reconnect()
            try:
                self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
                self.logger.info(f"Message negative-acknowledged successfully after reconnection. Requeue: {requeue}")
            except Exception as inner_e:
                self.logger.error(f"Failed to negative-acknowledge message after reconnection: {inner_e}")
                raise

    def get_message_count(self, queue_name: str) -> int:
        try:
            queue = self.channel.queue_declare(queue=queue_name, passive=True)
            message_count = queue.method.message_count
            self.logger.info(f"Message count for queue '{queue_name}' retrieved successfully.")
            return message_count
        except Exception as e:
            self.logger.error(f"Error occurred while getting message count: {e}")
            return -1

    def fetch_all_messages_in_queue(self, queue_name: str) -> List[pika.spec.Basic.Deliver]:
        messages = []
        try:
            while True:
                method_frame, properties, body = self.channel.basic_get(queue=queue_name, auto_ack=False)
                if method_frame:
                    messages.append((method_frame, properties, body))
                else:
                    break
            self.logger.info(f"Fetched {len(messages)} messages from queue '{queue_name}'.")
        except Exception as e:
            self.logger.error(f"Error fetching messages from queue '{queue_name}': {e}")
        return messages

    def process_single_message(self, method, properties, body, process_func: Optional[Callable[[str], None]] = None):
        try:
            msg_body = body.decode('utf-8')
            self.logger.info(f"Received message: {msg_body}")

            if properties.timestamp:
                import time
                message_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(properties.timestamp))
                self.logger.info(f"Message received at: {message_time}")
            else:
                self.logger.info("Message timestamp not available")

            if process_func:
                process_func(msg_body)

            self.safe_ack(method.delivery_tag)
            self.logger.info("Message processed and acknowledged successfully.")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            self.safe_nack(method.delivery_tag)

    def process_messages(self, queue_name: str, process_func: Optional[Callable[[str], None]] = None,
                         max_messages: Optional[int] = None):
        if not self.channel:
            self.connect()

        try:
            self.get_queue(queue_name)
            message_count = 0

            def callback(ch, method, properties, body):
                nonlocal message_count
                self.process_single_message(method, properties, body, process_func)
                message_count += 1
                if max_messages and message_count >= max_messages:
                    ch.stop_consuming()

            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()

            self.logger.info(f"Processed {message_count} messages from queue '{queue_name}'.")
        except Exception as e:
            self.logger.error(f"Error processing messages from queue '{queue_name}': {e}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# Example usage
def main():
    rabbitmq_utils = RabbitMQUtilities(host="64.225.63.198", port=5672, user="optionsdepth", password="Salam123+")
    with rabbitmq_utils:
        messages = rabbitmq_utils.fetch_all_messages_in_queue("youssef_local")
        print(f"Fetched messages: {messages}")

if __name__ == "__main__":
    main()