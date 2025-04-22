"""
KafkaProducerClient Module

The Kafka Producer Client module encapsulates Kafka Producer API from the `kafka-python` library.
It provides many functionalities for start, stop, flush, and send messages to Kafka topics.
"""
import json

from kafka import KafkaProducer
from kafka.errors import KafkaError


class KafkaProducerClient:
    """
    A class for managing the lifecycle of a Kafka producer, which encapsulates the connection
    and disconnection logic, allows managing and reusing the producer within an application.
    To send messages, it serializes the message value to JSON format, which gives an advantage
    to read once deserialized.
    """

    def __init__(
        self,
        bootstrap_servers
    ):
        """
        Initializes the Kafka producer client and establishes configurations for connecting
        to the Kafka broker(s).

        Args:
            bootstrap_servers (list):
                    A list of Kafka broker addresses (e.g., ['localhost:9092',
                    'kafka-broker-1:9092']) These addresses are used to establish the initial
                    connection to the Kafka cluster.
        """
        self._bootstrap_servers = bootstrap_servers
        self._producer = None

    def start(self):
        """
        This method starts the Kafka producer and connects to the Kafka broker(s).
        And configures the `KafkaProducer` instance, which handles the initial connection
        to Kafka broker(s) specified in `bootstrap_servers.`

        To serialize the message values to JSON, the producer configured with UTF-8 encoding
        standard.

        :return: It returns True if the producer started and connected successfully else
            False
        """
        if self._producer:
            print("Kafka producer already started!")
            return True

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print('Kafka producer started and connected.')
            return True
        except KafkaError as e:
            print(f'Error in starting the Kafka producer: {e}')
            self._producer = None
            return False

    def send_message(
        self,
        topic: str,
        value: dict = None
    ):
        """
        This method sends the serialized JSON value to the Kafka topic with the given topic
        name using the configured Kafka producer.

        :param topic: The topic name to send the message to.
        :param value: The serialized JSON value to send to the topic.
        :return: If the sending was successful, it returns metadata about the delivered message.
            Otherwise, it returns None if the producer is not running or an error occurred during
            sending.
        """
        if self._producer is None:
            print('Kafka producer is not running!')
            return None

        if value is None:
            print('At least one value must be specified!')
            return None

        try:
            response = self._producer.send(topic, value)
            return response
        except KafkaError as e:
            print(f'Error in sending the message: {e}')
            return None

    def flush(self):
        """
        This method flushes the producer and ensures all buffered messages are sent successfully.
        It forces the producer to send buffered messages that have not yet been transmitted to the
        Kafka brokers and blocks until all outstanding messages have been successfully sent or
        timeout is reached.

        To prevent data loss, calling the flush method is essential before stopping or closing
        the Kafka producer. As per the lifecycle, If the producer is terminated before all
        messages are flushed, there is a higher possibility that some messages may not be
        delivered to Kafka.

        Returns:
            This method does not return a value. It only performs an action to flush the produce
            rather than returning a result.

        Raises:
            If an exception occurs during the flushing process due to network issues, broker
            unavailability, or any other problem preventing the producer from sending messages
            to the brokers, an exception is raised to handle this scenario, and the caller
            should be prepared to handle this exception on the client side.
        """
        try:
            self._producer.flush()
        except Exception as e:
            raise KafkaError(f'Failed to flush producer: {e}')

    def stop(self):
        """
        This method gracefully closes the Kafka producer connection. It's essential to call this
        method once the Kafka producer processes all pending messages and resources are released.

        :return: If the Kafka producer was stopped successfully or already stopped, it returns True.
            Otherwise, if an error occurs while stopping the Kafka producer, it returns False.
        """
        if self._producer:
            try:
                self._producer.close()
                self._producer = None
                print('Kafka producer stopped and connection is closed.')
                return True
            except KafkaError as e:
                print(f'Error in stopping the Kafka producer: {e}')
                return False
        else:
            return True

    def is_producer_running(self):
        """
        This method checks if the Kafka producer is running.

        :return: If the Kafka producer has been running and connected, it returns True. Otherwise,
            it returns False.
        """
        return self._producer is not None
