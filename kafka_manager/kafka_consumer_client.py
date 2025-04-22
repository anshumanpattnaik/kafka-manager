"""
KafkaConsumerClient Module

The Kafka Consumer Client module encapsulates Kafka Consumer API from the `kafka-python` library.
It provides many functionalities to create, start, stop, and consume messages from Kafka topics.
"""

import json
import time

from kafka.consumer import KafkaConsumer
from kafka.errors import KafkaError


class KafkaConsumerClient:
    """
    A class for managing the lifecycle of a Kafka consumer, which encapsulates the connection
    and disconnection logic, allows managing and consuming messages from a Kafka consumer
    within an application. It implements a deserialization technique to decode the message
    value from JSON to read the content and handles errors in each process.
    """

    def __init__(
        self,
        bootstrap_servers,
        topics,
        group_id=None,
        auto_offset_reset='latest',
        **kwargs
    ):
        """
        Initializes the Kafka consumer client, establishes configurations for connecting to the
        Kafka broker(s), and subscribes to the topic(s).

        :param bootstrap_servers:
                A list of Kafka broker addresses (e.g., ['localhost:9092', 'kafka-broker-1:9092'])
                These addresses are used to establish the initial connection to the Kafka cluster.
        :param topics: A list of Kafka topics to subscribe to (e.g., ['test_topic1', 'test_topic2])
        :param group_id:
                A group ID is used to identify the consumer group (optional). Messages with the same
                group ID of Consumers will work together from the topics.
        :param auto_offset_reset:
                An optional parameter to sort the message ordering, which is set by default
                to 'latest' when the initial offset in Kafka does not exist.
                - 'earliest': Automatically sorts messages earlier than the initial offset.
                - 'latest': Automatically sorts messages later than the initial offset.
        :param kwargs:
                Additional arguments are passed directly to the Kafka Consumer constructor,
                which allows further customization of the consumer (e.g., Security Settings, etc.).
        """
        self._bootstrap_servers = bootstrap_servers
        self._topics = topics
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._consumer_kwargs = kwargs
        self._consumer = None
        self._running = False

    @property
    def consumer(self):
        """
        Returns the Kafka Consumer object.
        """
        return self._consumer

    def start(self):
        """
        This method starts the Kafka consumer and connects to the Kafka broker(s) and
        subscribes to the specified topic(s). And configures the `KafkaConsumer` instance,
        which handles consuming messages from a Kafka topic.

        The consumer is configured to deserialize the message from (UTF-8) encoded JSON.

        :return: It returns True if the consumer starts and connects successfully or returns False.
        """
        if self._running:
            print('Kafka consumer is already running!')
            return True

        try:
            self._consumer = KafkaConsumer(
                *self._topics,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                auto_offset_reset=self._auto_offset_reset,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
                **self._consumer_kwargs
            )
            self._running = True
            print(f'Kafka consumer is started and subscribed to topics: {self._topics} and '
                  f'group_id: {self._group_id}')
            return True
        except KafkaError as e:
            print(f'Error starting Kafka consumer: {e}')
            self._consumer = None
            return False

    def consume(
        self,
        message_handler
    ):
        """
        This method continuously enters a loop to poll for new messages from the Kafka topic
        and calls each received message the `message_handler` function, passing the message
        object as an argument. This technique is implemented to process the messages efficiently,
        and it continues to consume messages until the `stop()` method is called, or any
        discrepancy or exception occurs.

        :param message_handler:
                When each message is received, a method that takes a `KafkaConsumer` message
                object as an argument will be called. The message object contains attributes
                like `topic,` `partition,` `offset,` `key,` and `value.`
        """
        if self._running is None:
            print('Kafka consumer is not running!')
            return

        try:
            self._consumer.subscribe(self._topics)
            while self._running:
                records = self._consumer.poll(timeout_ms=1000)
                if records:
                    for _, consumer_list in records.items():
                        for message in consumer_list:
                            message_handler(message)
                            print(f'Received message: Partition={message.partition}, '
                                  f'Offset={message.offset}, Key={message.key}, '
                                  f'Value={message.value}')
                        if not self._running:
                            return
                time.sleep(0.01)
                return
        except KafkaError as e:
            print(f"Error during Kafka consumption: {e}")
        finally:
            self.stop()

    def stop(self):
        """
        This method gracefully stops the Kafka consumer, unsubscribes from the topic(s),
        and closes the connection. It's essential to call this method once the consumer completes
        message consumption to release resources. The following method will also be triggered
        when the consumer unsubscribes from the topic(s).

        :return: It returns True if the consumer has stopped successfully or if it was already
                stopped; otherwise, it returns False if an error occurred during the consumer
                unsubscription.
        """
        if self._running and self._consumer:
            try:
                self._consumer.unsubscribe()
                self._consumer = None
                self._running = False
                print('Kafka consumer is stopped and connection closed!')
                return True
            except KafkaError as e:
                print(f'Error stopping Kafka consumer: {e}')
                return False
        else:
            print('Kafka consumer is already stopped!')
            return True

    def is_running(self):
        """
        This method checks whether the Kafka consumer is currently running.

        :return: It returns True if the consumer has been started and is currently running else,
                it returns False.
        """
        return self._running
