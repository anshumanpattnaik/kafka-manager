"""
Kafka Manager Module

The Kafka Manager module provides a high-level abstraction over the `kafka-python` library to
simplify interaction with Apache Kafka. It offers many functionalities to manage Kafka Producers,
Consumers, Topics, and Admin Client operations, encapsulating the complexity behind
`kafka-python` usage.
"""
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import KafkaError

from kafka_manager.kafka_consumer_client import KafkaConsumerClient
from kafka_manager.kafka_producer_client import KafkaProducerClient


class KafkaManager:
    """
    A utility class to manage Kafka producers, consumers, and topics. It provides a higher level of
    abstraction for interacting with Kafka, encapsulating and defining methods to manage producers
    and consumers, and allowing administrative operations like topic creation, deletion, etc. It
    leverages the `KafkaProducerClient` and `KafkaConsumerClient` classes for lifecycle management
    to access and invoke the methods of producers and consumers.
    """

    def __init__(
        self,
        bootstrap_servers
    ):
        """
        Initializes the Kafka manager, establishes configurations for connecting to the
        Kafka broker(s) and instantiates `KafkaProducerClient` and initializes admin client.

        :param bootstrap_servers:
            A list of Kafka broker addresses (e.g., ['localhost:9092', 'kafka-broker-1:9092'])
            These addresses are used to establish the initial connection to the Kafka cluster.
        """
        self._bootstrap_servers = bootstrap_servers
        self._producer_client = KafkaProducerClient(bootstrap_servers=self._bootstrap_servers)
        self._admin_client = None
        self._consumers = {}

    @property
    def producer_client(self):
        """
        Returns the Kafka producer client.
        """
        return self._producer_client

    @property
    def admin_client(self):
        """
        Returns the Kafka admin client.
        """
        return self._admin_client

    @property
    def consumers(self):
        """
        Returns the Kafka consumer.
        """
        return self._consumers

    def start_producer(self):
        """
        This method starts the Kafka producer client and establishes the connection to the
        Kafka broker(s), which this instance manages.

        :return: It returns True if the Kafka producer started successfully else it returns False.
        """
        return self._producer_client.start()

    def send_message(
        self,
        topic,
        value
    ):
        """
        This method sends message to the specified Kafka topic by using the managed
        Kafka producer client.

        :param topic: The name of the Kafka topic send the message to.
        :param value: The serialized message JSON payload.
        :return: If the sent message was successful, it returns Metadata; otherwise, it returns
                None.
        """
        return self._producer_client.send_message(topic=topic, value=value)

    def stop_producer(self):
        """
        This method will stop the managed Kafka producer client.

        :return: It returns True if the Kafka producer stopped successfully else it returns False.
        """
        return self._producer_client.stop()

    def is_producer_running(self):
        """
        This method checks if the managed Kafka producer client is running.

        :return: It returns True if the managed Kafka producer client is running and False
                otherwise.
        """
        return self._producer_client.is_producer_running()

    def create_consumer(
        self,
        topics: list = None,
        group_id: str = None,
        auto_offset_reset: str = 'latest',
        **kwargs
    ):
        """
        This method creates a new `KafkaConsumerClient` instance with the given configuration,
        establishes the connection to the Kafka broker(s), and stores the specific consumer's
        configuration in the `consumers` dictionary with the group_id key.

        :param topics: A list of Kafka topics to subscribe to.
        :param group_id: A consumer group_id and defaults to None.
        :param auto_offset_reset:
                An optional parameter to sort the message ordering, which is set by default to
                'latest' when the initial offset in Kafka does not exist.
                    - 'earliest': Automatically sorts messages earlier than the initial offset.
                    - 'latest': Automatically sorts messages later than the initial offset.
        :param kwargs: Additional arguments are passed directly to the Kafka Consumer constructor,
                which allows further customization of the consumer (e.g., Security Settings, etc.).
        :return: It returns the new `KafkaConsumerClient` instance.
        """
        try:
            consumer_client = KafkaConsumerClient(bootstrap_servers=self._bootstrap_servers,
                                                  topics=topics, group_id=group_id,
                                                  auto_offset_reset=auto_offset_reset,
                                                  **kwargs)
            self._consumers[group_id if group_id else
            f"default_consumer_{len(self._consumers)}"] = consumer_client
            return consumer_client
        except KafkaError as e:
            print(f'Error in creating consumer: {e}')
            return None

    def start_consumer(
        self,
        consumer_id
    ):
        """
        This method starts a specific `KafkaConsumerClient` instance of the given `consumer_id`.

        :param consumer_id: An ID of the consumer client to start.
        :return: It returns True if the consumer started successfully else, it returns False if the
                consumer ID is not found.
        """
        if consumer_id in self._consumers:
            return self._consumers[consumer_id].start()
        print(f"Consumer with ID {consumer_id} not found.")
        return False

    def consume_messages(
        self,
        consumer_id,
        message_handler
    ):
        """
        This method consumes messages from the given `consumer_id` of a specific consumer.

        :param consumer_id: An ID of the consumer client to start consuming messages from.
        :param message_handler: A function to call when a message is received.
        """
        if consumer_id in self._consumers:
            self._consumers[consumer_id].consume(message_handler)
        else:
            print(f'Consumer with ID {consumer_id} not found.')

    def stop_consumer(
        self,
        consumer_id
    ):
        """
        This method stops a specific `KafkaConsumerClient` instance of the given `consumer_id`.

        :param consumer_id: An ID of the consumer client to stop consuming messages from.
        :return: It returns True if the consumer stopped successfully else, it returns False if the
                consumer ID is not found.
        """
        if consumer_id in self._consumers:
            return self._consumers[consumer_id].stop()
        print(f'Consumer with ID {consumer_id} not found.')
        return False

    def stop_all_consumers(self):
        """
        This method stops all `KafkaConsumerClient` instances.
        """
        for _, consumer_client in self._consumers.items():
            consumer_client.stop()
        self._consumers = {}

    def connect_admin_client(self):
        """
        This method connects to the Kafka admin client, establishes a connection to the
        Kafka broker(s), and performs administrative operations on the Kafka cluster.

        :return: It returns True if the connection to the Kafka admin client was successful; else,
            it returns False.
        """
        try:
            self._admin_client = KafkaAdminClient(
                bootstrap_servers=self._bootstrap_servers
            )
            print('Kafka admin client is connected!')
            return self._admin_client
        except KafkaError as e:
            print(f'Error in connecting to Kafka admin client: {e}')
        return False

    def create_topic(
        self,
        topic_name: str = None,
        num_partitions: int = 1,
        replication_factor: int = 1
    ):
        """
        This method creates a new topic with the given `topic_name` and it's an administrative
        operation, it can only be created by the admin client.

        :param topic_name: Name of the new topic.
        :param num_partitions: Number of partitions of the new topic. Defaults to 1.
        :param replication_factor: Replication factor of the new topic. Defaults to 1.
        :return: It returns True if the topic was created successfully; else it returns False
            otherwise.
        """
        if self._admin_client is None:
            print('Kafka admin client is not connected.')
            return False

        try:
            topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions,
                                   replication_factor=replication_factor)]
            self._admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f'Kafka topic {topic_name} created with {num_partitions} partitions and '
                  f'replication factor {replication_factor}.')
            return True
        except KafkaError as e:
            print(f'Error in creating Kafka topic: {e}')
            return False

    def delete_topic(
        self,
        topic_name
    ):
        """
        This method deletes a topic with the given `topic_name` and it's an administrative
        operation, it can only be deleted by the admin client.

        :param topic_name: Name of the new topic.
        :return: It returns True if the topic was deleted successfully; else it returns False
            otherwise.
        """
        if self._admin_client is None:
            print('Kafka admin client is not connected.')
            return False

        try:
            self._admin_client.delete_topics(topics=[topic_name])
            print(f'Kafka topic {topic_name} deleted.')
            return True
        except KafkaError as e:
            print(f'Error in deleting Kafka topic: {e}')
            return False

    def close_admin_client(self):
        """
        This method closes the Kafka admin client connection.

        :return: It returns True if the Kafka admin client was closed successfully; else it returns
            False otherwise.
        """
        if self._admin_client:
            try:
                self._admin_client.close()
                self._admin_client = None
                print('Kafka admin client connection is closed.')
                return True
            except KafkaError as e:
                print(f'Error in closing Kafka admin client: {e}')
                return False
        return True

    def close(self):
        """
        This method closes all the Kafka connections (producers, consumers, and admin clients).
        It's essential to call this method to ensure that all producers, consumers, and admin
        clients are stopped properly and resources are released.
        """
        self.stop_producer()
        self.stop_all_consumers()
        self.close_admin_client()
