kafka-manager
=============

.. image:: https://img.shields.io/badge/license-MIT-blue
    :target: https://github.com/anshumanpattnaik/kafka-manager/blob/main/LICENSE

.. image:: https://codecov.io/gh/anshumanpattnaik/kafka-manager/graph/badge.svg?token=8DES91MFEU
    :target: https://codecov.io/gh/anshumanpattnaik/kafka-manager

A Kafka Manager is a Python utility class that simplifies Kafka interactions by providing a high-level abstraction for
managing Producers, Consumers, and Topics. It provides a user-friendly interface for developers to implement Kafka
effectively in their applications, encapsulating the complexity of the Kafka-python library. This abstraction allows
quicker development and more manageable maintenance of Kafka-related applications.

Requirements
------------
* Python 3.7+
* kafka-python

Installation
------------
.. code:: bash

    $ pip install kafka-manager

Features
--------

Producer Management
*******************
It provides interfaces to start/stop producers, send messages to topics, and check producer running status. It also invokes functions to initialize and terminate producer instances to publish messages to Kafka, and it effectively checks the producer status to ensure that messages are sent successfully.

.. code:: python

    import json

    from kafka_manager.kafka_manager import KafkaManager

    bootstrap_servers = ['localhost:9092']  # Replace with your Kafka broker addresses
    topic_name = 'example_topic'  # Replace topic name with your choice
    group_id = 'example_group'  # Replace consumer group ID with your choice

    # Create a KafkaManager instance
    kafka_manager = KafkaManager(bootstrap_servers=bootstrap_servers)

    # Start the Kafka producer
    kafka_manager.start_producer();

    # Send Kafka message
    try:
        message_payload = json.dumps({
            "message_key": "message_value"
        })
        metadata = kafka_manager.send_message(topic=topic_name, value=message_payload)
        if metadata:
            print(f'Message sent successfully to Kafka topic: "{topic_name}"')
        else:
            print(f'Failed to send message to Kafka topic: "{topic_name}"')
    except Exception as e:
        print(f'Error in sending message to Kafka topic: {e}')

    # Stop Kafka producer
    kafka_manager.stop_producer();

Consumer Management
*******************
It enables configuring various configurations to Create/Manage consumers and provides an interface to start/stop consumers. The Kafka Manager allows developers to create consumers per their application needs, such as different deserialization methods or offset management strategies. It provides a user-defined callback function to consume messages, allowing developers to define custom logic for processing each received message and enabling further data processing.

.. code:: python

    from kafka_manager.kafka_manager import KafkaManager

    bootstrap_servers = ['localhost:9092']  # Replace with your Kafka broker addresses
    topic_name = 'example_topic'  # Replace topic name with your choice
    group_id = 'example_group'  # Replace consumer group ID with your choice

    # Create a KafkaManager instance
    kafka_manager = KafkaManager(bootstrap_servers=bootstrap_servers)

    # Create a Kafka Consumer
    consumer = kafka_manager.create_consumer(topics=[topic_name], group_id=group_id, auto_offset_reset='earliest')

    # Start the Kafka Consumer
    kafka_manager.start_consumer(consumer_id=group_id):

    def message_handler(message):
        """
        This method is a callback function called by the consumer, which handles the received messages when a new message
        arrives.

        In production real-world application, the received message would be processed as follows:
        - Perform some business logic
        - Store the message in a database for further processing.
        - Message deserialization
        - etc.

        :param message: Message received from the consumer.
        """
        print(f'Received message: Partition={message.partition}, Offset={message.offset}, Value={message.value}')

    # Consume Messages
    kafka_manager.consume_messages(consumer_id=group_id, message_handler=message_handler)

Topic Management
*******************
Kafka Manager allows developers to create and delete topics dynamically, which serve as categories from which messages are published. It's essential for managing data streams and evolving application requirements.

.. code:: python

    from kafka_manager.kafka_manager import KafkaManager

    bootstrap_servers = ['localhost:9092']  # Replace with your Kafka broker addresses
    topic_name = 'example_topic'  # Replace topic name with your choice
    group_id = 'example_group'  # Replace consumer group ID with your choice

    # Create a KafkaManager instance
    kafka_manager = KafkaManager(bootstrap_servers=bootstrap_servers)

    # For topic management connect to Kafka admin client
    kafka_manager.connect_admin_client()

    # Create a topic - (if it doesn't exist)
    kafka_manager.create_topic(topic_name=topic_name, num_partitions=1, replication_factor=1)

Admin Client
*******************
It provides interfaces to connect to the Kafka Admin client and allows developers to perform administrative operations such as creating and deleting topics. However, the admin-client connection is vital to performing many advanced Kafka management tasks, such as describing cluster configurations and managing Kafka ACLs.

.. code:: python

    from kafka_manager.kafka_manager import KafkaManager

    bootstrap_servers = ['localhost:9092']  # Replace with your Kafka broker addresses
    topic_name = 'example_topic'  # Replace topic name with your choice
    group_id = 'example_group'  # Replace consumer group ID with your choice

    # Create a KafkaManager instance
    kafka_manager = KafkaManager(bootstrap_servers=bootstrap_servers)

    # Connect to Kafka admin client
    admin_client = kafka_manager.connect_admin_client()

    # Listing Consumer Groups
    consumers_groups = admin_client.list_consumer_groups()
    print(consumers_groups)

    # Describing Consumer Groups
    admin_client.describe_consumer_groups(list(consumers_groups))

Error Handling
*******************
To handle errors in Kafka due to network failures, broker failures, or misconfigurations, Kafka Manager handles these exceptions efficiently and ensures application stability.

Resource Management
*******************
Kafka Manager resource management ensures that all connections to Kafka are correctly closed. It provides a close() function for proper shutdown, which prevents resource leaks and potential data corruption. It's essential for maintaining data integrity and managing the Kafka cluster and application.

License
*******

MIT License, See `LICENSE <https://github.com/anshumanpattnaik/kafka-manager/blob/main/LICENSE>`_.
