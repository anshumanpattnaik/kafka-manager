import json

from src.kafka_manager import KafkaManager

bootstrap_servers = ['localhost:9092']  # Replace with your Kafka broker addresses
topic_name = 'example_topic'  # Replace topic name with your choice
group_id = 'example_group'  # Replace consumer group ID with your choice


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


def main():
    # 1. Create a KafkaManager instance
    kafka_manager = KafkaManager(bootstrap_servers=bootstrap_servers)

    # 2. For topic management connect to Kafka admin client
    if not kafka_manager.connect_admin_client():
        print('Failed to connect to Kafka admin client.')
        exit()

    # 3. Create a topic - (if it doesn't exist)
    try:
        # Define topic
        if not kafka_manager.create_topic(topic_name=topic_name, num_partitions=1, replication_factor=1):
            print(f'Failed to create topic "{topic_name}". Check that if the topic already exists.')
        # In production, please ensure the topic exists; don't exit the check and continue.
    except Exception as e:
        print(f'Error in creating Kafka topic: {e}')

    # 4. Start the Kafka producer
    if not kafka_manager.start_producer():
        print('Failed to start Kafka producer.')
        exit()

    # 5. Send Kafka message
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

    # 6. Create a Kafka Consumer
    consumer = kafka_manager.create_consumer(topics=[topic_name], group_id=group_id, auto_offset_reset='earliest')
    if consumer is None:
        print('Failed to create consumer.')
        exit()

    # 7. Start the Kafka Consumer
    if not kafka_manager.start_consumer(consumer_id=group_id):
        print('Failed to start consumer.')
        exit()

    # 8. Consume Messages
    kafka_manager.consume_messages(consumer_id=group_id, message_handler=message_handler)

    # 9. Stop Kafka producers, Kafka consumers and Admin Client.
    kafka_manager.stop_producer()
    kafka_manager.stop_consumer(consumer_id=group_id)
    kafka_manager.close_admin_client()
    print('Stopped all Kafka producer, consumer and admin client.')


if __name__ == "__main__":
    main()
