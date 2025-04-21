from unittest.mock import Mock, MagicMock

from kafka.errors import KafkaError

MOCK_BOOTSTRAP_SERVERS = 'localhost:9092'


class TestKafkaManager:
    """ Test KafkaManager Class """

    def test_start_producer(self, mock_start_producer: Mock, kafka_manager: Mock) -> None:
        """ Test start producer method """
        mock_start_producer.return_value = True
        actual_response = kafka_manager.start_producer()
        assert actual_response is True

    def test_send_message(self, mock_send_message: Mock, kafka_manager: Mock) -> None:
        """ Test send message method"""
        topic = "mock_topic"
        value = {
            "message": "mock_message"
        }
        mock_send_message.return_value = MagicMock()
        actual_response = kafka_manager.send_message(topic, value)

        assert actual_response is not None
        mock_send_message.assert_called_once_with(topic=topic, value=value)

    def test_stop_producer(self, mock_stop_producer: Mock, kafka_manager: Mock) -> None:
        """ Test stop producer method """
        mock_stop_producer.return_value = True
        actual_response = kafka_manager.stop_producer()
        assert actual_response is True

    def test_is_producer_running(self, mock_is_producer_running: Mock, kafka_manager: Mock) -> None:
        """ Test is_producer_running method """
        mock_is_producer_running.return_value = True
        actual_response = kafka_manager.is_producer_running()
        assert actual_response is True

    def test_create_consumer(self, kafka_consumer_client: Mock, kafka_manager: Mock) -> None:
        """ Test create consumer method """
        topics = ["mock_topic_1"]
        group_id = "mock_group_id"
        auto_offset_reset = "earliest"

        mock_consumer_instance = kafka_consumer_client.return_value
        consumer = kafka_manager.create_consumer(topics=topics, group_id=group_id, auto_offset_reset=auto_offset_reset)

        assert consumer == mock_consumer_instance
        kafka_consumer_client.assert_called_once_with(bootstrap_servers=MOCK_BOOTSTRAP_SERVERS, topics=topics,
                                                      group_id=group_id, auto_offset_reset=auto_offset_reset)

    def test_create_consumer_with_kafka_error(self, kafka_consumer_client: Mock, kafka_manager: Mock) -> None:
        """ Test consumer creation failed """
        topics = ["mock_topic_1"]

        kafka_consumer_client.side_effect = KafkaError("Failed to create consumer")
        consumer = kafka_manager.create_consumer(topics=topics)
        assert consumer is None

    def test_start_consumer(self, kafka_consumer_client: Mock, kafka_manager: Mock) -> None:
        """ Test start consumer method """
        mock_kafka_consumer_client_instance = kafka_consumer_client.return_value
        mock_kafka_consumer_client_instance.start.return_value = True
        kafka_manager._consumers['test_consumer'] = mock_kafka_consumer_client_instance
        actual_response = kafka_manager.start_consumer('test_consumer')
        assert actual_response is True
        mock_kafka_consumer_client_instance.start.assert_called_once()

    def test_start_consumer_with_invalid_consumer_id(self, kafka_consumer_client: Mock, kafka_manager: Mock) -> None:
        """ Test start consumer with invalid consumer id """
        mock_kafka_consumer_client_instance = kafka_consumer_client.return_value
        actual_response = kafka_manager.start_consumer('invalid_consumer_id')
        assert actual_response is False
        mock_kafka_consumer_client_instance.assert_not_called()

    def test_consume_messages(self, kafka_consumer_client: Mock, kafka_manager: Mock) -> None:
        """ Test consume messages method"""
        mock_kafka_consumer_client_instance = kafka_consumer_client.return_value
        mock_kafka_consumer_client_instance.consume.return_value = True
        kafka_manager._consumers['test_consumer'] = mock_kafka_consumer_client_instance

        message_handler = MagicMock()
        kafka_manager.consume_messages('test_consumer', message_handler)
        mock_kafka_consumer_client_instance.consume.assert_called_once_with(message_handler)

    def test_consume_messages_with_invalid_consumer_id(self, kafka_consumer_client: Mock, kafka_manager: Mock) -> None:
        """ Test consume messages with invalid consumer id"""
        mock_kafka_consumer_client_instance = kafka_consumer_client.return_value
        message_handler = MagicMock()
        kafka_manager.consume_messages('invalid_consumer_id', message_handler)
        mock_kafka_consumer_client_instance.assert_not_called()

    def test_stop_consumers(self, kafka_consumer_client: Mock, kafka_manager: Mock) -> None:
        """ Test stop consumers method"""
        mock_kafka_consumer_client_instance = kafka_consumer_client.return_value
        mock_kafka_consumer_client_instance.stop.return_value = True
        kafka_manager._consumers['test_consumer'] = mock_kafka_consumer_client_instance

        actual_response = kafka_manager.stop_consumer('test_consumer')
        assert actual_response is True
        mock_kafka_consumer_client_instance.stop.assert_called_once()

    def test_stop_consumers_with_invalid_consumer_id(self, kafka_consumer_client: Mock, kafka_manager: Mock) -> None:
        """ Test stop consumers with invalid consumer id """
        mock_kafka_consumer_client_instance = kafka_consumer_client.return_value
        kafka_manager.stop_consumer('invalid_consumer_id')
        mock_kafka_consumer_client_instance.assert_not_called()

    def test_stop_all_consumers(self, kafka_manager: Mock) -> None:
        """ Test stop all consumers method """
        mock_consumer = MagicMock()
        kafka_manager._consumers['test_consumer'] = mock_consumer
        kafka_manager.stop_all_consumers()
        mock_consumer.stop.assert_called_once()
        assert kafka_manager._consumers == {}

    def test_connect_admin_client(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test connect admin client method """
        mock_kafka_admin_client_instance = kafka_admin_client.return_value
        actual_response = kafka_manager.connect_admin_client()
        assert actual_response is True
        assert kafka_manager._admin_client == mock_kafka_admin_client_instance
        kafka_admin_client.assert_called_once_with(bootstrap_servers=MOCK_BOOTSTRAP_SERVERS)

    def test_connect_admin_client_with_kafka_error(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test connect admin client method with Kafka error"""
        kafka_admin_client.side_effect = KafkaError("Failed to connect to Kafka Admin client")
        admin_client = kafka_manager.connect_admin_client()
        assert admin_client == False
        assert kafka_manager._admin_client is None

    def test_create_topic(self, kafka_admin_client: Mock, kafka_manager: Mock, mock_new_topic: Mock) -> None:
        """ Test create topic method """
        mock_kafka_admin_client_instance = kafka_admin_client.return_value
        mock_new_topic_instance = mock_new_topic.return_value

        mock_topic_name = "test_topic"
        mock_num_partitions = 2
        mock_replication_factor = 1

        admin_client_response = kafka_manager.connect_admin_client()
        assert admin_client_response == True

        create_topic_response = kafka_manager.create_topic(mock_topic_name, mock_num_partitions,
                                                           mock_replication_factor)
        assert create_topic_response == True

        mock_kafka_admin_client_instance.create_topics.assert_called_once_with(new_topics=[mock_new_topic_instance],
                                                                               validate_only=False)
        mock_new_topic.assert_called_once_with(name=mock_topic_name, num_partitions=mock_num_partitions,
                                               replication_factor=mock_replication_factor)

    def test_create_topic_with_not_connected_admin_client(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test create topic method with not connected admin client """
        kafka_admin_client.return_value = None

        mock_topic_name = "test_topic"
        mock_num_partitions = 2
        mock_replication_factor = 1

        create_topic_response = kafka_manager.create_topic(mock_topic_name, mock_num_partitions,
                                                           mock_replication_factor)
        assert create_topic_response == False

    def test_create_topic_with_kafka_error(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test create topic method with Kafka error """
        mock_kafka_admin_client_instance = kafka_admin_client.return_value
        mock_kafka_admin_client_instance.create_topics.side_effect = KafkaError('Failed to connect to Kafka Admin')

        admin_client_response = kafka_manager.connect_admin_client()
        assert admin_client_response == True

        mock_topic_name = "test_topic"
        mock_num_partitions = 2
        mock_replication_factor = 1

        create_topic_response = kafka_manager.create_topic(mock_topic_name, mock_num_partitions,
                                                           mock_replication_factor)
        assert create_topic_response == False

    def test_delete_topic(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test delete topic method """
        mock_kafka_admin_client_instance = kafka_admin_client.return_value

        admin_client_response = kafka_manager.connect_admin_client()
        assert admin_client_response == True

        mock_topic_name = "test_topic"
        delete_topic_response = kafka_manager.delete_topic(mock_topic_name)
        assert delete_topic_response == True
        mock_kafka_admin_client_instance.delete_topics.assert_called_once_with(topics=[mock_topic_name])

    def test_delete_topic_with_not_connected_admin_client(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test delete topic method with not connected admin client """
        kafka_admin_client.return_value = None

        mock_topic_name = "test_topic"
        delete_topic_response = kafka_manager.delete_topic(mock_topic_name)
        assert delete_topic_response == False

    def test_delete_topic_with_kafka_error(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test delete topic method with Kafka error """
        mock_kafka_admin_client_instance = kafka_admin_client.return_value
        mock_kafka_admin_client_instance.delete_topics.side_effect = KafkaError('Failed to connect to Kafka Admin')

        admin_client_response = kafka_manager.connect_admin_client()
        assert admin_client_response == True

        mock_topic_name = "test_topic"
        delete_topic_response = kafka_manager.delete_topic(mock_topic_name)
        assert delete_topic_response == False

    def test_close_admin_client(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test close admin client """
        mock_kafka_admin_client_instance = kafka_admin_client.return_value
        kafka_manager._admin_client = mock_kafka_admin_client_instance

        actual_response = kafka_manager.close_admin_client()
        assert actual_response == True
        mock_kafka_admin_client_instance.close.assert_called_once()
        assert kafka_manager._admin_client is None

    def test_close_admin_client_with_kafka_error(self, kafka_admin_client: Mock, kafka_manager: Mock) -> None:
        """ Test close admin client with Kafka error """
        mock_kafka_admin_client_instance = kafka_admin_client.return_value
        mock_kafka_admin_client_instance.close.side_effect = KafkaError('Failed to connect to Kafka Admin')
        kafka_manager._admin_client = mock_kafka_admin_client_instance

        actual_response = kafka_manager.close_admin_client()
        assert actual_response == False
        assert kafka_manager._admin_client is mock_kafka_admin_client_instance

    def test_close_admin_client_with_already_closed(self, kafka_manager: Mock) -> None:
        """ Test close admin client with already closed """
        actual_response = kafka_manager.close_admin_client()
        assert actual_response == True

    def test_close(self, kafka_manager: Mock, mock_stop_producer: Mock, mock_stop_all_consumers: Mock,
                   mock_close_admin_client: Mock) -> None:
        """ Test close method """
        kafka_manager.close()

        mock_stop_producer.assert_called_once()
        mock_stop_all_consumers.assert_called_once()
        mock_close_admin_client.assert_called_once()
