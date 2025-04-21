import json
from typing import Generator, Any
from unittest.mock import Mock, patch, MagicMock

import pytest
from kafka.errors import KafkaError

from kafka_manager.kafka_consumer_client import KafkaConsumerClient

TARGET = 'kafka_manager.kafka_consumer_client'
MOCK_BOOTSTRAP_SERVERS = 'localhost:9092'
TEST_TOPIC = 'test_topic'
TEST_GROUP_ID = 'test_group_id'
VALUE_DESERIALIZER = lambda v: json.loads(v.decode('utf-8')) if v else None


class TestKafkaConsumerClient:
    """ Test KafkaConsumerClient Class """

    @pytest.fixture
    def kafka_consumer_client(self) -> KafkaConsumerClient:
        """ KafkaConsumerClient fixture """
        consumer_client = KafkaConsumerClient(
            bootstrap_servers=[MOCK_BOOTSTRAP_SERVERS],
            group_id=TEST_GROUP_ID,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            topics=[TEST_TOPIC],
        )
        return consumer_client

    @pytest.fixture
    def mock_kafka_consumer(self) -> Generator[Mock, Any, None]:
        """ KafkaConsumer fixture """
        with patch(f'{TARGET}.KafkaConsumer') as mock_kafka_consumer:
            yield mock_kafka_consumer

    def test_kafka_consumer_client_start(self, kafka_consumer_client: Mock, mock_kafka_consumer: Mock) -> None:
        """ Test KafkaConsumerClient start method """
        assert kafka_consumer_client.start() == True
        assert kafka_consumer_client._running == True

    def test_kafka_consumer_client_start_already_running(self, kafka_consumer_client: Mock,
                                                         mock_kafka_consumer: Mock) -> None:
        """ Test kafka consumer already running """
        kafka_consumer_client._running = True
        assert kafka_consumer_client.start() == True
        assert kafka_consumer_client._running == True

    def test_kafka_consumer_client_start_with_kafka_error(self, kafka_consumer_client: Mock,
                                                          mock_kafka_consumer: Mock) -> None:
        """ Test KafkaConsumerClient start method with Kafka Error """
        mock_kafka_consumer.side_effect = KafkaError('Kafka Error')
        assert kafka_consumer_client.start() == False
        assert kafka_consumer_client._running == False

    def test_kafka_consumer_client_consume(self, kafka_consumer_client: Mock, mock_kafka_consumer: Mock) -> None:
        """ Test consume method """
        mock_consumer_instance = mock_kafka_consumer.return_value
        mock_message = MagicMock(value='{"key": "value"}'.encode('utf-8'))
        mock_consumer_instance.poll.return_value = {
            MagicMock(): [mock_message]
        }

        kafka_consumer_client._consumer = mock_consumer_instance

        mock_callback = MagicMock()
        kafka_consumer_client._running = True
        kafka_consumer_client.consume(mock_callback)

        mock_consumer_instance.subscribe.assert_called_once_with([TEST_TOPIC])
        mock_callback.assert_called_once_with(mock_message)

    def test_kafka_consumer_client_consume_with_no_message(self, kafka_consumer_client: Mock,
                                                           mock_kafka_consumer: Mock) -> None:
        """ Test consume method with no message """
        mock_consumer_instance = mock_kafka_consumer.return_value
        mock_consumer_instance.poll.return_value = {}
        kafka_consumer_client._consumer = mock_consumer_instance

        mock_callback = MagicMock()
        kafka_consumer_client._running = True
        kafka_consumer_client.consume(mock_callback)

        mock_consumer_instance.subscribe.assert_called_once_with([TEST_TOPIC])
        mock_callback.assert_not_called()

    def test_kafka_consumer_client_consume_when_kafka_consumer_not_running(self, kafka_consumer_client: Mock) -> None:
        """ Test consume method when kafka consumer not running """
        kafka_consumer_client._running = None
        mock_callback = MagicMock()
        actual_response = kafka_consumer_client.consume(mock_callback)
        assert actual_response is None

    def test_kafka_consumer_client_consume_with_kafka_error(self, kafka_consumer_client: Mock,
                                                            mock_kafka_consumer: Mock) -> None:
        """ Test consume method with Kafka Error """
        mock_consumer_instance = mock_kafka_consumer.return_value
        mock_consumer_instance.poll.side_effect = KafkaError('Kafka Error')

        kafka_consumer_client._consumer = mock_consumer_instance
        mock_callback = MagicMock()
        kafka_consumer_client._running = True
        kafka_consumer_client.consume(mock_callback)

        mock_consumer_instance.subscribe.assert_called_once_with([TEST_TOPIC])
        mock_callback.assert_not_called()

    def test_kafka_consumer_client_consume_message_exits_kafka_consumer_not_running(self, kafka_consumer_client: Mock,
                                                                                    mock_kafka_consumer: Mock) -> None:
        """ Test consume method when kafka consumer not running """
        mock_consume_instance = mock_kafka_consumer.return_value
        mock_message = MagicMock(value='{"mock_key": "mock_value"}'.encode('utf-8'))
        mock_consume_instance.poll.return_value = {
            MagicMock(): [mock_message]
        }

        kafka_consumer_client._consumer = mock_consume_instance
        kafka_consumer_client._running = True

        mock_message_handler = MagicMock(side_effect=lambda v: setattr(kafka_consumer_client, '_running', False))
        kafka_consumer_client.consume(mock_message_handler)

        mock_consume_instance.subscribe.assert_called_once_with([TEST_TOPIC])
        mock_message_handler.assert_called_once_with(mock_message)

    def test_kafka_consumer_client_stop(self, kafka_consumer_client: Mock, mock_kafka_consumer: Mock) -> None:
        """ Test consume method stop """
        mock_consume_instance = mock_kafka_consumer.return_value

        kafka_consumer_client._running = True
        kafka_consumer_client._consumer = True

        kafka_consumer_client._consumer = mock_consume_instance
        assert kafka_consumer_client.stop() == True

        mock_consume_instance.unsubscribe.assert_called_once()

    def test_kafka_consumer_client_stop_with_kafka_error(self, kafka_consumer_client: Mock,
                                                         mock_kafka_consumer: Mock) -> None:
        """ Test consumer method when kafka throws error while unsubscribe """
        mock_consumer_instance = mock_kafka_consumer.return_value

        kafka_consumer_client._running = True
        kafka_consumer_client._consumer = True
        kafka_consumer_client._consumer = mock_consumer_instance

        mock_consumer_instance.unsubscribe.side_effect = KafkaError('Kafka Error')
        assert kafka_consumer_client.stop() == False

    def test_kafka_consumer_client_is_running(self, kafka_consumer_client: Mock) -> None:
        """ Test kafka consumer is running """
        kafka_consumer_client._running = True
        assert kafka_consumer_client.is_running() == True
