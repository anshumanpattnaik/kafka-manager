from typing import Generator, Any
from unittest.mock import Mock, patch, MagicMock

import pytest
from kafka.errors import KafkaError

from kafka_manager.kafka_producer_client import KafkaProducerClient

MOCK_BOOTSTRAP_SERVER = 'localhost:9092'
TEST_TOPIC = 'test_topic'


class TestKafkaProducerClient:
    """ Test KafkaProducerClient Class """

    @pytest.fixture
    def kafka_producer_client(self) -> KafkaProducerClient:
        """ KafkaProducerClient fixture """
        producer_client = KafkaProducerClient(
            bootstrap_servers=[MOCK_BOOTSTRAP_SERVER]
        )
        return producer_client

    def test_kafka_producer_client_start(self, kafka_producer_client: Mock, mock_kafka_producer: Mock) -> None:
        """ Test KafkaProducerClient start method """
        assert kafka_producer_client.start() == True

    def test_kafka_producer_client_start_already_producer_running(self, kafka_producer_client: Mock) -> None:
        """ Test Kafka producer already running """
        kafka_producer_client._producer = True
        assert kafka_producer_client.start() is None

    def test_kafka_producer_client_start_with_kafka_error(self, kafka_producer_client: Mock,
                                                          mock_kafka_producer: Mock) -> None:
        """ Test Kafka producer with kafka error """
        mock_kafka_producer.side_effect = KafkaError('Kafka Error')
        assert kafka_producer_client.start() == False

    def test_kafka_producer_client_send_message(self, kafka_producer_client: Mock,
                                                mock_kafka_producer: Mock) -> None:
        """ Test Kafka producer with send message"""
        mock_kafka_producer_instance = mock_kafka_producer.return_value

        mock_response = MagicMock()
        mock_kafka_producer_instance.send.return_value = mock_response
        mock_response.get.return_value = "mock_message"

        kafka_producer_client._producer = mock_kafka_producer_instance

        mock_value = '{"key": "value"}'
        response = kafka_producer_client.send_message(TEST_TOPIC, mock_value)
        assert response.get.return_value == "mock_message"
        mock_kafka_producer_instance.send.assert_called_once_with(TEST_TOPIC, mock_value)

    def test_kafka_producer_client_send_message_when_producer_not_running(self, kafka_producer_client: Mock) -> None:
        """ Test Kafka producer not running """
        kafka_producer_client._producer = None

        mock_value = '{"key": "value"}'
        response = kafka_producer_client.send_message(TEST_TOPIC, mock_value)
        assert response is None

    def test_kafka_producer_client_send_message_when_no_value_specified(self, kafka_producer_client: Mock,
                                                                        mock_kafka_producer: Mock) -> None:
        """ Test Kafka producer client when no value specified """
        mock_kafka_producer_instance = mock_kafka_producer.return_value
        kafka_producer_client._producer = mock_kafka_producer_instance

        response = kafka_producer_client.send_message(TEST_TOPIC)
        assert response is None

    def test_kafka_producer_client_send_message_with_kafka_error(self, kafka_producer_client: Mock,
                                                                 mock_kafka_producer: Mock) -> None:
        """ Test Kafka producer with kafka error """
        mock_kafka_producer_instance = mock_kafka_producer.return_value

        mock_kafka_producer_instance.send.side_effect = KafkaError('Kafka Error')
        kafka_producer_client._producer = mock_kafka_producer_instance

        mock_value = '{"key": "value"}'
        response = kafka_producer_client.send_message(TEST_TOPIC, mock_value)
        assert response is None

    def test_kafka_producer_client_flush_messages(self, kafka_producer_client: Mock, mock_kafka_producer: Mock) -> None:
        """ Test kafka producer flush messages """
        mock_kafka_producer_instance = mock_kafka_producer.return_value
        kafka_producer_client._producer = mock_kafka_producer_instance

        response = kafka_producer_client.flush()
        assert response is None
        mock_kafka_producer_instance.flush.assert_called_once()

    def test_kafka_producer_client_stop(self, kafka_producer_client: Mock, mock_kafka_producer: Mock) -> None:
        """ Test kafka producer stop """
        mock_kafka_producer_instance = mock_kafka_producer.return_value
        kafka_producer_client._producer = mock_kafka_producer_instance

        response = kafka_producer_client.stop()
        assert response == True
        mock_kafka_producer_instance.close.assert_called_once()

    def test_kafka_producer_client_stop_with_kafka_error(self, kafka_producer_client: Mock,
                                                         mock_kafka_producer: Mock) -> None:
        """ Test kafka producer stop with kafka error """
        mock_kafka_producer_instance = mock_kafka_producer.return_value
        kafka_producer_client._producer = mock_kafka_producer_instance

        mock_kafka_producer_instance.close.side_effect = KafkaError('Kafka Error')
        response = kafka_producer_client.stop()
        assert response == False

    def test_kafka_producer_client_stop_already_stopped(self, kafka_producer_client: Mock) -> None:
        """ Test kafka producer stop when producer already stopped """
        response = kafka_producer_client.stop()
        assert response == True

    def test_kafka_producer_client_checking_is_producer_running(self, kafka_producer_client: Mock,
                                                                mock_kafka_producer: Mock) -> None:
        """ Test kafka producer checking is producer running """
        mock_kafka_producer_instance = mock_kafka_producer.return_value
        kafka_producer_client._producer = mock_kafka_producer_instance

        response = kafka_producer_client.is_producer_running()
        assert response is not None
