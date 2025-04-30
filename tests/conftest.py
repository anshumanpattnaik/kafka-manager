from typing import Generator, Any
from unittest.mock import Mock, patch

import pytest

from kafka_manager.kafka_manager import KafkaManager

TARGET_KAFKA_MANAGER = 'kafka_manager.kafka_manager'
TARGET_KAFKA_CONSUMER = 'kafka_manager.kafka_consumer_client'
TARGET_KAFKA_PRODUCER = 'kafka_manager.kafka_producer_client'

MOCK_BOOTSTRAP_SERVERS = 'localhost:9000'


@pytest.fixture
def kafka_manager() -> KafkaManager:
    """ KafkaManager fixture """
    manager = KafkaManager(bootstrap_servers=MOCK_BOOTSTRAP_SERVERS)
    return manager


@pytest.fixture
def kafka_producer_client() -> Generator[Mock, Any, None]:
    """ KafkaProducerClient fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaProducerClient') as mock_kafka_producer_client:
        yield mock_kafka_producer_client


@pytest.fixture
def kafka_consumer_client() -> Generator[Mock, Any, None]:
    """ KafkaConsumerClient fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaConsumerClient') as mock_kafka_consumer_client:
        yield mock_kafka_consumer_client


@pytest.fixture
def kafka_admin_client() -> Generator[Mock, Any, None]:
    """ KafkaAdminClient fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaAdminClient') as mock_kafka_admin_client:
        yield mock_kafka_admin_client


@pytest.fixture
def mock_start_producer() -> Generator[Mock, Any, None]:
    """ start producer fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaProducerClient.start') as mock_start:
        yield mock_start


@pytest.fixture
def mock_stop_producer() -> Generator[Mock, Any, None]:
    """ stop producer fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaProducerClient.stop') as mock_stop:
        yield mock_stop


@pytest.fixture
def mock_stop_all_consumers() -> Generator[Mock, Any, None]:
    """ stop all consumers fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaManager.stop_all_consumers') as mock_stop:
        yield mock_stop


@pytest.fixture
def mock_close_admin_client() -> Generator[Mock, Any, None]:
    """ close admin client fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaManager.close_admin_client') as close_admin_client:
        yield close_admin_client


@pytest.fixture
def mock_start_consumer() -> Generator[Mock, Any, None]:
    """ start consumer fixture """
    with patch('kafka_manager.kafka_consumer_client.KafkaConsumerClient.start') as mock_start:
        yield mock_start


@pytest.fixture
def mock_send_message() -> Generator[Mock, Any, None]:
    """ send message fixture"""
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaProducerClient.send_message') as send_message:
        yield send_message


@pytest.fixture
def mock_is_producer_running() -> Generator[Mock, Any, None]:
    """ is_producer_running fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.KafkaProducerClient.'
               f'is_producer_running') as mock_producer_running:
        yield mock_producer_running


@pytest.fixture
def mock_new_topic() -> Generator[Mock, Any, None]:
    """ new_topic fixture """
    with patch(f'{TARGET_KAFKA_MANAGER}.NewTopic') as new_topic:
        yield new_topic


@pytest.fixture
def mock_kafka_consumer() -> Generator[Mock, Any, None]:
    """ KafkaConsumer fixture """
    with patch(f'{TARGET_KAFKA_CONSUMER}.KafkaConsumer') as kafka_consumer:
        yield kafka_consumer


@pytest.fixture
def mock_kafka_producer() -> Generator[Mock, Any, None]:
    """ KafkaProducer fixture """
    with patch(f'{TARGET_KAFKA_PRODUCER}.KafkaProducer') as kafka_producer:
        yield kafka_producer

@pytest.fixture
def mock_kafka_producer_client() -> Generator[Mock, Any, None]:
    """ KafkaProducerClient fixture """
    with patch(f'{TARGET_KAFKA_PRODUCER}.KafkaProducerClient') as kafka_producer_client:
        yield kafka_producer_client
