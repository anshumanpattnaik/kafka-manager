import unittest

from kafka_manager.kafka_consumer_client import KafkaConsumerClient
from kafka_manager.kafka_producer_client import KafkaProducerClient
from tests.integration.integration_test_utils import TestUtils


class TestKafkaProducerClient(unittest.TestCase):
    SINGLE_TEST_TOPIC = "single_producer_test_topic"
    MULTIPLE_TEST_TOPIC = "multiple_producer_test_topic"

    KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

    test_utils = TestUtils()

    @classmethod
    def setUpClass(cls):
        """ Setup Kafka Topic """
        if not cls.test_utils.is_kafka_broker_available(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS
        ):
            raise unittest.SkipTest("Kafka broker is not available!")
        cls.test_utils.create_topic(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS, topic_name=cls.SINGLE_TEST_TOPIC
        )
        cls.test_utils.create_topic(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS, topic_name=cls.MULTIPLE_TEST_TOPIC
        )

    @classmethod
    def tearDownClass(cls):
        """ Clean up Kafka Topics after running all tests """
        if cls.test_utils.is_kafka_broker_available(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS
        ):
            cls.test_utils.delete_topic(
                bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS, topic_name=cls.SINGLE_TEST_TOPIC
            )
            cls.test_utils.delete_topic(
                bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS, topic_name=cls.MULTIPLE_TEST_TOPIC
            )

    def setUp(self):
        """ Setup Kafka Producer Client """
        self.producer_client = KafkaProducerClient(bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS)
        self.assertTrue(self.producer_client.start())

    def tearDown(self):
        """ Clean up Kafka Producer Client """
        self.assertTrue(self.producer_client.stop())

    def test_kafka_producer_client_send_and_receive_message(self):
        """ Test sending and receiving messages from Kafka """
        test_message = {
            "message": "Test Integration Messages"
        }

        self.assertIsNotNone(self.producer_client.send_message(self.SINGLE_TEST_TOPIC, test_message))

        consumer_client = KafkaConsumerClient(
            bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS,
            topics=[self.SINGLE_TEST_TOPIC],
            group_id='test_group_id',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        self.assertTrue(consumer_client.start())

        consumed_message = None

        def process_kafka_message(message):
            nonlocal consumed_message
            consumed_message = message.value
            consumer_client.stop()

        consumer_client.consume(process_kafka_message)
        self.assertEqual(consumed_message, test_message)

    def test_kafka_producer_client_send_and_receive_multiple_message(self):
        """ Test sending and receiving multiple messages from Kafka """
        total_no_messages = 2
        test_messages = [{"message": f"Test Integration Message_{i}"} for i in range(0, total_no_messages)]

        for value in test_messages:
            self.assertIsNotNone(self.producer_client.send_message(self.MULTIPLE_TEST_TOPIC, value))

        consumer_client = KafkaConsumerClient(
            bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS,
            topics=[self.MULTIPLE_TEST_TOPIC],
            group_id=f'test_group_id_multiple_messages_{self.id()}',
            auto_offset_reset='earliest'
        )
        self.assertTrue(consumer_client.start())

        consumed_messages = []

        def process_kafka_message(message):
            consumed_messages.append(message.value)
            if len(consumed_messages) == total_no_messages:
                consumer_client.stop()

        consumer_client.consume(process_kafka_message)
        self.assertEqual(consumed_messages, test_messages)

    def test_kafka_producer_client_send_message_non_existing_topic(self):
        """ Test sending and receiving messages to non-existing topic """
        nonexistent_topic = "testing_error_topic"
        test_message = {
            "message": "Test Integration Messages"
        }
        record_metadata = self.producer_client.send_message(nonexistent_topic, test_message)
        self.assertIsNone(record_metadata)

    def test_kafka_producer_client_send_with_empty_message(self):
        """ Test sending with empty message """
        self.assertIsNone(self.producer_client.send_message(self.SINGLE_TEST_TOPIC, None))

    def test_kafka_producer_client_with_invalid_address(self):
        """ Test start producer client with invalid address """
        invalid_address = "invalid_address:9092"
        producer_client = KafkaProducerClient(bootstrap_servers=[invalid_address])
        self.assertFalse(producer_client.start())

    def test_kafka_producer_client_with_not_started_producer(self):
        """ Test start producer client with not started producer """
        producer_client = KafkaProducerClient(bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS)
        self.assertTrue(producer_client.stop())

    def test_kafka_producer_client_send_message_with_none_value(self):
        """ Test send message with None value"""
        test_message = {
            "message": None
        }
        self.assertIsNotNone(
            self.producer_client.send_message(
                self.SINGLE_TEST_TOPIC,
                test_message
            )
        )

    def test_kafka_producer_client_send_messages_with_invalid_topic_name_with_spaces(self):
        """ Test send message with invalid topic name with spaces """
        invalid_topic_name_with_spaces = "invalid topic name"
        test_message = {
            "message": "Test Message"
        }

        with self.assertRaises(ValueError):
            self.producer_client.send_message(invalid_topic_name_with_spaces, test_message)

    def test_kafka_producer_client_stop_broker_before_sending_messages(self):
        """ Test stop broker before sending messages """
        test_message = {
            "message": "Test Message"
        }
        self.assertTrue(self.producer_client.stop())
        record_metadata = self.producer_client.send_message(self.SINGLE_TEST_TOPIC, test_message)
        self.assertIsNone(record_metadata)
