import unittest

from kafka_manager.kafka_consumer_client import KafkaConsumerClient
from kafka_manager.kafka_producer_client import KafkaProducerClient
from tests.integration.integration_test_utils import TestUtils


class TestKafkaConsumerClient(unittest.TestCase):
    SINGLE_TEST_TOPIC = "single_consumer_test_topic"
    MULTIPLE_TEST_TOPIC = "multiple_consumer_test_topic"

    KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]

    test_utils = TestUtils()

    @classmethod
    def setUpClass(cls):
        """ Setup Kafka Topic """
        if not cls.test_utils.is_kafka_broker_available(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS
        ):
            raise unittest.SkipTest("Kafka broker is not available!")
        cls.test_utils.create_topic(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS,
            topic_name=cls.SINGLE_TEST_TOPIC
        )
        cls.test_utils.create_topic(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS,
            topic_name=cls.MULTIPLE_TEST_TOPIC
        )

    @classmethod
    def tearDownClass(cls):
        """ Clean up Kafka Topics after running all tests """
        if cls.test_utils.is_kafka_broker_available(bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS):
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

    def test_kafka_consumer_client_start_and_stop(self):
        """ Test Kafka ConsumerClient start and stop"""
        consumer_client = KafkaConsumerClient(
            bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS,
            topics=[self.SINGLE_TEST_TOPIC],
            group_id='test_group_id',
            auto_offset_reset='earliest'
        )
        self.assertTrue(consumer_client.start())
        self.assertTrue(consumer_client.stop())

    def test_kafka_consumer_client_consume_single_message(self):
        """ Test Kafka Consumer Client consuming single message  """
        test_message = {
            "message": "Test Integration Messages"
        }

        self.assertIsNotNone(
            self.producer_client.send_message(
                self.SINGLE_TEST_TOPIC, test_message
            )
        )

        consumer_client = KafkaConsumerClient(
            bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS,
            topics=[self.SINGLE_TEST_TOPIC],
            group_id='test_group_id',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

        consumed_message = None

        def process_kafka_message(message):
            nonlocal consumed_message
            consumed_message = message.value
            consumer_client.stop()

        self.assertTrue(consumer_client.start())
        consumer_client.consume(process_kafka_message)
        self.assertEqual(consumed_message, test_message)

    def test_kafka_consumer_client_consume_multiple_message(self):
        """ Test Kafka Consumer Client consuming multiple messages """
        total_no_messages = 10
        test_messages = [{"message": f"Test Message_{i}"} for i in range(0, total_no_messages)]

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
