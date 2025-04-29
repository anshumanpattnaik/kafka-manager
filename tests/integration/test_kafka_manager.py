import time
import unittest
import uuid

from kafka import KafkaAdminClient

from kafka_manager.kafka_consumer_client import KafkaConsumerClient
from kafka_manager.kafka_manager import KafkaManager
from tests.integration.integration_test_utils import TestUtils


class TestKafkaManager(unittest.TestCase):
    KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
    TEST_TOPIC = f'test_manager_topic_{uuid.uuid4().hex[:8]}'
    TEST_GROUP_ID = f'test_manager_group_id_{uuid.uuid4().hex[:8]}'

    test_utils = TestUtils()

    @classmethod
    def setUpClass(cls):
        """ Setup Kafka Topic """
        if not cls.test_utils.is_kafka_broker_available(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS
        ):
            raise unittest.SkipTest("Kafka broker is not available!")
        cls.test_utils.create_topic(
            bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS, topic_name=cls.TEST_TOPIC
        )

    @classmethod
    def tearDownClass(cls):
        """ Clean up Kafka Topics after running all tests """
        if cls.test_utils.is_kafka_broker_available(bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS):
            cls.test_utils.delete_topic(
                bootstrap_servers=cls.KAFKA_BOOTSTRAP_SERVERS, topic_name=cls.TEST_TOPIC
            )

    def setUp(self):
        """ Setup Kafka Manager """
        self.kafka_manager = KafkaManager(
            bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS
        )
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS
        )

    def tearDown(self):
        """ Clean up Kafka Manager """
        self.kafka_manager.close_admin_client()
        self.kafka_manager.stop_producer()
        self.kafka_manager.stop_all_consumers()

    def test_kafka_manager_start_and_stop_producer(self):
        """ Test Kafka producer starting and stopping via Kafka Manager """
        self.assertTrue(self.kafka_manager.start_producer())
        self.assertTrue(self.kafka_manager.is_producer_running())
        self.assertTrue(self.kafka_manager.stop_producer())
        self.assertFalse(self.kafka_manager.is_producer_running())

    def test_kafka_manager_send_message(self):
        """ Test sending messages via Kafka Manager """
        self.kafka_manager.start_producer()
        test_message = {
            "message": "test_message"
        }
        test_metadata = self.kafka_manager.send_message(self.TEST_TOPIC, test_message)
        self.assertIsNotNone(test_metadata)
        self.kafka_manager.stop_producer()

        consumer_client = KafkaConsumerClient(
            bootstrap_servers=self.KAFKA_BOOTSTRAP_SERVERS,
            topics=[self.TEST_TOPIC],
            group_id=self.TEST_GROUP_ID,
            auto_offset_reset='earliest'
        )
        self.assertTrue(consumer_client.start())

        consumed_message = None

        def process_kafka_message(message):
            nonlocal consumed_message
            consumed_message = message.value
            consumer_client.stop()

        consumer_client.consume(process_kafka_message)
        self.assertEqual(consumed_message, test_message)

    def test_kafka_manager_create_consumer_and_send_message(self):
        """ Test creating and sending messages via Kafka Manager """
        consumer = self.kafka_manager.create_consumer(
            topics=[self.TEST_TOPIC],
            group_id=self.TEST_GROUP_ID,
            auto_offset_reset='earliest'
        )
        self.assertIsInstance(consumer, KafkaConsumerClient)
        self.assertTrue(self.kafka_manager.start_consumer(self.TEST_GROUP_ID))

        received_messages = []
        is_stop_consuming = False

        def process_kafka_message(message):
            nonlocal is_stop_consuming
            received_messages.append(message.value)
            is_stop_consuming = True

        self.kafka_manager.start_producer()
        test_message = {
            "message": "Test Message"
        }
        self.kafka_manager.send_message(self.TEST_TOPIC, test_message)
        self.kafka_manager.stop_producer()

        start_time = time.time()
        while not is_stop_consuming and time.time() - start_time < 5:
            self.kafka_manager.consume_messages(self.TEST_GROUP_ID, process_kafka_message)
            time.sleep(0.1)

        self.assertTrue(is_stop_consuming)
        self.assertEqual(len(received_messages), 1)
        self.assertEqual(received_messages[0], test_message)
        self.assertTrue(self.kafka_manager.stop_consumer(self.TEST_GROUP_ID))
