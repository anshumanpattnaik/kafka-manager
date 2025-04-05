import logging
import string
import time
from random import choice

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

logging.basicConfig(level=logging.INFO)


class TestUtils:

    @staticmethod
    def is_kafka_broker_available(
        bootstrap_servers,
        timeout=10
    ):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
                admin_client.close()
                return True
            except NoBrokersAvailable:
                time.sleep(1)
        return False

    @staticmethod
    def create_topic(
        bootstrap_servers,
        topic_name
    ):
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logging.info(f"Topic {topic_name} created successfully.")
        except TopicAlreadyExistsError:
            logging.info(f"Topic {topic_name} already exists.")
        except NoBrokersAvailable as e:
            logging.info(f"Kafka broker is not available: {e}")
        finally:
            admin_client.close()

    @staticmethod
    def delete_topic(
        bootstrap_servers,
        topic_name
    ):
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            admin_client.delete_topics(topics=[topic_name])
            logging.info(f"Topic {topic_name} deleted successfully.")
        except NoBrokersAvailable as e:
            logging.info(f"Kafka broker is not available: {e}")
            raise
        finally:
            admin_client.close()

    @staticmethod
    def random_string(size=5):
        return ''.join([choice(string.ascii_uppercase + string.digits) for _ in range(size)])
