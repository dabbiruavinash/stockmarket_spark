from kafka import kafkaProducer, kafkaConsumer
from json import dumps, loads
from config.kafka_config import kafkaConfig

class KafkaConnector:
        @staticmethod
        def get_producer():
               return KafkaProducer(
                   bootstrap_servers = KafkaConfig.BOOTSTRAP_SERVERS,
                   value_serializer = lambda x: dumps(x).encode('utf-8'))

         @staticmethod
    def get_consumer():
        return KafkaConsumer(
            KafkaConfig.TOPIC_NAME,
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            auto_offset_reset=KafkaConfig.AUTO_OFFSET_RESET,
            enable_auto_commit=True,
            group_id=KafkaConfig.CONSUMER_GROUP,
            value_deserializer=lambda x: loads(x.decode('utf-8')))