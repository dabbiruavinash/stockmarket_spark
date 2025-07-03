class kafkaConfig:
        BOOTSTRAP_SERVERS = "kafka-broker:9092"
        TOPIC_NAME = "stock-market-prices"
        CONSUMER_GROUP = "stock-market-group"
        AUTO_OFFSET_RESET = "earliest"