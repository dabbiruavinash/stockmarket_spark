from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from config.config import Config
from config.kafka_config import KafkaConfig
from schemas.market_schema import MarketSchema
from utils.logger import get_logger

class SparkProcessor:
    def __init__(self):
        self.logger = get_logger("spark_processor")
        self.spark = self._create_spark_session()
        self.schema = MarketSchema.get_stock_schema()
        
    def _create_spark_session(self):
        return SparkSession.builder \
            .appName("StockMarketProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .config("spark.sql.shuffle.partitions", Config.get_spark_config()["spark.sql.shuffle.partitions"]) \
            .getOrCreate()
            
    def read_kafka_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS) \
            .option("subscribe", KafkaConfig.TOPIC_NAME) \
            .option("startingOffsets", "earliest") \
            .load()
            
    def process_data(self, df):
        # Parse JSON from Kafka value
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")
        
        # Add processing time column
        processed_df = parsed_df.withColumn("processing_time", col("timestamp"))
        
        return processed_df
        
    def write_to_s3(self, df, path, format):
        df.writeStream \
            .format(format) \
            .outputMode("append") \
            .option("path", path) \
            .option("checkpointLocation", f"{path}/checkpoints") \
            .start() \
            .awaitTermination()