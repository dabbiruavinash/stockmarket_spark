from pyspark.sql import SparkSession
from processing.spark_processor import SparkProcessor
from connectors.aws_connector import AWSConnector
from schemas.market_schema import MarketSchema
from utils.logger import get_logger

class MarketConsumer:
    def __init__(self):
        self.logger = get_logger("market_consumer")
        self.spark_processor = SparkProcessor()
        self.aws_connector = AWSConnector()
        self.schema = MarketSchema.get_stock_schema()
        
    def process_stream(self):
        self.logger.info("Starting stock market consumer")
        try:
            df = self.spark_processor.read_kafka_stream()
            
            # Process the stream
            processed_df = self.spark_processor.process_data(df)
            
            # Write to S3 in Parquet format
            s3_path = f"s3://{Config.S3_BUCKET}/stock_prices/"
            self.spark_processor.write_to_s3(processed_df, s3_path, "parquet")
            
            # Create Glue table if not exists
            self.aws_connector.create_glue_table(
                Config.ATHENA_TABLE,
                MarketSchema.get_glue_schema(),
                s3_path
            )
            
            self.logger.info("Data processing completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in consumer: {e}")
            raise e