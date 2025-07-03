class Config:
       AWS_REGION = "us-east-1"
       S3_BUCKET = "stock-market-data-bucket"
       GLUE_DATABASE = "stock_market_db"
       ATHENA_TABLE = "stock_prices"
       ATHENA_OUTPUT_LOCATION = f"s3://{S3_BUCKET}/athena_results/"


       @staticmethod
       def get_spark_config():
              return {
                    "spark.sql.shuffle.partitions" : "5",
                    "spark.executor.memory" : "2g",
                    "spark.driver.memory" : "2g" }
