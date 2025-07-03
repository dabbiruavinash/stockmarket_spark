from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

class MarketSchema:
    @staticmethod
    def get_stock_schema():
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("exchange", StringType(), False)
        ])
        
    @staticmethod
    def get_glue_schema():
        return [
            {'Name': 'symbol', 'Type': 'string'},
            {'Name': 'price', 'Type': 'double'},
            {'Name': 'volume', 'Type': 'double'},
            {'Name': 'timestamp', 'Type': 'timestamp'},
            {'Name': 'exchange', 'Type': 'string'}
        ]