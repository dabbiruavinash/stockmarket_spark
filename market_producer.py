import random
import time
from datetime import datetime
from connectors.kafka_connector import KafkaConnector
from utils.logger import get_logger

class MarketProducer:
    def __init__(self):
        self.producer = KafkaConnector.get_producer()
        self.logger = get_logger("market_producer")
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        
    def generate_stock_data(self):
        return {
            "symbol": random.choice(self.symbols),
            "price": round(random.uniform(100, 500), 2),
            "volume": random.randint(100, 10000),
            "timestamp": datetime.now().isoformat(),
            "exchange": "NASDAQ"
        }
        
    def run(self, interval=1):
        self.logger.info("Starting stock market producer")
        try:
            while True:
                data = self.generate_stock_data()
                self.producer.send("stock-market-prices", value=data)
                self.logger.debug(f"Sent data: {data}")
                time.sleep(interval)
        except KeyboardInterrupt:
            self.logger.info("Stopping producer")
        except Exception as e:
            self.logger.error(f"Error in producer: {e}")
        finally:
            self.producer.close()