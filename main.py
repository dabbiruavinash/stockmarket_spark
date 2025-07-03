import argparse
from producers.market_producer import MarketProducer
from consumers.market_consumer import MarketConsumer
from utils.logger import get_logger

def run_producer():
    producer = MarketProducer()
    producer.run(interval=1)

def run_consumer():
    consumer = MarketConsumer()
    consumer.process_stream()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Market Data Pipeline")
    parser.add_argument("--mode", choices=["producer", "consumer"], required=True)
    
    args = parser.parse_args()
    logger = get_logger("main")
    
    try:
        if args.mode == "producer":
            logger.info("Running in producer mode")
            run_producer()
        else:
            logger.info("Running in consumer mode")
            run_consumer()
    except Exception as e:
        logger.error(f"Error in {args.mode}: {e}")
        raise e