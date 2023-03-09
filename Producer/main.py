from confluent_kafka import Producer
import logging as lg

def main():
    lg.info("Starting producer")
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    lg.info("Producer started")
    
    
if __name__ == "__main__":
    lg.basicConfig(
        format="%(asctime)s | Producer | %(levelname)s |>> %(message)s",
        level=lg.DEBUG,
        # filename="producer.log",
        # filemode="a",
    )
    main()