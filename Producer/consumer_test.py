from kafka import KafkaConsumer

config = {
    'bootstrap_servers': 'localhost:29092',
    'client_id': 'consumer-python',}

consumer = KafkaConsumer(**config)

consumer.subscribe(topics=['modification'])

for msg in consumer:
    print(msg)
    print(f"\t\t{msg.value.decode('utf-8')}")